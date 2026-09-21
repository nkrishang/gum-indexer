//! Per-chain health state machine. Logs and the status gauge change on *transitions* only.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainStatus {
    /// RPC failing persistently or the chain head is not advancing: payments cannot be detected right now.
    Down = 0,
    /// Working with reduced guarantees: intermittent RPC errors, or WSS down and running on the poll fallback.
    Degraded = 1,
    Healthy = 2,
}

const DEGRADED_AFTER_FAILURES: u32 = 2;
const DOWN_AFTER_FAILURES: u32 = 5;

#[derive(Debug)]
struct Inner {
    status: ChainStatus,
    consecutive_failures: u32,
    last_error_kind: Option<&'static str>,
    last_error: Option<String>,
    last_success_at: Option<Instant>,
    head: u64,
    head_changed_at: Option<Instant>,
    head_observed_at: Option<Instant>,
    ws_expected: bool,
    ws_connected: bool,
    mode: &'static str,
    leader: bool,
    confirmed_block: u64,
    last_sweep_at: Option<Instant>,
}

#[derive(Debug)]
pub struct ChainHealth {
    chain: &'static str,
    head_stall_threshold: Duration,
    inner: Mutex<Inner>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub status: ChainStatus,
    pub leader: bool,
    pub ingest_mode: &'static str,
    pub ws_connected: bool,
    pub head_block: u64,
    pub head_age_ms: Option<u64>,
    pub confirmed_block: u64,
    pub last_sweep_age_ms: Option<u64>,
    pub consecutive_rpc_failures: u32,
    pub last_rpc_success_age_ms: Option<u64>,
    pub last_error_kind: Option<&'static str>,
    pub last_error: Option<String>,
}

impl ChainHealth {
    pub fn new(chain: &'static str, head_stall_threshold: Duration) -> Self {
        let inner = Inner {
            status: ChainStatus::Healthy,
            consecutive_failures: 0,
            last_error_kind: None,
            last_error: None,
            last_success_at: None,
            head: 0,
            head_changed_at: None,
            head_observed_at: None,
            ws_expected: false,
            ws_connected: false,
            mode: "starting",
            leader: false,
            confirmed_block: 0,
            last_sweep_at: None,
        };
        metrics::gauge!("gum_chain_status", "chain" => chain).set(ChainStatus::Healthy as u8 as f64);
        Self { chain, head_stall_threshold, inner: Mutex::new(inner) }
    }

    pub fn record_rpc_ok(&self) {
        self.update(|i| {
            i.consecutive_failures = 0;
            i.last_success_at = Some(Instant::now());
        });
    }

    pub fn record_rpc_err(&self, kind: &'static str, error: &str) {
        self.update(|i| {
            i.consecutive_failures = i.consecutive_failures.saturating_add(1);
            i.last_error_kind = Some(kind);
            i.last_error = Some(error.chars().take(300).collect());
        });
    }

    pub fn observe_head(&self, head: u64) {
        self.observe_head_at(head, Instant::now());
    }

    fn observe_head_at(&self, head: u64, now: Instant) {
        self.update(|i| {
            if head > i.head || i.head_changed_at.is_none() {
                i.head = head.max(i.head);
                i.head_changed_at = Some(now);
            }
            i.head_observed_at = Some(now);
        });
        metrics::gauge!("gum_chain_head_block", "chain" => self.chain).set(head as f64);
    }

    pub fn observe_sweep(&self, confirmed_block: u64) {
        self.update(|i| {
            i.confirmed_block = confirmed_block;
            i.last_sweep_at = Some(Instant::now());
        });
        metrics::gauge!("gum_chain_confirmed_block", "chain" => self.chain).set(confirmed_block as f64);
    }

    pub fn set_ws(&self, expected: bool, connected: bool) {
        self.update(|i| {
            i.ws_expected = expected;
            i.ws_connected = connected;
        });
        metrics::gauge!("gum_ws_connected", "chain" => self.chain).set(if connected { 1.0 } else { 0.0 });
    }

    pub fn set_mode(&self, mode: &'static str) {
        self.update(|i| i.mode = mode);
    }

    pub fn set_leader(&self, leader: bool) {
        self.update(|i| i.leader = leader);
        metrics::gauge!("gum_chain_leader", "chain" => self.chain).set(if leader { 1.0 } else { 0.0 });
    }

    pub fn head(&self) -> u64 {
        self.inner.lock().unwrap().head
    }

    pub fn status(&self) -> ChainStatus {
        self.inner.lock().unwrap().status
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let i = self.inner.lock().unwrap();
        let age = |t: Option<Instant>| t.map(|t| t.elapsed().as_millis() as u64);
        if let Some(a) = age(i.head_changed_at) {
            metrics::gauge!("gum_chain_head_age_seconds", "chain" => self.chain).set(a as f64 / 1000.0);
        }
        HealthSnapshot {
            status: i.status,
            leader: i.leader,
            ingest_mode: i.mode,
            ws_connected: i.ws_connected,
            head_block: i.head,
            head_age_ms: age(i.head_changed_at),
            confirmed_block: i.confirmed_block,
            last_sweep_age_ms: age(i.last_sweep_at),
            consecutive_rpc_failures: i.consecutive_failures,
            last_rpc_success_age_ms: age(i.last_success_at),
            last_error_kind: i.last_error_kind,
            last_error: i.last_error.clone(),
        }
    }

    fn update(&self, f: impl FnOnce(&mut Inner)) {
        let mut i = self.inner.lock().unwrap();
        f(&mut i);
        let next = evaluate(&i, self.head_stall_threshold);
        if next == i.status {
            return;
        }
        let prev = std::mem::replace(&mut i.status, next);
        metrics::gauge!("gum_chain_status", "chain" => self.chain).set(next as u8 as f64);
        metrics::counter!("gum_chain_status_transitions_total", "chain" => self.chain, "to" => status_label(next))
            .increment(1);
        let (failures, kind, err, ws, head) = (
            i.consecutive_failures,
            i.last_error_kind.unwrap_or("none"),
            i.last_error.clone().unwrap_or_default(),
            i.ws_connected,
            i.head,
        );
        drop(i);
        match next {
            ChainStatus::Down => tracing::error!(
                chain = self.chain, from = ?prev, to = ?next, consecutive_rpc_failures = failures, error.kind = kind,
                last_error = %err, head, "chain is DOWN: payments on this chain are not being detected"
            ),
            ChainStatus::Degraded => tracing::warn!(
                chain = self.chain, from = ?prev, to = ?next, consecutive_rpc_failures = failures, error.kind = kind,
                last_error = %err, ws_connected = ws, "chain degraded"
            ),
            ChainStatus::Healthy => {
                tracing::info!(chain = self.chain, from = ?prev, to = ?next, head, "chain recovered")
            }
        }
    }
}

fn status_label(s: ChainStatus) -> &'static str {
    match s {
        ChainStatus::Down => "down",
        ChainStatus::Degraded => "degraded",
        ChainStatus::Healthy => "healthy",
    }
}

fn evaluate(i: &Inner, stall: Duration) -> ChainStatus {
    // Stalled = we looked again after the threshold had passed and the head still had not moved.
    // (Merely not having looked for a while — e.g. an idle, parked chain — is not a stall.)
    let stalled = match (i.head_changed_at, i.head_observed_at) {
        (Some(changed), Some(observed)) => observed.saturating_duration_since(changed) > stall,
        _ => false,
    };
    if i.consecutive_failures >= DOWN_AFTER_FAILURES || stalled {
        ChainStatus::Down
    } else if i.consecutive_failures >= DEGRADED_AFTER_FAILURES || (i.ws_expected && !i.ws_connected) {
        ChainStatus::Degraded
    } else {
        ChainStatus::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health() -> ChainHealth {
        ChainHealth::new("testchain", Duration::from_secs(10))
    }

    #[test]
    fn failures_escalate_and_one_success_recovers() {
        let h = health();
        h.record_rpc_err("rpc_transient", "boom");
        assert_eq!(h.status(), ChainStatus::Healthy, "a single blip is not an incident");
        h.record_rpc_err("rpc_transient", "boom");
        assert_eq!(h.status(), ChainStatus::Degraded);
        for _ in 0..3 {
            h.record_rpc_err("rpc_transient", "boom");
        }
        assert_eq!(h.status(), ChainStatus::Down);
        h.record_rpc_ok();
        assert_eq!(h.status(), ChainStatus::Healthy);
        assert_eq!(h.snapshot().last_error_kind, Some("rpc_transient"), "last error stays visible after recovery");
    }

    #[test]
    fn head_stall_means_down_even_when_rpc_answers() {
        let h = health();
        let t0 = Instant::now();
        h.observe_head_at(100, t0);
        h.observe_head_at(100, t0 + Duration::from_secs(5));
        assert_eq!(h.status(), ChainStatus::Healthy);
        h.observe_head_at(100, t0 + Duration::from_secs(11));
        assert_eq!(h.status(), ChainStatus::Down, "node answers but the chain is not producing blocks");
        h.observe_head_at(101, t0 + Duration::from_secs(12));
        assert_eq!(h.status(), ChainStatus::Healthy);
    }

    #[test]
    fn running_on_poll_fallback_is_degraded_not_down() {
        let h = health();
        h.set_ws(true, true);
        assert_eq!(h.status(), ChainStatus::Healthy);
        h.set_ws(true, false);
        assert_eq!(h.status(), ChainStatus::Degraded);
        h.set_ws(false, false);
        assert_eq!(h.status(), ChainStatus::Healthy, "poll mode by configuration is not a degradation");
    }
}
