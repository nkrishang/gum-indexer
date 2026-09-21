//! Logging, metrics and the log rate limiter.
//!
//! Logging policy: state transitions and anomalies are logged, routine events are counted. Every error carries
//! a stable `error.kind`. Repeating conditions go through [`LogLimiter`] so an outage produces a handful of
//! lines rather than thousands (Railway drops everything above 500 lines/s).

pub mod quicknode_usage;

use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// JSON on stdout (parsed by Railway into level + attributes) or human-readable output for local runs.
pub fn init_logging(json: bool) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,alloy=warn,hyper=warn"));
    let registry = tracing_subscriber::registry().with(filter);
    let result = if json {
        registry.with(fmt::layer().json().flatten_event(true).with_current_span(true).with_span_list(false)).try_init()
    } else {
        registry.with(fmt::layer().compact()).try_init()
    };
    let _ = result; // already initialised (tests)
}

const LATENCY_BUCKETS: &[f64] =
    &[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0];

/// Installs the global Prometheus recorder once per process and returns the render handle.
pub fn install_metrics() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let handle = PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), LATENCY_BUCKETS)
                .expect("static buckets are valid")
                .install_recorder()
                .expect("metrics recorder installs once");
            describe();
            handle
        })
        .clone()
}

/// `install_recorder` does not spawn the exporter's housekeeping; without it histograms grow unbounded.
pub fn spawn_upkeep(handle: PrometheusHandle, cancel: tokio_util::sync::CancellationToken) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => handle.run_upkeep(),
            }
        }
    });
}

fn describe() {
    use metrics::{describe_counter as c, describe_gauge as g, describe_histogram as h};
    c!(
        "gum_transfers_indexed_total",
        "Payment transfers to watched addresses, by chain, token and phase (pending|confirmed|orphaned|ignored)"
    );
    c!(
        "gum_transfers_missed_by_push_total",
        "Confirmed transfers that were never announced as pending (push path missed them)"
    );
    c!("gum_thresholds_reached_total", "Watches retired because their confirmed total crossed the threshold");
    c!("gum_logs_scanned_total", "Transfer logs examined, by chain and source (ws|poll|sweep)");
    c!("gum_logs_anomalous_total", "Logs from registry tokens with an unexpected shape, by chain and reason");
    h!("gum_detection_latency_seconds", "Block timestamp to detection, by chain and phase");
    g!("gum_chain_status", "0 down, 1 degraded, 2 healthy");
    g!("gum_chain_head_block", "Latest chain head observed");
    g!("gum_chain_confirmed_block", "Durable cursor: all blocks up to here are fully accounted");
    g!("gum_chain_head_age_seconds", "Seconds since the observed head last advanced");
    g!("gum_chain_leader", "1 when this instance holds the chain's ingest leadership");
    g!("gum_ws_connected", "1 when the chain's WSS connection is up");
    c!("gum_ws_reconnects_total", "WSS sessions ended and re-established, by chain and reason");
    c!("gum_ws_subscriptions_total", "eth_subscribe calls, by chain and kind");
    g!("gum_ws_buckets", "Open recipient-filter subscriptions");
    c!("gum_ingest_mode_switches_total", "Ingest source changes, by chain and target mode");
    c!("gum_sweeps_total", "Confirmation sweeps, by chain and trigger");
    c!("gum_sweep_range_shrinks_total", "eth_getLogs ranges halved after a provider limit error");
    c!("gum_reorgs_total", "Sweeps that orphaned at least one pending transfer");
    c!("gum_rpc_requests_total", "RPC calls, by chain, method and outcome");
    h!("gum_rpc_latency_seconds", "RPC call latency, by chain and method");
    c!("gum_rpc_credits_estimated_total", "Estimated QuickNode credits consumed, by chain and method");
    g!("gum_quicknode_credits_used", "Credits used this billing period (QuickNode Admin API)");
    g!("gum_quicknode_credits_remaining", "Credits remaining this billing period (QuickNode Admin API)");
    g!("gum_watches_active", "Active watches in the in-memory cache, by chain");
    c!("gum_watches_created_total", "Watches registered, by chain and token");
    c!("gum_webhook_deliveries_total", "Webhook delivery attempts, by event and outcome");
    h!("gum_webhook_latency_seconds", "Webhook HTTP round-trip");
    h!("gum_webhook_end_to_end_seconds", "Event creation to successful delivery");
    g!("gum_outbox_pending", "Undelivered webhook events");
    g!("gum_outbox_dead", "Webhook events given up on");
    g!("gum_outbox_oldest_age_seconds", "Age of the oldest undelivered webhook event");
    c!("gum_task_restarts_total", "Supervised task restarts, by chain and task");
}

/// Suppresses repeats of the same condition: the first occurrence passes, then one summary per `interval`.
#[derive(Debug)]
pub struct LogLimiter {
    interval: Duration,
    state: Mutex<HashMap<&'static str, (Instant, u64)>>,
}

impl LogLimiter {
    pub fn new(interval: Duration) -> Self {
        Self { interval, state: Mutex::new(HashMap::new()) }
    }

    /// `Some(n)` → log now; `n` occurrences were suppressed since the last line. `None` → stay quiet.
    pub fn check(&self, key: &'static str) -> Option<u64> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: &'static str, now: Instant) -> Option<u64> {
        let mut state = self.state.lock().unwrap();
        match state.get_mut(key) {
            None => {
                state.insert(key, (now, 0));
                Some(0)
            }
            Some((last, suppressed)) if now.saturating_duration_since(*last) >= self.interval => {
                let n = std::mem::take(suppressed);
                *last = now;
                Some(n)
            }
            Some((_, suppressed)) => {
                *suppressed += 1;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_passes_first_then_summarises() {
        let l = LogLimiter::new(Duration::from_secs(30));
        let t0 = Instant::now();
        assert_eq!(l.check_at("rpc_transient", t0), Some(0));
        assert_eq!(l.check_at("rpc_transient", t0 + Duration::from_secs(1)), None);
        assert_eq!(l.check_at("rpc_transient", t0 + Duration::from_secs(2)), None);
        assert_eq!(l.check_at("rpc_auth", t0 + Duration::from_secs(2)), Some(0), "keys are independent");
        assert_eq!(l.check_at("rpc_transient", t0 + Duration::from_secs(31)), Some(2));
    }
}
