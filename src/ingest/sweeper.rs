//! ConfirmSweeper: the authoritative pass. Scans `[cursor+1, head - confirmations]` with `eth_getLogs` and
//! applies each chunk in one transaction (confirm, orphan, count, retire, advance cursor).
//!
//! It runs when a pending transfer reaches depth, when the push path (re)connects, at startup, and on a slow
//! safety tick. A chain with no watches and nothing pending makes no `eth_getLogs` calls at all.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, B256},
    rpc::types::Filter,
};
use tokio_util::sync::CancellationToken;

use super::{ChainRuntime, IngestError, SweepTrigger, matcher::TRANSFER_TOPIC};
use crate::{
    rpc::errors::RpcErrorClass,
    store::{
        self,
        sweep::{self, SweepResult},
    },
};

/// Watches backfilled per `eth_getLogs` pass (their addresses go into one topic filter).
const BACKFILL_BATCH: i64 = 50;

struct Sweeper {
    rt: Arc<ChainRuntime>,
    /// Blocks holding transfers announced as pending and not yet resolved.
    pending_blocks: BTreeSet<u64>,
    /// Current `eth_getLogs` span; shrinks on provider limit errors, grows back on success up to `ceiling`.
    range: u64,
    /// Largest span the provider is known to accept. Rejected calls may still be billed, so once a limit is
    /// discovered we stay under it instead of probing upwards again after every success.
    ceiling: u64,
    ceiling_set_at: Instant,
    last_head_probe: Instant,
}

pub async fn run(rt: Arc<ChainRuntime>, cancel: CancellationToken) -> Result<(), IngestError> {
    let cfg = &rt.spec.cfg;
    let mut s = Sweeper {
        pending_blocks: store::pending_blocks(&rt.pool, rt.spec.chain_id).await?.into_iter().collect(),
        range: cfg.max_log_range,
        ceiling: cfg.max_log_range,
        ceiling_set_at: Instant::now(),
        last_head_probe: Instant::now(),
        rt: rt.clone(),
    };
    let mut rx = rt.sweep_rx.lock().await;
    let safety = Duration::from_millis(cfg.safety_sweep_interval_ms);
    let idle_probe = Duration::from_millis(cfg.idle_probe_interval_ms);
    let mut tick = tokio::time::interval(safety.min(idle_probe));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_safety = Instant::now();
    let mut due: Option<Instant> = None;

    // Catch up on whatever happened while nobody was leading this chain.
    let mut next: Option<&'static str> = Some("startup");

    loop {
        if let Some(reason) = next.take() {
            match s.sweep(reason).await {
                Ok(again_in) => due = again_in.map(|d| Instant::now() + d),
                Err(e) => {
                    if let Some(suppressed) = rt.limiter.check("sweep_failed") {
                        tracing::warn!(chain = rt.spec.name, error.kind = e.kind(), error = %e, reason, suppressed, "sweep failed; will retry");
                    }
                    // Database gone or RPC exhausted its retries: retry soon without spinning.
                    due = Some(Instant::now() + Duration::from_secs(2));
                    if matches!(e, IngestError::Store(_)) && rt.pool.is_closed() {
                        return Err(e);
                    }
                }
            }
            last_safety = Instant::now();
        }

        let sleep_due = async {
            match due {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            trigger = rx.recv() => match trigger {
                Some(SweepTrigger::Now(reason)) => next = Some(reason),
                Some(SweepTrigger::PendingAt { block }) => {
                    s.pending_blocks.insert(block);
                    let at = Instant::now() + s.depth_delay(cfg.confirmations);
                    due = Some(due.map_or(at, |d| d.min(at)));
                }
                None => return Ok(()),
            },
            _ = sleep_due => { due = None; next = Some("pending_depth"); }
            _ = tick.tick() => {
                let active = !rt.cache.is_empty() || !s.pending_blocks.is_empty();
                if active && last_safety.elapsed() >= safety {
                    next = Some("safety");
                } else if !active && s.last_head_probe.elapsed() >= idle_probe {
                    // Parked chain: one cheap call keeps outage detection alive; failures are tracked by health.
                    s.last_head_probe = Instant::now();
                    let _ = rt.rpc.block_number().await;
                    s.expire().await;
                }
            }
        }
    }
}

/// Extracts the block-range limit from messages like "eth_getLogs is limited to a 10,000 range".
fn range_limit_hint(message: &str) -> Option<u64> {
    let lower = message.to_ascii_lowercase();
    let tail = &lower[lower.find("limited to")? + "limited to".len()..];
    let digits: String = tail
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit() || *c == ',')
        .filter(|c| *c != ',')
        .collect();
    digits.parse().ok().filter(|n| *n > 0)
}

impl Sweeper {
    fn depth_delay(&self, blocks: u64) -> Duration {
        let bt = self.rt.spec.cfg.block_time();
        bt * (blocks.max(1) as u32) + (bt / 4).max(Duration::from_millis(50))
    }

    /// Runs one sweep. Returns how long to wait before sweeping again if transfers are still short of depth.
    async fn sweep(&mut self, reason: &'static str) -> Result<Option<Duration>, IngestError> {
        let rt = self.rt.clone();
        let (chain, cfg) = (rt.spec.name, &rt.spec.cfg);
        metrics::counter!("gum_sweeps_total", "chain" => chain, "trigger" => reason).increment(1);
        self.expire().await;

        self.last_head_probe = Instant::now();
        let head = rt.rpc.block_number().await?;
        let target = head.saturating_sub(cfg.confirmations);

        // Skips everything below the oldest active watch (or up to `target` when nothing is watched).
        let mut cursor = sweep::fast_forward_cursor(&rt.pool, rt.spec.chain_id, target).await?;
        if cursor > target + cfg.confirmations + 1 {
            // The node answering is far behind our durable cursor (lagging replica). Not an error; wait.
            if let Some(suppressed) = rt.limiter.check("node_behind_cursor") {
                tracing::warn!(
                    chain,
                    head,
                    cursor,
                    suppressed,
                    error.kind = "rpc_node_behind",
                    "RPC node is behind the durable cursor"
                );
            }
        }

        let tokens = rt.spec.token_addresses();
        while cursor < target {
            let from = cursor + 1;
            let to = (from + self.range - 1).min(target);
            let filter =
                Filter::new().address(tokens.clone()).event_signature(TRANSFER_TOPIC).from_block(from).to_block(to);
            let logs = match rt.rpc.get_logs(&filter).await {
                Ok(logs) => logs,
                Err(e) if e.class() == RpcErrorClass::RangeTooLarge && self.range > 1 => {
                    self.shrink_range(from, to, &e.to_string());
                    continue;
                }
                Err(e) if e.class() == RpcErrorClass::AheadOfHead => break,
                Err(e) => return Err(e.into()),
            };
            metrics::counter!("gum_logs_scanned_total", "chain" => chain, "source" => "sweep")
                .increment(logs.len() as u64);

            match sweep::apply_sweep(&rt.pool, &rt.spec, &rt.cache, cursor, to, &logs).await? {
                SweepResult::CursorMoved { expected, actual } => {
                    // Only possible if two instances believe they lead. The transaction wrote nothing.
                    tracing::warn!(
                        chain,
                        expected,
                        actual,
                        error.kind = "cursor_moved",
                        "durable cursor moved under this sweep; yielding"
                    );
                    return Ok(None);
                }
                SweepResult::Applied(out) => {
                    self.report(&out, reason, &logs);
                    rt.note_cache_changed(&out.discovered, out.completed.len());
                    if out.events > 0 {
                        rt.dispatcher_wake.notify_one();
                    }
                }
            }
            cursor = to;
            rt.health.observe_sweep(cursor);
            // A limit tied to response size rather than block count can ease; re-probe the ceiling hourly.
            if self.ceiling < cfg.max_log_range && self.ceiling_set_at.elapsed() > Duration::from_secs(3600) {
                self.ceiling = cfg.max_log_range;
            }
            if self.range < self.ceiling {
                self.range = (self.range * 2).min(self.ceiling);
            }
        }

        self.pending_blocks.retain(|b| *b > cursor);
        self.backfill().await?;
        Ok(self
            .pending_blocks
            .first()
            .map(|first| self.depth_delay(first.saturating_sub(target).min(cfg.confirmations))))
    }

    /// The provider rejected `[from, to]` as too large: use the limit it states ("limited to a 10,000
    /// range"), otherwise halve the span, and remember it as the ceiling.
    fn shrink_range(&mut self, from: u64, to: u64, error: &str) {
        let chain = self.rt.spec.name;
        let span = to - from + 1;
        self.range = match range_limit_hint(error) {
            Some(limit) if limit < span => limit.max(1),
            _ => (span / 2).max(1),
        };
        self.ceiling = self.range;
        self.ceiling_set_at = Instant::now();
        metrics::counter!("gum_sweep_range_shrinks_total", "chain" => chain).increment(1);
        if let Some(suppressed) = self.rt.limiter.check("range_shrink") {
            tracing::warn!(
                chain,
                from,
                to,
                new_range = self.range,
                suppressed,
                error,
                "provider rejected log range; shrinking"
            );
        }
    }

    /// Scans, once, the already-swept blocks that late-registered watches care about (see
    /// `store::create_watch`). All pending backfills of a batch share one `eth_getLogs` pass over
    /// the union of their ranges, filtered to their addresses.
    async fn backfill(&mut self) -> Result<(), IngestError> {
        let rt = self.rt.clone();
        let chain = rt.spec.name;
        loop {
            let batch = sweep::pending_backfills(&rt.pool, rt.spec.chain_id, BACKFILL_BATCH).await?;
            let (Some(from), Some(to)) =
                (batch.iter().map(|b| b.start_block).min(), batch.iter().map(|b| b.backfill_to).max())
            else {
                return Ok(());
            };
            let mut tokens: Vec<Address> = batch.iter().map(|b| b.token_address).collect();
            tokens.sort_unstable();
            tokens.dedup();
            let recipients: Vec<B256> = batch.iter().map(|b| b.payment_address.into_word()).collect();

            let mut logs = Vec::new();
            let mut at = from;
            while at <= to {
                let end = (at + self.range - 1).min(to);
                let filter = Filter::new()
                    .address(tokens.clone())
                    .event_signature(TRANSFER_TOPIC)
                    .topic2(recipients.clone())
                    .from_block(at)
                    .to_block(end);
                match rt.rpc.get_logs(&filter).await {
                    Ok(mut found) => {
                        logs.append(&mut found);
                        at = end + 1;
                    }
                    Err(e) if e.class() == RpcErrorClass::RangeTooLarge && self.range > 1 => {
                        self.shrink_range(at, end, &e.to_string());
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            metrics::counter!("gum_logs_scanned_total", "chain" => chain, "source" => "backfill")
                .increment(logs.len() as u64);

            let out = sweep::apply_backfill(&rt.pool, &rt.spec, &rt.cache, &batch, &logs).await?;
            tracing::info!(
                chain,
                watches = batch.len(),
                from,
                to,
                transfers = out.confirmed.len(),
                "backfilled watches registered after their blocks were swept"
            );
            self.report(&out, "backfill", &logs);
            rt.note_cache_changed(&[], out.completed.len());
            if out.events > 0 {
                rt.dispatcher_wake.notify_one();
            }
            if (batch.len() as i64) < BACKFILL_BATCH {
                return Ok(());
            }
        }
    }

    fn report(&self, out: &sweep::SweepOutcome, reason: &'static str, logs: &[alloy::rpc::types::Log]) {
        let chain = self.rt.spec.name;
        let now = chrono::Utc::now().timestamp() as f64;
        for m in &out.confirmed {
            let token = self.rt.spec.tokens[m.token_idx as usize].symbol;
            metrics::counter!("gum_transfers_indexed_total", "chain" => chain, "token" => token, "phase" => "confirmed").increment(1);
            let ts = logs.iter().find(|l| l.block_hash == Some(m.block_hash)).and_then(|l| l.block_timestamp);
            if let Some(ts) = ts {
                metrics::histogram!("gum_detection_latency_seconds", "chain" => chain, "phase" => "confirmed")
                    .record((now - ts as f64).max(0.0));
            }
        }
        // Backfilled transfers predate their watch, so the push path could never have reported them.
        if out.confirmed_without_pending > 0 && reason != "backfill" {
            metrics::counter!("gum_transfers_missed_by_push_total", "chain" => chain)
                .increment(out.confirmed_without_pending as u64);
            // Expected after downtime or a WSS gap (reason = startup/ws_connect); on a plain safety tick it means
            // the push path silently dropped a notification.
            if reason == "safety" {
                tracing::warn!(
                    chain,
                    count = out.confirmed_without_pending,
                    error.kind = "push_missed_transfer",
                    "safety sweep confirmed transfers the push path never reported"
                );
            }
        }
        if out.orphaned > 0 {
            metrics::counter!("gum_reorgs_total", "chain" => chain).increment(1);
            metrics::counter!("gum_transfers_indexed_total", "chain" => chain, "token" => "all", "phase" => "orphaned")
                .increment(out.orphaned as u64);
            tracing::warn!(
                chain,
                orphaned = out.orphaned,
                "reorg: pending transfers were not canonical at confirmation depth"
            );
        }
        if out.ignored > 0 {
            metrics::counter!("gum_transfers_indexed_total", "chain" => chain, "token" => "all", "phase" => "ignored")
                .increment(out.ignored as u64);
        }
        for (_, id) in &out.completed {
            metrics::counter!("gum_thresholds_reached_total", "chain" => chain).increment(1);
            tracing::info!(chain, watch_id = %id, "threshold reached; watch retired");
        }
    }

    /// Retires overdue watches (leader only, so exactly one instance does it per chain).
    async fn expire(&self) {
        let rt = &self.rt;
        match store::expire_watches(&rt.pool, &rt.spec, 500).await {
            Ok(expired) if !expired.is_empty() => {
                for w in &expired {
                    if let Some((key, _)) = w.cache_entry(&rt.spec) {
                        rt.remove_watch(&key, w.id);
                    }
                }
                rt.dispatcher_wake.notify_one();
                tracing::info!(chain = rt.spec.name, count = expired.len(), "watches expired");
            }
            Ok(_) => {}
            Err(e) => {
                if let Some(suppressed) = rt.limiter.check("expire_failed") {
                    tracing::warn!(chain = rt.spec.name, error.kind = e.kind(), error = %e, suppressed, "expiring watches failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::range_limit_hint;

    #[test]
    fn provider_range_limits_are_read_from_the_error() {
        assert_eq!(range_limit_hint("error code -32602: eth_getLogs is limited to a 10,000 range"), Some(10_000));
        assert_eq!(range_limit_hint("eth_getLogs is limited to a 100 range"), Some(100));
        assert_eq!(range_limit_hint("eth_getLogs is limited to a 2,000 range"), Some(2_000));
        assert_eq!(range_limit_hint("query returned more than 10000 results"), None);
        assert_eq!(range_limit_hint("backend response too large"), None);
    }
}
