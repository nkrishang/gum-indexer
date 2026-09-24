//! The pending path: learns about transfers at chain head as fast as possible. Best effort by design —
//! anything it misses is caught by the sweeper — so every failure here degrades latency, never correctness.
//!
//! * `ws_targeted`: `eth_subscribe(logs)` filtered on `topics[2] ∈ bucket`, one subscription per bucket of
//!   recipients. We are only notified of (and billed for) our own payments.
//! * `ws_firehose`: one token-wide subscription, recipients matched in-process.
//! * `poll`: `eth_getLogs(cursor+1 ..= latest)` on a timer. Also the automatic fallback while WSS is down.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, B256},
    providers::{DynProvider, Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter, Log},
};
use tokio::sync::{broadcast::error::RecvError, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    ChainRuntime, IngestError, SweepTrigger, WatchChange,
    matcher::{Skip, TRANSFER_TOPIC, match_log},
    sleep_or_cancel,
};
use crate::{config::IngestMode, rpc::errors::RpcErrorClass, store};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Targeted,
    Firehose,
    Poll,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Targeted => "ws_targeted",
            Self::Firehose => "ws_firehose",
            Self::Poll => "poll",
        }
    }
}

fn from_cfg(m: IngestMode) -> Mode {
    match m {
        IngestMode::WsTargeted | IngestMode::Auto => Mode::Targeted,
        IngestMode::WsFirehose => Mode::Firehose,
        IngestMode::Poll => Mode::Poll,
    }
}

/// `auto` uses recipient-filtered subscriptions up to `ws_targeted_max` watches, with hysteresis so a
/// watchlist hovering around the limit does not flap between modes.
fn desired_mode(rt: &ChainRuntime, current: Option<Mode>) -> Mode {
    let cfg = &rt.spec.cfg;
    if cfg.ingest_mode != IngestMode::Auto {
        return from_cfg(cfg.ingest_mode);
    }
    let watches = rt.cache.len();
    let large = from_cfg(cfg.large_scale_mode);
    match current {
        Some(m) if m == large && m != Mode::Targeted => {
            if watches < cfg.ws_targeted_max * 8 / 10 {
                Mode::Targeted
            } else {
                large
            }
        }
        _ => {
            if watches > cfg.ws_targeted_max {
                large
            } else {
                Mode::Targeted
            }
        }
    }
}

enum SessionEnd {
    Cancelled,
    ModeChanged,
    Parked,
    Disconnected(&'static str),
}

pub async fn run(rt: Arc<ChainRuntime>, cancel: CancellationToken) -> Result<(), IngestError> {
    let chain = rt.spec.name;
    let cfg = rt.spec.cfg.clone();
    let mut changes = rt.watch_rx.lock().await;
    let mut current: Option<Mode> = None;
    let mut ws_down_since: Option<Instant> = None;
    let mut reconnect_delay = Duration::from_millis(250);
    let mut poll = Poller::new(rt.clone());

    while !cancel.is_cancelled() {
        // Parked: nothing to watch means no subscriptions and no polling.
        if rt.cache.is_empty() {
            rt.health.set_mode("parked");
            rt.health.set_ws(false, false);
            current = None;
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                change = changes.recv() => if change.is_none() { return Ok(()) },
            }
            continue;
        }

        let mode = desired_mode(&rt, current);
        if current != Some(mode) {
            if current.is_some() {
                tracing::info!(
                    chain,
                    from = current.map(Mode::label),
                    to = mode.label(),
                    watches = rt.cache.len(),
                    "ingest mode changed"
                );
            }
            metrics::counter!("gum_ingest_mode_switches_total", "chain" => chain, "mode" => mode.label()).increment(1);
            current = Some(mode);
        }

        if mode == Mode::Poll {
            rt.health.set_mode(mode.label());
            rt.health.set_ws(false, false);
            poll.tick().await;
            drain(&mut changes);
            if sleep_or_cancel(cfg.poll_interval(), &cancel).await {
                return Ok(());
            }
            continue;
        }

        // WSS modes. Retries are handled here rather than inside alloy so that every reconnect is visible
        // (metric, health) and followed by a catch-up sweep from the durable cursor.
        let connect = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_ws(WsConnect::new(cfg.ws_url.clone()).with_max_retries(0))
            .await;
        let provider = match connect {
            Ok(p) => p.erased(),
            Err(e) => {
                let down_for = ws_down_since.get_or_insert_with(Instant::now).elapsed();
                rt.health.set_ws(true, false);
                if let Some(suppressed) = rt.limiter.check("ws_connect_failed") {
                    tracing::warn!(chain, error.kind = "ws_connect_failed", error = %e, down_ms = down_for.as_millis() as u64, suppressed, "WSS connect failed");
                }
                if down_for >= Duration::from_millis(cfg.ws_down_fallback_ms) {
                    rt.health.set_mode("poll_fallback");
                    poll.tick().await;
                    reconnect_delay = reconnect_delay.max(cfg.poll_interval());
                }
                if sleep_or_cancel(reconnect_delay, &cancel).await {
                    return Ok(());
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(10)).max(Duration::from_millis(250));
                continue;
            }
        };
        if let Some(since) = ws_down_since.take() {
            tracing::info!(chain, down_ms = since.elapsed().as_millis() as u64, "WSS reconnected");
        }
        reconnect_delay = Duration::from_millis(250);
        rt.health.set_mode(mode.label());
        rt.health.set_ws(true, true);
        poll.reset();

        let end = session(&rt, &provider, mode, &mut changes, &cancel).await;
        match end {
            SessionEnd::Cancelled => return Ok(()),
            SessionEnd::ModeChanged | SessionEnd::Parked => rt.health.set_ws(false, false),
            SessionEnd::Disconnected(reason) => {
                rt.health.set_ws(true, false);
                ws_down_since = Some(Instant::now());
                metrics::counter!("gum_ws_reconnects_total", "chain" => chain, "reason" => reason).increment(1);
                if let Some(suppressed) = rt.limiter.check("ws_session_ended") {
                    tracing::warn!(
                        chain,
                        reason,
                        error.kind = "ws_disconnected",
                        suppressed,
                        "WSS session ended; reconnecting"
                    );
                }
            }
        }
    }
    Ok(())
}

fn drain(changes: &mut mpsc::UnboundedReceiver<WatchChange>) {
    while changes.try_recv().is_ok() {}
}

// ---------------------------------------------------------------------------------------------
// WSS session
// ---------------------------------------------------------------------------------------------

enum BucketMsg {
    Log(Box<Log>),
    Lagged(u64),
    Closed,
}

struct Bucket {
    addresses: Vec<Address>,
    sub_id: B256,
    task: tokio::task::JoinHandle<()>,
}

struct Session<'a> {
    rt: &'a Arc<ChainRuntime>,
    provider: &'a DynProvider,
    tx: mpsc::Sender<BucketMsg>,
    sealed: Vec<Bucket>,
    /// The one bucket still below `ws_bucket_size`; it is replaced (subscribe new, then drop old) as watches arrive.
    open: Option<Bucket>,
    removed_since_rebuild: usize,
}

impl Session<'_> {
    async fn subscribe(&self, recipients: Option<&[Address]>, kind: &'static str) -> Result<Bucket, &'static str> {
        let spec = &self.rt.spec;
        let mut filter = Filter::new().address(spec.log_addresses()).event_signature(TRANSFER_TOPIC);
        if let Some(addrs) = recipients {
            filter = filter.topic2(addrs.iter().map(|a| a.into_word()).collect::<Vec<B256>>());
        }
        let mut sub = match self.provider.subscribe_logs(&filter).channel_size(spec.cfg.ws_channel_size).await {
            Ok(sub) => sub,
            Err(e) => {
                if let Some(suppressed) = self.rt.limiter.check("ws_subscribe_failed") {
                    tracing::warn!(chain = spec.name, kind, recipients = recipients.map_or(0, <[Address]>::len), error.kind = "ws_subscribe_failed", error = %e, suppressed, "eth_subscribe failed");
                }
                return Err("subscribe_failed");
            }
        };
        metrics::counter!("gum_ws_subscriptions_total", "chain" => spec.name, "kind" => kind).increment(1);
        metrics::counter!("gum_rpc_credits_estimated_total", "chain" => spec.name, "method" => "eth_subscribe")
            .increment(spec.cfg.credits_per_call as u64);
        let sub_id = *sub.local_id();
        let tx = self.tx.clone();
        let task = tokio::spawn(async move {
            loop {
                let msg = match sub.recv().await {
                    Ok(log) => BucketMsg::Log(Box::new(log)),
                    // The consumer fell behind alloy's broadcast buffer: notifications were dropped.
                    Err(RecvError::Lagged(n)) => BucketMsg::Lagged(n),
                    Err(RecvError::Closed) => {
                        let _ = tx.send(BucketMsg::Closed).await;
                        return;
                    }
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
            }
        });
        Ok(Bucket { addresses: recipients.map(<[Address]>::to_vec).unwrap_or_default(), sub_id, task })
    }

    async fn retire(&self, bucket: Bucket) {
        bucket.task.abort();
        let _ = self.provider.unsubscribe(bucket.sub_id).await;
    }

    /// Subscribes the whole current watchlist from scratch.
    async fn build_targeted(&mut self) -> Result<(), &'static str> {
        let size = self.rt.spec.cfg.ws_bucket_size;
        let mut addresses = self.rt.cache.addresses();
        addresses.sort_unstable();
        addresses.dedup();
        let mut fresh = Vec::new();
        for chunk in addresses.chunks(size) {
            fresh.push(self.subscribe(Some(chunk), "bucket").await?);
        }
        // New subscriptions are live before the old ones go away: no gap.
        for old in self.sealed.drain(..).chain(self.open.take()).collect::<Vec<_>>() {
            self.retire(old).await;
        }
        if fresh.last().is_some_and(|b| b.addresses.len() < size) {
            self.open = fresh.pop();
        }
        self.sealed = fresh;
        self.removed_since_rebuild = 0;
        self.publish();
        Ok(())
    }

    /// Adds newly registered recipients by replacing the open bucket.
    async fn add_recipients(&mut self, mut new: Vec<Address>) -> Result<(), &'static str> {
        let size = self.rt.spec.cfg.ws_bucket_size;
        new.sort_unstable();
        new.dedup();
        let mut addresses = self.open.as_ref().map(|b| b.addresses.clone()).unwrap_or_default();
        addresses.extend(new);
        addresses.sort_unstable();
        addresses.dedup();
        let mut replacement = Vec::new();
        for chunk in addresses.chunks(size) {
            replacement.push(self.subscribe(Some(chunk), "bucket").await?);
        }
        if let Some(old) = self.open.take() {
            self.retire(old).await;
        }
        if replacement.last().is_some_and(|b| b.addresses.len() < size) {
            self.open = replacement.pop();
        }
        self.sealed.extend(replacement);
        self.publish();
        Ok(())
    }

    fn subscribed(&self) -> usize {
        self.sealed.iter().chain(self.open.iter()).map(|b| b.addresses.len()).sum()
    }

    fn publish(&self) {
        metrics::gauge!("gum_ws_buckets", "chain" => self.rt.spec.name)
            .set((self.sealed.len() + usize::from(self.open.is_some())) as f64);
    }

    async fn close(mut self) {
        for bucket in self.sealed.drain(..).chain(self.open.take()).collect::<Vec<_>>() {
            bucket.task.abort();
        }
        metrics::gauge!("gum_ws_buckets", "chain" => self.rt.spec.name).set(0.0);
    }
}

async fn session(
    rt: &Arc<ChainRuntime>,
    provider: &DynProvider,
    mode: Mode,
    changes: &mut mpsc::UnboundedReceiver<WatchChange>,
    cancel: &CancellationToken,
) -> SessionEnd {
    let cfg = &rt.spec.cfg;
    let (tx, mut rx) = mpsc::channel::<BucketMsg>(cfg.ws_channel_size);
    let mut s = Session { rt, provider, tx, sealed: Vec::new(), open: None, removed_since_rebuild: 0 };

    // Anything registered while we were not subscribed is covered by building from the cache.
    drain(changes);
    let built = match mode {
        Mode::Firehose => s.subscribe(None, "firehose").await.map(|b| s.sealed.push(b)),
        _ => s.build_targeted().await,
    };
    if let Err(reason) = built {
        s.close().await;
        return SessionEnd::Disconnected(reason);
    }
    // Whatever happened between the last session and this one is only visible to a sweep.
    rt.request_sweep(SweepTrigger::Now("ws_connect"));

    let mut pending_new: Vec<Address> = Vec::new();
    let mut coalesce_at: Option<Instant> = None;
    let mut housekeeping = tokio::time::interval(Duration::from_secs(5));
    let mut last_log = Instant::now();

    let end = loop {
        let coalesce = async {
            match coalesce_at {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => break SessionEnd::Cancelled,
            msg = rx.recv() => match msg {
                Some(BucketMsg::Log(log)) => {
                    last_log = Instant::now();
                    handle_log(rt, &log, "ws").await;
                }
                Some(BucketMsg::Lagged(n)) => {
                    metrics::counter!("gum_ws_lagged_total", "chain" => rt.spec.name).increment(n);
                    tracing::warn!(chain = rt.spec.name, dropped = n, error.kind = "ws_lagged", "WSS consumer lagged; notifications dropped, sweeping to recover");
                    rt.request_sweep(SweepTrigger::Now("ws_lagged"));
                }
                Some(BucketMsg::Closed) | None => break SessionEnd::Disconnected("stream_closed"),
            },
            change = changes.recv() => match change {
                Some(WatchChange::Added(addr)) if mode == Mode::Targeted => {
                    pending_new.push(addr);
                    coalesce_at.get_or_insert_with(|| Instant::now() + Duration::from_millis(cfg.ws_coalesce_ms));
                }
                Some(WatchChange::Added(_)) => {}
                Some(WatchChange::Removed) => s.removed_since_rebuild += 1,
                None => break SessionEnd::Cancelled,
            },
            _ = coalesce => {
                coalesce_at = None;
                let new = std::mem::take(&mut pending_new);
                if let Err(reason) = s.add_recipients(new).await {
                    break SessionEnd::Disconnected(reason);
                }
            }
            _ = housekeeping.tick() => {
                if rt.cache.is_empty() {
                    break SessionEnd::Parked;
                }
                if desired_mode(rt, Some(mode)) != mode {
                    break SessionEnd::ModeChanged;
                }
                if mode == Mode::Firehose && last_log.elapsed() > Duration::from_millis(cfg.ws_stall_ms) {
                    break SessionEnd::Disconnected("stalled");
                }
                // Retired recipients still cost a notification if they ever receive funds; compact once
                // they are the majority of what we are subscribed to.
                if mode == Mode::Targeted && s.removed_since_rebuild * 2 > s.subscribed().max(cfg.ws_bucket_size / 4)
                    && let Err(reason) = s.build_targeted().await {
                        break SessionEnd::Disconnected(reason);
                    }
            }
        }
    };
    s.close().await;
    end
}

/// Shared by every source: match, persist as pending, schedule the confirmation sweep.
async fn handle_log(rt: &Arc<ChainRuntime>, log: &Log, source: &'static str) {
    let chain = rt.spec.name;
    metrics::counter!("gum_logs_scanned_total", "chain" => chain, "source" => source).increment(1);
    let m = match match_log(&rt.spec, &rt.cache, log) {
        Ok(m) => m,
        Err(skip @ (Skip::Malformed | Skip::Incomplete)) => {
            let reason = if skip == Skip::Malformed { "malformed_transfer" } else { "missing_block_fields" };
            metrics::counter!("gum_logs_anomalous_total", "chain" => chain, "reason" => reason).increment(1);
            if let Some(suppressed) = rt.limiter.check(reason) {
                tracing::warn!(chain, error.kind = reason, token = %log.inner.address, tx = ?log.transaction_hash, topics = log.inner.data.topics().len(),
                    data_len = log.inner.data.data.len(), suppressed, "unexpected Transfer log shape from a registry token");
            }
            return;
        }
        Err(_) => return,
    };
    if m.removed {
        // The node tells us a block we saw was reorged out. Verify through the authoritative path.
        rt.request_sweep(SweepTrigger::Now("log_removed"));
        return;
    }
    match store::sweep::record_pending(&rt.pool, &rt.spec, &m).await {
        Ok(true) => {
            let token = rt.spec.tokens[m.token_idx as usize].symbol;
            metrics::counter!("gum_transfers_indexed_total", "chain" => chain, "token" => token, "phase" => "pending")
                .increment(1);
            if let Some(ts) = log.block_timestamp {
                let age = chrono::Utc::now().timestamp_millis() as f64 / 1000.0 - ts as f64;
                metrics::histogram!("gum_detection_latency_seconds", "chain" => chain, "phase" => "pending")
                    .record(age.max(0.0));
            }
            rt.dispatcher_wake.notify_one();
            rt.request_sweep(SweepTrigger::PendingAt { block: m.block_number });
        }
        Ok(false) => {}
        Err(e) => {
            // Not fatal: the sweeper confirms this transfer from the chain regardless.
            rt.request_sweep(SweepTrigger::PendingAt { block: m.block_number });
            if let Some(suppressed) = rt.limiter.check("record_pending_failed") {
                tracing::warn!(chain, error.kind = e.kind(), error = %e, suppressed, "could not persist pending transfer; the sweep will pick it up");
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Poll source
// ---------------------------------------------------------------------------------------------

struct Poller {
    rt: Arc<ChainRuntime>,
    /// Highest block already examined by the poller. `None` until the first tick after a (re)start.
    scanned: Option<u64>,
}

impl Poller {
    fn new(rt: Arc<ChainRuntime>) -> Self {
        Self { rt, scanned: None }
    }

    fn reset(&mut self) {
        self.scanned = None;
    }

    async fn tick(&mut self) {
        let rt = self.rt.clone();
        let from = match self.scanned {
            Some(b) => b + 1,
            // Start at the durable cursor so transfers that arrived during a WSS gap surface as pending at once.
            None => match store::cursor(&rt.pool, rt.spec.chain_id).await {
                Ok(c) => c + 1,
                Err(_) => return,
            },
        };
        // `latest` instead of a number saves the eth_blockNumber call that would otherwise pair with each poll.
        let filter = Filter::new()
            .address(rt.spec.log_addresses())
            .event_signature(TRANSFER_TOPIC)
            .from_block(from)
            .to_block(BlockNumberOrTag::Latest);
        match rt.rpc.get_logs(&filter).await {
            Ok(logs) => {
                let mut highest = from.saturating_sub(1);
                for log in &logs {
                    highest = highest.max(log.block_number.unwrap_or(0));
                    handle_log(&rt, log, "poll").await;
                }
                if !logs.is_empty() {
                    rt.health.observe_head(highest);
                }
                self.scanned = Some(highest);
            }
            Err(e) if e.class() == RpcErrorClass::RangeTooLarge => {
                // Too far behind for one call; the sweeper owns catch-up. Jump to the head.
                if let Ok(head) = rt.rpc.block_number().await {
                    self.scanned = Some(head.saturating_sub(1));
                }
            }
            // Node behind our cursor, or a failure already logged and counted by the RPC client.
            Err(_) => {}
        }
    }
}
