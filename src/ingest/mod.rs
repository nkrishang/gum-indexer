//! Per-chain ingestion: WSS push detects, `eth_getLogs` from a durable cursor guarantees.
//!
//! Each chain is an isolated, supervised unit. Only the instance holding the chain's Postgres advisory lock
//! ingests; any instance can serve the API. An outage on one chain never affects the others.

pub mod health;
pub mod matcher;
pub mod notify;
pub mod source;
pub mod sweeper;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use alloy::primitives::Address;
use sqlx::{ConnectOptions, Connection, PgPool};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use uuid::Uuid;

use self::health::ChainHealth;
use crate::{
    cache::{WatchCache, WatchKey, WatchRef},
    registry::ChainSpec,
    rpc::{RpcClient, RpcFailure},
    store::{self, StoreError},
    telemetry::LogLimiter,
};

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Rpc(#[from] RpcFailure),
    #[error("chain id mismatch: config says {expected}, endpoint reports {actual}")]
    ChainIdMismatch { expected: u64, actual: u64 },
    #[error("token {symbol} ({address}): config says {expected} decimals, contract reports {actual:?}")]
    TokenDecimalsMismatch { symbol: &'static str, address: Address, expected: u8, actual: Option<u8> },
    #[error("lost the leadership lock connection: {0}")]
    LeadershipLost(String),
}

impl IngestError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Store(e) => e.kind(),
            Self::Rpc(e) => e.kind(),
            Self::ChainIdMismatch { .. } => "config_chain_id_mismatch",
            Self::TokenDecimalsMismatch { .. } => "config_token_decimals_mismatch",
            Self::LeadershipLost(_) => "leadership_lost",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SweepTrigger {
    /// Sweep as soon as possible (`reason` is a metrics label).
    Now(&'static str),
    /// A transfer was seen at head in `block`; sweep once it reaches confirmation depth.
    PendingAt { block: u64 },
}

#[derive(Debug, Clone, Copy)]
pub enum WatchChange {
    Added(Address),
    Removed,
}

/// Shared handle to one chain: used by the API (registration), the notify listener and the chain's own tasks.
pub struct ChainRuntime {
    pub spec: Arc<ChainSpec>,
    pub cache: Arc<WatchCache>,
    pub health: Arc<ChainHealth>,
    pub rpc: Arc<RpcClient>,
    pub(crate) pool: PgPool,
    pub(crate) dispatcher_wake: Arc<Notify>,
    pub(crate) limiter: LogLimiter,
    sweep_tx: mpsc::UnboundedSender<SweepTrigger>,
    sweep_rx: Mutex<mpsc::UnboundedReceiver<SweepTrigger>>,
    watch_tx: mpsc::UnboundedSender<WatchChange>,
    watch_rx: Mutex<mpsc::UnboundedReceiver<WatchChange>>,
    hydrated: AtomicBool,
}

impl ChainRuntime {
    pub fn new(spec: Arc<ChainSpec>, pool: PgPool, dispatcher_wake: Arc<Notify>) -> Result<Arc<Self>, IngestError> {
        let health = Arc::new(ChainHealth::new(spec.name, Duration::from_millis(spec.cfg.head_stall_threshold_ms)));
        let rpc = Arc::new(RpcClient::new(&spec, health.clone())?);
        let (sweep_tx, sweep_rx) = mpsc::unbounded_channel();
        let (watch_tx, watch_rx) = mpsc::unbounded_channel();
        Ok(Arc::new(Self {
            spec,
            cache: Arc::new(WatchCache::new()),
            health,
            rpc,
            pool,
            dispatcher_wake,
            limiter: LogLimiter::new(Duration::from_secs(30)),
            sweep_tx,
            sweep_rx: Mutex::new(sweep_rx),
            watch_tx,
            watch_rx: Mutex::new(watch_rx),
            hydrated: AtomicBool::new(false),
        }))
    }

    /// Makes a committed watch visible to the hot path. Idempotent.
    pub fn add_watch(&self, key: WatchKey, watch: WatchRef) {
        if self.cache.insert(key, watch) {
            let _ = self.watch_tx.send(WatchChange::Added(Address::from(key.address)));
            self.publish_watch_gauge();
        }
    }

    pub fn remove_watch(&self, key: &WatchKey, id: Uuid) {
        if self.cache.remove(key, id) {
            let _ = self.watch_tx.send(WatchChange::Removed);
            self.publish_watch_gauge();
        }
    }

    pub(crate) fn note_cache_changed(&self, added: &[WatchKey], removed: usize) {
        for key in added {
            let _ = self.watch_tx.send(WatchChange::Added(Address::from(key.address)));
        }
        for _ in 0..removed {
            let _ = self.watch_tx.send(WatchChange::Removed);
        }
        if !added.is_empty() || removed > 0 {
            self.publish_watch_gauge();
        }
    }

    fn publish_watch_gauge(&self) {
        metrics::gauge!("gum_watches_active", "chain" => self.spec.name).set(self.cache.len() as f64);
    }

    pub fn request_sweep(&self, trigger: SweepTrigger) {
        let _ = self.sweep_tx.send(trigger);
    }

    /// Latest head this process has seen. May be stale; registration only uses it as a lower bound.
    pub fn head_estimate(&self) -> u64 {
        self.health.head()
    }

    pub fn is_hydrated(&self) -> bool {
        self.hydrated.load(Ordering::Acquire)
    }

    /// Verifies the endpoint really is this chain and the registry matches the contracts, creates the cursor
    /// row on first sight and loads the active watchlist into memory.
    pub async fn bootstrap(&self) -> Result<(), IngestError> {
        let actual = self.rpc.chain_id().await?;
        if actual != self.spec.chain_id {
            return Err(IngestError::ChainIdMismatch { expected: self.spec.chain_id, actual });
        }
        for token in &self.spec.tokens {
            let actual = self.rpc.token_decimals(token.address).await?;
            if actual != Some(token.decimals) {
                return Err(IngestError::TokenDecimalsMismatch {
                    symbol: token.symbol,
                    address: token.address,
                    expected: token.decimals,
                    actual,
                });
            }
        }
        let head = self.rpc.block_number().await?;
        let cursor = store::ensure_cursor(&self.pool, self.spec.chain_id, head).await?;
        let started = std::time::Instant::now();
        let loaded = store::hydrate(&self.pool, &self.spec, |k, w| {
            self.cache.insert(k, w);
        })
        .await?;
        self.hydrated.store(true, Ordering::Release);
        self.publish_watch_gauge();
        self.health.observe_sweep(cursor);
        tracing::info!(
            chain = self.spec.name,
            chain_id = self.spec.chain_id,
            head,
            cursor,
            watches = loaded,
            hydrate_ms = started.elapsed().as_millis() as u64,
            tokens = self.spec.tokens.len(),
            "chain ready"
        );
        Ok(())
    }
}

/// Stable per-chain advisory lock key ("GUM" namespace in the high bits).
fn leadership_key(chain_id: u64) -> i64 {
    ((0x47_55_4Di64) << 40) ^ (chain_id as i64 & 0xFF_FFFF_FFFF)
}

/// Supervises one chain forever: bootstrap, win leadership, ingest; on any failure back off and start over.
pub async fn run_chain(rt: Arc<ChainRuntime>, cancel: CancellationToken) {
    let chain = rt.spec.name;
    let mut backoff = Duration::from_millis(500);
    while !cancel.is_cancelled() {
        if !rt.is_hydrated() {
            if let Err(e) = rt.bootstrap().await {
                // Misconfiguration must be loud; an unreachable RPC at boot is "just" an outage.
                let fatal =
                    matches!(e, IngestError::ChainIdMismatch { .. } | IngestError::TokenDecimalsMismatch { .. });
                if fatal {
                    tracing::error!(chain, error.kind = e.kind(), error = %e, "chain misconfigured; it will not ingest until fixed");
                } else if let Some(suppressed) = rt.limiter.check("bootstrap") {
                    tracing::warn!(chain, error.kind = e.kind(), error = %e, suppressed, "chain bootstrap failed; retrying");
                }
                sleep_or_cancel(backoff.max(if fatal { Duration::from_secs(30) } else { backoff }), &cancel).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
            backoff = Duration::from_millis(500);
        }

        // Abort-on-drop everywhere: if a supervisor is torn down its children must not outlive it.
        let task = AbortOnDropHandle::new(tokio::spawn(lead(rt.clone(), cancel.clone())));
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                metrics::counter!("gum_task_restarts_total", "chain" => chain, "task" => "ingest").increment(1);
                if let Some(suppressed) = rt.limiter.check(e.kind()) {
                    tracing::warn!(chain, error.kind = e.kind(), error = %e, suppressed, "ingest stopped; restarting");
                }
            }
            Err(join) => {
                metrics::counter!("gum_task_restarts_total", "chain" => chain, "task" => "ingest").increment(1);
                tracing::error!(chain, error.kind = "task_panic", error = %join, "ingest task panicked; restarting");
            }
        }
        rt.health.set_leader(false);
        sleep_or_cancel(backoff, &cancel).await;
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}

/// Holds the chain's advisory lock on a dedicated connection and runs sweeper + source while it is held.
async fn lead(rt: Arc<ChainRuntime>, cancel: CancellationToken) -> Result<(), IngestError> {
    let chain = rt.spec.name;
    let key = leadership_key(rt.spec.chain_id);
    // A dedicated (non-pooled) connection: the session-level lock must die with this task. A pooled connection
    // would go back to the pool still holding the lock if the task were aborted or panicked, wedging the chain.
    let mut conn = rt.pool.connect_options().connect().await.map_err(StoreError::from)?;
    loop {
        let (got,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut conn)
            .await
            .map_err(StoreError::from)?;
        if got {
            break;
        }
        // Another instance leads (deploy overlap). Stay warm and keep trying.
        if sleep_or_cancel(Duration::from_secs(2), &cancel).await {
            return Ok(());
        }
    }
    rt.health.set_leader(true);
    tracing::info!(chain, "leadership acquired");

    let work = CancellationToken::new();
    let mut sweeper = AbortOnDropHandle::new(tokio::spawn(sweeper::run(rt.clone(), work.clone())));
    let mut source = AbortOnDropHandle::new(tokio::spawn(source::run(rt.clone(), work.clone())));
    let mut keepalive = tokio::time::interval(Duration::from_secs(5));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let result: Result<(), IngestError> = loop {
        tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            _ = keepalive.tick() => {
                // The lock lives and dies with this connection; if it is gone, so is our leadership.
                if let Err(e) = sqlx::query("SELECT 1").execute(&mut conn).await {
                    break Err(IngestError::LeadershipLost(e.to_string()));
                }
            }
            r = &mut sweeper => break flatten("sweeper", r),
            r = &mut source => break flatten("source", r),
        }
    };

    // Let in-flight work (a sweep transaction) finish before the lock is released.
    work.cancel();
    if !sweeper.is_finished() {
        let _ = tokio::time::timeout(Duration::from_secs(20), &mut sweeper).await;
    }
    if !source.is_finished() {
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut source).await;
    }
    sweeper.abort();
    source.abort();
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)").bind(key).execute(&mut conn).await;
    let _ = conn.close().await;
    rt.health.set_leader(false);
    rt.health.set_ws(false, false);
    tracing::info!(chain, "leadership released");
    result
}

fn flatten(task: &'static str, r: Result<Result<(), IngestError>, tokio::task::JoinError>) -> Result<(), IngestError> {
    match r {
        Ok(inner) => inner,
        Err(join) => {
            tracing::error!(task, error.kind = "task_panic", error = %join, "ingest subtask panicked");
            Err(IngestError::LeadershipLost(format!("{task} panicked")))
        }
    }
}

/// Returns true when cancelled.
pub(crate) async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(d) => false,
    }
}
