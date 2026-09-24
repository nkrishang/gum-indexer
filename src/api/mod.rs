//! HTTP API. There is no authentication by design: the service is only reachable on the private network
//! (keep `/metrics` on the
//! private network in production).

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy::primitives::{Address, U256};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer};
use uuid::Uuid;

use crate::{
    events::{TransferView, u256_dec},
    ingest::{ChainRuntime, SweepTrigger, health::HealthSnapshot, notify},
    registry::{ChainSpec, Registry},
    store::{self, CreateOutcome, NewWatch, StoreError, WatchRow},
    webhook::target,
};

#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    pub registry: Registry,
    pub chains: Arc<HashMap<u64, Arc<ChainRuntime>>>,
    pub default_ttl: Option<Duration>,
    /// Cap on how far back `payments_since` reaches.
    pub max_backfill: Duration,
    pub target_policy: target::TargetPolicy,
    pub metrics: PrometheusHandle,
}

pub fn router(state: ApiState) -> Router {
    let v1 = Router::new()
        .route("/watches", post(create_watch))
        .route("/watches/{id}", get(get_watch).delete(cancel_watch))
        .route("/chains", get(list_chains))
        .route("/stats", get(stats));
    Router::new()
        .nest("/v1", v1)
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(render_metrics))
        .layer(RequestBodyLimitLayer::new(16 * 1024))
        .layer(TimeoutLayer::with_status_code(StatusCode::GATEWAY_TIMEOUT, Duration::from_secs(15)))
        .with_state(state)
}

// ---------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into() }
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": { "code": self.code, "message": self.message } }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        tracing::error!(error.kind = e.kind(), error = %e, "API request failed on the database");
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable", "storage is temporarily unavailable; retry")
    }
}

// ---------------------------------------------------------------------------------------------
// Watches
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateWatchRequest {
    pub payment_address: String,
    /// Chain slug ("base") or chain id ("8453").
    #[serde(alias = "blockchain")]
    pub chain: String,
    /// Token symbol ("USDC") or contract address; must be in the chain's registry.
    #[serde(alias = "erc20_token")]
    pub token: String,
    /// Base units (USDC has 6 decimals: "2500000" = 2.5 USDC). String or integer.
    pub balance_threshold: serde_json::Value,
    pub webhook_endpoint: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    /// Payments may have reached the address since this time (e.g. it was handed out before this
    /// registration). Blocks since then that were already swept are scanned once for the new watch.
    /// Omitted: the watch counts transfers from the next block on. Capped at `watch.max_backfill_secs`.
    #[serde(default)]
    pub payments_since: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WatchResponse {
    pub id: Uuid,
    pub chain: String,
    pub chain_id: u64,
    pub token: String,
    pub token_address: Address,
    pub payment_address: Address,
    #[serde(with = "u256_dec")]
    pub balance_threshold: U256,
    #[serde(with = "u256_dec")]
    pub confirmed_amount: U256,
    pub status: String,
    pub webhook_endpoint: String,
    pub start_block: u64,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfers: Option<Vec<TransferView>>,
}

fn watch_response(chain: &ChainSpec, w: &WatchRow, transfers: Option<Vec<TransferView>>) -> WatchResponse {
    let view = w.view(chain);
    WatchResponse {
        id: w.id,
        chain: view.chain,
        chain_id: view.chain_id,
        token: view.token,
        token_address: w.token_address,
        payment_address: w.payment_address,
        balance_threshold: w.threshold,
        confirmed_amount: w.confirmed_amount,
        status: w.status.clone(),
        webhook_endpoint: w.webhook_url.clone(),
        start_block: w.start_block,
        created_at: w.created_at,
        expires_at: w.expires_at,
        completed_at: w.completed_at,
        transfers,
    }
}

fn parse_threshold(v: &serde_json::Value) -> Result<U256, ApiError> {
    let text = match v {
        serde_json::Value::String(s) => s.trim().to_owned(),
        serde_json::Value::Number(n) if n.is_u64() => n.to_string(),
        _ => return Err(ApiError::invalid("balance_threshold must be a base-unit integer (string or number)")),
    };
    let value = U256::from_str_radix(&text, 10).map_err(|_| {
        ApiError::invalid("balance_threshold must be a non-negative base-unit integer that fits uint256")
    })?;
    if value.is_zero() {
        return Err(ApiError::invalid("balance_threshold must be greater than zero"));
    }
    Ok(value)
}

async fn create_watch(
    State(state): State<ApiState>,
    Json(req): Json<CreateWatchRequest>,
) -> Result<Response, ApiError> {
    let chain = state.registry.chain(&req.chain).ok_or_else(|| {
        let known: Vec<_> = state.registry.chains().iter().map(|c| c.name).collect();
        ApiError::invalid(format!("unsupported chain {:?}; supported: {}", req.chain, known.join(", ")))
    })?;
    let token = chain.token(&req.token).ok_or_else(|| {
        let known: Vec<_> = chain.tokens.iter().map(|t| t.symbol).collect();
        ApiError::invalid(format!(
            "token {:?} is not supported on {}; supported: {}",
            req.token,
            chain.name,
            known.join(", ")
        ))
    })?;
    let payment_address: Address = req
        .payment_address
        .trim()
        .parse()
        .map_err(|_| ApiError::invalid("payment_address is not a valid EVM address"))?;
    if payment_address.is_zero() {
        return Err(ApiError::invalid("payment_address must not be the zero address"));
    }
    let threshold = parse_threshold(&req.balance_threshold)?;
    let url = target::validate(&req.webhook_endpoint, &state.target_policy)
        .map_err(|m| ApiError::invalid(format!("webhook_endpoint: {m}")))?;
    let expires_at = match req.expires_at {
        Some(at) if at <= Utc::now() => return Err(ApiError::invalid("expires_at is in the past")),
        Some(at) => Some(at),
        None => state.default_ttl.and_then(|ttl| chrono::Duration::from_std(ttl).ok()).map(|ttl| Utc::now() + ttl),
    };

    let rt = state.chains.get(&chain.chain_id).filter(|rt| rt.is_hydrated()).ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "chain_not_ready",
            format!("{} is still starting; retry", chain.name),
        )
    })?;

    let lookback_blocks = match req.payments_since {
        Some(since) => {
            let age = (Utc::now() - since).to_std().unwrap_or_default();
            if age > state.max_backfill {
                tracing::warn!(
                    chain = chain.name,
                    age_secs = age.as_secs(),
                    max_secs = state.max_backfill.as_secs(),
                    "payments_since is older than watch.max_backfill_secs; scanning back only that far"
                );
            }
            lookback_blocks(age.min(state.max_backfill), chain.cfg.expected_block_time_ms)
        }
        None => 0,
    };
    let new = NewWatch {
        chain_id: chain.chain_id,
        token_address: token.address,
        payment_address,
        threshold,
        webhook_url: url.to_string(),
        expires_at,
        head_estimate: rt.head_estimate(),
        lookback_blocks,
    };
    match store::create_watch(&state.pool, &new).await? {
        CreateOutcome::Created(w) => {
            if let Some((key, entry)) = w.cache_entry(chain) {
                rt.add_watch(key, entry);
            }
            if let Some(to) = w.backfill_to {
                tracing::info!(chain = chain.name, watch_id = %w.id, from = w.start_block, to, "watch registered late; blocks already swept will be scanned for it");
                rt.request_sweep(SweepTrigger::Now("backfill"));
            }
            notify::announce_added(&state.pool, chain.chain_id, w.id).await;
            metrics::counter!("gum_watches_created_total", "chain" => chain.name, "token" => token.symbol).increment(1);
            Ok((StatusCode::CREATED, Json(watch_response(chain, &w, None))).into_response())
        }
        CreateOutcome::Existing(w) => Ok((StatusCode::OK, Json(watch_response(chain, &w, None))).into_response()),
        CreateOutcome::Conflict(w) => Err(ApiError::new(
            StatusCode::CONFLICT,
            "watch_conflict",
            format!("an active watch ({}) already exists for this address and token with different parameters", w.id),
        )),
    }
}

/// Blocks produced in `age`, over-estimated (block times vary, and a missed block means a missed
/// payment while an extra one costs nothing) and never zero, so a payment in the head block counts.
pub fn lookback_blocks(age: Duration, expected_block_time_ms: u64) -> u64 {
    let blocks = (age.as_millis() as u64 * 5 / 4).div_ceil(expected_block_time_ms.max(1));
    blocks + 2
}

async fn load(state: &ApiState, id: Uuid) -> Result<(Arc<ChainSpec>, WatchRow), ApiError> {
    let watch = store::get_watch(&state.pool, id)
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such watch"))?;
    let chain = state.registry.chain_by_id(watch.chain_id).cloned().ok_or_else(|| {
        ApiError::new(StatusCode::NOT_FOUND, "not_found", "watch belongs to a chain that is no longer enabled")
    })?;
    Ok((chain, watch))
}

async fn get_watch(State(state): State<ApiState>, Path(id): Path<Uuid>) -> Result<Json<WatchResponse>, ApiError> {
    let (chain, watch) = load(&state, id).await?;
    let transfers = store::transfers_for_watch(&state.pool, id, 500).await?;
    Ok(Json(watch_response(&chain, &watch, Some(transfers.iter().map(|t| t.view()).collect()))))
}

async fn cancel_watch(State(state): State<ApiState>, Path(id): Path<Uuid>) -> Result<Json<WatchResponse>, ApiError> {
    let (chain, existing) = load(&state, id).await?;
    let watch = match store::cancel_watch(&state.pool, id).await? {
        Some(cancelled) => {
            if let (Some(rt), Some((key, _))) = (state.chains.get(&chain.chain_id), cancelled.cache_entry(&chain)) {
                rt.remove_watch(&key, id);
            }
            notify::announce_removed(&state.pool, chain.chain_id, id).await;
            cancelled
        }
        None => existing, // already retired: cancelling is idempotent
    };
    Ok(Json(watch_response(&chain, &watch, None)))
}

// ---------------------------------------------------------------------------------------------
// Chains, stats, health
// ---------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct ChainView {
    name: &'static str,
    chain_id: u64,
    confirmations: u64,
    tokens: Vec<TokenView>,
    active_watches: usize,
    ready: bool,
    health: HealthSnapshot,
}

#[derive(Serialize)]
struct TokenView {
    symbol: &'static str,
    address: Address,
    decimals: u8,
    issuance: String,
    /// Set when `Transfer` logs are indexed from an emitter other than `address` (Arc USDC).
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_log_address: Option<Address>,
}

fn chain_views(state: &ApiState) -> Vec<ChainView> {
    state
        .registry
        .chains()
        .iter()
        .filter_map(|c| {
            let rt = state.chains.get(&c.chain_id)?;
            Some(ChainView {
                name: c.name,
                chain_id: c.chain_id,
                confirmations: c.cfg.confirmations,
                tokens: c
                    .tokens
                    .iter()
                    .map(|t| TokenView {
                        symbol: t.symbol,
                        address: t.address,
                        decimals: t.decimals,
                        issuance: t.issuance.clone(),
                        transfer_log_address: (t.log_address != t.address).then_some(t.log_address),
                    })
                    .collect(),
                active_watches: rt.cache.len(),
                ready: rt.is_hydrated(),
                health: rt.health.snapshot(),
            })
        })
        .collect()
}

async fn list_chains(State(state): State<ApiState>) -> Json<serde_json::Value> {
    Json(json!({ "chains": chain_views(&state) }))
}

async fn stats(State(state): State<ApiState>) -> Result<Json<serde_json::Value>, ApiError> {
    let rows = store::stats(&state.pool).await?;
    let mut per_chain: Vec<serde_json::Value> = Vec::new();
    let (mut total_confirmed, mut total_pending, mut total_orphaned) = (0i64, 0i64, 0i64);
    for chain in state.registry.chains() {
        let tokens: Vec<_> = rows
            .iter()
            .filter(|r| r.chain_id == chain.chain_id)
            .map(|r| {
                json!({
                    "token": chain.token_by_address(&r.token_address).map(|t| t.symbol).unwrap_or("UNKNOWN"),
                    "token_address": r.token_address,
                    "payments_pending_now": r.pending_count,
                    "payments_confirmed": r.confirmed_count,
                    "payments_orphaned": r.orphaned_count,
                    "confirmed_volume": r.confirmed_volume.to_string(),
                    "thresholds_reached": r.thresholds_reached,
                    "active_watches": r.active_watches,
                })
            })
            .collect();
        let sum =
            |f: fn(&store::StatsRow) -> i64| rows.iter().filter(|r| r.chain_id == chain.chain_id).map(f).sum::<i64>();
        let (confirmed, pending, orphaned) =
            (sum(|r| r.confirmed_count), sum(|r| r.pending_count), sum(|r| r.orphaned_count));
        total_confirmed += confirmed;
        total_pending += pending;
        total_orphaned += orphaned;
        per_chain.push(json!({
            "chain": chain.name,
            "chain_id": chain.chain_id,
            "payments_confirmed": confirmed,
            "payments_pending_now": pending,
            "payments_orphaned": orphaned,
            "thresholds_reached": sum(|r| r.thresholds_reached),
            "tokens": tokens,
        }));
    }
    Ok(Json(json!({
        "payments_confirmed": total_confirmed,
        "payments_pending_now": total_pending,
        "payments_orphaned": total_orphaned,
        "chains": per_chain,
    })))
}

/// Liveness. Deliberately independent of chain and database health: restarting the process does not fix an
/// RPC outage, and a restart loop would only add downtime.
async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(state): State<ApiState>) -> Response {
    let db = sqlx::query("SELECT 1").execute(&state.pool).await.is_ok();
    let chains: HashMap<_, _> = state.chains.values().map(|rt| (rt.spec.name, rt.is_hydrated())).collect();
    let ready = db && chains.values().all(|r| *r);
    let code = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, Json(json!({ "ready": ready, "database": db, "chains": chains }))).into_response()
}

async fn render_metrics(State(state): State<ApiState>) -> String {
    // Refresh age-based gauges at scrape time.
    for rt in state.chains.values() {
        let _ = rt.health.snapshot();
    }
    state.metrics.render()
}

#[cfg(test)]
mod tests {
    #[test]
    fn lookback_over_estimates_blocks_and_is_never_zero() {
        use std::time::Duration;
        assert_eq!(super::lookback_blocks(Duration::ZERO, 300), 2);
        // 60 s at 300 ms blocks is 200 blocks; +25% and 2 more.
        assert_eq!(super::lookback_blocks(Duration::from_secs(60), 300), 252);
        assert_eq!(super::lookback_blocks(Duration::from_millis(100), 0), 127, "a zero block time is treated as 1 ms");
    }
}
