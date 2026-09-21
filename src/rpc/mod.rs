//! Instrumented RPC client: every call is throttled, timed out, classified, retried when it makes sense,
//! counted (requests, latency, estimated credits) and fed into the chain's health state machine.

pub mod errors;

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, Bytes, bytes},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::{Filter, Log, TransactionRequest},
    transports::TransportError,
};
use backon::BackoffBuilder;
use tokio::sync::Mutex;

use self::errors::{RpcErrorClass, classify};
use crate::{ingest::health::ChainHealth, registry::ChainSpec, telemetry::LogLimiter};

#[derive(Debug, thiserror::Error)]
pub enum RpcFailure {
    #[error("{method}: {class:?}: {message}")]
    Rpc { method: &'static str, class: RpcErrorClass, message: String },
    #[error("{method}: timed out after {after:?}")]
    Timeout { method: &'static str, after: Duration },
    #[error("invalid RPC url for chain {chain}: {message}")]
    BadUrl { chain: &'static str, message: String },
}

impl RpcFailure {
    pub fn class(&self) -> RpcErrorClass {
        match self {
            Self::Rpc { class, .. } => *class,
            Self::Timeout { .. } => RpcErrorClass::Transient,
            Self::BadUrl { .. } => RpcErrorClass::Invalid,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Timeout { .. } => "rpc_timeout",
            Self::BadUrl { .. } => "rpc_bad_url",
            Self::Rpc { class, .. } => class.kind(),
        }
    }
}

/// Spaces calls so we stay under the plan's requests-per-second limit instead of discovering it via 429s.
#[derive(Debug)]
struct Throttle {
    gap: Duration,
    next_slot: Mutex<Instant>,
}

impl Throttle {
    fn new(max_rps: u32) -> Self {
        Self { gap: Duration::from_secs(1) / max_rps.max(1), next_slot: Mutex::new(Instant::now()) }
    }

    async fn acquire(&self) {
        let wait_until = {
            let mut slot = self.next_slot.lock().await;
            let at = (*slot).max(Instant::now());
            *slot = at + self.gap;
            at
        };
        tokio::time::sleep_until(wait_until.into()).await;
    }
}

pub struct RpcClient {
    chain: &'static str,
    provider: DynProvider,
    health: Arc<ChainHealth>,
    credits_per_call: u32,
    timeout: Duration,
    max_retries: usize,
    throttle: Throttle,
    limiter: LogLimiter,
}

impl RpcClient {
    pub fn new(spec: &ChainSpec, health: Arc<ChainHealth>) -> Result<Self, RpcFailure> {
        let url =
            spec.cfg.http_url.parse().map_err(|e| RpcFailure::BadUrl { chain: spec.name, message: format!("{e}") })?;
        let provider = ProviderBuilder::new().disable_recommended_fillers().connect_http(url).erased();
        Ok(Self {
            chain: spec.name,
            provider,
            health,
            credits_per_call: spec.cfg.credits_per_call,
            timeout: spec.cfg.rpc_timeout(),
            max_retries: spec.cfg.rpc_max_retries,
            throttle: Throttle::new(spec.cfg.rpc_max_rps),
            limiter: LogLimiter::new(Duration::from_secs(30)),
        })
    }

    pub async fn block_number(&self) -> Result<u64, RpcFailure> {
        let head = self.run("eth_blockNumber", || self.provider.get_block_number()).await?;
        self.health.observe_head(head);
        Ok(head)
    }

    pub async fn chain_id(&self) -> Result<u64, RpcFailure> {
        self.run("eth_chainId", || self.provider.get_chain_id()).await
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcFailure> {
        self.run("eth_getLogs", || self.provider.get_logs(filter)).await
    }

    /// `decimals()` of an ERC-20, used once at startup to validate the registry against the chain.
    pub async fn token_decimals(&self, token: Address) -> Result<Option<u8>, RpcFailure> {
        const DECIMALS_SELECTOR: Bytes = bytes!("313ce567");
        let req = TransactionRequest::default().to(token).input(DECIMALS_SELECTOR.into());
        let out = self.run("eth_call", || async { self.provider.call(req.clone()).await }).await?;
        Ok((out.len() == 32 && out[..31].iter().all(|b| *b == 0)).then(|| out[31]))
    }

    async fn run<T, F, Fut>(&self, method: &'static str, call: F) -> Result<T, RpcFailure>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, TransportError>>,
    {
        let mut backoff = backon::ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(10))
            .with_jitter()
            .with_max_times(self.max_retries)
            .build();
        loop {
            self.throttle.acquire().await;
            let started = Instant::now();
            let result = tokio::time::timeout(self.timeout, call()).await;
            let elapsed = started.elapsed();
            metrics::histogram!("gum_rpc_latency_seconds", "chain" => self.chain, "method" => method)
                .record(elapsed.as_secs_f64());

            let failure = match result {
                Ok(Ok(value)) => {
                    self.count(method, "ok", true);
                    self.health.record_rpc_ok();
                    return Ok(value);
                }
                Ok(Err(e)) => RpcFailure::Rpc { method, class: classify(&e), message: e.to_string() },
                Err(_) => RpcFailure::Timeout { method, after: self.timeout },
            };
            let class = failure.class();
            // QuickNode bills valid responses, including JSON-RPC errors; transport failures are free.
            let billed = matches!(failure, RpcFailure::Rpc { .. }) && !matches!(class, RpcErrorClass::Auth);
            self.count(method, failure.kind(), billed);

            match class {
                // Expected control-flow signals, not health problems: the caller adapts.
                RpcErrorClass::RangeTooLarge | RpcErrorClass::AheadOfHead => {
                    self.health.record_rpc_ok();
                    return Err(failure);
                }
                RpcErrorClass::Auth | RpcErrorClass::Invalid => {
                    self.health.record_rpc_err(failure.kind(), &failure.to_string());
                    if let Some(suppressed) = self.limiter.check(failure.kind()) {
                        tracing::error!(
                            chain = self.chain, method, error.kind = failure.kind(), error = %failure, suppressed,
                            "RPC call rejected; retrying cannot fix this (check endpoint URL, token and allow-lists)"
                        );
                    }
                    return Err(failure);
                }
                RpcErrorClass::RateLimited | RpcErrorClass::RateLimitedLong | RpcErrorClass::Transient => {
                    self.health.record_rpc_err(failure.kind(), &failure.to_string());
                    let Some(mut delay) = backoff.next() else {
                        if let Some(suppressed) = self.limiter.check(failure.kind()) {
                            tracing::warn!(
                                chain = self.chain, method, error.kind = failure.kind(), error = %failure, suppressed,
                                retries = self.max_retries, "RPC call failed after retries"
                            );
                        }
                        return Err(failure);
                    };
                    if class == RpcErrorClass::RateLimitedLong {
                        delay = Duration::from_secs(60);
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    fn count(&self, method: &'static str, outcome: &'static str, billed: bool) {
        metrics::counter!("gum_rpc_requests_total", "chain" => self.chain, "method" => method, "outcome" => outcome)
            .increment(1);
        if billed {
            metrics::counter!("gum_rpc_credits_estimated_total", "chain" => self.chain, "method" => method)
                .increment(self.credits_per_call as u64);
        }
    }
}
