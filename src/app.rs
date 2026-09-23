//! Wires everything together. Used by `main` and, in-process, by the test and benchmark harness.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use sqlx::PgPool;
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    api::{self, ApiState},
    config::Config,
    ingest::{self, ChainRuntime},
    registry::Registry,
    store, telemetry,
    webhook::Dispatcher,
};

pub struct App {
    pub addr: SocketAddr,
    pub chains: Arc<HashMap<u64, Arc<ChainRuntime>>>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    server: JoinHandle<()>,
}

impl App {
    /// Starts every component and returns once the HTTP listener is bound. Chains bootstrap in the background:
    /// an unreachable RPC at boot delays that chain, not the process.
    pub async fn start(cfg: Config, pool: PgPool) -> anyhow::Result<Self> {
        let metrics = telemetry::install_metrics();
        store::migrate(&pool).await?;

        let cancel = CancellationToken::new();
        let registry = Registry::from_config(&cfg);
        let wake = Arc::new(Notify::new());
        let mut tasks = Vec::new();

        let mut chains = HashMap::new();
        for spec in registry.chains() {
            let rt = ChainRuntime::new(spec.clone(), pool.clone(), wake.clone())?;
            chains.insert(spec.chain_id, rt.clone());
            tasks.push(tokio::spawn(ingest::run_chain(rt, cancel.clone())));
        }
        let chains = Arc::new(chains);

        tasks.push(tokio::spawn(ingest::notify::run(pool.clone(), (*chains).clone(), cancel.clone())));
        let dispatcher = Dispatcher::new(pool.clone(), cfg.webhook.clone(), wake.clone())?;
        tasks.push(tokio::spawn(dispatcher.run(cancel.clone())));
        telemetry::spawn_upkeep(metrics.clone(), cancel.clone());
        telemetry::quicknode_usage::spawn(cfg.quicknode.api_key.clone(), cancel.clone());

        if cfg.webhook.secret.is_empty() {
            tracing::warn!(
                error.kind = "config_no_webhook_secret",
                "GUM_WEBHOOK__SECRET is empty: webhook signatures are not meaningful"
            );
        }
        let state = ApiState {
            pool: pool.clone(),
            registry,
            chains: chains.clone(),
            default_ttl: (cfg.watch.default_ttl_secs > 0).then(|| Duration::from_secs(cfg.watch.default_ttl_secs)),
            max_backfill: Duration::from_secs(cfg.watch.max_backfill_secs),
            target_policy: cfg.webhook.target_policy(),
            metrics,
        };

        // `::` accepts IPv4 and IPv6; Railway's private network may be IPv6-only.
        let bind = if cfg.server.bind.contains(':') {
            format!("[{}]:{}", cfg.server.bind, cfg.server.port)
        } else {
            format!("{}:{}", cfg.server.bind, cfg.server.port)
        };
        let listener = TcpListener::bind(&bind).await?;
        let addr = listener.local_addr()?;
        let shutdown = cancel.clone();
        let server = tokio::spawn(async move {
            let served = axum::serve(listener, api::router(state))
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await;
            if let Err(e) = served {
                tracing::error!(error.kind = "http_server", error = %e, "HTTP server stopped unexpectedly");
            }
        });
        tracing::info!(%addr, profile = cfg.profile, chains = chains.len(), "gum-indexer started");
        Ok(Self { addr, chains, cancel, tasks, server })
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Graceful stop: no new HTTP requests, in-flight sweep transactions and webhook posts finish, advisory
    /// locks are released so the next instance takes over immediately.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let all = async {
            for t in self.tasks {
                let _ = t.await;
            }
            let _ = self.server.await;
        };
        if tokio::time::timeout(Duration::from_secs(25), all).await.is_err() {
            tracing::warn!(
                error.kind = "shutdown_timeout",
                "graceful shutdown timed out; exiting anyway (state is durable)"
            );
        }
    }

    /// Hard stop without any cleanup, to simulate a crash in tests.
    pub async fn kill(self) {
        for t in &self.tasks {
            t.abort();
        }
        self.server.abort();
        for t in self.tasks {
            let _ = t.await;
        }
        self.cancel.cancel();
    }
}
