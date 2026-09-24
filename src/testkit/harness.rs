//! Runs the real application in-process against test chains.

use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use figment::providers::Format;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{api::WatchResponse, app::App, config::Config};

pub const WEBHOOK_SECRET: &str = "test-webhook-secret";

/// One chain of a test configuration. URLs may point at Anvil directly or at a fault proxy.
#[derive(Debug, Clone)]
pub struct ChainParams {
    pub name: String,
    pub chain_id: u64,
    pub http_url: String,
    pub ws_url: String,
    pub tokens: Vec<(String, Address)>,
    /// Per symbol: a separate `Transfer` log emitter and its decimals (see `TokenConfig::transfer_log_address`).
    pub log_sources: Vec<(String, Address, u8)>,
    pub ingest_mode: String,
    pub confirmations: u64,
    pub extra: String,
}

impl ChainParams {
    pub fn new(name: &str, chain_id: u64, http_url: String, ws_url: String, tokens: Vec<(String, Address)>) -> Self {
        Self {
            name: name.into(),
            chain_id,
            http_url,
            ws_url,
            tokens,
            log_sources: Vec::new(),
            ingest_mode: "ws_targeted".into(),
            confirmations: 2,
            extra: String::new(),
        }
    }

    pub fn mode(mut self, mode: &str) -> Self {
        self.ingest_mode = mode.into();
        self
    }

    /// Indexes `symbol` from `emitter`'s `Transfer` logs, whose amounts carry `decimals` decimals.
    pub fn log_source(mut self, symbol: &str, emitter: Address, decimals: u8) -> Self {
        self.log_sources.push((symbol.into(), emitter, decimals));
        self
    }

    pub fn confirmations(mut self, n: u64) -> Self {
        self.confirmations = n;
        self
    }

    /// Extra `key = value` lines for the chain table (e.g. `ws_bucket_size = 2`).
    pub fn extra(mut self, toml: &str) -> Self {
        self.extra.push_str(toml);
        self.extra.push('\n');
        self
    }

    fn to_toml(&self) -> String {
        let mut out = format!(
            r#"
[chains.{name}]
chain_id = {chain_id}
http_url = "{http}"
ws_url = "{ws}"
ingest_mode = "{mode}"
large_scale_mode = "poll"
confirmations = {conf}
expected_block_time_ms = 100
max_log_range = 2000
poll_interval_ms = 100
safety_sweep_interval_ms = 1000
idle_probe_interval_ms = 1000
head_stall_threshold_ms = 600000
ws_coalesce_ms = 50
ws_down_fallback_ms = 300
rpc_timeout_ms = 2000
rpc_max_rps = 1000
rpc_max_retries = 2
credits_per_call = 20
"#,
            name = self.name,
            chain_id = self.chain_id,
            http = self.http_url,
            ws = self.ws_url,
            mode = self.ingest_mode,
            conf = self.confirmations,
        );
        for (symbol, address) in &self.tokens {
            out.push_str(&format!(
                "[[chains.{}.tokens]]\nsymbol = \"{symbol}\"\naddress = \"{address}\"\ndecimals = 6\nissuance = \"mock\"\n",
                self.name
            ));
            if let Some((_, emitter, decimals)) = self.log_sources.iter().find(|(s, ..)| s == symbol) {
                out.push_str(&format!("transfer_log_address = \"{emitter}\"\ntransfer_log_decimals = {decimals}\n"));
            }
        }
        out
    }
}

pub fn test_config(chains: &[ChainParams]) -> Config {
    let mut toml = String::from(
        r#"
profile = "test"
[server]
bind = "127.0.0.1"
port = 0
[database]
url = "provided-as-pool"
max_connections = 20
[api]
keys = "test-key"
[watch]
default_ttl_secs = 0
[webhook]
secret = "test-webhook-secret"
connect_timeout_ms = 1000
request_timeout_ms = 2000
max_concurrency = 64
max_per_host = 8
retry_base_ms = 50
retry_cap_ms = 400
max_age_secs = 3600
host_failure_threshold = 1000
host_park_ms = 100
allow_insecure_targets = true
"#,
    );
    let mut overrides = String::new();
    for chain in chains {
        toml.push_str(&chain.to_toml());
        overrides.push_str(&format!("[chains.{}]\n{}\n", chain.name, chain.extra));
    }
    let cfg: Config = figment::Figment::new()
        .merge(figment::providers::Toml::string(&toml))
        .merge(figment::providers::Toml::string(&overrides))
        .extract()
        .expect("test config parses");
    cfg.validate().expect("test config is valid");
    cfg
}

pub struct TestApp {
    pub app: Option<App>,
    pub base_url: String,
    pub pool: PgPool,
    client: reqwest::Client,
}

impl TestApp {
    pub async fn start(cfg: Config, pool: PgPool) -> Self {
        crate::telemetry::init_logging(false);
        let app = App::start(cfg, pool.clone()).await.expect("app starts");
        let base_url = format!("http://{}", app.addr);
        let this = Self { app: Some(app), base_url, pool, client: reqwest::Client::new() };
        this.wait_ready(Duration::from_secs(20)).await;
        this
    }

    pub async fn wait_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(resp) = self.client.get(format!("{}/readyz", self.base_url)).send().await
                && resp.status().is_success()
            {
                return;
            }
            assert!(Instant::now() < deadline, "app did not become ready within {timeout:?}");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn create_watch_raw(&self, body: serde_json::Value) -> reqwest::Response {
        self.client.post(format!("{}/v1/watches", self.base_url)).json(&body).send().await.expect("POST /v1/watches")
    }

    pub async fn create_watch(
        &self,
        chain: &str,
        token: &str,
        address: Address,
        threshold: U256,
        hook: &str,
    ) -> WatchResponse {
        let resp = self
            .create_watch_raw(serde_json::json!({
                "payment_address": address,
                "chain": chain,
                "token": token,
                "balance_threshold": threshold.to_string(),
                "webhook_endpoint": hook,
            }))
            .await;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        assert!(status.is_success(), "create watch failed: {status} {text}");
        serde_json::from_str(&text).expect("watch response")
    }

    pub async fn get_watch(&self, id: Uuid) -> WatchResponse {
        self.get_json(&format!("/v1/watches/{id}")).await
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> T {
        let resp = self.client.get(format!("{}{path}", self.base_url)).send().await.expect("GET");
        assert!(resp.status().is_success(), "GET {path} → {}", resp.status());
        resp.json().await.expect("json body")
    }

    pub async fn metrics(&self) -> String {
        self.client
            .get(format!("{}/metrics", self.base_url))
            .send()
            .await
            .expect("GET /metrics")
            .text()
            .await
            .expect("metrics text")
    }

    /// Polls `/v1/chains` until `pred` holds for the named chain's health object.
    pub async fn wait_chain(
        &self,
        chain: &str,
        what: &str,
        timeout: Duration,
        pred: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let deadline = Instant::now() + timeout;
        loop {
            let body: serde_json::Value = self.get_json("/v1/chains").await;
            let found = body["chains"]
                .as_array()
                .and_then(|a| a.iter().find(|c| c["name"] == chain))
                .cloned()
                .unwrap_or_default();
            if pred(&found) {
                return found;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {chain}: {what}; last = {found}");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(app) = self.app.take() {
            app.shutdown().await;
        }
    }

    /// Simulates a crash: tasks are aborted without any cleanup.
    pub async fn kill(mut self) {
        if let Some(app) = self.app.take() {
            app.kill().await;
        }
    }
}

pub fn usdc(units: u64) -> U256 {
    U256::from(units) * U256::from(1_000_000u64)
}

pub fn fresh_address() -> Address {
    Address::from(*<&[u8; 20]>::try_from(&Uuid::new_v4().as_bytes().repeat(2)[..20]).expect("20 bytes"))
}
