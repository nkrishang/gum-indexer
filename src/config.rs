//! Layered configuration: `config/default.toml` → `config/<profile>.toml` → generated local overrides → env.

use std::{collections::BTreeMap, path::Path, time::Duration};

use alloy::primitives::Address;
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Load(#[from] Box<figment::Error>),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_profile")]
    pub profile: String,
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub api: ApiConfig,
    pub watch: WatchConfig,
    pub webhook: WebhookConfig,
    #[serde(default)]
    pub quicknode: QuicknodeConfig,
    pub chains: BTreeMap<String, ChainConfig>,
}

fn default_profile() -> String {
    "mainnet".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ApiConfig {
    /// Comma-separated bearer keys. Empty means the API refuses every authenticated route.
    #[serde(default)]
    pub keys: String,
}

impl ApiConfig {
    pub fn key_list(&self) -> Vec<String> {
        self.keys.split(',').map(str::trim).filter(|k| !k.is_empty()).map(String::from).collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WatchConfig {
    pub default_ttl_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    #[serde(default)]
    pub secret: String,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_concurrency: usize,
    pub retry_base_ms: u64,
    pub retry_cap_ms: u64,
    pub max_age_secs: u64,
    pub host_failure_threshold: u32,
    pub host_park_ms: u64,
    pub allow_insecure_targets: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct QuicknodeConfig {
    /// Admin API key; when set, credits used/remaining are exported as gauges.
    #[serde(default)]
    pub api_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestMode {
    Auto,
    WsTargeted,
    WsFirehose,
    Poll,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    #[serde(default = "yes")]
    pub enabled: bool,
    pub chain_id: u64,
    #[serde(default)]
    pub http_url: String,
    #[serde(default)]
    pub ws_url: String,
    pub ingest_mode: IngestMode,
    /// Mode used by `auto` above `ws_targeted_max` active watches: `ws_firehose` or `poll`.
    pub large_scale_mode: IngestMode,
    #[serde(default = "d_bucket")]
    pub ws_bucket_size: usize,
    #[serde(default = "d_targeted_max")]
    pub ws_targeted_max: usize,
    /// New watches are coalesced for this long before the open bucket is resubscribed.
    #[serde(default = "d_coalesce")]
    pub ws_coalesce_ms: u64,
    /// WSS down for longer than this → poll until it is back.
    #[serde(default = "d_ws_fallback")]
    pub ws_down_fallback_ms: u64,
    #[serde(default = "d_channel")]
    pub ws_channel_size: usize,
    /// Firehose only: no notification for this long on a busy token means the subscription is stale.
    #[serde(default = "d_ws_stall")]
    pub ws_stall_ms: u64,
    pub poll_interval_ms: u64,
    pub safety_sweep_interval_ms: u64,
    pub idle_probe_interval_ms: u64,
    pub confirmations: u64,
    pub expected_block_time_ms: u64,
    pub max_log_range: u64,
    pub head_stall_threshold_ms: u64,
    #[serde(default = "d_rpc_timeout")]
    pub rpc_timeout_ms: u64,
    #[serde(default = "d_rpc_rps")]
    pub rpc_max_rps: u32,
    #[serde(default = "d_rpc_retries")]
    pub rpc_max_retries: usize,
    pub credits_per_call: u32,
    #[serde(default)]
    pub tokens: Vec<TokenConfig>,
}

fn yes() -> bool {
    true
}
fn d_bucket() -> usize {
    500
}
fn d_targeted_max() -> usize {
    5_000
}
fn d_coalesce() -> u64 {
    500
}
fn d_ws_fallback() -> u64 {
    5_000
}
fn d_channel() -> usize {
    16_384
}
fn d_ws_stall() -> u64 {
    60_000
}
fn d_rpc_timeout() -> u64 {
    10_000
}
fn d_rpc_rps() -> u32 {
    25
}
fn d_rpc_retries() -> usize {
    4
}

impl ChainConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
    pub fn block_time(&self) -> Duration {
        Duration::from_millis(self.expected_block_time_ms.max(1))
    }
    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_millis(self.rpc_timeout_ms)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenConfig {
    pub symbol: String,
    pub address: Address,
    pub decimals: u8,
    /// Informational: native | usdt0 | bridged | mock.
    #[serde(default)]
    pub issuance: String,
}

impl Config {
    /// Loads from `dir` (normally `./config`). Profile comes from `GUM_PROFILE` (default `mainnet`).
    pub fn load(dir: &Path) -> Result<Self, ConfigError> {
        let profile = std::env::var("GUM_PROFILE").unwrap_or_else(|_| default_profile());
        let mut fig = Figment::new().merge(Toml::file(dir.join("default.toml")));
        if profile != "mainnet" {
            fig = fig
                .merge(Toml::file(dir.join(format!("{profile}.toml"))))
                .merge(Toml::file(dir.join(format!("{profile}.generated.toml"))));
        }
        let fig = fig.merge(Env::prefixed("GUM_").split("__"));
        let mut cfg: Config = fig.extract().map_err(Box::new)?;
        cfg.profile = profile;
        if let Ok(url) = std::env::var("DATABASE_URL") {
            cfg.database.url = url;
        }
        if let Ok(port) = std::env::var("PORT") {
            cfg.server.port =
                port.parse().map_err(|_| ConfigError::Invalid(format!("PORT is not a port number: {port}")))?;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_toml_str(toml: &str) -> Result<Self, ConfigError> {
        let cfg: Config = Figment::new().merge(Toml::string(toml)).extract().map_err(Box::new)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn enabled_chains(&self) -> impl Iterator<Item = (&String, &ChainConfig)> {
        self.chains.iter().filter(|(_, c)| c.enabled)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        if self.database.url.is_empty() {
            return bad("DATABASE_URL is not set".into());
        }
        if self.enabled_chains().next().is_none() {
            return bad("no chain is enabled".into());
        }
        let mut seen_ids = BTreeMap::new();
        for (name, c) in self.enabled_chains() {
            if let Some(other) = seen_ids.insert(c.chain_id, name) {
                return bad(format!("chains {other} and {name} share chain_id {}", c.chain_id));
            }
            if c.http_url.is_empty() {
                return bad(format!(
                    "chain {name}: http_url is empty (set GUM_CHAINS__{}__HTTP_URL or disable the chain)",
                    name.to_uppercase()
                ));
            }
            let needs_ws = c.ingest_mode != IngestMode::Poll;
            if needs_ws && c.ws_url.is_empty() {
                return bad(format!("chain {name}: ingest_mode needs ws_url (or set ingest_mode = \"poll\")"));
            }
            if !matches!(c.large_scale_mode, IngestMode::WsFirehose | IngestMode::Poll) {
                return bad(format!("chain {name}: large_scale_mode must be ws_firehose or poll"));
            }
            if c.tokens.is_empty() {
                return bad(format!("chain {name}: no tokens configured"));
            }
            if c.tokens.len() > u8::MAX as usize {
                return bad(format!("chain {name}: too many tokens"));
            }
            if c.max_log_range == 0 || c.ws_bucket_size == 0 || c.poll_interval_ms == 0 {
                return bad(format!("chain {name}: max_log_range, ws_bucket_size and poll_interval_ms must be > 0"));
            }
            let mut syms = std::collections::BTreeSet::new();
            let mut addrs = std::collections::BTreeSet::new();
            for t in &c.tokens {
                if !syms.insert(t.symbol.to_uppercase()) || !addrs.insert(t.address) {
                    return bad(format!("chain {name}: duplicate token {}", t.symbol));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_with(extra: &str) -> Result<Config, ConfigError> {
        let base = include_str!("../config/default.toml");
        let fig = Figment::new().merge(Toml::string(base)).merge(Toml::string(extra));
        let cfg: Config = fig.extract().map_err(Box::new)?;
        cfg.validate()?;
        Ok(cfg)
    }

    const URLS: &str = r#"
        [database]
        url = "postgres://x"
        [chains.monad]
        http_url = "http://m"
        ws_url = "ws://m"
        [chains.arbitrum]
        http_url = "http://a"
        ws_url = "ws://a"
        [chains.base]
        http_url = "http://b"
        ws_url = "ws://b"
    "#;

    #[test]
    fn default_config_is_valid_once_urls_are_set() {
        let cfg = default_with(URLS).unwrap();
        assert_eq!(cfg.chains["monad"].chain_id, 143);
        assert_eq!(cfg.chains["monad"].tokens.len(), 3);
        assert_eq!(cfg.chains["arbitrum"].tokens.len(), 2);
        // USDT is deliberately not offered on Base.
        assert_eq!(cfg.chains["base"].tokens.iter().map(|t| t.symbol.as_str()).collect::<Vec<_>>(), ["USDC"]);
        assert_eq!(cfg.chains["base"].ws_bucket_size, 500);
    }

    #[test]
    fn missing_rpc_url_is_rejected_with_the_env_var_name() {
        let err = default_with("[database]\nurl = \"postgres://x\"").unwrap_err().to_string();
        assert!(err.contains("GUM_CHAINS__"), "{err}");
    }

    #[test]
    fn disabled_chain_needs_no_url() {
        let base = include_str!("../config/default.toml");
        let cfg: Config = Figment::new()
            .merge(Toml::string(base))
            .merge(Toml::string(URLS))
            .merge(Toml::string("[chains.monad]\nenabled = false\nhttp_url = \"\""))
            .extract()
            .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.enabled_chains().count(), 2);
    }

    #[test]
    fn local_profile_layers_over_default() {
        let base = include_str!("../config/default.toml");
        let local = include_str!("../config/local.toml");
        let cfg: Config = Figment::new()
            .merge(Toml::string(base))
            .merge(Toml::string(local))
            .merge(Toml::string("[database]\nurl = \"postgres://x\""))
            .extract()
            .unwrap();
        cfg.validate().unwrap();
        assert!(cfg.webhook.allow_insecure_targets);
        assert_eq!(cfg.chains["base"].http_url, "http://127.0.0.1:18547");
    }
}
