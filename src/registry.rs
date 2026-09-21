//! Immutable view of the configured chains and tokens, resolved once at startup.

use std::sync::Arc;

use alloy::primitives::Address;

use crate::config::{ChainConfig, Config};

#[derive(Debug)]
pub struct TokenSpec {
    /// Position in the chain's token list; part of the cache key.
    pub idx: u8,
    /// `'static` so it can be used as a metrics label without allocating on the hot path.
    pub symbol: &'static str,
    pub address: Address,
    pub decimals: u8,
    pub issuance: String,
}

#[derive(Debug)]
pub struct ChainSpec {
    pub name: &'static str,
    pub chain_id: u64,
    pub cfg: ChainConfig,
    pub tokens: Vec<TokenSpec>,
}

impl ChainSpec {
    pub fn new(name: &str, cfg: ChainConfig) -> Self {
        let tokens = cfg
            .tokens
            .iter()
            .enumerate()
            .map(|(i, t)| TokenSpec {
                idx: i as u8,
                symbol: leak(&t.symbol.to_uppercase()),
                address: t.address,
                decimals: t.decimals,
                issuance: t.issuance.clone(),
            })
            .collect();
        Self { name: leak(name), chain_id: cfg.chain_id, cfg, tokens }
    }

    /// Resolves a token by symbol (case-insensitive) or by contract address.
    /// Tokens are never resolved through on-chain `symbol()`: bridged variants reuse canonical symbols.
    pub fn token(&self, symbol_or_address: &str) -> Option<&TokenSpec> {
        let s = symbol_or_address.trim();
        if let Ok(addr) = s.parse::<Address>() {
            return self.token_by_address(&addr);
        }
        self.tokens.iter().find(|t| t.symbol.eq_ignore_ascii_case(s))
    }

    #[inline]
    pub fn token_by_address(&self, address: &Address) -> Option<&TokenSpec> {
        // A chain has a handful of tokens; a linear scan beats hashing.
        self.tokens.iter().find(|t| t.address == *address)
    }

    pub fn token_addresses(&self) -> Vec<Address> {
        self.tokens.iter().map(|t| t.address).collect()
    }
}

#[derive(Debug, Clone)]
pub struct Registry {
    chains: Arc<Vec<Arc<ChainSpec>>>,
}

impl Registry {
    pub fn from_config(cfg: &Config) -> Self {
        let chains = cfg.enabled_chains().map(|(n, c)| Arc::new(ChainSpec::new(n, c.clone()))).collect();
        Self { chains: Arc::new(chains) }
    }

    pub fn chains(&self) -> &[Arc<ChainSpec>] {
        &self.chains
    }

    /// Resolves by slug ("base") or numeric chain id ("8453").
    pub fn chain(&self, name_or_id: &str) -> Option<&Arc<ChainSpec>> {
        let s = name_or_id.trim();
        let id = s.parse::<u64>().ok();
        self.chains.iter().find(|c| c.name.eq_ignore_ascii_case(s) || Some(c.chain_id) == id)
    }

    pub fn chain_by_id(&self, chain_id: u64) -> Option<&Arc<ChainSpec>> {
        self.chains.iter().find(|c| c.chain_id == chain_id)
    }
}

fn leak(s: &str) -> &'static str {
    Box::leak(s.to_owned().into_boxed_str())
}
