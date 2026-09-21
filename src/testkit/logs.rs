//! Synthetic logs and chain specs for tests and micro-benchmarks.

use alloy::{
    primitives::{Address, B256, Bytes, LogData, U256, keccak256},
    rpc::types::Log,
};
use figment::providers::Format;

use crate::{config::Config, ingest::matcher::TRANSFER_TOPIC, registry::ChainSpec};

/// The mainnet Base chain spec (USDC only) with placeholder URLs.
pub fn base_chain() -> ChainSpec {
    let cfg: Config = figment::Figment::new()
        .merge(figment::providers::Toml::string(include_str!("../../config/default.toml")))
        .merge(figment::providers::Toml::string(
            "[database]\nurl=\"postgres://x\"\n[chains.base]\nhttp_url=\"http://b\"\nws_url=\"ws://b\"",
        ))
        .extract()
        .expect("default config parses");
    ChainSpec::new("base", cfg.chains["base"].clone())
}

/// Deterministic fake block hash; `fork` distinguishes competing blocks at the same height.
pub fn block_hash(number: u64, fork: u8) -> B256 {
    let mut seed = [0u8; 9];
    seed[..8].copy_from_slice(&number.to_be_bytes());
    seed[8] = fork;
    keccak256(seed)
}

pub fn transfer_log(token: Address, from: Address, to: Address, amount: U256, block: u64, log_index: u64) -> Log {
    transfer_log_on_fork(token, from, to, amount, block, log_index, 0)
}

pub fn transfer_log_on_fork(
    token: Address,
    from: Address,
    to: Address,
    amount: U256,
    block: u64,
    log_index: u64,
    fork: u8,
) -> Log {
    let data = LogData::new_unchecked(
        vec![TRANSFER_TOPIC, from.into_word(), to.into_word()],
        Bytes::from(amount.to_be_bytes::<32>().to_vec()),
    );
    let mut tx_seed = [0u8; 17];
    tx_seed[..8].copy_from_slice(&block.to_be_bytes());
    tx_seed[8..16].copy_from_slice(&log_index.to_be_bytes());
    tx_seed[16] = fork;
    Log {
        inner: alloy::primitives::Log { address: token, data },
        block_hash: Some(block_hash(block, fork)),
        block_number: Some(block),
        block_timestamp: None,
        transaction_hash: Some(keccak256(tx_seed)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}
