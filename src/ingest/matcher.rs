//! Hot path: decide whether a raw log is an ERC-20 Transfer to a watched address.
//! No ABI decoding and no allocation for the overwhelmingly common "not ours" case.

use alloy::{
    primitives::{Address, B256, U256, b256},
    rpc::types::Log,
};

use crate::{
    cache::{WatchCache, WatchKey, WatchRef},
    registry::ChainSpec,
};

/// keccak256("Transfer(address,address,uint256)")
pub const TRANSFER_TOPIC: B256 = b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedTransfer {
    pub watch: WatchRef,
    pub token_idx: u8,
    pub from: Address,
    pub to: Address,
    pub amount: U256,
    pub block_number: u64,
    pub block_hash: B256,
    pub tx_hash: B256,
    pub log_index: u64,
    pub removed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skip {
    /// Not a token we index, or not a Transfer.
    Foreign,
    /// A registry token emitted a Transfer with an unexpected shape (topic count / data length).
    /// Worth surfacing: it would mean a token upgrade changed its event.
    Malformed,
    /// Log lacks block metadata (a pending-block log); never expected from our filters.
    Incomplete,
    /// Zero-value transfer (address-poisoning spam).
    ZeroValue,
    Unwatched,
    /// Watched, but the block predates the watch.
    BeforeStart,
}

#[inline]
pub fn match_log(chain: &ChainSpec, cache: &WatchCache, log: &Log) -> Result<MatchedTransfer, Skip> {
    let topics = log.inner.data.topics();
    if topics.first() != Some(&TRANSFER_TOPIC) {
        return Err(Skip::Foreign);
    }
    let token = chain.token_by_address(&log.inner.address).ok_or(Skip::Foreign)?;
    let data = log.inner.data.data.as_ref();
    if topics.len() != 3 || data.len() != 32 {
        return Err(Skip::Malformed);
    }
    let to = Address::from_word(topics[2]);
    let watch = cache.get(&WatchKey::new(token.idx, &to)).ok_or(Skip::Unwatched)?;

    let (Some(block_number), Some(block_hash), Some(tx_hash), Some(log_index)) =
        (log.block_number, log.block_hash, log.transaction_hash, log.log_index)
    else {
        return Err(Skip::Incomplete);
    };
    if block_number < watch.start_block {
        return Err(Skip::BeforeStart);
    }
    let amount = U256::from_be_slice(data);
    if amount.is_zero() {
        return Err(Skip::ZeroValue);
    }
    Ok(MatchedTransfer {
        watch,
        token_idx: token.idx,
        from: Address::from_word(topics[1]),
        to,
        amount,
        block_number,
        block_hash,
        tx_hash,
        log_index,
        removed: log.removed,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Bytes, LogData};
    use figment::providers::Format;
    use uuid::Uuid;

    use super::*;
    use crate::config::Config;

    pub(crate) fn test_chain() -> ChainSpec {
        let cfg: Config = figment::Figment::new()
            .merge(figment::providers::Toml::string(include_str!("../../config/default.toml")))
            .merge(figment::providers::Toml::string(
                "[database]\nurl=\"postgres://x\"\n[chains.base]\nhttp_url=\"http://b\"\nws_url=\"ws://b\"\n[chains.monad]\nenabled=false\n[chains.arbitrum]\nenabled=false",
            ))
            .extract()
            .unwrap();
        ChainSpec::new("base", cfg.chains["base"].clone())
    }

    pub(crate) fn transfer_log(
        token: Address,
        from: Address,
        to: Address,
        amount: U256,
        block: u64,
        index: u64,
    ) -> Log {
        let data = LogData::new_unchecked(
            vec![TRANSFER_TOPIC, from.into_word(), to.into_word()],
            Bytes::from(amount.to_be_bytes::<32>().to_vec()),
        );
        Log {
            inner: alloy::primitives::Log { address: token, data },
            block_hash: Some(B256::repeat_byte(block as u8)),
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: Some(B256::repeat_byte(0x77)),
            transaction_index: Some(0),
            log_index: Some(index),
            removed: false,
        }
    }

    fn setup() -> (ChainSpec, WatchCache, Address, Address) {
        let chain = test_chain();
        let cache = WatchCache::new();
        let usdc = chain.tokens[0].address;
        let payee = Address::repeat_byte(0x42);
        cache.insert(WatchKey::new(0, &payee), WatchRef { id: Uuid::new_v4(), seq: 1, start_block: 100 });
        (chain, cache, usdc, payee)
    }

    #[test]
    fn matches_transfer_to_watched_address() {
        let (chain, cache, usdc, payee) = setup();
        let log = transfer_log(usdc, Address::repeat_byte(1), payee, U256::from(2_500_000u64), 100, 3);
        let m = match_log(&chain, &cache, &log).unwrap();
        assert_eq!((m.to, m.amount, m.block_number, m.log_index), (payee, U256::from(2_500_000u64), 100, 3));
        assert_eq!(m.from, Address::repeat_byte(1));
    }

    #[test]
    fn mint_from_zero_address_counts() {
        let (chain, cache, usdc, payee) = setup();
        let log = transfer_log(usdc, Address::ZERO, payee, U256::from(1u64), 101, 0);
        assert!(match_log(&chain, &cache, &log).is_ok());
    }

    #[test]
    fn skips_are_classified() {
        let (chain, cache, usdc, payee) = setup();
        let other = Address::repeat_byte(9);
        let one = U256::from(1u64);
        assert_eq!(match_log(&chain, &cache, &transfer_log(usdc, other, other, one, 100, 0)), Err(Skip::Unwatched));
        assert_eq!(match_log(&chain, &cache, &transfer_log(other, other, payee, one, 100, 0)), Err(Skip::Foreign));
        assert_eq!(match_log(&chain, &cache, &transfer_log(usdc, other, payee, one, 99, 0)), Err(Skip::BeforeStart));
        assert_eq!(
            match_log(&chain, &cache, &transfer_log(usdc, other, payee, U256::ZERO, 100, 0)),
            Err(Skip::ZeroValue),
            "zero-value transfers are address-poisoning spam"
        );

        let mut incomplete = transfer_log(usdc, other, payee, one, 100, 0);
        incomplete.block_hash = None;
        assert_eq!(match_log(&chain, &cache, &incomplete), Err(Skip::Incomplete));

        // ERC-721 style Transfer (tokenId indexed → 4 topics, empty data) from a registry token address.
        let mut malformed = transfer_log(usdc, other, payee, one, 100, 0);
        malformed.inner.data = LogData::new_unchecked(
            vec![TRANSFER_TOPIC, other.into_word(), payee.into_word(), B256::ZERO],
            Bytes::new(),
        );
        assert_eq!(match_log(&chain, &cache, &malformed), Err(Skip::Malformed));
    }
}
