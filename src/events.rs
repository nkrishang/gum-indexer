//! Webhook event types and payloads. Amounts are base-unit decimal strings (uint256 does not fit JSON numbers).

use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Transfer seen at chain head. Not yet counted toward the threshold.
    #[serde(rename = "payment.pending")]
    PaymentPending,
    /// Transfer is canonical at the chain's confirmation depth and has been counted.
    #[serde(rename = "payment.confirmed")]
    PaymentConfirmed,
    /// A previously announced pending transfer was reorged out; it was never counted.
    #[serde(rename = "payment.orphaned")]
    PaymentOrphaned,
    /// Confirmed total crossed the threshold; the watch is retired.
    #[serde(rename = "threshold.reached")]
    ThresholdReached,
    /// The watch reached its expiry without crossing the threshold; it is retired.
    #[serde(rename = "watch.expired")]
    WatchExpired,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PaymentPending => "payment.pending",
            Self::PaymentConfirmed => "payment.confirmed",
            Self::PaymentOrphaned => "payment.orphaned",
            Self::ThresholdReached => "threshold.reached",
            Self::WatchExpired => "watch.expired",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchView {
    pub id: Uuid,
    pub chain: String,
    pub chain_id: u64,
    pub token: String,
    pub token_address: Address,
    pub payment_address: Address,
    #[serde(with = "u256_dec")]
    pub balance_threshold: U256,
    /// Confirmed total at the time the event was created.
    #[serde(with = "u256_dec")]
    pub confirmed_amount: U256,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferView {
    pub tx_hash: B256,
    pub log_index: u64,
    pub block_number: u64,
    pub block_hash: B256,
    pub from: Address,
    #[serde(with = "u256_dec")]
    pub amount: U256,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    /// Unique per event; deliveries are at-least-once, so consumers dedupe on it.
    pub id: Uuid,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub created_at: DateTime<Utc>,
    /// Monotonic per watch. Events of one watch are delivered in this order.
    pub sequence: i64,
    pub watch: WatchView,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub transfer: Option<TransferView>,
}

pub mod u256_dec {
    use alloy::primitives::U256;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(v: &U256, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<U256, D::Error> {
        let s = String::deserialize(d)?;
        U256::from_str_radix(&s, 10).map_err(D::Error::custom)
    }
}
