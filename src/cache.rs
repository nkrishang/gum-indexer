//! In-memory watchlist, hydrated from Postgres at boot. Exists purely to answer "is this recipient watched?"
//! on the per-log hot path without touching the database. Amounts are never cached: accounting is done
//! atomically in SQL, and only for the (rare) logs that match.

use std::sync::atomic::{AtomicI64, Ordering};

use alloy::primitives::Address;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WatchKey {
    pub token_idx: u8,
    pub address: [u8; 20],
}

impl WatchKey {
    #[inline]
    pub fn new(token_idx: u8, address: &Address) -> Self {
        Self { token_idx, address: address.0.0 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchRef {
    pub id: Uuid,
    pub seq: i64,
    pub start_block: u64,
}

/// Lock-free concurrent map (papaya): readers never block and hold no guards across `.await`.
/// Addresses are uniformly distributed already, so a fast seeded hasher is sufficient.
pub struct WatchCache {
    map: papaya::HashMap<WatchKey, WatchRef, foldhash::fast::RandomState>,
    /// Highest `watches.seq` known to this cache; sweeps delta-load everything above it.
    last_seq: AtomicI64,
}

impl Default for WatchCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchCache {
    pub fn new() -> Self {
        Self { map: papaya::HashMap::with_hasher(foldhash::fast::RandomState::default()), last_seq: AtomicI64::new(0) }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: papaya::HashMap::with_capacity_and_hasher(capacity, foldhash::fast::RandomState::default()),
            last_seq: AtomicI64::new(0),
        }
    }

    /// Returns true when the key was not present before.
    pub fn insert(&self, key: WatchKey, watch: WatchRef) -> bool {
        self.last_seq.fetch_max(watch.seq, Ordering::AcqRel);
        self.map.pin().insert(key, watch).is_none()
    }

    #[inline]
    pub fn get(&self, key: &WatchKey) -> Option<WatchRef> {
        self.map.pin().get(key).copied()
    }

    /// Removes the entry only if it still belongs to `id`; a newer watch on the same target is left alone.
    pub fn remove(&self, key: &WatchKey, id: Uuid) -> bool {
        matches!(self.map.pin().remove_if(key, |_, w| w.id == id), Ok(Some(_)))
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn last_seq(&self) -> i64 {
        self.last_seq.load(Ordering::Acquire)
    }

    pub fn note_seq(&self, seq: i64) {
        self.last_seq.fetch_max(seq, Ordering::AcqRel);
    }

    /// Snapshot of watched recipient addresses (deduplicated across tokens is not needed: topic filters OR them).
    pub fn addresses(&self) -> Vec<Address> {
        self.map.pin().iter().map(|(k, _)| Address::from(k.address)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(seq: i64) -> WatchRef {
        WatchRef { id: Uuid::new_v4(), seq, start_block: 10 }
    }

    #[test]
    fn insert_get_remove_roundtrip() {
        let c = WatchCache::new();
        let k = WatchKey::new(1, &Address::repeat_byte(0xab));
        let a = w(7);
        assert!(c.insert(k, a));
        assert_eq!(c.get(&k), Some(a));
        assert_eq!(c.get(&WatchKey::new(0, &Address::repeat_byte(0xab))), None, "token index is part of the key");
        assert_eq!(c.last_seq(), 7);
        assert!(c.remove(&k, a.id));
        assert!(c.is_empty());
    }

    #[test]
    fn remove_does_not_evict_a_newer_watch_on_the_same_target() {
        let c = WatchCache::new();
        let k = WatchKey::new(0, &Address::repeat_byte(1));
        let old = w(1);
        let new = w(2);
        c.insert(k, old);
        c.insert(k, new);
        assert!(!c.remove(&k, old.id));
        assert_eq!(c.get(&k), Some(new));
    }
}
