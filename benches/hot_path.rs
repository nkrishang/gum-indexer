//! Micro-benchmarks for the per-log hot path and the watch cache.
//! `cargo bench --all-features` (add `-- --quick` for a fast pass).

use std::hint::black_box;

use alloy::{
    primitives::{Address, U256},
    rpc::types::Log,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gum_indexer::{
    cache::{WatchCache, WatchKey, WatchRef},
    ingest::matcher::match_log,
    testkit::logs::{base_chain, transfer_log},
};
use uuid::Uuid;

fn address(i: u64) -> Address {
    let mut raw = [0u8; 20];
    raw[..8].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
    raw[12..].copy_from_slice(&i.to_be_bytes());
    Address::from(raw)
}

fn filled_cache(n: u64) -> WatchCache {
    let cache = WatchCache::with_capacity(n as usize);
    for i in 0..n {
        cache.insert(WatchKey::new(0, &address(i)), WatchRef { id: Uuid::nil(), seq: i as i64, start_block: 0 });
    }
    cache
}

/// A block's worth of token transfers where 1 in 1,000 goes to a watched address (realistic for a firehose
/// or a catch-up sweep; the targeted WSS mode never sees the other 999).
fn logs(n: u64, watched: u64) -> Vec<Log> {
    let chain = base_chain();
    let token = chain.tokens[0].address;
    (0..n)
        .map(|i| {
            let to = if i % 1000 == 0 { address(i % watched) } else { address(u64::MAX - i) };
            transfer_log(token, address(i + 7), to, U256::from(1_000_000u64), 100 + i / 200, i % 200)
        })
        .collect()
}

fn bench_match(c: &mut Criterion) {
    let chain = base_chain();
    let mut group = c.benchmark_group("match_log");
    for watches in [1_000u64, 100_000, 1_000_000] {
        let cache = filled_cache(watches);
        let batch = logs(10_000, watches);
        group.throughput(Throughput::Elements(batch.len() as u64));
        group.bench_with_input(BenchmarkId::new("10k_logs_vs_watches", watches), &watches, |b, _| {
            b.iter(|| {
                let mut hits = 0u32;
                for log in &batch {
                    hits += match_log(&chain, &cache, black_box(log)).is_ok() as u32;
                }
                black_box(hits)
            })
        });
    }
    group.finish();
}

fn bench_maps(c: &mut Criterion) {
    let n = 1_000_000u64;
    let keys: Vec<WatchKey> = (0..10_000).map(|i| WatchKey::new(0, &address(i * 97 % n))).collect();
    let mut group = c.benchmark_group("lookup_1m_entries");
    group.throughput(Throughput::Elements(keys.len() as u64));

    let papaya = filled_cache(n);
    group.bench_function("papaya+foldhash (WatchCache)", |b| {
        b.iter(|| keys.iter().filter(|k| papaya.get(black_box(k)).is_some()).count())
    });

    let dash: dashmap::DashMap<WatchKey, WatchRef, foldhash::fast::RandomState> =
        dashmap::DashMap::with_capacity_and_hasher(n as usize, Default::default());
    for i in 0..n {
        dash.insert(WatchKey::new(0, &address(i)), WatchRef { id: Uuid::nil(), seq: i as i64, start_block: 0 });
    }
    group.bench_function("dashmap+foldhash", |b| {
        b.iter(|| keys.iter().filter(|k| dash.contains_key(black_box(*k))).count())
    });

    let mut std_map: std::collections::HashMap<WatchKey, WatchRef> =
        std::collections::HashMap::with_capacity(n as usize);
    for i in 0..n {
        std_map.insert(WatchKey::new(0, &address(i)), WatchRef { id: Uuid::nil(), seq: i as i64, start_block: 0 });
    }
    group.bench_function("std HashMap+SipHash (single-threaded baseline)", |b| {
        b.iter(|| keys.iter().filter(|k| std_map.contains_key(black_box(*k))).count())
    });
    group.finish();
}

fn bench_cache_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache");
    group.sample_size(10);
    group.bench_function("hydrate_100k", |b| b.iter(|| black_box(filled_cache(100_000)).len()));
    group.bench_function("register_into_1m", |b| {
        let cache = filled_cache(1_000_000);
        let mut next = 2_000_000u64;
        b.iter_batched(
            || {
                next += 1;
                next
            },
            |i| {
                cache.insert(WatchKey::new(0, &address(i)), WatchRef { id: Uuid::nil(), seq: i as i64, start_block: 0 })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    // What an eth_getLogs response costs to deserialize: relevant for sweeps over busy tokens.
    let batch = logs(10_000, 1_000);
    let json = serde_json::to_vec(&batch).unwrap();
    let mut group = c.benchmark_group("decode");
    group.throughput(Throughput::Bytes(json.len() as u64));
    group.bench_function("eth_getLogs_10k_logs_json", |b| {
        b.iter(|| serde_json::from_slice::<Vec<Log>>(black_box(&json)).unwrap().len())
    });
    group.finish();
}

criterion_group!(benches, bench_match, bench_maps, bench_cache_writes, bench_decode);
criterion_main!(benches);
