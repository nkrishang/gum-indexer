//! Database-level guarantees: idempotent accounting, exactly-once threshold crossing, orphan handling,
//! webhook ordering and the registration-vs-sweep race. Needs Postgres (`docker compose up -d postgres`).

use std::{sync::Arc, time::Duration};

use alloy::primitives::{Address, U256};
use gum_indexer::{
    cache::WatchCache,
    events::EventPayload,
    ingest::matcher::match_log,
    registry::ChainSpec,
    store::{
        self, CreateOutcome, NewWatch, WatchRow,
        outbox::{self, OutboxItem},
        sweep::{self, SweepOutcome, SweepResult},
    },
    testkit::logs::{base_chain, transfer_log, transfer_log_on_fork},
};
use sqlx::PgPool;

const HOOK: &str = "https://merchant.example/hooks/gum";

fn usdc(n: u64) -> U256 {
    U256::from(n * 1_000_000)
}

fn payer() -> Address {
    Address::repeat_byte(0xee)
}

struct Fixture {
    pool: PgPool,
    chain: ChainSpec,
    cache: WatchCache,
    token: Address,
}

impl Fixture {
    async fn new(pool: PgPool, cursor: u64) -> Self {
        let chain = base_chain();
        store::ensure_cursor(&pool, chain.chain_id, cursor).await.unwrap();
        let token = chain.tokens[0].address;
        Self { pool, chain, cache: WatchCache::new(), token }
    }

    async fn watch(&self, address: Address, threshold: U256, head: u64) -> WatchRow {
        let new = NewWatch {
            chain_id: self.chain.chain_id,
            token_address: self.token,
            payment_address: address,
            threshold,
            webhook_url: HOOK.into(),
            expires_at: None,
            head_estimate: head,
        };
        match store::create_watch(&self.pool, &new).await.unwrap() {
            CreateOutcome::Created(w) => {
                let (k, r) = w.cache_entry(&self.chain).unwrap();
                self.cache.insert(k, r);
                w
            }
            other => panic!("expected Created, got {other:?}"),
        }
    }

    async fn sweep(&self, to: u64, logs: &[alloy::rpc::types::Log]) -> SweepOutcome {
        let cursor = store::cursor(&self.pool, self.chain.chain_id).await.unwrap();
        match sweep::apply_sweep(&self.pool, &self.chain, &self.cache, cursor, to, logs).await.unwrap() {
            SweepResult::Applied(o) => o,
            SweepResult::CursorMoved { .. } => panic!("cursor moved unexpectedly"),
        }
    }

    async fn pending(&self, log: &alloy::rpc::types::Log) -> bool {
        let m = match_log(&self.chain, &self.cache, log).expect("log should match a watch");
        sweep::record_pending(&self.pool, &self.chain, &m).await.unwrap()
    }

    async fn events(&self) -> Vec<EventPayload> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT payload FROM webhook_outbox ORDER BY created_at, seq")
                .fetch_all(&self.pool)
                .await
                .unwrap();
        rows.into_iter().map(|(p,)| serde_json::from_value(p).unwrap()).collect()
    }

    async fn event_types(&self) -> Vec<&'static str> {
        self.events().await.iter().map(|e| e.event_type.as_str()).collect()
    }
}

#[sqlx::test]
async fn registration_is_idempotent_and_starts_after_cursor_and_head(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);

    let w = f.watch(addr, usdc(10), 140).await;
    assert_eq!(w.start_block, 141, "head estimate ahead of the cursor wins");
    assert_eq!(w.status, "active");

    let again = NewWatch {
        chain_id: f.chain.chain_id,
        token_address: f.token,
        payment_address: addr,
        threshold: usdc(10),
        webhook_url: HOOK.into(),
        expires_at: None,
        head_estimate: 150,
    };
    assert!(matches!(store::create_watch(&f.pool, &again).await.unwrap(), CreateOutcome::Existing(e) if e.id == w.id));

    let different = NewWatch { threshold: usdc(11), ..again.clone() };
    assert!(
        matches!(store::create_watch(&f.pool, &different).await.unwrap(), CreateOutcome::Conflict(e) if e.id == w.id)
    );

    // A stale head estimate can never place start_block at or below the cursor.
    let stale = f.watch(Address::repeat_byte(2), usdc(1), 3).await;
    assert_eq!(stale.start_block, 101);
}

#[sqlx::test]
async fn pending_then_confirmed_counts_exactly_once(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    let w = f.watch(addr, usdc(100), 100).await;
    let log = transfer_log(f.token, payer(), addr, usdc(30), 105, 0);

    assert!(f.pending(&log).await);
    assert!(!f.pending(&log).await, "a replayed notification is a no-op");
    let mid = store::get_watch(&f.pool, w.id).await.unwrap().unwrap();
    assert_eq!(mid.confirmed_amount, U256::ZERO, "pending amounts never count");

    let out = f.sweep(110, std::slice::from_ref(&log)).await;
    assert_eq!((out.confirmed.len(), out.confirmed_without_pending, out.orphaned), (1, 0, 0));

    // The same logs swept again (e.g. crash after commit, replay from an overlapping range) change nothing.
    let out = f.sweep(111, std::slice::from_ref(&log)).await;
    assert_eq!(out.confirmed.len(), 0);

    let after = store::get_watch(&f.pool, w.id).await.unwrap().unwrap();
    assert_eq!(after.confirmed_amount, usdc(30));
    assert_eq!(after.status, "active");
    assert_eq!(f.event_types().await, ["payment.pending", "payment.confirmed"]);

    let events = f.events().await;
    assert_eq!(events.iter().map(|e| e.sequence).collect::<Vec<_>>(), [1, 2], "sequence is gap-free per watch");
    assert_eq!(events[1].watch.confirmed_amount, usdc(30));
    assert_eq!(events[1].transfer.as_ref().unwrap().amount, usdc(30));
}

#[sqlx::test]
async fn stale_cursor_is_rejected_without_writing(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    let w = f.watch(addr, usdc(100), 100).await;
    let log = transfer_log(f.token, payer(), addr, usdc(5), 105, 0);
    f.sweep(110, std::slice::from_ref(&log)).await;

    // A second leader that planned from the old cursor must not apply.
    let res = sweep::apply_sweep(&f.pool, &f.chain, &f.cache, 100, 110, &[log]).await.unwrap();
    assert!(matches!(res, SweepResult::CursorMoved { expected: 100, actual: 110 }));
    assert_eq!(store::get_watch(&f.pool, w.id).await.unwrap().unwrap().confirmed_amount, usdc(5));
}

#[sqlx::test]
async fn transfer_missed_by_the_push_path_is_still_confirmed(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    let w = f.watch(addr, usdc(100), 100).await;
    let out = f.sweep(110, &[transfer_log(f.token, payer(), addr, usdc(7), 103, 4)]).await;
    assert_eq!((out.confirmed.len(), out.confirmed_without_pending), (1, 1));
    assert_eq!(store::get_watch(&f.pool, w.id).await.unwrap().unwrap().confirmed_amount, usdc(7));
    assert_eq!(f.event_types().await, ["payment.confirmed"]);
}

#[sqlx::test]
async fn threshold_crossing_retires_the_watch_exactly_once(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    let w = f.watch(addr, usdc(50), 100).await;

    let logs = [
        transfer_log(f.token, payer(), addr, usdc(20), 101, 0),
        transfer_log(f.token, payer(), addr, usdc(30), 102, 0), // crosses: 50 >= 50
        transfer_log(f.token, payer(), addr, usdc(99), 103, 0), // after retirement: not counted
    ];
    assert!(f.pending(&logs[2]).await);
    let out = f.sweep(110, &logs).await;
    assert_eq!(out.confirmed.len(), 2);
    assert_eq!(out.completed.len(), 1);
    assert_eq!(out.ignored, 1, "a canonical transfer after retirement is ignored, not orphaned");

    let after = store::get_watch(&f.pool, w.id).await.unwrap().unwrap();
    assert_eq!(after.status, "completed");
    assert_eq!(after.confirmed_amount, usdc(50));
    assert!(after.completed_at.is_some());
    assert!(f.cache.is_empty(), "retired watch is evicted from the cache");
    assert_eq!(
        f.event_types().await,
        ["payment.pending", "payment.confirmed", "payment.confirmed", "threshold.reached"]
    );

    // The address can be watched again; the old watch's history is untouched.
    let again = f.watch(addr, usdc(5), 110).await;
    assert_ne!(again.id, w.id);
    let stats = store::stats(&f.pool).await.unwrap();
    assert_eq!((stats[0].confirmed_count, stats[0].thresholds_reached, stats[0].active_watches), (2, 1, 1));
    assert_eq!(stats[0].confirmed_volume, usdc(50));
}

#[sqlx::test]
async fn reorged_out_pending_transfer_is_orphaned_and_replacement_is_counted(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    let w = f.watch(addr, usdc(100), 100).await;

    // Seen at head in block 105 (fork 0) …
    assert!(f.pending(&transfer_log_on_fork(f.token, payer(), addr, usdc(10), 105, 2, 0)).await);
    // … but the canonical chain at depth has the transaction re-included in a different block 106 (fork 1).
    let canonical = transfer_log_on_fork(f.token, payer(), addr, usdc(10), 106, 0, 1);
    let out = f.sweep(110, &[canonical]).await;
    assert_eq!((out.orphaned, out.confirmed.len(), out.confirmed_without_pending), (1, 1, 1));

    let after = store::get_watch(&f.pool, w.id).await.unwrap().unwrap();
    assert_eq!(after.confirmed_amount, usdc(10), "counted once, from the canonical block only");
    let mut types = f.event_types().await;
    types.sort_unstable();
    assert_eq!(types, ["payment.confirmed", "payment.orphaned", "payment.pending"]);

    let transfers = store::transfers_for_watch(&f.pool, w.id, 10).await.unwrap();
    assert_eq!(transfers.iter().map(|t| t.status.as_str()).collect::<Vec<_>>(), ["orphaned", "confirmed"]);
}

#[sqlx::test]
async fn late_pending_below_the_cursor_is_resolved_by_the_next_sweep(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    f.watch(addr, usdc(100), 100).await;
    f.sweep(110, &[]).await;
    // A delayed notification from an abandoned block at a height the cursor has already passed.
    assert!(f.pending(&transfer_log_on_fork(f.token, payer(), addr, usdc(1), 104, 0, 9)).await);
    let out = f.sweep(120, &[]).await;
    assert_eq!(out.orphaned, 1);
    assert!(store::pending_blocks(&f.pool, f.chain.chain_id).await.unwrap().is_empty());
}

#[sqlx::test]
async fn sweep_discovers_watches_this_instance_never_cached(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let addr = Address::repeat_byte(1);
    let w = f.watch(addr, usdc(100), 100).await;

    // Simulate the sweeping instance not having received the registration (created on another instance).
    let other_instance_cache = WatchCache::new();
    let log = transfer_log(f.token, payer(), addr, usdc(3), 104, 0);
    let res = sweep::apply_sweep(&f.pool, &f.chain, &other_instance_cache, 100, 110, &[log]).await.unwrap();
    let SweepResult::Applied(out) = res else { panic!("cursor moved") };
    assert_eq!(out.discovered.len(), 1);
    assert_eq!(out.confirmed.len(), 1);
    assert_eq!(other_instance_cache.len(), 1);
    assert_eq!(store::get_watch(&f.pool, w.id).await.unwrap().unwrap().confirmed_amount, usdc(3));
}

#[sqlx::test]
async fn transfers_before_start_block_and_to_cancelled_watches_do_not_count(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let early = Address::repeat_byte(1);
    let cancelled = Address::repeat_byte(2);
    let w1 = f.watch(early, usdc(100), 105).await; // start_block 106
    let w2 = f.watch(cancelled, usdc(100), 100).await;
    store::cancel_watch(&f.pool, w2.id).await.unwrap().unwrap();

    let logs = [
        transfer_log(f.token, payer(), early, usdc(9), 105, 0),
        transfer_log(f.token, payer(), cancelled, usdc(9), 107, 0),
    ];
    let out = f.sweep(110, &logs).await;
    assert_eq!(out.confirmed.len(), 0);
    assert_eq!(store::get_watch(&f.pool, w1.id).await.unwrap().unwrap().confirmed_amount, U256::ZERO);
    assert_eq!(store::get_watch(&f.pool, w2.id).await.unwrap().unwrap().confirmed_amount, U256::ZERO);
    assert!(f.event_types().await.is_empty());
}

#[sqlx::test]
async fn fast_forward_never_skips_blocks_an_active_watch_cares_about(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let chain_id = f.chain.chain_id;
    assert_eq!(sweep::fast_forward_cursor(&f.pool, chain_id, 500).await.unwrap(), 500, "no watches: jump to target");

    f.watch(Address::repeat_byte(1), usdc(1), 700).await; // start_block 701
    assert_eq!(sweep::fast_forward_cursor(&f.pool, chain_id, 900).await.unwrap(), 700, "stops right below start_block");
    assert_eq!(sweep::fast_forward_cursor(&f.pool, chain_id, 650).await.unwrap(), 700, "never moves backwards");
}

#[sqlx::test]
async fn outbox_delivers_each_watch_in_order_and_leases_survive_crashes(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let a = Address::repeat_byte(1);
    let b = Address::repeat_byte(2);
    f.watch(a, usdc(10), 100).await;
    f.watch(b, usdc(10), 100).await;
    let logs =
        [transfer_log(f.token, payer(), a, usdc(10), 101, 0), transfer_log(f.token, payer(), b, usdc(4), 101, 1)];
    f.sweep(110, &logs).await; // a: confirmed + threshold.reached, b: confirmed

    let lease = Duration::from_millis(300);
    let first: Vec<OutboxItem> = outbox::lease_due(&f.pool, 10, lease).await.unwrap();
    let mut kinds: Vec<_> = first.iter().map(|i| i.event_type.as_str()).collect();
    kinds.sort_unstable();
    assert_eq!(kinds, ["payment.confirmed", "payment.confirmed"], "threshold.reached waits for its predecessor");
    assert!(outbox::lease_due(&f.pool, 10, lease).await.unwrap().is_empty(), "leased events are not handed out twice");

    // "Crash": never ack. After the lease expires the same events are due again, with a higher attempt count.
    tokio::time::sleep(lease + Duration::from_millis(100)).await;
    let retry = outbox::lease_due(&f.pool, 10, lease).await.unwrap();
    assert_eq!(retry.len(), 2);
    assert!(retry.iter().all(|i| i.attempts == 2));

    for item in &retry {
        outbox::mark_delivered(&f.pool, item.id, 200).await.unwrap();
    }
    let next = outbox::lease_due(&f.pool, 10, lease).await.unwrap();
    assert_eq!(next.iter().map(|i| i.event_type.as_str()).collect::<Vec<_>>(), ["threshold.reached"]);

    // A dead event unblocks the rest of its watch instead of wedging it forever.
    outbox::mark_dead(&f.pool, next[0].id, Some(500), "gave up").await.unwrap();
    let depth = outbox::depth(&f.pool).await.unwrap();
    assert_eq!((depth.pending, depth.dead), (0, 1));
}

#[sqlx::test]
async fn expiry_retires_overdue_watches_and_notifies(pool: PgPool) {
    let f = Fixture::new(pool, 100).await;
    let w = f.watch(Address::repeat_byte(1), usdc(10), 100).await;
    sqlx::query("UPDATE watches SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(w.id)
        .execute(&f.pool)
        .await
        .unwrap();
    let expired = store::expire_watches(&f.pool, &f.chain, 100).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert!(store::expire_watches(&f.pool, &f.chain, 100).await.unwrap().is_empty());
    assert_eq!(f.event_types().await, ["watch.expired"]);
}

/// Registrations race sweeps from several tasks. Invariant: once the cursor has passed a watch's start_block,
/// the sweeping side knows the watch — i.e. no registration can slip in "behind" a sweep.
#[sqlx::test]
async fn registrations_racing_sweeps_are_never_skipped(pool: PgPool) {
    let f = Arc::new(Fixture::new(pool, 1_000).await);
    let sweeper_cache = Arc::new(WatchCache::new());

    let registrars: Vec<_> = (0..8u8)
        .map(|t| {
            let f = f.clone();
            tokio::spawn(async move {
                for i in 0..40u8 {
                    let mut raw = [0u8; 20];
                    raw[0] = t + 1;
                    raw[1] = i;
                    let new = NewWatch {
                        chain_id: f.chain.chain_id,
                        token_address: f.token,
                        payment_address: Address::from(raw),
                        threshold: usdc(1),
                        webhook_url: HOOK.into(),
                        expires_at: None,
                        head_estimate: 0, // worst case: start_block is decided by the cursor alone
                    };
                    store::create_watch(&f.pool, &new).await.unwrap();
                }
            })
        })
        .collect();

    let sweeper = {
        let (f, cache) = (f.clone(), sweeper_cache.clone());
        tokio::spawn(async move {
            let mut violations = 0usize;
            for _ in 0..150 {
                let cursor = store::cursor(&f.pool, f.chain.chain_id).await.unwrap();
                let res = sweep::apply_sweep(&f.pool, &f.chain, &cache, cursor, cursor + 1, &[]).await.unwrap();
                assert!(matches!(res, SweepResult::Applied(_)));
                // Everything registered with start_block <= new cursor must be known to the sweeper by now.
                let (unknown,): (i64,) = sqlx::query_as(
                    "SELECT count(*) FROM watches WHERE chain_id = $1 AND start_block <= $2 AND seq > $3",
                )
                .bind(f.chain.chain_id as i64)
                .bind((cursor + 1) as i64)
                .bind(cache.last_seq())
                .fetch_one(&f.pool)
                .await
                .unwrap();
                violations += unknown as usize;
            }
            violations
        })
    };

    for r in registrars {
        r.await.unwrap();
    }
    assert_eq!(sweeper.await.unwrap(), 0, "a watch started at a block the sweep had already passed unseen");

    let cursor = store::cursor(&f.pool, f.chain.chain_id).await.unwrap();
    f.sweep(cursor + 1, &[]).await; // fixture cache plays a second instance; it must converge too
    assert_eq!(store::active_watch_count(&f.pool, f.chain.chain_id).await.unwrap(), 320);
    assert_eq!(f.cache.len(), 320);
}

/// Regression for the 2026-09-23 Monad incident: a burst of events for one consumer went out with up
/// to `max_concurrency` (64) deliveries at once, more than gum-server's 16-connection pool, and the
/// consumer answered 503. Deliveries to one host are now capped at `max_per_host`; the rest wait in
/// the outbox (without spending an attempt) and all arrive.
#[sqlx::test]
async fn deliveries_to_one_host_are_capped_and_nothing_is_lost(pool: PgPool) {
    const WATCHES: u8 = 40;
    const PER_HOST: usize = 4;
    let f = Fixture::new(pool, 100).await;
    let sink = gum_indexer::testkit::sink::WebhookSink::start().await;
    sink.set_delay(Duration::from_millis(100));
    for i in 1..=WATCHES {
        f.watch(Address::repeat_byte(i), usdc(10), 100).await;
    }
    sqlx::query("UPDATE watches SET expires_at = now() - interval '1 second', webhook_url = $1")
        .bind(sink.url())
        .execute(&f.pool)
        .await
        .unwrap();
    assert_eq!(store::expire_watches(&f.pool, &f.chain, 100).await.unwrap().len(), WATCHES as usize);

    let cfg = gum_indexer::config::WebhookConfig {
        secret: "s".into(),
        connect_timeout_ms: 1_000,
        request_timeout_ms: 5_000,
        max_concurrency: 64,
        max_per_host: PER_HOST,
        retry_base_ms: 50,
        retry_cap_ms: 200,
        max_age_secs: 3_600,
        host_failure_threshold: 5,
        host_park_ms: 1_000,
        allow_insecure_targets: true,
        host_allowlist: vec![],
    };
    let dispatcher =
        gum_indexer::webhook::Dispatcher::new(f.pool.clone(), cfg, Arc::new(tokio::sync::Notify::new())).unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let run = tokio::spawn(dispatcher.run(cancel.clone()));

    let got = sink.wait_for("every watch.expired", Duration::from_secs(20), |got| got.len() == WATCHES as usize).await;
    cancel.cancel();
    let _ = run.await;
    assert!(sink.max_in_flight() as usize <= PER_HOST, "{} deliveries at once", sink.max_in_flight());
    assert!(got.iter().all(|r| r.attempt == 1), "waiting for a slot is not a failed attempt");
}
