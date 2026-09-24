//! End-to-end: real app, real Anvil chain, real Postgres, real HTTP webhooks.
//! Needs `anvil` on PATH and Postgres (`docker compose up -d postgres`).
//!
//! Metrics are process-global and tests share a process, so every test uses its own chain name.

use std::time::Duration;

use alloy::primitives::{Address, U256};
use gum_indexer::{
    testkit::{
        chain::TestChain,
        harness::{ChainParams, TestApp, WEBHOOK_SECRET, fresh_address, test_config, usdc},
        proxy::{Fault, RpcProxy, TcpProxy},
        sink::WebhookSink,
    },
    webhook::sign,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

const T: Duration = Duration::from_secs(15);

async fn pool(opts: PgPoolOptions, conn: PgConnectOptions) -> PgPool {
    opts.max_connections(20).connect_with(conn).await.expect("test database")
}

struct Env {
    chain: TestChain,
    token: Address,
    sink: WebhookSink,
    name: String,
}

impl Env {
    async fn new(name: &str) -> Self {
        let chain = TestChain::start(31_337, None).await;
        let token = chain.deploy_token("USDC").await;
        Self { chain, token, sink: WebhookSink::start().await, name: name.into() }
    }

    fn params(&self) -> ChainParams {
        ChainParams::new(
            &self.name,
            31_337,
            self.chain.http_url(),
            self.chain.ws_url(),
            vec![("USDC".into(), self.token)],
        )
    }

    async fn watch(&self, app: &TestApp, address: Address, threshold: U256) -> gum_indexer::api::WatchResponse {
        app.create_watch(&self.name, "USDC", address, threshold, &self.sink.url()).await
    }
}

/// Waits until the push path is subscribed for this chain (so a payment.pending is actually expected).
async fn wait_subscribed(app: &TestApp, chain: &str) {
    app.wait_chain(chain, "WSS connected", T, |c| c["health"]["ws_connected"] == true).await;
    let needle = format!("gum_ws_buckets{{chain=\"{chain}\"}} ");
    let deadline = std::time::Instant::now() + T;
    loop {
        let metrics = app.metrics().await;
        let buckets = metrics.lines().find_map(|l| l.strip_prefix(&needle)).and_then(|v| v.trim().parse::<f64>().ok());
        if buckets.unwrap_or(0.0) >= 1.0 {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "no WSS bucket subscribed for {chain}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn metric(metrics: &str, prefix: &str) -> f64 {
    metrics.lines().filter(|l| l.starts_with(prefix)).filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok()).sum()
}

#[sqlx::test]
async fn happy_path_pending_confirmed_threshold(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("happy").await;
    let app = TestApp::start(test_config(&[env.params()]), pool(opts, conn).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(50)).await;
    assert_eq!(watch.status, "active");
    wait_subscribed(&app, "happy").await;

    // 1) Seen at head → payment.pending immediately, without any further block.
    let first = env.chain.mint(env.token, payee, usdc(20)).await;
    let got = env.sink.wait_for_types(&["payment.pending"], T).await;
    let transfer = got[0].payload.transfer.as_ref().unwrap();
    assert_eq!(
        (transfer.tx_hash, transfer.amount, transfer.block_number),
        (first.tx_hash, usdc(20), first.block_number)
    );
    assert_eq!(got[0].payload.watch.confirmed_amount, U256::ZERO, "pending is not counted");
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, U256::ZERO);

    // 2) Confirmation depth reached → payment.confirmed, amount counted.
    env.chain.mine(2).await;
    env.sink.wait_for_types(&["payment.pending", "payment.confirmed"], T).await;
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(20));

    // 3) Second payment crosses the threshold → threshold.reached, watch retired.
    env.chain.mint(env.token, payee, usdc(30)).await;
    env.chain.mine(2).await;
    let got = env
        .sink
        .wait_for_types(
            &["payment.pending", "payment.confirmed", "payment.pending", "payment.confirmed", "threshold.reached"],
            T,
        )
        .await;

    let sequences: Vec<i64> = got.iter().map(|r| r.payload.sequence).collect();
    assert_eq!(sequences, [1, 2, 3, 4, 5], "events of one watch arrive in order");
    let last = got.last().unwrap();
    assert_eq!(last.payload.event_type.as_str(), "threshold.reached");
    assert_eq!(last.payload.watch.confirmed_amount, usdc(50));
    assert_eq!(last.payload.watch.status, "completed");
    for r in &got {
        assert!(
            sign::verify(WEBHOOK_SECRET, &r.signature, &r.body, chrono::Utc::now().timestamp(), 300),
            "bad signature"
        );
    }

    let done = app.get_watch(watch.id).await;
    assert_eq!((done.status.as_str(), done.confirmed_amount), ("completed", usdc(50)));
    assert_eq!(done.transfers.unwrap().len(), 2);

    // 4) Retired means retired: further transfers are not reported.
    env.chain.mint(env.token, payee, usdc(5)).await;
    env.chain.mine(3).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(env.sink.received().len(), 5);

    let stats: serde_json::Value = app.get_json("/v1/stats").await;
    let chain_stats = stats["chains"].as_array().unwrap().iter().find(|c| c["chain"] == "happy").unwrap().clone();
    assert_eq!(chain_stats["payments_confirmed"], 2);
    assert_eq!(chain_stats["thresholds_reached"], 1);
    assert_eq!(chain_stats["tokens"][0]["confirmed_volume"], usdc(50).to_string());
    app.shutdown().await;
}

/// A token whose `Transfer` logs come from a separate emitter at more decimals (Arc's USDC: the ERC-20 at 0x3600…
/// logs at 6 decimals, the EIP-7708 system emitter logs every movement at 18). Only the emitter counts, scaled to
/// the token's decimals, so an ERC-20 transfer (logged by both) counts once and a native send is not missed.
#[sqlx::test]
async fn split_log_emitter_counts_each_payment_once_in_token_units(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("split_emitter").await;
    let emitter = env.chain.deploy_token_with_decimals("SYS", 18).await;
    let params = env.params().log_source("USDC", emitter, 18);
    let app = TestApp::start(test_config(&[params]), pool(opts, conn).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(5)).await;
    wait_subscribed(&app, "split_emitter").await;
    let wei = |units: U256| units * U256::from(10u64).pow(U256::from(12u64));

    // An ERC-20-interface payment of 2 USDC: both emitters log it.
    env.chain.mint(env.token, payee, usdc(2)).await;
    let erc20_copy = env.chain.mint(emitter, payee, wei(usdc(2))).await;
    env.chain.mine(2).await;
    let got = env.sink.wait_for_types(&["payment.pending", "payment.confirmed"], T).await;
    let confirmed = got.iter().find(|r| r.payload.event_type.as_str() == "payment.confirmed").unwrap();
    let transfer = confirmed.payload.transfer.as_ref().unwrap();
    assert_eq!((transfer.tx_hash, transfer.amount), (erc20_copy.tx_hash, usdc(2)), "scaled to 6 decimals");
    assert_eq!(confirmed.payload.watch.token_address, env.token, "consumers see the ERC-20, not the emitter");
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(2), "counted once");

    // A native send of dust (below one base unit), then of 3 USDC: only the emitter logs these.
    env.chain.mint(emitter, payee, U256::from(999_999_999_999u64)).await;
    env.chain.mint(emitter, payee, wei(usdc(3))).await;
    env.chain.mine(2).await;
    env.sink
        .wait_for_types(
            &["payment.pending", "payment.confirmed", "payment.pending", "payment.confirmed", "threshold.reached"],
            T,
        )
        .await;
    let done = app.get_watch(watch.id).await;
    assert_eq!((done.status.as_str(), done.confirmed_amount), ("completed", usdc(5)));
    assert_eq!(done.transfers.unwrap().len(), 2, "dust is not a payment");

    // A late registration's backfill reads the same emitter.
    env.watch(&app, fresh_address(), usdc(1_000_000)).await; // keeps sweeps advancing the cursor
    let late = fresh_address();
    let handed_out = chrono::Utc::now() - chrono::Duration::minutes(5);
    let paid = env.chain.mint(emitter, late, wei(usdc(4))).await;
    env.chain.mine(5).await;
    let deadline = std::time::Instant::now() + T;
    while app.get_json::<serde_json::Value>("/v1/chains").await["chains"][0]["health"]["confirmed_block"]
        .as_u64()
        .unwrap_or(0)
        < paid.block_number
    {
        assert!(std::time::Instant::now() < deadline, "cursor never passed block {}", paid.block_number);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let resp = app
        .create_watch_raw(serde_json::json!({
            "payment_address": late, "chain": "split_emitter", "token": "USDC", "balance_threshold": usdc(10).to_string(),
            "webhook_endpoint": env.sink.url(), "payments_since": handed_out,
        }))
        .await;
    assert_eq!(resp.status(), 201);
    let late_watch: gum_indexer::api::WatchResponse = resp.json().await.unwrap();
    env.chain.mine(3).await;
    let deadline = std::time::Instant::now() + T;
    while app.get_watch(late_watch.id).await.confirmed_amount != usdc(4) {
        assert!(std::time::Instant::now() < deadline, "backfill did not count the emitter's log");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    app.shutdown().await;
}

/// Payments that arrive while the service is down are recovered from the durable cursor — exactly once.
#[sqlx::test]
async fn crash_and_restart_recovers_payments_made_during_downtime(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("restart").await;
    let cfg = test_config(&[env.params()]);
    let app = TestApp::start(cfg.clone(), pool(opts.clone(), conn.clone()).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(100)).await;
    wait_subscribed(&app, "restart").await;

    env.chain.mint(env.token, payee, usdc(10)).await;
    env.sink.wait_for_types(&["payment.pending"], T).await;

    // Crash before the first payment reaches depth; two more payments land while nobody is watching.
    let old_pool = app.pool.clone();
    app.kill().await;
    old_pool.close().await;
    env.chain.mint(env.token, payee, usdc(20)).await;
    env.chain.mint(env.token, payee, usdc(30)).await;
    env.chain.mine(5).await;

    let app = TestApp::start(cfg, pool(opts, conn).await).await;
    let got = env
        .sink
        .wait_for("three confirmations", T, |got| {
            got.iter().filter(|r| r.payload.event_type.as_str() == "payment.confirmed").count() == 3
        })
        .await;
    tokio::time::sleep(Duration::from_millis(1200)).await; // another safety sweep must not double count

    let w = app.get_watch(watch.id).await;
    assert_eq!(w.confirmed_amount, usdc(60), "10 (seen before crash) + 20 + 30 (during downtime), each once");
    assert_eq!(w.transfers.unwrap().iter().filter(|t| t.status == "confirmed").count(), 3);
    let pendings = got.iter().filter(|r| r.payload.event_type.as_str() == "payment.pending").count();
    assert_eq!(pendings, 1, "payments found by the catch-up sweep are confirmed directly");
    let mut ids: Vec<_> = env.sink.received().iter().map(|r| r.payload.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), env.sink.received().len(), "no event delivered twice");
    app.shutdown().await;
}

#[sqlx::test]
async fn reorged_out_payment_is_reported_orphaned_and_never_counted(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("reorg").await;
    let app = TestApp::start(test_config(&[env.params().confirmations(4)]), pool(opts, conn).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(100)).await;
    wait_subscribed(&app, "reorg").await;

    env.chain.mint(env.token, payee, usdc(40)).await;
    env.sink.wait_for_types(&["payment.pending"], T).await;

    // The block holding the payment is replaced by an empty one, then the chain moves past confirmation depth.
    env.chain.reorg(1).await;
    env.chain.mine(6).await;
    let got = env.sink.wait_for_types(&["payment.pending", "payment.orphaned"], T).await;
    assert_eq!(got[1].payload.transfer.as_ref().unwrap().amount, usdc(40));

    let w = app.get_watch(watch.id).await;
    assert_eq!((w.status.as_str(), w.confirmed_amount), ("active", U256::ZERO));

    // The watch keeps working after the reorg.
    env.chain.mint(env.token, payee, usdc(15)).await;
    env.chain.mine(4).await;
    env.sink
        .wait_for("confirmation after reorg", T, |got| {
            got.iter().any(|r| r.payload.event_type.as_str() == "payment.confirmed")
        })
        .await;
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(15));
    app.shutdown().await;
}

#[sqlx::test]
async fn wss_outage_falls_back_to_polling_and_recovers(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("wsdrop").await;
    let ws_proxy = TcpProxy::start(env.chain.port()).await;
    let mut params = env.params();
    params.ws_url = ws_proxy.ws_url();
    let app = TestApp::start(test_config(&[params]), pool(opts, conn).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(100)).await;
    wait_subscribed(&app, "wsdrop").await;

    ws_proxy.outage();
    app.wait_chain("wsdrop", "poll fallback", T, |c| c["health"]["ingest_mode"] == "poll_fallback").await;
    let degraded = app.wait_chain("wsdrop", "degraded", T, |c| c["health"]["status"] == "degraded").await;
    assert_eq!(degraded["health"]["ws_connected"], false);

    // Payment during the outage is still announced promptly (via polling) and confirmed.
    env.chain.mint(env.token, payee, usdc(12)).await;
    env.sink.wait_for_types(&["payment.pending"], T).await;
    env.chain.mine(2).await;
    env.sink.wait_for_types(&["payment.pending", "payment.confirmed"], T).await;

    ws_proxy.restore();
    app.wait_chain("wsdrop", "healthy on WSS again", T, |c| {
        c["health"]["status"] == "healthy" && c["health"]["ws_connected"] == true
    })
    .await;
    wait_subscribed(&app, "wsdrop").await;
    env.chain.mint(env.token, payee, usdc(8)).await;
    env.chain.mine(2).await;
    env.sink
        .wait_for("second confirmation", T, |got| {
            got.iter().filter(|r| r.payload.event_type.as_str() == "payment.confirmed").count() == 2
        })
        .await;
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(20));
    assert!(metric(&app.metrics().await, "gum_ws_reconnects_total{chain=\"wsdrop\"") >= 1.0);
    app.shutdown().await;
}

/// A severed socket (not a full outage): notifications in the gap are lost by WSS, found by the reconnect sweep.
#[sqlx::test]
async fn payment_in_a_wss_gap_is_caught_by_the_reconnect_sweep(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("wsgap").await;
    let ws_proxy = TcpProxy::start(env.chain.port()).await;
    let mut params = env.params().extra("ws_down_fallback_ms = 60000\nsafety_sweep_interval_ms = 60000");
    params.ws_url = ws_proxy.ws_url();
    let app = TestApp::start(test_config(&[params]), pool(opts, conn).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(100)).await;
    wait_subscribed(&app, "wsgap").await;
    // The sweep queued by the initial connect must be over before the outage starts; otherwise, on a loaded
    // machine, it can run late and (correctly) confirm the payment below, which this test wants to attribute
    // to the *re*connect sweep.
    let deadline = std::time::Instant::now() + T;
    while metric(&app.metrics().await, "gum_sweeps_total{chain=\"wsgap\",trigger=\"ws_connect\"}") < 1.0 {
        assert!(std::time::Instant::now() < deadline, "initial ws_connect sweep never ran");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    ws_proxy.outage();
    app.wait_chain("wsgap", "WSS down", T, |c| c["health"]["ws_connected"] == false).await;
    env.chain.mint(env.token, payee, usdc(33)).await; // nobody is listening, polling fallback is disabled
    env.chain.mine(3).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(env.sink.received().is_empty());

    ws_proxy.restore(); // reconnect → catch-up sweep from the durable cursor
    env.sink.wait_for_types(&["payment.confirmed"], T).await;
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(33));
    app.shutdown().await;
}

#[sqlx::test]
async fn webhook_endpoint_outage_delays_but_never_loses_or_reorders(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("hookdown").await;
    let app = TestApp::start(test_config(&[env.params()]), pool(opts, conn).await).await;
    let payee = fresh_address();
    env.watch(&app, payee, usdc(10)).await;
    wait_subscribed(&app, "hookdown").await;

    env.sink.set_down(true);
    env.chain.mint(env.token, payee, usdc(10)).await;
    env.chain.mine(2).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(env.sink.received().is_empty());
    assert!(env.sink.rejected() >= 2, "deliveries are being retried while the endpoint is down");

    env.sink.set_down(false);
    let got = env.sink.wait_for_types(&["payment.pending", "payment.confirmed", "threshold.reached"], T).await;
    assert_eq!(got.iter().map(|r| r.payload.sequence).collect::<Vec<_>>(), [1, 2, 3], "order survives retries");
    assert!(got[0].attempt > 1);
    assert!(
        metric(&app.metrics().await, "gum_webhook_deliveries_total{event=\"payment.pending\",outcome=\"retry\"}")
            >= 1.0
    );
    app.shutdown().await;
}

#[sqlx::test]
async fn rpc_faults_and_range_limits_cause_no_loss_and_no_double_count(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("rpcfault").await;
    let rpc = RpcProxy::start(env.chain.http_url()).await;
    let mut params = env.params().mode("poll");
    params.http_url = rpc.http_url();
    let cfg = test_config(&[params]);
    let app = TestApp::start(cfg.clone(), pool(opts.clone(), conn.clone()).await).await;
    let payee = fresh_address();
    let watch = env.watch(&app, payee, usdc(1000)).await;

    // Rate limits, provider errors and gateway failures while payments arrive.
    rpc.fail("eth_getLogs", 3, Fault::Http(429));
    env.chain.mint(env.token, payee, usdc(1)).await;
    tokio::time::sleep(Duration::from_millis(800)).await; // let the injected faults be consumed
    rpc.fail("eth_blockNumber", 2, Fault::Rpc { code: -32007, message: "50/second request limit reached".into() });
    env.chain.mint(env.token, payee, usdc(2)).await;
    rpc.fail("eth_getLogs", 2, Fault::Http(502));
    env.chain.mint(env.token, payee, usdc(3)).await;
    env.chain.mine(3).await;
    env.sink
        .wait_for("3 confirmations despite faults", T, |got| {
            got.iter().filter(|r| r.payload.event_type.as_str() == "payment.confirmed").count() == 3
        })
        .await;
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(6));

    // Long downtime + a provider that only serves 5-block ranges: the catch-up shrinks its range and completes.
    let old_pool = app.pool.clone();
    app.kill().await;
    old_pool.close().await;
    env.chain.mint(env.token, payee, usdc(4)).await;
    env.chain.mine(60).await;
    env.chain.mint(env.token, payee, usdc(5)).await;
    env.chain.mine(3).await;
    rpc.limit_log_range(5);
    let app = TestApp::start(cfg, pool(opts, conn).await).await;
    env.sink
        .wait_for("5 confirmations after catch-up", T, |got| {
            got.iter().filter(|r| r.payload.event_type.as_str() == "payment.confirmed").count() == 5
        })
        .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(15));
    let metrics = app.metrics().await;
    let shrinks = metric(&metrics, "gum_sweep_range_shrinks_total{chain=\"rpcfault\"}");
    assert!(
        (1.0..=2.0).contains(&shrinks),
        "the limit is read from the provider's error, not rediscovered by bisection: {shrinks}"
    );
    let rpc_lines: Vec<_> =
        metrics.lines().filter(|l| l.starts_with("gum_rpc_requests_total{chain=\"rpcfault\"")).collect();
    assert!(
        metric(
            &metrics,
            "gum_rpc_requests_total{chain=\"rpcfault\",method=\"eth_getLogs\",outcome=\"rpc_rate_limited\"}"
        ) >= 1.0,
        "{rpc_lines:#?}"
    );
    app.shutdown().await;
}

/// Deploy overlap: two instances on one database. Exactly one ingests; when it leaves, the other takes over.
#[sqlx::test]
async fn two_instances_never_double_count_and_fail_over(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("twins").await;
    let cfg = test_config(&[env.params()]);
    let a = TestApp::start(cfg.clone(), pool(opts.clone(), conn.clone()).await).await;
    let b = TestApp::start(cfg, pool(opts, conn).await).await;

    let leader_of = |v: &serde_json::Value| v["health"]["leader"] == true;
    a.wait_chain("twins", "a leads", T, leader_of).await;
    let b_view = b.wait_chain("twins", "b is ready", T, |c| c["ready"] == true).await;
    assert_eq!(b_view["health"]["leader"], false, "only one instance holds the chain's advisory lock");

    // Registered through the standby's API; the leader learns about it via NOTIFY and ingests.
    let payee = fresh_address();
    let watch = env.watch(&b, payee, usdc(100)).await;
    a.wait_chain("twins", "leader sees the watch", T, |c| c["active_watches"] == 1).await;
    wait_subscribed(&a, "twins").await;
    env.chain.mint(env.token, payee, usdc(25)).await;
    env.chain.mine(2).await;
    env.sink.wait_for_types(&["payment.pending", "payment.confirmed"], T).await;

    // Old instance drains (SIGTERM during a deploy); the new one must take over without losing anything.
    a.shutdown().await;
    env.chain.mint(env.token, payee, usdc(35)).await;
    env.chain.mine(2).await;
    b.wait_chain("twins", "b leads", T, leader_of).await;
    env.sink
        .wait_for("second confirmation after failover", T, |got| {
            got.iter().filter(|r| r.payload.event_type.as_str() == "payment.confirmed").count() == 2
        })
        .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(b.get_watch(watch.id).await.confirmed_amount, usdc(60));
    let confirmed = env.sink.received().iter().filter(|r| r.payload.event_type.as_str() == "payment.confirmed").count();
    assert_eq!(confirmed, 2, "no duplicate accounting or duplicate events across instances");
    b.shutdown().await;
}

#[sqlx::test]
async fn one_chain_down_does_not_affect_the_other_and_liveness_stays_green(
    opts: PgPoolOptions,
    conn: PgConnectOptions,
) {
    let good = Env::new("iso_good").await;
    let bad_chain = TestChain::start(31_338, None).await;
    let bad_token = bad_chain.deploy_token("USDC").await;
    let bad_proxy = TcpProxy::start(bad_chain.port()).await;
    let bad =
        ChainParams::new("iso_bad", 31_338, bad_proxy.http_url(), bad_proxy.ws_url(), vec![("USDC".into(), bad_token)]);
    let app = TestApp::start(test_config(&[good.params(), bad]), pool(opts, conn).await).await;

    let payee = fresh_address();
    let bad_payee = fresh_address();
    let good_watch = good.watch(&app, payee, usdc(5)).await;
    app.create_watch("iso_bad", "USDC", bad_payee, usdc(5), &good.sink.url()).await;

    bad_proxy.outage();
    app.wait_chain("iso_bad", "down", T, |c| c["health"]["status"] == "down").await;

    wait_subscribed(&app, "iso_good").await;
    good.chain.mint(good.token, payee, usdc(5)).await;
    good.chain.mine(2).await;
    good.sink.wait_for_types(&["payment.pending", "payment.confirmed", "threshold.reached"], T).await;
    assert_eq!(app.get_watch(good_watch.id).await.status, "completed");
    let view = app.wait_chain("iso_good", "healthy", T, |c| c["health"]["status"] == "healthy").await;
    assert_eq!(view["health"]["consecutive_rpc_failures"], 0);

    let health = reqwest::get(format!("{}/healthz", app.base_url)).await.unwrap();
    assert!(health.status().is_success(), "liveness must not depend on chain health");
    assert!(metric(&app.metrics().await, "gum_chain_status{chain=\"iso_bad\"}") == 0.0);

    // The outage ends: the payment made meanwhile is picked up and the chain reports healthy again.
    bad_chain.mint(bad_token, bad_payee, usdc(5)).await;
    bad_chain.mine(2).await;
    bad_proxy.restore();
    app.wait_chain("iso_bad", "recovered", T, |c| c["health"]["status"] == "healthy").await;
    good.sink
        .wait_for("payment on recovered chain", T, |got| {
            got.iter()
                .any(|r| r.payload.watch.chain == "iso_bad" && r.payload.event_type.as_str() == "threshold.reached")
        })
        .await;
    app.shutdown().await;
}

#[sqlx::test]
async fn a_chain_with_no_watches_makes_no_log_queries(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("parked").await;
    let rpc = RpcProxy::start(env.chain.http_url()).await;
    let mut params = env.params().mode("poll");
    params.http_url = rpc.http_url();
    let app = TestApp::start(test_config(&[params]), pool(opts, conn).await).await;

    env.chain.mine(20).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(rpc.calls("eth_getLogs"), 0, "idle chains must not burn credits on log queries");
    assert!(rpc.calls("eth_blockNumber") <= 6, "only the slow health probe runs: {:?}", rpc.call_counts());
    let view = app.wait_chain("parked", "parked", T, |c| c["health"]["ingest_mode"] == "parked").await;
    assert_eq!(view["health"]["status"], "healthy");

    // The first watch un-parks the chain; blocks mined while parked are skipped, not scanned.
    let payee = fresh_address();
    env.watch(&app, payee, usdc(1)).await;
    env.chain.mint(env.token, payee, usdc(1)).await;
    env.chain.mine(2).await;
    env.sink.wait_for_types(&["payment.pending", "payment.confirmed", "threshold.reached"], T).await;
    app.shutdown().await;
}

#[sqlx::test]
async fn buckets_mode_switch_multi_token_and_noise(opts: PgPoolOptions, conn: PgConnectOptions) {
    let mut env = Env::new("buckets").await;
    let usdt = env.chain.deploy_token("USDT").await;
    env.name = "buckets".into();
    let mut params = env.params().extra("ws_bucket_size = 2\nws_targeted_max = 5");
    params.tokens.push(("USDT".into(), usdt));
    params.ingest_mode = "auto".into();
    let app = TestApp::start(test_config(&[params]), pool(opts, conn).await).await;

    // Five recipients with bucket size 2 → three subscriptions, all live.
    let payees: Vec<Address> = (0..5).map(|_| fresh_address()).collect();
    for (i, p) in payees.iter().enumerate() {
        let token = if i % 2 == 0 { "USDC" } else { "USDT" };
        app.create_watch("buckets", token, *p, usdc(1000), &env.sink.url()).await;
    }
    wait_subscribed(&app, "buckets").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(metric(&app.metrics().await, "gum_ws_buckets{chain=\"buckets\"}"), 3.0);

    // Noise: transfers to strangers, a zero-value transfer to a payee, and the wrong token for a payee.
    env.chain.mint_batch(env.token, (0..50).map(|_| fresh_address()).collect(), usdc(1)).await;
    env.chain.mint(env.token, payees[0], U256::ZERO).await;
    env.chain.mint(usdt, payees[0], usdc(9)).await; // payees[0] watches USDC, not USDT
    for (i, p) in payees.iter().enumerate() {
        let token = if i % 2 == 0 { env.token } else { usdt };
        env.chain.mint(token, *p, usdc(i as u64 + 1)).await;
    }
    env.chain.mine(2).await;
    let got = env.sink.wait_for("5 pending + 5 confirmed", T, |got| got.len() == 10).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(env.sink.received().len(), 10, "noise produced no events");
    let tokens: std::collections::BTreeSet<_> = got.iter().map(|r| r.payload.watch.token.clone()).collect();
    assert_eq!(tokens.into_iter().collect::<Vec<_>>(), ["USDC", "USDT"]);

    // A sixth watch crosses ws_targeted_max → the chain switches to its large-scale mode and keeps working.
    let sixth = fresh_address();
    let w = app.create_watch("buckets", "USDC", sixth, usdc(2), &env.sink.url()).await;
    app.wait_chain("buckets", "large-scale mode", T, |c| c["health"]["ingest_mode"] == "poll").await;
    env.chain.mint(env.token, sixth, usdc(2)).await;
    env.chain.mine(2).await;
    env.sink
        .wait_for("threshold in poll mode", T, |got| {
            got.iter().any(|r| r.payload.watch.id == w.id && r.payload.event_type.as_str() == "threshold.reached")
        })
        .await;
    app.shutdown().await;
}

#[sqlx::test]
async fn api_validation_auth_and_idempotency(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("api").await;
    let app = TestApp::start(test_config(&[env.params()]), pool(opts, conn).await).await;
    let payee = fresh_address();
    let body = |patch: serde_json::Value| {
        let mut base = serde_json::json!({
            "payment_address": payee, "chain": "api", "token": "USDC",
            "balance_threshold": "1000000", "webhook_endpoint": env.sink.url(),
        });
        base.as_object_mut().unwrap().extend(patch.as_object().unwrap().clone());
        base
    };

    // No credentials are needed: the private network is the boundary, so a bare request reaches validation.
    let client = reqwest::Client::new();
    let plain = client
        .post(format!("{}/v1/watches", app.base_url))
        .json(&body(serde_json::json!({ "payment_address": "0x1234" })))
        .send()
        .await
        .unwrap();
    assert_eq!(plain.status(), 422);

    for (patch, needle) in [
        (serde_json::json!({ "payment_address": "0x1234" }), "payment_address"),
        (serde_json::json!({ "payment_address": Address::ZERO }), "zero address"),
        (serde_json::json!({ "chain": "dogechain" }), "unsupported chain"),
        (serde_json::json!({ "token": "USDT" }), "not supported on api"),
        (serde_json::json!({ "balance_threshold": "0" }), "greater than zero"),
        (serde_json::json!({ "balance_threshold": "1.5" }), "base-unit integer"),
        (serde_json::json!({ "balance_threshold": -5 }), "base-unit integer"),
        (serde_json::json!({ "webhook_endpoint": "ftp://x" }), "webhook_endpoint"),
        (serde_json::json!({ "expires_at": "2001-01-01T00:00:00Z" }), "past"),
    ] {
        let resp = app.create_watch_raw(body(patch.clone())).await;
        assert_eq!(resp.status(), 422, "{patch}");
        let text = resp.text().await.unwrap();
        assert!(text.contains(needle), "{patch}: {text}");
    }

    // The spec's field names are accepted as aliases; numeric thresholds too.
    let created = app
        .create_watch_raw(serde_json::json!({
            "payment_address": payee, "blockchain": "31337", "erc20_token": env.token,
            "balance_threshold": 1000000, "webhook_endpoint": env.sink.url(),
        }))
        .await;
    assert_eq!(created.status(), 201);
    let created: gum_indexer::api::WatchResponse = created.json().await.unwrap();
    assert_eq!((created.token.as_str(), created.balance_threshold), ("USDC", usdc(1)));

    let again = app.create_watch_raw(body(serde_json::json!({}))).await;
    assert_eq!(again.status(), 200, "same registration is idempotent");
    assert_eq!(again.json::<gum_indexer::api::WatchResponse>().await.unwrap().id, created.id);
    assert_eq!(app.create_watch_raw(body(serde_json::json!({ "balance_threshold": "2000000" }))).await.status(), 409);

    let cancelled = client.delete(format!("{}/v1/watches/{}", app.base_url, created.id)).send().await.unwrap();
    assert_eq!(cancelled.json::<gum_indexer::api::WatchResponse>().await.unwrap().status, "cancelled");
    env.chain.mint(env.token, payee, usdc(1)).await;
    env.chain.mine(3).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert!(env.sink.received().is_empty(), "cancelled watches are not reported");
    assert_eq!(
        client.get(format!("{}/v1/watches/{}", app.base_url, uuid::Uuid::new_v4())).send().await.unwrap().status(),
        404
    );
    app.shutdown().await;
}

/// Regression for 2026-09-23, end to end: an address is paid, the sweeper moves past that block,
/// and only then is its watch registered (the registering service was stalled). With
/// `payments_since` the earlier payment is found by a backfill and delivered like any other;
/// without it, it is not counted (the behaviour for callers that do not ask).
#[sqlx::test]
async fn payments_made_before_a_late_registration_are_counted(opts: PgPoolOptions, conn: PgConnectOptions) {
    let env = Env::new("late").await;
    let db = pool(opts, conn).await;
    let app = TestApp::start(test_config(&[env.params()]), db.clone()).await;
    env.watch(&app, fresh_address(), usdc(1_000_000)).await; // keeps sweeps advancing the cursor

    let (late, unannounced) = (fresh_address(), fresh_address());
    let handed_out = chrono::Utc::now() - chrono::Duration::minutes(5);
    let paid = env.chain.mint(env.token, late, usdc(4)).await;
    env.chain.mint(env.token, unannounced, usdc(4)).await;
    env.chain.mine(5).await;
    // Once the durable cursor is past the payment, only a backfill can find it.
    let deadline = std::time::Instant::now() + T;
    loop {
        let (cursor,): (i64,) = sqlx::query_as("SELECT confirmed_block FROM chain_cursors WHERE chain_id = 31337")
            .fetch_one(&db)
            .await
            .unwrap();
        if cursor as u64 >= paid.block_number {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "cursor never passed block {}", paid.block_number);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let resp = app
        .create_watch_raw(serde_json::json!({
            "payment_address": late, "chain": "late", "token": "USDC", "balance_threshold": usdc(10).to_string(),
            "webhook_endpoint": env.sink.url(), "payments_since": handed_out,
        }))
        .await;
    assert_eq!(resp.status(), 201);
    let watch: gum_indexer::api::WatchResponse = resp.json().await.unwrap();
    assert!(watch.start_block <= paid.block_number, "start {} after payment {}", watch.start_block, paid.block_number);

    // Paid again after registering: counted by the sweeps as usual, which completes the threshold.
    env.chain.mint(env.token, late, usdc(6)).await;
    env.chain.mine(3).await;
    let got = env
        .sink
        .wait_for("the backfilled payment, the live one, and threshold.reached", T, |got| {
            got.iter().any(|r| r.payload.watch.id == watch.id && r.payload.event_type.as_str() == "threshold.reached")
        })
        .await;
    let backfilled = got.iter().find(|r| {
        r.payload.watch.id == watch.id
            && r.payload.event_type.as_str() == "payment.confirmed"
            && r.payload.transfer.as_ref().is_some_and(|t| t.tx_hash == paid.tx_hash)
    });
    assert!(backfilled.is_some(), "the payment made before registration is delivered with its transaction");
    assert_eq!(app.get_watch(watch.id).await.confirmed_amount, usdc(10));
    assert!(metric(&app.metrics().await, "gum_logs_scanned_total{chain=\"late\",source=\"backfill\"}") >= 1.0);

    // Registered just as late, without `payments_since`: the earlier payment is not counted.
    let control = env.watch(&app, unannounced, usdc(10)).await;
    env.chain.mine(5).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(app.get_watch(control.id).await.confirmed_amount, U256::ZERO);
    app.shutdown().await;
}
