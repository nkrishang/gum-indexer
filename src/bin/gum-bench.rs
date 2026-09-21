//! End-to-end load benchmark against a local Anvil chain.
//!
//!   docker compose up -d postgres
//!   cargo run --release --features testkit --bin gum-bench -- --watches 100000 --payments 500 --noise 200
//!
//! Anvil runs in automine mode: a transaction is mined the instant it is sent, so
//! `webhook received − send returned` is the service's own detection + persistence + delivery latency,
//! with no block-interval wait mixed in. RPC calls are counted by a proxy in front of Anvil's HTTP port,
//! which is how "calls per payment" (≈ QuickNode credits per payment) is measured.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, U256};
use clap::Parser;
use gum_indexer::{
    store,
    testkit::{
        chain::TestChain,
        harness::{ChainParams, TestApp, fresh_address, test_config},
        proxy::RpcProxy,
        sink::WebhookSink,
    },
};
use hdrhistogram::Histogram;
use sqlx::{
    ConnectOptions, Connection,
    postgres::{PgConnectOptions, PgPoolOptions},
};

#[derive(Parser, Debug)]
#[command(about = "gum-indexer end-to-end load benchmark")]
struct Args {
    /// Active watches pre-loaded before the service starts (measures hydration and cache scale).
    #[arg(long, default_value_t = 10_000)]
    watches: u64,
    /// Payments to watched addresses.
    #[arg(long, default_value_t = 300)]
    payments: u64,
    /// Payments per second.
    #[arg(long, default_value_t = 50)]
    rate: u64,
    /// Unwatched transfers emitted alongside every payment (token-wide background traffic).
    #[arg(long, default_value_t = 100)]
    noise: usize,
    /// ws_targeted | ws_firehose | poll | auto
    #[arg(long, default_value = "auto")]
    mode: String,
    /// Mode `auto` switches to above 5,000 watches: ws_firehose | poll
    #[arg(long, default_value = "ws_firehose")]
    large_scale_mode: String,
    #[arg(long, default_value_t = 2)]
    confirmations: u64,
    /// Write the JSON report here as well as to stdout.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

fn watched_address(i: u64) -> Address {
    let mut raw = [0u8; 20];
    raw[12..].copy_from_slice(&i.to_be_bytes());
    Address::from(raw)
}

fn summarize(name: &str, h: &Histogram<u64>) -> serde_json::Value {
    let ms = |v: u64| (v as f64) / 1000.0;
    println!(
        "  {name:<34} n={:<6} p50={:>8.2}ms  p90={:>8.2}ms  p99={:>8.2}ms  max={:>8.2}ms",
        h.len(),
        ms(h.value_at_quantile(0.5)),
        ms(h.value_at_quantile(0.9)),
        ms(h.value_at_quantile(0.99)),
        ms(h.max())
    );
    serde_json::json!({
        "count": h.len(), "p50_ms": ms(h.value_at_quantile(0.5)), "p90_ms": ms(h.value_at_quantile(0.9)),
        "p99_ms": ms(h.value_at_quantile(0.99)), "max_ms": ms(h.max()), "mean_ms": h.mean() / 1000.0,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.payments <= args.watches, "--payments cannot exceed --watches");
    // Keep the service's own logs out of the report unless asked for.
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "warn") };
    }

    // A throwaway database so runs never interfere with each other or with dev data.
    let base_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://gum:gum@localhost:54329/gum".into());
    let base: PgConnectOptions = base_url.parse()?;
    let db_name = format!("gum_bench_{}", std::process::id());
    let mut admin = base.clone().connect().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {db_name}"))).execute(&mut admin).await?;
    let pool = PgPoolOptions::new().max_connections(32).connect_with(base.clone().database(&db_name)).await?;
    store::migrate(&pool).await?;

    let chain = TestChain::start(31_337, None).await;
    let token = chain.deploy_token("USDC").await;
    let rpc = RpcProxy::start(chain.http_url()).await;
    let sink = WebhookSink::start().await;

    println!(
        "gum-bench: {} watches, {} payments @ {}/s, {} noise transfers per payment, mode={} (large-scale: {})",
        args.watches, args.payments, args.rate, args.noise, args.mode, args.large_scale_mode
    );

    // Bulk-load the watchlist straight into Postgres: this is what a restart of a busy service looks like.
    store::ensure_cursor(&pool, 31_337, chain.head().await).await?;
    let load_started = Instant::now();
    sqlx::query(
        "INSERT INTO watches (id, chain_id, token_address, payment_address, threshold, webhook_url, start_block)
         SELECT gen_random_uuid(), 31337, $1, decode(lpad(to_hex(g), 40, '0'), 'hex'), 1000000, $2, 0
         FROM generate_series(1, $3::bigint) g",
    )
    .bind(token.as_slice())
    .bind(sink.url())
    .bind(args.watches as i64)
    .execute(&pool)
    .await?;
    let bulk_insert_ms = load_started.elapsed().as_millis();

    let params = ChainParams::new("bench", 31_337, rpc.http_url(), chain.ws_url(), vec![("USDC".into(), token)])
        .mode(&args.mode)
        .confirmations(args.confirmations)
        .extra(&format!("large_scale_mode = \"{}\"\nws_stall_ms = 600000\nsafety_sweep_interval_ms = 30000\nidle_probe_interval_ms = 60000", args.large_scale_mode));
    let boot_started = Instant::now();
    let app = TestApp::start(test_config(&[params]), pool.clone()).await;
    let boot_ms = boot_started.elapsed().as_millis();
    let view = app
        .wait_chain("bench", "ingest mode settled", Duration::from_secs(120), |c| {
            c["active_watches"] == args.watches
                && c["health"]["leader"] == true
                && matches!(c["health"]["ingest_mode"].as_str(), Some("ws_targeted" | "ws_firehose" | "poll"))
        })
        .await;
    let mode = view["health"]["ingest_mode"].as_str().unwrap_or("?").to_owned();
    if mode.starts_with("ws") {
        app.wait_chain("bench", "WSS connected", Duration::from_secs(120), |c| c["health"]["ws_connected"] == true)
            .await;
        tokio::time::sleep(Duration::from_millis(1500)).await; // let every bucket subscribe
    }
    println!(
        "  hydrated {} watches: service ready in {boot_ms} ms (bulk insert took {bulk_insert_ms} ms); ingest mode: {mode}",
        args.watches
    );
    let calls_before = rpc.call_counts();

    // Drive load.
    let mut sent: HashMap<Address, Instant> = HashMap::with_capacity(args.payments as usize);
    let gap = Duration::from_secs_f64(1.0 / args.rate.max(1) as f64);
    let run_started = Instant::now();
    let mut next = Instant::now();
    for i in 1..=args.payments {
        tokio::time::sleep_until(next.into()).await;
        next += gap;
        if args.noise > 0 {
            chain
                .mint_batch_nowait(token, (0..args.noise).map(|_| fresh_address()).collect(), U256::from(1_000_000u64))
                .await;
        }
        // Clock starts before the transaction is even submitted, so the figures below are upper bounds that
        // include Anvil accepting, executing and mining it.
        let payee = watched_address(i);
        sent.insert(payee, Instant::now());
        chain.mint_nowait(token, payee, U256::from(1_000_000u64)).await;
    }
    // Sends are fire-and-forget: make sure the last payment is actually mined before adding confirmations.
    let last = watched_address(args.payments);
    while chain.balance_of(token, last).await.is_zero() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let send_secs = run_started.elapsed().as_secs_f64();
    chain.mine(args.confirmations + 1).await;

    let expected = args.payments as usize;
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline
        && sink.received().iter().filter(|r| r.payload.event_type.as_str() == "threshold.reached").count() < expected
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let total_secs = run_started.elapsed().as_secs_f64();

    // Analyse.
    let mut pending = Histogram::<u64>::new(3)?;
    let mut confirmed = Histogram::<u64>::new(3)?;
    let mut threshold = Histogram::<u64>::new(3)?;
    let mut counts: HashMap<&str, u64> = HashMap::new();
    let mut ids = std::collections::HashSet::new();
    let mut duplicates = 0u64;
    for r in sink.received() {
        if !ids.insert(r.payload.id) {
            duplicates += 1;
            continue;
        }
        let kind = r.payload.event_type.as_str();
        *counts.entry(kind).or_default() += 1;
        let Some(t0) = sent.get(&r.payload.watch.payment_address) else { continue };
        let micros = r.at.saturating_duration_since(*t0).as_micros() as u64;
        match kind {
            "payment.pending" => pending.record(micros)?,
            "payment.confirmed" => confirmed.record(micros)?,
            "threshold.reached" => threshold.record(micros)?,
            _ => {}
        }
    }

    println!("\nlatency, transaction submitted → webhook received (automine: submitted ≈ mined):");
    let pending_json = summarize("payment.pending", &pending);
    let confirmed_json = summarize(&format!("payment.confirmed ({} conf)", args.confirmations), &confirmed);
    let threshold_json = summarize("threshold.reached", &threshold);

    let calls_after = rpc.call_counts();
    let mut calls: HashMap<String, u64> = HashMap::new();
    for (method, n) in &calls_after {
        let delta = n - calls_before.get(method).copied().unwrap_or(0);
        if delta > 0 {
            calls.insert(method.clone(), delta);
        }
    }
    let http_calls: u64 = calls.values().sum();
    let per_payment = http_calls as f64 / args.payments as f64;
    let logs_total = args.payments * (args.noise as u64 + 1);
    println!(
        "\nthroughput: {:.0} payments/s offered, {:.0} transfer logs/s on the token, all events delivered in {:.1}s",
        args.payments as f64 / send_secs,
        logs_total as f64 / send_secs,
        total_secs
    );
    println!("events: {counts:?}, duplicates delivered: {duplicates}");
    println!("HTTP RPC calls during the run: {calls:?}");
    println!(
        "  → {per_payment:.2} calls per payment ≈ {:.0} QuickNode credits per payment at 20/call (WSS notifications not included)",
        per_payment * 20.0
    );

    let missed = expected as u64 - counts.get("threshold.reached").copied().unwrap_or(0);
    if missed > 0 {
        let done: std::collections::HashSet<Address> = sink
            .received()
            .iter()
            .filter(|r| r.payload.event_type.as_str() == "threshold.reached")
            .map(|r| r.payload.watch.payment_address)
            .collect();
        for payee in sent.keys().filter(|a| !done.contains(*a)).take(5) {
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "SELECT t.status, t.block_number FROM transfers t JOIN watches w ON w.id = t.watch_id WHERE w.payment_address = $1",
            )
            .bind(payee.as_slice())
            .fetch_all(&pool)
            .await?;
            let on_chain = chain.balance_of(token, *payee).await;
            let cursor = store::cursor(&pool, 31_337).await?;
            println!(
                "  MISSING {payee}: on-chain balance {on_chain}, transfers in db {rows:?}, cursor {cursor}, head {}",
                chain.head().await
            );
        }
    }
    let report = serde_json::json!({
        "args": { "watches": args.watches, "payments": args.payments, "rate": args.rate, "noise": args.noise,
                  "mode": mode, "confirmations": args.confirmations },
        "boot_ms_with_hydration": boot_ms,
        "latency": { "pending": pending_json, "confirmed": confirmed_json, "threshold_reached": threshold_json },
        "events": counts, "duplicates": duplicates, "missed_payments": missed,
        "rpc_calls": calls, "rpc_calls_per_payment": per_payment,
        "offered_payments_per_sec": args.payments as f64 / send_secs,
        "token_logs_per_sec": logs_total as f64 / send_secs,
    });
    if let Some(path) = &args.out {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
        println!("report written to {}", path.display());
    }

    app.shutdown().await;
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)")))
        .execute(&mut admin)
        .await?;
    admin.close().await?;
    anyhow::ensure!(
        missed == 0 && duplicates == 0,
        "benchmark detected {missed} missed payments and {duplicates} duplicate events"
    );
    Ok(())
}
