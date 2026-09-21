//! Measures the provider facts the ingest configuration depends on but QuickNode does not document.
//! Costs a few credits; run it once per chain against the real endpoints and copy the results into config.
//!
//!   cargo run --release --features testkit --bin gum-qn-probe -- limits  --http-url … --ws-url … --token 0x…
//!   cargo run --release --features testkit --bin gum-qn-probe -- billing --ws-url … --token 0x… --seconds 300 --admin-key …
//!
//! `limits`  → largest `topics[2]` OR-array accepted by eth_subscribe / eth_getLogs (→ ws_bucket_size), how many
//!             subscriptions one connection holds (→ ws_targeted_max = buckets × size), max eth_getLogs block range
//!             (→ max_log_range).
//! `billing` → runs a token-wide logs subscription, counts notifications and bytes, and diffs the account's credit
//!             usage around it. Tells you whether WSS is billed per notification or per byte (→ large_scale_mode:
//!             ws_firehose if metered, poll if per-notification). Keep other traffic on the account quiet meanwhile;
//!             usage reporting can lag, so the probe waits before taking the second reading.

use std::time::{Duration, Instant};

use alloy::{
    primitives::{Address, B256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};
use clap::{Parser, Subcommand};
use gum_indexer::ingest::matcher::TRANSFER_TOPIC;

#[derive(Parser)]
#[command(about = "Probe RPC provider limits and WSS billing")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Limits {
        #[arg(long)]
        http_url: String,
        #[arg(long)]
        ws_url: String,
        /// A token contract on this chain (e.g. USDC).
        #[arg(long)]
        token: Address,
        /// Stop opening subscriptions after this many.
        #[arg(long, default_value_t = 64)]
        max_subscriptions: usize,
    },
    Billing {
        #[arg(long)]
        ws_url: String,
        #[arg(long)]
        token: Address,
        #[arg(long, default_value_t = 300)]
        seconds: u64,
        /// QuickNode Admin API key (x-api-key). Without it only notification and byte counts are reported.
        #[arg(long)]
        admin_key: Option<String>,
        /// Credits per call on this chain (20 Arbitrum/Base, 30 Monad).
        #[arg(long, default_value_t = 20)]
        credits_per_call: u64,
        /// Seconds to wait for usage reporting to settle before the second reading.
        #[arg(long, default_value_t = 120)]
        settle_seconds: u64,
    },
}

fn recipients(n: usize) -> Vec<B256> {
    (1..=n as u64)
        .map(|i| {
            let mut raw = [0u8; 20];
            raw[4..12].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
            raw[12..].copy_from_slice(&i.to_be_bytes());
            Address::from(raw).into_word()
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Limits { http_url, ws_url, token, max_subscriptions } => {
            limits(&http_url, &ws_url, token, max_subscriptions).await
        }
        Command::Billing { ws_url, token, seconds, admin_key, credits_per_call, settle_seconds } => {
            billing(&ws_url, token, seconds, admin_key, credits_per_call, settle_seconds).await
        }
    }
}

async fn limits(http_url: &str, ws_url: &str, token: Address, max_subscriptions: usize) -> anyhow::Result<()> {
    let http = ProviderBuilder::new().disable_recommended_fillers().connect_http(http_url.parse()?);
    let head = http.get_block_number().await?;
    println!("head = {head}\n");
    let base = || Filter::new().address(token).event_signature(TRANSFER_TOPIC);

    println!("eth_getLogs: recipients in topics[2] (single recent block)");
    let mut getlogs_ok = 0;
    for n in [100, 500, 1_000, 2_000, 5_000, 10_000, 20_000] {
        let filter = base().topic2(recipients(n)).from_block(head.saturating_sub(1)).to_block(head);
        let started = Instant::now();
        match http.get_logs(&filter).await {
            Ok(_) => {
                getlogs_ok = n;
                println!("  {n:>6} ok      ({:?}, request ≈ {} KB)", started.elapsed(), n * 70 / 1024);
            }
            Err(e) => {
                println!("  {n:>6} REJECTED {}", short(&e.to_string()));
                break;
            }
        }
    }

    println!("\neth_subscribe(logs): recipients in topics[2]");
    let ws = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_ws(WsConnect::new(ws_url).with_max_retries(0))
        .await?
        .erased();
    let mut subscribe_ok = 0;
    for n in [100, 500, 1_000, 2_000, 5_000, 10_000] {
        match ws.subscribe_logs(&base().topic2(recipients(n))).await {
            Ok(sub) => {
                subscribe_ok = n;
                println!("  {n:>6} ok");
                let _ = ws.unsubscribe(*sub.local_id()).await;
            }
            Err(e) => {
                println!("  {n:>6} REJECTED {}", short(&e.to_string()));
                break;
            }
        }
    }

    let bucket = subscribe_ok.clamp(100, 1_000);
    println!("\nconcurrent subscriptions on one connection ({bucket} recipients each)");
    let mut held = Vec::new();
    for i in 1..=max_subscriptions {
        match ws.subscribe_logs(&base().topic2(recipients(bucket))).await {
            Ok(sub) => held.push(sub),
            Err(e) => {
                println!("  subscription #{i} REJECTED {}", short(&e.to_string()));
                break;
            }
        }
    }
    println!("  {} subscriptions held", held.len());
    for sub in &held {
        let _ = ws.unsubscribe(*sub.local_id()).await;
    }

    println!("\neth_getLogs: block range (no recipient filter, empty-ish result: a quiet recipient)");
    let quiet = recipients(1);
    let mut range_ok = 0u64;
    for span in [100u64, 500, 1_000, 2_000, 5_000, 10_000, 20_000] {
        if span > head {
            break;
        }
        let filter = base().topic2(quiet.clone()).from_block(head - span).to_block(head);
        match http.get_logs(&filter).await {
            Ok(_) => {
                range_ok = span;
                println!("  {span:>6} blocks ok");
            }
            Err(e) => {
                println!("  {span:>6} blocks REJECTED {}", short(&e.to_string()));
                break;
            }
        }
    }

    println!("\nsuggested config for this chain:");
    println!("  ws_bucket_size  = {}", (subscribe_ok / 2).max(100));
    println!(
        "  ws_targeted_max = {}   # {} subscriptions × bucket size, keeping half the headroom",
        (subscribe_ok / 2).max(100) * (held.len() / 2).max(1),
        (held.len() / 2).max(1)
    );
    println!("  max_log_range   = {}", range_ok.max(100));
    println!("  (eth_getLogs accepted up to {getlogs_ok} recipients in one filter)");
    Ok(())
}

#[derive(serde::Deserialize)]
struct UsageEnvelope {
    data: Usage,
}

#[derive(serde::Deserialize)]
struct Usage {
    credits_used: Option<f64>,
}

async fn credits_used(client: &reqwest::Client, key: &str) -> anyhow::Result<f64> {
    let env: UsageEnvelope = client
        .get("https://api.quicknode.com/v0/usage/rpc")
        .header("x-api-key", key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    env.data.credits_used.ok_or_else(|| anyhow::anyhow!("usage response has no credits_used"))
}

async fn billing(
    ws_url: &str,
    token: Address,
    seconds: u64,
    admin_key: Option<String>,
    credits_per_call: u64,
    settle: u64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let before = match &admin_key {
        Some(key) => Some(credits_used(&client, key).await?),
        None => None,
    };
    let ws = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_ws(WsConnect::new(ws_url).with_max_retries(0))
        .await?
        .erased();
    let mut sub =
        ws.subscribe_logs(&Filter::new().address(token).event_signature(TRANSFER_TOPIC)).channel_size(65_536).await?;
    println!("subscribed to every Transfer of {token}; listening for {seconds}s …");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let (mut notifications, mut bytes, mut lagged) = (0u64, 0u64, 0u64);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            msg = sub.recv() => match msg {
                Ok(log) => {
                    notifications += 1;
                    // Payload plus the JSON-RPC notification envelope (~110 bytes).
                    bytes += serde_json::to_vec(&log).map(|v| v.len() as u64).unwrap_or(0) + 110;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => lagged += n,
                Err(_) => anyhow::bail!("subscription closed after {notifications} notifications"),
            }
        }
    }
    let _ = ws.unsubscribe(*sub.local_id()).await;
    let notifications = notifications + lagged;
    let mb = bytes as f64 / 1_048_576.0;
    let per_notification = notifications * credits_per_call;
    let metered = mb / 0.1 * 15.0;
    println!(
        "\n{notifications} notifications, ≈ {mb:.2} MB in {seconds}s ({:.1}/s)",
        notifications as f64 / seconds as f64
    );
    println!(
        "  if billed per notification: ≈ {per_notification} credits   (→ {:.0}M / month at this rate)",
        per_notification as f64 / seconds as f64 * 2_592_000.0 / 1e6
    );
    println!(
        "  if billed per 0.1 MB (15cr): ≈ {metered:.0} credits   (→ {:.1}M / month at this rate)",
        metered / seconds as f64 * 2_592_000.0 / 1e6
    );

    if let (Some(key), Some(before)) = (&admin_key, before) {
        println!("\nwaiting {settle}s for usage reporting to settle …");
        tokio::time::sleep(Duration::from_secs(settle)).await;
        let used = credits_used(&client, key).await? - before;
        println!("account credits consumed during the probe: {used:.0}");
        let verdict = if (used - per_notification as f64).abs() < (used - metered).abs() {
            "PER NOTIFICATION → large_scale_mode = \"poll\""
        } else {
            "METERED BY DATA → large_scale_mode = \"ws_firehose\""
        };
        println!("closest model: {verdict}");
        println!(
            "(if other traffic hit the account meanwhile, or the number is still 0, re-check the dashboard's usage tab later)"
        );
    } else {
        println!("\nno --admin-key given: compare the two estimates above with the dashboard's usage for this window.");
    }
    Ok(())
}

fn short(message: &str) -> String {
    message.chars().take(160).collect()
}
