//! Local mode: three Anvil chains that mimic Monad, Arbitrum and Base, with mock stablecoins.
//!
//!   docker compose up -d postgres
//!   cargo run --features testkit --bin gum-devnet            # terminal 1: chains + webhook sink
//!   GUM_PROFILE=local cargo run                              # terminal 2: the service (reads .env values)
//!   cargo run --features testkit --bin gum-devnet -- pay --chain base --token USDC --to 0x… --amount 2.5
//!
//! Ports 18545-18547 (chains) and 19000 (webhook sink) are used so a stock Anvil on 8545 is left alone.

use std::{collections::BTreeMap, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{Address, U256},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};
use clap::{Parser, Subcommand};
use gum_indexer::testkit::chain::{MockERC20, TestChain};

const CHAINS: [(&str, u64, u16, f64, &[&str]); 3] = [
    ("monad", 143, 18545, 1.0, &["USDC", "USDT", "AUSD"]),
    ("arbitrum", 42161, 18546, 1.0, &["USDC", "USDT"]),
    ("base", 8453, 18547, 2.0, &["USDC"]),
];
const GENERATED: &str = "config/local.generated.toml";
/// Anvil's first default account (publicly known test key).
const DEV_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

#[derive(Parser)]
#[command(about = "gum-indexer local devnet")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the chains, deploy tokens, write config/local.generated.toml and print received webhooks (default).
    Up,
    /// Send a payment on a running devnet.
    Pay {
        #[arg(long)]
        chain: String,
        #[arg(long, default_value = "USDC")]
        token: String,
        #[arg(long)]
        to: Address,
        /// Human units, e.g. 2.5
        #[arg(long)]
        amount: f64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command.unwrap_or(Command::Up) {
        Command::Up => up().await,
        Command::Pay { chain, token, to, amount } => pay(&chain, &token, to, amount).await,
    }
}

async fn up() -> anyhow::Result<()> {
    let mut generated = String::from("# Written by gum-devnet. Token addresses of the running local chains.\n");
    let mut chains = Vec::new();
    for (name, chain_id, port, block_time, tokens) in CHAINS {
        let chain = TestChain::start_on(chain_id, Some(block_time), Some(port)).await;
        println!("{name:<9} chain_id={chain_id:<6} http://127.0.0.1:{port}  block time {block_time}s");
        for symbol in tokens {
            let address = chain.deploy_token(symbol).await;
            println!("            {symbol:<5} {address}");
            generated.push_str(&format!(
                "[[chains.{name}.tokens]]\nsymbol = \"{symbol}\"\naddress = \"{address}\"\ndecimals = 6\nissuance = \"mock\"\n"
            ));
        }
        chains.push(chain);
    }
    std::fs::write(GENERATED, generated)?;
    println!("\nwrote {GENERATED}");

    let sink = gum_indexer::testkit::sink::WebhookSink::start_on(19000).await;
    println!("webhook sink listening on {} (use it as webhook_endpoint)\n", sink.url());
    println!(
        "start the service:  GUM_PROFILE=local DATABASE_URL=postgres://gum:gum@localhost:54329/gum GUM_API__KEYS=dev-key GUM_WEBHOOK__SECRET=dev cargo run"
    );
    println!(
        "register a watch:   curl -s localhost:8080/v1/watches -H 'authorization: Bearer dev-key' -H 'content-type: application/json' \\"
    );
    println!(
        "                      -d '{{\"payment_address\":\"0x000000000000000000000000000000000000dEaD\",\"chain\":\"base\",\"token\":\"USDC\",\"balance_threshold\":\"5000000\",\"webhook_endpoint\":\"{}\"}}'",
        sink.url()
    );
    println!(
        "pay it:             cargo run --features testkit --bin gum-devnet -- pay --chain base --to 0x000000000000000000000000000000000000dEaD --amount 2.5\n"
    );

    let mut seen = 0;
    // SIGTERM as well as Ctrl-C: the Anvil children are only stopped when the chains are dropped.
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        let received = sink.received();
        for r in &received[seen..] {
            let p = &r.payload;
            let amount = p.transfer.as_ref().map(|t| t.amount.to_string()).unwrap_or_default();
            println!(
                "webhook ← {:<18} chain={:<9} token={:<5} to={} amount={:<12} confirmed_total={} seq={}",
                p.event_type.as_str(),
                p.watch.chain,
                p.watch.token,
                p.watch.payment_address,
                amount,
                p.watch.confirmed_amount,
                p.sequence
            );
        }
        seen = received.len();
    }
    drop(chains);
    let _ = std::fs::remove_file(GENERATED);
    Ok(())
}

async fn pay(chain: &str, token: &str, to: Address, amount: f64) -> anyhow::Result<()> {
    let (_, _, port, _, _) =
        CHAINS.iter().find(|c| c.0 == chain).ok_or_else(|| anyhow::anyhow!("unknown chain {chain}"))?;
    let generated: BTreeMap<String, toml_tokens::Chain> = toml_tokens::read(GENERATED)?;
    let address = generated
        .get(chain)
        .and_then(|c| c.tokens.iter().find(|t| t.symbol.eq_ignore_ascii_case(token)))
        .map(|t| t.address)
        .ok_or_else(|| anyhow::anyhow!("{token} is not deployed on {chain}; is `gum-devnet up` running?"))?;
    let signer: PrivateKeySigner = DEV_KEY.parse()?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(format!("http://127.0.0.1:{port}").parse()?);
    let units = U256::from((amount * 1_000_000.0).round() as u128);
    let receipt = MockERC20::new(address, provider).mint(to, units).send().await?.get_receipt().await?;
    println!(
        "paid {amount} {token} ({units} base units) to {to} on {chain}: tx {} in block {}",
        receipt.transaction_hash,
        receipt.block_number.unwrap_or_default()
    );
    Ok(())
}

mod toml_tokens {
    use std::collections::BTreeMap;

    use alloy::primitives::Address;
    use figment::providers::Format;
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Token {
        pub symbol: String,
        pub address: Address,
    }

    #[derive(Deserialize)]
    pub struct Chain {
        pub tokens: Vec<Token>,
    }

    #[derive(Deserialize)]
    struct Root {
        chains: BTreeMap<String, Chain>,
    }

    pub fn read(path: &str) -> anyhow::Result<BTreeMap<String, Chain>> {
        let root: Root = figment::Figment::new().merge(figment::providers::Toml::file(path)).extract()?;
        Ok(root.chains)
    }
}
