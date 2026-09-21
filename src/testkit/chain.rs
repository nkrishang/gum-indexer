//! A local Anvil chain with mock stablecoins.

use alloy::{
    network::EthereumWallet,
    node_bindings::{Anvil, AnvilInstance},
    primitives::{Address, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder, ext::AnvilApi},
    rpc::types::anvil::ReorgOptions,
    signers::local::PrivateKeySigner,
    sol,
};

sol!(
    #[sol(rpc)]
    MockERC20,
    "contracts/MockERC20.json"
);

pub struct TestChain {
    pub chain_id: u64,
    pub deployer: Address,
    provider: std::sync::RwLock<DynProvider>,
    signer: PrivateKeySigner,
    anvil: AnvilInstance,
}

#[derive(Debug, Clone, Copy)]
pub struct Mined {
    pub tx_hash: B256,
    pub block_number: u64,
    pub block_hash: B256,
}

impl TestChain {
    /// `block_time = None` → automine: every transaction is its own block and tests mine confirmations explicitly.
    pub async fn start(chain_id: u64, block_time: Option<f64>) -> Self {
        Self::start_on(chain_id, block_time, None).await
    }

    /// Like [`Self::start`] but on a fixed port (used by `gum-devnet`, whose URLs live in config/local.toml).
    pub async fn start_on(chain_id: u64, block_time: Option<f64>, port: Option<u16>) -> Self {
        let mut anvil = Anvil::new().chain_id(chain_id);
        if let Some(port) = port {
            anvil = anvil.port(port);
        }
        if let Some(bt) = block_time {
            anvil = anvil.block_time_f64(bt);
        }
        let anvil = anvil
            .try_spawn()
            .unwrap_or_else(|e| panic!("could not start anvil (is Foundry installed and port {port:?} free?): {e}"));
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let deployer = signer.address();
        let provider = std::sync::RwLock::new(Self::connect(&anvil, &signer));
        Self { chain_id, provider, deployer, signer, anvil }
    }

    fn connect(anvil: &AnvilInstance, signer: &PrivateKeySigner) -> DynProvider {
        ProviderBuilder::new().wallet(EthereumWallet::from(signer.clone())).connect_http(anvil.endpoint_url()).erased()
    }

    pub fn provider(&self) -> DynProvider {
        self.provider.read().unwrap().clone()
    }

    pub fn http_url(&self) -> String {
        self.anvil.endpoint()
    }

    pub fn ws_url(&self) -> String {
        self.anvil.ws_endpoint()
    }

    pub fn port(&self) -> u16 {
        self.anvil.port()
    }

    pub async fn deploy_token(&self, symbol: &str) -> Address {
        let token = MockERC20::deploy(self.provider(), format!("Mock {symbol}"), symbol.to_string(), 6)
            .await
            .expect("deploy mock token");
        *token.address()
    }

    /// Mints `amount` to `to`: emits `Transfer(0x0, to, amount)`.
    pub async fn mint(&self, token: Address, to: Address, amount: U256) -> Mined {
        let receipt = MockERC20::new(token, self.provider())
            .mint(to, amount)
            .send()
            .await
            .expect("send mint")
            .get_receipt()
            .await
            .expect("mint receipt");
        Mined {
            tx_hash: receipt.transaction_hash,
            block_number: receipt.block_number.expect("mined"),
            block_hash: receipt.block_hash.expect("mined"),
        }
    }

    /// One transaction, one `Transfer` log per recipient. Used to generate background noise at volume.
    pub async fn mint_batch(&self, token: Address, to: Vec<Address>, amount: U256) -> Mined {
        let receipt = MockERC20::new(token, self.provider())
            .mintBatch(to, amount)
            .gas(25_000_000)
            .send()
            .await
            .expect("send mintBatch")
            .get_receipt()
            .await
            .expect("mintBatch receipt");
        Mined {
            tx_hash: receipt.transaction_hash,
            block_number: receipt.block_number.expect("mined"),
            block_hash: receipt.block_hash.expect("mined"),
        }
    }

    /// Fire-and-forget mint for load generation: returns once the node accepted the transaction
    /// (with automine that means it is already mined), without polling for a receipt.
    pub async fn mint_nowait(&self, token: Address, to: Address, amount: U256) -> B256 {
        *MockERC20::new(token, self.provider()).mint(to, amount).gas(120_000).send().await.expect("send mint").tx_hash()
    }

    pub async fn mint_batch_nowait(&self, token: Address, to: Vec<Address>, amount: U256) -> B256 {
        *MockERC20::new(token, self.provider())
            .mintBatch(to, amount)
            .gas(25_000_000)
            .send()
            .await
            .expect("send mintBatch")
            .tx_hash()
    }

    /// A plain ERC-20 transfer from the deployer (mint to the deployer first).
    pub async fn transfer(&self, token: Address, to: Address, amount: U256) -> Mined {
        let receipt = MockERC20::new(token, self.provider())
            .transfer(to, amount)
            .send()
            .await
            .expect("send transfer")
            .get_receipt()
            .await
            .expect("transfer receipt");
        Mined {
            tx_hash: receipt.transaction_hash,
            block_number: receipt.block_number.expect("mined"),
            block_hash: receipt.block_hash.expect("mined"),
        }
    }

    pub async fn balance_of(&self, token: Address, owner: Address) -> U256 {
        MockERC20::new(token, self.provider()).balanceOf(owner).call().await.expect("balanceOf")
    }

    pub async fn mine(&self, blocks: u64) {
        self.provider().anvil_mine(Some(blocks), None).await.expect("anvil_mine");
    }

    pub async fn head(&self) -> u64 {
        self.provider().get_block_number().await.expect("eth_blockNumber")
    }

    /// Replaces the last `depth` blocks with empty ones: every transaction in them disappears.
    pub async fn reorg(&self, depth: u64) {
        self.provider().anvil_reorg(ReorgOptions { depth, tx_block_pairs: vec![] }).await.expect("anvil_reorg");
        // The removed transactions rewound the deployer's nonce; drop the wallet's cached nonce with the provider.
        *self.provider.write().unwrap() = Self::connect(&self.anvil, &self.signer);
    }
}
