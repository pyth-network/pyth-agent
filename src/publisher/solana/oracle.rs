// This module is responsible for loading the current state of the
// on-chain Oracle program accounts from Solana.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use pyth_sdk_solana::state::{load_mapping_account, load_price_account, load_product_account};
use slog::Logger;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_shadow::{BlockchainShadow, SyncOptions};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Interval;

use crate::publisher::store::global;

// Oracle is responsible for loading data stored in the Oracle.
pub struct Oracle {
    config: Config,

    // The actual data
    data: Data,

    // The RPC client to use to poll data from the RPC node
    // Also pass in a websocket client to use for "get account data" if
    // websocket data is found.
    rpc_client: RpcClient,

    // The interval with which to poll for data
    poll_interval: Interval,

    // Channel on which account updates are received from the subscriber
    updates_rx: mpsc::Receiver<(Pubkey, solana_sdk::account::Account)>,

    // Channel on which to communicate to the global store
    global_store_tx: mpsc::Sender<global::Message>,

    logger: Logger,
}

pub struct Config {
    pub commitment: CommitmentLevel,
    pub oracle_account_key: Pubkey,
    pub mapping_account_key: Pubkey,
}

#[derive(Default, Debug, Clone)]
pub struct Data {
    pub mapping_accounts: MappingAccounts,
    pub product_accounts: ProductAccounts,
    pub price_accounts: PriceAccounts,
}

impl Data {
    pub fn new() -> Self {
        Data {
            mapping_accounts: HashMap::new(),
            product_accounts: HashMap::new(),
            price_accounts: HashMap::new(),
        }
    }
}

pub type MappingAccounts = HashMap<Pubkey, MappingAccount>;
pub type ProductAccounts = HashMap<Pubkey, ProductAccount>;
pub type PriceAccounts = HashMap<Pubkey, PriceAccount>;

pub type MappingAccount = pyth_sdk_solana::state::MappingAccount;
#[derive(Debug, Clone)]
pub struct ProductAccount {
    pub account_data: pyth_sdk_solana::state::ProductAccount,
    pub price_accounts: Vec<Pubkey>,
}
pub type PriceAccount = pyth_sdk_solana::state::PriceAccount;

impl Oracle {
    pub fn new(
        config: Config,
        global_store_tx: mpsc::Sender<global::Message>,
        poll_interval_duration: Duration,
        rpc_url: &str,
        updates_rx: mpsc::Receiver<(Pubkey, solana_sdk::account::Account)>,
        logger: Logger,
    ) -> Self {
        let rpc_client = RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig {
                commitment: config.commitment,
            },
        );
        let poll_interval = tokio::time::interval(poll_interval_duration);

        Oracle {
            config,
            data: Default::default(),
            global_store_tx,
            rpc_client,
            poll_interval,
            updates_rx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            if let Err(err) = self.handle_next().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    async fn handle_next(&mut self) -> Result<()> {
        tokio::select! {
            Some((account_key, account)) = self.updates_rx.recv() => {
                self.handle_update(&account_key, &account).await?;
            }
            _ = self.poll_interval.tick() => {
                self.poll().await?;
            }
        }

        Ok(())
    }

    async fn handle_update(&mut self, account_key: &Pubkey, account: &Account) -> Result<()> {
        // We are only interested in price account updates, all other types of updates
        // will be fetched using polling.
        if !self.data.price_accounts.contains_key(account_key) {
            return Ok(());
        }

        self.handle_price_account_update(account_key, account).await
    }

    async fn handle_price_account_update(
        &mut self,
        account_key: &Pubkey,
        account: &Account,
    ) -> Result<()> {
        let price_account = *load_price_account(&account.data)?;
        self.data
            .price_accounts
            .insert(account_key.clone(), price_account);
        self.notify_price_account_update(account_key, &price_account)
            .await
    }

    async fn poll(&mut self) -> Result<()> {
        self.fetch().await?;
        self.notify_global_store().await?;

        Ok(())
    }

    async fn fetch(&mut self) -> Result<()> {
        self.data.mapping_accounts = self.fetch_mapping_accounts().await?;
        self.data.product_accounts = self.fetch_product_accounts().await?;
        self.data.price_accounts = self.fetch_price_accounts().await?;

        Ok(())
    }

    async fn fetch_mapping_accounts(&self) -> Result<MappingAccounts> {
        let mut accounts = HashMap::new();

        let mut account_key = self.config.mapping_account_key;
        while account_key != Pubkey::default() {
            let account =
                *load_mapping_account(&self.rpc_client.get_account_data(&account_key).await?)?;
            accounts.insert(account_key, account);

            account_key = account.next;
        }

        Ok(accounts)
    }

    async fn fetch_product_accounts(&self) -> Result<ProductAccounts> {
        let mut product_accounts = HashMap::new();

        for mapping_account in self.data.mapping_accounts.values() {
            product_accounts.extend(
                self.fetch_product_accounts_from_mapping_account(mapping_account)
                    .await?,
            );
        }

        Ok(product_accounts)
    }

    async fn fetch_price_accounts(&self) -> Result<PriceAccounts> {
        let mut price_accounts = HashMap::new();

        for product_account in self.data.product_accounts.values() {
            for price_account_key in &product_account.price_accounts {
                let price_account = self.fetch_price_account(price_account_key).await?;
                price_accounts.insert(price_account_key.clone(), price_account);
            }
        }

        Ok(price_accounts)
    }

    async fn fetch_product_accounts_from_mapping_account(
        &self,
        mapping_account: &MappingAccount,
    ) -> Result<ProductAccounts> {
        let mut product_accounts = HashMap::new();

        for account_key in &mapping_account.products {
            // Update the price accounts
            let product_account = self.fetch_product_account(account_key).await?;
            product_accounts.insert(account_key.clone(), product_account);
        }

        Ok(product_accounts)
    }

    async fn fetch_product_account(&self, product_account_key: &Pubkey) -> Result<ProductAccount> {
        // Fetch the product account
        let product_account = *load_product_account(
            &self
                .rpc_client
                .get_account_data(product_account_key)
                .await?,
        )?;

        // Fetch the price accounts
        let price_accounts = self
            .fetch_price_accounts_from_product_account(product_account.px_acc)
            .await?;

        // Create the product account object
        let product_account = ProductAccount {
            account_data: product_account,
            price_accounts: price_accounts.keys().cloned().collect(),
        };

        Ok(product_account)
    }

    async fn fetch_price_accounts_from_product_account(
        &self,
        first_price_account_key: Pubkey,
    ) -> Result<PriceAccounts> {
        let mut price_accounts = HashMap::new();

        let mut price_account_key = first_price_account_key;
        while price_account_key != Pubkey::default() {
            let price_account = self.fetch_price_account(&price_account_key).await?;
            price_accounts.insert(price_account_key, price_account);

            price_account_key = price_account.next;
        }

        Ok(price_accounts)
    }

    async fn fetch_price_account(&self, price_account_key: &Pubkey) -> Result<PriceAccount> {
        let data = self.rpc_client.get_account_data(price_account_key).await?;
        let price_account = *load_price_account(&data)?;

        Ok(price_account)
    }

    async fn notify_global_store(&mut self) -> Result<()> {
        for (product_account_key, product_account) in &self.data.product_accounts.clone() {
            self.notify_product_account_update(product_account_key, product_account)
                .await?;
        }

        for (price_account_key, price_account) in &self.data.price_accounts.clone() {
            self.notify_price_account_update(price_account_key, price_account)
                .await?;
        }

        Ok(())
    }

    async fn notify_product_account_update(
        &self,
        account_key: &Pubkey,
        account: &ProductAccount,
    ) -> Result<()> {
        self.global_store_tx
            .send(global::Message::ProductAccountUpdate {
                account_key: account_key.clone(),
                account: account.clone(),
            })
            .await
            .map_err(|_| anyhow!("failed to notify product account update"))
    }

    async fn notify_price_account_update(
        &self,
        account_key: &Pubkey,
        account: &PriceAccount,
    ) -> Result<()> {
        self.global_store_tx
            .send(global::Message::PriceAccountUpdate {
                account_key: account_key.clone(),
                account: account.clone(),
            })
            .await
            .map_err(|_| anyhow!("failed to notify price account update"))
    }
}

pub struct Subscriber {
    // Configuration
    commitment: CommitmentLevel,
    oracle_account_key: Pubkey,
    rpc_url: String,
    wss_url: String,

    // Channel on which changes are sent to our Oracle
    updates_tx: mpsc::Sender<(Pubkey, solana_sdk::account::Account)>,

    logger: Logger,
}

impl Subscriber {
    pub fn new(
        commitment: CommitmentLevel,
        oracle_account_key: Pubkey,
        rpc_url: &str,
        wss_url: &str,
        updates_tx: mpsc::Sender<(Pubkey, solana_sdk::account::Account)>,
        logger: Logger,
    ) -> Self {
        Subscriber {
            commitment,
            oracle_account_key,
            rpc_url: rpc_url.to_string(),
            wss_url: wss_url.to_string(),
            updates_tx,
            logger,
        }
    }

    pub async fn run(&self) {
        match self.start_shadow().await {
            Ok(mut shadow_rx) => self.forward_updates(&mut shadow_rx).await,
            Err(err) => error!(self.logger, "{:#}", err; "error" => format!("{:?}", err)),
        }
    }

    async fn forward_updates(&self, shadow_rx: &mut broadcast::Receiver<(Pubkey, Account)>) {
        loop {
            if let Err(err) = self.forward_update(shadow_rx).await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
            }
        }
    }

    // TODO: deal with channel closing
    async fn forward_update(
        &self,
        shadow_rx: &mut broadcast::Receiver<(Pubkey, Account)>,
    ) -> Result<()> {
        self.updates_tx
            .send(shadow_rx.recv().await?)
            .await
            .map_err(|_| anyhow!("failed to forward update"))
    }

    pub async fn start_shadow(
        &self,
    ) -> Result<broadcast::Receiver<(Pubkey, solana_sdk::account::Account)>> {
        let shadow = BlockchainShadow::new_for_program(
            &self.oracle_account_key,
            SyncOptions {
                network: solana_shadow::Network::Custom(self.rpc_url.clone(), self.wss_url.clone()),
                commitment: self.commitment,
                ..SyncOptions::default()
            },
        )
        .await?;

        Ok(shadow.updates_channel())
    }
}
