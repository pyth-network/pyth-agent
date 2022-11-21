// This module is responsible for loading the current state of the
// on-chain Oracle program accounts from Solana.

use {
    anyhow::Result,
    pyth_sdk_solana::state::{
        load_mapping_account,
        load_price_account,
        load_product_account,
    },
    slog::Logger,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::{
            CommitmentConfig,
            CommitmentLevel,
        },
        pubkey::Pubkey,
    },
    std::{
        collections::HashMap,
        time::Duration,
    },
    tokio::time::Interval,
};

#[derive(Default, Debug, Clone)]
pub struct Data {
    pub mapping_accounts: HashMap<Pubkey, MappingAccount>,
    pub product_accounts: HashMap<Pubkey, ProductAccount>,
    pub price_accounts:   HashMap<Pubkey, PriceAccount>,
}

pub type MappingAccount = pyth_sdk_solana::state::MappingAccount;
#[derive(Debug, Clone)]
pub struct ProductAccount {
    pub account_data:   pyth_sdk_solana::state::ProductAccount,
    pub price_accounts: Vec<Pubkey>,
}
pub type PriceAccount = pyth_sdk_solana::state::PriceAccount;

// Oracle is responsible for fetching Solana account data stored in the Pyth on-chain Oracle.
pub struct Oracle {
    config: Config,

    // The Solana account data
    data: Data,

    // The RPC client to use to poll data from the RPC node
    // Also pass in a websocket client to use for "get account data" if
    // websocket data is found.
    rpc_client: RpcClient,

    // The interval with which to poll for data
    poll_interval: Interval,

    logger: Logger,
}

pub struct Config {
    pub commitment:             CommitmentLevel,
    pub oracle_account_key:     Pubkey,
    pub mapping_account_key:    Pubkey,
    pub rpc_url:                String,
    pub poll_interval_duration: Duration,
}

impl Oracle {
    pub fn new(config: Config, logger: Logger) -> Self {
        let rpc_client = RpcClient::new_with_commitment(
            config.rpc_url.clone(),
            CommitmentConfig {
                commitment: config.commitment,
            },
        );
        let poll_interval = tokio::time::interval(config.poll_interval_duration);

        Oracle {
            config,
            data: Default::default(),
            rpc_client,
            poll_interval,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.poll_interval.tick().await;

            if let Err(err) = self.poll().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    async fn poll(&mut self) -> Result<()> {
        self.data.mapping_accounts = self
            .fetch_mapping_accounts(self.config.mapping_account_key)
            .await?;
        self.data.product_accounts = self
            .fetch_product_accounts(self.data.mapping_accounts.values())
            .await?;
        self.data.price_accounts = self
            .fetch_price_accounts(self.data.product_accounts.values())
            .await?;

        Ok(())
    }

    async fn fetch_mapping_accounts(
        &self,
        mapping_account_key: Pubkey,
    ) -> Result<HashMap<Pubkey, MappingAccount>> {
        let mut accounts = HashMap::new();

        let mut account_key = mapping_account_key;
        while account_key != Pubkey::default() {
            let account =
                *load_mapping_account(&self.rpc_client.get_account_data(&account_key).await?)?;
            accounts.insert(account_key, account);

            account_key = account.next;
        }

        Ok(accounts)
    }

    async fn fetch_product_accounts<'a, A>(
        &self,
        mapping_accounts: A,
    ) -> Result<HashMap<Pubkey, ProductAccount>>
    where
        A: IntoIterator<Item = &'a MappingAccount>,
    {
        let mut product_accounts = HashMap::new();

        for mapping_account in mapping_accounts {
            product_accounts.extend(
                self.fetch_product_accounts_from_mapping_account(mapping_account)
                    .await?,
            );
        }

        Ok(product_accounts)
    }

    async fn fetch_price_accounts<'a, P>(
        &self,
        product_accounts: P,
    ) -> Result<HashMap<Pubkey, PriceAccount>>
    where
        P: IntoIterator<Item = &'a ProductAccount>,
    {
        let mut price_accounts = HashMap::new();

        for product_account in product_accounts {
            for price_account_key in &product_account.price_accounts {
                let price_account = self.fetch_price_account(price_account_key).await?;
                price_accounts.insert(*price_account_key, price_account);
            }
        }

        Ok(price_accounts)
    }

    async fn fetch_product_accounts_from_mapping_account(
        &self,
        mapping_account: &MappingAccount,
    ) -> Result<HashMap<Pubkey, ProductAccount>> {
        let mut product_accounts = HashMap::new();

        for account_key in &mapping_account.products {
            // Update the price accounts
            let product_account = self.fetch_product_account(account_key).await?;
            product_accounts.insert(*account_key, product_account);
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
            account_data:   product_account,
            price_accounts: price_accounts.keys().cloned().collect(),
        };

        Ok(product_account)
    }

    async fn fetch_price_accounts_from_product_account(
        &self,
        first_price_account_key: Pubkey,
    ) -> Result<HashMap<Pubkey, PriceAccount>> {
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
}
