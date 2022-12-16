// The Global Store stores a copy of all the product and price information held in the Pyth
// on-chain aggregation contracts, across both the primary and secondary networks.
// This enables this data to be easily queried by other components.
use {
    super::super::solana::oracle::{
        self,
        PriceAccount,
        ProductAccount,
    },
    crate::agent::pythd::adapter,
    anyhow::{
        anyhow,
        Result,
    },
    pyth_sdk::Identifier,
    slog::Logger,
    solana_sdk::pubkey::Pubkey,
    std::collections::{
        BTreeMap,
        HashMap,
    },
    tokio::{
        sync::{
            mpsc,
            oneshot,
        },
        task::JoinHandle,
    },
};

/// AllAccountsData contains the full data for the price and product accounts, sourced
/// from the primary network.
#[derive(Debug, Clone, Default)]
pub struct AllAccountsData {
    pub product_accounts: HashMap<Pubkey, oracle::ProductAccount>,
    pub price_accounts:   HashMap<Pubkey, oracle::PriceAccount>,
}

/// AllAccountsMetadata contains the metadata for all the price and product accounts.
///
/// Important: this relies on the metadata for all accounts being consistent across both networks.
#[derive(Debug, Clone, Default)]
pub struct AllAccountsMetadata {
    pub product_accounts_metadata: HashMap<Pubkey, ProductAccountMetadata>,
    pub price_accounts_metadata:   HashMap<Pubkey, PriceAccountMetadata>,
}

/// ProductAccountMetadata contains the metadata for a product account.
#[derive(Debug, Clone, Default)]
pub struct ProductAccountMetadata {
    /// Attribute dictionary
    pub attr_dict:      BTreeMap<String, String>,
    /// Price accounts associated with this product
    pub price_accounts: Vec<Pubkey>,
}

impl From<oracle::ProductAccount> for ProductAccountMetadata {
    fn from(product_account: oracle::ProductAccount) -> Self {
        ProductAccountMetadata {
            attr_dict:      product_account
                .account_data
                .iter()
                .map(|(key, val)| (key.to_owned(), val.to_owned()))
                .collect(),
            price_accounts: product_account.price_accounts,
        }
    }
}

/// PriceAccountMetadata contains the metadata for a price account.
#[derive(Debug, Clone)]
pub struct PriceAccountMetadata {
    /// Exponent
    pub expo: i32,
}

impl From<oracle::PriceAccount> for PriceAccountMetadata {
    fn from(price_account: oracle::PriceAccount) -> Self {
        PriceAccountMetadata {
            expo: price_account.expo,
        }
    }
}

#[derive(Debug)]
pub enum Update {
    ProductAccountUpdate {
        account_key: Pubkey,
        account:     ProductAccount,
    },
    PriceAccountUpdate {
        account_key: Pubkey,
        account:     PriceAccount,
    },
}

#[derive(Debug)]
pub enum Lookup {
    LookupAllAccountsMetadata {
        result_tx: oneshot::Sender<Result<AllAccountsMetadata>>,
    },
    LookupAllAccountsData {
        result_tx: oneshot::Sender<Result<AllAccountsData>>,
    },
}

pub struct Store {
    /// The actual data
    account_data:     AllAccountsData,
    account_metadata: AllAccountsMetadata,

    /// Channel on which lookup requests are received
    lookup_rx: mpsc::Receiver<Lookup>,

    /// Channel on which account updates are received from the primary network
    primary_updates_rx: mpsc::Receiver<Update>,

    /// Channel on which account updates are received from the secondary network
    secondary_updates_rx: mpsc::Receiver<Update>,

    /// Channel on which to communicate with the pythd API adapter
    pythd_adapter_tx: mpsc::Sender<adapter::Message>,

    logger: Logger,
}

pub fn spawn_store(
    lookup_rx: mpsc::Receiver<Lookup>,
    primary_updates_rx: mpsc::Receiver<Update>,
    secondary_updates_rx: mpsc::Receiver<Update>,
    pythd_adapter_tx: mpsc::Sender<adapter::Message>,
    logger: Logger,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        Store::new(
            lookup_rx,
            primary_updates_rx,
            secondary_updates_rx,
            pythd_adapter_tx,
            logger,
        )
        .run()
        .await
    })
}

impl Store {
    pub fn new(
        lookup_rx: mpsc::Receiver<Lookup>,
        primary_updates_rx: mpsc::Receiver<Update>,
        secondary_updates_rx: mpsc::Receiver<Update>,
        pythd_adapter_tx: mpsc::Sender<adapter::Message>,
        logger: Logger,
    ) -> Self {
        Store {
            account_data: Default::default(),
            account_metadata: Default::default(),
            lookup_rx,
            primary_updates_rx,
            secondary_updates_rx,
            pythd_adapter_tx,
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
            Some(update) = self.primary_updates_rx.recv() => {
                self.update_data(&update).await;
                self.update_metadata(&update).await;
            }
            Some(update) = self.secondary_updates_rx.recv() => {
                // We only use the secondary store to update the metadata, which is
                // the same between both networks. This is so that if one network is offline
                // we still have the metadata available to us. We don't update the data
                // itself, because the aggregate prices may diverge slightly between
                // the two networks.
                self.update_metadata(&update).await;
            }
            Some(lookup) = self.lookup_rx.recv() => {
                self.handle_lookup(lookup).await?
            }
        };

        Ok(())
    }

    async fn update_data(&mut self, update: &Update) -> Result<()> {
        match update {
            Update::ProductAccountUpdate {
                account_key,
                account,
            } => {
                self.account_data
                    .product_accounts
                    .insert(*account_key, account.clone());
            }
            Update::PriceAccountUpdate {
                account_key,
                account,
            } => {
                // Sanity-check that we are updating with more recent data
                if self
                    .account_data
                    .price_accounts
                    .get(account_key)
                    .map(|cur| cur.timestamp > account.timestamp)
                    .unwrap_or_default()
                {
                    return Ok(());
                }

                // Update the stored data
                self.account_data
                    .price_accounts
                    .insert(*account_key, *account);

                // Notify the Pythd API adapter that this account has changed
                self.pythd_adapter_tx
                    .send(adapter::Message::GlobalStoreUpdate {
                        price_identifier: Identifier::new(account_key.to_bytes()),
                        price:            account.agg.price,
                        conf:             account.agg.conf,
                        status:           account.agg.status,
                        valid_slot:       account.valid_slot,
                        pub_slot:         account.agg.pub_slot,
                    })
                    .await
                    .map_err(|_| anyhow!("failed to notify pythd adapter of account update"))?;
            }
        }

        Ok(())
    }

    async fn update_metadata(&mut self, update: &Update) {
        match update {
            Update::ProductAccountUpdate {
                account_key,
                account,
            } => {
                self.account_metadata
                    .product_accounts_metadata
                    .insert(*account_key, account.clone().into());
            }
            Update::PriceAccountUpdate {
                account_key,
                account,
            } => {
                self.account_metadata
                    .price_accounts_metadata
                    .insert(*account_key, (*account).into());
            }
        }
    }

    async fn handle_lookup(&self, lookup: Lookup) -> Result<()> {
        match lookup {
            Lookup::LookupAllAccountsMetadata { result_tx } => result_tx
                .send(Ok(self.account_metadata.clone()))
                .map_err(|_| anyhow!("failed to send metadata to pythd adapter")),
            Lookup::LookupAllAccountsData { result_tx } => result_tx
                .send(Ok(self.account_data.clone()))
                .map_err(|_| anyhow!("failed to send data to pythd adapter")),
        }
    }
}
