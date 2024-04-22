// The Global Store stores a copy of all the product and price information held in the Pyth
// on-chain aggregation contracts, across both the primary and secondary networks.
// This enables this data to be easily queried by other components.
use {
    super::super::solana::oracle::{
        self,
        PriceEntry,
        ProductEntry,
    },
    crate::agent::{
        metrics::{
            PriceGlobalMetrics,
            ProductGlobalMetrics,
            PROMETHEUS_REGISTRY,
        },
        pythd::adapter::AdapterApi,
        solana::network::Network,
    },
    anyhow::{
        anyhow,
        Result,
    },
    pyth_sdk::Identifier,
    slog::Logger,
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{
            BTreeMap,
            HashMap,
            HashSet,
        },
        sync::Arc,
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
    pub product_accounts: HashMap<Pubkey, oracle::ProductEntry>,
    pub price_accounts:   HashMap<Pubkey, oracle::PriceEntry>,
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

impl From<oracle::ProductEntry> for ProductAccountMetadata {
    fn from(product_account: oracle::ProductEntry) -> Self {
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

impl From<oracle::PriceEntry> for PriceAccountMetadata {
    fn from(price_account: oracle::PriceEntry) -> Self {
        PriceAccountMetadata {
            expo: price_account.expo,
        }
    }
}

#[derive(Debug)]
pub enum Update {
    ProductAccountUpdate {
        account_key: Pubkey,
        account:     ProductEntry,
    },
    PriceAccountUpdate {
        account_key: Pubkey,
        account:     PriceEntry,
    },
}

#[derive(Debug)]
pub enum Lookup {
    LookupAllAccountsMetadata {
        result_tx: oneshot::Sender<Result<AllAccountsMetadata>>,
    },
    LookupAllAccountsData {
        network:   Network,
        result_tx: oneshot::Sender<Result<AllAccountsData>>,
    },
    LookupPriceAccounts {
        network:   Network,
        price_ids: HashSet<Pubkey>,
        result_tx: oneshot::Sender<Result<HashMap<Pubkey, PriceEntry>>>,
    },
}

pub struct Store<S> {
    /// The actual data on primary network
    account_data_primary: AllAccountsData,

    /// The actual data on secondary network
    /// This data is not necessarily consistent across both networks, so we need to store it
    /// separately.
    account_data_secondary: AllAccountsData,

    /// The account metadata for both networks
    /// The metadata is consistent across both networks, so we only need to store it once.
    account_metadata: AllAccountsMetadata,

    /// Prometheus metrics for products
    product_metrics: ProductGlobalMetrics,

    /// Prometheus metrics for prices
    price_metrics: PriceGlobalMetrics,

    /// Channel on which lookup requests are received
    lookup_rx: mpsc::Receiver<Lookup>,

    /// Channel on which account updates are received from the primary network
    primary_updates_rx: mpsc::Receiver<Update>,

    /// Channel on which account updates are received from the secondary network
    secondary_updates_rx: mpsc::Receiver<Update>,

    /// Reference the Pythd Adapter.
    pythd_adapter: Arc<S>,

    logger: Logger,
}

pub fn spawn_store<S>(
    lookup_rx: mpsc::Receiver<Lookup>,
    primary_updates_rx: mpsc::Receiver<Update>,
    secondary_updates_rx: mpsc::Receiver<Update>,
    pythd_adapter: Arc<S>,
    logger: Logger,
) -> JoinHandle<()>
where
    S: AdapterApi,
    S: Send,
    S: Sync,
    S: 'static,
{
    tokio::spawn(async move {
        Store::new(
            lookup_rx,
            primary_updates_rx,
            secondary_updates_rx,
            pythd_adapter,
            logger,
        )
        .await
        .run()
        .await
    })
}

impl<S> Store<S>
where
    S: AdapterApi,
    S: Send,
    S: Sync,
{
    pub async fn new(
        lookup_rx: mpsc::Receiver<Lookup>,
        primary_updates_rx: mpsc::Receiver<Update>,
        secondary_updates_rx: mpsc::Receiver<Update>,
        pythd_adapter: Arc<S>,
        logger: Logger,
    ) -> Self {
        let prom_registry_ref = &mut &mut PROMETHEUS_REGISTRY.lock().await;

        Store {
            account_data_primary: Default::default(),
            account_data_secondary: Default::default(),
            account_metadata: Default::default(),
            product_metrics: ProductGlobalMetrics::new(prom_registry_ref),
            price_metrics: PriceGlobalMetrics::new(prom_registry_ref),
            lookup_rx,
            primary_updates_rx,
            secondary_updates_rx,
            pythd_adapter,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            if let Err(err) = self.handle_next().await {
                error!(self.logger, "{}", err);
                debug!(self.logger, "error context"; "context" => format!("{:?}", err));
            }
        }
    }

    async fn handle_next(&mut self) -> Result<()> {
        tokio::select! {
            Some(update) = self.primary_updates_rx.recv() => {
                self.update_data(Network::Primary, &update).await?;
                self.update_metadata(&update)?;
            }
            Some(update) = self.secondary_updates_rx.recv() => {
                self.update_data(Network::Secondary, &update).await?;
                self.update_metadata(&update)?;
            }
            Some(lookup) = self.lookup_rx.recv() => {
                self.handle_lookup(lookup).await?
            }
        };

        Ok(())
    }

    async fn update_data(&mut self, network: Network, update: &Update) -> Result<()> {
        // Choose the right account data to update
        let account_data = match network {
            Network::Primary => &mut self.account_data_primary,
            Network::Secondary => &mut self.account_data_secondary,
        };

        match update {
            Update::ProductAccountUpdate {
                account_key,
                account,
            } => {
                let attr_dict = ProductAccountMetadata::from(account.clone()).attr_dict;

                let maybe_symbol = attr_dict.get("symbol").cloned();

                self.product_metrics.update(account_key, maybe_symbol);

                // Update the stored data
                account_data
                    .product_accounts
                    .insert(*account_key, account.clone());
            }
            Update::PriceAccountUpdate {
                account_key,
                account,
            } => {
                // Sanity-check that we are updating with more recent data
                if let Some(existing_price) = account_data.price_accounts.get(account_key) {
                    if existing_price.timestamp > account.timestamp {
                        // This message is not an error. It is common
                        // for primary and secondary network to have
                        // slight difference in their timestamps.
                        debug!(self.logger, "Global store: ignoring stale update of an existing newer price";
                        "price_key" => account_key.to_string(),
                        "existing_timestamp" => existing_price.timestamp,
                        "new_timestamp" => account.timestamp,
                                      );
                        return Ok(());
                    }
                }

                // Update metrics
                self.price_metrics.update(account_key, account);

                // Update the stored data
                account_data.price_accounts.insert(*account_key, *account);

                // Notify the Pythd API adapter that this account has changed.
                // As the account data might differ between the two networks
                // we only notify the adapter of the primary network updates.
                if let Network::Primary = network {
                    self.pythd_adapter
                        .global_store_update(
                            Identifier::new(account_key.to_bytes()),
                            account.agg.price,
                            account.agg.conf,
                            account.agg.status,
                            account.valid_slot,
                            account.agg.pub_slot,
                        )
                        .await
                        .map_err(|_| anyhow!("failed to notify pythd adapter of account update"))?;
                }
            }
        }

        Ok(())
    }

    fn update_metadata(&mut self, update: &Update) -> Result<()> {
        match update {
            Update::ProductAccountUpdate {
                account_key,
                account,
            } => {
                self.account_metadata
                    .product_accounts_metadata
                    .insert(*account_key, account.clone().into());

                Ok(())
            }
            Update::PriceAccountUpdate {
                account_key,
                account,
            } => {
                self.account_metadata
                    .price_accounts_metadata
                    .insert(*account_key, (*account).into());

                Ok(())
            }
        }
    }

    async fn handle_lookup(&self, lookup: Lookup) -> Result<()> {
        match lookup {
            Lookup::LookupAllAccountsMetadata { result_tx } => result_tx
                .send(Ok(self.account_metadata.clone()))
                .map_err(|_| anyhow!("failed to send metadata to pythd adapter")),
            Lookup::LookupAllAccountsData { network, result_tx } => result_tx
                .send(Ok(match network {
                    Network::Primary => self.account_data_primary.clone(),
                    Network::Secondary => self.account_data_secondary.clone(),
                }))
                .map_err(|_| anyhow!("failed to send data to pythd adapter")),
            Lookup::LookupPriceAccounts {
                network,
                price_ids,
                result_tx,
            } => {
                let account_data = match network {
                    Network::Primary => &self.account_data_primary,
                    Network::Secondary => &self.account_data_secondary,
                };

                result_tx
                    .send(
                        price_ids
                            .into_iter()
                            .map(|id| {
                                account_data
                                    .price_accounts
                                    .get(&id)
                                    .cloned()
                                    .map(|v| (id, v))
                                    .ok_or(anyhow!("price id not found"))
                            })
                            .collect::<Result<HashMap<_, _>>>(),
                    )
                    .map_err(|_| anyhow!("failed to send price accounts data"))
            }
        }
    }
}
