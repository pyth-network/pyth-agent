// The Global Store stores a copy of all the product and price information held in the Pyth
// on-chain aggregation contracts, across both the primary and secondary networks.
// This enables this data to be easily queried by other components.
use {
    super::State,
    crate::agent::{
        metrics::{
            PriceGlobalMetrics,
            ProductGlobalMetrics,
        },
        solana::{
            network::Network,
            oracle::{
                self,
                PriceEntry,
                ProductEntry,
            },
        },
        state::StateApi,
    },
    anyhow::{
        anyhow,
        Result,
    },
    prometheus_client::registry::Registry,
    pyth_sdk::Identifier,
    slog::Logger,
    solana_sdk::pubkey::Pubkey,
    std::collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    tokio::sync::RwLock,
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

pub struct Store {
    /// The actual data on primary network
    account_data_primary: RwLock<AllAccountsData>,

    /// The actual data on secondary network
    /// This data is not necessarily consistent across both networks, so we need to store it
    /// separately.
    account_data_secondary: RwLock<AllAccountsData>,

    /// The account metadata for both networks
    /// The metadata is consistent across both networks, so we only need to store it once.
    account_metadata: RwLock<AllAccountsMetadata>,

    /// Prometheus metrics for products
    product_metrics: ProductGlobalMetrics,

    /// Prometheus metrics for prices
    price_metrics: PriceGlobalMetrics,

    /// Shared logger configuration.
    logger: Logger,
}

impl Store {
    pub fn new(logger: Logger, registry: &mut Registry) -> Self {
        Store {
            account_data_primary: Default::default(),
            account_data_secondary: Default::default(),
            account_metadata: Default::default(),
            product_metrics: ProductGlobalMetrics::new(registry),
            price_metrics: PriceGlobalMetrics::new(registry),
            logger,
        }
    }
}

#[cfg(test)]
impl Store {
    // Allow Setting Fields during Tests.
    pub async fn _account_data_primary(&self, data: AllAccountsData) {
        *self.account_data_primary.write().await = data;
    }

    pub async fn _account_data_secondary(&self, data: AllAccountsData) {
        *self.account_data_secondary.write().await = data;
    }

    pub async fn _account_metadata(&self, data: AllAccountsMetadata) {
        *self.account_metadata.write().await = data;
    }
}

#[async_trait::async_trait]
pub trait GlobalStore {
    async fn update(&self, network: Network, update: &Update) -> Result<()>;
    async fn accounts_metadata(&self) -> Result<AllAccountsMetadata>;
    async fn accounts_data(&self, network: Network) -> Result<AllAccountsData>;
    async fn price_accounts(
        &self,
        network: Network,
        price_ids: HashSet<Pubkey>,
    ) -> Result<HashMap<Pubkey, PriceEntry>>;
}

// Allow downcasting Adapter into GlobalStore for functions that depend on the `GlobalStore` service.
impl<'a> From<&'a State> for &'a Store {
    fn from(adapter: &'a State) -> &'a Store {
        &adapter.global_store
    }
}

#[async_trait::async_trait]
impl<T> GlobalStore for T
where
    for<'a> &'a T: Into<&'a Store>,
    T: StateApi,
    T: Sync,
{
    async fn update(&self, network: Network, update: &Update) -> Result<()> {
        update_data(self, network, &update).await?;
        update_metadata(self, &update).await?;
        Ok(())
    }

    async fn accounts_metadata(&self) -> Result<AllAccountsMetadata> {
        Ok(self.into().account_metadata.read().await.clone())
    }

    async fn accounts_data(&self, network: Network) -> Result<AllAccountsData> {
        match network {
            Network::Primary => Ok(self.into().account_data_primary.read().await.clone()),
            Network::Secondary => Ok(self.into().account_data_secondary.read().await.clone()),
        }
    }

    async fn price_accounts(
        &self,
        network: Network,
        price_ids: HashSet<Pubkey>,
    ) -> Result<HashMap<Pubkey, PriceEntry>> {
        let account_data = match network {
            Network::Primary => &self.into().account_data_primary,
            Network::Secondary => &self.into().account_data_secondary,
        }
        .read()
        .await;

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
            .collect()
    }
}

async fn update_data<S>(state: &S, network: Network, update: &Update) -> Result<()>
where
    S: StateApi,
    for<'a> &'a S: Into<&'a Store>,
{
    let store: &Store = state.into();

    // Choose the right account data to update
    let account_data = match network {
        Network::Primary => &store.account_data_primary,
        Network::Secondary => &store.account_data_secondary,
    };

    match update {
        Update::ProductAccountUpdate {
            account_key,
            account,
        } => {
            let attr_dict = ProductAccountMetadata::from(account.clone()).attr_dict;
            let maybe_symbol = attr_dict.get("symbol").cloned();
            store.product_metrics.update(account_key, maybe_symbol);

            // Update the stored data
            account_data
                .write()
                .await
                .product_accounts
                .insert(*account_key, account.clone());
        }
        Update::PriceAccountUpdate {
            account_key,
            account,
        } => {
            // Sanity-check that we are updating with more recent data
            if let Some(existing_price) = account_data.read().await.price_accounts.get(account_key)
            {
                if existing_price.timestamp > account.timestamp {
                    // This message is not an error. It is common
                    // for primary and secondary network to have
                    // slight difference in their timestamps.
                    debug!(store.logger, "Global store: ignoring stale update of an existing newer price";
                        "price_key" => account_key.to_string(),
                        "existing_timestamp" => existing_price.timestamp,
                        "new_timestamp" => account.timestamp,
                    );
                    return Ok(());
                }
            }

            // Update metrics
            store.price_metrics.update(account_key, account);

            // Update the stored data
            account_data
                .write()
                .await
                .price_accounts
                .insert(*account_key, *account);

            // Notify the Pythd API adapter that this account has changed.
            // As the account data might differ between the two networks
            // we only notify the adapter of the primary network updates.
            if let Network::Primary = network {
                StateApi::global_store_update(
                    state,
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

async fn update_metadata<S>(state: &S, update: &Update) -> Result<()>
where
    S: StateApi,
    for<'a> &'a S: Into<&'a Store>,
{
    let store: &Store = state.into();

    match update {
        Update::ProductAccountUpdate {
            account_key,
            account,
        } => {
            store
                .account_metadata
                .write()
                .await
                .product_accounts_metadata
                .insert(*account_key, account.clone().into());

            Ok(())
        }
        Update::PriceAccountUpdate {
            account_key,
            account,
        } => {
            store
                .account_metadata
                .write()
                .await
                .price_accounts_metadata
                .insert(*account_key, (*account).into());

            Ok(())
        }
    }
}
