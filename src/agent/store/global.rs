use prometheus::{
    register_counter_with_registry,
    register_gauge_with_registry,
    register_int_gauge_with_registry,
    Counter,
    Gauge,
    IntGauge,
};
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
        metrics::PROMETHEUS_REGISTRY,
        pythd::adapter,
    },
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

/// Product account global store metrics. Most fields correspond with a subset of PriceAccount fields.
pub struct ProductGlobalMetrics {
    /// Trivial dummy metric, reporting the pubkey underlying the human-readable symbol
    symbol_to_key: IntGauge,
    update_count:  Counter,
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

/// Price account global store metrics. Most fields correspond with a subset of PriceAccount fields.
pub struct PriceGlobalMetrics {
    /// Note: the exponent is not applied to this metric
    price:          IntGauge,
    expo:           IntGauge,
    conf:           Gauge,
    timestamp:      IntGauge,
    /// Note: the exponent is not applied to this metric
    prev_price:     IntGauge,
    prev_conf:      Gauge,
    prev_timestamp: IntGauge,

    /// How many times this Price was updated in the global store
    update_count: Counter,
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
        result_tx: oneshot::Sender<Result<AllAccountsData>>,
    },
}

pub struct Store {
    /// The actual data
    account_data:     AllAccountsData,
    account_metadata: AllAccountsMetadata,

    /// Prometheus metrics for products
    product_metrics: HashMap<Pubkey, ProductGlobalMetrics>,

    /// Prometheus metrics for prices
    price_metrics: HashMap<Pubkey, PriceGlobalMetrics>,

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
            product_metrics: HashMap::new(),
            price_metrics: HashMap::new(),
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
                self.update_data(&update).await?;
                self.update_metadata(&update)?;
            }
            Some(update) = self.secondary_updates_rx.recv() => {
                // We only use the secondary store to update the metadata, which is
                // the same between both networks. This is so that if one network is offline
                // we still have the metadata available to us. We don't update the data
                // itself, because the aggregate prices may diverge slightly between
                // the two networks.
                self.update_metadata(&update)?;
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
                // Update metrics
                self.update_product_metrics(account_key, account)?;

                // Update the stored data
                self.account_data
                    .product_accounts
                    .insert(*account_key, account.clone());
            }
            Update::PriceAccountUpdate {
                account_key,
                account,
            } => {
                // Sanity-check that we are updating with more recent data
                if let Some(existing_price) = self.account_data.price_accounts.get(account_key) {
                    if existing_price.timestamp > account.timestamp {
                        warn!(self.logger, "Global store: denied stale update of an existing newer price";
                        "price_key" => account_key.to_string(),
                        "existing_timestamp" => existing_price.timestamp,
                        "new_timestamp" => account.timestamp,
                                      );
                        return Ok(());
                    }
                }

                // Update metrics
                self.update_price_metrics(account_key, account)?;

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

    /// Update global store metrics for the specified product
    fn update_product_metrics(
        &mut self,
        product_key: &Pubkey,
        prod_account: &ProductEntry,
    ) -> Result<()> {
        let metadata: ProductAccountMetadata = prod_account.clone().into();

        let symbol_string = metadata
            .attr_dict
            .get("symbol")
            .cloned()
            .map(|mut sym| {
                // Remove whitespace, preventing invalid metric names
                sym.retain(|sym_char| !sym_char.is_whitespace());

                // Prometheus does not like slashes or dots
                sym.replace(".", "_dot_").replace("/", "_slash_")
            })
            // Use placeholder if the attribute was not found
            .unwrap_or(format!("unknown_{}", product_key.to_string()));

        // Denying unused var warnings and destructuring prevents
        // forgetting to use a metric field
        #[deny(unused_variables)]
        let ProductGlobalMetrics {
            symbol_to_key,
            update_count,
        } = self
            .product_metrics
            .entry(*product_key)
            .or_insert(ProductGlobalMetrics { // Instantiate if not found
                symbol_to_key: register_int_gauge_with_registry!(
                    format!(
                        "global_{}_has_pubkey_{}",
                        symbol_string,
                        product_key.to_string(),
                    ),
                    format!(
                        "Dummy metric for indicating that human-readable symbol {} corresponds with product pubkey {}. To create a valid metric name, replacements are made: '. -> _dot_, / -> _slash_'; Set to 'unknown_<pubkey>' if symbol is not defined.",
                        product_key.to_string(),
                        symbol_string
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                update_count: register_counter_with_registry!(
                    format!("global_{}_update_count", symbol_string),
                    format!(
                        "Global Store's update count since agent startup for symbol {}",
                        symbol_string
                    ),
                    PROMETHEUS_REGISTRY
                )?,
            });

        symbol_to_key.set(42);
        update_count.inc();

        Ok(())
    }

    /// Update global store metrics for the specified price
    fn update_price_metrics(
        &mut self,
        price_key: &Pubkey,
        price_account: &PriceEntry,
    ) -> Result<()> {
        // Denying unused var warnings and destructuring prevents
        // forgetting to use a metric field
        #[deny(unused_variables)]
        let PriceGlobalMetrics {
            price,
            expo,
            conf,
            timestamp,
            prev_price,
            prev_conf,
            prev_timestamp,
            update_count,
        } = self
            .price_metrics
            .entry(*price_key)
            .or_insert(PriceGlobalMetrics {
                price:          register_int_gauge_with_registry!(
                    format!("global_{}_price", price_key.to_string()),
                    format!(
                        "Global store's latest price field value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                expo:           register_int_gauge_with_registry!(
                    format!("global_{}_expo", price_key.to_string()),
                    format!(
                        "Global store's latest expo field value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                conf:           register_gauge_with_registry!(
                    format!("global_{}_conf", price_key.to_string()),
                    format!(
                        "Global store's latest confidence interval value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                timestamp:      register_int_gauge_with_registry!(
                    format!("global_{}_timestamp", price_key.to_string()),
                    format!(
                        "Global store's latest publish timestamp field value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                prev_price:     register_int_gauge_with_registry!(
                    format!("global_{}_prev_price", price_key.to_string()),
                    format!(
                        "Global store's latest prev_price field value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                prev_conf:      register_gauge_with_registry!(
                    format!("global_{}_prev_conf", price_key.to_string()),
                    format!(
                        "Global store's latest previous confidence interval value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                prev_timestamp: register_int_gauge_with_registry!(
                    format!("global_{}_prev_timestamp", price_key.to_string()),
                    format!(
                        "Global store's latest last trading timestamp value for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
                update_count:   register_counter_with_registry!(
                    format!("global_{}_update_count", price_key.to_string()),
                    format!(
                        "Global store's number of updates since agent startup for price {}",
                        price_key.to_string()
                    ),
                    PROMETHEUS_REGISTRY
                )?,
            });

        price.set(price_account.agg.price);
        expo.set(price_account.expo.into());
        conf.set(price_account.agg.conf as f64);
        timestamp.set(price_account.timestamp);

        prev_price.set(price_account.prev_price);
        prev_conf.set(price_account.prev_conf as f64);

        prev_timestamp.set(price_account.prev_timestamp);

        update_count.inc();
        Ok(())
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
