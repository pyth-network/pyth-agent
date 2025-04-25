// The Local Store stores a copy of all the price information this local publisher
// is contributing to the network. The Exporters will then take this data and publish
// it to the networks.
use {
    super::State,
    crate::agent::metrics::PriceLocalMetrics,
    anyhow::{
        Result,
        anyhow,
    },
    chrono::NaiveDateTime,
    prometheus_client::registry::Registry,
    pyth_sdk_solana::state::PriceStatus,
    solana_sdk::bs58,
    std::collections::HashMap,
    tokio::sync::RwLock,
};

#[derive(Copy, Clone, Debug)]
pub struct PriceInfo {
    pub status:    PriceStatus,
    pub price:     i64,
    pub conf:      u64,
    pub timestamp: NaiveDateTime,
}

impl PriceInfo {
    /// Returns false if any non-timestamp fields differ with `other`. Used for last published state comparison in exporter.
    pub fn cmp_no_timestamp(&self, other: &Self) -> bool {
        // Prevent forgetting to use a new field if we expand the type.
        #[deny(unused_variables)]
        let Self {
            status,
            price,
            conf,
            timestamp: _,
        } = self;

        status == &other.status && price == &other.price && conf == &other.conf
    }
}

pub struct Store {
    prices:  RwLock<HashMap<pyth_sdk::Identifier, PriceInfo>>,
    metrics: PriceLocalMetrics,
}

impl Store {
    pub fn new(registry: &mut Registry) -> Self {
        Store {
            prices:  RwLock::new(HashMap::new()),
            metrics: PriceLocalMetrics::new(registry),
        }
    }
}

#[async_trait::async_trait]
pub trait LocalStore {
    async fn update(
        &self,
        price_identifier: pyth_sdk::Identifier,
        price_info: PriceInfo,
    ) -> Result<()>;
    async fn get_all_price_infos(&self) -> HashMap<pyth_sdk::Identifier, PriceInfo>;
}

// Allow downcasting State into GlobalStore for functions that depend on the `GlobalStore` service.
impl<'a> From<&'a State> for &'a Store {
    fn from(state: &'a State) -> &'a Store {
        &state.local_store
    }
}

#[async_trait::async_trait]
impl<T> LocalStore for T
where
    for<'a> &'a T: Into<&'a Store>,
    T: Sync,
{
    async fn update(
        &self,
        price_identifier: pyth_sdk::Identifier,
        price_info: PriceInfo,
    ) -> Result<()> {
        tracing::debug!(
            identifier = bs58::encode(price_identifier.to_bytes()).into_string(),
            price_update = ?price_info,
            "Local store received price update."
        );

        // Drop the update if it is older than the current one stored for the price
        if let Some(current_price_info) = self.into().prices.read().await.get(&price_identifier) {
            if current_price_info.timestamp > price_info.timestamp {
                return Err(anyhow!(
                    "Received stale timestamp for price {}",
                    price_identifier
                ));
            }
        }

        self.into().metrics.update(&price_identifier, &price_info);
        self.into()
            .prices
            .write()
            .await
            .insert(price_identifier, price_info);

        Ok(())
    }

    async fn get_all_price_infos(&self) -> HashMap<pyth_sdk::Identifier, PriceInfo> {
        self.into().prices.read().await.clone()
    }
}
