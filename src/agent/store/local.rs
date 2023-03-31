// The Local Store stores a copy of all the price information this local publisher
// is contributing to the network. The Exporters will then take this data and publish
// it to the networks.
use {
    super::PriceIdentifier,
    crate::agent::metrics::{
        PriceLocalMetrics,
        PROMETHEUS_REGISTRY,
    },
    anyhow::{
        anyhow,
        Result,
    },
    pyth_sdk::{
        PriceStatus,
        UnixTimestamp,
    },
    slog::Logger,
    solana_sdk::bs58,
    std::collections::HashMap,
    tokio::{
        sync::{
            mpsc,
            oneshot,
        },
        task::JoinHandle,
    },
};

#[derive(Clone, Debug)]
pub struct PriceInfo {
    pub status:    PriceStatus,
    pub price:     i64,
    pub conf:      u64,
    pub timestamp: UnixTimestamp,
}

#[derive(Debug)]
pub enum Message {
    Update {
        price_identifier: PriceIdentifier,
        price_info:       PriceInfo,
    },
    LookupAllPriceInfo {
        result_tx: oneshot::Sender<HashMap<PriceIdentifier, PriceInfo>>,
    },
}

pub fn spawn_store(rx: mpsc::Receiver<Message>, logger: Logger) -> JoinHandle<()> {
    tokio::spawn(async move { Store::new(rx, logger).await.run().await })
}

pub struct Store {
    prices:  HashMap<PriceIdentifier, PriceInfo>,
    metrics: PriceLocalMetrics,
    rx:      mpsc::Receiver<Message>,
    logger:  Logger,
}

impl Store {
    pub async fn new(rx: mpsc::Receiver<Message>, logger: Logger) -> Self {
        Store {
            prices: HashMap::new(),
            metrics: PriceLocalMetrics::new(&mut &mut PROMETHEUS_REGISTRY.lock().await),
            rx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.rx.recv().await {
            if let Err(err) = self.handle(message) {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
            }
        }
    }

    fn handle(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Update {
                price_identifier,
                price_info,
            } => {
                self.update(price_identifier, price_info)?;
                Ok(())
            }
            Message::LookupAllPriceInfo { result_tx } => result_tx
                .send(self.get_all_price_infos())
                .map_err(|_| anyhow!("failed to send LookupAllPriceInfo result")),
        }
    }

    pub fn update(
        &mut self,
        price_identifier: PriceIdentifier,
        price_info: PriceInfo,
    ) -> Result<()> {
        debug!(self.logger, "local store received price update"; "identifier" => bs58::encode(price_identifier.to_bytes()).into_string());

        // Drop the update if it is older than the current one stored for the price
        if let Some(current_price_info) = self.prices.get(&price_identifier) {
            if current_price_info.timestamp > price_info.timestamp {
                return Err(anyhow!(
                    "Received stale timestamp for price {}",
                    price_identifier
                ));
            }
        }

        self.metrics.update(&price_identifier, &price_info);

        // let this_price_metrics =
        //     self.metrics
        //         .entry(price_identifier)
        //         .or_insert(PriceLocalMetrics {
        //             // Instantiate metrics if they don't exist
        //             price:        register_int_gauge_with_registry!(
        //                 format!("local_{}_price", price_identifier),
        //                 format!("Local Store's price value for price {}", price_identifier),
        //                 PROMETHEUS_REGISTRY
        //             )?,
        //             conf:         register_gauge_with_registry!(
        //                 format!("local_{}_conf", price_identifier),
        //                 format!(
        //                     "Local Store's confidence interval for price {}",
        //                     price_identifier
        //                 ),
        //                 PROMETHEUS_REGISTRY
        //             )?,
        //             timestamp:    register_int_gauge_with_registry!(
        //                 format!("local_{}_timestamp", price_identifier),
        //                 format!("Local Store's timestamp for price {}", price_identifier),
        //                 PROMETHEUS_REGISTRY
        //             )?,
        //             update_count: register_counter_with_registry!(
        //                 format!("local_{}_update_count", price_identifier),
        //                 format!(
        //                     "Local Store's number of updates since process start for price {}",
        //                     price_identifier
        //                 ),
        //                 PROMETHEUS_REGISTRY
        //             )?,
        //         });

        // this_price_metrics.price.set(price_info.price);
        // this_price_metrics.conf.set(price_info.conf as f64);
        // this_price_metrics.timestamp.set(price_info.timestamp);
        // this_price_metrics.update_count.inc();

        self.prices.insert(price_identifier, price_info);

        Ok(())
    }

    pub fn get_all_price_infos(&self) -> HashMap<PriceIdentifier, PriceInfo> {
        self.prices.clone()
    }
}
