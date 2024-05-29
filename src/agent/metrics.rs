use {
    super::adapter::{
        local::PriceInfo,
        Adapter,
    },
    crate::agent::{
        solana::oracle::PriceEntry,
        store::PriceIdentifier,
    },
    lazy_static::lazy_static,
    prometheus_client::{
        encoding::{
            text::encode,
            EncodeLabelSet,
        },
        metrics::{
            counter::Counter,
            family::Family,
            gauge::Gauge,
        },
        registry::Registry,
    },
    serde::Deserialize,
    slog::Logger,
    solana_sdk::pubkey::Pubkey,
    std::{
        net::SocketAddr,
        sync::{
            atomic::AtomicU64,
            Arc,
        },
        time::Instant,
    },
    tokio::sync::Mutex,
    warp::{
        hyper::StatusCode,
        reply,
        Filter,
        Rejection,
        Reply,
    },
};

pub fn default_bind_address() -> SocketAddr {
    "127.0.0.1:8888".parse().unwrap()
}

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
        }
    }
}

lazy_static! {
    pub static ref PROMETHEUS_REGISTRY: Arc<Mutex<Registry>> =
        Arc::new(Mutex::new(<Registry>::default()));
}

/// Internal metrics server state, holds state needed for serving
/// dashboard and metrics.
pub struct MetricsServer {
    pub start_time: Instant,
    pub logger:     Logger,
    pub adapter:    Arc<Adapter>,
}

impl MetricsServer {
    /// Instantiate a metrics API with a dashboard
    pub async fn spawn(
        addr: impl Into<SocketAddr> + 'static,
        logger: Logger,
        adapter: Arc<Adapter>,
    ) {
        let server = MetricsServer {
            start_time: Instant::now(),
            logger,
            adapter,
        };

        let shared_state = Arc::new(Mutex::new(server));

        let shared_state4dashboard = shared_state.clone();
        let dashboard_route = warp::path("dashboard")
            .or(warp::path::end())
            .and_then(move |_| {
                let shared_state = shared_state4dashboard.clone();
                async move {
                    let locked_state = shared_state.lock().await;
                    let response = locked_state
                        .render_dashboard() // Defined in a separate impl block near dashboard-specific code
                        .await
                        .unwrap_or_else(|e| {
                            // Add logging here
                            error!(locked_state.logger,"Dashboard: Rendering failed"; "error" => e.to_string());

                            // Withhold failure details from client
                            "Could not render dashboard! See the logs for details".to_owned()
                        });
                    Result::<Box<dyn Reply>, Rejection>::Ok(Box::new(reply::with_status(
                        reply::html(response),
                        StatusCode::OK,
                    )))
                }
            });

        let shared_state4metrics = shared_state.clone();
        let metrics_route = warp::path("metrics")
            .and(warp::path::end())
            .and_then(move || {
                let shared_state = shared_state4metrics.clone();
                async move {
		    let locked_state = shared_state.lock().await;
                    let mut buf = String::new();
                    let response = encode(&mut buf, &&PROMETHEUS_REGISTRY.lock().await).map_err(|e| -> Box<dyn std::error::Error> {e.into()
		    }).and_then(|_| -> Result<_, Box<dyn std::error::Error>> {

			Ok(Box::new(reply::with_status(buf, StatusCode::OK)))
		    }).unwrap_or_else(|e| {
			error!(locked_state.logger, "Metrics: Could not gather metrics from registry"; "error" => e.to_string());

			Box::new(reply::with_status("Could not gather metrics. See logs for details".to_string(), StatusCode::INTERNAL_SERVER_ERROR))
		    });

		    Result::<Box<dyn Reply>, Rejection>::Ok(response)
                }
            });

        warp::serve(dashboard_route.or(metrics_route))
            .bind(addr)
            .await;
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ProductGlobalLabels {
    pubkey: String,
    /// Set to "unknown_<pubkey>" if not found in the attribute set
    symbol: String,
}

/// Product account global store metrics.
#[derive(Default)]
pub struct ProductGlobalMetrics {
    /// How many times the global store has updated this product
    update_count: Family<ProductGlobalLabels, Counter>,
}

impl ProductGlobalMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = Default::default();

        #[deny(unused_variables)]
        let Self { update_count } = &metrics;

        registry.register(
            "global_prod_update_count",
            "The global store's update count for a product account",
            update_count.clone(),
        );

        metrics
    }

    pub fn update(&self, product_key: &Pubkey, maybe_symbol: Option<String>) {
        let symbol_string = maybe_symbol.unwrap_or(format!("unknown_{}", product_key));

        #[deny(unused_variables)]
        let Self { update_count } = self;

        update_count
            .get_or_create(&ProductGlobalLabels {
                pubkey: product_key.to_string(),
                symbol: symbol_string,
            })
            .inc();
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PriceGlobalLabels {
    pubkey: String,
}

/// Price account global store metrics. Most fields correspond with a subset of PriceEntry fields.
#[derive(Default)]
pub struct PriceGlobalMetrics {
    /// Note: the exponent is not applied to this metric
    price: Family<PriceGlobalLabels, Gauge>,

    expo: Family<PriceGlobalLabels, Gauge>,

    /// f64 is used to get u64 support. Official docs:
    /// https://docs.rs/prometheus-client/latest/prometheus_client/metrics/gauge/struct.Gauge.html#using-atomicu64-as-storage-and-f64-on-the-interface
    conf:      Family<PriceGlobalLabels, Gauge<f64, AtomicU64>>,
    timestamp: Family<PriceGlobalLabels, Gauge>,

    /// Note: the exponent is not applied to this metric
    prev_price:     Family<PriceGlobalLabels, Gauge>,
    prev_conf:      Family<PriceGlobalLabels, Gauge<f64, AtomicU64>>,
    prev_timestamp: Family<PriceGlobalLabels, Gauge>,

    /// How many times this Price was updated in the global store
    update_count: Family<PriceGlobalLabels, Counter>,
}

impl PriceGlobalMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = Default::default();

        #[deny(unused_variables)]
        let Self {
            price,
            expo,
            conf,
            timestamp,
            prev_price,
            prev_conf,
            prev_timestamp,
            update_count,
        } = &metrics;

        registry.register(
            "global_price_price",
            "The global store's price value for a price account",
            price.clone(),
        );

        registry.register(
            "global_price_expo",
            "The global store's exponent value for a price account",
            expo.clone(),
        );

        registry.register(
            "global_price_conf",
            "The global store's confidence interval value for a price account",
            conf.clone(),
        );

        registry.register(
            "global_price_timestamp",
            "The global store's publish timestamp value for a price account",
            timestamp.clone(),
        );

        registry.register(
            "global_price_prev_price",
            "The global store's prev_price value for a price account",
            prev_price.clone(),
        );

        registry.register(
            "global_price_prev_conf",
            "The global store's prev_conf (previous confidence interval) value for a price account",
            prev_conf.clone(),
        );

        registry.register(
            "global_price_prev_timestamp",
            "The global store's prev_timestamp (last publish timestamp with status 'trading') value for a price account",
            prev_timestamp.clone(),
        );

        registry.register(
            "global_price_update_count",
            "The global store's update count for a price account",
            update_count.clone(),
        );

        metrics
    }

    pub fn update(&self, price_key: &Pubkey, price_account: &PriceEntry) {
        #[deny(unused_variables)]
        let Self {
            price,
            expo,
            conf,
            timestamp,
            prev_price,
            prev_conf,
            prev_timestamp,
            update_count,
        } = self;

        price
            .get_or_create(&PriceGlobalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_account.agg.price);

        expo.get_or_create(&PriceGlobalLabels {
            pubkey: price_key.to_string(),
        })
        .set(price_account.expo as i64);

        conf.get_or_create(&PriceGlobalLabels {
            pubkey: price_key.to_string(),
        })
        .set(price_account.agg.conf as f64);

        timestamp
            .get_or_create(&PriceGlobalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_account.timestamp);

        prev_price
            .get_or_create(&PriceGlobalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_account.prev_price);

        prev_conf
            .get_or_create(&PriceGlobalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_account.prev_conf as f64);

        prev_timestamp
            .get_or_create(&PriceGlobalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_account.prev_timestamp);

        update_count
            .get_or_create(&PriceGlobalLabels {
                pubkey: price_key.to_string(),
            })
            .inc();
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PriceLocalLabels {
    pubkey: String,
}

/// Metrics exposed to Prometheus by the local store for each price
#[derive(Default)]
pub struct PriceLocalMetrics {
    price:     Family<PriceLocalLabels, Gauge>,
    /// f64 is used to get u64 support. Official docs:
    /// https://docs.rs/prometheus-client/latest/prometheus_client/metrics/gauge/struct.Gauge.html#using-atomicu64-as-storage-and-f64-on-the-interface
    conf:      Family<PriceLocalLabels, Gauge<f64, AtomicU64>>,
    timestamp: Family<PriceLocalLabels, Gauge>,

    /// How many times this price was updated in the local store
    update_count: Family<PriceLocalLabels, Counter>,
}
impl PriceLocalMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = Self::default();

        #[deny(unused_variables)]
        let PriceLocalMetrics {
            price,
            conf,
            timestamp,
            update_count,
        } = &metrics;

        registry.register(
            "local_store_price",
            "Price value from the local store",
            price.clone(),
        );
        registry.register(
            "local_store_conf",
            "Confidence interval value from the local store",
            conf.clone(),
        );
        registry.register(
            "local_store_timestamp",
            "Publish timestamp value from the local store",
            timestamp.clone(),
        );
        registry.register(
            "local_store_update_count",
            "How many times we've seen an update for this price in the local store",
            update_count.clone(),
        );

        metrics
    }

    pub fn update(&self, price_id: &PriceIdentifier, price_info: &PriceInfo) {
        #[deny(unused_variables)]
        let Self {
            price,
            conf,
            timestamp,
            update_count,
        } = self;

        let price_key = Pubkey::from(price_id.to_bytes());

        price
            .get_or_create(&PriceLocalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_info.price);
        conf.get_or_create(&PriceLocalLabels {
            pubkey: price_key.to_string(),
        })
        .set(price_info.conf as f64);
        timestamp
            .get_or_create(&PriceLocalLabels {
                pubkey: price_key.to_string(),
            })
            .set(price_info.timestamp.and_utc().timestamp());
        update_count
            .get_or_create(&PriceLocalLabels {
                pubkey: price_key.to_string(),
            })
            .inc();
    }
}
