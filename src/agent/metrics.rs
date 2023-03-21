use {
    super::store::{
        global::Lookup,
        local::Message,
    },
    lazy_static::lazy_static,
    prometheus::{
        Encoder,
        Registry,
        TextEncoder,
    },
    serde::Deserialize,
    slog::Logger,
    std::{
        net::SocketAddr,
        sync::Arc,
        time::Instant,
    },
    tokio::sync::{
        mpsc,
        Mutex,
    },
    warp::{
        hyper::StatusCode,
        reply::{self,},
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
    pub static ref PROMETHEUS_REGISTRY: Registry = Registry::new();
}

/// Internal metrics server state, holds state needed for serving
/// dashboard and metrics.
pub struct MetricsServer {
    /// Used to pull the state of all symbols in local store
    pub local_store_tx:         mpsc::Sender<Message>,
    pub global_store_lookup_tx: mpsc::Sender<Lookup>,
    pub start_time:             Instant,
    pub logger:                 Logger,
}

impl MetricsServer {
    /// Instantiate a metrics API with a dashboard
    pub async fn spawn(
        addr: impl Into<SocketAddr> + 'static,
        local_store_tx: mpsc::Sender<Message>,
        global_store_lookup_tx: mpsc::Sender<Lookup>,
        logger: Logger,
    ) {
        let server = MetricsServer {
            local_store_tx,
            global_store_lookup_tx,
            start_time: Instant::now(),
            logger,
        };

        let shared_state = Arc::new(Mutex::new(server));

        let shared_state4dashboard = shared_state.clone();
        let dashboard_route = warp::path("dashboard")
            .and(warp::path::end())
            .and_then(move || {
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
                    let encoder = TextEncoder::new();
                    let mut buf = vec![];
                    let response = encoder.encode(&PROMETHEUS_REGISTRY.gather(), &mut buf).map_err(|e| -> Box<dyn std::error::Error> {
			e.into()
		    }).and_then(|_| -> Result<_, Box<dyn std::error::Error>> {
			let response_txt = String::from_utf8(buf)?;

			Ok(Box::new(reply::with_status(response_txt, StatusCode::OK)))
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
