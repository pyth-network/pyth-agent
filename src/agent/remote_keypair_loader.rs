use {
    anyhow::{
        Context,
        Result,
    },
    serde::Deserialize,
    slog::Logger,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::Keypair,
        signer::Signer,
    },
    std::{
        net::SocketAddr,
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc,
            oneshot,
            Mutex,
        },
        task::JoinHandle,
    },
    warp::{
        hyper::StatusCode,
        reply::{
            self,
            WithStatus,
        },
        Filter,
        Rejection,
    },
};

pub fn default_min_keypair_balance_sol() -> u64 {
    1
}

pub fn default_bind_address() -> SocketAddr {
    "0.0.0.0:9001"
        .parse()
        .expect("INTERNAL: Could not build default remote keypair loader bind address")
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_min_keypair_balance_sol")]
    primary_min_keypair_balance_sol:   u64,
    #[serde(default = "default_min_keypair_balance_sol")]
    secondary_min_keypair_balance_sol: u64,
    #[serde(default = "default_bind_address")]
    bind_address:                      SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            primary_min_keypair_balance_sol:   default_min_keypair_balance_sol(),
            secondary_min_keypair_balance_sol: default_min_keypair_balance_sol(),
            bind_address:                      default_bind_address(),
        }
    }
}

#[derive(Debug)]
pub struct KeypairRequest {
    /// Where to send the key back
    response_tx: oneshot::Sender<Keypair>,
}

pub struct RemoteKeypairLoader {
    primary_current_keypair:   Option<Keypair>,
    secondary_current_keypair: Option<Keypair>,
    primary_rpc_url:           String,
    secondary_rpc_url:         Option<String>,
    config:                    Config,
}

impl RemoteKeypairLoader {
    pub async fn spawn(
        primary_requests_rx: mpsc::Receiver<KeypairRequest>,
        secondary_requests_rx: mpsc::Receiver<KeypairRequest>,
        primary_rpc_url: String,
        secondary_rpc_url: Option<String>,
        config: Config,
        logger: Logger,
    ) -> Vec<JoinHandle<()>> {
        let bind_address = config.bind_address.clone();

        let shared_state = Arc::new(Mutex::new(Self {
            primary_current_keypair: None,
            secondary_current_keypair: None,
            primary_rpc_url,
            secondary_rpc_url,
            config,
        }));

        let request_handler_jh = tokio::spawn(handle_key_requests(
            primary_requests_rx,
            secondary_requests_rx,
            shared_state.clone(),
            logger.clone(),
        ));

        let logger4primary = logger.clone();
        let shared_state4primary = shared_state.clone();
        let primary_upload_route = warp::path!("primary" / "load_keypair")
            .and(warp::post())
            .and(warp::body::content_length_limit(1024))
            .and(warp::body::json())
            .and(warp::path::end())
            .and_then(move |kp: Vec<u8>| {
                let shared_state = shared_state4primary.clone();
                let logger = logger4primary.clone();
                async move {
                    let mut locked_state = shared_state.lock().await;

                    let min_balance = locked_state.config.primary_min_keypair_balance_sol;
                    let rpc_url = locked_state.primary_rpc_url.clone();

                    let response = Self::handle_new_keypair(
                        &mut (locked_state.primary_current_keypair),
                        kp,
                        min_balance,
                        rpc_url,
                        "primary",
                        logger,
                    )
                    .await;

                    Result::<WithStatus<_>, Rejection>::Ok(response)
                }
            });

        let secondary_upload_route = warp::path!("secondary" / "load_keypair")
            .and(warp::post())
            .and(warp::body::content_length_limit(1024))
            .and(warp::body::json())
            .and(warp::path::end())
            .and_then(move |kp: Vec<u8>| {
                let shared_state = shared_state.clone();
                let logger = logger.clone();
                async move {
                    let mut locked_state = shared_state.lock().await;

                    if let Some(rpc_url) = locked_state.secondary_rpc_url.clone() {
                        let min_balance = locked_state.config.secondary_min_keypair_balance_sol;

                        let response = Self::handle_new_keypair(
                            &mut (locked_state.secondary_current_keypair),
                            kp,
                            min_balance,
                            rpc_url,
                            "secondary",
                            logger,
                        )
                        .await;

                        Result::<WithStatus<_>, Rejection>::Ok(response)
                    } else {
                        Result::<WithStatus<_>, Rejection>::Ok(reply::with_status(
                            "Secondary network is not active",
                            StatusCode::SERVICE_UNAVAILABLE,
                        ))
                    }
                }
            });

        let http_api_jh = tokio::spawn(
            warp::serve(primary_upload_route.or(secondary_upload_route)).bind(bind_address),
        );

        // WARNING: All jobs spawned here must report their join handles in this vec
        return vec![request_handler_jh, http_api_jh];
    }

    /// Validate and apply a keypair to the specified mut reference,
    /// hiding errors in logs.
    ///
    /// Returns the appropriate HTTP response depending on checks success.
    ///
    /// NOTE(2023-03-22): Lifetime bounds are currently necessary
    /// because of https://github.com/rust-lang/rust/issues/63033
    async fn handle_new_keypair<'a, 'b: 'a>(
        keypair_slot: &'a mut Option<Keypair>,
        new_keypair_bytes: Vec<u8>,
        min_keypair_balance_sol: u64,
        rpc_url: String,
        network_name: &'b str,
        logger: Logger,
    ) -> WithStatus<&'static str> {
        let mut upload_ok = true;

        match Keypair::from_bytes(&new_keypair_bytes) {
            Ok(kp) => {
                match Self::validate_keypair(&kp, min_keypair_balance_sol, rpc_url.clone()).await {
                    Ok(()) => {
                        *keypair_slot = Some(kp);
                    }
                    Err(e) => {
                        warn!(logger, "Remote keypair loader: Keypair failed validation";
                        "network" => network_name,
                        "error" => e.to_string(),
                        );
                        upload_ok = false;
                    }
                }
            }
            Err(e) => {
                warn!(logger, "Remote keypair loader: Keypair failed validation";
                "network" => network_name,
                "error" => e.to_string(),
                );
                upload_ok = false;
            }
        }

        if upload_ok {
            reply::with_status("keypair upload OK", StatusCode::OK)
        } else {
            reply::with_status(
                "Could not upload keypair. See logs for details.",
                StatusCode::BAD_REQUEST,
            )
        }
    }

    /// Validate keypair balance before using it in transactions.
    pub async fn validate_keypair(
        kp: &Keypair,
        min_keypair_balance_sol: u64,
        rpc_url: String,
    ) -> Result<()> {
        let c = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

        let balance_lamports = c
            .get_balance(&kp.pubkey())
            .await
            .context("Could not check keypair's balance")?;

        let lamports_in_sol = 1_000_000_000;

        if balance_lamports > min_keypair_balance_sol * lamports_in_sol {
            Ok(())
        } else {
            Err(anyhow::anyhow!(format!(
                "Keypair {} balance of {} SOL below threshold of {} SOL",
                kp.pubkey(),
                balance_lamports as f64 / lamports_in_sol as f64,
                min_keypair_balance_sol
            )))
        }
    }

    /// Get a keypair using the specified request
    /// sender. primary/secondary is decided by the channel the tx
    /// that request_tx comes from.
    pub async fn request_keypair(request_tx: &mpsc::Sender<KeypairRequest>) -> Result<Keypair> {
        let (tx, rx) = oneshot::channel();

        request_tx.send(KeypairRequest { response_tx: tx }).await?;

        Ok(rx.await?)
    }
}

/// Query channel receivers indefinitely, sending back the requested
/// keypair if available.
async fn handle_key_requests(
    mut primary_rx: mpsc::Receiver<KeypairRequest>,
    mut secondary_rx: mpsc::Receiver<KeypairRequest>,
    shared_state: Arc<Mutex<RemoteKeypairLoader>>,
    logger: Logger,
) {
    loop {
        let locked_state = shared_state.lock().await;

        // Only handle requests for defined keypairs. The possibility
        // of missing keypair is the reason we are not
        // tokio::select!()-ing on the two channel receivers.

        if let Some(primary_keypair) = locked_state.primary_current_keypair.as_ref() {
            // Drain all primary keypair requests
            while let Ok(KeypairRequest { response_tx }) = primary_rx.try_recv() {
                let copied_keypair = Keypair::from_bytes(&primary_keypair.to_bytes())
                    .expect("INTERNAL: could not convert Keypair to bytes and back");

                match response_tx.send(copied_keypair) {
                    Ok(()) => {}
                    Err(_e) => {
                        warn!(logger, "remote_keypair_loader: Could not send back primary keypair to channel";
                        );
                    }
                }
            }
        }

        if let Some(secondary_keypair) = locked_state.secondary_current_keypair.as_ref() {
            // Drain all secondary keypair requests
            while let Ok(KeypairRequest { response_tx }) = secondary_rx.try_recv() {
                let copied_keypair = Keypair::from_bytes(&secondary_keypair.to_bytes())
                    .expect("INTERNAL: could not convert Keypair to bytes and back");

                match response_tx.send(copied_keypair) {
                    Ok(()) => {}
                    Err(_e) => {
                        warn!(logger, "remote_keypair_loader: Could not send back secondary keypair to channel";
                        );
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
