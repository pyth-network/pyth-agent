//! Keypair Management API
//!
//! The Keypair Manager allows hotloading keypairs via a HTTP request.

use {
    super::State,
    crate::agent::solana::network::Network,
    anyhow::{Context, Result},
    serde::Deserialize,
    slog::Logger,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, signer::Signer},
    std::{net::SocketAddr, sync::Arc},
    tokio::{sync::RwLock, task::JoinHandle},
    warp::{
        hyper::StatusCode,
        reply::{self, WithStatus},
        Filter, Rejection,
    },
};

pub fn default_min_keypair_balance_sol() -> u64 {
    1
}

pub fn default_bind_address() -> SocketAddr {
    "127.0.0.1:9001"
        .parse()
        .expect("INTERNAL: Could not build default remote keypair loader bind address")
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    primary_min_keypair_balance_sol: u64,
    secondary_min_keypair_balance_sol: u64,
    bind_address: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            primary_min_keypair_balance_sol: default_min_keypair_balance_sol(),
            secondary_min_keypair_balance_sol: default_min_keypair_balance_sol(),
            bind_address: default_bind_address(),
        }
    }
}

#[derive(Default)]
pub struct KeypairState {
    primary_current_keypair: RwLock<Option<Keypair>>,
    secondary_current_keypair: RwLock<Option<Keypair>>,
}

#[async_trait::async_trait]
pub trait Keypairs {
    async fn request_keypair(&self, network: Network) -> Result<Keypair>;
    async fn update_keypair(&self, network: Network, new_keypair: Keypair);
}

// Allow downcasting Adapter into Keypairs for functions that depend on the `Keypairs` service.
impl<'a> From<&'a State> for &'a KeypairState {
    fn from(adapter: &'a State) -> &'a KeypairState {
        &adapter.keypairs
    }
}

#[async_trait::async_trait]
impl<T> Keypairs for T
where
    for<'a> &'a T: Into<&'a KeypairState>,
    T: Sync,
{
    async fn request_keypair(&self, network: Network) -> Result<Keypair> {
        let keypair = match network {
            Network::Primary => &self.into().primary_current_keypair,
            Network::Secondary => &self.into().secondary_current_keypair,
        }
        .read()
        .await;

        Ok(Keypair::from_bytes(
            &keypair
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Keypair not available"))?
                .to_bytes(),
        )?)
    }

    async fn update_keypair(&self, network: Network, new_keypair: Keypair) {
        *match network {
            Network::Primary => self.into().primary_current_keypair.write().await,
            Network::Secondary => self.into().secondary_current_keypair.write().await,
        } = Some(new_keypair);
    }
}

pub async fn spawn<S>(
    primary_rpc_url: String,
    secondary_rpc_url: Option<String>,
    config: Config,
    logger: Logger,
    state: Arc<S>,
) -> Vec<JoinHandle<()>>
where
    S: Keypairs,
    S: Send + Sync + 'static,
    for<'a> &'a S: Into<&'a KeypairState>,
{
    let ip = config.bind_address.ip();

    if !ip.is_loopback() {
        warn!(logger, "Remote key loader: bind address is not localhost. Make sure the access on the selected address is secure."; "bind_address" => config.bind_address,);
    }

    let primary_upload_route = {
        let state = state.clone();
        let logger = logger.clone();
        let rpc_url = primary_rpc_url.clone();
        let min_balance = config.primary_min_keypair_balance_sol;
        warp::path!("primary" / "load_keypair")
            .and(warp::post())
            .and(warp::body::content_length_limit(1024))
            .and(warp::body::json())
            .and(warp::path::end())
            .and_then(move |kp: Vec<u8>| {
                let state = state.clone();
                let logger = logger.clone();
                let rpc_url = rpc_url.clone();
                async move {
                    let response = handle_new_keypair(
                        state,
                        Network::Primary,
                        kp,
                        min_balance,
                        rpc_url,
                        "primary",
                        logger,
                    )
                    .await;
                    Result::<WithStatus<_>, Rejection>::Ok(response)
                }
            })
    };

    let secondary_upload_route = warp::path!("secondary" / "load_keypair")
        .and(warp::post())
        .and(warp::body::content_length_limit(1024))
        .and(warp::body::json())
        .and(warp::path::end())
        .and_then(move |kp: Vec<u8>| {
            let state = state.clone();
            let logger = logger.clone();
            let rpc_url = secondary_rpc_url.clone();
            async move {
                if let Some(rpc_url) = rpc_url {
                    let min_balance = config.secondary_min_keypair_balance_sol;
                    let response = handle_new_keypair(
                        state,
                        Network::Secondary,
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

    let http_api_jh = {
        let (_, serve) = warp::serve(primary_upload_route.or(secondary_upload_route))
            .bind_with_graceful_shutdown(config.bind_address, async {
                let _ = crate::agent::EXIT.subscribe().changed().await;
            });
        tokio::spawn(serve)
    };

    // WARNING: All jobs spawned here must report their join handles in this vec
    vec![http_api_jh]
}

/// Validate and apply a keypair to the specified mut reference,
/// hiding errors in logs.
///
/// Returns the appropriate HTTP response depending on checks success.
///
/// NOTE(2023-03-22): Lifetime bounds are currently necessary
/// because of https://github.com/rust-lang/rust/issues/63033
async fn handle_new_keypair<'a, 'b: 'a, S>(
    state: Arc<S>,
    network: Network,
    new_keypair_bytes: Vec<u8>,
    min_keypair_balance_sol: u64,
    rpc_url: String,
    network_name: &'b str,
    logger: Logger,
) -> WithStatus<&'static str>
where
    S: Keypairs,
{
    let mut upload_ok = true;
    match Keypair::from_bytes(&new_keypair_bytes) {
        Ok(kp) => match validate_keypair(&kp, min_keypair_balance_sol, rpc_url.clone()).await {
            Ok(()) => {
                Keypairs::update_keypair(&*state, network, kp).await;
            }
            Err(e) => {
                warn!(logger, "Remote keypair loader: Keypair failed validation";
                    "network" => network_name,
                    "error" => e.to_string(),
                );
                upload_ok = false;
            }
        },
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
