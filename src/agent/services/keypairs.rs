//! Keypairs
//!
//! The Keypairs Service allows hotloading keys for the running agent.

use {
    crate::agent::{
        solana::network::Network,
        state::keypairs::Keypairs,
    },
    anyhow::{
        bail,
        Result,
    },
    serde::Deserialize,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::Keypair,
        signer::Signer,
    },
    std::{
        net::SocketAddr,
        sync::Arc,
    },
    tokio::task::JoinHandle,
    warp::{
        hyper::StatusCode,
        reject::Rejection,
        reply::{
            self,
            WithStatus,
        },
        Filter,
    },
};

const DEFAULT_MIN_KEYPAIR_BALANCE_SOL: u64 = 1;

pub fn default_bind_address() -> SocketAddr {
    "127.0.0.1:9001"
        .parse()
        .expect("INTERNAL: Could not build default remote keypair loader bind address")
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    primary_min_keypair_balance_sol:   u64,
    secondary_min_keypair_balance_sol: u64,
    bind_address:                      SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            primary_min_keypair_balance_sol:   DEFAULT_MIN_KEYPAIR_BALANCE_SOL,
            secondary_min_keypair_balance_sol: DEFAULT_MIN_KEYPAIR_BALANCE_SOL,
            bind_address:                      default_bind_address(),
        }
    }
}

pub async fn keypairs<S>(
    primary_rpc_urls: Vec<String>,
    secondary_rpc_urls: Option<Vec<String>>,
    config: Config,
    state: Arc<S>,
) -> Vec<JoinHandle<()>>
where
    S: Keypairs,
    S: Send + Sync + 'static,
{
    let ip = config.bind_address.ip();

    if !ip.is_loopback() {
        tracing::warn!(
            bind_address = ?config.bind_address,
            "Remote key loader: bind address is not localhost. Make sure the access on the selected address is secure.",
        );
    }

    let primary_upload_route = {
        let state = state.clone();
        let rpc_urls = primary_rpc_urls.clone();
        let min_balance = config.primary_min_keypair_balance_sol;
        warp::path!("primary" / "load_keypair")
            .and(warp::post())
            .and(warp::body::content_length_limit(1024))
            .and(warp::body::json())
            .and(warp::path::end())
            .and_then(move |kp: Vec<u8>| {
                let state = state.clone();
                let rpc_urls = rpc_urls.clone();
                async move {
                    let response = handle_new_keypair(
                        state,
                        Network::Primary,
                        kp,
                        min_balance,
                        rpc_urls,
                        "primary",
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
            let rpc_urls = secondary_rpc_urls.clone();
            async move {
                if let Some(rpc_urls) = rpc_urls {
                    let min_balance = config.secondary_min_keypair_balance_sol;
                    let response = handle_new_keypair(
                        state,
                        Network::Secondary,
                        kp,
                        min_balance,
                        rpc_urls,
                        "secondary",
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
    rpc_urls: Vec<String>,
    network_name: &'b str,
) -> WithStatus<&'static str>
where
    S: Keypairs,
{
    let mut upload_ok = true;
    match Keypair::from_bytes(&new_keypair_bytes) {
        Ok(kp) => match validate_keypair(&kp, min_keypair_balance_sol, rpc_urls.clone()).await {
            Ok(()) => {
                Keypairs::update_keypair(&*state, network, kp).await;
            }
            Err(e) => {
                tracing::warn!(
                    network = network_name,
                    error = e.to_string(),
                    "Remote keypair loader: Keypair failed validation",
                );
                upload_ok = false;
            }
        },
        Err(e) => {
            tracing::warn!(
                network = network_name,
                error = e.to_string(),
                "Remote keypair loader: Keypair failed validation",
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
    rpc_urls: Vec<String>,
) -> Result<()> {
    let balance_lamports = match get_balance(kp, rpc_urls).await {
        Ok(balance_lamports) => balance_lamports,
        Err(_) => bail!("Could not check keypair's balance"),
    };

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

async fn get_balance(kp: &Keypair, rpc_urls: Vec<String>) -> Result<u64> {
    for rpc_url in rpc_urls {
        let c = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
        match c.get_balance(&kp.pubkey()).await {
            Ok(balance) => return Ok(balance),
            Err(e) => tracing::warn!("getBalance error for rpc endpoint {}: {}", rpc_url, e),
        }
    }
    bail!("getBalance failed for all RPC endpoints")
}
