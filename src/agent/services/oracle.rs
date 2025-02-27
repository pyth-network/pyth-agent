//! Oracle
//!
//! The Oracle service is respoinsible for reacting to all remote/on-chain events.

use {
    crate::agent::{
        solana::{
            key_store::KeyStore,
            network::{
                Config,
                Network,
            },
        },
        state::oracle::Oracle,
    },
    anyhow::{Context, Result},
    solana_account_decoder::UiAccountEncoding,
    solana_client::{
        nonblocking::{
            pubsub_client::PubsubClient,
            rpc_client::RpcClient,
        },
        rpc_config::{
            RpcAccountInfoConfig,
            RpcProgramAccountsConfig,
        },
    },
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentConfig,
        pubkey::Pubkey,
        signature::Keypair,
    },
    std::{
        sync::Arc,
        time::{
            Duration,
            Instant,
        },
    },
    tokio::task::JoinHandle,
    tokio_stream::StreamExt,
    tracing::instrument,
};

#[instrument(skip(config, state))]
pub fn oracle<S>(config: Config, network: Network, state: Arc<S>) -> Vec<JoinHandle<()>>
where
    S: Oracle,
    S: Send + Sync + 'static,
{
    let mut handles = Vec::new();

    let Ok(key_store) = KeyStore::new(config.key_store.clone()) else {
        tracing::warn!("Key store not available, Oracle won't start.");
        return handles;
    };

    handles.push(tokio::spawn(poller(
        config.clone(),
        network,
        state.clone(),
        key_store.pyth_oracle_program_key,
        key_store.publish_keypair,
        key_store.pyth_price_store_program_key,
        config.oracle.max_lookup_batch_size,
    )));

    if config.oracle.subscriber_enabled {
        let number_of_workers = 100;
        let channel_size = 1000;
        let (sender, receiver) = tokio::sync::mpsc::channel(channel_size);
        let max_elapsed_time = Duration::from_secs(30);
        let sleep_time = Duration::from_secs(1);

        handles.push(tokio::spawn(async move {
            loop {
                let current_time = Instant::now();
                if let Err(ref err) = subscriber(
                    config.clone(),
                    network,
                    state.clone(),
                    key_store.pyth_oracle_program_key,
                    sender.clone(),
                )
                .await
                {
                    tracing::error!(?err, "Subscriber exited unexpectedly");
                    if current_time.elapsed() < max_elapsed_time {
                        tracing::warn!(?sleep_time, "Subscriber restarting too quickly. Sleeping");
                        tokio::time::sleep(sleep_time).await;
                    }
                }
            }
        }));

        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        for _ in 0..number_of_workers {
            let receiver = receiver.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    let mut receiver = receiver.lock().await;
                    if let Some(task) = receiver.recv().await {
                        drop(receiver);
                        if let Err(err) = task.await {
                            tracing::error!(%err, "error running price update");
                        }
                    }
                }
            }));
        }
    }

    handles
}

/// When an account RPC Subscription update is receiveed.
///
/// We check if the account is one we're aware of and tracking, and if so, spawn
/// a small background task that handles that update. We only do this for price
/// accounts, all other accounts are handled below in the poller.
#[instrument(skip(config, state))]
async fn subscriber<S>(
    config: Config,
    network: Network,
    state: Arc<S>,
    program_key: Pubkey,
    sender: tokio::sync::mpsc::Sender<tokio::task::JoinHandle<()>>,
) -> Result<()>
where
    S: Oracle,
    S: Send + Sync + 'static,
{
    // Setup PubsubClient to listen for account changes on the Oracle program.
    let client = PubsubClient::new(config.wss_url.as_str()).await?;

    let (mut notifier, _unsub) = {
        let commitment = config.oracle.commitment;
        let config = RpcProgramAccountsConfig {
            account_config: RpcAccountInfoConfig {
                commitment: Some(CommitmentConfig { commitment }),
                encoding: Some(UiAccountEncoding::Base64Zstd),
                ..Default::default()
            },
            filters:        None,
            with_context:   Some(true),
        };
        client.program_subscribe(&program_key, Some(config)).await
    }?;

    while let Some(update) = notifier.next().await {
        match update.value.account.decode::<Account>() {
            Some(account) => {
                let pubkey: Pubkey = update.value.pubkey.as_str().try_into()?;
                let state = state.clone();
                sender
                    .send(tokio::spawn(async move {
                        if let Err(err) =
                            Oracle::handle_price_account_update(&*state, network, &pubkey, &account)
                                .await
                        {
                            tracing::error!(?err, "Failed to handle account update");
                        }
                    }))
                    .await
                    .context("sending handle_price_account_update task to worker")?;
            }

            None => {
                tracing::error!(
                    update = ?update,
                    "Failed to decode account from update.",
                );
            }
        }
    }

    tracing::debug!("Subscriber closed connection.");
    Ok(())
}

/// On poll lookup all Pyth Product/Price accounts and sync.
#[instrument(skip(config, publish_keypair, state))]
async fn poller<S>(
    config: Config,
    network: Network,
    state: Arc<S>,
    oracle_program_key: Pubkey,
    publish_keypair: Option<Keypair>,
    pyth_price_store_program_key: Option<Pubkey>,
    max_lookup_batch_size: usize,
) where
    S: Oracle,
    S: Send + Sync + 'static,
{
    // Setup an RpcClient for manual polling.
    let mut tick = tokio::time::interval(config.oracle.poll_interval_duration);
    let client = Arc::new(RpcClient::new_with_timeout_and_commitment(
        config.rpc_url,
        config.rpc_timeout,
        CommitmentConfig {
            commitment: config.oracle.commitment,
        },
    ));

    loop {
        if let Err(err) = async {
            tick.tick().await;
            tracing::debug!("Polling for updates.");
            Oracle::poll_updates(
                &*state,
                network,
                oracle_program_key,
                publish_keypair.as_ref(),
                pyth_price_store_program_key,
                &client,
                max_lookup_batch_size,
            )
            .await?;
            Oracle::sync_global_store(&*state, network).await
        }
        .await
        {
            tracing::error!(err = ?err, "Failed to handle poll updates.");
        }
    }
}
