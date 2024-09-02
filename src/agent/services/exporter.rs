use {
    crate::agent::{
        solana::network::{
            self,
            Network,
        },
        state::{
            exporter::Exporter,
            transactions::Transactions,
        },
    },
    anyhow::Result,
    futures_util::future,
    serde::{
        Deserialize,
        Serialize,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::commitment_config::CommitmentConfig,
    std::{
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::watch,
        task::JoinHandle,
        time::Interval,
    },
    tracing::instrument,
};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Config {
    /// Duration of the interval at which to refresh the cached network state (current slot and blockhash).
    /// It is recommended to set this to slightly less than the network's block time,
    /// as the slot fetched will be used as the time of the price update.
    #[serde(with = "humantime_serde")]
    pub refresh_network_state_interval_duration:         Duration,
    /// Duration of the interval at which to publish updates
    #[serde(with = "humantime_serde")]
    pub publish_interval_duration:                       Duration,
    /// Age after which a price update is considered stale and not published
    #[serde(with = "humantime_serde")]
    pub staleness_threshold:                             Duration,
    /// Wait at least this long before publishing an unchanged price
    /// state; unchanged price state means only timestamp has changed
    /// with other state identical to last published state.
    pub unchanged_publish_threshold:                     Duration,
    /// Maximum size of a batch
    pub max_batch_size:                                  usize,
    /// Capacity of the channel between the Exporter and the Transaction Monitor
    pub inflight_transactions_channel_capacity:          usize,
    /// Configuration for the Transaction Monitor
    pub transaction_monitor:                             transaction_monitor::Config,
    /// Number of compute units requested per update_price instruction within the transaction
    /// (i.e., requested units equals `n * compute_unit_limit`, where `n` is the number of update_price
    /// instructions)
    pub compute_unit_limit:                              u32,
    /// Price per compute unit offered for update_price transactions If dynamic compute unit is
    /// enabled and this value is set, the actual price per compute unit will be the maximum of the
    /// network dynamic price and this value.
    pub compute_unit_price_micro_lamports:               Option<u64>,
    /// Enable using dynamic price per compute unit based on the network previous prioritization
    /// fees.
    pub dynamic_compute_unit_pricing_enabled:            bool,
    /// Maximum total compute unit fee paid for a single transaction. Defaults to 0.001 SOL. This
    /// is a safety measure while using dynamic compute price to prevent the exporter from paying
    /// too much for a single transaction
    pub maximum_compute_unit_price_micro_lamports:       u64,
    /// Maximum slot gap between the current slot and the oldest slot amongst all the accounts in
    /// the batch. This is used to calculate the dynamic price per compute unit. When the slot gap
    /// reaches this number we will use the maximum total_compute_fee for the transaction.
    pub maximum_slot_gap_for_dynamic_compute_unit_price: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            refresh_network_state_interval_duration:         Duration::from_millis(200),
            publish_interval_duration:                       Duration::from_secs(1),
            staleness_threshold:                             Duration::from_secs(5),
            unchanged_publish_threshold:                     Duration::from_secs(5),
            max_batch_size:                                  12,
            inflight_transactions_channel_capacity:          10000,
            transaction_monitor:                             Default::default(),
            // The largest transactions without accumulator spend around 38k compute units
            // and accumulator cpi costs around 10k compute units. We set the limit to 60k
            // to have some buffer.
            compute_unit_limit:                              60000,
            compute_unit_price_micro_lamports:               None,
            dynamic_compute_unit_pricing_enabled:            false,
            // Maximum compute unit price (as a cap on the dynamic price)
            maximum_compute_unit_price_micro_lamports:       1_000_000,
            // A publisher update is not included if it is 25 slots behind the current slot.
            // Due to the delay in the network (until a block gets confirmed) and potential
            // ws issues we add 15 slots to make sure we do not overpay.
            maximum_slot_gap_for_dynamic_compute_unit_price: 40,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NetworkState {
    pub blockhash:    solana_sdk::hash::Hash,
    pub current_slot: u64,
}

/// NetworkStateQuerier periodically queries the current state of the network,
/// fetching the blockhash and slot number.
struct NetworkStateQuerier {
    /// The RPC client
    rpc_client: RpcClient,

    /// The interval with which to query the network state
    query_interval: Interval,

    /// Channel the current network state is sent on
    network_state_tx: watch::Sender<NetworkState>,
}

impl NetworkStateQuerier {
    #[instrument(
        skip(rpc_endpoint, rpc_timeout, query_interval),
        fields(
            rpc_timeout    = rpc_timeout.as_millis(),
            query_interval = query_interval.period().as_millis(),
        )
    )]
    pub fn new(
        rpc_endpoint: &str,
        rpc_timeout: Duration,
        query_interval: Interval,
        network_state_tx: watch::Sender<NetworkState>,
    ) -> Self {
        NetworkStateQuerier {
            rpc_client: RpcClient::new_with_timeout(rpc_endpoint.to_string(), rpc_timeout),
            query_interval,
            network_state_tx,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.query_interval.tick().await;
            if let Err(err) = self.query_network_state().await {
                tracing::error!(err = ?err, "Network state query failed");
            }
        }
    }

    #[instrument(skip(self))]
    async fn query_network_state(&mut self) -> Result<()> {
        // Fetch the blockhash and current slot in parallel
        let current_slot_future = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::confirmed());
        let latest_blockhash_future = self.rpc_client.get_latest_blockhash();

        let (current_slot_result, latest_blockhash_result) =
            future::join(current_slot_future, latest_blockhash_future).await;

        // Send the result on the channel
        self.network_state_tx.send(NetworkState {
            blockhash:    latest_blockhash_result?,
            current_slot: current_slot_result?,
        })?;

        Ok(())
    }
}

#[instrument(skip(config, state))]
pub fn exporter<S>(config: network::Config, network: Network, state: Arc<S>) -> Vec<JoinHandle<()>>
where
    S: Exporter,
    S: Transactions,
    S: Send + Sync + 'static,
{
    let mut handles = Vec::new();

    // Create and spawn the network state querier
    let (network_state_tx, network_state_rx) = watch::channel(Default::default());
    let mut network_state_querier = NetworkStateQuerier::new(
        &config.rpc_url,
        config.rpc_timeout,
        tokio::time::interval(config.exporter.refresh_network_state_interval_duration),
        network_state_tx,
    );

    handles.push(tokio::spawn(transaction_monitor::transaction_monitor(
        config.clone(),
        state.clone(),
    )));

    handles.push(tokio::spawn(exporter::exporter(
        config,
        network,
        state,
        network_state_rx,
    )));

    handles.push(tokio::spawn(
        async move { network_state_querier.run().await },
    ));

    handles
}

mod exporter {
    use {
        super::NetworkState,
        crate::agent::{
            solana::{
                key_store::KeyStore,
                network::{
                    Config,
                    Network,
                },
            },
            state::exporter::{
                get_publish_keypair,
                publish_batches,
                Exporter,
            },
        },
        solana_client::nonblocking::rpc_client::RpcClient,
        std::sync::Arc,
        tokio::sync::watch,
    };

    pub async fn exporter<S>(
        config: Config,
        network: Network,
        state: Arc<S>,
        network_state_rx: watch::Receiver<NetworkState>,
    ) where
        S: Exporter,
        S: Send + Sync + 'static,
    {
        let mut publish_interval = tokio::time::interval(config.exporter.publish_interval_duration);
        let mut dynamic_compute_unit_price_update_interval =
            tokio::time::interval(config.exporter.publish_interval_duration);

        let client = Arc::new(RpcClient::new_with_timeout(
            config.rpc_url.to_string(),
            config.rpc_timeout,
        ));
        let Ok(key_store) = KeyStore::new(config.key_store.clone()) else {
            tracing::warn!("Key store not available, Exporter won't start.");
            return;
        };

        loop {
            tokio::select! {
                _ = publish_interval.tick() => {
                    if let Ok(publish_keypair) = get_publish_keypair(&*state, network, key_store.publish_keypair.as_ref()).await {
                        if let Ok(permissioned_updates) = Exporter::get_permissioned_updates(
                            &*state,
                            &publish_keypair,
                            config.exporter.staleness_threshold,
                            config.exporter.unchanged_publish_threshold,
                        ).await {
                            if let Err(err) = publish_batches(
                                state.clone(),
                                client.clone(),
                                network,
                                &network_state_rx,
                                key_store.accumulator_key,
                                &publish_keypair,
                                key_store.oracle_program_key,
                                key_store.publish_program_key,
                                config.exporter.max_batch_size,
                                config.exporter.staleness_threshold,
                                config.exporter.compute_unit_limit,
                                config.exporter.compute_unit_price_micro_lamports,
                                config.exporter.maximum_compute_unit_price_micro_lamports,
                                config.exporter.maximum_slot_gap_for_dynamic_compute_unit_price,
                                config.exporter.dynamic_compute_unit_pricing_enabled,
                                permissioned_updates,
                            ).await {
                                tracing::error!(err = ?err, "Exporter failed to publish.");
                            }
                        }
                    }
                }
                _ = dynamic_compute_unit_price_update_interval.tick() => {
                    if config.exporter.dynamic_compute_unit_pricing_enabled {
                        if let Ok(publish_keypair) = get_publish_keypair(&*state, network, key_store.publish_keypair.as_ref()).await {
                            if let Err(err) = Exporter::update_recent_compute_unit_price(
                                &*state,
                                &publish_keypair,
                                &client,
                                config.exporter.staleness_threshold,
                                config.exporter.unchanged_publish_threshold,
                            ).await {
                                tracing::error!(err = ?err, "Exporter failed to compute unit price.");
                            }
                        }
                    }
                }
            }
        }
    }
}

mod transaction_monitor {
    use {
        crate::agent::{
            solana::network,
            state::transactions::Transactions,
        },
        serde::{
            Deserialize,
            Serialize,
        },
        solana_client::nonblocking::rpc_client::RpcClient,
        std::{
            sync::Arc,
            time::Duration,
        },
        tracing::instrument,
    };

    #[derive(Clone, Serialize, Deserialize, Debug)]
    #[serde(default)]
    pub struct Config {
        /// Duration of the interval with which to poll the status of transactions.
        /// It is recommended to set this to a value close to the Exporter's publish_interval.
        #[serde(with = "humantime_serde")]
        pub poll_interval_duration: Duration,
        /// Maximum number of recent transactions to monitor. When this number is exceeded,
        /// the oldest transactions are no longer monitored. It is recommended to set this to
        /// a value at least as large as (number of products published / number of products in a batch).
        pub max_transactions:       usize,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                poll_interval_duration: Duration::from_secs(4),
                max_transactions:       100,
            }
        }
    }

    #[instrument(skip(config, state))]
    pub async fn transaction_monitor<S>(config: network::Config, state: Arc<S>)
    where
        S: Transactions,
    {
        let client = RpcClient::new_with_timeout(config.rpc_url.to_string(), config.rpc_timeout);
        let mut poll_interval =
            tokio::time::interval(config.exporter.transaction_monitor.poll_interval_duration);

        loop {
            poll_interval.tick().await;
            if let Err(err) = Transactions::poll_transactions_status(&*state, &client).await {
                tracing::error!(err = ?err, "Transaction monitor failed.");
            }
        }
    }
}
