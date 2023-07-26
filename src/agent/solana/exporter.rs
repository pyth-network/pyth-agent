use {
    self::transaction_monitor::TransactionMonitor,
    super::{
        super::store::{
            self,
            local::PriceInfo,
            PriceIdentifier,
        },
        key_store,
    },
    crate::agent::remote_keypair_loader::{
        KeypairRequest,
        RemoteKeypairLoader,
    },
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    bincode::Options,
    chrono::Utc,
    futures_util::future::{
        self,
        join_all,
    },
    key_store::KeyStore,
    pyth_sdk::Identifier,
    pyth_sdk_solana::state::PriceStatus,
    serde::{
        Deserialize,
        Serialize,
    },
    slog::Logger,
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::RpcSendTransactionConfig,
    },
    solana_sdk::{
        bs58,
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::{
            AccountMeta,
            Instruction,
        },
        pubkey::Pubkey,
        signature::{
            Keypair,
            Signature,
        },
        signer::Signer,
        sysvar::clock,
        transaction::Transaction,
    },
    std::{
        collections::{
            HashMap,
            HashSet,
        },
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc,
            mpsc::{
                error::TryRecvError,
                Sender,
            },
            oneshot,
            watch,
        },
        task::JoinHandle,
        time::{
            self,
            Interval,
        },
    },
};

const PYTH_ORACLE_VERSION: u32 = 2;
const UPDATE_PRICE_NO_FAIL_ON_ERROR: i32 = 13;
// const UPDATE_PRICE: i32 = 7; // Useful for making tx errors more visible in place of UPDATE_PRICE_NO_FAIL_ON_ERROR

#[repr(C)]
#[derive(Serialize, PartialEq, Debug, Clone)]
struct UpdPriceCmd {
    version:  u32,
    cmd:      i32,
    status:   PriceStatus,
    unused_:  u32,
    price:    i64,
    conf:     u64,
    pub_slot: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Config {
    /// Duration of the interval at which to refresh the cached network state (current slot and blockhash).
    /// It is recommended to set this to slightly less than the network's block time,
    /// as the slot fetched will be used as the time of the price update.
    #[serde(with = "humantime_serde")]
    pub refresh_network_state_interval_duration: Duration,
    /// Duration of the interval at which to publish updates
    #[serde(with = "humantime_serde")]
    pub publish_interval_duration:               Duration,
    /// Age after which a price update is considered stale and not published
    #[serde(with = "humantime_serde")]
    pub staleness_threshold:                     Duration,
    /// Wait at least this long before publishing an unchanged price
    /// state; unchanged price state means only timestamp has changed
    /// with other state identical to last published state.
    pub unchanged_publish_threshold:             Duration,
    /// Maximum size of a batch
    pub max_batch_size:                          usize,
    /// Capacity of the channel between the Exporter and the Transaction Monitor
    pub inflight_transactions_channel_capacity:  usize,
    /// Configuration for the Transaction Monitor
    pub transaction_monitor:                     transaction_monitor::Config,
    /// Number of compute units requested per update_price instruction within the transaction
    /// (i.e., requested units equals `n * compute_unit_limit`, where `n` is the number of update_price
    /// instructions)
    pub compute_unit_limit:                      u32,
    /// Price per compute unit offered for update_price transactions
    pub compute_unit_price_micro_lamports:       Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            refresh_network_state_interval_duration: Duration::from_millis(200),
            publish_interval_duration:               Duration::from_secs(1),
            staleness_threshold:                     Duration::from_secs(5),
            unchanged_publish_threshold:             Duration::from_secs(5),
            max_batch_size:                          12,
            inflight_transactions_channel_capacity:  10000,
            transaction_monitor:                     Default::default(),
            // The largest transactions appear to be about ~12000 CUs. We leave ourselves some breathing room.
            compute_unit_limit:                      40000,
            compute_unit_price_micro_lamports:       None,
        }
    }
}

pub fn spawn_exporter(
    config: Config,
    rpc_url: &str,
    rpc_timeout: Duration,
    publisher_permissions_rx: mpsc::Receiver<HashMap<Pubkey, HashSet<Pubkey>>>,
    key_store: KeyStore,
    local_store_tx: Sender<store::local::Message>,
    keypair_request_tx: mpsc::Sender<KeypairRequest>,
    logger: Logger,
) -> Result<Vec<JoinHandle<()>>> {
    // Create and spawn the network state querier
    let (network_state_tx, network_state_rx) = watch::channel(Default::default());
    let mut network_state_querier = NetworkStateQuerier::new(
        rpc_url,
        rpc_timeout,
        time::interval(config.refresh_network_state_interval_duration),
        network_state_tx,
        logger.clone(),
    );
    let network_state_querier_jh = tokio::spawn(async move { network_state_querier.run().await });

    // Create and spawn the transaction monitor
    let (transactions_tx, transactions_rx) =
        mpsc::channel(config.inflight_transactions_channel_capacity);
    let mut transaction_monitor = TransactionMonitor::new(
        config.transaction_monitor.clone(),
        rpc_url,
        rpc_timeout,
        transactions_rx,
        logger.clone(),
    );
    let transaction_monitor_jh = tokio::spawn(async move { transaction_monitor.run().await });

    // Create and spawn the exporter
    let mut exporter = Exporter::new(
        config,
        rpc_url,
        rpc_timeout,
        key_store,
        local_store_tx,
        network_state_rx,
        transactions_tx,
        publisher_permissions_rx,
        keypair_request_tx,
        logger,
    );
    let exporter_jh = tokio::spawn(async move { exporter.run().await });

    Ok(vec![
        network_state_querier_jh,
        transaction_monitor_jh,
        exporter_jh,
    ])
}

/// Exporter is responsible for exporting data held in the local store
/// to the global Pyth Network.
pub struct Exporter {
    rpc_client: RpcClient,

    config: Config,

    /// Interval at which to publish updates
    publish_interval: Interval,

    /// The Key Store
    key_store: KeyStore,

    /// Channel on which to communicate with the local store
    local_store_tx: Sender<store::local::Message>,

    /// The last state published for each price identifier. Used to
    /// rule out stale data and prevent repetitive publishing of
    /// unchanged prices.
    last_published_state: HashMap<PriceIdentifier, PriceInfo>,

    /// Watch receiver channel to access the current network state
    network_state_rx: watch::Receiver<NetworkState>,

    // Channel on which to send inflight transactions to the transaction monitor
    inflight_transactions_tx: Sender<Signature>,

    /// Permissioned symbols as read by the oracle module
    publisher_permissions_rx: mpsc::Receiver<HashMap<Pubkey, HashSet<Pubkey>>>,

    /// Currently known permissioned prices of this publisher
    our_prices: HashSet<Pubkey>,

    keypair_request_tx: Sender<KeypairRequest>,

    logger: Logger,
}

impl Exporter {
    pub fn new(
        config: Config,
        rpc_url: &str,
        rpc_timeout: Duration,
        key_store: KeyStore,
        local_store_tx: Sender<store::local::Message>,
        network_state_rx: watch::Receiver<NetworkState>,
        inflight_transactions_tx: Sender<Signature>,
        publisher_permissions_rx: mpsc::Receiver<HashMap<Pubkey, HashSet<Pubkey>>>,
        keypair_request_tx: mpsc::Sender<KeypairRequest>,
        logger: Logger,
    ) -> Self {
        let publish_interval = time::interval(config.publish_interval_duration);
        Exporter {
            rpc_client: RpcClient::new_with_timeout(rpc_url.to_string(), rpc_timeout),
            config,
            publish_interval,
            key_store,
            local_store_tx,
            last_published_state: HashMap::new(),
            network_state_rx,
            inflight_transactions_tx,
            publisher_permissions_rx,
            our_prices: HashSet::new(),
            keypair_request_tx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.publish_interval.tick().await;
            if let Err(err) = self.publish_updates().await {
                error!(self.logger, "{}", err);
                debug!(self.logger, "error context"; "context" => format!("{:?}", err));
            }
        }
    }

    /// Publishes any price updates in the local store that we haven't sent to this network.
    ///
    /// The strategy used to do this is as follows:
    /// - Fetch all the price updates currently present in the local store
    /// - Filter out price updates we have previously attempted to publish, or which are
    ///   too old to publish.
    /// - Collect the price updates into batches.
    /// - Publish all the batches, staggering them evenly over the interval at which this method is called.
    ///
    /// This design is intended to:
    /// - Decouple the rate at which the local store is updated and the rate at which we publish transactions.
    ///   A user sending an unusually high rate of price updates shouldn't be reflected in the transaction rate.
    /// - Degrade gracefully if the blockchain RPC node exhibits poor performance. If the RPC node takes a long
    ///   time to respond, no internal queues grow unboundedly. At any single point in time there are at most
    ///   (n / batch_size) requests in flight.
    async fn publish_updates(&mut self) -> Result<()> {
        let local_store_contents = self.fetch_local_store_contents().await?;

        let now = Utc::now().timestamp();

        // Filter the contents to only include information we haven't already sent,
        // and to ignore stale information.
        let fresh_updates = local_store_contents
            .iter()
            .filter(|(identifier, info)| {
                // Filter out timestamps older than what we already published
                if let Some(last_info) = self.last_published_state.get(identifier) {
                    last_info.timestamp < info.timestamp
                } else {
                    true // No prior data found, letting the price through
                }
            })
            .filter(|(_identifier, info)| {
                // Filter out timestamps that are old
                (now - info.timestamp) < self.config.staleness_threshold.as_secs() as i64
            })
            .filter(|(identifier, info)| {
                // Filter out unchanged price data if the max delay wasn't reached

                if let Some(last_info) = self.last_published_state.get(identifier) {
                    if (info.timestamp - last_info.timestamp)
                        > self.config.unchanged_publish_threshold.as_secs() as i64
                    {
                        true // max delay since last published state reached, we publish anyway
                    } else {
                        !last_info.cmp_no_timestamp(*info) // Filter out if data is unchanged
                    }
                } else {
                    true // No prior data found, letting the price through
                }
            })
            .collect::<Vec<_>>();

        let publish_keypair = if let Some(kp) = self.key_store.publish_keypair.as_ref() {
            // It's impossible to sanely return a &Keypair in the
            // other if branch, so we clone the reference.
            Keypair::from_bytes(&kp.to_bytes())
                .context("INTERNAL: Could not convert keypair to bytes and back")?
        } else {
            // Request the keypair from remote keypair loader.  Doing
            // this here guarantees that the up to date loaded keypair
            // is being used.
            //
            // Currently, we're guaranteed not to clog memory or block
            // the keypair loader under the following assumptions:
            // - The Exporter publishing loop waits for a publish
            //   attempt to finish before beginning the next
            //   one. Currently realized in run()
            // - The Remote Key Loader does not read channels for
            //   keypairs it does not have. Currently expressed in
            //   handle_key_requests() in remote_keypair_loader.rs

            debug!(
                self.logger,
                "Exporter: Publish keypair is None, requesting remote loaded key"
            );
            let kp = RemoteKeypairLoader::request_keypair(&self.keypair_request_tx).await?;
            debug!(self.logger, "Exporter: Keypair received");
            kp
        };

        self.update_our_prices(&publish_keypair.pubkey());

        debug!(self.logger, "Exporter: filtering prices permissioned to us";
               "our_prices" => format!("{:?}", self.our_prices),
               "publish_pubkey" => publish_keypair.pubkey().to_string(),
        );

        // Filter out price accounts we're not permissioned to update
        let permissioned_updates = fresh_updates
            .into_iter()
            .filter(|(id, _data)| {
                let key_from_id = Pubkey::new((*id).clone().to_bytes().as_slice());
                if self.our_prices.contains(&key_from_id) {
                    true
                } else {
                    // Note: This message is not an error. Some
                    // publishers have different permissions on
                    // primary/secondary networks
                    debug!(
                    self.logger,
                    "Exporter: Attempted to publish a price without permission, skipping";
                    "unpermissioned_price_account" => key_from_id.to_string(),
                    "permissioned_accounts" => format!("{:?}", self.our_prices)
                            );
                    false
                }
            })
            .collect::<Vec<_>>();

        if permissioned_updates.is_empty() {
            return Ok(());
        }

        // Split the updates up into batches
        let batches = permissioned_updates.chunks(self.config.max_batch_size);

        // Publish all the batches, staggering the requests over the publish interval
        let num_batches = batches.len();
        let mut batch_send_interval = time::interval(
            self.config
                .publish_interval_duration
                .div_f64(num_batches as f64),
        );
        let mut batch_state = HashMap::new();
        let mut batch_futures = vec![];
        for batch in batches {
            batch_futures.push(self.publish_batch(batch, &publish_keypair));

            for (identifier, info) in batch {
                batch_state.insert(**identifier, (**info).clone());
            }

            batch_send_interval.tick().await;
        }

        // Wait for all the update requests to complete. Note that this doesn't wait for the
        // transactions themselves to be processed or confirmed, just the RPC requests to return.
        join_all(batch_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.last_published_state.extend(batch_state);

        Ok(())
    }

    /// Update permissioned prices of this publisher from oracle using
    /// the publisher permissions channel.
    ///
    /// The loop ensures that we clear the channel and use
    /// only the final, latest message; try_recv() is
    /// non-blocking.
    ///
    /// Note: This behavior is similar to
    /// tokio::sync::watch::channel(), which was not appropriate here
    /// because its internal RwLock would complain about not being
    /// Send with the HashMap<HashSet<Pubkey>> inside.
    /// TODO(2023-05-05): Debug the watch::channel() compilation errors
    fn update_our_prices(&mut self, publish_pubkey: &Pubkey) {
        loop {
            match self.publisher_permissions_rx.try_recv() {
                Ok(publisher_permissions) => {
                    self.our_prices = publisher_permissions.get(publish_pubkey) .cloned()
            .unwrap_or_else( || {
		warn!(
		    self.logger,
		    "Exporter: No permissioned prices were found for the publishing keypair on-chain. This is expected only on startup.";
		    "publish_pubkey" => publish_pubkey.to_string(),
		);
		HashSet::new()
	    });
                    trace!(
                        self.logger,
                        "Exporter: read permissioned price accounts from channel";
                        "new_value" => format!("{:?}", self.our_prices),
                    );
                }
                // Expected failures when channel is empty
                Err(TryRecvError::Empty) => {
                    trace!(
                        self.logger,
                        "Exporter: No more permissioned price accounts in channel, using cached value";
                        "cached_value" => format!("{:?}", self.our_prices),
                    );
                    break;
                }
                // Unexpected failures (channel closed, internal errors etc.)
                Err(other) => {
                    warn!(
                        self.logger,
                        "Exporter: Updating permissioned price accounts failed unexpectedly, using cached value";
                        "cached_value" => format!("{:?}", self.our_prices),
                        "error" => other.to_string(),
                    );
                    break;
                }
            }
        }
    }

    async fn fetch_local_store_contents(&self) -> Result<HashMap<PriceIdentifier, PriceInfo>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.local_store_tx
            .send(store::local::Message::LookupAllPriceInfo { result_tx })
            .await
            .map_err(|_| anyhow!("failed to send lookup price info message to local store"))?;
        result_rx
            .await
            .map_err(|_| anyhow!("failed to fetch from local store"))
    }

    async fn publish_batch(
        &self,
        batch: &[(&Identifier, &PriceInfo)],
        publish_keypair: &Keypair,
    ) -> Result<()> {
        let mut instructions = Vec::new();

        // Refresh the data in the batch
        let local_store_contents = self.fetch_local_store_contents().await?;
        let refreshed_batch = batch.iter().map(|(identifier, _)| {
            (
                identifier,
                local_store_contents
                    .get(identifier)
                    .ok_or_else(|| anyhow!("price identifier not found in local store"))
                    .with_context(|| identifier.to_string()),
            )
        });
        let price_accounts = refreshed_batch
            .clone()
            .map(|(identifier, _)| bs58::encode(identifier.to_bytes()).into_string())
            .collect::<Vec<_>>();

        let network_state = *self.network_state_rx.borrow();
        for (identifier, price_info_result) in refreshed_batch {
            let price_info = price_info_result?;

            let stale_price = (Utc::now().timestamp() - price_info.timestamp)
                > self.config.staleness_threshold.as_secs() as i64;
            if stale_price {
                continue;
            }

            let instruction = if let Some(accumulator_program_key) = self.key_store.accumulator_key
            {
                self.create_instruction_with_accumulator(
                    publish_keypair.pubkey(),
                    Pubkey::new(&identifier.to_bytes()),
                    &price_info,
                    network_state.current_slot,
                    accumulator_program_key,
                )?
            } else {
                self.create_instruction_without_accumulator(
                    publish_keypair.pubkey(),
                    Pubkey::new(&identifier.to_bytes()),
                    &price_info,
                    network_state.current_slot,
                )?
            };

            instructions.push(instruction);
        }

        // Pay priority fees, if configured
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            self.config.compute_unit_limit * instructions.len() as u32,
        ));
        if let Some(compute_unit_price_micro_lamports) =
            self.config.compute_unit_price_micro_lamports
        {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                compute_unit_price_micro_lamports,
            ));
        }

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&publish_keypair.pubkey()),
            &vec![publish_keypair],
            network_state.blockhash,
        );

        let signature = self
            .rpc_client
            .send_transaction_with_config(
                &transaction,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..RpcSendTransactionConfig::default()
                },
            )
            .await?;
        debug!(self.logger, "sent upd_price transaction"; "signature" => signature.to_string(), "instructions" => instructions.len(), "price_accounts" => format!("{:?}", price_accounts));

        self.inflight_transactions_tx.send(signature).await?;

        Ok(())
    }

    fn create_instruction_without_accumulator(
        &self,
        publish_pubkey: Pubkey,
        price_id: Pubkey,
        price_info: &PriceInfo,
        current_slot: u64,
    ) -> Result<Instruction> {
        Ok(Instruction {
            program_id: self.key_store.program_key,
            accounts:   vec![
                AccountMeta {
                    pubkey:      publish_pubkey,
                    is_signer:   true,
                    is_writable: true,
                },
                AccountMeta {
                    pubkey:      price_id,
                    is_signer:   false,
                    is_writable: true,
                },
                AccountMeta {
                    pubkey:      clock::id(),
                    is_signer:   false,
                    is_writable: false,
                },
            ],
            data:       bincode::DefaultOptions::new()
                .with_little_endian()
                .with_fixint_encoding()
                .serialize(
                    &(UpdPriceCmd {
                        version:  PYTH_ORACLE_VERSION,
                        cmd:      UPDATE_PRICE_NO_FAIL_ON_ERROR,
                        status:   price_info.status,
                        unused_:  0,
                        price:    price_info.price,
                        conf:     price_info.conf,
                        pub_slot: current_slot,
                    }),
                )?,
        })
    }

    fn create_instruction_with_accumulator(
        &self,
        publish_pubkey: Pubkey,
        price_id: Pubkey,
        price_info: &PriceInfo,
        current_slot: u64,
        accumulator_program_key: Pubkey,
    ) -> Result<Instruction> {
        let (whitelist_pubkey, _whitelist_bump) = Pubkey::find_program_address(
            &["message".as_bytes(), "whitelist".as_bytes()],
            &accumulator_program_key,
        );

        let (oracle_auth_pda, _) = Pubkey::find_program_address(
            &[b"upd_price_write", &accumulator_program_key.to_bytes()],
            &self.key_store.program_key,
        );

        let (accumulator_data_pubkey, _accumulator_data_pubkey) = Pubkey::find_program_address(
            &[
                &oracle_auth_pda.to_bytes(),
                "message".as_bytes(),
                &price_id.to_bytes(),
            ],
            &accumulator_program_key,
        );

        Ok(Instruction {
            program_id: self.key_store.program_key,
            accounts:   vec![
                AccountMeta {
                    pubkey:      publish_pubkey,
                    is_signer:   true,
                    is_writable: true,
                },
                AccountMeta {
                    pubkey:      price_id,
                    is_signer:   false,
                    is_writable: true,
                },
                AccountMeta {
                    pubkey:      clock::id(),
                    is_signer:   false,
                    is_writable: false,
                },
                // accumulator program key
                AccountMeta {
                    pubkey:      accumulator_program_key,
                    is_signer:   false,
                    is_writable: false,
                },
                // whitelist
                AccountMeta {
                    pubkey:      whitelist_pubkey,
                    is_signer:   false,
                    is_writable: false,
                },
                // oracle_auth_pda
                AccountMeta {
                    pubkey:      oracle_auth_pda,
                    is_signer:   false,
                    is_writable: false,
                },
                // accumulator_data
                AccountMeta {
                    pubkey:      accumulator_data_pubkey,
                    is_signer:   false,
                    is_writable: true,
                },
            ],
            data:       bincode::DefaultOptions::new()
                .with_little_endian()
                .with_fixint_encoding()
                .serialize(
                    &(UpdPriceCmd {
                        version:  PYTH_ORACLE_VERSION,
                        cmd:      UPDATE_PRICE_NO_FAIL_ON_ERROR,
                        status:   price_info.status,
                        unused_:  0,
                        price:    price_info.price,
                        conf:     price_info.conf,
                        pub_slot: current_slot,
                    }),
                )?,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NetworkState {
    blockhash:    Hash,
    current_slot: u64,
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

    /// Logger
    logger: Logger,
}

impl NetworkStateQuerier {
    pub fn new(
        rpc_endpoint: &str,
        rpc_timeout: Duration,
        query_interval: Interval,
        network_state_tx: watch::Sender<NetworkState>,
        logger: Logger,
    ) -> Self {
        NetworkStateQuerier {
            rpc_client: RpcClient::new_with_timeout(rpc_endpoint.to_string(), rpc_timeout),
            query_interval,
            network_state_tx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.query_interval.tick().await;
            if let Err(err) = self.query_network_state().await {
                error!(self.logger, "{}", err);
                debug!(self.logger, "error context"; "context" => format!("{:?}", err));
            }
        }
    }

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

mod transaction_monitor {
    use {
        anyhow::Result,
        serde::{
            Deserialize,
            Serialize,
        },
        slog::Logger,
        solana_client::nonblocking::rpc_client::RpcClient,
        solana_sdk::{
            commitment_config::CommitmentConfig,
            signature::Signature,
        },
        std::{
            collections::VecDeque,
            time::Duration,
        },
        tokio::{
            sync::mpsc,
            time::{
                self,
                Interval,
            },
        },
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

    /// TransactionMonitor monitors the percentage of recently sent transactions that
    /// have been successfully confirmed.
    pub struct TransactionMonitor {
        config: Config,

        /// The RPC client
        rpc_client: RpcClient,

        /// Channel the signatures of transactions we have sent are received.
        transactions_rx: mpsc::Receiver<Signature>,

        /// Vector storing the signatures of transactions we have sent
        sent_transactions: VecDeque<Signature>,

        /// Interval with which to poll the status of transactions
        poll_interval: Interval,

        logger: Logger,
    }

    impl TransactionMonitor {
        pub fn new(
            config: Config,
            rpc_url: &str,
            rpc_timeout: Duration,
            transactions_rx: mpsc::Receiver<Signature>,
            logger: Logger,
        ) -> Self {
            let poll_interval = time::interval(config.poll_interval_duration);
            let rpc_client = RpcClient::new_with_timeout(rpc_url.to_string(), rpc_timeout);
            TransactionMonitor {
                config,
                rpc_client,
                sent_transactions: VecDeque::new(),
                transactions_rx,
                poll_interval,
                logger,
            }
        }

        pub async fn run(&mut self) {
            loop {
                if let Err(err) = self.handle_next().await {
                    error!(self.logger, "{}", err);
                    debug!(self.logger, "error context"; "context" => format!("{:?}", err));
                }
            }
        }

        async fn handle_next(&mut self) -> Result<()> {
            tokio::select! {
                Some(signature) = self.transactions_rx.recv() => {
                    self.add_transaction(signature);
                    Ok(())
                }
                _ = self.poll_interval.tick() => {
                    self.poll_transactions_status().await
                }
            }
        }

        fn add_transaction(&mut self, signature: Signature) {
            debug!(self.logger, "monitoring new transaction"; "signature" => signature.to_string());

            // Add the new transaction to the list
            self.sent_transactions.push_back(signature);

            // Pop off the oldest transaction if necessary
            if self.sent_transactions.len() > self.config.max_transactions {
                self.sent_transactions.pop_front();
            }
        }

        async fn poll_transactions_status(&mut self) -> Result<()> {
            if self.sent_transactions.is_empty() {
                return Ok(());
            }

            let signatures_contiguous = self.sent_transactions.make_contiguous();

            // Poll the status of each transaction, in a single RPC request
            let statuses = self
                .rpc_client
                .get_signature_statuses(signatures_contiguous)
                .await?
                .value;

            debug!(self.logger, "Processing Signature Statuses"; "statuses" => format!("{:?}", statuses));

            // Determine the percentage of the recently sent transactions that have successfully been committed
            // TODO: expose as metric
            let confirmed = statuses
                .into_iter()
                .zip(signatures_contiguous)
                .map(|(status, sig)| status.map(|some_status| (some_status, sig))) // Collate Some() statuses with their tx signatures before flatten()
                .flatten()
                .filter(|(status, sig)| {
                    if let Some(err) = status.err.as_ref() {
                        warn!(self.logger, "TX status has err value";
                        "error" => err.to_string(),
                        "tx_signature" => sig.to_string(),
                                          )
                    }

                    status.satisfies_commitment(CommitmentConfig::confirmed())
                })
                .count();
            let percentage_confirmed =
                ((confirmed as f64) / (self.sent_transactions.len() as f64)) * 100.0;
            info!(self.logger, "monitoring transaction hit rate"; "percentage confirmed" => format!("{:.}", percentage_confirmed));

            Ok(())
        }
    }
}
