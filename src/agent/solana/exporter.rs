use {
    self::transaction_monitor::TransactionMonitor,
    super::{
        super::store::{
            self,
            local::PriceInfo,
            PriceIdentifier,
        },
        key_store,
        network::Network,
    },
    crate::agent::{
        market_schedule::MarketSchedule,
        remote_keypair_loader::{
            KeypairRequest,
            RemoteKeypairLoader,
        },
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
            BTreeMap,
            HashMap,
        },
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc::{
                self,
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
            // The largest transactions appear to be about ~12000 CUs. We leave ourselves some breathing room.
            compute_unit_limit:                              40000,
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

pub fn spawn_exporter(
    config: Config,
    network: Network,
    rpc_url: &str,
    rpc_timeout: Duration,
    publisher_permissions_rx: mpsc::Receiver<HashMap<Pubkey, HashMap<Pubkey, MarketSchedule>>>,
    key_store: KeyStore,
    local_store_tx: Sender<store::local::Message>,
    global_store_tx: Sender<store::global::Lookup>,
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
        network,
        rpc_url,
        rpc_timeout,
        key_store,
        local_store_tx,
        global_store_tx,
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
    rpc_client: Arc<RpcClient>,

    config: Config,

    /// The exporter network
    network: Network,

    /// Interval at which to publish updates
    publish_interval: Interval,

    /// The Key Store
    key_store: KeyStore,

    /// Channel on which to communicate with the local store
    local_store_tx: Sender<store::local::Message>,

    /// Channel on which to communicate with the global store
    global_store_tx: Sender<store::global::Lookup>,

    /// The last state published for each price identifier. Used to
    /// rule out stale data and prevent repetitive publishing of
    /// unchanged prices.
    last_published_state: HashMap<PriceIdentifier, PriceInfo>,

    /// Watch receiver channel to access the current network state
    network_state_rx: watch::Receiver<NetworkState>,

    // Channel on which to send inflight transactions to the transaction monitor
    inflight_transactions_tx: Sender<Signature>,

    /// publisher => { permissioned_price => market hours } as read by the oracle module
    publisher_permissions_rx: mpsc::Receiver<HashMap<Pubkey, HashMap<Pubkey, MarketSchedule>>>,

    /// Currently known permissioned prices of this publisher along with their market hours
    our_prices: HashMap<Pubkey, MarketSchedule>,

    /// Interval to update the dynamic price (if enabled)
    dynamic_compute_unit_price_update_interval: Interval,

    /// Recent compute unit price in micro lamports (set if dynamic compute unit pricing is enabled)
    recent_compute_unit_price_micro_lamports: Option<u64>,

    keypair_request_tx: Sender<KeypairRequest>,

    logger: Logger,
}

impl Exporter {
    pub fn new(
        config: Config,
        network: Network,
        rpc_url: &str,
        rpc_timeout: Duration,
        key_store: KeyStore,
        local_store_tx: Sender<store::local::Message>,
        global_store_tx: Sender<store::global::Lookup>,
        network_state_rx: watch::Receiver<NetworkState>,
        inflight_transactions_tx: Sender<Signature>,
        publisher_permissions_rx: mpsc::Receiver<HashMap<Pubkey, HashMap<Pubkey, MarketSchedule>>>,
        keypair_request_tx: mpsc::Sender<KeypairRequest>,
        logger: Logger,
    ) -> Self {
        let publish_interval = time::interval(config.publish_interval_duration);
        Exporter {
            rpc_client: Arc::new(RpcClient::new_with_timeout(
                rpc_url.to_string(),
                rpc_timeout,
            )),
            config,
            network,
            publish_interval,
            key_store,
            local_store_tx,
            global_store_tx,
            last_published_state: HashMap::new(),
            network_state_rx,
            inflight_transactions_tx,
            publisher_permissions_rx,
            our_prices: HashMap::new(),
            dynamic_compute_unit_price_update_interval: tokio::time::interval(
                time::Duration::from_secs(1),
            ),
            recent_compute_unit_price_micro_lamports: None,
            keypair_request_tx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.publish_interval.tick() => {
                    if let Err(err) = self.publish_updates().await {
                        error!(self.logger, "{}", err);
                        debug!(self.logger, "error context"; "context" => format!("{:?}", err));
                    }
                }
                _ = self.dynamic_compute_unit_price_update_interval.tick() => {
                    if self.config.dynamic_compute_unit_pricing_enabled {
                        if let Err(err) = self.update_recent_compute_unit_price().await {
                            error!(self.logger, "{}", err);
                            debug!(self.logger, "error context"; "context" => format!("{:?}", err));
                        }
                    }
                }
            }
        }
    }

    async fn update_recent_compute_unit_price(&mut self) -> Result<()> {
        let permissioned_updates = self.get_permissioned_updates().await?;
        let price_accounts = permissioned_updates
            .iter()
            .map(|(identifier, _)| Pubkey::from(identifier.to_bytes()))
            .collect::<Vec<_>>();
        self.recent_compute_unit_price_micro_lamports = self
            .estimate_compute_unit_price_micro_lamports(&price_accounts)
            .await?;
        Ok(())
    }

    async fn estimate_compute_unit_price_micro_lamports(
        &self,
        price_accounts: &[Pubkey],
    ) -> Result<Option<u64>> {
        let mut slot_compute_fee: BTreeMap<u64, u64> = BTreeMap::new();

        // Maximum allowed number of accounts is 128. So we need to chunk the requests
        let prioritization_fees_batches =
            futures_util::future::join_all(price_accounts.chunks(128).map(|price_accounts| {
                self.rpc_client
                    .get_recent_prioritization_fees(price_accounts)
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to get recent prioritization fees")?;

        prioritization_fees_batches
            .iter()
            .for_each(|prioritization_fees| {
                prioritization_fees.iter().for_each(|fee| {
                    // Get the maximum prioritaztion fee over all fees retrieved for this slot
                    let prioritization_fee = slot_compute_fee
                        .get(&fee.slot)
                        .map_or(fee.prioritization_fee, |other| {
                            fee.prioritization_fee.max(*other)
                        });
                    slot_compute_fee.insert(fee.slot, prioritization_fee);
                })
            });

        let mut prioritization_fees = slot_compute_fee
            .iter()
            .rev()
            .take(20) // Only take the last 20 slot priority fees
            .map(|(_, fee)| *fee)
            .collect::<Vec<_>>();

        prioritization_fees.sort();

        let median_priority_fee = prioritization_fees
            .get(prioritization_fees.len() / 2)
            .cloned();

        Ok(median_priority_fee)
    }

    async fn get_publish_keypair(&self) -> Result<Keypair> {
        if let Some(kp) = self.key_store.publish_keypair.as_ref() {
            // It's impossible to sanely return a &Keypair in the
            // other if branch, so we clone the reference.
            Ok(Keypair::from_bytes(&kp.to_bytes())
                .context("INTERNAL: Could not convert keypair to bytes and back")?)
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
            Ok(kp)
        }
    }

    async fn get_permissioned_updates(&mut self) -> Result<Vec<(PriceIdentifier, PriceInfo)>> {
        let local_store_contents = self.fetch_local_store_contents().await?;

        let now = Utc::now().timestamp();

        // Filter the contents to only include information we haven't already sent,
        // and to ignore stale information.
        let fresh_updates = local_store_contents
            .into_iter()
            .filter(|(_identifier, info)| {
                // Filter out timestamps that are old
                (now - info.timestamp) < self.config.staleness_threshold.as_secs() as i64
            })
            .filter(|(identifier, info)| {
                // Filter out unchanged price data if the max delay wasn't reached

                if let Some(last_info) = self.last_published_state.get(identifier) {
                    if info.timestamp.saturating_sub(last_info.timestamp)
                        > self.config.unchanged_publish_threshold.as_secs() as i64
                    {
                        true // max delay since last published state reached, we publish anyway
                    } else {
                        !last_info.cmp_no_timestamp(info) // Filter out if data is unchanged
                    }
                } else {
                    true // No prior data found, letting the price through
                }
            })
            .collect::<Vec<_>>();

        let publish_keypair = self.get_publish_keypair().await?;

        self.update_our_prices(&publish_keypair.pubkey());

        debug!(self.logger, "Exporter: filtering prices permissioned to us";
               "our_prices" => format!("{:?}", self.our_prices.keys()),
               "publish_pubkey" => publish_keypair.pubkey().to_string(),
        );

        // Get a fresh system time
        let now = Utc::now();

        // Filter out price accounts we're not permissioned to update
        Ok(fresh_updates
            .into_iter()
            .filter(|(id, _data)| {
                let key_from_id = Pubkey::from((*id).clone().to_bytes());
                if let Some(schedule) = self.our_prices.get(&key_from_id) {
                    let ret = schedule.can_publish_at(&now);

                    if !ret {
                        debug!(self.logger, "Exporter: Attempted to publish price outside market hours";
                            "price_account" => key_from_id.to_string(),
                            "schedule" => format!("{:?}", schedule),
                            "utc_time" => now.format("%c").to_string(),
                        );
                    }

                    ret
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
            .collect::<Vec<_>>())
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
        let permissioned_updates = self.get_permissioned_updates().await?;

        if permissioned_updates.is_empty() {
            return Ok(());
        }

        // Split the updates up into batches
        let batches = permissioned_updates.chunks(self.config.max_batch_size);

        let mut batch_state = HashMap::new();
        let mut batch_futures = vec![];

        for batch in batches {
            batch_futures.push(self.publish_batch(batch));

            for (identifier, info) in batch {
                batch_state.insert(*identifier, (*info).clone());
            }
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
                            HashMap::new()
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

    async fn publish_batch(&self, batch: &[(Identifier, PriceInfo)]) -> Result<()> {
        let publish_keypair = self.get_publish_keypair().await?;

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
            .map(|(identifier, _)| Pubkey::from(identifier.to_bytes()))
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
                    Pubkey::from(identifier.to_bytes()),
                    price_info,
                    network_state.current_slot,
                    accumulator_program_key,
                )?
            } else {
                self.create_instruction_without_accumulator(
                    publish_keypair.pubkey(),
                    Pubkey::from(identifier.to_bytes()),
                    price_info,
                    network_state.current_slot,
                )?
            };

            instructions.push(instruction);
        }

        // Pay priority fees, if configured
        let total_compute_limit: u32 = self.config.compute_unit_limit * instructions.len() as u32;

        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            total_compute_limit,
        ));

        // Calculate the compute unit price in micro lamports
        let mut compute_unit_price_micro_lamports = None;

        // If the unit price value is set, use it as the minimum price
        if let Some(price) = self.config.compute_unit_price_micro_lamports {
            compute_unit_price_micro_lamports = Some(price);
        }

        // If dynamic compute unit pricing is enabled, we use the following two methods to calculate an
        // estimate of the price:
        // - We exponentially increase price based on the price staleness (slot gap between the
        // current slot and the oldest slot amongst all the accounts in this batch).
        // - We use the network recent prioritization fees to get the minimum unit price
        // that landed a transaction using Pyth price accounts (permissioned to this publisher)
        // as writable. We take the median over the last 20 slots and divide it by two to make
        // sure that it decays over time. The API doesn't return the priority fees for the Pyth
        // price reads and so, this reflects the unit price that publishers have paid in the
        // pverious slots.
        //
        // The two methods above combined act like massively increasing the price when they cannot
        // land transactions on-chain that decays over time. The decaying behaviour is important to
        // keep the uptime high during congestion whereas without it we would publish price after a
        // large gap and then we can publish it again after the next large gap.
        if self.config.dynamic_compute_unit_pricing_enabled {
            // Use the estimated previous price if it is higher
            // than the current price.
            if let Some(estimated_recent_price) = self.recent_compute_unit_price_micro_lamports {
                // Get the estimated compute unit price and wrap it so it stays below the maximum
                // total compute unit fee. We additionally divide such price by 2 to create an
                // exponential decay. This will make sure that a spike doesn't get propagated
                // forever.
                let estimated_price = estimated_recent_price >> 1;

                compute_unit_price_micro_lamports = compute_unit_price_micro_lamports
                    .map(|price| price.max(estimated_price))
                    .or(Some(estimated_price));
            }

            // Use exponentially higher price if this publisher hasn't published in a while for the accounts
            // in this batch. This will use the maximum total compute unit fee if the publisher
            // hasn't updated for >= MAXIMUM_SLOT_GAP_FOR_DYNAMIC_COMPUTE_UNIT_PRICE slots.
            let (result_tx, result_rx) = oneshot::channel();
            self.global_store_tx
                .send(store::global::Lookup::LookupPriceAccounts {
                    network: self.network,
                    price_ids: price_accounts.clone().into_iter().collect(),
                    result_tx,
                })
                .await?;

            let result = result_rx.await??;

            // Calculate the maximum slot difference between the publisher latest slot and
            // current slot amongst all the accounts. Here, the aggregate slot is
            // used instead of the publishers latest update to avoid overpaying.
            let oldest_slot = result
                .values()
                .filter(|account| account.min_pub != 255) // Only consider live price accounts
                .flat_map(|account| {
                    account
                        .comp
                        .iter()
                        .find(|c| c.publisher == publish_keypair.pubkey())
                        .map(|c| c.latest.pub_slot.max(account.agg.pub_slot))
                })
                .min();

            if let Some(oldest_slot) = oldest_slot {
                let slot_gap = network_state.current_slot.saturating_sub(oldest_slot);

                // Set the dynamic price exponentially based on the slot gap. If the max slot gap is
                // 25, on this number (or more) the maximum unit price is paid, and then on slot 24 it
                // is half of that and gets halved each lower slot. Given that we have max total
                // compute price of 10**12 and 250k compute units in one tx (12 updates) these are the
                // estimated prices based on slot gaps:
                // 25 (or more): 4_000_000
                // 20          :   125_000
                // 18          :    31_250
                // 15          :     3_906
                // 13          :       976
                // 10          :       122
                let exponential_price = self.config.maximum_compute_unit_price_micro_lamports
                    >> self
                        .config
                        .maximum_slot_gap_for_dynamic_compute_unit_price
                        .saturating_sub(slot_gap);

                compute_unit_price_micro_lamports = compute_unit_price_micro_lamports
                    .map(|price| price.max(exponential_price))
                    .or(Some(exponential_price));
            }
        }

        if let Some(mut compute_unit_price_micro_lamports) = compute_unit_price_micro_lamports {
            compute_unit_price_micro_lamports = compute_unit_price_micro_lamports
                .min(self.config.maximum_compute_unit_price_micro_lamports);

            debug!(self.logger, "setting compute unit price"; "unit_price" => compute_unit_price_micro_lamports);
            instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                compute_unit_price_micro_lamports,
            ));
        }

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&publish_keypair.pubkey()),
            &vec![&publish_keypair],
            network_state.blockhash,
        );

        let tx = self.inflight_transactions_tx.clone();
        let logger = self.logger.clone();
        let rpc_client = self.rpc_client.clone();

        // Fire this off in a separate task so we don't block the main thread of the exporter
        tokio::spawn(async move {
            let signature = match rpc_client
                .send_transaction_with_config(
                    &transaction,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        ..RpcSendTransactionConfig::default()
                    },
                )
                .await
            {
                Ok(signature) => signature,
                Err(err) => {
                    error!(logger, "{}", err);
                    debug!(logger, "error context"; "context" => format!("{:?}", err));
                    return;
                }
            };

            debug!(logger, "sent upd_price transaction"; "signature" => signature.to_string(), "instructions" => instructions.len(), "price_accounts" => format!("{:?}", price_accounts));

            if let Err(err) = tx.send(signature).await {
                error!(logger, "{}", err);
                debug!(logger, "error context"; "context" => format!("{:?}", err));
            }
        });

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
