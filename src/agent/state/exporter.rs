use {
    super::{
        local::PriceInfo,
        oracle::PricePublishingMetadata,
        transactions::Transactions,
        State,
    },
    crate::agent::{
        services::exporter::NetworkState,
        solana::network::Network,
        state::{
            global::GlobalStore,
            keypairs::Keypairs,
            local::LocalStore,
        },
    },
    anyhow::{
        anyhow,
        bail,
        Context,
        Result,
    },
    bincode::Options,
    bytemuck::{
        bytes_of,
        cast_slice,
    },
    chrono::Utc,
    futures_util::future::join_all,
    pyth_price_store::accounts::buffer::BufferedPrice,
    pyth_sdk::Identifier,
    pyth_sdk_solana::state::PriceStatus,
    serde::Serialize,
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::RpcSendTransactionConfig,
    },
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction,
        instruction::{
            AccountMeta,
            Instruction,
        },
        pubkey::Pubkey,
        signature::Keypair,
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
    tokio::sync::{
        watch,
        RwLock,
    },
    tracing::instrument,
};

const PYTH_ORACLE_VERSION: u32 = 2;
const UPDATE_PRICE_NO_FAIL_ON_ERROR: i32 = 13;

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

#[derive(Default)]
pub struct ExporterState {
    /// The last state published for each price identifier. Used to
    /// rule out stale data and prevent repetitive publishing of
    /// unchanged prices.
    last_published_state: RwLock<HashMap<pyth_sdk::Identifier, PriceInfo>>,

    /// Currently known permissioned prices of this publisher along with their market hours
    our_prices: RwLock<HashMap<Pubkey, PricePublishingMetadata>>,

    publisher_buffer_key: RwLock<Option<Pubkey>>,

    /// Recent compute unit price in micro lamports (set if dynamic compute unit pricing is enabled)
    recent_compute_unit_price_micro_lamports: RwLock<Option<u64>>,
}

impl ExporterState {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
pub trait Exporter
where
    Self: Keypairs,
    Self: LocalStore,
    Self: GlobalStore,
    Self: Transactions,
{
    async fn record_publish(&self, batch_state: HashMap<Identifier, PriceInfo>);
    async fn get_permissioned_updates(
        &self,
        publish_keypair: &Keypair,
        staleness_threshold: Duration,
        unchanged_publish_threshold: Duration,
    ) -> Result<Vec<PermissionedUpdate>>;
    async fn get_publisher_buffer_key(&self) -> Option<Pubkey>;
    async fn get_recent_compute_unit_price_micro_lamports(&self) -> Option<u64>;
    async fn update_recent_compute_unit_price(
        &self,
        publish_keypair: &Keypair,
        rpc_client: &RpcClient,
        staleness_threshold: Duration,
        unchanged_publish_threshold: Duration,
    ) -> Result<()>;
    async fn update_on_chain_state(
        &self,
        network: Network,
        publish_keypair: Option<&Keypair>,
        publisher_permissions: HashMap<Pubkey, HashMap<Pubkey, PricePublishingMetadata>>,
        publisher_buffer_key: Option<Pubkey>,
    ) -> Result<()>;
}

/// Allow downcasting State into ExporterState for functions that depend on the `Exporter` service.
impl<'a> From<&'a State> for &'a ExporterState {
    fn from(state: &'a State) -> &'a ExporterState {
        &state.exporter
    }
}

#[derive(Debug, Clone)]
pub struct PermissionedUpdate {
    pub feed_id:    pyth_sdk::Identifier,
    pub feed_index: u32,
    pub info:       PriceInfo,
}

#[async_trait::async_trait]
impl<T> Exporter for T
where
    for<'a> &'a T: Into<&'a ExporterState>,
    T: Sync + Send + 'static,
    T: Keypairs,
    T: LocalStore,
    T: GlobalStore,
    T: Transactions,
{
    async fn record_publish(&self, batch_state: HashMap<Identifier, PriceInfo>) {
        self.into()
            .last_published_state
            .write()
            .await
            .extend(batch_state);
    }

    #[instrument(
        skip_all,
        fields(
            publish_keypair             = publish_keypair.pubkey().to_string(),
            staleness_threshold         = staleness_threshold.as_millis(),
            unchanged_publish_threshold = unchanged_publish_threshold.as_millis(),
        )
    )]
    async fn get_permissioned_updates(
        &self,
        publish_keypair: &Keypair,
        staleness_threshold: Duration,
        unchanged_publish_threshold: Duration,
    ) -> Result<Vec<PermissionedUpdate>> {
        let local_store_contents = LocalStore::get_all_price_infos(self).await;
        let now = Utc::now().naive_utc();

        {
            let keys = self.into().our_prices.read().await;
            let keys = keys.keys();
            tracing::debug!(
                our_prices = ?keys,
                publish_pubkey = publish_keypair.pubkey().to_string(),
                "Exporter: filtering prices permissioned to us",
            );
        }

        let last_published_state = self.into().last_published_state.read().await;
        let our_prices = self.into().our_prices.read().await;

        // Filter the contents to only include information we haven't already sent,
        // and to ignore stale information.
        Ok(local_store_contents
            .into_iter()
            .filter(|(_identifier, info)| {
                // Filter out timestamps that are old
                now < info.timestamp + staleness_threshold
            })
            .filter(|(identifier, info)| {
                // Filter out unchanged price data if the max delay wasn't reached
                if let Some(last_info) = last_published_state.get(identifier) {
                    if info.timestamp > last_info.timestamp + unchanged_publish_threshold {
                        true // max delay since last published state reached, we publish anyway
                    } else {
                        !last_info.cmp_no_timestamp(info) // Filter out if data is unchanged
                    }
                } else {
                    true // No prior data found, letting the price through
                }
            })
            .filter_map(|(feed_id, info)| {
                let key_from_id = Pubkey::from(feed_id.clone().to_bytes());
                if let Some(publisher_permission) = our_prices.get(&key_from_id) {
                    let now_utc = Utc::now();
                    let can_publish = publisher_permission.schedule.can_publish_at(&now_utc);

                    if can_publish {
                        Some(PermissionedUpdate {
                            feed_id,
                            feed_index: publisher_permission.feed_index,
                            info,
                        })
                    } else {
                        tracing::debug!(
                            price_account = key_from_id.to_string(),
                            schedule = ?publisher_permission.schedule,
                            utc_time = now_utc.format("%c").to_string(),
                            "Exporter: Attempted to publish price outside market hours",
                        );
                        None
                    }
                } else {
                    // Note: This message is not an error. Some
                    // publishers have different permissions on
                    // primary/secondary networks
                    tracing::debug!(
                        unpermissioned_price_account = key_from_id.to_string(),
                        permissioned_accounts = ?self.into().our_prices,
                        "Exporter: Attempted to publish a price without permission, skipping",
                    );
                    None
                }
            })
            .filter(|update| {
                // Filtering out prices that are being updated too frequently according to publisher_permission.publish_interval
                let last_info = match last_published_state.get(&update.feed_id) {
                    Some(last_info) => last_info,
                    None => {
                        // No prior data found, letting the price through
                        return true;
                    }
                };

                let key_from_id = Pubkey::from((update.feed_id).clone().to_bytes());
                let publisher_metadata = match our_prices.get(&key_from_id) {
                    Some(metadata) => metadata,
                    None => {
                        // Should never happen since we have filtered out the price above
                        return false;
                    }
                };

                if let Some(publish_interval) = publisher_metadata.publish_interval {
                    if update.info.timestamp < last_info.timestamp + publish_interval {
                        // Updating the price too soon after the last update, skipping
                        return false;
                    }
                }
                true
            })
            .collect::<Vec<_>>())
    }

    async fn get_publisher_buffer_key(&self) -> Option<Pubkey> {
        *self.into().publisher_buffer_key.read().await
    }

    async fn get_recent_compute_unit_price_micro_lamports(&self) -> Option<u64> {
        *self
            .into()
            .recent_compute_unit_price_micro_lamports
            .read()
            .await
    }

    #[instrument(
        skip_all,
        fields(
            publish_keypair             = publish_keypair.pubkey().to_string(),
            staleness_threshold         = staleness_threshold.as_millis(),
            unchanged_publish_threshold = unchanged_publish_threshold.as_millis(),
        )
    )]
    async fn update_recent_compute_unit_price(
        &self,
        publish_keypair: &Keypair,
        rpc_client: &RpcClient,
        staleness_threshold: Duration,
        unchanged_publish_threshold: Duration,
    ) -> Result<()> {
        let permissioned_updates = self
            .get_permissioned_updates(
                publish_keypair,
                staleness_threshold,
                unchanged_publish_threshold,
            )
            .await?;
        let price_accounts = permissioned_updates
            .iter()
            .map(|update| Pubkey::from(update.feed_id.to_bytes()))
            .collect::<Vec<_>>();

        *self
            .into()
            .recent_compute_unit_price_micro_lamports
            .write()
            .await =
            estimate_compute_unit_price_micro_lamports(rpc_client, &price_accounts).await?;

        Ok(())
    }

    #[instrument(skip(self, publish_keypair, publisher_permissions))]
    async fn update_on_chain_state(
        &self,
        network: Network,
        publish_keypair: Option<&Keypair>,
        publisher_permissions: HashMap<Pubkey, HashMap<Pubkey, PricePublishingMetadata>>,
        publisher_buffer_key: Option<Pubkey>,
    ) -> Result<()> {
        let publish_keypair = get_publish_keypair(self, network, publish_keypair).await?;
        *self.into().our_prices.write().await = publisher_permissions
            .get(&publish_keypair.pubkey())
            .cloned()
            .unwrap_or_else(|| {
                tracing::warn!(
                    publish_pubkey = &publish_keypair.pubkey().to_string(),
                    "Exporter: No permissioned prices were found for the publishing keypair on-chain. This is expected only on startup.",
                );
                HashMap::new()
            });
        *self.into().publisher_buffer_key.write().await = publisher_buffer_key;

        Ok(())
    }
}

#[instrument(skip(state, publish_keypair))]
pub async fn get_publish_keypair<S>(
    state: &S,
    network: Network,
    publish_keypair: Option<&Keypair>,
) -> Result<Keypair>
where
    S: Exporter,
    S: Keypairs,
{
    if let Some(kp) = publish_keypair.as_ref() {
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

        tracing::debug!("Exporter: Publish keypair is None, requesting remote loaded key");
        let kp = Keypairs::request_keypair(state, network).await?;
        tracing::debug!("Exporter: Keypair received");
        Ok(kp)
    }
}

async fn estimate_compute_unit_price_micro_lamports(
    rpc_client: &RpcClient,
    price_accounts: &[Pubkey],
) -> Result<Option<u64>> {
    let mut slot_compute_fee: BTreeMap<u64, u64> = BTreeMap::new();

    // Maximum allowed number of accounts is 128. So we need to chunk the requests
    let prioritization_fees_batches = futures_util::future::join_all(
        price_accounts
            .chunks(128)
            .map(|price_accounts| rpc_client.get_recent_prioritization_fees(price_accounts)),
    )
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
#[instrument(
    skip(state, client, network_state_rx, publish_keypair, staleness_threshold, permissioned_updates),
    fields(
        publish_keypair     = publish_keypair.pubkey().to_string(),
        staleness_threshold = staleness_threshold.as_millis(),
    )
)]
pub async fn publish_batches<S>(
    state: Arc<S>,
    client: Arc<RpcClient>,
    network: Network,
    network_state_rx: &watch::Receiver<NetworkState>,
    accumulator_key: Option<Pubkey>,
    publish_keypair: &Keypair,
    pyth_oracle_program_key: Pubkey,
    pyth_price_store_program_key: Option<Pubkey>,
    publisher_buffer_key: Option<Pubkey>,
    max_batch_size: usize,
    staleness_threshold: Duration,
    compute_unit_limit: u32,
    compute_unit_price_micro_lamports: Option<u64>,
    maximum_compute_unit_price_micro_lamports: u64,
    maximum_slot_gap_for_dynamic_compute_unit_price: u64,
    dynamic_compute_unit_pricing_enabled: bool,
    permissioned_updates: Vec<PermissionedUpdate>,
) -> Result<()>
where
    S: Sync + Send + 'static,
    S: Keypairs,
    S: LocalStore,
    S: GlobalStore,
    S: Transactions,
    S: Exporter,
{
    if permissioned_updates.is_empty() {
        return Ok(());
    }

    // Split the updates up into batches
    let batches = permissioned_updates.chunks(max_batch_size);

    let mut batch_state = HashMap::new();
    let mut batch_futures = vec![];

    let network_state = *network_state_rx.borrow();
    for batch in batches {
        batch_futures.push(publish_batch(
            state.clone(),
            client.clone(),
            network,
            network_state,
            accumulator_key,
            publish_keypair,
            pyth_oracle_program_key,
            pyth_price_store_program_key,
            publisher_buffer_key,
            batch,
            staleness_threshold,
            compute_unit_limit,
            compute_unit_price_micro_lamports,
            maximum_compute_unit_price_micro_lamports,
            maximum_slot_gap_for_dynamic_compute_unit_price,
            dynamic_compute_unit_pricing_enabled,
        ));

        for update in batch {
            batch_state.insert(update.feed_id, update.info.clone());
        }
    }

    // Wait for all the update requests to complete. Note that this doesn't wait for the
    // transactions themselves to be processed or confirmed, just the RPC requests to return.
    join_all(batch_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Exporter::record_publish(&*state, batch_state).await;
    Ok(())
}

#[instrument(
    skip(state, client, network_state, publish_keypair, batch, staleness_threshold),
    fields(
        publish_keypair     = publish_keypair.pubkey().to_string(),
        blockhash           = network_state.blockhash.to_string(),
        current_slot        = network_state.current_slot,
        staleness_threshold = staleness_threshold.as_millis(),
        batch               = ?batch.iter().map(|update| update.feed_id.to_string()).collect::<Vec<_>>(),
    )
)]
async fn publish_batch<S>(
    state: Arc<S>,
    client: Arc<RpcClient>,
    network: Network,
    network_state: NetworkState,
    accumulator_key: Option<Pubkey>,
    publish_keypair: &Keypair,
    pyth_oracle_program_key: Pubkey,
    pyth_price_store_program_key: Option<Pubkey>,
    publisher_buffer_key: Option<Pubkey>,
    batch: &[PermissionedUpdate],
    staleness_threshold: Duration,
    compute_unit_limit: u32,
    compute_unit_price_micro_lamports_opt: Option<u64>,
    maximum_compute_unit_price_micro_lamports: u64,
    maximum_slot_gap_for_dynamic_compute_unit_price: u64,
    dynamic_compute_unit_pricing_enabled: bool,
) -> Result<()>
where
    S: Sync + Send + 'static,
    S: Keypairs,
    S: LocalStore,
    S: GlobalStore,
    S: Transactions,
    S: Exporter,
{
    let mut instructions = Vec::new();

    let price_accounts = batch
        .iter()
        .map(|update| Pubkey::from(update.feed_id.to_bytes()))
        .collect::<Vec<_>>();

    let now = Utc::now().naive_utc();
    let mut updates = Vec::new();
    // Refresh the data in the batch
    let local_store_contents = LocalStore::get_all_price_infos(&*state).await;
    for update in batch {
        let mut update = update.clone();
        update.info = local_store_contents
            .get(&update.feed_id)
            .ok_or_else(|| anyhow!("price identifier not found in local store"))
            .with_context(|| update.feed_id.to_string())?
            .clone();

        let stale_price = now > update.info.timestamp + staleness_threshold;
        if stale_price {
            continue;
        }
        updates.push(update);
    }

    if let Some(pyth_price_store_program_key) = pyth_price_store_program_key {
        let instruction = create_instruction_with_price_store_program(
            publish_keypair.pubkey(),
            pyth_price_store_program_key,
            publisher_buffer_key.context("failed to fetch publisher buffer key")?,
            updates,
        )?;
        instructions.push(instruction);
    } else {
        for update in updates {
            let instruction = if let Some(accumulator_program_key) = accumulator_key {
                create_instruction_with_accumulator(
                    publish_keypair.pubkey(),
                    pyth_oracle_program_key,
                    Pubkey::from(update.feed_id.to_bytes()),
                    &update.info,
                    network_state.current_slot,
                    accumulator_program_key,
                )?
            } else {
                create_instruction_without_accumulator(
                    publish_keypair.pubkey(),
                    pyth_oracle_program_key,
                    Pubkey::from(update.feed_id.to_bytes()),
                    &update.info,
                    network_state.current_slot,
                )?
            };

            instructions.push(instruction);
        }
    }

    // Pay priority fees, if configured
    let total_compute_limit: u32 = compute_unit_limit * instructions.len() as u32;

    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
        total_compute_limit,
    ));

    // Calculate the compute unit price in micro lamports
    let mut compute_unit_price_micro_lamports = None;

    // If the unit price value is set, use it as the minimum price
    if let Some(price) = compute_unit_price_micro_lamports_opt {
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
    if dynamic_compute_unit_pricing_enabled {
        // Use the estimated previous price if it is higher
        // than the current price.
        let recent_compute_unit_price_micro_lamports =
            Exporter::get_recent_compute_unit_price_micro_lamports(&*state).await;

        if let Some(estimated_recent_price) = recent_compute_unit_price_micro_lamports {
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
        let result = GlobalStore::price_accounts(
            &*state,
            network,
            price_accounts.clone().into_iter().collect(),
        )
        .await?;

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
            let exponential_price = maximum_compute_unit_price_micro_lamports
                >> maximum_slot_gap_for_dynamic_compute_unit_price.saturating_sub(slot_gap);

            compute_unit_price_micro_lamports = compute_unit_price_micro_lamports
                .map(|price| price.max(exponential_price))
                .or(Some(exponential_price));
        }
    }

    if let Some(mut compute_unit_price_micro_lamports) = compute_unit_price_micro_lamports {
        compute_unit_price_micro_lamports =
            compute_unit_price_micro_lamports.min(maximum_compute_unit_price_micro_lamports);

        tracing::debug!(
            unit_price = compute_unit_price_micro_lamports,
            "setting compute unit price",
        );
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

    tokio::spawn(async move {
        let signature = match client
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
                tracing::error!(err = ?err, "Exporter: failed to send transaction.");
                return;
            }
        };

        tracing::debug!(
            signature = signature.to_string(),
            instructions = instructions.len(),
            price_accounts = ?price_accounts,
            "Sent upd_price transaction.",
        );

        Transactions::add_transaction(&*state, signature).await;
    });

    Ok(())
}

fn create_instruction_without_accumulator(
    publish_pubkey: Pubkey,
    pyth_oracle_program_key: Pubkey,
    price_id: Pubkey,
    price_info: &PriceInfo,
    current_slot: u64,
) -> Result<Instruction> {
    Ok(Instruction {
        program_id: pyth_oracle_program_key,
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

fn create_instruction_with_price_store_program(
    publish_pubkey: Pubkey,
    pyth_price_store_program_key: Pubkey,
    publisher_buffer_key: Pubkey,
    prices: Vec<PermissionedUpdate>,
) -> Result<Instruction> {
    use pyth_price_store::instruction::{
        Instruction as PublishInstruction,
        SubmitPricesArgsHeader,
        PUBLISHER_CONFIG_SEED,
    };
    let (publisher_config_key, publisher_config_bump) = Pubkey::find_program_address(
        &[PUBLISHER_CONFIG_SEED.as_bytes(), &publish_pubkey.to_bytes()],
        &pyth_price_store_program_key,
    );

    let mut values = Vec::new();
    for update in prices {
        if update.feed_index == 0 {
            bail!("no feed index for feed {:?}", update.feed_id);
        }
        values.push(BufferedPrice::new(
            update.feed_index,
            (update.info.status as u8).into(),
            update.info.price,
            update.info.conf,
        )?);
    }
    let mut data = vec![PublishInstruction::SubmitPrices as u8];
    data.extend_from_slice(bytes_of(&SubmitPricesArgsHeader {
        publisher_config_bump,
    }));
    data.extend(cast_slice(&values));

    let instruction = Instruction {
        program_id: pyth_price_store_program_key,
        accounts: vec![
            AccountMeta {
                pubkey:      publish_pubkey,
                is_signer:   true,
                is_writable: true,
            },
            AccountMeta {
                pubkey:      publisher_config_key,
                is_signer:   false,
                is_writable: false,
            },
            AccountMeta {
                pubkey:      publisher_buffer_key,
                is_signer:   false,
                is_writable: true,
            },
        ],
        data,
    };
    Ok(instruction)
}

fn create_instruction_with_accumulator(
    publish_pubkey: Pubkey,
    pyth_oracle_program_key: Pubkey,
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
        &pyth_oracle_program_key,
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
        program_id: pyth_oracle_program_key,
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
