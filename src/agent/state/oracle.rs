#[allow(deprecated)]
use crate::agent::legacy_schedule::LegacySchedule;
use {
    super::{
        super::solana::network::Network,
        exporter::Exporter,
    },
    crate::agent::{
        market_schedule::MarketSchedule,
        state::{
            global::Update,
            Prices,
            State,
        },
    },
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    pyth_price_store::instruction::PUBLISHER_CONFIG_SEED,
    pyth_sdk_solana::state::{
        load_product_account,
        GenericPriceAccount,
        PriceComp,
        PythnetPriceAccount,
        SolanaPriceAccount,
    },
    serde::{
        Deserialize,
        Serialize,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentLevel,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
    },
    std::{
        collections::{
            HashMap,
            HashSet,
        },
        sync::Arc,
        time::Duration,
    },
    tokio::sync::RwLock,
    tracing::instrument,
};

#[derive(Debug)]
pub struct ProductEntry {
    pub account_data:     pyth_sdk_solana::state::ProductAccount,
    pub schedule:         MarketSchedule,
    pub price_accounts:   Vec<Pubkey>,
    pub publish_interval: Option<Duration>,
}

#[derive(Default, Debug, Clone)]
pub struct PricePublishingMetadata {
    pub schedule:         MarketSchedule,
    pub publish_interval: Option<Duration>,
    pub feed_index:       u32,
}

/// This shim is used to abstract over SolanaPriceAccount and PythnetPriceAccount so we
/// can iterate over either of these. The API is intended to force users to be aware of
/// the account type they have, and so doesn't provide this abstraction (a good thing)
/// and the agent should implement this in a better way.
///
/// For now, to implement the abstraction in the smallest way possible we use a shim
/// type that uses the size of the accounts to determine the underlying representation
/// and construct the right one regardless of which network we read. This will only work
/// as long as we don't care about any extended fields.
///
/// TODO: Refactor the agent's network handling code.
#[derive(Copy, Clone, Debug)]
pub struct PriceEntry {
    // We intentionally act as if we have a truncated account where the underlying memory is unavailable.
    account:  GenericPriceAccount<0, ()>,
    pub comp: [PriceComp; 64],
}

impl From<SolanaPriceAccount> for PriceEntry {
    fn from(other: SolanaPriceAccount) -> PriceEntry {
        unsafe {
            // NOTE: We know the size is 32 because It's a Solana account. This is for tests only.
            let comp_mem = std::slice::from_raw_parts(other.comp.as_ptr(), 32);
            let account =
                *(&other as *const SolanaPriceAccount as *const GenericPriceAccount<0, ()>);
            let mut comp = [PriceComp::default(); 64];
            comp[0..32].copy_from_slice(comp_mem);
            PriceEntry { account, comp }
        }
    }
}

impl PriceEntry {
    /// Construct the right underlying GenericPriceAccount based on the account size.
    #[instrument(skip(acc))]
    pub fn load_from_account(acc: &[u8]) -> Option<Self> {
        unsafe {
            let size = match acc.len() {
                n if n == std::mem::size_of::<SolanaPriceAccount>() => 32,
                n if n == std::mem::size_of::<PythnetPriceAccount>() => 64,
                _ => return None,
            };

            // Getting a pointer to avoid copying the account
            let account_ptr = &*(acc.as_ptr() as *const GenericPriceAccount<0, ()>);
            let comp_mem = std::slice::from_raw_parts(account_ptr.comp.as_ptr(), size);
            let mut comp = [PriceComp::default(); 64];
            comp[0..size].copy_from_slice(comp_mem);
            Some(Self {
                account: *account_ptr,
                comp,
            })
        }
    }
}

/// Implement `Deref` so we can access the underlying account fields.
impl std::ops::Deref for PriceEntry {
    type Target = GenericPriceAccount<0, ()>;
    fn deref(&self) -> &Self::Target {
        &self.account
    }
}

#[derive(Default, Debug)]
pub struct Data {
    pub product_accounts:      HashMap<Pubkey, Arc<ProductEntry>>,
    pub price_accounts:        HashMap<Pubkey, Arc<PriceEntry>>,
    /// publisher => {their permissioned price accounts => price publishing metadata}
    pub publisher_permissions: HashMap<Pubkey, HashMap<Pubkey, PricePublishingMetadata>>,
    pub publisher_buffer_key:  Option<Pubkey>,
}

fn default_handle_price_account_update_channel_size() -> usize {
    1000
}

fn default_handle_price_account_update_worker_poll_size() -> usize {
    25
}

fn default_subscriber_finished_min_time() -> Duration {
    Duration::from_secs(30)
}

fn default_subscriber_finished_sleep_time() -> Duration {
    Duration::from_secs(1)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Config {
    /// The commitment level to use when reading data from the RPC node.
    pub commitment:               CommitmentLevel,
    /// The interval with which to poll account information.
    #[serde(with = "humantime_serde")]
    pub poll_interval_duration:   Duration,
    /// Whether subscribing to account updates over websocket is enabled
    pub subscriber_enabled:       bool,
    /// Capacity of the channel over which the Subscriber sends updates to the Oracle
    pub updates_channel_capacity: usize,
    /// Capacity of the channel over which the Poller sends data to the Oracle
    pub data_channel_capacity:    usize,

    /// Ask the RPC for up to this many product/price accounts in a
    /// single request. Tune this setting if you're experiencing
    /// timeouts on data fetching. In order to keep concurrent open
    /// socket count at bay, the batches are looked up sequentially,
    /// trading off overall time it takes to fetch all symbols.
    pub max_lookup_batch_size: usize,

    /// Number of workers used to wait for the handle_price_account_update
    #[serde(default = "default_handle_price_account_update_worker_poll_size")]
    pub handle_price_account_update_worker_poll_size: usize,
    /// Channel size used to wait for the handle_price_account_update
    #[serde(default = "default_handle_price_account_update_channel_size")]
    pub handle_price_account_update_channel_size: usize,
    /// Minimum time for a subscriber to run
    #[serde(default = "default_subscriber_finished_min_time")]
    pub subscriber_finished_min_time: Duration,
    /// Time to sleep if the subscriber do not run for more than the minimum time
    #[serde(default = "default_subscriber_finished_sleep_time")]
    pub subscriber_finished_sleep_time: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            commitment:               CommitmentLevel::Confirmed,
            poll_interval_duration:   Duration::from_secs(5),
            subscriber_enabled:       true,
            updates_channel_capacity: 10000,
            data_channel_capacity:    10000,
            max_lookup_batch_size:    100,
            handle_price_account_update_worker_poll_size:
                default_handle_price_account_update_worker_poll_size(),
            handle_price_account_update_channel_size:
                default_handle_price_account_update_channel_size(),
            subscriber_finished_min_time: default_subscriber_finished_min_time(),
            subscriber_finished_sleep_time: default_subscriber_finished_sleep_time(),
        }
    }
}

#[derive(Default)]
pub struct OracleState {
    data: RwLock<Data>,
}

#[async_trait::async_trait]
pub trait Oracle {
    async fn sync_global_store(&self, network: Network) -> Result<()>;
    async fn poll_updates(
        &self,
        network: Network,
        oracle_program_key: Pubkey,
        publish_keypair: Option<&Keypair>,
        pyth_price_store_program_key: Option<Pubkey>,
        rpc_client: &RpcClient,
        max_lookup_batch_size: usize,
    ) -> Result<()>;
    async fn handle_price_account_update(
        &self,
        network: Network,
        account_key: &Pubkey,
        account: &Account,
    ) -> Result<()>;
}

// Allow downcasting State into Keypairs for functions that depend on the `Keypairs` service.
impl<'a> From<&'a State> for &'a OracleState {
    fn from(state: &'a State) -> &'a OracleState {
        &state.oracle
    }
}

#[async_trait::async_trait]
impl<T> Oracle for T
where
    for<'a> &'a T: Into<&'a OracleState>,
    T: Send + Sync + 'static,
    T: Prices,
    T: Exporter,
{
    #[instrument(skip(self, account_key))]
    async fn handle_price_account_update(
        &self,
        network: Network,
        account_key: &Pubkey,
        account: &Account,
    ) -> Result<()> {
        let mut data = self.into().data.write().await;

        // We are only interested in price account updates, all other types of updates
        // will be fetched using polling.
        if !data.price_accounts.contains_key(account_key) {
            return Ok(());
        }

        let price_entry = PriceEntry::load_from_account(&account.data)
            .with_context(|| format!("load price account {}", account_key))?;

        tracing::debug!(
            pubkey = account_key.to_string(),
            price = price_entry.agg.price,
            conf = price_entry.agg.conf,
            status = ?price_entry.agg.status,
            "Observed on-chain price account update.",
        );

        data.price_accounts.insert(*account_key, price_entry.into());
        drop(data);

        Prices::update_global_price(
            self,
            network,
            &Update::PriceAccountUpdate {
                account_key: *account_key,
                account:     Arc::new(price_entry),
            },
        )
        .await?;

        Ok(())
    }

    /// Poll target Solana based chain for Pyth related accounts.
    #[instrument(skip(self, publish_keypair, rpc_client))]
    async fn poll_updates(
        &self,
        network: Network,
        oracle_program_key: Pubkey,
        publish_keypair: Option<&Keypair>,
        pyth_price_store_program_key: Option<Pubkey>,
        rpc_client: &RpcClient,
        max_lookup_batch_size: usize,
    ) -> Result<()> {
        let mut publisher_permissions = HashMap::new();
        let (product_accounts, price_accounts) =
            fetch_product_and_price_accounts(rpc_client, oracle_program_key, max_lookup_batch_size)
                .await?;

        for (price_key, price_entry) in price_accounts.iter() {
            for component in price_entry.comp {
                if component.publisher == Pubkey::default() {
                    continue;
                }

                let component_pub_entry = publisher_permissions
                    .entry(component.publisher)
                    .or_insert(HashMap::new());

                let publisher_permission = if let Some(prod_entry) =
                    product_accounts.get(&price_entry.prod)
                {
                    PricePublishingMetadata {
                        schedule:         prod_entry.schedule.clone(),
                        publish_interval: prod_entry.publish_interval,
                        // TODO: update sdk
                        feed_index:       price_entry.drv4,
                    }
                } else {
                    tracing::warn!(
                        price = price_key.to_string(),
                        missing_product = price_entry.prod.to_string(),
                        "Oracle: INTERNAL: could not find product from price `prod` field, market hours falling back to 24/7.",
                    );
                    Default::default()
                };

                component_pub_entry.insert(*price_key, publisher_permission);
            }
        }

        let mut publisher_buffer_key = None;
        if let (Some(pyth_price_store_program_key), Some(publish_keypair)) =
            (pyth_price_store_program_key, publish_keypair)
        {
            match fetch_publisher_buffer_key(
                rpc_client,
                pyth_price_store_program_key,
                publish_keypair.pubkey(),
            )
            .await
            {
                Ok(r) => {
                    publisher_buffer_key = Some(r);
                }
                Err(err) => {
                    tracing::warn!("failed to fetch publisher buffer key: {:?}", err);
                }
            }
        }

        let new_data = Data {
            product_accounts,
            price_accounts,
            publisher_permissions,
            publisher_buffer_key,
        };

        let mut data = self.into().data.write().await;
        log_data_diff(&data, &new_data);
        *data = new_data;
        let data_publisher_permissions = data.publisher_permissions.clone();
        let data_publisher_buffer_key = data.publisher_buffer_key;
        drop(data);

        Exporter::update_on_chain_state(
            self,
            network,
            publish_keypair,
            data_publisher_permissions,
            data_publisher_buffer_key,
        )
        .await?;

        Ok(())
    }

    /// Sync Product/Price Accounts found by polling to the Global Store.
    #[instrument(skip(self))]
    async fn sync_global_store(&self, network: Network) -> Result<()> {
        for (product_account_key, product_account) in
            &self.into().data.read().await.product_accounts
        {
            Prices::update_global_price(
                self,
                network,
                &Update::ProductAccountUpdate {
                    account_key: *product_account_key,
                    account:     product_account.clone(),
                },
            )
            .await
            .map_err(|_| anyhow!("failed to notify product account update"))?;
        }

        for (price_account_key, price_account) in &self.into().data.read().await.price_accounts {
            Prices::update_global_price(
                self,
                network,
                &Update::PriceAccountUpdate {
                    account_key: *price_account_key,
                    account:     price_account.clone(),
                },
            )
            .await
            .map_err(|_| anyhow!("failed to notify price account update"))?;
        }
        Ok(())
    }
}

async fn fetch_publisher_buffer_key(
    rpc_client: &RpcClient,
    pyth_price_store_program_key: Pubkey,
    publisher_pubkey: Pubkey,
) -> Result<Pubkey> {
    let (publisher_config_key, _bump) = Pubkey::find_program_address(
        &[
            PUBLISHER_CONFIG_SEED.as_bytes(),
            &publisher_pubkey.to_bytes(),
        ],
        &pyth_price_store_program_key,
    );
    let data = rpc_client.get_account_data(&publisher_config_key).await?;
    let config = pyth_price_store::accounts::publisher_config::read(&data)?;
    Ok(config.buffer_account.into())
}

type ProductAndPriceAccounts = (
    HashMap<Pubkey, Arc<ProductEntry>>,
    HashMap<Pubkey, Arc<PriceEntry>>,
);

#[instrument(skip(rpc_client))]
async fn fetch_product_and_price_accounts(
    rpc_client: &RpcClient,
    oracle_program_key: Pubkey,
    max_lookup_batch_size: usize,
) -> Result<ProductAndPriceAccounts> {
    let mut product_entries = HashMap::new();
    let mut price_entries = HashMap::new();

    let oracle_accounts = rpc_client.get_program_accounts(&oracle_program_key).await?;

    // Go over all the product accounts and partially fill the product entires. The product
    // entires need to have prices inside them which gets filled by going over all the
    // price accounts.
    for (product_key, product) in oracle_accounts.iter().filter_map(|(pubkey, account)| {
        load_product_account(&account.data)
            .ok()
            .map(|product| (pubkey, product))
    }) {
        #[allow(deprecated)]
        let legacy_schedule: LegacySchedule = if let Some((_wsched_key, wsched_val)) =
            product.iter().find(|(k, _v)| *k == "weekly_schedule")
        {
            wsched_val.parse().unwrap_or_else(|err| {
                tracing::warn!(
                    product_key = product_key.to_string(),
                    weekly_schedule = wsched_val,
                    "Oracle: Product has weekly_schedule defined but it could not be parsed. Falling back to 24/7 publishing.",
                );
                tracing::debug!(err = ?err, "Parsing error context.");
                Default::default()
            })
        } else {
            Default::default() // No market hours specified, meaning 24/7 publishing
        };

        let market_schedule: Option<MarketSchedule> = if let Some((_msched_key, msched_val)) =
            product.iter().find(|(k, _v)| *k == "schedule")
        {
            match msched_val.parse::<MarketSchedule>() {
                Ok(schedule) => Some(schedule),
                Err(err) => {
                    tracing::warn!(
                        product_key = product_key.to_string(),
                        schedule = msched_val,
                        "Oracle: Product has schedule defined but it could not be parsed. Falling back to legacy schedule.",
                    );
                    tracing::debug!(err = ?err, "Parsing error context.");
                    None
                }
            }
        } else {
            None
        };

        let publish_interval: Option<Duration> = if let Some((
            _publish_interval_key,
            publish_interval_val,
        )) =
            product.iter().find(|(k, _v)| *k == "publish_interval")
        {
            match publish_interval_val.parse::<f64>() {
                Ok(interval) => Some(Duration::from_secs_f64(interval)),
                Err(err) => {
                    tracing::warn!(
                        product_key = product_key.to_string(),
                        publish_interval = publish_interval_val,
                        "Oracle: Product has publish_interval defined but it could not be parsed. Falling back to None.",
                    );
                    tracing::debug!(err = ?err, "parsing error context");
                    None
                }
            }
        } else {
            None
        };

        product_entries.insert(
            *product_key,
            ProductEntry {
                account_data: *product,
                schedule: market_schedule.unwrap_or_else(|| legacy_schedule.into()),
                price_accounts: [].into(),
                publish_interval,
            },
        );
    }

    // Load the price accounts into price entry and also fill the product entires
    for (price_key, price) in oracle_accounts.iter().filter_map(|(pubkey, account)| {
        PriceEntry::load_from_account(&account.data).map(|product| (pubkey, product))
    }) {
        if let Some(prod) = product_entries.get_mut(&price.prod) {
            prod.price_accounts.push(*price_key);
            price_entries.insert(*price_key, price);
        } else {
            tracing::warn!(
                missing_product = price.prod.to_string(),
                price_key = price_key.to_string(),
                "Could not find product entry for price, listed in its prod field, skipping",
            );

            continue;
        }
    }

    Ok((
        product_entries
            .into_iter()
            .map(|(x, y)| (x, Arc::new(y)))
            .collect(),
        price_entries
            .into_iter()
            .map(|(x, y)| (x, Arc::new(y)))
            .collect(),
    ))
}

#[instrument(skip(rpc_client, product_key_batch))]
async fn fetch_batch_of_product_and_price_accounts(
    rpc_client: &RpcClient,
    product_key_batch: &[Pubkey],
) -> Result<(HashMap<Pubkey, ProductEntry>, HashMap<Pubkey, PriceEntry>)> {
    let mut product_entries = HashMap::new();

    let product_keys = product_key_batch;

    // Look up the batch with a single request
    let product_accounts = rpc_client.get_multiple_accounts(product_keys).await?;

    // Log missing products, fill the product entries with initial values
    for (product_key, product_account) in product_keys.iter().zip(product_accounts) {
        if let Some(prod_acc) = product_account {
            let product = load_product_account(prod_acc.data.as_slice())
                .context(format!("Could not parse product account {}", product_key))?;

            #[allow(deprecated)]
            let legacy_schedule: LegacySchedule = if let Some((_wsched_key, wsched_val)) =
                product.iter().find(|(k, _v)| *k == "weekly_schedule")
            {
                wsched_val.parse().unwrap_or_else(|err| {
                        tracing::warn!(
                            product_key = product_key.to_string(),
                            weekly_schedule = wsched_val,
                            "Oracle: Product has weekly_schedule defined but it could not be parsed. Falling back to 24/7 publishing.",
                        );
                        tracing::debug!(err = ?err, "Parsing error context.");
                        Default::default()
                    })
            } else {
                Default::default() // No market hours specified, meaning 24/7 publishing
            };

            let market_schedule: Option<MarketSchedule> = if let Some((_msched_key, msched_val)) =
                product.iter().find(|(k, _v)| *k == "schedule")
            {
                match msched_val.parse::<MarketSchedule>() {
                    Ok(schedule) => Some(schedule),
                    Err(err) => {
                        tracing::warn!(
                                product_key = product_key.to_string(),
                                schedule = msched_val,
                                "Oracle: Product has schedule defined but it could not be parsed. Falling back to legacy schedule.",
                            );
                        tracing::debug!(err = ?err, "Parsing error context.");
                        None
                    }
                }
            } else {
                None
            };

            let publish_interval: Option<Duration> = if let Some((
                _publish_interval_key,
                publish_interval_val,
            )) =
                product.iter().find(|(k, _v)| *k == "publish_interval")
            {
                match publish_interval_val.parse::<f64>() {
                    Ok(interval) => Some(Duration::from_secs_f64(interval)),
                    Err(err) => {
                        tracing::warn!(
                                product_key = product_key.to_string(),
                                publish_interval = publish_interval_val,
                                "Oracle: Product has publish_interval defined but it could not be parsed. Falling back to None.",
                            );
                        tracing::debug!(err = ?err, "parsing error context");
                        None
                    }
                }
            } else {
                None
            };

            product_entries.insert(
                *product_key,
                ProductEntry {
                    account_data: *product,
                    schedule: market_schedule.unwrap_or_else(|| legacy_schedule.into()),
                    price_accounts: vec![],
                    publish_interval,
                },
            );
        } else {
            tracing::warn!(
                product_key = product_key.to_string(),
                "Oracle: Could not find product on chain, skipping",
            );
        }
    }

    let mut price_entries = HashMap::new();

    // Starting with top-level prices, look up price accounts in
    // batches, filling price entries and adding found prices to
    // the product entries
    let mut todo = product_entries
        .values()
        .filter_map(|p| {
            if p.account_data.px_acc != Pubkey::default() {
                Some(p.account_data.px_acc)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    while !todo.is_empty() {
        let price_accounts = rpc_client.get_multiple_accounts(todo.as_slice()).await?;

        // Any non-zero price.next pubkey will be gathered here and looked up on next iteration
        let mut next_todo = vec![];

        // Process the response of each lookup request. If there's
        // a next price, it will be looked up on next iteration,
        // as todo gets replaced with next_todo.
        for (price_key, price_account) in todo.iter().zip(price_accounts) {
            if let Some(price_acc) = price_account {
                let price = PriceEntry::load_from_account(&price_acc.data)
                    .context(format!("Could not parse price account at {}", price_key))?;

                let next_price = price.next;
                if let Some(prod) = product_entries.get_mut(&price.prod) {
                    prod.price_accounts.push(*price_key);
                    price_entries.insert(*price_key, price);
                } else {
                    tracing::warn!(
                            missing_product = price.prod.to_string(),
                            price_key = price_key.to_string(),
                            "Could not find product entry for price, listed in its prod field, skipping",
                        );

                    continue;
                }

                if next_price != Pubkey::default() {
                    next_todo.push(next_price);
                }
            } else {
                tracing::warn!(
                    price_key = price_key.to_string(),
                    "Could not look up price account on chain, skipping",
                );
                continue;
            }
        }

        todo = next_todo;
    }
    Ok((product_entries, price_entries))
}

#[instrument(skip(data, new_data))]
fn log_data_diff(data: &Data, new_data: &Data) {
    // Log new accounts which have been found
    let previous_product_accounts = data
        .product_accounts
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    tracing::info!(
        new = ?new_data
            .product_accounts
            .keys()
            .cloned()
            .collect::<HashSet<_>>().difference(&previous_product_accounts),
        total = data.product_accounts.len(),
        "Fetched product accounts.",
    );
    let previous_price_accounts = data.price_accounts.keys().cloned().collect::<HashSet<_>>();
    tracing::info!(
        new = ?new_data
            .price_accounts
            .keys()
            .cloned()
            .collect::<HashSet<_>>().difference(&previous_price_accounts),
        total = data.price_accounts.len(),
        "Fetched price accounts.",
    );

    let previous_publishers = data.publisher_permissions.keys().collect::<HashSet<_>>();
    let new_publishers = new_data
        .publisher_permissions
        .keys()
        .collect::<HashSet<_>>();
    tracing::info!(
        new_publishers = ?new_publishers.difference(&previous_publishers).collect::<HashSet<_>>(),
        total_publishers = new_publishers.len(),
        "Updated publisher permissions.",
    );
}
