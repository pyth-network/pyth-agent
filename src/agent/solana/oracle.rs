// This module is responsible for loading the current state of the
// on-chain Oracle program accounts from Solana.
use {
    self::subscriber::Subscriber,
    super::key_store::KeyStore,
    crate::agent::store::global,
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    pyth_sdk_solana::state::{
        load_mapping_account,
        load_price_account,
        load_product_account,
    },
    serde::{
        Deserialize,
        Serialize,
    },
    slog::Logger,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        account::Account,
        commitment_config::{
            CommitmentConfig,
            CommitmentLevel,
        },
        pubkey::Pubkey,
    },
    std::{
        collections::{
            HashMap,
            HashSet,
        },
        time::Duration,
    },
    tokio::{
        sync::mpsc,
        task::JoinHandle,
        time::Interval,
    },
};

#[derive(Default, Debug, Clone)]
pub struct Data {
    pub mapping_accounts:      HashMap<Pubkey, MappingAccount>,
    pub product_accounts:      HashMap<Pubkey, ProductEntry>,
    pub price_accounts:        HashMap<Pubkey, PriceEntry>,
    /// publisher => {their permissioned price accounts}
    pub publisher_permissions: HashMap<Pubkey, HashSet<Pubkey>>,
}

impl Data {
    fn new(
        mapping_accounts: HashMap<Pubkey, MappingAccount>,
        product_accounts: HashMap<Pubkey, ProductEntry>,
        price_accounts: HashMap<Pubkey, PriceEntry>,
        publisher_permissions: HashMap<Pubkey, HashSet<Pubkey>>,
    ) -> Self {
        Data {
            mapping_accounts,
            product_accounts,
            price_accounts,
            publisher_permissions,
        }
    }
}

pub type MappingAccount = pyth_sdk_solana::state::MappingAccount;
#[derive(Debug, Clone)]
pub struct ProductEntry {
    pub account_data:   pyth_sdk_solana::state::ProductAccount,
    pub price_accounts: Vec<Pubkey>,
}
pub type PriceEntry = pyth_sdk_solana::state::PriceAccount;

// Oracle is responsible for fetching Solana account data stored in the Pyth on-chain Oracle.
pub struct Oracle {
    /// The Solana account data
    data: Data,

    /// Channel on which polled data are received from the Poller
    data_rx: mpsc::Receiver<Data>,

    /// Channel on which account updates are received from the Subscriber
    updates_rx: mpsc::Receiver<(Pubkey, solana_sdk::account::Account)>,

    /// Channel on which updates are sent to the global store
    global_store_tx: mpsc::Sender<global::Update>,

    logger: Logger,
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            commitment:               CommitmentLevel::Confirmed,
            poll_interval_duration:   Duration::from_secs(2 * 60),
            subscriber_enabled:       true,
            updates_channel_capacity: 10000,
            data_channel_capacity:    10000,
            max_lookup_batch_size:    100,
        }
    }
}

pub fn spawn_oracle(
    config: Config,
    rpc_url: &str,
    wss_url: &str,
    rpc_timeout: Duration,
    global_store_update_tx: mpsc::Sender<global::Update>,
    publisher_permissions_tx: mpsc::Sender<HashMap<Pubkey, HashSet<Pubkey>>>,
    key_store: KeyStore,
    logger: Logger,
) -> Vec<JoinHandle<()>> {
    let mut jhs = vec![];

    // Create and spawn the account subscriber
    let (updates_tx, updates_rx) = mpsc::channel(config.updates_channel_capacity);
    if config.subscriber_enabled {
        let subscriber = Subscriber::new(
            rpc_url.to_string(),
            wss_url.to_string(),
            rpc_timeout,
            config.commitment,
            key_store.program_key.clone(),
            updates_tx,
            logger.clone(),
        );
        jhs.push(tokio::spawn(async move { subscriber.run().await }));
    }

    // Create and spawn the Poller
    let (data_tx, data_rx) = mpsc::channel(config.data_channel_capacity);
    let mut poller = Poller::new(
        data_tx,
        publisher_permissions_tx,
        rpc_url,
        rpc_timeout,
        config.commitment,
        config.poll_interval_duration,
        config.max_lookup_batch_size,
        key_store.mapping_key,
        logger.clone(),
    );
    jhs.push(tokio::spawn(async move { poller.run().await }));

    // Create and spawn the Oracle
    let mut oracle = Oracle::new(data_rx, updates_rx, global_store_update_tx, logger);
    jhs.push(tokio::spawn(async move { oracle.run().await }));

    jhs
}

impl Oracle {
    pub fn new(
        data_rx: mpsc::Receiver<Data>,
        updates_rx: mpsc::Receiver<(Pubkey, solana_sdk::account::Account)>,
        global_store_tx: mpsc::Sender<global::Update>,
        logger: Logger,
    ) -> Self {
        Oracle {
            data: Default::default(),
            data_rx,
            updates_rx,
            global_store_tx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            if let Err(err) = self.handle_next().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    async fn handle_next(&mut self) -> Result<()> {
        tokio::select! {
            Some((account_key, account)) = self.updates_rx.recv() => {
                self.handle_account_update(&account_key, &account).await
            }
            Some(data) = self.data_rx.recv() => {
                self.handle_data_update(data);
                self.send_all_data_to_global_store().await
            }
        }
    }

    fn handle_data_update(&mut self, data: Data) {
        // Log new accounts which have been found
        let previous_mapping_accounts = self
            .data
            .mapping_accounts
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        info!(self.logger, "fetched mapping accounts"; "new" => format!("{:?}", data
                .mapping_accounts
                .keys()
                .cloned()
                .collect::<HashSet<_>>().difference(&previous_mapping_accounts)), "total" => data.mapping_accounts.len());
        let previous_product_accounts = self
            .data
            .product_accounts
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        info!(self.logger, "fetched product accounts"; "new" => format!("{:?}", data
                .product_accounts
                .keys()
                .cloned()
                .collect::<HashSet<_>>().difference(&previous_product_accounts)), "total" => data.product_accounts.len());
        let previous_price_accounts = self
            .data
            .price_accounts
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        info!(self.logger, "fetched price accounts"; "new" => format!("{:?}", data
                .price_accounts
                .keys()
                .cloned()
                .collect::<HashSet<_>>().difference(&previous_price_accounts)), "total" => data.price_accounts.len());

        let previous_publishers = self
            .data
            .publisher_permissions
            .keys()
            .collect::<HashSet<_>>();
        let new_publishers = data.publisher_permissions.keys().collect::<HashSet<_>>();
        info!(
        self.logger,
        "updated publisher permissions";
        "new" => format!("{:?}", new_publishers.difference(&previous_publishers).collect::<HashSet<_>>()),
        "total" => new_publishers.len(),
        );

        // Update the data with the new data structs
        self.data = data;
    }

    async fn handle_account_update(
        &mut self,
        account_key: &Pubkey,
        account: &Account,
    ) -> Result<()> {
        debug!(self.logger, "handling account update");

        // We are only interested in price account updates, all other types of updates
        // will be fetched using polling.
        if !self.data.price_accounts.contains_key(account_key) {
            return Ok(());
        }

        self.handle_price_account_update(account_key, account).await
    }

    async fn handle_price_account_update(
        &mut self,
        account_key: &Pubkey,
        account: &Account,
    ) -> Result<()> {
        let price_account = *load_price_account(&account.data)
            .with_context(|| format!("load price account {}", account_key))?;

        debug!(self.logger, "observed on-chain price account update"; "pubkey" => account_key.to_string(), "price" => price_account.agg.price, "conf" => price_account.agg.conf, "status" => format!("{:?}", price_account.agg.status));

        self.data.price_accounts.insert(*account_key, price_account);

        self.notify_price_account_update(account_key, &price_account)
            .await?;

        Ok(())
    }

    async fn send_all_data_to_global_store(&self) -> Result<()> {
        for (product_account_key, product_account) in &self.data.product_accounts {
            self.notify_product_account_update(product_account_key, product_account)
                .await?;
        }

        for (price_account_key, price_account) in &self.data.price_accounts {
            self.notify_price_account_update(price_account_key, price_account)
                .await?;
        }

        Ok(())
    }

    async fn notify_product_account_update(
        &self,
        account_key: &Pubkey,
        account: &ProductEntry,
    ) -> Result<()> {
        self.global_store_tx
            .send(global::Update::ProductAccountUpdate {
                account_key: account_key.clone(),
                account:     account.clone(),
            })
            .await
            .map_err(|_| anyhow!("failed to notify product account update"))
    }

    async fn notify_price_account_update(
        &self,
        account_key: &Pubkey,
        account: &PriceEntry,
    ) -> Result<()> {
        self.global_store_tx
            .send(global::Update::PriceAccountUpdate {
                account_key: account_key.clone(),
                account:     account.clone(),
            })
            .await
            .map_err(|_| anyhow!("failed to notify price account update"))
    }
}

struct Poller {
    /// The channel on which to send polled update data
    data_tx: mpsc::Sender<Data>,

    /// Updates about permissioned price accounts from oracle to exporter
    publisher_permissions_tx: mpsc::Sender<HashMap<Pubkey, HashSet<Pubkey>>>,

    /// The RPC client to use to poll data from the RPC node
    rpc_client: RpcClient,

    /// The interval with which to poll for data
    poll_interval: Interval,

    /// Passed from Oracle config
    max_lookup_batch_size: usize,

    mapping_key: Pubkey,

    /// Logger
    logger: Logger,
}

impl Poller {
    pub fn new(
        data_tx: mpsc::Sender<Data>,
        publisher_permissions_tx: mpsc::Sender<HashMap<Pubkey, HashSet<Pubkey>>>,
        rpc_url: &str,
        rpc_timeout: Duration,
        commitment: CommitmentLevel,
        poll_interval_duration: Duration,
        max_lookup_batch_size: usize,
        mapping_key: Pubkey,
        logger: Logger,
    ) -> Self {
        let rpc_client = RpcClient::new_with_timeout_and_commitment(
            rpc_url.to_string(),
            rpc_timeout,
            CommitmentConfig { commitment },
        );
        let poll_interval = tokio::time::interval(poll_interval_duration);

        Poller {
            data_tx,
            publisher_permissions_tx,
            rpc_client,
            poll_interval,
            max_lookup_batch_size,
            mapping_key,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.poll_interval.tick().await;
            info!(self.logger, "fetching all pyth account data");
            if let Err(err) = self.poll_and_send().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    async fn poll_and_send(&mut self) -> Result<()> {
        let fresh_data = self.poll().await?;

        self.publisher_permissions_tx
            .send(fresh_data.publisher_permissions.clone())
            .await
            .context("Updating permissioned price accounts for exporter")?;

        self.data_tx
            .send(fresh_data)
            .await
            .context("failed to send data to oracle")?;

        Ok(())
    }

    async fn poll(&self) -> Result<Data> {
        let mapping_accounts = self.fetch_mapping_accounts(self.mapping_key).await?;
        let (product_accounts, price_accounts) = self
            .fetch_product_and_price_accounts(mapping_accounts.values())
            .await?;

        let mut publisher_permissions = HashMap::new();

        for (price_key, price_entry) in price_accounts.iter() {
            for component in price_entry.comp {
                let component_pub_entry = publisher_permissions
                    .entry(component.publisher)
                    .or_insert(HashSet::new());

                component_pub_entry.insert(price_key.clone());
                break;
            }
        }

        Ok(Data::new(
            mapping_accounts,
            product_accounts,
            price_accounts,
            publisher_permissions,
        ))
    }

    async fn fetch_mapping_accounts(
        &self,
        mapping_account_key: Pubkey,
    ) -> Result<HashMap<Pubkey, MappingAccount>> {
        let mut accounts = HashMap::new();

        let mut account_key = mapping_account_key;
        while account_key != Pubkey::default() {
            let account = *load_mapping_account(
                &self
                    .rpc_client
                    .get_account_data(&account_key)
                    .await
                    .with_context(|| format!("load mapping account {}", account_key))?,
            )?;
            accounts.insert(account_key, account);

            account_key = account.next;
        }

        Ok(accounts)
    }

    async fn fetch_product_and_price_accounts<'a, A>(
        &self,
        mapping_accounts: A,
    ) -> Result<(HashMap<Pubkey, ProductEntry>, HashMap<Pubkey, PriceEntry>)>
    where
        A: IntoIterator<Item = &'a MappingAccount>,
    {
        let mut product_keys = vec![];

        // Get all product keys
        for mapping_account in mapping_accounts {
            for account_key in mapping_account
                .products
                .iter()
                .filter(|pubkey| **pubkey != Pubkey::default())
            {
                product_keys.push(account_key.clone());
            }
        }

        let mut product_entries = HashMap::new();
        let mut price_entries = HashMap::new();

        // Lookup products and their prices using the configured batch size
        for product_key_batch in product_keys.as_slice().chunks(self.max_lookup_batch_size) {
            let (mut batch_products, mut batch_prices) = self
                .fetch_batch_of_product_and_price_accounts(product_key_batch)
                .await?;

            product_entries.extend(batch_products.drain());
            price_entries.extend(batch_prices.drain());
        }

        Ok((product_entries, price_entries))
    }

    async fn fetch_batch_of_product_and_price_accounts(
        &self,
        product_key_batch: &[Pubkey],
    ) -> Result<(HashMap<Pubkey, ProductEntry>, HashMap<Pubkey, PriceEntry>)> {
        let mut product_entries = HashMap::new();

        let product_keys = product_key_batch;

        // Look up the batch with a single request
        let product_accounts = self.rpc_client.get_multiple_accounts(product_keys).await?;

        // Log missing products, fill the product entries with initial values
        for (product_key, product_account) in product_keys.iter().zip(product_accounts) {
            if let Some(prod_acc) = product_account {
                let product = load_product_account(prod_acc.data.as_slice())
                    .context(format!("Could not parse product account {}", product_key))?;

                product_entries.insert(
                    *product_key,
                    ProductEntry {
                        account_data:   *product,
                        price_accounts: vec![],
                    },
                );
            } else {
                warn!(self.logger, "Oracle: Could not find product on chain, skipping";
		      "product_key" => product_key.to_string(),);
            }
        }

        let mut price_entries = HashMap::new();

        // Starting with top-level prices, look up price accounts in
        // batches, filling price entries and adding found prices to
        // the product entries
        let mut todo = product_entries
            .values()
            .map(|p| p.account_data.px_acc)
            .collect::<Vec<_>>();

        while !todo.is_empty() {
            let price_accounts = self
                .rpc_client
                .get_multiple_accounts(todo.as_slice())
                .await?;

            // Any non-zero price.next pubkey will be gathered here and looked up on next iteration
            let mut next_todo = vec![];

            // Process the response of each lookup request. If there's
            // a next price, it will be looked up on next iteration,
            // as todo gets replaced with next_todo.
            for (price_key, price_account) in todo.iter().zip(price_accounts) {
                if let Some(price_acc) = price_account {
                    let price = load_price_account(&price_acc.data)
                        .context(format!("Could not parse price account at {}", price_key))?;

                    if let Some(prod) = product_entries.get_mut(&price.prod) {
                        prod.price_accounts.push(*price_key);
                        price_entries.insert(*price_key, *price);
                    } else {
                        warn!(self.logger, "Could not find product entry for price, listed in its prod field, skipping";
                         "missing_product" => price.prod.to_string(),
                         "price_key" => price_key.to_string(),
                        );

                        continue;
                    }

                    if price.next != Pubkey::default() {
                        next_todo.push(price.next.clone());
                    }
                } else {
                    warn!(self.logger, "Could not look up price account on chain, skipping"; "price_key" => price_key.to_string(),);
                    continue;
                }
            }

            todo = next_todo;
        }
        Ok((product_entries, price_entries))
    }
}

mod subscriber {
    use {
        anyhow::{
            anyhow,
            Result,
        },
        slog::Logger,
        solana_sdk::{
            account::Account,
            commitment_config::CommitmentLevel,
            pubkey::Pubkey,
        },
        solana_shadow::{
            BlockchainShadow,
            SyncOptions,
        },
        std::time::Duration,
        tokio::sync::{
            broadcast,
            mpsc,
        },
    };

    /// Subscriber subscribes to all changes on the given account, and sends those changes
    /// on updates_tx. This is a convenience wrapper around the Blockchain Shadow crate.
    pub struct Subscriber {
        /// HTTP RPC endpoint
        rpc_url: String,
        /// WSS RPC endpoint
        wss_url: String,

        /// Timeout for RPC requests
        rpc_timeout: Duration,

        /// Commitment level used to read account data
        commitment: CommitmentLevel,

        /// Public key of the root account to monitor. Note that all
        /// accounts owned by this account are also monitored.
        account_key: Pubkey,

        /// Channel on which updates are sent
        updates_tx: mpsc::Sender<(Pubkey, solana_sdk::account::Account)>,

        logger: Logger,
    }

    impl Subscriber {
        pub fn new(
            rpc_url: String,
            wss_url: String,
            rpc_timeout: Duration,
            commitment: CommitmentLevel,
            account_key: Pubkey,
            updates_tx: mpsc::Sender<(Pubkey, solana_sdk::account::Account)>,
            logger: Logger,
        ) -> Self {
            Subscriber {
                rpc_url,
                wss_url,
                rpc_timeout,
                commitment,
                account_key,
                updates_tx,
                logger,
            }
        }

        pub async fn run(&self) {
            match self.start_shadow().await {
                Ok(mut shadow_rx) => self.forward_updates(&mut shadow_rx).await,
                Err(err) => error!(self.logger, "{:#}", err; "error" => format!("{:?}", err)),
            }
        }

        async fn forward_updates(&self, shadow_rx: &mut broadcast::Receiver<(Pubkey, Account)>) {
            loop {
                if let Err(err) = self.forward_update(shadow_rx).await {
                    error!(self.logger, "error forwarding updates: {:#}", err; "error" => format!("{:?}", err))
                }
            }
        }

        async fn forward_update(
            &self,
            shadow_rx: &mut broadcast::Receiver<(Pubkey, Account)>,
        ) -> Result<()> {
            self.updates_tx
                .send(shadow_rx.recv().await?)
                .await
                .map_err(|_| anyhow!("failed to forward update"))
        }

        pub async fn start_shadow(
            &self,
        ) -> Result<broadcast::Receiver<(Pubkey, solana_sdk::account::Account)>> {
            debug!(self.logger, "subscribed to account updates"; "account" => self.account_key.to_string());

            let shadow = BlockchainShadow::new_for_program(
                &self.account_key,
                SyncOptions {
                    network: solana_shadow::Network::Custom(
                        self.rpc_url.clone(),
                        self.wss_url.clone(),
                    ),
                    commitment: self.commitment,
                    rpc_timeout: self.rpc_timeout,
                    max_lag: Some(10000),
                    ..SyncOptions::default()
                },
            )
            .await?;

            Ok(shadow.updates_channel())
        }
    }
}
