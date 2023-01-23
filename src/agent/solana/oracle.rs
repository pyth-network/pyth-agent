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
    futures_util::future::join_all,
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
    pub mapping_accounts: HashMap<Pubkey, MappingAccount>,
    pub product_accounts: HashMap<Pubkey, ProductAccount>,
    pub price_accounts:   HashMap<Pubkey, PriceAccount>,
}

impl Data {
    fn new(
        mapping_accounts: HashMap<Pubkey, MappingAccount>,
        product_accounts: HashMap<Pubkey, ProductAccount>,
        price_accounts: HashMap<Pubkey, PriceAccount>,
    ) -> Self {
        Data {
            mapping_accounts,
            product_accounts,
            price_accounts,
        }
    }
}

pub type MappingAccount = pyth_sdk_solana::state::MappingAccount;
#[derive(Debug, Clone)]
pub struct ProductAccount {
    pub account_data:   pyth_sdk_solana::state::ProductAccount,
    pub price_accounts: Vec<Pubkey>,
}
pub type PriceAccount = pyth_sdk_solana::state::PriceAccount;

// Oracle is responsible for fetching Solana account data stored in the Pyth on-chain Oracle.
pub struct Oracle {
    // The Solana account data
    data: Data,

    // Channel on which polled data are received from the Poller
    data_rx: mpsc::Receiver<Data>,

    // Channel on which account updates are received from the Subscriber
    updates_rx: mpsc::Receiver<(Pubkey, solana_sdk::account::Account)>,

    // Channel on which updates are sent to the global store
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            commitment:               CommitmentLevel::Confirmed,
            poll_interval_duration:   Duration::from_secs(5 * 60),
            subscriber_enabled:       true,
            updates_channel_capacity: 10000,
            data_channel_capacity:    10000,
        }
    }
}

pub fn spawn_oracle(
    config: Config,
    rpc_url: &str,
    wss_url: &str,
    key_store: KeyStore,
    global_store_update_tx: mpsc::Sender<global::Update>,
    logger: Logger,
) -> Vec<JoinHandle<()>> {
    let mut jhs = vec![];

    // Create and spawn the account subscriber
    let (updates_tx, updates_rx) = mpsc::channel(config.updates_channel_capacity);
    if config.subscriber_enabled {
        let subscriber = Subscriber::new(
            rpc_url.to_string(),
            wss_url.to_string(),
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
        key_store.mapping_key,
        rpc_url,
        config.commitment,
        config.poll_interval_duration,
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
        account: &ProductAccount,
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
        account: &PriceAccount,
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

    /// The root mapping account key to fetch data from
    mapping_account_key: Pubkey,

    /// The RPC client to use to poll data from the RPC node
    rpc_client: RpcClient,

    /// The interval with which to poll for data
    poll_interval: Interval,

    /// Logger
    logger: Logger,
}

impl Poller {
    pub fn new(
        data_tx: mpsc::Sender<Data>,
        mapping_account_key: Pubkey,
        rpc_url: &str,
        commitment: CommitmentLevel,
        poll_interval_duration: Duration,
        logger: Logger,
    ) -> Self {
        let rpc_client =
            RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig { commitment });
        let poll_interval = tokio::time::interval(poll_interval_duration);

        Poller {
            data_tx,
            mapping_account_key,
            rpc_client,
            poll_interval,
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
        self.data_tx
            .send(self.poll().await?)
            .await
            .map_err(|_| anyhow!("failed to send data to oracle"))
    }

    async fn poll(&self) -> Result<Data> {
        let mapping_accounts = self
            .fetch_mapping_accounts(self.mapping_account_key)
            .await?;
        let product_accounts = self
            .fetch_product_accounts(mapping_accounts.values())
            .await?;
        let price_accounts = self.fetch_price_accounts(product_accounts.values()).await?;

        Ok(Data::new(
            mapping_accounts,
            product_accounts,
            price_accounts,
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

    async fn fetch_product_accounts<'a, A>(
        &self,
        mapping_accounts: A,
    ) -> Result<HashMap<Pubkey, ProductAccount>>
    where
        A: IntoIterator<Item = &'a MappingAccount>,
    {
        let mut pubkeys = vec![];
        let mut futures = vec![];

        // Fetch all product accounts in parallel
        for mapping_account in mapping_accounts {
            for account_key in mapping_account
                .products
                .iter()
                .filter(|pubkey| **pubkey != Pubkey::default())
            {
                pubkeys.push(account_key.clone());
                futures.push(self.fetch_product_account(account_key));
            }
        }

        let product_accounts = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(pubkeys
            .into_iter()
            .zip(product_accounts.into_iter())
            .collect())
    }

    async fn fetch_price_accounts<'a, P>(
        &self,
        product_accounts: P,
    ) -> Result<HashMap<Pubkey, PriceAccount>>
    where
        P: IntoIterator<Item = &'a ProductAccount>,
    {
        let mut price_accounts = HashMap::new();

        for product_account in product_accounts {
            for price_account_key in &product_account.price_accounts {
                let price_account = self.fetch_price_account(price_account_key).await?;
                price_accounts.insert(*price_account_key, price_account);
            }
        }

        Ok(price_accounts)
    }

    async fn fetch_product_account(&self, product_account_key: &Pubkey) -> Result<ProductAccount> {
        // Fetch the product account
        let product_account = *load_product_account(
            &self
                .rpc_client
                .get_account_data(product_account_key)
                .await?,
        )
        .with_context(|| format!("load product account {}", product_account_key))?;

        // Fetch the price accounts associated with this product account
        let mut price_accounts = HashMap::new();
        let mut price_account_key = product_account.px_acc;
        while price_account_key != Pubkey::default() {
            let price_account = self.fetch_price_account(&price_account_key).await?;
            price_accounts.insert(price_account_key, price_account);

            price_account_key = price_account.next;
        }

        // Create the product account object
        let product_account = ProductAccount {
            account_data:   product_account,
            price_accounts: price_accounts.keys().cloned().collect(),
        };

        Ok(product_account)
    }

    async fn fetch_price_account(&self, price_account_key: &Pubkey) -> Result<PriceAccount> {
        let data = self.rpc_client.get_account_data(price_account_key).await?;
        let price_account = *load_price_account(&data)
            .with_context(|| format!("load price account {}", price_account_key))?;

        Ok(price_account)
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
            commitment: CommitmentLevel,
            account_key: Pubkey,
            updates_tx: mpsc::Sender<(Pubkey, solana_sdk::account::Account)>,
            logger: Logger,
        ) -> Self {
            Subscriber {
                rpc_url,
                wss_url,
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
                    max_lag: Some(10000),
                    ..SyncOptions::default()
                },
            )
            .await?;

            Ok(shadow.updates_channel())
        }
    }
}
