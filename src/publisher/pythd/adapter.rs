use {
    super::{
        super::{
            solana,
            store::{
                global,
                PriceIdentifier,
            },
        },
        api::{
            self,
            Conf,
            NotifyPrice,
            NotifyPriceSched,
            Price,
            PriceAccountMetadata,
            ProductAccount,
            ProductAccountMetadata,
            SubscriptionID,
        },
    },
    anyhow::{
        anyhow,
        Result,
    },
    pyth_sdk::{
        Identifier,
        PriceStatus,
    },
    pyth_sdk_solana::state::PriceComp,
    slog::Logger,
    std::{
        collections::HashMap,
        time::Duration,
    },
    tokio::{
        sync::{
            broadcast,
            mpsc,
            oneshot,
        },
        time::{
            self,
            Interval,
        },
    },
};

/// Adapter is the adapter between the pythd websocket API, and the stores.
/// It is responsible for implementing the business logic for responding to
/// the pythd websocket API calls.
pub struct Adapter {
    /// Channel on which messages are received
    message_rx: mpsc::Receiver<Message>,

    /// Subscription ID counter
    subscription_id_count: SubscriptionID,

    /// Notify Price Sched subscriptions
    notify_price_sched_subscriptions: HashMap<PriceIdentifier, Vec<NotifyPriceSchedSubscription>>,

    /// The fixed interval at which Notify Price Sched notifications are sent
    notify_price_sched_interval: Interval,

    /// Channels on which to communicate to the global store
    global_store_tx: mpsc::Sender<global::Message>,

    /// Channel on which the shutdown is broadcast
    shutdown_rx: broadcast::Receiver<()>,

    /// The logger
    logger: Logger,
}

/// Represents a single Notify Price Sched subscription
struct NotifyPriceSchedSubscription {
    /// ID of this subscription
    subscription_id:       SubscriptionID,
    /// Channel notifications are sent on
    notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
}

#[derive(Debug)]
pub enum Message {
    GetProductList {
        result_tx: oneshot::Sender<Result<Vec<ProductAccountMetadata>>>,
    },
    GetProduct {
        account:   api::Pubkey,
        result_tx: oneshot::Sender<Result<ProductAccount>>,
    },
    GetAllProducts {
        result_tx: oneshot::Sender<Result<Vec<ProductAccount>>>,
    },
    SubscribePrice {
        account:         api::Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
        result_tx:       oneshot::Sender<Result<SubscriptionID>>,
    },
    SubscribePriceSched {
        account:               api::Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
        result_tx:             oneshot::Sender<Result<SubscriptionID>>,
    },
    UpdatePrice {
        account: api::Pubkey,
        price:   Price,
        conf:    Conf,
        status:  String,
    },
}

impl Adapter {
    pub fn new(
        message_rx: mpsc::Receiver<Message>,
        notify_price_sched_interval: Duration,
        global_store_tx: mpsc::Sender<global::Message>,
        shutdown_rx: broadcast::Receiver<()>,
        logger: Logger,
    ) -> Self {
        Adapter {
            message_rx,
            subscription_id_count: 0,
            notify_price_sched_subscriptions: HashMap::new(),
            notify_price_sched_interval: time::interval(notify_price_sched_interval),
            global_store_tx,
            shutdown_rx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(message) = self.message_rx.recv() => {
                    if let Err(err) = self.handle_message(message).await {
                        error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    info!(self.logger, "shutdown signal received");
                    return;
                }
                _ = self.notify_price_sched_interval.tick() => {
                    if let Err(err) = self.send_notify_price_sched().await {
                        error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
                    }
                }
            }
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::GetProductList { result_tx } => {
                self.send(result_tx, self.handle_get_product_list().await)
            }
            Message::GetProduct { account, result_tx } => todo!(),
            Message::GetAllProducts { result_tx } => {
                self.send(result_tx, self.handle_get_all_products().await)
            }
            Message::SubscribePrice {
                account,
                notify_price_tx,
                result_tx,
            } => todo!(),
            Message::SubscribePriceSched {
                account,
                notify_price_sched_tx,
                result_tx,
            } => {
                let subscription_id = self
                    .handle_subscribe_price_sched(&account.parse()?, notify_price_sched_tx)
                    .await;
                let res = self.send(result_tx, Ok(subscription_id));
                res
            }
            Message::UpdatePrice {
                account,
                price,
                conf,
                status,
            } => todo!(),
        }
    }

    fn send<T>(&self, tx: oneshot::Sender<T>, item: T) -> Result<()> {
        tx.send(item).map_err(|_| anyhow!("sending channel full"))
    }

    async fn handle_get_product_list(&self) -> Result<Vec<ProductAccountMetadata>> {
        let all_accounts_metadata = self.lookup_all_accounts_metadata().await?;

        let mut result = Vec::new();
        for (product_account_key, product_account) in
            all_accounts_metadata.product_accounts_metadata
        {
            // Transform the price accounts into the API PriceAccountMetadata structs
            // the API uses.
            let price_accounts_metadata = product_account
                .price_accounts
                .iter()
                .filter_map(|price_account_key| {
                    all_accounts_metadata
                        .price_accounts_metadata
                        .get(price_account_key)
                        .map(|acc| (price_account_key, acc))
                })
                .map(|(price_account_key, price_account)| PriceAccountMetadata {
                    account:        price_account_key.to_string(),
                    price_type:     "price".to_owned(),
                    price_exponent: price_account.expo as i64,
                })
                .collect();

            // Create the product account metadata struct
            result.push(ProductAccountMetadata {
                account:   product_account_key.to_string(),
                attr_dict: product_account.attr_dict,
                prices:    price_accounts_metadata,
            })
        }

        Ok(result)
    }

    // Fetches the Solana-specific Oracle data from the global store
    async fn lookup_all_accounts_metadata(&self) -> Result<global::AllAccountsMetadata> {
        let (result_tx, result_rx) = oneshot::channel();
        self.global_store_tx
            .send(global::Message::LookupAllAccountsMetadata { result_tx })
            .await?;
        result_rx.await?
    }

    async fn handle_get_all_products(&self) -> Result<Vec<ProductAccount>> {
        let solana_data = self.lookup_solana_oracle_data().await?;

        let mut result = Vec::new();
        for (product_account_key, product_account) in solana_data.product_accounts {
            // Extract all the price accounts from the product account
            let price_accounts = product_account
                .price_accounts
                .iter()
                .filter_map(|price_account_key| {
                    solana_data
                        .price_accounts
                        .get(price_account_key)
                        .map(|acc| (price_account_key, acc))
                })
                .map(|(price_account_key, price_account)| {
                    Self::solana_price_account_to_pythd_api_price_account(
                        price_account_key,
                        price_account,
                    )
                })
                .collect();

            // Create the product account metadata struct
            result.push(ProductAccount {
                account: product_account_key.to_string(),
                attr_dict: product_account
                    .account_data
                    .iter()
                    .filter(|(key, val)| !key.is_empty() && !val.is_empty())
                    .map(|(key, val)| (key.to_owned(), val.to_owned()))
                    .collect(),
                price_accounts,
            })
        }

        Ok(result)
    }

    async fn lookup_solana_oracle_data(&self) -> Result<solana::oracle::Data> {
        let (result_tx, result_rx) = oneshot::channel();
        self.global_store_tx
            .send(global::Message::LookupSolanaOracleData { result_tx })
            .await?;
        result_rx.await?
    }

    fn solana_price_account_to_pythd_api_price_account(
        price_account_key: &solana_sdk::pubkey::Pubkey,
        price_account: &pyth_sdk_solana::state::PriceAccount,
    ) -> api::PriceAccount {
        api::PriceAccount {
            account:            price_account_key.to_string(),
            price_type:         "price".to_string(),
            price_exponent:     price_account.expo as i64,
            status:             Self::price_status_to_str(price_account.agg.status),
            price:              price_account.agg.price,
            conf:               price_account.agg.conf,
            twap:               price_account.ema_price.val,
            twac:               price_account.ema_conf.val,
            valid_slot:         price_account.valid_slot,
            pub_slot:           price_account.agg.pub_slot,
            prev_slot:          price_account.prev_slot,
            prev_price:         price_account.prev_price,
            prev_conf:          price_account.prev_conf,
            publisher_accounts: price_account
                .comp
                .iter()
                .filter(|comp| **comp != PriceComp::default())
                .map(|comp| api::PublisherAccount {
                    account: comp.publisher.to_string(),
                    status:  Self::price_status_to_str(comp.agg.status),
                    price:   comp.agg.price,
                    conf:    comp.agg.conf,
                    slot:    comp.agg.pub_slot,
                })
                .collect(),
        }
    }

    // TODO: implement Display on pyth_sdk::PriceStatus and then just call pyth_sdk::PriceStatus::to_string
    fn price_status_to_str(price_status: PriceStatus) -> String {
        match price_status {
            PriceStatus::Unknown => "unknown",
            PriceStatus::Trading => "trading",
            PriceStatus::Halted => "halted",
            PriceStatus::Auction => "auction",
        }
        .to_string()
    }

    async fn handle_subscribe_price_sched(
        &mut self,
        account_pubkey: &solana_sdk::pubkey::Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
    ) -> SubscriptionID {
        let subscription_id = self.next_subscription_id();
        self.notify_price_sched_subscriptions
            .entry(Identifier::new(account_pubkey.to_bytes()))
            .or_default()
            .push(NotifyPriceSchedSubscription {
                subscription_id,
                notify_price_sched_tx,
            });
        subscription_id
    }

    fn next_subscription_id(&mut self) -> SubscriptionID {
        self.subscription_id_count += 1;
        self.subscription_id_count
    }

    async fn send_notify_price_sched(&self) -> Result<()> {
        for subscription in self.notify_price_sched_subscriptions.values().flatten() {
            subscription
                .notify_price_sched_tx
                .send(NotifyPriceSched {
                    subscription: subscription.subscription_id,
                })
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            Adapter,
            Message,
        },
        crate::publisher::{
            pythd::{
                api,
                api::{
                    NotifyPriceSched,
                    PriceAccountMetadata,
                    ProductAccountMetadata,
                    PublisherAccount,
                },
            },
            solana,
            store::global,
        },
        iobuffer::IoBuffer,
        pyth_sdk_solana::state::{
            MappingAccount,
            PriceAccount,
            PriceComp,
            PriceInfo,
            PriceType,
            Rational,
            MAP_TABLE_SIZE,
        },
        slog_extlog::slog_test,
        std::{
            collections::{
                BTreeMap,
                HashMap,
            },
            str::FromStr,
            time::Duration,
        },
        tokio::{
            sync::{
                broadcast,
                mpsc,
                oneshot,
            },
            task::JoinHandle,
        },
    };

    struct TestAdapter {
        message_tx:      mpsc::Sender<Message>,
        shutdown_tx:     broadcast::Sender<()>,
        global_store_rx: mpsc::Receiver<global::Message>,
        jh:              JoinHandle<()>,
    }

    impl Drop for TestAdapter {
        fn drop(&mut self) {
            let _ = self.shutdown_tx.send(());
            self.jh.abort();
        }
    }

    async fn setup() -> TestAdapter {
        // Create and spawn an adapter
        let (adapter_tx, adapter_rx) = mpsc::channel(100);
        let (global_store_tx, global_store_rx) = mpsc::channel(1000);
        let notify_price_sched_interval = Duration::from_nanos(10);
        let logger = slog_test::new_test_logger(IoBuffer::new());
        let (shutdown_tx, shutdown_rx) = broadcast::channel(10);
        let mut adapter = Adapter::new(
            adapter_rx,
            notify_price_sched_interval,
            global_store_tx,
            shutdown_rx,
            logger,
        );
        let jh = tokio::spawn(async move { adapter.run().await });

        TestAdapter {
            message_tx: adapter_tx,
            global_store_rx,
            shutdown_tx,
            jh,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_subscribe_price_sched() {
        let test_adapter = setup().await;

        // Send a Subscribe Price Sched message
        let account = "2wrWGm63xWubz7ue4iYR3qvBbaUJhZVi4eSpNuU8k8iF".to_string();
        let (notify_price_sched_tx, mut notify_price_sched_rx) = mpsc::channel(1000);
        let (result_tx, result_rx) = oneshot::channel();
        test_adapter
            .message_tx
            .send(Message::SubscribePriceSched {
                account,
                notify_price_sched_tx,
                result_tx,
            })
            .await
            .unwrap();

        let subscription_id = result_rx.await.unwrap().unwrap();

        // Expect that we recieve several Notify Price Sched notifications
        for _ in 0..10 {
            assert_eq!(
                notify_price_sched_rx.recv().await.unwrap(),
                NotifyPriceSched {
                    subscription: subscription_id,
                }
            )
        }
    }

    fn get_test_all_accounts_metadata() -> global::AllAccountsMetadata {
        global::AllAccountsMetadata {
            product_accounts_metadata: HashMap::from([
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                    )
                    .unwrap(),
                    global::ProductAccountMetadata {
                        attr_dict:      BTreeMap::from(
                            [
                                ("symbol", "Crypto.LTC/USD"),
                                ("asset_type", "Crypto"),
                                ("quote_currency", "USD"),
                                ("description", "LTC/USD"),
                                ("generic_symbol", "LTCUSD"),
                                ("base", "LTC"),
                            ]
                            .map(|(k, v)| (k.to_string(), v.to_string())),
                        ),
                        price_accounts: vec![
                            solana_sdk::pubkey::Pubkey::from_str(
                                "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                            )
                            .unwrap(),
                        ],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                    )
                    .unwrap(),
                    global::ProductAccountMetadata {
                        attr_dict:      BTreeMap::from(
                            [
                                ("symbol", "Crypto.ETH/USD"),
                                ("asset_type", "Crypto"),
                                ("quote_currency", "USD"),
                                ("description", "ETH/USD"),
                                ("generic_symbol", "ETHUSD"),
                                ("base", "ETH"),
                            ]
                            .map(|(k, v)| (k.to_string(), v.to_string())),
                        ),
                        price_accounts: vec![
                            solana_sdk::pubkey::Pubkey::from_str(
                                "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                            )
                            .unwrap(),
                        ],
                    },
                ),
            ]),
            price_accounts_metadata:   HashMap::from([
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU",
                    )
                    .unwrap(),
                    global::PriceAccountMetadata { expo: -8 },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                    )
                    .unwrap(),
                    global::PriceAccountMetadata { expo: -10 },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                    )
                    .unwrap(),
                    global::PriceAccountMetadata { expo: -6 },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                    )
                    .unwrap(),
                    global::PriceAccountMetadata { expo: -9 },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid",
                    )
                    .unwrap(),
                    global::PriceAccountMetadata { expo: -6 },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                    )
                    .unwrap(),
                    global::PriceAccountMetadata { expo: 2 },
                ),
            ]),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_get_product_list() {
        // Start the test adapter
        let mut test_adapter = setup().await;

        // Send a Get Product List message
        let (result_tx, result_rx) = oneshot::channel();
        test_adapter
            .message_tx
            .send(Message::GetProductList { result_tx })
            .await
            .unwrap();

        // Return the product list to the adapter, from the global store
        match test_adapter.global_store_rx.recv().await.unwrap() {
            global::Message::LookupAllAccountsMetadata { result_tx } => result_tx
                .send(Ok(get_test_all_accounts_metadata()))
                .unwrap(),
            _ => panic!("Uexpected message received from adapter"),
        };

        // Check that the result is what we expected
        let expected = vec![
            ProductAccountMetadata {
                account:   "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc".to_string(),
                attr_dict: BTreeMap::from(
                    [
                        ("symbol", "Crypto.ETH/USD"),
                        ("asset_type", "Crypto"),
                        ("quote_currency", "USD"),
                        ("description", "ETH/USD"),
                        ("generic_symbol", "ETHUSD"),
                        ("base", "ETH"),
                    ]
                    .map(|(k, v)| (k.to_string(), v.to_string())),
                ),
                prices:    vec![
                    PriceAccountMetadata {
                        account:        "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD".to_string(),
                        price_type:     "price".to_string(),
                        price_exponent: -9,
                    },
                    PriceAccountMetadata {
                        account:        "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid".to_string(),
                        price_type:     "price".to_string(),
                        price_exponent: -6,
                    },
                    PriceAccountMetadata {
                        account:        "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ".to_string(),
                        price_type:     "price".to_string(),
                        price_exponent: 2,
                    },
                ],
            },
            ProductAccountMetadata {
                account:   "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t".to_string(),
                attr_dict: BTreeMap::from(
                    [
                        ("symbol", "Crypto.LTC/USD"),
                        ("asset_type", "Crypto"),
                        ("quote_currency", "USD"),
                        ("description", "LTC/USD"),
                        ("generic_symbol", "LTCUSD"),
                        ("base", "LTC"),
                    ]
                    .map(|(k, v)| (k.to_string(), v.to_string())),
                ),
                prices:    vec![
                    PriceAccountMetadata {
                        account:        "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU".to_string(),
                        price_type:     "price".to_string(),
                        price_exponent: -8,
                    },
                    PriceAccountMetadata {
                        account:        "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr".to_string(),
                        price_type:     "price".to_string(),
                        price_exponent: -10,
                    },
                    PriceAccountMetadata {
                        account:        "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6".to_string(),
                        price_type:     "price".to_string(),
                        price_exponent: -6,
                    },
                ],
            },
        ];

        let mut result = result_rx.await.unwrap().unwrap();
        result.sort();
        assert_eq!(result, expected);
    }

    fn pad_price_comps(mut inputs: Vec<PriceComp>) -> [PriceComp; 32] {
        inputs.resize(32, PriceComp::default());
        inputs.try_into().unwrap()
    }

    fn get_test_global_oracle_data() -> solana::oracle::Data {
        solana::oracle::Data {
            mapping_accounts: HashMap::from([(
                solana_sdk::pubkey::Pubkey::from_str(
                    "7ycfa1ENNT5dVVoMtiMjsgVbkWKFJbu6nF2h1UVT18Cf",
                )
                .unwrap(),
                MappingAccount {
                    magic:    0xa1b2c3d4,
                    ver:      4,
                    atype:    3,
                    size:     500,
                    num:      3,
                    unused:   0,
                    next:     solana_sdk::pubkey::Pubkey::default(),
                    products: [solana_sdk::pubkey::Pubkey::default(); MAP_TABLE_SIZE],
                },
            )]),
            product_accounts: HashMap::from([
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                    )
                    .unwrap(),
                    solana::oracle::ProductAccount {
                        account_data:   pyth_sdk_solana::state::ProductAccount {
                            magic:  0xa1b2c3d4,
                            ver:    6,
                            atype:  4,
                            size:   340,
                            px_acc: solana_sdk::pubkey::Pubkey::from_str(
                                "EKZYBqisdcsn3shN1rQRuWTzH3iqMbj1dxFtDFmrBi8o",
                            )
                            .unwrap(),
                            // LTC/USD
                            attr:   [
                                6, 115, 121, 109, 98, 111, 108, 14, 67, 114, 121, 112, 116, 111,
                                46, 76, 84, 67, 47, 85, 83, 68, 10, 97, 115, 115, 101, 116, 95,
                                116, 121, 112, 101, 6, 67, 114, 121, 112, 116, 111, 14, 113, 117,
                                111, 116, 101, 95, 99, 117, 114, 114, 101, 110, 99, 121, 3, 85, 83,
                                68, 11, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 7,
                                76, 84, 67, 47, 85, 83, 68, 14, 103, 101, 110, 101, 114, 105, 99,
                                95, 115, 121, 109, 98, 111, 108, 6, 76, 84, 67, 85, 83, 68, 4, 98,
                                97, 115, 101, 3, 76, 84, 67, 114, 4, 83, 112, 111, 116, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            ],
                        },
                        price_accounts: vec![
                            solana_sdk::pubkey::Pubkey::from_str(
                                "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                            )
                            .unwrap(),
                        ],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                    )
                    .unwrap(),
                    solana::oracle::ProductAccount {
                        account_data:   pyth_sdk_solana::state::ProductAccount {
                            magic:  0xa1b2c3d4,
                            ver:    5,
                            atype:  3,
                            size:   478,
                            px_acc: solana_sdk::pubkey::Pubkey::from_str(
                                "JTmFx5zX9mM94itfk2nQcJnQQDPjcv4UPD7SYj6xDCV",
                            )
                            .unwrap(),
                            // ETH/USD
                            attr:   [
                                6, 115, 121, 109, 98, 111, 108, 14, 67, 114, 121, 112, 116, 111,
                                46, 69, 84, 72, 47, 85, 83, 68, 10, 97, 115, 115, 101, 116, 95,
                                116, 121, 112, 101, 6, 67, 114, 121, 112, 116, 111, 14, 113, 117,
                                111, 116, 101, 95, 99, 117, 114, 114, 101, 110, 99, 121, 3, 85, 83,
                                68, 11, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, 110, 7,
                                69, 84, 72, 47, 85, 83, 68, 14, 103, 101, 110, 101, 114, 105, 99,
                                95, 115, 121, 109, 98, 111, 108, 6, 69, 84, 72, 85, 83, 68, 4, 98,
                                97, 115, 101, 3, 69, 84, 72, 114, 4, 83, 112, 111, 116, 68, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            ],
                        },
                        price_accounts: vec![
                            solana_sdk::pubkey::Pubkey::from_str(
                                "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid",
                            )
                            .unwrap(),
                            solana_sdk::pubkey::Pubkey::from_str(
                                "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                            )
                            .unwrap(),
                        ],
                    },
                ),
            ]),
            price_accounts:   HashMap::from([
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic:          0xa1b2c3d4,
                        ver:            7,
                        atype:          9,
                        size:           300,
                        ptype:          PriceType::Price,
                        expo:           -8,
                        num:            8794,
                        num_qt:         32,
                        last_slot:      172761888,
                        valid_slot:     310,
                        ema_price:      Rational {
                            val:   5882210200,
                            numer: 921349408,
                            denom: 1566332030,
                        },
                        ema_conf:       Rational {
                            val:   1422289,
                            numer: 2227777916,
                            denom: 1566332030,
                        },
                        timestamp:      1667333704,
                        min_pub:        23,
                        drv2:           0xde,
                        drv3:           0xdeed,
                        drv4:           0xdeeed,
                        prod:           solana_sdk::pubkey::Pubkey::from_str(
                            "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                        )
                        .unwrap(),
                        next:           solana_sdk::pubkey::Pubkey::from_str(
                            "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                        )
                        .unwrap(),
                        prev_slot:      172761778,
                        prev_price:     22691000,
                        prev_conf:      398674,
                        prev_timestamp: 1667333702,
                        agg:            PriceInfo {
                            price:    736382,
                            conf:     85623946,
                            status:   pyth_sdk::PriceStatus::Unknown,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 7262746,
                        },
                        comp:           [PriceComp::default(); 32],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic:          0xa1b2c3d4,
                        ver:            6,
                        atype:          4,
                        size:           435,
                        ptype:          PriceType::Price,
                        expo:           -10,
                        num:            6539,
                        num_qt:         15,
                        last_slot:      7832648638,
                        valid_slot:     94728946,
                        ema_price:      Rational {
                            val:   84739769,
                            numer: 97656786,
                            denom: 1294738,
                        },
                        ema_conf:       Rational {
                            val:   987897,
                            numer: 2649374,
                            denom: 97364947,
                        },
                        timestamp:      1608606648,
                        min_pub:        35,
                        drv2:           0xde,
                        drv3:           0xdeed,
                        drv4:           0xdeeed,
                        prod:           solana_sdk::pubkey::Pubkey::from_str(
                            "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                        )
                        .unwrap(),
                        next:           solana_sdk::pubkey::Pubkey::from_str(
                            "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                        )
                        .unwrap(),
                        prev_slot:      1727612348,
                        prev_price:     746383678,
                        prev_conf:      757368,
                        prev_timestamp: 98746483673,
                        agg:            PriceInfo {
                            price:    8474837,
                            conf:     27468478,
                            status:   pyth_sdk::PriceStatus::Unknown,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 2736478,
                        },
                        comp:           pad_price_comps(vec![PriceComp {
                            publisher: solana_sdk::pubkey::Pubkey::from_str(
                                "C9syZ2MoGUwbPyGEgiy8MxesaEEKLdJw8gnwx2jLK1cV",
                            )
                            .unwrap(),
                            agg:       PriceInfo {
                                price:    85698,
                                conf:     23645,
                                status:   pyth_sdk::PriceStatus::Trading,
                                corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                pub_slot: 14765,
                            },
                            latest:    PriceInfo {
                                price:    46985,
                                conf:     32565,
                                status:   pyth_sdk::PriceStatus::Trading,
                                corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                pub_slot: 4368,
                            },
                        }]),
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic:          0xa1b2c3d4,
                        ver:            7,
                        atype:          6,
                        size:           256,
                        ptype:          PriceType::Price,
                        expo:           -6,
                        num:            474628,
                        num_qt:         20,
                        last_slot:      837476397,
                        valid_slot:     9575847498,
                        ema_price:      Rational {
                            val:   12895763,
                            numer: 736294,
                            denom: 2947646,
                        },
                        ema_conf:       Rational {
                            val:   826493,
                            numer: 58376592,
                            denom: 274628,
                        },
                        timestamp:      1192869883,
                        min_pub:        40,
                        drv2:           0xde,
                        drv3:           0xdeed,
                        drv4:           0xdeeed,
                        prod:           solana_sdk::pubkey::Pubkey::from_str(
                            "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                        )
                        .unwrap(),
                        next:           solana_sdk::pubkey::Pubkey::from_str(
                            "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                        )
                        .unwrap(),
                        prev_slot:      86484638,
                        prev_price:     28463947,
                        prev_conf:      83628234,
                        prev_timestamp: 3482628346,
                        agg:            PriceInfo {
                            price:    8254826,
                            conf:     6385638,
                            status:   pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 58462846,
                        },
                        comp:           pad_price_comps(vec![
                            PriceComp {
                                publisher: solana_sdk::pubkey::Pubkey::from_str(
                                    "DaMuPaW5dhGfRJaX7TzLWXd8hDCMJ5WA2XibJ12hjBNQ",
                                )
                                .unwrap(),
                                agg:       PriceInfo {
                                    price:    8251,
                                    conf:     7653,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 365545,
                                },
                                latest:    PriceInfo {
                                    price:    65465,
                                    conf:     451,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 886562,
                                },
                            },
                            PriceComp {
                                publisher: solana_sdk::pubkey::Pubkey::from_str(
                                    "FHuAg9vpDGeyhZn4W4FRcCzx6MC18r4bF9fTVJqeMijU",
                                )
                                .unwrap(),
                                agg:       PriceInfo {
                                    price:    39865,
                                    conf:     7456,
                                    status:   pyth_sdk::PriceStatus::Unknown,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 865,
                                },
                                latest:    PriceInfo {
                                    price:    5846,
                                    conf:     32468,
                                    status:   pyth_sdk::PriceStatus::Unknown,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 7158,
                                },
                            },
                        ]),
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic:          0xa1b2c3d4,
                        ver:            6,
                        atype:          6,
                        size:           569,
                        ptype:          PriceType::Price,
                        expo:           -9,
                        num:            474628,
                        num_qt:         24,
                        last_slot:      7865294,
                        valid_slot:     9865884,
                        ema_price:      Rational {
                            val:   863947389,
                            numer: 36846438,
                            denom: 49576384,
                        },
                        ema_conf:       Rational {
                            val:   974836,
                            numer: 97648958,
                            denom: 2536747,
                        },
                        timestamp:      1190171722,
                        min_pub:        23,
                        drv2:           0xde,
                        drv3:           0xdeed,
                        drv4:           0xdeeed,
                        prod:           solana_sdk::pubkey::Pubkey::from_str(
                            "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                        )
                        .unwrap(),
                        next:           solana_sdk::pubkey::Pubkey::default(),
                        prev_slot:      791279274,
                        prev_price:     98272648,
                        prev_conf:      124986284,
                        prev_timestamp: 1507933434,
                        agg:            PriceInfo {
                            price:    876384,
                            conf:     1349364,
                            status:   pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 987236484,
                        },
                        comp:           pad_price_comps(vec![
                            PriceComp {
                                publisher: solana_sdk::pubkey::Pubkey::from_str(
                                    "F42dQ3SMssashRsA4SRfwJxFkGKV1bE3TcmpkagX8vvX",
                                )
                                .unwrap(),
                                agg:       PriceInfo {
                                    price:    54842,
                                    conf:     599755,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 1976465,
                                },
                                latest:    PriceInfo {
                                    price:    394764,
                                    conf:     26485,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 369454,
                                },
                            },
                            PriceComp {
                                publisher: solana_sdk::pubkey::Pubkey::from_str(
                                    "AmmvowPnL2z1CVGR2fQNjgAmmJvRfpCKqpQMpTg9QsoG",
                                )
                                .unwrap(),
                                agg:       PriceInfo {
                                    price:    65649,
                                    conf:     55896,
                                    status:   pyth_sdk::PriceStatus::Unknown,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 32976,
                                },
                                latest:    PriceInfo {
                                    price:    18616,
                                    conf:     254458,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 3126545,
                                },
                            },
                        ]),
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic:          0xa1b2c3d4,
                        ver:            8,
                        atype:          4,
                        size:           225,
                        ptype:          PriceType::Price,
                        expo:           -6,
                        num:            38637,
                        num_qt:         21,
                        last_slot:      385638,
                        valid_slot:     28463828,
                        ema_price:      Rational {
                            val:   46280183,
                            numer: 2846192,
                            denom: 98367492,
                        },
                        ema_conf:       Rational {
                            val:   1645284,
                            numer: 3957639,
                            denom: 9857392,
                        },
                        timestamp:      1580448604,
                        min_pub:        19,
                        drv2:           0xde,
                        drv3:           0xdeed,
                        drv4:           0xdeeed,
                        prod:           solana_sdk::pubkey::Pubkey::from_str(
                            "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                        )
                        .unwrap(),
                        next:           solana_sdk::pubkey::Pubkey::from_str(
                            "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                        )
                        .unwrap(),
                        prev_slot:      893734828,
                        prev_price:     13947294,
                        prev_conf:      349274938,
                        prev_timestamp: 1064251291,
                        agg:            PriceInfo {
                            price:    397492,
                            conf:     33487,
                            status:   pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 529857382,
                        },
                        comp:           pad_price_comps(vec![
                            PriceComp {
                                publisher: solana_sdk::pubkey::Pubkey::from_str(
                                    "8MMroLyuyxyeDRrzMNfpymC5RvmHtQiYooXX9bgeUJdM",
                                )
                                .unwrap(),
                                agg:       PriceInfo {
                                    price:    69854,
                                    conf:     732565,
                                    status:   pyth_sdk::PriceStatus::Unknown,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 213654,
                                },
                                latest:    PriceInfo {
                                    price:    79556,
                                    conf:     565461,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 863125,
                                },
                            },
                            PriceComp {
                                publisher: solana_sdk::pubkey::Pubkey::from_str(
                                    "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                                )
                                .unwrap(),
                                agg:       PriceInfo {
                                    price:    3265,
                                    conf:     8962196,
                                    status:   pyth_sdk::PriceStatus::Trading,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 301541,
                                },
                                latest:    PriceInfo {
                                    price:    465132,
                                    conf:     8476531,
                                    status:   pyth_sdk::PriceStatus::Unknown,
                                    corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                    pub_slot: 78964,
                                },
                            },
                        ]),
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic:          0xa1b2c3d4,
                        ver:            6,
                        atype:          3,
                        size:           287,
                        ptype:          PriceType::Price,
                        expo:           2,
                        num:            74638,
                        num_qt:         46,
                        last_slot:      28467283,
                        valid_slot:     4846283,
                        ema_price:      Rational {
                            val:   876979749,
                            numer: 37356978,
                            denom: 987859474,
                        },
                        ema_conf:       Rational {
                            val:   2664983,
                            numer: 4987935,
                            denom: 653893789,
                        },
                        timestamp:      997964053,
                        min_pub:        14,
                        drv2:           0xde,
                        drv3:           0xdeed,
                        drv4:           0xdeeed,
                        prod:           solana_sdk::pubkey::Pubkey::from_str(
                            "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                        )
                        .unwrap(),
                        next:           solana_sdk::pubkey::Pubkey::default(),
                        prev_slot:      8878456286,
                        prev_price:     24746384,
                        prev_conf:      6373957,
                        prev_timestamp: 1056634590,
                        agg:            PriceInfo {
                            price:    836489,
                            conf:     6769467,
                            status:   pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 6863892,
                        },
                        comp:           pad_price_comps(vec![PriceComp {
                            publisher: solana_sdk::pubkey::Pubkey::from_str(
                                "33B2brfdz16kizEXeQvYzJXHiS1X95L8pfetuyntEiXg",
                            )
                            .unwrap(),
                            agg:       PriceInfo {
                                price:    61478,
                                conf:     312545,
                                status:   pyth_sdk::PriceStatus::Trading,
                                corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                pub_slot: 302156,
                            },
                            latest:    PriceInfo {
                                price:    85315,
                                conf:     754256,
                                status:   pyth_sdk::PriceStatus::Unknown,
                                corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                                pub_slot: 7101326,
                            },
                        }]),
                    },
                ),
            ]),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_get_all_products() {
        // Start the test adapter
        let mut test_adapter = setup().await;

        // Send a Get All Products message
        let (result_tx, result_rx) = oneshot::channel();
        test_adapter
            .message_tx
            .send(Message::GetAllProducts { result_tx })
            .await
            .unwrap();

        // Return the Solana oracle data to the adapter, from the global store
        match test_adapter.global_store_rx.recv().await.unwrap() {
            global::Message::LookupSolanaOracleData { result_tx } => {
                result_tx.send(Ok(get_test_global_oracle_data())).unwrap()
            }
            _ => panic!("Uexpected message received from adapter"),
        };

        // Check that the result of the conversion to the Pythd API format is what we expected
        let expected = vec![
            api::ProductAccount {
                account:        "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc".to_string(),
                attr_dict:      BTreeMap::from(
                    [
                        ("symbol", "Crypto.ETH/USD"),
                        ("asset_type", "Crypto"),
                        ("quote_currency", "USD"),
                        ("description", "ETH/USD"),
                        ("generic_symbol", "ETHUSD"),
                        ("base", "ETH"),
                    ]
                    .map(|(k, v)| (k.to_string(), v.to_string())),
                ),
                price_accounts: vec![
                    api::PriceAccount {
                        account:            "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD"
                            .to_string(),
                        price_type:         "price".to_string(),
                        price_exponent:     -9,
                        status:             "trading".to_string(),
                        price:              876384,
                        conf:               1349364,
                        twap:               863947389,
                        twac:               974836,
                        valid_slot:         9865884,
                        pub_slot:           987236484,
                        prev_slot:          791279274,
                        prev_price:         98272648,
                        prev_conf:          124986284,
                        publisher_accounts: vec![
                            PublisherAccount {
                                account: "F42dQ3SMssashRsA4SRfwJxFkGKV1bE3TcmpkagX8vvX".to_string(),
                                status:  "trading".to_string(),
                                price:   54842,
                                conf:    599755,
                                slot:    1976465,
                            },
                            PublisherAccount {
                                account: "AmmvowPnL2z1CVGR2fQNjgAmmJvRfpCKqpQMpTg9QsoG".to_string(),
                                status:  "unknown".to_string(),
                                price:   65649,
                                conf:    55896,
                                slot:    32976,
                            },
                        ],
                    },
                    api::PriceAccount {
                        account:            "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid"
                            .to_string(),
                        price_type:         "price".to_string(),
                        price_exponent:     -6,
                        status:             "trading".to_string(),
                        price:              397492,
                        conf:               33487,
                        twap:               46280183,
                        twac:               1645284,
                        valid_slot:         28463828,
                        pub_slot:           529857382,
                        prev_slot:          893734828,
                        prev_price:         13947294,
                        prev_conf:          349274938,
                        publisher_accounts: vec![
                            PublisherAccount {
                                account: "8MMroLyuyxyeDRrzMNfpymC5RvmHtQiYooXX9bgeUJdM".to_string(),
                                status:  "unknown".to_string(),
                                price:   69854,
                                conf:    732565,
                                slot:    213654,
                            },
                            PublisherAccount {
                                account: "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ".to_string(),
                                status:  "trading".to_string(),
                                price:   3265,
                                conf:    8962196,
                                slot:    301541,
                            },
                        ],
                    },
                    api::PriceAccount {
                        account:            "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ"
                            .to_string(),
                        price_type:         "price".to_string(),
                        price_exponent:     2,
                        status:             "trading".to_string(),
                        price:              836489,
                        conf:               6769467,
                        twap:               876979749,
                        twac:               2664983,
                        valid_slot:         4846283,
                        pub_slot:           6863892,
                        prev_slot:          8878456286,
                        prev_price:         24746384,
                        prev_conf:          6373957,
                        publisher_accounts: vec![PublisherAccount {
                            account: "33B2brfdz16kizEXeQvYzJXHiS1X95L8pfetuyntEiXg".to_string(),
                            status:  "trading".to_string(),
                            price:   61478,
                            conf:    312545,
                            slot:    302156,
                        }],
                    },
                ],
            },
            api::ProductAccount {
                account:        "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t".to_string(),
                attr_dict:      BTreeMap::from(
                    [
                        ("symbol", "Crypto.LTC/USD"),
                        ("asset_type", "Crypto"),
                        ("quote_currency", "USD"),
                        ("description", "LTC/USD"),
                        ("generic_symbol", "LTCUSD"),
                        ("base", "LTC"),
                    ]
                    .map(|(k, v)| (k.to_string(), v.to_string())),
                ),
                price_accounts: vec![
                    api::PriceAccount {
                        account:            "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU"
                            .to_string(),
                        price_type:         "price".to_string(),
                        price_exponent:     -8,
                        status:             "unknown".to_string(),
                        price:              736382,
                        conf:               85623946,
                        twap:               5882210200,
                        twac:               1422289,
                        valid_slot:         310,
                        pub_slot:           7262746,
                        prev_slot:          172761778,
                        prev_price:         22691000,
                        prev_conf:          398674,
                        publisher_accounts: vec![],
                    },
                    api::PriceAccount {
                        account:            "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr"
                            .to_string(),
                        price_type:         "price".to_string(),
                        price_exponent:     -10,
                        status:             "unknown".to_string(),
                        price:              8474837,
                        conf:               27468478,
                        twap:               84739769,
                        twac:               987897,
                        valid_slot:         94728946,
                        pub_slot:           2736478,
                        prev_slot:          1727612348,
                        prev_price:         746383678,
                        prev_conf:          757368,
                        publisher_accounts: vec![PublisherAccount {
                            account: "C9syZ2MoGUwbPyGEgiy8MxesaEEKLdJw8gnwx2jLK1cV".to_string(),
                            status:  "trading".to_string(),
                            price:   85698,
                            conf:    23645,
                            slot:    14765,
                        }],
                    },
                    api::PriceAccount {
                        account:            "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6"
                            .to_string(),
                        price_type:         "price".to_string(),
                        price_exponent:     -6,
                        status:             "trading".to_string(),
                        price:              8254826,
                        conf:               6385638,
                        twap:               12895763,
                        twac:               826493,
                        valid_slot:         9575847498,
                        pub_slot:           58462846,
                        prev_slot:          86484638,
                        prev_price:         28463947,
                        prev_conf:          83628234,
                        publisher_accounts: vec![
                            PublisherAccount {
                                account: "DaMuPaW5dhGfRJaX7TzLWXd8hDCMJ5WA2XibJ12hjBNQ".to_string(),
                                status:  "trading".to_string(),
                                price:   8251,
                                conf:    7653,
                                slot:    365545,
                            },
                            PublisherAccount {
                                account: "FHuAg9vpDGeyhZn4W4FRcCzx6MC18r4bF9fTVJqeMijU".to_string(),
                                status:  "unknown".to_string(),
                                price:   39865,
                                conf:    7456,
                                slot:    865,
                            },
                        ],
                    },
                ],
            },
        ];

        let mut result = result_rx.await.unwrap().unwrap();
        result.sort();
        assert_eq!(result, expected);
    }
}
