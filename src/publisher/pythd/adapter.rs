use std::{collections::HashMap, time::Duration};

use super::super::store::{global, PriceIdentifier};
use super::api::{
    self, Conf, NotifyPrice, NotifyPriceSched, Price, PriceAccountMetadata, ProductAccount,
    ProductAccountMetadata, SubscriptionID,
};
use anyhow::{anyhow, Result};
use pyth_sdk::Identifier;
use slog::Logger;
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{self, Interval},
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
    subscription_id: SubscriptionID,
    /// Channel notifications are sent on
    notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
}

#[derive(Debug)]
pub enum Message {
    GetProductList {
        result_tx: oneshot::Sender<Result<Vec<ProductAccountMetadata>>>,
    },
    GetProduct {
        account: api::Pubkey,
        result_tx: oneshot::Sender<Result<ProductAccount>>,
    },
    GetAllProducts {
        result_tx: oneshot::Sender<Result<Vec<ProductAccount>>>,
    },
    SubscribePrice {
        account: api::Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
        result_tx: oneshot::Sender<Result<SubscriptionID>>,
    },
    SubscribePriceSched {
        account: api::Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
        result_tx: oneshot::Sender<Result<SubscriptionID>>,
    },
    UpdatePrice {
        account: api::Pubkey,
        price: Price,
        conf: Conf,
        status: String,
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
            Message::GetAllProducts { result_tx } => todo!(),
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
                    account: price_account_key.to_string(),
                    price_type: "price".to_owned(),
                    price_exponent: price_account.expo as i64,
                })
                .collect();

            // Create the product account metadata struct
            result.push(ProductAccountMetadata {
                account: product_account_key.to_string(),
                attr_dict: product_account.attr_dict,
                prices: price_accounts_metadata,
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
    use iobuffer::IoBuffer;
    use slog_extlog::slog_test;
    use std::{
        collections::{BTreeMap, HashMap},
        str::FromStr,
        time::Duration,
    };
    use tokio::{
        sync::{broadcast, mpsc, oneshot},
        task::JoinHandle,
    };

    use crate::publisher::{
        pythd::api::{NotifyPriceSched, PriceAccountMetadata, ProductAccountMetadata},
        store::global,
    };

    use super::{Adapter, Message};

    struct TestAdapter {
        message_tx: mpsc::Sender<Message>,
        shutdown_tx: broadcast::Sender<()>,
        global_store_rx: mpsc::Receiver<global::Message>,
        jh: JoinHandle<()>,
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
                    subscription: subscription_id
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
            price_accounts_metadata: HashMap::from([
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
                account: "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc".to_string(),
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
                prices: vec![
                    PriceAccountMetadata {
                        account: "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD".to_string(),
                        price_type: "price".to_string(),
                        price_exponent: -9,
                    },
                    PriceAccountMetadata {
                        account: "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid".to_string(),
                        price_type: "price".to_string(),
                        price_exponent: -6,
                    },
                    PriceAccountMetadata {
                        account: "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ".to_string(),
                        price_type: "price".to_string(),
                        price_exponent: 2,
                    },
                ],
            },
            ProductAccountMetadata {
                account: "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t".to_string(),
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
                prices: vec![
                    PriceAccountMetadata {
                        account: "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU".to_string(),
                        price_type: "price".to_string(),
                        price_exponent: -8,
                    },
                    PriceAccountMetadata {
                        account: "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr".to_string(),
                        price_type: "price".to_string(),
                        price_exponent: -10,
                    },
                    PriceAccountMetadata {
                        account: "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6".to_string(),
                        price_type: "price".to_string(),
                        price_exponent: -6,
                    },
                ],
            },
        ];

        let mut result = result_rx.await.unwrap().unwrap();
        result.sort();
        assert_eq!(result, expected);
    }
}
