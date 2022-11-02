use std::{collections::HashMap, time::Duration};

use super::super::solana;
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

    /// Channels on which to communicate to the global stores
    primary_global_store_tx: mpsc::Sender<global::Message>,

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
        primary_global_store_tx: mpsc::Sender<global::Message>,
        shutdown_rx: broadcast::Receiver<()>,
        logger: Logger,
    ) -> Self {
        Adapter {
            message_rx,
            subscription_id_count: 0,
            notify_price_sched_subscriptions: HashMap::new(),
            notify_price_sched_interval: time::interval(notify_price_sched_interval),
            primary_global_store_tx,
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
        self.lookup_product_list(&self.primary_global_store_tx)
            .await

        // TODO: fall back to secondary global store if primary is unavailable
    }

    async fn lookup_product_list(
        &self,
        global_store_tx: &mpsc::Sender<global::Message>,
    ) -> Result<Vec<ProductAccountMetadata>> {
        let solana_data = self.lookup_solana_oracle_data(global_store_tx).await?;

        let mut result = Vec::new();
        for (product_account_key, product_account) in solana_data.product_accounts {
            // Transform the price accounts into the PriceAccountMetadata structs
            // the API uses.
            let price_accounts_metadata = product_account
                .price_accounts
                .iter()
                .filter_map(|price_account_key| {
                    solana_data
                        .price_accounts
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
                attr_dict: product_account
                    .account_data
                    .iter()
                    .filter(|(key, val)| !key.is_empty() && !val.is_empty())
                    .map(|(key, val)| (key.to_owned(), val.to_owned()))
                    .collect(),
                prices: price_accounts_metadata,
            })
        }

        Ok(result)
    }

    // Fetches the Solana-specific Oracle data from the given global store
    async fn lookup_solana_oracle_data(
        &self,
        tx: &mpsc::Sender<global::Message>,
    ) -> Result<solana::oracle::Data> {
        let (result_tx, result_rx) = oneshot::channel();
        tx.send(global::Message::LookupSolanaOracleData { result_tx })
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
    use pyth_sdk_solana::state::{
        MappingAccount, PriceAccount, PriceComp, PriceInfo, PriceType, Rational, MAP_TABLE_SIZE,
    };
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
        solana,
        store::global,
    };

    use super::{Adapter, Message};

    struct TestAdapter {
        message_tx: mpsc::Sender<Message>,
        shutdown_tx: broadcast::Sender<()>,
        primary_global_store_rx: mpsc::Receiver<global::Message>,
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
        let (primary_global_store_tx, primary_global_store_rx) = mpsc::channel(1000);
        let notify_price_sched_interval = Duration::from_nanos(10);
        let logger = slog_test::new_test_logger(IoBuffer::new());
        let (shutdown_tx, shutdown_rx) = broadcast::channel(10);
        let mut adapter = Adapter::new(
            adapter_rx,
            notify_price_sched_interval,
            primary_global_store_tx,
            shutdown_rx,
            logger,
        );
        let jh = tokio::spawn(async move { adapter.run().await });

        TestAdapter {
            message_tx: adapter_tx,
            primary_global_store_rx,
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

    fn get_test_primary_oracle_data() -> solana::oracle::Data {
        solana::oracle::Data {
            mapping_accounts: HashMap::from([(
                solana_sdk::pubkey::Pubkey::from_str(
                    "7ycfa1ENNT5dVVoMtiMjsgVbkWKFJbu6nF2h1UVT18Cf",
                )
                .unwrap(),
                MappingAccount {
                    magic: 0xa1b2c3d4,
                    ver: 4,
                    atype: 3,
                    size: 500,
                    num: 3,
                    unused: 0,
                    next: solana_sdk::pubkey::Pubkey::default(),
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
                        account_data: pyth_sdk_solana::state::ProductAccount {
                            magic: 0xa1b2c3d4,
                            ver: 6,
                            atype: 4,
                            size: 340,
                            px_acc: solana_sdk::pubkey::Pubkey::from_str(
                                "EKZYBqisdcsn3shN1rQRuWTzH3iqMbj1dxFtDFmrBi8o",
                            )
                            .unwrap(),
                            // LTC/USD
                            attr: [
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
                        account_data: pyth_sdk_solana::state::ProductAccount {
                            magic: 0xa1b2c3d4,
                            ver: 5,
                            atype: 3,
                            size: 478,
                            px_acc: solana_sdk::pubkey::Pubkey::from_str(
                                "JTmFx5zX9mM94itfk2nQcJnQQDPjcv4UPD7SYj6xDCV",
                            )
                            .unwrap(),
                            // ETH/USD
                            attr: [
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
            price_accounts: HashMap::from([
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic: 0xa1b2c3d4,
                        ver: 7,
                        atype: 9,
                        size: 300,
                        ptype: PriceType::Price,
                        expo: -8,
                        num: 8794,
                        num_qt: 32,
                        last_slot: 172761888,
                        valid_slot: 310,
                        ema_price: Rational {
                            val: 5882210200,
                            numer: 921349408,
                            denom: 1566332030,
                        },
                        ema_conf: Rational {
                            val: 1422289,
                            numer: 2227777916,
                            denom: 1566332030,
                        },
                        timestamp: 1667333704,
                        min_pub: 23,
                        drv2: 0xde,
                        drv3: 0xdeed,
                        drv4: 0xdeeed,
                        prod: solana_sdk::pubkey::Pubkey::from_str(
                            "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                        )
                        .unwrap(),
                        next: solana_sdk::pubkey::Pubkey::from_str(
                            "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                        )
                        .unwrap(),
                        prev_slot: 172761778,
                        prev_price: 22691000,
                        prev_conf: 398674,
                        prev_timestamp: 1667333702,
                        agg: PriceInfo {
                            price: 736382,
                            conf: 85623946,
                            status: pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 7262746,
                        },
                        comp: [PriceComp::default(); 32],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "3VQwtcntVQN1mj1MybQw8qK7Li3KNrrgNskSQwZAPGNr",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic: 0xa1b2c3d4,
                        ver: 6,
                        atype: 4,
                        size: 435,
                        ptype: PriceType::Price,
                        expo: -10,
                        num: 6539,
                        num_qt: 15,
                        last_slot: 7832648638,
                        valid_slot: 94728946,
                        ema_price: Rational {
                            val: 84739769,
                            numer: 97656786,
                            denom: 1294738,
                        },
                        ema_conf: Rational {
                            val: 987897,
                            numer: 2649374,
                            denom: 97364947,
                        },
                        timestamp: 1608606648,
                        min_pub: 35,
                        drv2: 0xde,
                        drv3: 0xdeed,
                        drv4: 0xdeeed,
                        prod: solana_sdk::pubkey::Pubkey::from_str(
                            "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                        )
                        .unwrap(),
                        next: solana_sdk::pubkey::Pubkey::from_str(
                            "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                        )
                        .unwrap(),
                        prev_slot: 1727612348,
                        prev_price: 746383678,
                        prev_conf: 757368,
                        prev_timestamp: 98746483673,
                        agg: PriceInfo {
                            price: 8474837,
                            conf: 27468478,
                            status: pyth_sdk::PriceStatus::Unknown,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 2736478,
                        },
                        comp: [PriceComp::default(); 32],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "2V7t5NaKY7aGkwytCWQgvUYZfEr9XMwNChhJEakTExk6",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic: 0xa1b2c3d4,
                        ver: 7,
                        atype: 6,
                        size: 256,
                        ptype: PriceType::Price,
                        expo: -6,
                        num: 474628,
                        num_qt: 20,
                        last_slot: 837476397,
                        valid_slot: 9575847498,
                        ema_price: Rational {
                            val: 12895763,
                            numer: 736294,
                            denom: 2947646,
                        },
                        ema_conf: Rational {
                            val: 826493,
                            numer: 58376592,
                            denom: 274628,
                        },
                        timestamp: 1192869883,
                        min_pub: 40,
                        drv2: 0xde,
                        drv3: 0xdeed,
                        drv4: 0xdeeed,
                        prod: solana_sdk::pubkey::Pubkey::from_str(
                            "CkMrDWtmFJZcmAUC11qNaWymbXQKvnRx4cq1QudLav7t",
                        )
                        .unwrap(),
                        next: solana_sdk::pubkey::Pubkey::from_str(
                            "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                        )
                        .unwrap(),
                        prev_slot: 86484638,
                        prev_price: 28463947,
                        prev_conf: 83628234,
                        prev_timestamp: 3482628346,
                        agg: PriceInfo {
                            price: 8254826,
                            conf: 6385638,
                            status: pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 58462846,
                        },
                        comp: [PriceComp::default(); 32],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GG3FTE7xhc9Diy7dn9P6BWzoCrAEE4D3p5NBYrDAm5DD",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic: 0xa1b2c3d4,
                        ver: 6,
                        atype: 6,
                        size: 569,
                        ptype: PriceType::Price,
                        expo: -9,
                        num: 474628,
                        num_qt: 24,
                        last_slot: 7865294,
                        valid_slot: 9865884,
                        ema_price: Rational {
                            val: 863947389,
                            numer: 36846438,
                            denom: 49576384,
                        },
                        ema_conf: Rational {
                            val: 974836,
                            numer: 97648958,
                            denom: 2536747,
                        },
                        timestamp: 1190171722,
                        min_pub: 23,
                        drv2: 0xde,
                        drv3: 0xdeed,
                        drv4: 0xdeeed,
                        prod: solana_sdk::pubkey::Pubkey::from_str(
                            "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                        )
                        .unwrap(),
                        next: solana_sdk::pubkey::Pubkey::default(),
                        prev_slot: 791279274,
                        prev_price: 98272648,
                        prev_conf: 124986284,
                        prev_timestamp: 1507933434,
                        agg: PriceInfo {
                            price: 876384,
                            conf: 1349364,
                            status: pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 987236484,
                        },
                        comp: [PriceComp::default(); 32],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "fTNjSfj5uW9e4CAMHzUcm65ftRNBxCN1gG5GS1mYfid",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic: 0xa1b2c3d4,
                        ver: 8,
                        atype: 4,
                        size: 225,
                        ptype: PriceType::Price,
                        expo: -6,
                        num: 38637,
                        num_qt: 21,
                        last_slot: 385638,
                        valid_slot: 28463828,
                        ema_price: Rational {
                            val: 46280183,
                            numer: 2846192,
                            denom: 98367492,
                        },
                        ema_conf: Rational {
                            val: 1645284,
                            numer: 3957639,
                            denom: 9857392,
                        },
                        timestamp: 1580448604,
                        min_pub: 19,
                        drv2: 0xde,
                        drv3: 0xdeed,
                        drv4: 0xdeeed,
                        prod: solana_sdk::pubkey::Pubkey::from_str(
                            "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                        )
                        .unwrap(),
                        next: solana_sdk::pubkey::Pubkey::from_str(
                            "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                        )
                        .unwrap(),
                        prev_slot: 893734828,
                        prev_price: 13947294,
                        prev_conf: 349274938,
                        prev_timestamp: 1064251291,
                        agg: PriceInfo {
                            price: 397492,
                            conf: 33487,
                            status: pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 529857382,
                        },
                        comp: [PriceComp::default(); 32],
                    },
                ),
                (
                    solana_sdk::pubkey::Pubkey::from_str(
                        "GKNcUmNacSJo4S2Kq3DuYRYRGw3sNUfJ4tyqd198t6vQ",
                    )
                    .unwrap(),
                    PriceAccount {
                        magic: 0xa1b2c3d4,
                        ver: 6,
                        atype: 3,
                        size: 287,
                        ptype: PriceType::Price,
                        expo: 2,
                        num: 74638,
                        num_qt: 46,
                        last_slot: 28467283,
                        valid_slot: 4846283,
                        ema_price: Rational {
                            val: 876979749,
                            numer: 37356978,
                            denom: 987859474,
                        },
                        ema_conf: Rational {
                            val: 2664983,
                            numer: 4987935,
                            denom: 653893789,
                        },
                        timestamp: 997964053,
                        min_pub: 14,
                        drv2: 0xde,
                        drv3: 0xdeed,
                        drv4: 0xdeeed,
                        prod: solana_sdk::pubkey::Pubkey::from_str(
                            "BjHoZWRxo9dgbR1NQhPyTiUs6xFiX6mGS4TMYvy3b2yc",
                        )
                        .unwrap(),
                        next: solana_sdk::pubkey::Pubkey::default(),
                        prev_slot: 8878456286,
                        prev_price: 24746384,
                        prev_conf: 6373957,
                        prev_timestamp: 1056634590,
                        agg: PriceInfo {
                            price: 836489,
                            conf: 6769467,
                            status: pyth_sdk::PriceStatus::Trading,
                            corp_act: pyth_sdk_solana::state::CorpAction::NoCorpAct,
                            pub_slot: 6863892,
                        },
                        comp: [PriceComp::default(); 32],
                    },
                ),
            ]),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_get_product_list_primary() {
        // Start the test adapter
        let mut test_adapter = setup().await;

        // Send a Get Product List message
        let (result_tx, result_rx) = oneshot::channel();
        test_adapter
            .message_tx
            .send(Message::GetProductList { result_tx })
            .await
            .unwrap();

        // Return the product list to the adapter, from the primary global store
        match test_adapter.primary_global_store_rx.recv().await.unwrap() {
            global::Message::LookupSolanaOracleData { result_tx } => {
                result_tx.send(Ok(get_test_primary_oracle_data())).unwrap()
            }
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
