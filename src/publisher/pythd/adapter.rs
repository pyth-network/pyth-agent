use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::Utc;
use pyth_sdk::{Identifier, PriceStatus};
use slog::Logger;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Interval};

use crate::publisher::store::global::SolanaData;
use solana_sdk::pubkey::Pubkey;

use super::super::store::{global, local, PriceIdentifier};
use super::api::{
    self, Conf, NotifyPrice, NotifyPriceSched, Price, PriceAccountMetadata, ProductAccount,
    ProductAccountMetadata, PublisherAccount, SubscriptionID,
};

pub type PriceUpdate = super::api::PriceUpdate;

// Adapter is the adapter between the pythd websocket API, and the stores.
// It is responsible for implementing the business logic for responding to
// the pythd websocket API calls.
// TODO: better name than Adapter
pub struct Adapter {
    // Channel on which messages are received
    message_rx: mpsc::Receiver<Message>,

    // Subscription ID counter
    subscription_id_count: SubscriptionID,

    // Notify Price Subscriptions
    notify_price_subscriptions: HashMap<PriceIdentifier, Vec<NotifyPriceSubscription>>,

    // Notify Price Sched subscriptions
    notify_price_sched_subscriptions: HashMap<PriceIdentifier, Vec<NotifyPriceSchedSubscription>>,
    notify_price_sched_interval: Interval,

    // Channel on which to communicate to the local store
    local_tx: mpsc::Sender<local::Message>,

    // Channel on which to communicate to the global stores
    primary_global_tx: mpsc::Sender<global::Message>,
    secondary_global_tx: Option<mpsc::Sender<global::Message>>,

    // The logger
    logger: Logger,
}

#[derive(Debug)]
pub enum Message {
    GlobalStoreUpdate {
        price_identifier: PriceIdentifier,
        update: PriceUpdate,
    },
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
    Shutdown,
}

struct NotifyPriceSubscription {
    subscription_id: SubscriptionID,
    notify_price_tx: mpsc::Sender<NotifyPrice>,
}

struct NotifyPriceSchedSubscription {
    subscription_id: SubscriptionID,
    notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
}

// TODO: refactor to make nice
// TODO: tests
impl Adapter {
    pub fn new(
        message_rx: Receiver<Message>,
        notify_price_sched_interval: Duration,
        local_tx: Sender<local::Message>,
        primary_global_tx: Sender<global::Message>,
        secondary_global_tx: Option<Sender<global::Message>>,
        logger: Logger,
    ) -> Self {
        Adapter {
            message_rx,
            subscription_id_count: 0,
            notify_price_subscriptions: HashMap::new(),
            notify_price_sched_subscriptions: HashMap::new(),
            notify_price_sched_interval: time::interval(notify_price_sched_interval),
            local_tx,
            primary_global_tx,
            secondary_global_tx,
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
            Some(message) = self.message_rx.recv() => {
                self.handle_message(message).await
            }
            _ = self.notify_price_sched_interval.tick() => {
                self.send_subscribe_price_sched().await
            }
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::GlobalStoreUpdate {
                price_identifier,
                update,
            } => {
                self.handle_global_store_update(price_identifier, update)
                    .await
            }
            Message::GetProductList { result_tx } => {
                self.send(result_tx, self.handle_get_product_list().await)
            }
            Message::GetProduct { account, result_tx } => {
                self.send(result_tx, self.handle_get_product(&account.parse()?).await)
            }
            Message::GetAllProducts { result_tx } => {
                self.send(result_tx, self.handle_get_all_products().await)
            }
            Message::SubscribePrice {
                account,
                notify_price_tx,
                result_tx,
            } => {
                let subscription_id = self
                    .handle_subscribe_price(&account.parse()?, notify_price_tx)
                    .await;
                self.send(result_tx, Ok(subscription_id))
            }
            Message::SubscribePriceSched {
                account,
                result_tx,
                notify_price_sched_tx,
            } => {
                let subscription_id = self
                    .handle_subscribe_price_sched(&account.parse()?, notify_price_sched_tx)
                    .await;
                self.send(result_tx, Ok(subscription_id))
            }
            Message::UpdatePrice {
                account,
                price,
                conf,
                status,
            } => {
                self.handle_update_price(&account.parse()?, price, conf, status)
                    .await
            }
            Message::Shutdown => todo!(),
        }
    }

    fn send<T>(&self, tx: oneshot::Sender<T>, item: T) -> Result<()> {
        tx.send(item).map_err(|_| anyhow!("sending channel full"))
    }

    async fn handle_global_store_update(
        &self,
        price_identifier: PriceIdentifier,
        update: PriceUpdate,
    ) -> Result<()> {
        // Look up any subcriptions associated with the price identifier
        let empty = Vec::new();
        let subscriptions = self
            .notify_price_subscriptions
            .get(&price_identifier)
            .unwrap_or(&empty);

        // Send the Notify Price update to each subscription
        for subscription in subscriptions {
            subscription
                .notify_price_tx
                .send(NotifyPrice {
                    subscription: subscription.subscription_id,
                    result: update.clone(),
                })
                .await?;
        }

        Ok(())
    }

    async fn handle_get_product_list(&self) -> Result<Vec<ProductAccountMetadata>> {
        let primary_result = self.lookup_product_list(&self.primary_global_tx).await;

        // Fall back to secondary network if possible
        if primary_result.is_err() {
            if let Some(secondary_global_tx) = &self.secondary_global_tx {
                return self.lookup_product_list(secondary_global_tx).await;
            }
        }

        primary_result
    }

    async fn lookup_product_list(
        &self,
        global_store_tx: &Sender<global::Message>,
    ) -> Result<Vec<ProductAccountMetadata>> {
        let solana_data = self.lookup_solana_data(global_store_tx).await?;

        let mut result = Vec::new();
        for (product_account_key, product_account) in solana_data.product_accounts {
            // Extract all the price accounts from the product account
            let prices = product_account
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
                    .map(|(key, val)| (key.to_owned(), val.to_owned()))
                    .collect(),
                prices,
            })
        }

        Ok(result)
    }

    async fn handle_get_product(&self, product_account_key: &Pubkey) -> Result<ProductAccount> {
        let primary_result = self
            .lookup_product(product_account_key, &self.primary_global_tx)
            .await;

        // Fall back to secondary network if possible
        if primary_result.is_err() {
            if let Some(secondary_global_tx) = &self.secondary_global_tx {
                return self
                    .lookup_product(product_account_key, secondary_global_tx)
                    .await;
            }
        }

        primary_result
    }

    async fn lookup_product(
        &self,
        product_account_key: &Pubkey,
        global_store_tx: &Sender<global::Message>,
    ) -> Result<ProductAccount> {
        let solana_data = self.lookup_solana_data(global_store_tx).await?;

        // Look up the product account
        let product_account = solana_data
            .product_accounts
            .get(product_account_key)
            .ok_or_else(|| anyhow!("product account not found"))?;

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

        Ok(ProductAccount {
            account: product_account_key.to_string(),
            attr_dict: product_account
                .account_data
                .iter()
                .map(|(key, val)| (key.to_owned(), val.to_owned()))
                .collect(),
            price_accounts,
        })
    }

    async fn handle_get_all_products(&self) -> Result<Vec<ProductAccount>> {
        let primary_result = self.lookup_all_products(&self.primary_global_tx).await;

        // Fall back to secondary network if possible
        if primary_result.is_err() {
            if let Some(secondary_global_tx) = &self.secondary_global_tx {
                return self.lookup_all_products(secondary_global_tx).await;
            }
        }

        primary_result
    }

    async fn lookup_all_products(
        &self,
        global_store_tx: &Sender<global::Message>,
    ) -> Result<Vec<ProductAccount>> {
        let solana_data = self.lookup_solana_data(global_store_tx).await?;

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
                    .map(|(key, val)| (key.to_owned(), val.to_owned()))
                    .collect(),
                price_accounts,
            })
        }

        Ok(result)
    }

    fn solana_price_account_to_pythd_api_price_account(
        price_account_key: &Pubkey,
        price_account: &pyth_sdk_solana::state::PriceAccount,
    ) -> api::PriceAccount {
        api::PriceAccount {
            account: price_account_key.to_string(),
            price_type: "price".to_string(),
            price_exponent: price_account.expo as i64,
            status: Self::price_status_to_str(price_account.agg.status),
            price: price_account.agg.price,
            conf: price_account.agg.conf,
            twap: price_account.ema_price.val,
            twac: price_account.ema_conf.val,
            valid_slot: price_account.valid_slot,
            pub_slot: price_account.agg.pub_slot,
            prev_slot: price_account.prev_slot,
            prev_price: price_account.prev_price,
            prev_conf: price_account.prev_conf,
            publisher_accounts: price_account
                .comp
                .iter()
                .map(|comp| PublisherAccount {
                    account: comp.publisher.to_string(),
                    status: Self::price_status_to_str(comp.agg.status),
                    price: comp.agg.price,
                    conf: comp.agg.conf,
                    slot: comp.agg.pub_slot,
                })
                .collect(),
        }
    }

    async fn handle_subscribe_price(
        &mut self,
        account: &Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
    ) -> SubscriptionID {
        let subscription_id = self.next_subscription_id();
        self.notify_price_subscriptions
            .entry(Identifier::new(account.to_bytes()))
            .or_default()
            .push(NotifyPriceSubscription {
                subscription_id,
                notify_price_tx,
            });
        subscription_id
    }

    async fn handle_subscribe_price_sched(
        &mut self,
        account: &Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
    ) -> SubscriptionID {
        let subscription_id = self.next_subscription_id();
        self.notify_price_sched_subscriptions
            .entry(Identifier::new(account.to_bytes()))
            .or_default()
            .push(NotifyPriceSchedSubscription {
                subscription_id,
                notify_price_sched_tx,
            });
        subscription_id
    }

    async fn send_subscribe_price_sched(&self) -> Result<()> {
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

    fn next_subscription_id(&mut self) -> SubscriptionID {
        self.subscription_id_count += 1;
        self.subscription_id_count
    }

    async fn handle_update_price(
        &self,
        account: &Pubkey,
        price: Price,
        conf: Conf,
        status: String,
    ) -> Result<()> {
        self.local_tx
            .send(local::Message::Update {
                price_identifier: pyth_sdk::Identifier::new(account.to_bytes()),
                price_info: local::PriceInfo {
                    status: Adapter::map_status(&status)?,
                    price,
                    conf,
                    timestamp: Utc::now().timestamp(),
                },
            })
            .await
            .map_err(|_| anyhow!("failed to send update to local store"))
    }

    // TODO: enum from string
    fn map_status(status: &str) -> Result<PriceStatus> {
        match status {
            "unknown" => Ok(PriceStatus::Unknown),
            "trading" => Ok(PriceStatus::Trading),
            "halted" => Ok(PriceStatus::Halted),
            "auction" => Ok(PriceStatus::Auction),
            _ => Err(anyhow!("invalid price status: {:#?}", status)),
        }
    }

    async fn lookup_solana_data(&self, tx: &Sender<global::Message>) -> Result<SolanaData> {
        let (result_tx, result_rx) = oneshot::channel();
        tx.send(global::Message::LookupSolanaData { result_tx })
            .await?;
        result_rx.await?
    }

    fn price_status_to_str(price_status: PriceStatus) -> String {
        match price_status {
            PriceStatus::Unknown => "unknown",
            PriceStatus::Trading => "trading",
            PriceStatus::Halted => "halted",
            PriceStatus::Auction => "auction",
        }
        .to_string()
    }
}
