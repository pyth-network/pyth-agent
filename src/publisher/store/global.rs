// This module is the "Global Store": storing a copy of all the information
// held in the global Pyth Network. This enables this data to be easily
// queried by other components.

use std::collections::HashMap;

use crate::publisher::{
    pythd::{adapter, api::PriceUpdate},
    solana::oracle,
};

use super::PriceIdentifier;
use anyhow::{anyhow, Result};
use pyth_sdk::{PriceFeed, PriceStatus};
use pyth_sdk_solana::state::{AccKey, PriceAccount, ProductAccount};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct PriceInfo {
    price_feed: PriceFeed,
}

impl PriceInfo {
    pub fn new(price_feed: PriceFeed) -> Self {
        PriceInfo { price_feed }
    }
}

pub struct Store {
    data: Data,

    // Channel on which messages are received
    message_rx: mpsc::Receiver<Message>,

    // Channel on which to communicate with the pythd API adapter
    pythd_adapter_tx: mpsc::Sender<adapter::Message>,
}

// Data contains all the current data stored on the global Pyth Network.
#[derive(Default)]
struct Data {
    // Solana-agnostic data.
    prices: HashMap<PriceIdentifier, PriceInfo>,

    // Solana-specific data.
    solana_data: SolanaData,
}

#[derive(Default)]
struct SolanaData {
    product_accounts: HashMap<AccKey, ProductAccount>,
    price_accounts: HashMap<AccKey, PriceAccount>,
}

#[derive(Debug)]
pub enum Message {
    ProductAccountUpdate {
        acc_key: AccKey,
        account: ProductAccount,
    },
    PriceAccountUpdate {
        acc_key: AccKey,
        account: PriceAccount,
    },
    LookupPriceInfo {
        identifier: PriceIdentifier,
        result_tx: oneshot::Sender<Result<PriceInfo>>,
    },
    LookupPriceAccount {
        acc_key: AccKey,
        result_tx: oneshot::Sender<Result<PriceAccount>>,
    },
    LookupAllPriceAccounts {
        result_tx: oneshot::Sender<Result<HashMap<AccKey, PriceAccount>>>,
    },
    LookupProductAccount {
        acc_key: AccKey,
        result_tx: oneshot::Sender<Result<ProductAccount>>,
    },
    LookupAllProductAccounts {
        result_tx: oneshot::Sender<Result<HashMap<AccKey, ProductAccount>>>,
    },
}

pub struct Update {
    pub price_info: PriceInfo,
    pub price_account: PriceAccount,
}

impl Store {
    async fn run(&mut self) {
        while let Some(message) = self.message_rx.recv().await {
            // TODO: log error
            self.handle_message(message).await;
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::ProductAccountUpdate { acc_key, account } => {
                Ok(self.handle_product_account_update(acc_key, account))
            }
            Message::PriceAccountUpdate { acc_key, account } => {
                self.handle_price_account_update(acc_key, account).await
            }
            Message::LookupPriceInfo {
                identifier,
                result_tx: result,
            } => self.send(result, self.lookup_price_info(identifier)),
            Message::LookupPriceAccount {
                acc_key,
                result_tx: result,
            } => self.send(result, self.lookup_price_account(acc_key)),
            Message::LookupAllPriceAccounts { result_tx: result } => {
                self.send(result, Ok(self.lookup_all_price_accounts()))
            }
            Message::LookupProductAccount {
                acc_key,
                result_tx: result,
            } => self.send(result, self.lookup_product_account(acc_key)),
            Message::LookupAllProductAccounts { result_tx: result } => {
                self.send(result, Ok(self.lookup_all_product_accounts()))
            }
        }
    }

    fn send<T>(&self, tx: oneshot::Sender<T>, item: T) -> Result<()> {
        tx.send(item).map_err(|_| anyhow!("could not send result"))
    }

    fn lookup_price_info(&self, identifier: PriceIdentifier) -> Result<PriceInfo> {
        self.data
            .prices
            .get(&identifier)
            .ok_or(anyhow!("price info not found in store"))
            .cloned()
    }

    fn lookup_price_account(&self, acc_key: AccKey) -> Result<PriceAccount> {
        self.data
            .solana_data
            .price_accounts
            .get(&acc_key)
            .ok_or_else(|| anyhow!("price account not found in store"))
            .cloned()
    }

    fn lookup_all_price_accounts(&self) -> HashMap<AccKey, PriceAccount> {
        self.data.solana_data.price_accounts.clone()
    }

    fn lookup_product_account(&self, acc_key: AccKey) -> Result<ProductAccount> {
        self.data
            .solana_data
            .product_accounts
            .get(&acc_key)
            .ok_or_else(|| anyhow!("product account not found in store"))
            .cloned()
    }

    fn lookup_all_product_accounts(&self) -> HashMap<AccKey, ProductAccount> {
        self.data.solana_data.product_accounts.clone()
    }

    fn handle_product_account_update(&mut self, acc_key: AccKey, account: ProductAccount) {
        self.data
            .solana_data
            .product_accounts
            .insert(acc_key, account);
    }

    async fn handle_price_account_update(
        &mut self,
        acc_key: AccKey,
        account: PriceAccount,
    ) -> Result<()> {
        // If the update isn't fresh, then skip this update
        let price_feed = account.to_price_feed(&Pubkey::new(&acc_key.val));
        if !self.is_fresh_update(price_feed) {
            return Ok(());
        }

        // Update the store
        let identifier = PriceIdentifier::new(acc_key.val);
        self.data
            .solana_data
            .price_accounts
            .insert(acc_key, account);
        let price_info = PriceInfo::new(price_feed);
        self.data.prices.insert(identifier, price_info.clone());

        // Notify the pythd API adapter that we have been updated
        self.pythd_adapter_tx
            .send(adapter::Message::PriceUpdate(adapter::PriceUpdate {
                price: price_feed.get_current_price_unchecked().price,
                conf: price_feed.get_current_price_unchecked().conf,
                status: Self::map_status(price_feed.status),
                valid_slot: account.valid_slot,
                pub_slot: account.agg.pub_slot,
            }))
            .await
            .map_err(|_| anyhow!("channel closed"))
    }

    fn map_status(status: PriceStatus) -> String {
        match status {
            PriceStatus::Unknown => "unknown",
            PriceStatus::Trading => "trading",
            PriceStatus::Halted => "halted",
            PriceStatus::Auction => "auction",
        }
        .to_string()
    }

    fn is_fresh_update(&self, price_feed: PriceFeed) -> bool {
        self.data
            .prices
            .get(&price_feed.id)
            .map(|cur| cur.price_feed.publish_time < price_feed.publish_time)
            .unwrap_or_default()
    }
}
