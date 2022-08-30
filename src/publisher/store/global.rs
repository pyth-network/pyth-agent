// This module is the "Global Store": storing a copy of all the information
// held in the global Pyth Network. This enables this data to be easily
// queried by other components.

use std::collections::HashMap;

use crate::publisher::{
    pythd::adapter,
    solana::oracle::{self, PriceAccount, ProductAccount},
};

use super::PriceIdentifier;
use anyhow::{anyhow, Result};
use pyth_sdk::{PriceFeed, PriceStatus};
use slog::Logger;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

pub type SolanaData = oracle::Data;

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
    pythd_adapter_tx: Option<mpsc::Sender<adapter::Message>>,

    logger: Logger,
}

// Data contains all the current data stored on the global Pyth Network.
#[derive(Default)]
struct Data {
    // Solana-agnostic data.
    prices: HashMap<PriceIdentifier, PriceInfo>,

    // Solana-specific data.
    solana_data: SolanaData,
}

impl Data {
    fn new() -> Self {
        Data {
            prices: HashMap::new(),
            solana_data: SolanaData::new(),
        }
    }
}

#[derive(Debug)]
pub enum Message {
    ProductAccountUpdate {
        account_key: Pubkey,
        account: ProductAccount,
    },
    PriceAccountUpdate {
        account_key: Pubkey,
        account: PriceAccount,
    },
    LookupPriceInfo {
        identifier: PriceIdentifier,
        result_tx: oneshot::Sender<Result<PriceInfo>>,
    },
    LookupSolanaData {
        result_tx: oneshot::Sender<Result<SolanaData>>,
    },
}

pub struct Update {
    pub price_info: PriceInfo,
    pub price_account: PriceAccount,
}

impl Store {
    pub fn new(
        message_rx: Receiver<Message>,
        pythd_adapter_tx: Option<Sender<adapter::Message>>,
        logger: Logger,
    ) -> Self {
        Store {
            data: Data::new(),
            message_rx,
            pythd_adapter_tx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.message_rx.recv().await {
            if let Err(err) = self.handle_message(message).await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
            }
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::ProductAccountUpdate {
                account_key,
                account,
            } => {
                self.handle_product_account_update(account_key, account);
                Ok(())
            }
            Message::PriceAccountUpdate {
                account_key,
                account,
            } => self.handle_price_account_update(account_key, account).await,
            Message::LookupPriceInfo {
                identifier,
                result_tx: result,
            } => self.send(result, self.lookup_price_info(identifier)),
            Message::LookupSolanaData { result_tx: result } => {
                self.send(result, Ok(self.lookup_solana_data()))
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
            .ok_or_else(|| anyhow!("price info not found in store"))
            .cloned()
    }

    fn lookup_solana_data(&self) -> SolanaData {
        self.data.solana_data.clone()
    }

    fn handle_product_account_update(&mut self, account_key: Pubkey, account: ProductAccount) {
        self.data
            .solana_data
            .product_accounts
            .insert(account_key, account);
    }

    async fn handle_price_account_update(
        &mut self,
        account_key: Pubkey,
        account: PriceAccount,
    ) -> Result<()> {
        // If the update isn't fresh, then skip this update
        let price_feed = account.to_price_feed(&account_key);
        if !self.is_fresh_update(price_feed) {
            return Ok(());
        }

        // Update the global store
        let identifier = PriceIdentifier::new(account_key.to_bytes());
        self.data
            .solana_data
            .price_accounts
            .insert(account_key, account);
        let price_info = PriceInfo::new(price_feed);
        self.data.prices.insert(identifier, price_info.clone());

        // Notify the pythd API adapter that this price account has been updated,
        // if we are the primary store.
        if let Some(pythd_adapter_tx) = &self.pythd_adapter_tx {
            pythd_adapter_tx
                .send(adapter::Message::GlobalStoreUpdate {
                    update: adapter::PriceUpdate {
                        price: price_feed.get_current_price_unchecked().price,
                        conf: price_feed.get_current_price_unchecked().conf,
                        status: Self::map_status(price_feed.status),
                        valid_slot: account.valid_slot,
                        pub_slot: account.agg.pub_slot,
                    },
                    price_identifier: identifier,
                })
                .await?;
        };

        Ok(())
    }

    // TODO: map these more elegantly
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
