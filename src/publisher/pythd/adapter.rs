use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::prelude::*;
use pyth_sdk::PriceStatus;
use tokio::sync::{mpsc, oneshot};

use super::super::store::{global, local, PriceIdentifier};
use super::api::{Conf, Price, ProductAccount, ProductAccountMetadata, PubKey, SubscriptionID};

pub type PriceUpdate = super::api::PriceUpdate;

// Adapter is the adapter between the pythd websocket API, and the stores.
// It is responsible for implementing the business logic for responding to
// the pythd websocket API calls.
//
// TODO: better name than Adapter (processor?)
pub struct Adapter {
    subscriptions: HashMap<SubscriptionID, Subscription>,

    // Channel on which to communicate to the local store
    local_tx: mpsc::Sender<local::Message>,

    // Channel on which to communicate to the global store
    global_tx: mpsc::Sender<global::Message>,
}

pub enum Message {
    PriceUpdate(PriceUpdate),
}

struct Subscription {
    price_idenfifier: PriceIdentifier,
    last_sent_timestamp: pyth_sdk::UnixTimestamp,
}

#[async_trait]
impl super::api::Protocol for Adapter {
    async fn get_product_list(&self) -> Result<Vec<ProductAccountMetadata>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.global_tx
            .send(global::Message::LookupAllProductAccounts { result_tx })
            .await?;
        let message = result_rx.await??;

        // TODO: map result to ProductAccountMetadata vector

        todo!()
    }

    async fn get_product(&self, account: PubKey) -> Result<ProductAccount> {
        todo!();
    }

    async fn get_all_products(&self) -> Result<Vec<ProductAccount>> {
        todo!();
    }

    async fn subscribe_price(&self, account: PubKey) -> Result<SubscriptionID> {
        todo!();
    }

    async fn subscribe_price_sched(&self, account: PubKey) -> Result<SubscriptionID> {
        todo!();
    }

    async fn update_price(
        &self,
        account: PubKey,
        price: Price,
        conf: Conf,
        status: &str,
    ) -> Result<()> {
        // self.local_store.update(
        //     pyth_sdk::Identifier::new(solana_sdk::pubkey::Pubkey::from_str(&account)?.to_bytes()),
        //     local::PriceInfo {
        //         status: Adapter::map_status(status)?,
        //         price,
        //         conf,
        //         timestamp: Utc::now().timestamp(),
        //     },
        // );

        Ok(())
    }
}

impl Adapter {
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

    // Sends subscribe_price_sched messages when appropiate
    fn poll_price_sched() -> Result<()> {
        // - Send these all on a loop
        todo!();
    }
}

#[async_trait]
pub trait Observer {
    async fn subscription_updated(&self, subscription_id: SubscriptionID, update: PriceUpdate);
}
