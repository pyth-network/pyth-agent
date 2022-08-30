// This module is the "Local Store": storing a copy of all the price information
// this local publisher is contributing to the network. The exporters will use reference
// this global store

use std::collections::HashMap;

use super::PriceIdentifier;
use anyhow::{anyhow, Result};
use pyth_sdk::{PriceStatus, UnixTimestamp};
use slog::Logger;
use tokio::sync::{mpsc, oneshot};

pub type Price = i64;
pub type Conf = u64;

#[derive(Clone)]
pub struct PriceInfo {
    pub status: PriceStatus,
    pub price: Price,
    pub conf: Conf,
    pub timestamp: UnixTimestamp,
}

pub struct Store {
    prices: HashMap<PriceIdentifier, PriceInfo>,
    rx: mpsc::Receiver<Message>,
    logger: Logger,
}

pub enum Message {
    Update {
        price_identifier: PriceIdentifier,
        price_info: PriceInfo,
    },
    LookupAllPriceInfo {
        result_tx: oneshot::Sender<HashMap<PriceIdentifier, PriceInfo>>,
    },
}

impl Store {
    pub fn new(rx: mpsc::Receiver<Message>, logger: Logger) -> Self {
        Store {
            prices: HashMap::new(),
            rx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.rx.recv().await {
            if let Err(err) = self.handle(message) {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
            }
        }
    }

    fn handle(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Update {
                price_identifier,
                price_info,
            } => {
                self.update(price_identifier, price_info);
                Ok(())
            }
            Message::LookupAllPriceInfo { result_tx } => result_tx
                .send(self.get_all_price_infos())
                .map_err(|_| anyhow!("failed to send LookupAllPriceInfo result")),
        }
    }

    pub fn update(&mut self, price_identifier: PriceIdentifier, price_info: PriceInfo) {
        // Drop the update if it is older than the current one stored for the price
        if let Some(current_price_info) = self.prices.get(&price_identifier) {
            if current_price_info.timestamp > price_info.timestamp {
                return;
            }
        }

        self.prices.insert(price_identifier, price_info);
    }

    pub fn get_all_price_infos(&self) -> HashMap<PriceIdentifier, PriceInfo> {
        self.prices.clone()
    }
}
