use std::{collections::HashMap, time::Duration};

use super::api::{
    self, Conf, NotifyPrice, NotifyPriceSched, Price, ProductAccount, ProductAccountMetadata,
    SubscriptionID,
};
use anyhow::Result;
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

    /// The fixed interval at which Notify Price Sched notifications are sent
    notify_price_sched_interval: Interval,

    /// Channel on which the shutdown is broadcast
    shutdown_rx: broadcast::Receiver<()>,

    /// The logger
    logger: Logger,
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
        shutdown_rx: broadcast::Receiver<()>,
        logger: Logger,
    ) -> Self {
        Adapter {
            message_rx,
            notify_price_sched_interval: time::interval(notify_price_sched_interval),
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
                    todo!()
                }
            };
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::GetProductList { result_tx } => todo!(),
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
            } => todo!(),
            Message::UpdatePrice {
                account,
                price,
                conf,
                status,
            } => todo!(),
        }
    }
}
