use super::api::{
    Conf, NotifyPrice, NotifyPriceSched, Price, ProductAccount, ProductAccountMetadata, Pubkey,
    SubscriptionID,
};
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

// Adapter is the adapter between the pythd websocket API, and the stores.
// It is responsible for implementing the business logic for responding to
// the pythd websocket API calls.
#[derive(Debug)]
pub enum Message {
    GetProductList {
        result_tx: oneshot::Sender<Result<Vec<ProductAccountMetadata>>>,
    },
    GetProduct {
        account: Pubkey,
        result_tx: oneshot::Sender<Result<ProductAccount>>,
    },
    GetAllProducts {
        result_tx: oneshot::Sender<Result<Vec<ProductAccount>>>,
    },
    SubscribePrice {
        account: Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
        result_tx: oneshot::Sender<Result<SubscriptionID>>,
    },
    SubscribePriceSched {
        account: Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
        result_tx: oneshot::Sender<Result<SubscriptionID>>,
    },
    UpdatePrice {
        account: Pubkey,
        price: Price,
        conf: Conf,
        status: String,
    },
}
