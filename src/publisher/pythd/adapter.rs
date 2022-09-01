use super::api::{
    Conf, NotifyPrice, NotifyPriceSched, Price, ProductAccount, ProductAccountMetadata, PubKey,
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
        account: PubKey,
        result_tx: oneshot::Sender<Result<ProductAccount>>,
    },
    GetAllProducts {
        result_tx: oneshot::Sender<Result<Vec<ProductAccount>>>,
    },
    SubscribePrice {
        account: PubKey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
        result_tx: oneshot::Sender<Result<SubscriptionID>>,
    },
    SubscribePriceSched {
        account: PubKey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
        result_tx: oneshot::Sender<Result<SubscriptionID>>,
    },
    UpdatePrice {
        account: PubKey,
        price: Price,
        conf: Conf,
        status: String,
    },
}
