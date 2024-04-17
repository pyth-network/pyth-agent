use {
    super::{
        Method,
        NotifyPrice,
        SubscribePriceParams,
        SubscribeResult,
    },
    crate::agent::pythd::adapter,
    anyhow::{
        anyhow,
        Result,
    },
    jrpc::{
        Request,
        Value,
    },
    tokio::sync::{
        mpsc,
        oneshot,
    },
};

pub async fn subscribe_price(
    adapter_tx: &mpsc::Sender<adapter::Message>,
    notify_price_tx: &mpsc::Sender<NotifyPrice>,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value> {
    let params: SubscribePriceParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    let (result_tx, result_rx) = oneshot::channel();
    adapter_tx
        .send(adapter::Message::SubscribePrice {
            result_tx,
            account: params.account,
            notify_price_tx: notify_price_tx.clone(),
        })
        .await?;

    Ok(serde_json::to_value(SubscribeResult {
        subscription: result_rx.await??,
    })?)
}
