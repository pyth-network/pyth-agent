use {
    super::{
        Method,
        NotifyPriceSched,
        SubscribePriceSchedParams,
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

pub async fn subscribe_price_sched(
    adapter_tx: &mpsc::Sender<adapter::Message>,
    notify_price_sched_tx: &mpsc::Sender<NotifyPriceSched>,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value> {
    let params: SubscribePriceSchedParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    let (result_tx, result_rx) = oneshot::channel();
    adapter_tx
        .send(adapter::Message::SubscribePriceSched {
            result_tx,
            account: params.account,
            notify_price_sched_tx: notify_price_sched_tx.clone(),
        })
        .await?;

    Ok(serde_json::to_value(SubscribeResult {
        subscription: result_rx.await??,
    })?)
}
