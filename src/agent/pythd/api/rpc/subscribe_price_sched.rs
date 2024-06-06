use {
    super::{
        Method,
        NotifyPriceSched,
        SubscribePriceSchedParams,
        SubscribeResult,
    },
    crate::agent::state,
    anyhow::{
        anyhow,
        Result,
    },
    jrpc::{
        Request,
        Value,
    },
    tokio::sync::mpsc,
};

pub async fn subscribe_price_sched<S>(
    state: &S,
    notify_price_sched_tx: &mpsc::Sender<NotifyPriceSched>,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let params: SubscribePriceSchedParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    let account = params.account.parse::<solana_sdk::pubkey::Pubkey>()?;
    let subscription = state
        .subscribe_price_sched(&account, notify_price_sched_tx.clone())
        .await;

    Ok(serde_json::to_value(SubscribeResult { subscription })?)
}
