use {
    super::{
        Method,
        NotifyPrice,
        SubscribePriceParams,
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
    tracing::instrument,
};

#[instrument(skip_all, fields(account))]
pub async fn subscribe_price<S>(
    state: &S,
    notify_price_tx: &mpsc::Sender<NotifyPrice>,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let params: SubscribePriceParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    let account = params.account.parse::<solana_sdk::pubkey::Pubkey>()?;
    tracing::Span::current().record("account", account.to_string());

    let subscription = state
        .subscribe_price(&account, notify_price_tx.clone())
        .await;

    Ok(serde_json::to_value(SubscribeResult { subscription })?)
}
