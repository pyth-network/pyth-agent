use {
    super::{
        Method,
        UpdatePriceParams,
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
    tokio::sync::mpsc,
};

pub async fn update_price(
    adapter_tx: &mpsc::Sender<adapter::Message>,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value> {
    let params: UpdatePriceParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    adapter_tx
        .send(adapter::Message::UpdatePrice {
            account: params.account,
            price:   params.price,
            conf:    params.conf,
            status:  params.status,
        })
        .await?;

    Ok(serde_json::to_value(0)?)
}
