use {
    super::{
        GetProductParams,
        Method,
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

pub async fn get_product(
    adapter_tx: &mpsc::Sender<adapter::Message>,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value> {
    let params: GetProductParams = {
        let value = request.params.clone();
        serde_json::from_value(value.ok_or_else(|| anyhow!("Missing request parameters"))?)
    }?;

    let (result_tx, result_rx) = oneshot::channel();
    adapter_tx
        .send(adapter::Message::GetProduct {
            account: params.account,
            result_tx,
        })
        .await?;
    Ok(serde_json::to_value(result_rx.await??)?)
}
