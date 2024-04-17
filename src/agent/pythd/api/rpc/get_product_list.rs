use {
    crate::agent::pythd::adapter,
    anyhow::Result,
    tokio::sync::{
        mpsc,
        oneshot,
    },
};

pub async fn get_product_list(
    adapter_tx: &mpsc::Sender<adapter::Message>,
) -> Result<serde_json::Value> {
    let (result_tx, result_rx) = oneshot::channel();
    adapter_tx
        .send(adapter::Message::GetProductList { result_tx })
        .await?;
    Ok(serde_json::to_value(result_rx.await??)?)
}
