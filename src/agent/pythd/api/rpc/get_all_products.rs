use {
    crate::agent::pythd::adapter,
    anyhow::Result,
};

pub async fn get_all_products<S>(adapter: &S) -> Result<serde_json::Value>
where
    S: adapter::AdapterApi,
{
    let products = adapter.get_all_products().await?;
    Ok(serde_json::to_value(products)?)
}
