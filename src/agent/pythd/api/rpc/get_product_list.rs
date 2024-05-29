use {
    crate::agent::state,
    anyhow::Result,
};

pub async fn get_product_list<S>(adapter: &S) -> Result<serde_json::Value>
where
    S: state::StateApi,
{
    let product_list = adapter.get_product_list().await?;
    Ok(serde_json::to_value(product_list)?)
}
