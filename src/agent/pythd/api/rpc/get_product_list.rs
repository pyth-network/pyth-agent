use {
    crate::agent::state,
    anyhow::Result,
};

pub async fn get_product_list<S>(state: &S) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let product_list = state.get_product_list().await?;
    Ok(serde_json::to_value(product_list)?)
}
