use {
    crate::agent::state,
    anyhow::Result,
    tracing::instrument,
};

#[instrument(skip_all)]
pub async fn get_product_list<S>(state: &S) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let product_list = state.get_product_list().await?;
    Ok(serde_json::to_value(product_list)?)
}
