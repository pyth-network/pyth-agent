use {
    crate::agent::state,
    anyhow::Result,
    tracing::instrument,
};

#[instrument(skip_all)]
pub async fn get_all_products<S>(state: &S) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let products = state.get_all_products().await?;
    Ok(serde_json::to_value(products)?)
}
