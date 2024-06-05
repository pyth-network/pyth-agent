use {
    super::{
        GetProductParams,
        Method,
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
};

pub async fn get_product<S>(
    adapter: &S,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let params: GetProductParams = {
        let value = request.params.clone();
        serde_json::from_value(value.ok_or_else(|| anyhow!("Missing request parameters"))?)
    }?;

    let account = params.account.parse::<solana_sdk::pubkey::Pubkey>()?;
    let product = adapter.get_product(&account).await?;
    Ok(serde_json::to_value(product)?)
}
