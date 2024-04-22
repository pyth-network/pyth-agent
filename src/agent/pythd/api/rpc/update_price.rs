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
};

pub async fn update_price<S>(
    adapter: &S,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value>
where
    S: adapter::AdapterApi,
{
    let params: UpdatePriceParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    adapter
        .update_price(
            &params.account.parse::<solana_sdk::pubkey::Pubkey>()?,
            params.price,
            params.conf,
            params.status,
        )
        .await?;

    Ok(serde_json::to_value(0)?)
}
