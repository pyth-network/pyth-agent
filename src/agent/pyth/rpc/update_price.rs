use {
    super::{
        Method,
        UpdatePriceParams,
    },
    crate::agent::state,
    anyhow::{
        Result,
        anyhow,
    },
    jrpc::{
        Request,
        Value,
    },
    tracing::instrument,
};

#[instrument(skip_all, fields(account))]
pub async fn update_price<S>(
    state: &S,
    request: &Request<Method, Value>,
) -> Result<serde_json::Value>
where
    S: state::Prices,
{
    let params: UpdatePriceParams = serde_json::from_value(
        request
            .params
            .clone()
            .ok_or_else(|| anyhow!("Missing request parameters"))?,
    )?;

    tracing::Span::current().record("account", params.account.to_string());

    state
        .update_local_price(
            &params.account.parse::<solana_sdk::pubkey::Pubkey>()?,
            params.price,
            params.conf,
            params.status,
        )
        .await?;

    Ok(serde_json::to_value(0)?)
}
