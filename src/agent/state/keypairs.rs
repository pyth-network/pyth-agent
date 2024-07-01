//! Keypair Management API
//!
//! The Keypair Manager allows hotloading keypairs via a HTTP request.

use {
    super::State,
    crate::agent::solana::network::Network,
    anyhow::Result,
    solana_sdk::signature::Keypair,
    tokio::sync::RwLock,
    tracing::instrument,
};

#[derive(Default)]
pub struct KeypairState {
    primary_current_keypair:   RwLock<Option<Keypair>>,
    secondary_current_keypair: RwLock<Option<Keypair>>,
}

#[async_trait::async_trait]
pub trait Keypairs {
    async fn request_keypair(&self, network: Network) -> Result<Keypair>;
    async fn update_keypair(&self, network: Network, new_keypair: Keypair);
}

// Allow downcasting State into Keypairs for functions that depend on the `Keypairs` service.
impl<'a> From<&'a State> for &'a KeypairState {
    fn from(state: &'a State) -> &'a KeypairState {
        &state.keypairs
    }
}

#[async_trait::async_trait]
impl<T> Keypairs for T
where
    for<'a> &'a T: Into<&'a KeypairState>,
    T: Sync,
{
    #[instrument(skip(self))]
    async fn request_keypair(&self, network: Network) -> Result<Keypair> {
        let keypair = match network {
            Network::Primary => &self.into().primary_current_keypair,
            Network::Secondary => &self.into().secondary_current_keypair,
        }
        .read()
        .await;

        Ok(Keypair::from_bytes(
            &keypair
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Keypair not available"))?
                .to_bytes(),
        )?)
    }

    #[instrument(skip(self))]
    async fn update_keypair(&self, network: Network, new_keypair: Keypair) {
        *match network {
            Network::Primary => self.into().primary_current_keypair.write().await,
            Network::Secondary => self.into().secondary_current_keypair.write().await,
        } = Some(new_keypair);
    }
}
