use {
    anyhow::bail,
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::RpcSendTransactionConfig,
        rpc_response::RpcPrioritizationFee,
    },
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentConfig,
        pubkey::Pubkey,
        signature::{
            Keypair,
            Signature,
            Signer,
        },
        transaction::Transaction,
    },
    solana_transaction_status::TransactionStatus,
    std::time::Duration,
    url::Url,
};

pub struct RpcMultiClient {
    rpc_clients: Vec<RpcClient>,
}

impl RpcMultiClient {
    pub fn new_with_timeout(rpc_urls: Vec<Url>, timeout: Duration) -> Self {
        let clients = rpc_urls
            .iter()
            .map(|rpc_url| RpcClient::new_with_timeout(rpc_url.to_string(), timeout))
            .collect();
        Self {
            rpc_clients: clients,
        }
    }

    pub fn new_with_commitment(rpc_urls: Vec<Url>, commitment_config: CommitmentConfig) -> Self {
        let clients = rpc_urls
            .iter()
            .map(|rpc_url| RpcClient::new_with_commitment(rpc_url.to_string(), commitment_config))
            .collect();
        Self {
            rpc_clients: clients,
        }
    }

    pub fn new_with_timeout_and_commitment(
        rpc_urls: Vec<Url>,
        timeout: Duration,
        commitment_config: CommitmentConfig,
    ) -> Self {
        let clients = rpc_urls
            .iter()
            .map(|rpc_url| {
                RpcClient::new_with_timeout_and_commitment(
                    rpc_url.to_string(),
                    timeout,
                    commitment_config,
                )
            })
            .collect();
        Self {
            rpc_clients: clients,
        }
    }

    pub async fn get_balance(&self, kp: &Keypair) -> anyhow::Result<u64> {
        for client in self.rpc_clients.iter() {
            match client.get_balance(&kp.pubkey()).await {
                Ok(balance) => return Ok(balance),
                Err(e) => {
                    tracing::warn!("getBalance error for rpc endpoint {}: {}", client.url(), e)
                }
            }
        }
        bail!("getBalance failed for all RPC endpoints")
    }

    pub async fn send_transaction_with_config(
        &self,
        transaction: &Transaction,
    ) -> anyhow::Result<Signature> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client
                .send_transaction_with_config(
                    transaction,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        ..RpcSendTransactionConfig::default()
                    },
                )
                .await
            {
                Ok(signature) => return Ok(signature),
                Err(e) => tracing::warn!(
                    "sendTransactionWithConfig failed for rpc endpoint {}: {:?}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("sendTransactionWithConfig failed for all rpc endpoints")
    }

    pub async fn get_signature_statuses(
        &self,
        signatures_contiguous: &mut [Signature],
    ) -> anyhow::Result<Vec<Option<TransactionStatus>>> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client
                .get_signature_statuses(signatures_contiguous)
                .await
            {
                Ok(statuses) => return Ok(statuses.value),
                Err(e) => tracing::warn!(
                    "getSignatureStatus failed for rpc endpoint {}: {:?}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("getSignatureStatuses failed for all rpc endpoints")
    }

    pub async fn get_recent_prioritization_fees(
        &self,
        price_accounts: &[Pubkey],
    ) -> anyhow::Result<Vec<RpcPrioritizationFee>> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client
                .get_recent_prioritization_fees(price_accounts)
                .await
            {
                Ok(fees) => return Ok(fees),
                Err(e) => tracing::warn!(
                    "getRecentPrioritizationFee failed for rpc endpoint {}: {:?}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("getRecentPrioritizationFees failed for every rpc endpoint")
    }

    pub async fn get_program_accounts(
        &self,
        oracle_program_key: Pubkey,
    ) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client.get_program_accounts(&oracle_program_key).await {
                Ok(accounts) => return Ok(accounts),
                Err(e) => tracing::warn!(
                    "getProgramAccounts failed for rpc endpoint {}: {}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("getProgramAccounts failed for all rpc endpoints")
    }

    pub async fn get_account_data(&self, publisher_config_key: &Pubkey) -> anyhow::Result<Vec<u8>> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client.get_account_data(publisher_config_key).await {
                Ok(data) => return Ok(data),
                Err(e) => tracing::warn!(
                    "getAccountData failed for rpc endpoint {}: {:?}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("getAccountData failed for all rpc endpoints")
    }

    pub async fn get_slot_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<u64> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client.get_slot_with_commitment(commitment_config).await {
                Ok(slot) => return Ok(slot),
                Err(e) => tracing::warn!(
                    "getSlotWithCommitment failed for rpc endpoint {}: {:?}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("getSlotWithCommitment failed for all rpc endpoints")
    }

    pub async fn get_latest_blockhash(&self) -> anyhow::Result<solana_sdk::hash::Hash> {
        for rpc_client in self.rpc_clients.iter() {
            match rpc_client.get_latest_blockhash().await {
                Ok(hash) => return Ok(hash),
                Err(e) => tracing::warn!(
                    "getLatestBlockhash failed for rpc endpoint {}: {:?}",
                    rpc_client.url(),
                    e
                ),
            }
        }
        bail!("getLatestBlockhash failed for all rpc endpoints")
    }
}
