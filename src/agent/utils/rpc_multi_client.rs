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
    std::{
        sync::Arc,
        time::{
            Duration,
            Instant,
        },
    },
    tokio::sync::Mutex,
    url::Url,
};

macro_rules! retry_rpc_operation {
    ($self:expr, $operation_name:expr, $client:ident => $operation:expr) => {{
        let mut attempts = 0;
        let max_attempts = $self.rpc_clients.len() * 2;

        while attempts < max_attempts {
            let index_option = {
                let mut state = $self.round_robin_state.lock().await;
                let now = Instant::now();
                let start_index = state.current_index;

                let mut found_index = None;
                for _ in 0..state.endpoint_states.len() {
                    let index = state.current_index;
                    state.current_index = (state.current_index + 1) % state.endpoint_states.len();

                    let endpoint_state = &state.endpoint_states[index];
                    if endpoint_state.is_healthy
                        || endpoint_state.last_failure.map_or(true, |failure_time| {
                            now.duration_since(failure_time) >= state.cooldown_duration
                        })
                    {
                        found_index = Some(index);
                        break;
                    }
                }

                if found_index.is_none() {
                    let index = start_index;
                    state.current_index = (start_index + 1) % state.endpoint_states.len();
                    found_index = Some(index);
                }
                found_index
            };

            if let Some(index) = index_option {
                let $client = &$self.rpc_clients[index];
                match $operation {
                    Ok(result) => {
                        let mut state = $self.round_robin_state.lock().await;
                        if index < state.endpoint_states.len() {
                            state.endpoint_states[index].is_healthy = true;
                            state.endpoint_states[index].last_failure = None;
                        }
                        return Ok(result);
                    }
                    Err(e) => {
                        let client = &$self.rpc_clients[index];
                        tracing::warn!(
                            "{} error for rpc endpoint {}: {}",
                            $operation_name,
                            client.url(),
                            e
                        );
                        let mut state = $self.round_robin_state.lock().await;
                        if index < state.endpoint_states.len() {
                            state.endpoint_states[index].last_failure = Some(Instant::now());
                            state.endpoint_states[index].is_healthy = false;
                        }
                    }
                }
            }
            attempts += 1;
        }

        bail!(
            "{} failed for all RPC endpoints after {} attempts",
            $operation_name,
            attempts
        )
    }};
}


#[derive(Debug, Clone)]
struct EndpointState {
    last_failure: Option<Instant>,
    is_healthy:   bool,
}

#[derive(Debug)]
struct RoundRobinState {
    current_index:     usize,
    endpoint_states:   Vec<EndpointState>,
    cooldown_duration: Duration,
}

impl RoundRobinState {
    fn new(endpoint_count: usize, cooldown_duration: Duration) -> Self {
        Self {
            current_index: 0,
            endpoint_states: vec![
                EndpointState {
                    last_failure: None,
                    is_healthy:   true,
                };
                endpoint_count
            ],
            cooldown_duration,
        }
    }
}

pub struct RpcMultiClient {
    rpc_clients:       Vec<RpcClient>,
    round_robin_state: Arc<Mutex<RoundRobinState>>,
}

impl RpcMultiClient {
    pub fn new_with_timeout(rpc_urls: Vec<Url>, timeout: Duration) -> Self {
        Self::new_with_timeout_and_cooldown(rpc_urls, timeout, Duration::from_secs(30))
    }

    pub fn new_with_timeout_and_cooldown(
        rpc_urls: Vec<Url>,
        timeout: Duration,
        cooldown_duration: Duration,
    ) -> Self {
        let clients: Vec<RpcClient> = rpc_urls
            .iter()
            .map(|rpc_url| RpcClient::new_with_timeout(rpc_url.to_string(), timeout))
            .collect();
        let round_robin_state = Arc::new(Mutex::new(RoundRobinState::new(
            clients.len(),
            cooldown_duration,
        )));
        Self {
            rpc_clients: clients,
            round_robin_state,
        }
    }

    pub fn new_with_commitment(rpc_urls: Vec<Url>, commitment_config: CommitmentConfig) -> Self {
        Self::new_with_commitment_and_cooldown(rpc_urls, commitment_config, Duration::from_secs(30))
    }

    pub fn new_with_commitment_and_cooldown(
        rpc_urls: Vec<Url>,
        commitment_config: CommitmentConfig,
        cooldown_duration: Duration,
    ) -> Self {
        let clients: Vec<RpcClient> = rpc_urls
            .iter()
            .map(|rpc_url| RpcClient::new_with_commitment(rpc_url.to_string(), commitment_config))
            .collect();
        let round_robin_state = Arc::new(Mutex::new(RoundRobinState::new(
            clients.len(),
            cooldown_duration,
        )));
        Self {
            rpc_clients: clients,
            round_robin_state,
        }
    }

    pub fn new_with_timeout_and_commitment(
        rpc_urls: Vec<Url>,
        timeout: Duration,
        commitment_config: CommitmentConfig,
    ) -> Self {
        Self::new_with_timeout_commitment_and_cooldown(
            rpc_urls,
            timeout,
            commitment_config,
            Duration::from_secs(30),
        )
    }

    pub fn new_with_timeout_commitment_and_cooldown(
        rpc_urls: Vec<Url>,
        timeout: Duration,
        commitment_config: CommitmentConfig,
        cooldown_duration: Duration,
    ) -> Self {
        let clients: Vec<RpcClient> = rpc_urls
            .iter()
            .map(|rpc_url| {
                RpcClient::new_with_timeout_and_commitment(
                    rpc_url.to_string(),
                    timeout,
                    commitment_config,
                )
            })
            .collect();
        let round_robin_state = Arc::new(Mutex::new(RoundRobinState::new(
            clients.len(),
            cooldown_duration,
        )));
        Self {
            rpc_clients: clients,
            round_robin_state,
        }
    }


    pub async fn get_balance(&self, kp: &Keypair) -> anyhow::Result<u64> {
        retry_rpc_operation!(self, "getBalance", client => client.get_balance(&kp.pubkey()).await)
    }

    pub async fn send_transaction_with_config(
        &self,
        transaction: &Transaction,
    ) -> anyhow::Result<Signature> {
        retry_rpc_operation!(
            self,
            "sendTransactionWithConfig",
            client => client
                .send_transaction_with_config(
                    transaction,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        ..RpcSendTransactionConfig::default()
                    },
                )
                .await
        )
    }

    pub async fn get_signature_statuses(
        &self,
        signatures_contiguous: &mut [Signature],
    ) -> anyhow::Result<Vec<Option<TransactionStatus>>> {
        retry_rpc_operation!(
            self,
            "getSignatureStatuses",
            client => client.get_signature_statuses(signatures_contiguous).await.map(|statuses| statuses.value)
        )
    }

    pub async fn get_recent_prioritization_fees(
        &self,
        price_accounts: &[Pubkey],
    ) -> anyhow::Result<Vec<RpcPrioritizationFee>> {
        retry_rpc_operation!(
            self,
            "getRecentPrioritizationFees",
            client => client.get_recent_prioritization_fees(price_accounts).await
        )
    }

    pub async fn get_program_accounts(
        &self,
        oracle_program_key: Pubkey,
    ) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        retry_rpc_operation!(
            self,
            "getProgramAccounts",
            client => client.get_program_accounts(&oracle_program_key).await
        )
    }

    pub async fn get_account_data(&self, publisher_config_key: &Pubkey) -> anyhow::Result<Vec<u8>> {
        retry_rpc_operation!(
            self,
            "getAccountData",
            client => client.get_account_data(publisher_config_key).await
        )
    }

    pub async fn get_slot_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<u64> {
        retry_rpc_operation!(
            self,
            "getSlotWithCommitment",
            client => client.get_slot_with_commitment(commitment_config).await
        )
    }

    pub async fn get_latest_blockhash(&self) -> anyhow::Result<solana_sdk::hash::Hash> {
        retry_rpc_operation!(
            self,
            "getLatestBlockhash",
            client => client.get_latest_blockhash().await
        )
    }
}
