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
        future::Future,
        pin::Pin,
        sync::Arc,
        time::{
            Duration,
            Instant,
        },
    },
    tokio::sync::Mutex,
    url::Url,
};


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
    async fn retry_with_round_robin<'a, T, F>(
        &'a self,
        operation_name: &str,
        operation: F,
    ) -> anyhow::Result<T>
    where
        F: Fn(usize) -> Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>,
    {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;

        while attempts < max_attempts {
            let index_option = self.get_next_endpoint().await;

            if let Some(index) = index_option {
                let future = operation(index);
                match future.await {
                    Ok(result) => {
                        self.handle_success(index).await;
                        return Ok(result);
                    }
                    Err(e) => {
                        self.handle_error(index, operation_name, &e.to_string())
                            .await;
                    }
                }
            }
            attempts += 1;
        }

        bail!(
            "{} failed for all RPC endpoints after {} attempts",
            operation_name,
            attempts
        )
    }

    async fn get_next_endpoint(&self) -> Option<usize> {
        let mut state = self.round_robin_state.lock().await;
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
    }

    async fn handle_success(&self, index: usize) {
        let mut state = self.round_robin_state.lock().await;
        if index < state.endpoint_states.len() {
            state.endpoint_states[index].is_healthy = true;
            state.endpoint_states[index].last_failure = None;
        }
    }

    async fn handle_error(&self, index: usize, operation_name: &str, error: &str) {
        let client = &self.rpc_clients[index];
        tracing::warn!(
            "{} error for rpc endpoint {}: {}",
            operation_name,
            client.url(),
            error
        );
        let mut state = self.round_robin_state.lock().await;
        if index < state.endpoint_states.len() {
            state.endpoint_states[index].last_failure = Some(Instant::now());
            state.endpoint_states[index].is_healthy = false;
        }
    }
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
        let pubkey = kp.pubkey();
        self.retry_with_round_robin("getBalance", |index| {
            let client = &self.rpc_clients[index];
            Box::pin(async move {
                client
                    .get_balance(&pubkey)
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn send_transaction_with_config(
        &self,
        transaction: &Transaction,
    ) -> anyhow::Result<Signature> {
        self.retry_with_round_robin("sendTransactionWithConfig", |index| {
            let client = &self.rpc_clients[index];
            let transaction = transaction.clone();
            Box::pin(async move {
                client
                    .send_transaction_with_config(
                        &transaction,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            ..RpcSendTransactionConfig::default()
                        },
                    )
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn get_signature_statuses(
        &self,
        signatures_contiguous: &mut [Signature],
    ) -> anyhow::Result<Vec<Option<TransactionStatus>>> {
        self.retry_with_round_robin("getSignatureStatuses", |index| {
            let client = &self.rpc_clients[index];
            let signatures = signatures_contiguous.to_vec();
            Box::pin(async move {
                client
                    .get_signature_statuses(&signatures)
                    .await
                    .map(|statuses| statuses.value)
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn get_recent_prioritization_fees(
        &self,
        price_accounts: &[Pubkey],
    ) -> anyhow::Result<Vec<RpcPrioritizationFee>> {
        self.retry_with_round_robin("getRecentPrioritizationFees", |index| {
            let client = &self.rpc_clients[index];
            let price_accounts = price_accounts.to_vec();
            Box::pin(async move {
                client
                    .get_recent_prioritization_fees(&price_accounts)
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn get_program_accounts(
        &self,
        oracle_program_key: Pubkey,
    ) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        self.retry_with_round_robin("getProgramAccounts", |index| {
            let client = &self.rpc_clients[index];
            Box::pin(async move {
                client
                    .get_program_accounts(&oracle_program_key)
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn get_account_data(&self, publisher_config_key: &Pubkey) -> anyhow::Result<Vec<u8>> {
        let publisher_config_key = *publisher_config_key;
        self.retry_with_round_robin("getAccountData", |index| {
            let client = &self.rpc_clients[index];
            Box::pin(async move {
                client
                    .get_account_data(&publisher_config_key)
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn get_slot_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<u64> {
        self.retry_with_round_robin("getSlotWithCommitment", |index| {
            let client = &self.rpc_clients[index];
            Box::pin(async move {
                client
                    .get_slot_with_commitment(commitment_config)
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }

    pub async fn get_latest_blockhash(&self) -> anyhow::Result<solana_sdk::hash::Hash> {
        self.retry_with_round_robin("getLatestBlockhash", |index| {
            let client = &self.rpc_clients[index];
            Box::pin(async move {
                client
                    .get_latest_blockhash()
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .await
    }
}
