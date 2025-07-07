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
        sync::{
            Arc,
            Mutex,
        },
        time::{
            Duration,
            Instant,
        },
    },
    url::Url,
};

macro_rules! retry_rpc_operation {
    ($self:expr, $operation_name:expr, $client:ident => $operation:expr) => {{
        let mut attempts = 0;
        let max_attempts = $self.rpc_clients.len() * 2;

        while attempts < max_attempts {
            if let Some(index) = $self.get_next_endpoint() {
                let $client = &$self.rpc_clients[index];
                match $operation {
                    Ok(result) => {
                        $self.handle_success(index);
                        return Ok(result);
                    }
                    Err(e) => {
                        $self.handle_error(index, $operation_name, &e);
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

    fn get_next_healthy_endpoint(&mut self) -> Option<usize> {
        let now = Instant::now();
        let start_index = self.current_index;

        for _ in 0..self.endpoint_states.len() {
            let index = self.current_index;
            self.current_index = (self.current_index + 1) % self.endpoint_states.len();

            let state = &self.endpoint_states[index];
            if state.is_healthy
                || state.last_failure.map_or(true, |failure_time| {
                    now.duration_since(failure_time) >= self.cooldown_duration
                })
            {
                return Some(index);
            }
        }

        let index = start_index;
        self.current_index = (start_index + 1) % self.endpoint_states.len();
        Some(index)
    }

    fn mark_endpoint_failed(&mut self, index: usize) {
        if index < self.endpoint_states.len() {
            self.endpoint_states[index].last_failure = Some(Instant::now());
            self.endpoint_states[index].is_healthy = false;
        }
    }

    fn mark_endpoint_healthy(&mut self, index: usize) {
        if index < self.endpoint_states.len() {
            self.endpoint_states[index].is_healthy = true;
            self.endpoint_states[index].last_failure = None;
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

    fn get_next_endpoint(&self) -> Option<usize> {
        let mut state = self.round_robin_state.lock().unwrap();
        state.get_next_healthy_endpoint()
    }

    fn handle_success(&self, index: usize) {
        let mut state = self.round_robin_state.lock().unwrap();
        state.mark_endpoint_healthy(index);
    }

    fn handle_error(&self, index: usize, operation_name: &str, error: &dyn std::fmt::Display) {
        let client = &self.rpc_clients[index];
        tracing::warn!(
            "{} error for rpc endpoint {}: {}",
            operation_name,
            client.url(),
            error
        );
        let mut state = self.round_robin_state.lock().unwrap();
        state.mark_endpoint_failed(index);
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
