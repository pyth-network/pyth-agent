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
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
    url::Url,
};

#[derive(Debug, Clone)]
struct EndpointState {
    last_failure: Option<Instant>,
    is_healthy: bool,
}

#[derive(Debug)]
struct RoundRobinState {
    current_index: usize,
    endpoint_states: Vec<EndpointState>,
    cooldown_duration: Duration,
}

impl RoundRobinState {
    fn new(endpoint_count: usize, cooldown_duration: Duration) -> Self {
        Self {
            current_index: 0,
            endpoint_states: vec![EndpointState { last_failure: None, is_healthy: true }; endpoint_count],
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
            if state.is_healthy || state.last_failure.map_or(true, |failure_time| 
                now.duration_since(failure_time) >= self.cooldown_duration) {
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
    rpc_clients: Vec<RpcClient>,
    round_robin_state: Arc<Mutex<RoundRobinState>>,
}

impl RpcMultiClient {
    pub fn new_with_timeout(rpc_urls: Vec<Url>, timeout: Duration) -> Self {
        Self::new_with_timeout_and_cooldown(rpc_urls, timeout, Duration::from_secs(30))
    }

    pub fn new_with_timeout_and_cooldown(rpc_urls: Vec<Url>, timeout: Duration, cooldown_duration: Duration) -> Self {
        let clients: Vec<RpcClient> = rpc_urls
            .iter()
            .map(|rpc_url| RpcClient::new_with_timeout(rpc_url.to_string(), timeout))
            .collect();
        let round_robin_state = Arc::new(Mutex::new(RoundRobinState::new(clients.len(), cooldown_duration)));
        Self {
            rpc_clients: clients,
            round_robin_state,
        }
    }

    pub fn new_with_commitment(rpc_urls: Vec<Url>, commitment_config: CommitmentConfig) -> Self {
        Self::new_with_commitment_and_cooldown(rpc_urls, commitment_config, Duration::from_secs(30))
    }

    pub fn new_with_commitment_and_cooldown(rpc_urls: Vec<Url>, commitment_config: CommitmentConfig, cooldown_duration: Duration) -> Self {
        let clients: Vec<RpcClient> = rpc_urls
            .iter()
            .map(|rpc_url| RpcClient::new_with_commitment(rpc_url.to_string(), commitment_config))
            .collect();
        let round_robin_state = Arc::new(Mutex::new(RoundRobinState::new(clients.len(), cooldown_duration)));
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
        Self::new_with_timeout_commitment_and_cooldown(rpc_urls, timeout, commitment_config, Duration::from_secs(30))
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
        let round_robin_state = Arc::new(Mutex::new(RoundRobinState::new(clients.len(), cooldown_duration)));
        Self {
            rpc_clients: clients,
            round_robin_state,
        }
    }


    pub async fn get_balance(&self, kp: &Keypair) -> anyhow::Result<u64> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_balance(&kp.pubkey()).await {
                    Ok(balance) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(balance);
                    }
                    Err(e) => {
                        tracing::warn!("getBalance error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getBalance failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn send_transaction_with_config(
        &self,
        transaction: &Transaction,
    ) -> anyhow::Result<Signature> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client
                    .send_transaction_with_config(
                        transaction,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            ..RpcSendTransactionConfig::default()
                        },
                    )
                    .await
                {
                    Ok(signature) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(signature);
                    }
                    Err(e) => {
                        tracing::warn!("sendTransactionWithConfig error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("sendTransactionWithConfig failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn get_signature_statuses(
        &self,
        signatures_contiguous: &mut [Signature],
    ) -> anyhow::Result<Vec<Option<TransactionStatus>>> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_signature_statuses(signatures_contiguous).await {
                    Ok(statuses) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(statuses.value);
                    }
                    Err(e) => {
                        tracing::warn!("getSignatureStatuses error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getSignatureStatuses failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn get_recent_prioritization_fees(
        &self,
        price_accounts: &[Pubkey],
    ) -> anyhow::Result<Vec<RpcPrioritizationFee>> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_recent_prioritization_fees(price_accounts).await {
                    Ok(fees) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(fees);
                    }
                    Err(e) => {
                        tracing::warn!("getRecentPrioritizationFees error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getRecentPrioritizationFees failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn get_program_accounts(
        &self,
        oracle_program_key: Pubkey,
    ) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_program_accounts(&oracle_program_key).await {
                    Ok(accounts) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(accounts);
                    }
                    Err(e) => {
                        tracing::warn!("getProgramAccounts error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getProgramAccounts failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn get_account_data(&self, publisher_config_key: &Pubkey) -> anyhow::Result<Vec<u8>> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_account_data(publisher_config_key).await {
                    Ok(data) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(data);
                    }
                    Err(e) => {
                        tracing::warn!("getAccountData error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getAccountData failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn get_slot_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<u64> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_slot_with_commitment(commitment_config).await {
                    Ok(slot) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(slot);
                    }
                    Err(e) => {
                        tracing::warn!("getSlotWithCommitment error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getSlotWithCommitment failed for all RPC endpoints after {} attempts", attempts)
    }

    pub async fn get_latest_blockhash(&self) -> anyhow::Result<solana_sdk::hash::Hash> {
        let mut attempts = 0;
        let max_attempts = self.rpc_clients.len() * 2;
        
        while attempts < max_attempts {
            let endpoint_index = {
                let mut state = self.round_robin_state.lock().unwrap();
                state.get_next_healthy_endpoint()
            };
            
            if let Some(index) = endpoint_index {
                let client = &self.rpc_clients[index];
                match client.get_latest_blockhash().await {
                    Ok(hash) => {
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_healthy(index);
                        return Ok(hash);
                    }
                    Err(e) => {
                        tracing::warn!("getLatestBlockhash error for rpc endpoint {}: {}", client.url(), e);
                        let mut state = self.round_robin_state.lock().unwrap();
                        state.mark_endpoint_failed(index);
                    }
                }
            }
            attempts += 1;
        }
        
        bail!("getLatestBlockhash failed for all RPC endpoints after {} attempts", attempts)
    }
}
