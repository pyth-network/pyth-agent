use {
    super::State,
    anyhow::Result,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::Signature,
    },
    std::collections::VecDeque,
    tokio::sync::RwLock,
};

#[derive(Default)]
pub struct TransactionsState {
    sent_transactions: RwLock<VecDeque<Signature>>,
    max_transactions:  usize,
}

impl TransactionsState {
    pub fn new(max_transactions: usize) -> Self {
        Self {
            sent_transactions: Default::default(),
            max_transactions,
        }
    }
}

#[async_trait::async_trait]
pub trait Transactions {
    async fn add_transaction(&self, signature: Signature);
    async fn poll_transactions_status(&self, rpc: &RpcClient) -> Result<()>;
}

/// Allow downcasting State into TransactionsState for functions that depend on the `Transactions` service.
impl<'a> From<&'a State> for &'a TransactionsState {
    fn from(state: &'a State) -> &'a TransactionsState {
        &state.transactions
    }
}

#[async_trait::async_trait]
impl<T> Transactions for T
where
    for<'a> &'a T: Into<&'a TransactionsState>,
    T: Sync + Send + 'static,
{
    async fn add_transaction(&self, signature: Signature) {
        tracing::debug!(
            signature = signature.to_string(),
            "Monitoring new transaction.",
        );

        // Add the new transaction to the list
        let mut txs = self.into().sent_transactions.write().await;
        txs.push_back(signature);

        // Pop off the oldest transaction if necessary
        if txs.len() > self.into().max_transactions {
            txs.pop_front();
        }
    }

    async fn poll_transactions_status(&self, rpc: &RpcClient) -> Result<()> {
        let mut txs = self.into().sent_transactions.write().await;
        if txs.is_empty() {
            return Ok(());
        }

        let signatures_contiguous = txs.make_contiguous();

        // Poll the status of each transaction, in a single RPC request
        let statuses = rpc
            .get_signature_statuses(signatures_contiguous)
            .await?
            .value;

        tracing::debug!(
            statuses = ?statuses,
            "Processing Signature Statuses",
        );

        // Determine the percentage of the recently sent transactions that have successfully been committed
        // TODO: expose as metric
        let confirmed = statuses
            .into_iter()
            .zip(signatures_contiguous)
            .map(|(status, sig)| status.map(|some_status| (some_status, sig))) // Collate Some() statuses with their tx signatures before flatten()
            .flatten()
            .filter(|(status, sig)| {
                if let Some(err) = status.err.as_ref() {
                    tracing::warn!(
                        error = err.to_string(),
                        tx_signature = sig.to_string(),
                        "TX status has err value",
                    );
                }

                status.satisfies_commitment(CommitmentConfig::confirmed())
            })
            .count();

        let percentage_confirmed = ((confirmed as f64) / (txs.len() as f64)) * 100.0;

        tracing::info!(
            percentage_confirmed = format!("{:.}", percentage_confirmed),
            "monitoring transaction hit rate",
        );

        Ok(())
    }
}
