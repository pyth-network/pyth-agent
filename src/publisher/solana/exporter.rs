// This module is responsible for exporting data held in the local store to
// the global Pyth Network.

use std::{collections::HashMap, path::Path, time::Duration};

use anyhow::{anyhow, Result};
use chrono::Utc;
use futures_util::future::{self, join_all};
use pyth_sdk::{PriceStatus, UnixTimestamp};
use serde::Serialize;
use slog::Logger;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    sysvar::clock,
    transaction::Transaction,
};
use tokio::{
    fs,
    io::AsyncReadExt,
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
    time::{self, Interval},
};

use crate::publisher::store::local;

use super::super::store::{self, local::PriceInfo, PriceIdentifier};

struct PriceUpdate {
    pub_key: Pubkey,
    price: i64,
    conf: u64,
    status: PriceStatus,
}

// Exporter is responsible for exporting data held in the local store
// to the global Pyth Network.
pub struct Exporter {
    rpc_client: RpcClient,

    // Configuration
    max_batch_size: usize,
    fetch_current_slot_and_blockhash_interval: Interval,
    staleness_threshold: Duration,

    // The accounts
    publisher_pub_key: Pubkey,
    program_pub_key: Pubkey,
    mapping_pub_key: Pubkey,

    // Channel on which to communicate with the local store
    local_store_tx: Sender<store::local::Message>,

    // The keystore
    key_store: KeyStore,

    // Channel on which to send outstanding transactions to the
    // transaction monitor
    outstanding_transactions_tx: Sender<Signature>,

    // Information about the current state of the network
    latest_blockhash: Hash,
    current_slot: u64,

    // The last time an update was sent for this price identifier
    last_sent_timestamp: HashMap<PriceIdentifier, UnixTimestamp>,

    logger: Logger,
}

/*
An upd_price transaction needs:
- Program public key
- Publisher account public key
- Price account public key
- Sysvar clock account public key
- Price (int64)
- Conf (uint64)
- Status (enum)
- Recent blockhash
- Publish slot (uint64)
*/

const PYTH_ORACLE_VERSION: u32 = 2;

// TODO: verify that serialization is correct
#[derive(Serialize, PartialEq, Debug, Clone)]
enum OracleCommand {
    UpdatePriceNoFailOnError = 13,
}

/*
  uint32_t     ver_;
  int32_t      cmd_;
  uint32_t     status_;
  uint32_t     unused_;
  int64_t      price_;
  uint64_t     conf_;
  uint64_t     pub_slot_;
*/
#[derive(Serialize, PartialEq, Debug, Clone)]
struct UpdPriceCmd {
    version: u32,        // encoded directly
    cmd: OracleCommand,  // u32
    status: PriceStatus, // u32
    price: i64,          //
    conf: u64,
    pub_slot: u64,
}

impl Exporter {
    pub fn new(
        rpc_url: &str,
        max_batch_size: usize,
        fetch_current_slot_and_blockhash_interval: Duration,
        staleness_threshold: Duration,
        publisher_pub_key: Pubkey,
        program_pub_key: Pubkey,
        mapping_pub_key: Pubkey,
        local_store_tx: Sender<local::Message>,
        key_store: KeyStore,
        outstanding_transactions_tx: Sender<Signature>,
        logger: Logger,
    ) -> Self {
        Exporter {
            rpc_client: RpcClient::new(rpc_url.to_string()),
            max_batch_size,
            fetch_current_slot_and_blockhash_interval: time::interval(
                fetch_current_slot_and_blockhash_interval,
            ),
            staleness_threshold,
            publisher_pub_key,
            program_pub_key,
            mapping_pub_key,
            local_store_tx,
            key_store,
            outstanding_transactions_tx,
            latest_blockhash: Hash::default(),
            current_slot: 0,
            last_sent_timestamp: HashMap::new(),
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            self.fetch_current_slot_and_blockhash_interval.tick().await;
            if let Err(err) = self.send_updates().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    /*
    Requirements of algorithm:
    - If it is a partial batch, then send at intervals
    - If we have a full batch, then publish immediately (at flush)
    - Don't send updates faster than we receive new slots
    - We want to send updates when we receive a new slot, and stagger the updates


    - How to interact with local store?
    - poll or push model?
    - Want to decouple rate of sending updates to rate updates are received
      from publishers.
    - Don't want to send updates twice.
    - Keep track of last update sent for each price identifier.


    */

    async fn send_updates(&mut self) -> Result<()> {
        let local_store_contents = self.fetch_local_store_contents().await?;

        // Filter the contents to only include information we haven't already sent,
        // and to ignore stale information.
        let fresh_info = local_store_contents
            .iter()
            .filter(|(identifier, info)| {
                *self.last_sent_timestamp.get(identifier).unwrap_or(&0) < info.timestamp
            })
            .filter(|(_identifier, info)| {
                (Utc::now().timestamp() - info.timestamp)
                    < self.staleness_threshold.as_secs() as i64
            })
            .collect::<Vec<_>>();

        // Update the last time we sent each price info
        for (identifier, info) in &fresh_info {
            self.last_sent_timestamp
                .insert(**identifier, info.timestamp);
        }

        // Map the price info into updates
        let updates = fresh_info
            .iter()
            .map(|(identifier, info)| PriceUpdate {
                pub_key: Pubkey::new(&identifier.to_bytes()),
                price: info.price,
                conf: info.conf,
                status: info.status,
            })
            .collect::<Vec<_>>();

        // Split the updates up into batches
        let batches = updates.chunks(self.max_batch_size);

        // Publish the batches concurrently, and wait for the requests to return
        let batch_futures = batches
            .map(|batch| self.publish_batch(batch))
            .collect::<Vec<_>>();
        join_all(batch_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    async fn fetch_local_store_contents(&self) -> Result<HashMap<PriceIdentifier, PriceInfo>> {
        let (result_tx, result_rx) = oneshot::channel();
        let store_contents = self
            .local_store_tx
            .send(store::local::Message::LookupAllPriceInfo { result_tx });
        result_rx
            .await
            .map_err(|_| anyhow!("failed to fetch from local store"))
    }

    async fn fetch_current_slot_and_blockhash(&mut self) -> Result<()> {
        let current_slot_future = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::confirmed());
        let latest_blockhash_future = self.rpc_client.get_latest_blockhash();

        let (current_slot_result, latest_blockhash_result) =
            future::join(current_slot_future, latest_blockhash_future).await;

        self.current_slot = current_slot_result?;
        self.latest_blockhash = latest_blockhash_result?;

        Ok(())
    }

    async fn publish_batch(&self, batch: &[PriceUpdate]) -> Result<()> {
        let mut instructions = Vec::new();

        for update in batch {
            let instruction = Instruction {
                program_id: self.program_pub_key,
                accounts: vec![
                    AccountMeta {
                        pubkey: self.publisher_pub_key,
                        is_signer: true,
                        is_writable: true,
                    },
                    AccountMeta {
                        pubkey: update.pub_key,
                        is_signer: true,
                        is_writable: true,
                    },
                    AccountMeta {
                        pubkey: clock::id(),
                        is_signer: false,
                        is_writable: false,
                    },
                ],
                data: bincode::serialize(&UpdPriceCmd {
                    version: PYTH_ORACLE_VERSION,
                    cmd: OracleCommand::UpdatePriceNoFailOnError,
                    status: update.status,
                    price: update.price,
                    conf: update.conf,
                    pub_slot: self.current_slot,
                })?,
            };

            instructions.push(instruction);
        }

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.publisher_pub_key),
            &vec![self.key_store.get_keypair(self.publisher_pub_key)?],
            self.latest_blockhash,
        );

        let signature = self
            .rpc_client
            .send_transaction_with_config(
                &transaction,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..RpcSendTransactionConfig::default()
                },
            )
            .await?;

        self.outstanding_transactions_tx.send(signature).await?;

        Ok(())
    }
}

pub struct TransactionMonitor {
    // The RPC client
    rpc_client: RpcClient,

    // Interval with which to poll the status of outstanding transactions
    poll_interval: Interval,

    // Vector storing the signatures of outstanding transactions
    outstanding_transactions: Vec<Signature>,

    // Maximum number of outstanding transactions to monitor
    max_outstanding_transactions: usize,

    // Channel the signatures of outstanding transactions are sent on
    signatures_rx: mpsc::Receiver<Signature>,

    logger: Logger,
}

impl TransactionMonitor {
    pub fn new(
        rpc_url: &str,
        poll_interval: Duration,
        max_outstanding_transactions: usize,
        signatures_rx: mpsc::Receiver<Signature>,
        logger: Logger,
    ) -> Self {
        TransactionMonitor {
            rpc_client: RpcClient::new(rpc_url.to_string()),
            poll_interval: time::interval(poll_interval),
            outstanding_transactions: Vec::new(),
            max_outstanding_transactions,
            signatures_rx,
            logger,
        }
    }

    pub async fn run(&mut self) {
        loop {
            if let Err(err) = self.handle_next().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    async fn handle_next(&mut self) -> Result<()> {
        tokio::select! {
            Some(signature) = self.signatures_rx.recv() => {
                self.add_outstanding_transaction(signature);
                Ok(())
            }
            _ = self.poll_interval.tick() => {
                self.poll_outstanding_transactions_status().await
            }
        }
    }

    fn add_outstanding_transaction(&mut self, signature: Signature) {
        // Add the new outstanding transaction to the list
        self.outstanding_transactions.push(signature);

        // Keep the amount of outstanding transactions we monitor at a reasonable size
        if self.outstanding_transactions.len() > self.max_outstanding_transactions {
            warn!(self.logger, "many unacked transactions"; "count" => self.outstanding_transactions.len(), "max" => self.max_outstanding_transactions);
            self.outstanding_transactions
                .drain(0..(self.max_outstanding_transactions / 2));
        }
    }

    async fn poll_outstanding_transactions_status(&mut self) -> Result<()> {
        // Poll the status of each outstanding transaction, in parallel
        let confirmed = join_all(
            self.outstanding_transactions
                .iter()
                .map(|signature| self.poll_transaction(signature)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        // Keep only those transactions which are not yet confirmed
        self.outstanding_transactions = self
            .outstanding_transactions
            .iter()
            .enumerate()
            .filter(|(i, _)| !confirmed[*i])
            .map(|(_, signature)| signature)
            .cloned()
            .collect();

        Ok(())
    }

    async fn poll_transaction(&self, signature: &Signature) -> Result<bool> {
        Ok(self
            .rpc_client
            .confirm_transaction_with_commitment(signature, CommitmentConfig::confirmed())
            .await?
            .value)
    }
}

/*
Key store:
- primary publishing and funding key
    - publish_key_pair.json
[115,175,236,45,14,245,34,253,247,153,44,47,194,94,254,67,248,250,5,124,213,43,200,211,5,130,87,40,21,233,10,132,186,253,27,183,51,5,72,15,233,59,12,85,217,219,53,23,171,150,82,237,238,238,113,164,7,176,124,197,241,232,75,150]

- mapping account public key
    - mapping_key.json
994bwXUyZaJ2yCZs1Jfkcc4wunsn3HNQp1iJD8i7rvN6

- program account public key
    - program_key.json
994bwXUyZaJ2yCZs1Jfkcc4wunsn3HNQp1iJD8i7rvN6

- price account key pair
    - account_PUBKEY.json
[87,224,18,194,93,147,177,187,120,246,190,212,238,11,218,33,56,68,155,244,70,188,9,31,132,189,246,41,14,45,152,158,10,81,188,199,144,112,7,116,41,218,219,254,145,196,65,29,192,118,127,148,77,206,5,114,140,191,126,48,40,204,145,161]

- Typical public
*/

pub struct KeyStore {
    keypairs: HashMap<Pubkey, Keypair>,
    logger: Logger,
}

impl KeyStore {
    pub fn new(logger: Logger) -> Self {
        KeyStore {
            keypairs: HashMap::new(),
            logger,
        }
    }

    fn get_keypair(&self, pub_key: Pubkey) -> Result<&Keypair> {
        self.keypairs
            .get(&pub_key)
            .ok_or_else(|| anyhow!("keypair not found"))
    }

    async fn add_keypair_from_file(&mut self, path: &Path) -> Result<()> {
        let keypair = self.read_keypair_from_file(path).await?;
        self.keypairs.insert(keypair.pubkey(), keypair);

        Ok(())
    }

    async fn read_keypair_from_file(&self, path: &Path) -> Result<Keypair> {
        let mut file = fs::File::open(path).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        self.parse_keypair(contents)
    }

    fn parse_keypair(&self, bytes: Vec<u8>) -> Result<Keypair> {
        Keypair::from_bytes(&bytes).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use iobuffer::IoBuffer;
    use slog_extlog::slog_test;
    use solana_sdk::signer::Signer;

    use super::KeyStore;

    #[test]
    fn parse_keypair_test() {
        let logger = slog_test::new_test_logger(IoBuffer::new());
        let key_store = KeyStore::new(logger);

        let contents = "[115,175,236,45,14,245,34,253,247,153,44,47,194,94,254,67,248,250,5,124,213,43,200,211,5,130,87,40,21,233,10,132,186,253,27,183,51,5,72,15,233,59,12,85,217,219,53,23,171,150,82,237,238,238,113,164,7,176,124,197,241,232,75,150]".as_bytes().to_vec();
        let keypair = key_store.parse_keypair(contents).unwrap();

        println!("{}", keypair.pubkey())
    }
}
