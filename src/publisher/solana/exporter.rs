// This module is responsible for exporting data held in the local store to
// the global Pyth Network.

use std::{collections::HashMap, path::Path};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use parking_lot::RwLock;
use pyth_sdk::PriceStatus;
use serde::Serialize;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::Keypair,
    sysvar::clock,
    transaction::Transaction,
};
use tokio::time::{sleep, Duration};

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
    flush_interval: Duration,

    // The accounts
    publisher_pub_key: Pubkey,
    program_pub_key: Pubkey,

    // The key store
    key_store: KeyStore,

    // Current information about the network
    recent_blockhash: RwLock<Option<Hash>>,
    current_slot: RwLock<Option<u64>>,

    // Pending updates which have yet to be sent
    pending_updates: RwLock<HashMap<PriceIdentifier, PriceInfo>>,
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

// #[async_trait]
// impl store::local::Observer for Exporter {
//     async fn price_updated(&self, identifier: PriceIdentifier, info: PriceInfo) {
//         if let Some(current) = self.pending_updates.read().get(&identifier) {
//             if current.timestamp > info.timestamp {
//                 return;
//             }
//         }

//         self.pending_updates.write().insert(identifier, info);
//     }
// }

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
    async fn run(&self) {
        loop {
            self.flush().await;
            sleep(self.flush_interval).await;
        }
    }

    async fn flush(&self) -> Result<()> {
        // Collect all the pending updates, emptying the hashmap
        let updates = self
            .pending_updates
            .write()
            .drain()
            .map(|(identifier, info)| PriceUpdate {
                pub_key: Pubkey::new(&identifier.to_bytes()),
                price: info.price,
                conf: info.conf,
                status: info.status,
            })
            .collect::<Vec<_>>();

        // Split the updates up into batches
        let batches = updates.chunks(self.max_batch_size);

        // Publish each batch
        for batch in batches {
            self.publish_batch(batch).await?;
        }

        Ok(())
    }

    async fn publish_batch(&self, batch: &[PriceUpdate]) -> Result<()> {
        let mut instructions = Vec::new();

        let current_slot = self
            .current_slot
            .read()
            .ok_or_else(|| anyhow!("current slot not available"))?;

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
                    pub_slot: current_slot,
                })?,
            };

            instructions.push(instruction);
        }

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.publisher_pub_key),
            &vec![self.key_store.get_keypair(self.publisher_pub_key)?],
            self.recent_blockhash
                .read()
                .ok_or_else(|| anyhow!("recent blockhash not available"))?,
        );

        // TODO: make this async
        self.rpc_client.send_transaction_with_config(
            &transaction,
            RpcSendTransactionConfig {
                skip_preflight: true, // TODO: do we want to do this?
                ..RpcSendTransactionConfig::default()
            },
        )?;

        Ok(())
    }
}

struct KeyStore {
    keypairs: HashMap<Pubkey, Keypair>,
}

impl KeyStore {
    fn get_keypair(&self, pub_key: Pubkey) -> Result<&Keypair> {
        self.keypairs
            .get(&pub_key)
            .ok_or_else(|| anyhow!("keypair not found"))
    }

    fn load(&mut self, path: &Path) -> Result<()> {
        // TODO: load in key store from key store directory
        todo!();
    }
}
