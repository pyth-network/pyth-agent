use {
    super::super::store::{
        self,
        local::PriceInfo,
        PriceIdentifier,
    },
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    chrono::Utc,
    futures_util::future::{
        self,
        join_all,
    },
    key_store::KeyStore,
    pyth_sdk::{
        Identifier,
        PriceStatus,
        UnixTimestamp,
    },
    serde::Serialize,
    slog::Logger,
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::RpcSendTransactionConfig,
    },
    solana_sdk::{
        commitment_config::CommitmentConfig,
        hash::Hash,
        instruction::{
            AccountMeta,
            Instruction,
        },
        pubkey::Pubkey,
        signer::Signer,
        sysvar::clock,
        transaction::Transaction,
    },
    std::{
        collections::HashMap,
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc::Sender,
            oneshot,
            watch,
        },
        time::{
            self,
            Interval,
        },
    },
};

struct PriceUpdate {
    pub_key: Pubkey,
    price:   i64,
    conf:    u64,
    status:  PriceStatus,
}

const PYTH_ORACLE_VERSION: u32 = 2;

#[derive(Serialize, PartialEq, Debug, Clone)]
enum OracleCommand {
    UpdatePriceNoFailOnError = 13,
}

#[derive(Serialize, PartialEq, Debug, Clone)]
struct UpdPriceCmd {
    version:  u32,
    cmd:      OracleCommand,
    status:   PriceStatus,
    price:    i64,
    conf:     u64,
    pub_slot: u64,
}

struct Config {
    /// Interval at which to refresh the cached network state (current slot and blockhash).
    /// It is recommended to set this to slightly less than the network's block time,
    /// as the slot fetched will be used as the time of the price update.
    refresh_network_state_interval: Interval,
    /// Interval at which to publish updates
    publish_interval:               Interval,
    /// Age after which a price update is considered stale and not published
    staleness_threshold:            Duration,
    /// Maximum size of a batch
    max_batch_size:                 usize,
}

/// Exporter is responsible for exporting data held in the local store
/// to the global Pyth Network.
pub struct Exporter {
    rpc_client: RpcClient,

    config: Config,

    /// The Key Store
    key_store: KeyStore,

    /// Channel on which to communicate with the local store
    local_store_tx: Sender<store::local::Message>,

    /// The last time an update was published for each price identifier
    last_published_at: HashMap<PriceIdentifier, UnixTimestamp>,

    /// Watch receiver channel to access the current network state
    network_state_rx: watch::Receiver<NetworkState>,

    logger: Logger,
}

impl Exporter {
    pub async fn run(&mut self) {
        loop {
            self.config.publish_interval.tick().await;
            if let Err(err) = self.publish_updates().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    /// Publishes any price updates in the local store that we haven't sent to this network.
    ///
    /// The strategy used to do this is as follows:
    /// - Fetch all the price updates currently present in the local store
    /// - Filter out price updates we have previously attempted to publish, or which are
    ///   too old to publish.
    /// - Collect the price updates into batches.
    /// - Publish all the batches, staggering them evenly over the interval at which this method is called.
    ///
    /// This design is intended to:
    /// - Decouple the rate at which the local store is updated and the rate at which we publish transactions.
    ///   A user sending an unusually high rate of price updates shouldn't be reflected in the transaction rate.
    /// - Degrade gracefully if the blockchain RPC node exhibits poor performance. If the RPC node takes a long
    ///   time to respond, no internal queues grow unboundedly. At any single point in time there are at most
    ///   (n / batch_size) requests in flight.
    async fn publish_updates(&mut self) -> Result<()> {
        let local_store_contents = self.fetch_local_store_contents().await?;

        // Filter the contents to only include information we haven't already sent,
        // and to ignore stale information.
        let fresh_updates = local_store_contents
            .iter()
            .filter(|(identifier, info)| {
                *self.last_published_at.get(identifier).unwrap_or(&0) < info.timestamp
            })
            .filter(|(_identifier, info)| {
                (Utc::now().timestamp() - info.timestamp)
                    < self.config.staleness_threshold.as_secs() as i64
            })
            .collect::<Vec<_>>();

        // Split the updates up into batches
        let batches = fresh_updates.chunks(self.config.max_batch_size);

        // Publish all the batches, staggering the requests over the publish interval
        let num_batches = batches.len();
        let mut batch_send_interval = time::interval(
            self.config
                .publish_interval
                .period()
                .div_f64(num_batches as f64),
        );
        let mut batch_timestamps = HashMap::new();
        let mut batch_futures = vec![];
        for batch in batches {
            batch_futures.push(self.publish_batch(batch));

            for (identifier, info) in batch {
                batch_timestamps.insert(**identifier, info.timestamp);
            }

            batch_send_interval.tick().await;
        }

        // Wait for all the update requests to complete. Note that this doesn't wait for the
        // transactions themselves to be processed or confirmed, just the RPC requests to return.
        join_all(batch_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.last_published_at.extend(batch_timestamps);

        Ok(())
    }

    async fn fetch_local_store_contents(&self) -> Result<HashMap<PriceIdentifier, PriceInfo>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.local_store_tx
            .send(store::local::Message::LookupAllPriceInfo { result_tx })
            .await
            .map_err(|_| anyhow!("failed to send lookup price info message to local store"))?;
        result_rx
            .await
            .map_err(|_| anyhow!("failed to fetch from local store"))
    }

    async fn publish_batch(&self, batch: &[(&Identifier, &PriceInfo)]) -> Result<()> {
        let mut instructions = Vec::new();

        // Refresh the data in the batch
        let local_store_contents = self.fetch_local_store_contents().await?;
        let refreshed_batch = batch.iter().map(|(identifier, _)| {
            (
                identifier,
                local_store_contents
                    .get(identifier)
                    .ok_or_else(|| anyhow!("price identifier not found in local store"))
                    .with_context(|| identifier.to_string()),
            )
        });

        let network_state = *self.network_state_rx.borrow();
        for (identifier, price_info_result) in refreshed_batch {
            let price_info = price_info_result?;

            let stale_price = (Utc::now().timestamp() - price_info.timestamp)
                < self.config.staleness_threshold.as_secs() as i64;
            if stale_price {
                continue;
            }

            let instruction = Instruction {
                program_id: self.key_store.program_key,
                accounts:   vec![
                    AccountMeta {
                        pubkey:      self.key_store.publish_keypair.pubkey(),
                        is_signer:   true,
                        is_writable: true,
                    },
                    AccountMeta {
                        pubkey:      Pubkey::new(&identifier.to_bytes()),
                        is_signer:   true,
                        is_writable: true,
                    },
                    AccountMeta {
                        pubkey:      clock::id(),
                        is_signer:   false,
                        is_writable: false,
                    },
                ],
                data:       bincode::serialize(&UpdPriceCmd {
                    version:  PYTH_ORACLE_VERSION,
                    cmd:      OracleCommand::UpdatePriceNoFailOnError,
                    status:   price_info.status,
                    price:    price_info.price,
                    conf:     price_info.conf,
                    pub_slot: network_state.current_slot,
                })?,
            };

            instructions.push(instruction);
        }

        // TODO: add priority fee support
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.key_store.publish_keypair.pubkey()),
            &vec![&self.key_store.publish_keypair],
            network_state.blockhash,
        );

        let _signature = self
            .rpc_client
            .send_transaction_with_config(
                &transaction,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..RpcSendTransactionConfig::default()
                },
            )
            .await?;

        // TODO: monitor transaction confirmation rate

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct NetworkState {
    blockhash:    Hash,
    current_slot: u64,
}

/// NetworkStateQuerier periodically queries the current state of the network,
/// fetching the blockhash and slot number.
struct NetworkStateQuerier {
    /// The RPC client
    rpc_client: RpcClient,

    /// The interval with which to query the network state
    query_interval: Interval,

    /// Channel the current network state is sent on
    network_state_tx: watch::Sender<NetworkState>,

    /// Logger
    logger: Logger,
}

impl NetworkStateQuerier {
    pub async fn run(&mut self) {
        loop {
            self.query_interval.tick().await;
            if let Err(err) = self.query_network_state().await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
            }
        }
    }

    async fn query_network_state(&mut self) -> Result<()> {
        // Fetch the blockhash and current slot in parallel
        let current_slot_future = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::confirmed());
        let latest_blockhash_future = self.rpc_client.get_latest_blockhash();

        let (current_slot_result, latest_blockhash_result) =
            future::join(current_slot_future, latest_blockhash_future).await;

        // Send the result on the channel
        self.network_state_tx.send(NetworkState {
            blockhash:    latest_blockhash_result?,
            current_slot: current_slot_result?,
        })?;

        Ok(())
    }
}

/// The key_store module is responsible for parsing the pythd key store.
mod key_store {
    use {
        anyhow::{
            anyhow,
            Context,
            Result,
        },
        solana_sdk::{
            pubkey::Pubkey,
            signature::Keypair,
            signer::keypair,
        },
        std::{
            fs,
            path::{
                Path,
                PathBuf,
            },
            str::FromStr,
        },
    };

    pub struct Config {
        /// Root directory of the KeyStore
        pub root_path:            PathBuf,
        /// Path to the keypair used to publish price updates, relative to the root
        pub publish_keypair_path: PathBuf,
        /// Path to the public key of the Oracle program, relative to the root
        pub program_key_path:     PathBuf,
        /// Path to the public key of the root mapping account, relative to the root
        pub mapping_key_path:     PathBuf,
    }

    pub struct KeyStore {
        /// The keypair used to publish price updates
        pub publish_keypair: Keypair,
        /// Public key of the Oracle program
        pub program_key:     Pubkey,
        /// Public key of the root mapping account
        pub mapping_key:     Pubkey,
    }

    impl KeyStore {
        pub fn new(config: Config) -> Result<Self> {
            Ok(KeyStore {
                publish_keypair: keypair::read_keypair_file(
                    config.root_path.join(config.publish_keypair_path),
                )
                .map_err(|e| anyhow!(e.to_string()))
                .context("reading publish keypair")?,
                program_key:     Self::pubkey_from_path(
                    config.root_path.join(config.program_key_path),
                )
                .context("reading program key")?,
                mapping_key:     Self::pubkey_from_path(
                    config.root_path.join(config.mapping_key_path),
                )
                .context("reading mapping key")?,
            })
        }

        fn pubkey_from_path(path: impl AsRef<Path>) -> Result<Pubkey> {
            let contents = fs::read_to_string(path)?;
            Pubkey::from_str(&contents).map_err(|e| e.into())
        }
    }
}
