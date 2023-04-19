pub mod exporter;
pub mod oracle;

/// This module encapsulates all the interaction with a single Solana network:
/// - The Oracle, which reads data from the network
/// - The Exporter, which publishes data to the network
pub mod network {

    use {
        super::{
            super::{
                store,
                store::global,
            },
            exporter,
            key_store::{
                self,
                KeyStore,
            },
            oracle,
        },
        crate::agent::remote_keypair_loader::KeypairRequest,
        anyhow::Result,
        serde::{
            Deserialize,
            Serialize,
        },
        slog::Logger,
        std::time::Duration,
        tokio::{
            sync::{
                mpsc,
                mpsc::Sender,
            },
            task::JoinHandle,
        },
    };

    /// Configuration for a network
    #[derive(Clone, Serialize, Deserialize, Debug)]
    #[serde(default)]
    pub struct Config {
        /// HTTP RPC endpoint
        pub rpc_url:     String,
        /// WSS RPC endpoint
        pub wss_url:     String,
        /// Timeout for the requests to the RPC
        #[serde(with = "humantime_serde")]
        pub rpc_timeout: Duration,
        /// Keystore
        pub key_store:   key_store::Config,
        /// Configuration for the Oracle reading data from this network
        pub oracle:      oracle::Config,
        /// Configuration for the Exporter publishing data to this network
        pub exporter:    exporter::Config,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                rpc_url:     "http://localhost:8899".to_string(),
                wss_url:     "ws://localhost:8900".to_string(),
                rpc_timeout: Duration::from_secs(10),
                key_store:   Default::default(),
                oracle:      Default::default(),
                exporter:    Default::default(),
            }
        }
    }

    pub fn spawn_network(
        config: Config,
        local_store_tx: Sender<store::local::Message>,
        global_store_update_tx: mpsc::Sender<global::Update>,
        keypair_request_tx: mpsc::Sender<KeypairRequest>,
        logger: Logger,
    ) -> Result<Vec<JoinHandle<()>>> {
        // Spawn the Oracle
        let mut jhs = oracle::spawn_oracle(
            config.oracle.clone(),
            &config.rpc_url,
            &config.wss_url,
            config.rpc_timeout,
            KeyStore::new(config.key_store.clone(), &logger)?,
            global_store_update_tx.clone(),
            logger.clone(),
        );

        // Spawn the Exporter
        let exporter_jhs = exporter::spawn_exporter(
            config.exporter,
            &config.rpc_url,
            config.rpc_timeout,
            KeyStore::new(config.key_store.clone(), &logger)?,
            local_store_tx,
            keypair_request_tx,
            logger,
        )?;
        jhs.extend(exporter_jhs);

        Ok(jhs)
    }
}

/// The key_store module is responsible for parsing the pythd key store.
mod key_store {
    use {
        anyhow::{
            Context,
            Result,
        },
        serde::{
            Deserialize,
            Serialize,
        },
        slog::Logger,
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

    #[derive(Clone, Serialize, Deserialize, Debug)]
    #[serde(default)]
    pub struct Config {
        /// Root directory of the KeyStore
        pub root_path:            PathBuf,
        /// Path to the keypair used to publish price updates,
        /// relative to the root. If set to a non-existent file path,
        /// the system expects a keypair to be loaded via the remote
        /// keypair loader. If the path is valid, the remote keypair
        /// loading is disabled.
        pub publish_keypair_path: PathBuf,
        /// Path to the public key of the Oracle program, relative to the root
        pub program_key_path:     PathBuf,
        /// Path to the public key of the root mapping account, relative to the root
        pub mapping_key_path:     PathBuf,
        /// Path to the public key of the accumulator program, relative to the root.
        pub accumulator_key_path: Option<PathBuf>,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                root_path:            Default::default(),
                publish_keypair_path: "publish_key_pair.json".into(),
                program_key_path:     "program_key.json".into(),
                mapping_key_path:     "mapping_key.json".into(),
                // FIXME: Temporary for the accumulator demo. Should be None
                accumulator_key_path: Some("accumulator_program_key.json".into()),
            }
        }
    }

    pub struct KeyStore {
        /// The keypair used to publish price updates. When None,
        /// publishing will not start until a new keypair is supplied
        /// via the remote loading endpoint
        pub publish_keypair: Option<Keypair>,
        /// Public key of the Oracle program
        pub program_key:     Pubkey,
        /// Public key of the root mapping account
        pub mapping_key:     Pubkey,
        /// Public key of the accumulator program (if provided)
        pub accumulator_key: Option<Pubkey>,
    }

    impl KeyStore {
        pub fn new(config: Config, logger: &Logger) -> Result<Self> {
            let full_keypair_path = config.root_path.join(config.publish_keypair_path);

            let publish_keypair = match keypair::read_keypair_file(&full_keypair_path) {
                Ok(k) => Some(k),
                Err(e) => {
                    warn!(logger,
			  "Reading publish keypair returned an error. Waiting for a remote-loaded key before publishing.";
			  "full_keypair_path" => full_keypair_path.to_str(), "error" => e.to_string());
                    None
                }
            };

            let accumulator_key: Option<Pubkey> = if let Some(key_path) = config.accumulator_key_path {
                Some(Self::pubkey_from_path(config.root_path.join(key_path)).context("Reading accumulator key")?)
            } else {
                None
            };

            Ok(KeyStore {
                publish_keypair,
                program_key: Self::pubkey_from_path(config.root_path.join(config.program_key_path))
                    .context("reading program key")?,
                mapping_key: Self::pubkey_from_path(config.root_path.join(config.mapping_key_path))
                    .context("reading mapping key")?,
                accumulator_key,
            })
        }

        fn pubkey_from_path(path: impl AsRef<Path>) -> Result<Pubkey> {
            let contents = fs::read_to_string(path)?;
            Pubkey::from_str(contents.trim()).map_err(|e| e.into())
        }
    }
}
