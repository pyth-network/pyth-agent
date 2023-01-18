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
        anyhow::Result,
        serde::{
            Deserialize,
            Serialize,
        },
        slog::Logger,
        tokio::{
            sync::{
                mpsc,
                mpsc::Sender,
            },
            task::JoinHandle,
        },
    };

    /// Configuration for a network
    #[derive(Clone, Serialize, Deserialize, Debug, Default)]
    pub struct Config {
        /// Keystore
        pub key_store: key_store::Config,
        /// Configuration for the Oracle reading data from this network
        pub oracle:    oracle::Config,
        /// Configuration for the Exporter publishing data to this network
        pub exporter:  exporter::Config,
    }

    pub fn spawn_network(
        config: Config,
        local_store_tx: Sender<store::local::Message>,
        global_store_update_tx: mpsc::Sender<global::Update>,
        logger: Logger,
    ) -> Result<Vec<JoinHandle<()>>> {
        // Create the key store
        let key_store = KeyStore::new(config.key_store.clone())?;

        // Spawn the Oracle
        let mut jhs = oracle::spawn_oracle(
            config.oracle.clone(),
            global_store_update_tx,
            logger.clone(),
        );

        // Spawn the Exporter
        let exporter_jhs =
            exporter::spawn_exporter(config.exporter, key_store, local_store_tx, logger)?;
        jhs.extend(exporter_jhs);

        Ok(jhs)
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
        serde::{
            Deserialize,
            Serialize,
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


    #[derive(Clone, Serialize, Deserialize, Debug)]
    #[serde(default)]
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

    impl Default for Config {
        fn default() -> Self {
            Self {
                root_path:            Default::default(),
                publish_keypair_path: "publish_key_pair.json".into(),
                program_key_path:     "program_key.json".into(),
                mapping_key_path:     "mapping_key.json".into(),
            }
        }
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
            Pubkey::from_str(contents.trim()).map_err(|e| e.into())
        }
    }
}
