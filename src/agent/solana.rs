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

    pub fn default_rpc_url() -> String {
        "http://localhost:8899".to_string()
    }

    pub fn default_wss_url() -> String {
        "http://localhost:8900".to_string()
    }

    pub fn default_rpc_timeout() -> Duration {
        Duration::from_secs(10)
    }

    /// Configuration for a network
    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct Config {
        /// HTTP RPC endpoint
        #[serde(default = "default_rpc_url")]
        pub rpc_url:     String,
        /// WSS RPC endpoint
        #[serde(default = "default_wss_url")]
        pub wss_url:     String,
        /// Timeout for the requests to the RPC
        #[serde(with = "humantime_serde", default = "default_rpc_timeout")]
        pub rpc_timeout: Duration,
        /// Keystore
        pub key_store:   key_store::Config,
        /// Configuration for the Oracle reading data from this network
        #[serde(default)]
        pub oracle:      oracle::Config,
        /// Configuration for the Exporter publishing data to this network
        #[serde(default)]
        pub exporter:    exporter::Config,
    }

    pub fn spawn_network(
        config: Config,
        local_store_tx: Sender<store::local::Message>,
        global_store_update_tx: mpsc::Sender<global::Update>,
        keypair_request_tx: mpsc::Sender<KeypairRequest>,
        logger: Logger,
    ) -> Result<Vec<JoinHandle<()>>> {
        // Publisher permissions updates between oracle and exporter
        let (publisher_permissions_tx, publisher_permissions_rx) =
            mpsc::channel(config.oracle.updates_channel_capacity);

        // Spawn the Oracle
        let mut jhs = oracle::spawn_oracle(
            config.oracle.clone(),
            &config.rpc_url,
            &config.wss_url,
            config.rpc_timeout,
            global_store_update_tx.clone(),
            publisher_permissions_tx,
            KeyStore::new(config.key_store.clone(), &logger)?,
            logger.clone(),
        );

        // Spawn the Exporter
        let exporter_jhs = exporter::spawn_exporter(
            config.exporter,
            &config.rpc_url,
            config.rpc_timeout,
            publisher_permissions_rx,
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
            de::Error,
            Deserialize,
            Deserializer,
            Serialize,
            Serializer,
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
    pub struct Config {
        /// Path to the keypair used to publish price updates. If set
        /// to a non-existent file path, the system expects a keypair
        /// to be loaded via the remote keypair loader. If the path is
        /// valid, the remote keypair loading is disabled.
        pub publish_keypair_path: PathBuf,
        /// The public key of the Oracle program
        #[serde(
            serialize_with = "pubkey_string_ser",
            deserialize_with = "pubkey_string_de"
        )]
        pub program_key:          Pubkey,
        /// The public key of the root mapping account
        #[serde(
            serialize_with = "pubkey_string_ser",
            deserialize_with = "pubkey_string_de"
        )]
        pub mapping_key:          Pubkey,
        /// The public key of the accumulator program.
        #[serde(
            serialize_with = "opt_pubkey_string_ser",
            deserialize_with = "opt_pubkey_string_de",
            default
        )]
        pub accumulator_key:      Option<Pubkey>,
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
            let publish_keypair = match keypair::read_keypair_file(&config.publish_keypair_path) {
                Ok(k) => Some(k),
                Err(e) => {
                    warn!(logger,
			  "Reading publish keypair returned an error. Waiting for a remote-loaded key before publishing.";
			  "publish_keypair_path" => config.publish_keypair_path.display(), "error" => e.to_string());
                    None
                }
            };

            Ok(KeyStore {
                publish_keypair,
                program_key: config.program_key,
                mapping_key: config.mapping_key,
                accumulator_key: config.accumulator_key,
            })
        }
    }

    // Helper methods for stringified SOL addresses

    fn pubkey_string_ser<S>(k: &Pubkey, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_str(&k.to_string())
    }

    fn pubkey_string_de<'de, D>(de: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let pubkey_string = String::deserialize(de)?;
        let pubkey = Pubkey::from_str(&pubkey_string).map_err(D::Error::custom)?;
        Ok(pubkey)
    }

    fn opt_pubkey_string_ser<S>(k_opt: &Option<Pubkey>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let k_str_opt = (*k_opt).map(|k| k.to_string());

        Option::<String>::serialize(&k_str_opt, ser)
    }

    fn opt_pubkey_string_de<'de, D>(de: D) -> Result<Option<Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<String>::deserialize(de)? {
            Some(k) => Ok(Some(Pubkey::from_str(&k).map_err(D::Error::custom)?)),
            None => Ok(None),
        }
    }
}
