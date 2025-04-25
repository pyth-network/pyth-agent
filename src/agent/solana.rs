/// This module encapsulates all the interaction with a single Solana network:
/// - The Oracle, which reads data from the network
/// - The Exporter, which publishes data to the network
pub mod network {
    use {
        super::key_store::{
            self,
        },
        crate::agent::{
            services::exporter,
            state::oracle::{
                self,
            },
        },
        serde::{
            Deserialize,
            Serialize,
        },
        std::time::Duration,
        url::Url,
    };

    #[derive(Clone, Copy, Serialize, Deserialize, Debug)]
    pub enum Network {
        Primary,
        Secondary,
    }

    pub fn default_rpc_urls() -> Vec<Url> {
        vec![Url::parse("http://localhost:8899").unwrap()]
    }

    pub fn default_wss_urls() -> Vec<Url> {
        vec![Url::parse("http://localhost:8900").unwrap()]
    }

    pub fn default_rpc_timeout() -> Duration {
        Duration::from_secs(10)
    }

    /// Configuration for a network
    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct Config {
        /// HTTP RPC endpoint list
        #[serde(default = "default_rpc_urls")]
        pub rpc_urls:    Vec<Url>,
        /// WSS RPC endpoint
        #[serde(default = "default_wss_urls")]
        pub wss_urls:    Vec<Url>,
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
}

/// The key_store module is responsible for parsing the pythd key store.
pub mod key_store {
    use {
        anyhow::Result,
        serde::{
            Deserialize,
            Deserializer,
            Serialize,
            Serializer,
            de::Error,
        },
        solana_sdk::{
            pubkey::Pubkey,
            signature::Keypair,
            signer::keypair,
        },
        std::{
            path::PathBuf,
            str::FromStr,
        },
    };

    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct Config {
        /// Path to the keypair used to publish price updates. If set
        /// to a non-existent file path, the system expects a keypair
        /// to be loaded via the remote keypair loader. If the path is
        /// valid, the remote keypair loading is disabled.
        pub publish_keypair_path:         PathBuf,
        /// The public key of the Oracle program
        #[serde(
            serialize_with = "pubkey_string_ser",
            deserialize_with = "pubkey_string_de",
            alias = "program_key" // for compatibility
        )]
        pub pyth_oracle_program_key:      Pubkey,
        /// The public key of the pyth-price-store program
        #[serde(
            serialize_with = "opt_pubkey_string_ser",
            deserialize_with = "opt_pubkey_string_de",
            default
        )]
        pub pyth_price_store_program_key: Option<Pubkey>,
        /// The public key of the accumulator program.
        #[serde(
            serialize_with = "opt_pubkey_string_ser",
            deserialize_with = "opt_pubkey_string_de",
            default
        )]
        pub accumulator_key:              Option<Pubkey>,
    }

    pub struct KeyStore {
        /// The keypair used to publish price updates. When None,
        /// publishing will not start until a new keypair is supplied
        /// via the remote loading endpoint
        pub publish_keypair:              Option<Keypair>,
        /// Public key of the Oracle program
        pub pyth_oracle_program_key:      Pubkey,
        /// Public key of the pyth-price-store program
        pub pyth_price_store_program_key: Option<Pubkey>,
        /// Public key of the accumulator program (if provided)
        pub accumulator_key:              Option<Pubkey>,
    }

    impl KeyStore {
        pub fn new(config: Config) -> Result<Self> {
            let publish_keypair = match keypair::read_keypair_file(&config.publish_keypair_path) {
                Ok(k) => Some(k),
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        publish_keypair_path = config.publish_keypair_path.display().to_string(),
                        "Reading publish keypair returned an error. Waiting for a remote-loaded key before publishing.",
                    );
                    None
                }
            };

            Ok(KeyStore {
                publish_keypair,
                pyth_oracle_program_key: config.pyth_oracle_program_key,
                pyth_price_store_program_key: config.pyth_price_store_program_key,
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
