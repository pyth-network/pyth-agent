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
