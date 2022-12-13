/// The key_store module is responsible for parsing the pythd key store.
mod key_store {
    use {
        anyhow::{
            Error,
            Result,
        },
        slog::Logger,
        solana_sdk::signature::Keypair,
    };

    pub struct KeyStore {
        logger: Logger,
    }

    impl KeyStore {
        pub fn new(logger: Logger) -> Self {
            KeyStore { logger }
        }

        /// Takes a byte array of a string representation of a keypair of the form "[115, ..., 150]",
        /// where the first 64 bytes represent the private key and the last 64 bytes represent the public key,
        /// and parses this into a solana_sdk Keypair object.
        fn parse_keypair(&self, contents: &str) -> Result<Keypair> {
            let byte_vec: Vec<u8> = serde_json::from_str(contents)?;
            Keypair::from_bytes(&byte_vec[..]).map_err(|e| e.into())
        }
    }


    #[cfg(test)]
    mod tests {
        use {
            super::KeyStore,
            iobuffer::IoBuffer,
            slog_extlog::slog_test,
            solana_sdk::signer::Signer,
        };

        #[test]
        fn parse_keypair_test() {
            let logger = slog_test::new_test_logger(IoBuffer::new());
            let key_store = KeyStore::new(logger);
            let contents = "[115,175,236,45,14,245,34,253,247,153,44,47,194,94,254,67,248,250,5,124,213,43,200,211,5,130,87,40,21,233,10,132,186,253,27,183,51,5,72,15,233,59,12,85,217,219,53,23,171,150,82,237,238,238,113,164,7,176,124,197,241,232,75,150]";
            let keypair = key_store.parse_keypair(contents).unwrap();

            assert_eq!(
                vec![
                    115, 175, 236, 45, 14, 245, 34, 253, 247, 153, 44, 47, 194, 94, 254, 67, 248,
                    250, 5, 124, 213, 43, 200, 211, 5, 130, 87, 40, 21, 233, 10, 132
                ],
                keypair.secret().to_bytes()
            );
            assert_eq!(
                vec![
                    186, 253, 27, 183, 51, 5, 72, 15, 233, 59, 12, 85, 217, 219, 53, 23, 171, 150,
                    82, 237, 238, 238, 113, 164, 7, 176, 124, 197, 241, 232, 75, 150
                ],
                keypair.pubkey().to_bytes()
            );
        }
    }
}
