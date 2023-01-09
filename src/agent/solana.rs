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
        /// Configuration for the Oracle reading data from this network
        pub oracle:   oracle::Config,
        /// Configuration for the Exporter publishing data to this network
        pub exporter: exporter::Config,
    }

    pub fn spawn_network(
        config: Config,
        local_store_tx: Sender<store::local::Message>,
        global_store_update_tx: mpsc::Sender<global::Update>,
        logger: Logger,
    ) -> Result<Vec<JoinHandle<()>>> {
        // Spawn the Oracle
        let mut jhs = oracle::spawn_oracle(
            config.oracle.clone(),
            global_store_update_tx,
            logger.clone(),
        );

        // Spawn the Exporter
        let exporter_jhs = exporter::spawn_exporter(config.exporter, local_store_tx, logger)?;
        jhs.extend(exporter_jhs);

        Ok(jhs)
    }
}
