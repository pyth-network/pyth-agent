pub mod pythd;
pub mod solana;
pub mod store;

use std::{str::FromStr, time::Duration};

use anyhow::Result;
use futures_util::future::join_all;
use slog::{Drain, Logger};
use solana_sdk::commitment_config::CommitmentLevel;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};

use solana_sdk::pubkey::Pubkey;

use self::{
    pythd::{adapter::Adapter, api::rpc::Server},
    solana::{
        exporter::{Exporter, KeyStore, TransactionMonitor},
        oracle::{self, Oracle, Subscriber},
    },
    store::{global, local},
};

pub struct Publisher {}

// TODO: structure this better, possibly with some more abstractions (e.g. grouping all components needed for each "network" together)
// TODO: create config structs for all components (some already exist), with sensible defaults, and use config crate to populate these
impl Publisher {
    pub fn new() -> Self {
        Publisher {}
    }

    pub async fn run(&self) {
        let logger = slog::Logger::root(
            slog_async::Async::new(
                slog_term::CompactFormat::new(slog_term::TermDecorator::new().build())
                    .build()
                    .fuse(),
            )
            .build()
            .fuse(),
            o!(),
        );

        if let Err(err) = self.try_run(logger.clone()).await {
            error!(logger, "{:#}", err; "error" => format!("{:?}", err));
        };
    }

    async fn try_run(&self, logger: Logger) -> Result<()> {
        // Create the shutdown task
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1000);

        // Configuration paramaters
        let adapter_notify_price_sched_interval = Duration::from_secs(25);
        let primary_oracle_commitment = CommitmentLevel::Confirmed;
        let primary_oracle_poll_interval_duration = Duration::from_secs(30);
        let primary_oracle_rpc_url = "https://MAINNET_NODE:8999";
        let primary_oracle_wss_url = "wss://MAINNET_NODE:9000";
        let primary_oracle_account_key = Pubkey::from_str("SOME_ACCOUNT_KEY")?;
        let primary_oracle_mapping_account_key = Pubkey::from_str("MAPPING_ACCOUNT_KEY")?;
        let secondary_oracle_commitment = CommitmentLevel::Confirmed;
        let secondary_oracle_poll_interval_duration = Duration::from_secs(30);
        let secondary_oracle_rpc_url = "https://PYTHNET_NODE:8999";
        let secondary_oracle_wss_url = "wss://PYTHNET_NODE:9000";
        let secondary_oracle_account_key = Pubkey::from_str("SOME_ACCOUNT_KEY")?;
        let secondary_oracle_mapping_account_key = Pubkey::from_str("MAPPING_ACCOUNT_KEY")?;
        let primary_exporter_max_batch_size = 8;
        let primary_exporter_fetch_current_slot_and_blockhash_interval = Duration::from_secs(10);
        let primary_exporter_staleness_threshold = Duration::from_secs(60);
        let primary_exporter_publisher_pub_key = Pubkey::from_str("SOME_PUBLISHER_PUB_KEY")?;
        let primary_exporter_program_pub_key = Pubkey::from_str("SOME_PROGRAM_PUB_KEY")?;
        let primary_exporter_mapping_pub_key = Pubkey::from_str("SOME_MAPPING_PUB_KEY")?;
        let primary_transaction_monitor_poll_interval = Duration::from_secs(120);
        let primary_transaction_max_outstanding_transactions = 100;
        let secondary_exporter_max_batch_size = 8;
        let secondary_exporter_fetch_current_slot_and_blockhash_interval = Duration::from_secs(10);
        let secondary_exporter_staleness_threshold = Duration::from_secs(60);
        let secondary_exporter_publisher_pub_key = Pubkey::from_str("SOME_PUBLISHER_PUB_KEY")?;
        let secondary_exporter_program_pub_key = Pubkey::from_str("SOME_PROGRAM_PUB_KEY")?;
        let secondary_exporter_mapping_pub_key = Pubkey::from_str("SOME_MAPPING_PUB_KEY")?;
        let secondary_transaction_monitor_poll_interval = Duration::from_secs(120);
        let secondary_transaction_max_outstanding_transactions = 100;

        // Channels
        let (adapter_tx, adapter_rx) = mpsc::channel(1000);
        let (local_store_tx, local_store_rx) = mpsc::channel(1000);
        let (primary_global_store_tx, primary_global_store_rx) = mpsc::channel(1000);
        let (secondary_global_store_tx, secondary_global_store_rx) = mpsc::channel(1000);
        let (primary_oracle_updates_tx, primary_oracle_updates_rx) = mpsc::channel(1000);
        let (secondary_oracle_updates_tx, secondary_oracle_updates_rx) = mpsc::channel(1000);
        let (primary_transaction_monitor_signature_tx, primary_transaction_monitor_signature_rx) =
            mpsc::channel(1000);
        let (
            secondary_transaction_monitor_signature_tx,
            secondary_transaction_monitor_signature_rx,
        ) = mpsc::channel(1000);

        // Create the subscribers
        let primary_oracle_subscriber = Subscriber::new(
            primary_oracle_commitment,
            primary_oracle_account_key,
            primary_oracle_rpc_url,
            primary_oracle_wss_url,
            primary_oracle_updates_tx,
            logger.clone(),
        );
        let secondary_oracle_subscriber = Subscriber::new(
            secondary_oracle_commitment,
            secondary_oracle_account_key,
            secondary_oracle_rpc_url,
            secondary_oracle_wss_url,
            secondary_oracle_updates_tx,
            logger.clone(),
        );

        // Create the oracles
        let mut primary_oracle = Oracle::new(
            oracle::Config {
                commitment: primary_oracle_commitment,
                oracle_account_key: primary_oracle_account_key,
                mapping_account_key: primary_oracle_mapping_account_key,
            },
            primary_global_store_tx.clone(),
            primary_oracle_poll_interval_duration,
            primary_oracle_rpc_url,
            primary_oracle_updates_rx,
            logger.clone(),
        );
        let mut secondary_oracle = Oracle::new(
            oracle::Config {
                commitment: secondary_oracle_commitment,
                oracle_account_key: secondary_oracle_account_key,
                mapping_account_key: secondary_oracle_mapping_account_key,
            },
            secondary_global_store_tx.clone(),
            secondary_oracle_poll_interval_duration,
            secondary_oracle_rpc_url,
            secondary_oracle_updates_rx,
            logger.clone(),
        );

        // Create the exporters
        let mut primary_exporter = Exporter::new(
            primary_oracle_rpc_url,
            primary_exporter_max_batch_size,
            primary_exporter_fetch_current_slot_and_blockhash_interval,
            primary_exporter_staleness_threshold,
            primary_exporter_publisher_pub_key,
            primary_exporter_program_pub_key,
            primary_exporter_mapping_pub_key,
            local_store_tx.clone(),
            KeyStore::new(logger.clone()),
            primary_transaction_monitor_signature_tx,
            logger.clone(),
        );
        let mut primary_transaction_monitor = TransactionMonitor::new(
            primary_oracle_rpc_url,
            primary_transaction_monitor_poll_interval,
            primary_transaction_max_outstanding_transactions,
            primary_transaction_monitor_signature_rx,
            logger.clone(),
        );
        let mut secondary_exporter = Exporter::new(
            secondary_oracle_rpc_url,
            secondary_exporter_max_batch_size,
            secondary_exporter_fetch_current_slot_and_blockhash_interval,
            secondary_exporter_staleness_threshold,
            secondary_exporter_publisher_pub_key,
            secondary_exporter_program_pub_key,
            secondary_exporter_mapping_pub_key,
            local_store_tx.clone(),
            KeyStore::new(logger.clone()),
            secondary_transaction_monitor_signature_tx,
            logger.clone(),
        );
        let mut secondary_transaction_monitor = TransactionMonitor::new(
            secondary_oracle_rpc_url,
            secondary_transaction_monitor_poll_interval,
            secondary_transaction_max_outstanding_transactions,
            secondary_transaction_monitor_signature_rx,
            logger.clone(),
        );

        // Create the global stores
        let mut primary_global_store = global::Store::new(
            primary_global_store_rx,
            Some(adapter_tx.clone()),
            logger.clone(),
        );
        let mut secondary_global_store =
            global::Store::new(secondary_global_store_rx, None, logger.clone());

        // Create the local store
        let mut local_store = local::Store::new(local_store_rx, logger.clone());

        // Create the pythd adapter
        let mut pythd_adapter = Adapter::new(
            adapter_rx,
            adapter_notify_price_sched_interval,
            local_store_tx,
            primary_global_store_tx,
            Some(secondary_global_store_tx),
            logger.clone(),
        );

        // Create the pythd API server
        let pythd_api_server = Server::new(
            adapter_tx,
            pythd::api::rpc::Config::default(),
            logger.clone(),
        );

        // Spawn the tasks
        let primary_subscriber_jh =
            tokio::spawn(async move { primary_oracle_subscriber.run().await });
        let secondary_subscriber_jh =
            tokio::spawn(async move { secondary_oracle_subscriber.run().await });
        let primary_oracle_jh = tokio::spawn(async move { primary_oracle.run().await });
        let secondary_oracle_jh = tokio::spawn(async move { secondary_oracle.run().await });
        let primary_global_store_jh = tokio::spawn(async move { primary_global_store.run().await });
        let secondary_global_store_jh =
            tokio::spawn(async move { secondary_global_store.run().await });
        let local_store_jh = tokio::spawn(async move { local_store.run().await });
        let primary_transaction_monitor_jh =
            tokio::spawn(async move { primary_transaction_monitor.run().await });
        let secondary_transaction_monitor_jh =
            tokio::spawn(async move { secondary_transaction_monitor.run().await });
        let primary_exporter_jh = tokio::spawn(async move { primary_exporter.run().await });
        let secondary_exporter_jh = tokio::spawn(async move { secondary_exporter.run().await });
        let pythd_adapter_jh = tokio::spawn(async move { pythd_adapter.run().await });
        let pythd_api_server = tokio::spawn(async move { pythd_api_server.run(shutdown_rx).await });

        // Broadcast the shutdown to all components after the shutdown signal has been received
        if let Err(err) = signal::ctrl_c().await {
            error!(logger, "{:#}", err; "error" => format!("{:?}", err));
        }
        let _ = shutdown_tx.send(());

        // Wait for all the tasks to finish
        join_all(vec![
            primary_subscriber_jh,
            secondary_subscriber_jh,
            primary_oracle_jh,
            secondary_oracle_jh,
            primary_global_store_jh,
            secondary_global_store_jh,
            local_store_jh,
            primary_transaction_monitor_jh,
            secondary_transaction_monitor_jh,
            primary_exporter_jh,
            secondary_exporter_jh,
            pythd_adapter_jh,
            pythd_api_server,
        ])
        .await;
        Ok(())
    }
}
