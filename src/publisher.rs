pub mod pythd;
pub mod solana;
pub mod store;
use {
    self::{
        pythd::api::rpc,
        solana::{
            exporter,
            oracle,
        },
    },
    anyhow::Result,
    futures_util::future::join_all,
    slog::{
        Drain,
        Logger,
    },
    tokio::sync::{
        broadcast,
        mpsc,
    },
};

pub struct Publisher {}

impl Publisher {
    pub async fn start(&self) {
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

        if let Err(err) = self.spawn(logger.clone()).await {
            error!(logger, "{:#}", err; "error" => format!("{:?}", err));
        };
    }

    async fn spawn(&self, logger: Logger) -> Result<()> {
        // TODO: proper configuration. Currently we just use the derived default values.

        // Create the channels
        // TODO: make size of all channels configurable
        // TODO: make all components listen to shutdown signal
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1000);
        let (primary_oracle_updates_tx, primary_oracle_updates_rx) = mpsc::channel(1000);
        let (secondary_oracle_updates_tx, secondary_oracle_updates_rx) = mpsc::channel(1000);
        let (global_store_lookup_tx, global_store_lookup_rx) = mpsc::channel(1000);
        let (local_store_tx, local_store_rx) = mpsc::channel(1000);
        let (pythd_adapter_tx, pythd_adapter_rx) = mpsc::channel(1000);

        // Spawn the Oracles
        // TODO: possibly encapsulate each network (group the oracle and exporters together)
        let primary_oracle_jhs = oracle::spawn_oracle(
            Default::default(),
            primary_oracle_updates_tx,
            logger.clone(),
        );
        let secondary_oracle_jhs = oracle::spawn_oracle(
            Default::default(),
            secondary_oracle_updates_tx,
            logger.clone(),
        );

        // Spawn the Exporters
        let primary_exporter_jhs =
            exporter::spawn_exporter(Default::default(), local_store_tx.clone(), logger.clone())?;
        let secondary_exporter_jhs =
            exporter::spawn_exporter(Default::default(), local_store_tx.clone(), logger.clone())?;

        // Spawn the Global Store
        let global_store_jh = store::global::spawn_store(
            global_store_lookup_rx,
            primary_oracle_updates_rx,
            secondary_oracle_updates_rx,
            pythd_adapter_tx.clone(),
            logger.clone(),
        );

        // Spawn the Local Store
        let local_store_jh = store::local::spawn_store(local_store_rx, logger.clone());

        // Spawn the Pythd Adapter
        let adapter_jh = pythd::adapter::spawn_adapter(
            Default::default(),
            pythd_adapter_rx,
            global_store_lookup_tx,
            local_store_tx,
            shutdown_tx.subscribe(),
            logger.clone(),
        );

        // Spawn the Pythd API Server
        let pythd_api_server_jh =
            rpc::spawn_server(Default::default(), pythd_adapter_tx, shutdown_rx, logger);

        // Wait for all tasks to complete
        join_all(
            vec![
                primary_oracle_jhs,
                primary_exporter_jhs,
                secondary_oracle_jhs,
                secondary_exporter_jhs,
                vec![
                    global_store_jh,
                    local_store_jh,
                    adapter_jh,
                    pythd_api_server_jh,
                ],
            ]
            .into_iter()
            .flatten(),
        )
        .await;

        Ok(())
    }
}
