/* ###################################################### System Architecture #######################################################

+--------------------------------+ +--------------------------------+
|     RPC Node, e.g. Pythnet     | | RPC Node, e.g. Solana Mainnet  |
+--------------------------------+ +--------------------------------+
|                     ^            ^                     |
+---|---------------------|------+ +---|---------------------|------+
|   |    Primary Network  |      | |      Secondary Network  |      |
|   v                     |      | |   |                     v      |
|  +--------+    +----------+    | |  +----------+    +--------+    |
|  | Oracle |    | Exporter |    | |  | Exporter |    | Oracle |    |
|  +--------+    +----------+    | |  +----------+    +--------+    |
|      |           ^  ^  ^       | |     ^  ^  ^          |         |
+------|-----------|--|--|-------+ +-----|--|--|----------|---------+
       |           |  |  |               |  |  |          |      +------------------------+
       |        +--|-----|---------------|-----|---+      |      |  Pythd Websocket API   |
       |        |  |         Local Store |     |   |<---------------+-------+   +------+  |
       |        +--|-----|---------------|-----|---+      |      |  |       |<--|JRPC  |  |
       v           |     |               |     |  |       v      |  |Adapter|   | WS   |  |
    +--------------------|---------------|--------|-----------+  |  |       |-->|Server|  |
    |                    |  Global Store |        |           |---->+-------+   +------+  |
    +--------------------|---------------|--------|-----------+  |               ^    |   |
                         |               |        |    |         +---------------|----|---+
                         |               |        v    v                         |    |
                      +---------------------+   +--------------+                 |    |
                      |Remote Keypair Loader|   |Metrics Server|                 |    |
                      +---------------------+   +--------------+                 |    |
                           ^                            |                        |    |
                           |                            v                        |    v
                      +-------------------------------------------------------------------+
                      |                               User                                |
                      +-------------------------------------------------------------------+
Generated with textik.com on 2023-04-03

The arrows on the diagram above represent the direction of data flow.

Publisher data write path:
- The user submits fresh price data to the system using the Pythd JRPC Websocket API.
- The Adapter then transforms this into the Pyth SDK data structures and sends it to the Local Store.
- The Local Store holds the latest price data the user has submitted for each price feed.
- The Exporters periodically query the Local Store for the latest user-submitted data,
and send it to the RPC node. They query the Global Store to get the on-chain status to dynamically
adjust the compute unit price (if enabled).

Publisher data read path:
- The Oracles continually fetch data from the RPC node, and pass this to the Global Store.
- The Global Store holds a unified view of the latest observed data from both networks, in the Pyth SDK data structures.
- When a user queries for this data using the Pythd JRPC Websocket API, the Adapter fetches
the latest data from the Global Store. It transforms this from the Pyth SDK data structures into the
Pythd JRPC Websocket API data structures.
- The Pythd JRPC Websocket API then sends this data to the user.

Remote Keypair Loading:
- If no keypair is found at startup, Exporters poll the Remote Keypair Loader for a signing keypair
- When the keypair is uploaded, it is given to the Exporters

Metrics Server:
- Every update in global and local store is reflected in the metrics
- Metrics are served using Prometheus

Note that there is an Oracle and Exporter for each network, but only one Local Store and Global Store.

################################################################################################################################## */

pub mod dashboard;
pub mod legacy_schedule;
pub mod market_schedule;
pub mod metrics;
pub mod pythd;
pub mod remote_keypair_loader;
pub mod solana;
pub mod store;
use {
    self::{
        config::Config,
        pythd::api::rpc,
        solana::network,
    },
    anyhow::Result,
    futures_util::future::join_all,
    slog::Logger,
    tokio::sync::{
        broadcast,
        mpsc,
    },
};

pub struct Agent {
    config: Config,
}

impl Agent {
    pub fn new(config: Config) -> Self {
        Agent { config }
    }

    pub async fn start(&self, logger: Logger) {
        info!(logger, "Starting {}", env!("CARGO_PKG_NAME");
              "config" => format!("{:?}", &self.config),
              "version" => env!("CARGO_PKG_VERSION"),
              "cwd" => std::env::current_dir().map(|p| format!("{}", p.display())).unwrap_or("<could not get current directory>".to_owned())
        );

        if let Err(err) = self.spawn(logger.clone()).await {
            error!(logger, "{}", err);
            debug!(logger, "error context"; "context" => format!("{:?}", err));
        };
    }

    async fn spawn(&self, logger: Logger) -> Result<()> {
        // job handles
        let mut jhs = vec![];

        // Create the channels
        // TODO: make all components listen to shutdown signal
        let (shutdown_tx, shutdown_rx) =
            broadcast::channel(self.config.channel_capacities.shutdown);
        let (primary_oracle_updates_tx, primary_oracle_updates_rx) =
            mpsc::channel(self.config.channel_capacities.primary_oracle_updates);
        let (secondary_oracle_updates_tx, secondary_oracle_updates_rx) =
            mpsc::channel(self.config.channel_capacities.secondary_oracle_updates);
        let (global_store_lookup_tx, global_store_lookup_rx) =
            mpsc::channel(self.config.channel_capacities.global_store_lookup);
        let (local_store_tx, local_store_rx) =
            mpsc::channel(self.config.channel_capacities.local_store);
        let (pythd_adapter_tx, pythd_adapter_rx) =
            mpsc::channel(self.config.channel_capacities.pythd_adapter);
        let (primary_keypair_loader_tx, primary_keypair_loader_rx) = mpsc::channel(10);
        let (secondary_keypair_loader_tx, secondary_keypair_loader_rx) = mpsc::channel(10);

        // Spawn the primary network
        jhs.extend(network::spawn_network(
            self.config.primary_network.clone(),
            network::Network::Primary,
            local_store_tx.clone(),
            global_store_lookup_tx.clone(),
            primary_oracle_updates_tx,
            primary_keypair_loader_tx,
            logger.new(o!("primary" => true)),
        )?);

        // Spawn the secondary network, if needed
        if let Some(config) = &self.config.secondary_network {
            jhs.extend(network::spawn_network(
                config.clone(),
                network::Network::Secondary,
                local_store_tx.clone(),
                global_store_lookup_tx.clone(),
                secondary_oracle_updates_tx,
                secondary_keypair_loader_tx,
                logger.new(o!("primary" => false)),
            )?);
        }

        // Spawn the Global Store
        jhs.push(store::global::spawn_store(
            global_store_lookup_rx,
            primary_oracle_updates_rx,
            secondary_oracle_updates_rx,
            pythd_adapter_tx.clone(),
            logger.clone(),
        ));

        // Spawn the Local Store
        jhs.push(store::local::spawn_store(local_store_rx, logger.clone()));

        // Spawn the Pythd Adapter
        jhs.push(pythd::adapter::spawn_adapter(
            self.config.pythd_adapter.clone(),
            pythd_adapter_rx,
            global_store_lookup_tx.clone(),
            local_store_tx.clone(),
            shutdown_tx.subscribe(),
            logger.clone(),
        ));

        // Spawn the Pythd API Server
        jhs.push(rpc::spawn_server(
            self.config.pythd_api_server.clone(),
            pythd_adapter_tx,
            shutdown_rx,
            logger.clone(),
        ));

        // Spawn the metrics server
        jhs.push(tokio::spawn(metrics::MetricsServer::spawn(
            self.config.metrics_server.bind_address,
            local_store_tx,
            global_store_lookup_tx,
            logger.clone(),
        )));

        // Spawn the remote keypair loader endpoint for both networks
        jhs.append(
            &mut remote_keypair_loader::RemoteKeypairLoader::spawn(
                primary_keypair_loader_rx,
                secondary_keypair_loader_rx,
                self.config.primary_network.rpc_url.clone(),
                self.config
                    .secondary_network
                    .as_ref()
                    .map(|c| c.rpc_url.clone()),
                self.config.remote_keypair_loader.clone(),
                logger,
            )
            .await,
        );

        // Wait for all tasks to complete
        join_all(jhs).await;

        Ok(())
    }
}

pub mod config {
    use {
        super::{
            metrics,
            pythd,
            remote_keypair_loader,
            solana::network,
        },
        anyhow::Result,
        config as config_rs,
        config_rs::{
            Environment,
            File,
        },
        serde::Deserialize,
        std::path::Path,
    };

    /// Configuration for all components of the Agent
    #[derive(Deserialize, Debug)]
    pub struct Config {
        #[serde(default)]
        pub channel_capacities:    ChannelCapacities,
        pub primary_network:       network::Config,
        pub secondary_network:     Option<network::Config>,
        #[serde(default)]
        pub pythd_adapter:         pythd::adapter::Config,
        #[serde(default)]
        pub pythd_api_server:      pythd::api::rpc::Config,
        #[serde(default)]
        pub metrics_server:        metrics::Config,
        #[serde(default)]
        pub remote_keypair_loader: remote_keypair_loader::Config,
    }

    impl Config {
        pub fn new(config_file: impl AsRef<Path>) -> Result<Self> {
            // Build a new configuration object, allowing the default values to be
            // overridden by those in the config_file or "AGENT_"-prefixed environment
            // variables.
            config_rs::Config::builder()
                .add_source(File::from(config_file.as_ref()))
                .add_source(Environment::with_prefix("agent"))
                .build()?
                .try_deserialize()
                .map_err(|e| e.into())
        }
    }

    /// Capacities of the channels top-level components use to communicate
    #[derive(Deserialize, Debug)]
    pub struct ChannelCapacities {
        /// Capacity of the channel used to broadcast shutdown events to all components
        pub shutdown:                 usize,
        /// Capacity of the channel used to send updates from the primary Oracle to the Global Store
        pub primary_oracle_updates:   usize,
        /// Capacity of the channel used to send updates from the secondary Oracle to the Global Store
        pub secondary_oracle_updates: usize,
        /// Capacity of the channel the Pythd API Adapter uses to send lookup requests to the Global Store
        pub global_store_lookup:      usize,
        /// Capacity of the channel the Pythd API Adapter uses to communicate with the Local Store
        pub local_store_lookup:       usize,
        /// Capacity of the channel on which the Local Store receives messages
        pub local_store:              usize,
        /// Capacity of the channel on which the Pythd API Adapter receives messages
        pub pythd_adapter:            usize,
        /// Capacity of the slog logging channel. Adjust this value if you see complaints about channel capacity from slog
        pub logger_buffer:            usize,
    }

    impl Default for ChannelCapacities {
        fn default() -> Self {
            Self {
                shutdown:                 10000,
                primary_oracle_updates:   10000,
                secondary_oracle_updates: 10000,
                global_store_lookup:      10000,
                local_store_lookup:       10000,
                local_store:              10000,
                pythd_adapter:            10000,
                logger_buffer:            10000,
            }
        }
    }
}
