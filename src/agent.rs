/* ###################################################### System Architecture #######################################################

+-----------------------------+      +----------------------------+
|         Primary Network     |      |      Secondary Network     |
|                             |      |                            |
|    +--------------------+   |      |   +--------------------+   |
|    |     RPC Node       |   |      |   |     RPC Node       |   |
|    +--------------------+   |      |   +--------------------+   |
|       |              ^      |      |      ^              |      |
|       |              |      |      |      |              |      |
|       v              |      |      |      |              v      |
|  +----------+  +----------+ |      | +----------+  +----------+ |
|  |  Oracle  |  | Exporter | |      | | Exporter |  |  Oracle  | |
|  +----------+  +----------+ |      | +----------+  +----------+ |         +---------------------------------+
|       |              ^      |      |      ^              |      |         |                                 |
+-------|--------------|------+      +------|--------------|------+         |       Pythd Websocket API       |       +--------+
        |              |                    |              |                |                                 |       |        |
        |        +--------------------------------+        |                |   +---------+    +----------+   |       |        |
        |        |          Local Store           |<----------------------------|         |<---|          |<----------|        |
        |        +--------------------------------+        |                |   |         |    |   JRPC   |   |       |        |
        v                                                  v                |   | Adapter |    |    WS    |   |       |  User  |
    +------------------------------------------------------------+          |   |         |    |  Server  |   |       |        |
    |                      Global Store                          |------------->|         |--->|          |---------->|        |
    +------------------------------------------------------------+          |   +---------+    +----------+   |       |        |
                                                                            |                                 |       |        |
                                                                            +---------------------------------+       +--------+

The arrows on the diagram above represent the direction of data flow.

Write path:
- The user submits fresh price data to the system using the Pythd JRPC Websocket API.
- The Adapter then transforms this into the Pyth SDK data structures and sends it to the Local Store.
- The Local Store holds the latest price data the user has submitted for each price feed.
- The Exporters periodically query the Local Store for the latest user-submitted data,
  and send it to the RPC node.

Read path:
- The Oracles continually fetch data from the RPC node, and pass this to the Global Store.
- The Global Store holds a unified view of the latest observed data from both networks, in the Pyth SDK data structures.
- When a user queries for this data using the Pythd JRPC Websocket API, the Adapter fetches
  the latest data from the Global Store. It transforms this from the Pyth SDK data structures into the
  Pythd JRPC Websocket API data structures.
- The Pythd JRPC Websocket API then sends this data to the user.

Note that there is an Oracle and Exporter for each network, but only one Local Store and Global Store.

################################################################################################################################## */

pub mod pythd;
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
        if let Err(err) = self.spawn(logger.clone()).await {
            error!(logger, "{:#}", err; "error" => format!("{:?}", err));
        };
    }

    async fn spawn(&self, logger: Logger) -> Result<()> {
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

        // Spawn the primary network
        jhs.extend(network::spawn_network(
            self.config.primary_network.clone(),
            local_store_tx.clone(),
            primary_oracle_updates_tx,
            logger.clone(),
        )?);

        // Spawn the secondary network, if needed
        if let Some(config) = &self.config.secondary_network {
            jhs.extend(network::spawn_network(
                config.clone(),
                local_store_tx.clone(),
                secondary_oracle_updates_tx,
                logger.clone(),
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
            global_store_lookup_tx,
            local_store_tx,
            shutdown_tx.subscribe(),
            logger.clone(),
        ));

        // Spawn the Pythd API Server
        jhs.push(rpc::spawn_server(
            self.config.pythd_api_server.clone(),
            pythd_adapter_tx,
            shutdown_rx,
            logger,
        ));

        // Wait for all tasks to complete
        join_all(jhs).await;

        Ok(())
    }
}

pub mod config {
    use {
        super::{
            pythd,
            solana::network,
        },
        anyhow::{
            anyhow,
            Result,
        },
        config as config_rs,
        config_rs::{
            Environment,
            File,
        },
        serde::Deserialize,
        std::path::Path,
    };

    /// Configuration for all components of the Agent
    #[derive(Default, Deserialize, Debug)]
    #[serde(default)]
    pub struct Config {
        pub channel_capacities: ChannelCapacities,
        pub primary_network:    network::Config,
        pub secondary_network:  Option<network::Config>,
        pub pythd_adapter:      pythd::adapter::Config,
        pub pythd_api_server:   pythd::api::rpc::Config,
    }

    impl Config {
        pub fn new(config_file: impl AsRef<Path>) -> Result<Self> {
            // Build a new configuration object, allowing the default values to be
            // overridden by those in the config_file or "AGENT_"-prefixed environment
            // variables.
            config_rs::Config::builder()
                .add_source(File::with_name(
                    config_file
                        .as_ref()
                        .to_str()
                        .ok_or_else(|| anyhow!("invalid path to config file"))?,
                ))
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
            }
        }
    }
}
