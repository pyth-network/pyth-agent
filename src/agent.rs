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
use {
    self::{
        pyth::rpc,
        solana::network,
    },
    anyhow::Result,
    config::Config,
    futures_util::future::join_all,
    lazy_static::lazy_static,
    std::sync::Arc,
    tokio::sync::watch,
};

pub mod config;
pub mod legacy_schedule;
pub mod market_schedule;
pub mod metrics;
pub mod pyth;
pub mod services;
pub mod solana;
pub mod state;

lazy_static! {
    /// A static exit flag to indicate to running threads that we're shutting down. This is used to
    /// gracefully shut down the application.
    ///
    /// We make this global based on the fact the:
    /// - The `Sender` side does not rely on any async runtime.
    /// - Exit logic doesn't really require carefully threading this value through the app.
    /// - The `Receiver` side of a watch channel performs the detection based on if the change
    ///   happened after the subscribe, so it means all listeners should always be notified
    ///   correctly.
    pub static ref EXIT: watch::Sender<bool> = watch::channel(false).0;
}

pub struct Agent {
    config: Config,
}

impl Agent {
    pub fn new(config: Config) -> Self {
        Agent { config }
    }

    pub async fn start(&self) {
        tracing::info!(
            config = format!("{:?}", &self.config),
            version = env!("CARGO_PKG_VERSION"),
            cwd = std::env::current_dir()
                .map(|p| format!("{}", p.display()))
                .unwrap_or("<could not get current directory>".to_owned()),
            "Starting {}",
            env!("CARGO_PKG_NAME"),
        );

        if let Err(err) = self.spawn().await {
            tracing::error!(err = ?err, "Agent spawn failed.");
        };
    }

    async fn spawn(&self) -> Result<()> {
        // job handles
        let mut jhs = vec![];

        // Create the Application State.
        let state = Arc::new(state::State::new(self.config.state.clone()).await);

        // Spawn the primary network
        jhs.extend(network::spawn_network(
            self.config.primary_network.clone(),
            network::Network::Primary,
            state.clone(),
        )?);

        // Spawn the secondary network, if needed
        if let Some(config) = &self.config.secondary_network {
            jhs.extend(network::spawn_network(
                config.clone(),
                network::Network::Secondary,
                state.clone(),
            )?);
        }

        // Create the Notifier task for the Pythd RPC.
        jhs.push(tokio::spawn(services::notifier(state.clone())));

        // Spawn the Pythd API Server
        jhs.push(tokio::spawn(rpc::run(
            self.config.pythd_api_server.clone(),
            state.clone(),
        )));

        // Spawn the metrics server
        jhs.push(tokio::spawn(metrics::spawn(
            self.config.metrics_server.bind_address,
        )));

        // Spawn the remote keypair loader endpoint for both networks
        jhs.append(
            &mut services::keypairs(
                self.config.primary_network.rpc_url.clone(),
                self.config
                    .secondary_network
                    .as_ref()
                    .map(|c| c.rpc_url.clone()),
                self.config.remote_keypair_loader.clone(),
                state,
            )
            .await,
        );

        // Wait for all tasks to complete
        join_all(jhs).await;

        Ok(())
    }
}
