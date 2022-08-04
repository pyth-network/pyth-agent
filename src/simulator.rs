use anyhow::anyhow;
use anyhow::Result;
use chrono::Duration;
use chrono::Utc;
use futures_util::future::join_all;
use rand::seq::SliceRandom;
use rand::Rng;
use schnorrkel::Keypair;

use slog::Logger;
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
    time::sleep,
};

use rand::rngs::OsRng;

use super::prover::{
    circom::ProofGenerator, Data, DataFeedIdentifier, Message, Prover, PublisherIdentifier,
};

struct Config {
    publisher_node_count: usize,
    data_feed_count: usize,
    prover_node_count: usize,
    prover_rx_capacity: usize,
    prover_fee: i64,
    prover_staleness_threshold: chrono::Duration,
    gossip_node_count: usize,
    gossip_send_interval: tokio::time::Duration,
    user_node_count: usize,
    user_node_startup_delay: tokio::time::Duration,
    user_node_proof_request_interval: tokio::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            publisher_node_count: 47,
            data_feed_count: 30,
            prover_node_count: 1,
            prover_rx_capacity: 100,
            prover_fee: 35,
            prover_staleness_threshold: chrono::Duration::seconds(10),
            gossip_node_count: 1,
            gossip_send_interval: tokio::time::Duration::from_secs(10),
            user_node_count: 1,
            user_node_startup_delay: tokio::time::Duration::from_secs(5),
            user_node_proof_request_interval: tokio::time::Duration::from_secs(3600),
        }
    }
}

pub struct Simulator {
    config: Config,
    logger: Logger,
}

impl Simulator {
    pub fn new(logger: Logger) -> Self {
        Simulator {
            logger,
            config: Default::default(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(self.logger, "starting simulator");

        // Generate fake keypairs for all the publishers
        let publisher_keypairs = self.generate_publisher_keypairs();

        // Generate fake data feed identifiers
        let data_feeds = self.generate_data_feed_identifiers();

        // Spawn the prover node tasks
        let (prover_txs, prover_node_jhs) = self.spawn_prover_nodes();

        // Spawn the gossip node tasks
        let gossip_node_jhs =
            self.spawn_gossip_nodes(prover_txs.clone(), publisher_keypairs, data_feeds.clone());

        // Spawn the user tasks
        let user_node_jhs = self.spawn_user_nodes(prover_txs, data_feeds);

        // Wait for all the gossip nodes to complete
        for result in Simulator::join_all(gossip_node_jhs).await? {
            if let Some(err) = result.err() {
                error!(self.logger, "error running gossip node"; "error" => err.to_string())
            }
        }

        // Wait for all the prover nodes to complete
        for result in Simulator::join_all(prover_node_jhs).await? {
            if let Some(err) = result.err() {
                error!(self.logger, "error running prover node"; "error" => err.to_string())
            }
        }

        // Wait for all the user nodes to complete
        for result in Simulator::join_all(user_node_jhs).await? {
            if let Some(err) = result.err() {
                error!(self.logger, "error running user node"; "error" => err.to_string())
            }
        }

        Ok(())
    }

    fn generate_publisher_keypairs(&mut self) -> Vec<schnorrkel::Keypair> {
        let mut keypairs = Vec::new();
        for _ in 0..self.config.publisher_node_count {
            keypairs.push(schnorrkel::Keypair::generate_with(OsRng));
        }
        keypairs
    }

    fn generate_data_feed_identifiers(&mut self) -> Vec<pyth_sdk::Identifier> {
        let mut data_feeds = Vec::new();
        for _ in 0..self.config.data_feed_count {
            data_feeds.push(pyth_sdk::Identifier::new(OsRng.gen::<[u8; 32]>()))
        }
        data_feeds
    }

    fn spawn_prover_nodes(&mut self) -> (Vec<Sender<Message>>, Vec<JoinHandle<Result<()>>>) {
        let mut prover_txs = Vec::new();
        let mut prover_jhs = Vec::new();

        for i in 0..self.config.prover_node_count {
            // Create the channel used to send messages to the prover
            let (tx, rx) = mpsc::channel(self.config.prover_rx_capacity);
            prover_txs.push(tx);

            // Create the Prover
            let mut prover = Prover::new(
                rx,
                self.config.prover_staleness_threshold,
                ProofGenerator::new(self.config.prover_fee, self.logger.new(o!())),
                self.config.prover_fee,
                self.logger.new(o!("prover_idx" => i)),
            );

            // Run the Prover in a new Tokio task
            prover_jhs.push(tokio::spawn(async move { prover.run().await }));
        }

        (prover_txs, prover_jhs)
    }

    fn spawn_gossip_nodes(
        &mut self,
        prover_txs: Vec<Sender<Message>>,
        publisher_keypairs: Vec<Keypair>,
        data_feeds: Vec<pyth_sdk::Identifier>,
    ) -> Vec<JoinHandle<Result<()>>> {
        let mut gossip_node_jhs = Vec::new();

        for _ in 0..self.config.gossip_node_count {
            // Run the Gossip node
            let mut gossip_node = SimulatedGossipNode::new(
                prover_txs.clone(),
                publisher_keypairs
                    .iter()
                    .map(|keypair| keypair.public)
                    .collect(),
                data_feeds.clone(),
                self.config.gossip_send_interval,
                self.logger.new(o!()),
            );

            // Run the Gossip node in a new Tokio task
            gossip_node_jhs.push(tokio::spawn(async move { gossip_node.run().await }));
        }

        gossip_node_jhs
    }

    fn spawn_user_nodes(
        &mut self,
        prover_txs: Vec<Sender<Message>>,
        data_feeds: Vec<pyth_sdk::Identifier>,
    ) -> Vec<JoinHandle<Result<()>>> {
        let mut user_node_jhs = Vec::new();

        for _ in 0..self.config.user_node_count {
            // Create the user node
            let mut user_node = SimulatedUserNode::new(
                prover_txs.clone(),
                data_feeds.clone(),
                self.config.user_node_proof_request_interval,
                self.config.user_node_startup_delay,
                self.logger.new(o!()),
            );

            // Run the User node in a new Tokio task
            user_node_jhs.push(tokio::spawn(async move { user_node.run().await }));
        }

        user_node_jhs
    }

    async fn join_all(handles: Vec<JoinHandle<Result<()>>>) -> Result<Vec<Result<()>>> {
        let results = join_all(handles)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(results)
    }
}

// Represents a simulated gossip node, which will periodically notify the
// provers with newly generated data representing the data feeds.
struct SimulatedGossipNode {
    // The set of all provers in the network
    prover_txs: Vec<Sender<Message>>,

    // The set of all publishers in the network
    publishers: Vec<PublisherIdentifier>,

    // The set of all data feeds in the network
    data_feeds: Vec<DataFeedIdentifier>,

    // How often to send updates to the prover
    send_interval: tokio::time::Duration,

    logger: Logger,
}

impl SimulatedGossipNode {
    fn new(
        prover_txs: Vec<Sender<Message>>,
        publishers: Vec<PublisherIdentifier>,
        data_feeds: Vec<DataFeedIdentifier>,
        send_interval: tokio::time::Duration,
        logger: Logger,
    ) -> Self {
        SimulatedGossipNode {
            prover_txs,
            publishers,
            data_feeds,
            send_interval,
            logger,
        }
    }

    async fn run(&mut self) -> Result<()> {
        info!(self.logger, "starting gossip node");

        loop {
            // Generate a random data update for each publisher, for each data feed,
            // and send this to all the provers.
            self.generate_and_send_data().await?;

            // Sleep a while before sending generating and sending new data
            sleep(self.send_interval).await;
        }
    }

    async fn generate_and_send_data(&mut self) -> Result<()> {
        for publisher in self.publishers.clone() {
            for data_feed in self.data_feeds.clone() {
                let data = self.generate_data(publisher, data_feed);
                for prover in self.prover_txs.clone() {
                    prover.send(Message::Data { data }).await?;
                }
            }
        }

        Ok(())
    }

    fn generate_data(
        &mut self,
        publisher: schnorrkel::PublicKey,
        data_feed: DataFeedIdentifier,
    ) -> Data {
        Data {
            publisher_pub_key: publisher,
            data_feed_identifier: data_feed,
            price: OsRng.gen_range(0..4),
            conf: OsRng.gen_range(0..4),
            timestamp: Utc::now() + Duration::seconds(OsRng.gen_range(0..=2)),
        }
    }
}

struct SimulatedUserNode {
    // The set of all provers in the network
    prover_txs: Vec<Sender<Message>>,

    // The set of all data feeds in the network
    data_feeds: Vec<DataFeedIdentifier>,

    // Startup delay before sending proof requests
    startup_delay: tokio::time::Duration,

    // How often to send proof requests to the prover
    request_interval: tokio::time::Duration,

    logger: Logger,
}

impl SimulatedUserNode {
    pub fn new(
        prover_txs: Vec<Sender<Message>>,
        data_feeds: Vec<DataFeedIdentifier>,
        request_interval: tokio::time::Duration,
        startup_delay: tokio::time::Duration,
        logger: Logger,
    ) -> Self {
        SimulatedUserNode {
            prover_txs,
            data_feeds,
            request_interval,
            startup_delay,
            logger,
        }
    }

    async fn run(&mut self) -> Result<()> {
        info!(self.logger, "starting simulated user");

        sleep(self.startup_delay).await;

        loop {
            // Request proofs from a random prover
            self.request_proof().await?;

            // Sleep a while before requesting another proof
            sleep(self.request_interval).await;
        }
    }

    async fn request_proof(&self) -> Result<()> {
        debug!(self.logger, "requesting proof");

        // Choose a random prover to request a proof from
        let prover = self
            .prover_txs
            .choose(&mut OsRng)
            .ok_or_else(|| anyhow!("failed to choose random prover"))?;

        // Choose a random data feed identifier to request a proof for
        let data_feed_identifier = *self
            .data_feeds
            .choose(&mut OsRng)
            .ok_or_else(|| anyhow!("failed to choose random prover"))?;

        // Send a proof request to this prover
        prover
            .send(Message::ProofRequest {
                data_feed_identifier,
            })
            .await
            .map_err(|_| anyhow!("error sending proof request"))
    }
}
