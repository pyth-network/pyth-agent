use anyhow::Result;
use chrono::Utc;
use futures_util::future::join_all;
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
    prover_staleness_threshold: chrono::Duration,
    gossip_node_count: usize,
    gossip_send_interval: tokio::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            publisher_node_count: 50,
            data_feed_count: 50,
            prover_node_count: 2,
            prover_rx_capacity: 100,
            prover_staleness_threshold: chrono::Duration::seconds(10),
            gossip_node_count: 2,
            gossip_send_interval: tokio::time::Duration::from_secs(10),
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
        // Generate fake keypairs for all the publishers
        let publisher_keypairs = self.generate_publisher_keypairs();

        // Generate fake data feed identifiers
        let data_feeds = self.generate_data_feed_identifiers();

        // Spawn the prover node tasks
        let (prover_txs, prover_node_jhs) = self.spawn_prover_nodes();

        // Spawn the gossip node tasks
        let gossip_node_jhs = self.spawn_gossip_nodes(prover_txs, publisher_keypairs, data_feeds);

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
                ProofGenerator::new(),
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
            );

            // Run the Gossip node in a new Tokio task
            gossip_node_jhs.push(tokio::spawn(async move { gossip_node.run().await }));
        }

        gossip_node_jhs
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
}

impl SimulatedGossipNode {
    fn new(
        prover_txs: Vec<Sender<Message>>,
        publishers: Vec<PublisherIdentifier>,
        data_feeds: Vec<DataFeedIdentifier>,
        send_interval: tokio::time::Duration,
    ) -> Self {
        SimulatedGossipNode {
            prover_txs,
            publishers,
            data_feeds,
            send_interval,
        }
    }

    async fn run(&mut self) -> Result<()> {
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
            price: OsRng.gen_range(0..10),
            conf: OsRng.gen_range(0..10),
            timestamp: Utc::now(),

            // For now, all nodes report all other nodes are online
            online: self.publishers.len() as i64,
        }
    }
}
