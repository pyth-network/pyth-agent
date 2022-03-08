use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use schnorrkel::PublicKey;
use slog::Logger;
use tokio::sync::mpsc::Receiver;

pub type PublisherIdentifier = PublicKey;
pub type DataFeedIdentifier = pyth_sdk::Identifier;
pub type Online = i64;
pub type Price = i64;
pub type Conf = u64;

pub struct Prover {
    // Incomming message channel
    rx: Receiver<Message>,

    // Cache of the latest data available to go into each proof
    data: HashMap<CacheKey, Data>,

    // Staleness threshold for data to be included in the proof
    staleness_threshold: Duration,

    // Circom proof generator
    proof_generator: circom::ProofGenerator,

    logger: Logger,
}

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CacheKey {
    publisher_identifier: PublisherIdentifier,
    data_feed_identifier: DataFeedIdentifier,
}

#[derive(Copy, Clone, Debug)]
pub struct Data {
    // The publishers metadata
    pub publisher_pub_key: PublicKey,

    // The price data
    pub data_feed_identifier: DataFeedIdentifier,
    pub price: Price,
    pub conf: Conf,
    pub timestamp: DateTime<Utc>,

    // An estimate of the number of publishers online
    pub online: Online,
}

impl From<Data> for CacheKey {
    fn from(data: Data) -> Self {
        CacheKey {
            publisher_identifier: data.publisher_pub_key,
            data_feed_identifier: data.data_feed_identifier,
        }
    }
}

#[derive(Debug)]
pub enum Message {
    // New data (from the gossip network)
    Data {
        data: Data,
    },
    // Request for a new proof to be generated for the given data feed
    ProofRequest {
        data_feed_identifier: DataFeedIdentifier,
    },
}

impl Prover {
    pub fn new(
        rx: Receiver<Message>,
        staleness_threshold: Duration,
        proof_generator: circom::ProofGenerator,
        logger: Logger,
    ) -> Self {
        Prover {
            rx,
            data: HashMap::default(),
            staleness_threshold,
            proof_generator,
            logger,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Some(message) = self.rx.recv().await {
            match message {
                Message::Data { data } => {
                    self.data.insert(data.into(), data);
                }
                Message::ProofRequest {
                    data_feed_identifier,
                } => {
                    let proof = self.generate_proof(data_feed_identifier).await?;
                    info!(self.logger, "generated proof"; "data_feed" => data_feed_identifier.to_string())
                }
            }
        }

        Ok(())
    }

    async fn generate_proof(
        &self,
        data_feed_identifier: DataFeedIdentifier,
    ) -> Result<circom::Proof> {
        let data = self
            .data
            .values()
            .filter(|data| (Utc::now() - data.timestamp) < self.staleness_threshold)
            .filter(|data| data.data_feed_identifier == data_feed_identifier);

        self.proof_generator
            .generate_proof(data.cloned().collect())
            .await
    }
}

pub mod circom {

    use super::Data;
    use anyhow::Result;

    #[derive(Debug)]
    pub struct Proof;

    type Price = super::Price;
    type Conf = super::Conf;
    type Vote = Price;
    type Online = i64;

    struct PriceModel {
        votes: Vec<Vote>,
    }

    // struct Signature {

    // }

    // struct Signatures {
    //     A: Vec<
    // }

    struct ProofInput {
        price_model: PriceModel,
        prices: Vec<Price>,
        confs: Vec<Conf>,
        online: Vec<Online>,
        publisher_count: i64,

        // Signatures
        // sig_a
    }

    impl From<Vec<Data>> for ProofInput {
        fn from(data: Vec<Data>) -> Self {
            let mut votes = Vec::new();
            let mut prices = Vec::new();
            let mut confs = Vec::new();
            let mut online = Vec::new();

            // Add each item of data to the proof input
            for item in &data {
                // Calculate the votes this item contributes
                let upper = item.price + (item.conf as i64);
                let original = item.price;
                let lower = item.price - (item.conf as i64);
                votes.push(upper);
                votes.push(original);
                votes.push(lower);

                // Add the price and conf values
                prices.push(item.price);
                confs.push(item.conf);

                // Add the observed number of publishers online
                online.push(item.online);
            }

            // TODO: sort the votes
            // TODO: correct representation of proof model

            ProofInput {
                price_model: PriceModel { votes },
                prices,
                confs,
                online,
                publisher_count: data.len() as i64,
            }
        }
    }

    pub struct ProofGenerator;

    impl ProofGenerator {
        pub fn new() -> Self {
            todo!();
        }

        pub async fn generate_proof(&self, data: Vec<Data>) -> Result<Proof> {
            // Proof generation:
            // - Compile the circuit to get a low-level representation in r1cs
            //   - Create a script which
            // - Compute the witness (input.json)
            // - Assign this witness to the prover

            // Convert the input data into the proof input format

            // TODO: actual proof generation

            todo!();
        }
    }
}
