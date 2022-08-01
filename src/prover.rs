use std::{collections::HashMap, fmt};

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
pub type Fee = i64;

pub struct Prover {
    // Incomming message channel
    rx: Receiver<Message>,

    // Cache of the latest data available to go into each proof
    data: HashMap<CacheKey, Data>,

    // Staleness threshold for data to be included in the proof
    staleness_threshold: Duration,

    // Circom proof generator
    proof_generator: circom::ProofGenerator,

    // Fee this prover charges per proof
    fee: Fee,

    logger: Logger,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
        fee: Fee,
        logger: Logger,
    ) -> Self {
        Prover {
            rx,
            data: HashMap::default(),
            staleness_threshold,
            proof_generator,
            fee,
            logger,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(self.logger, "starting prover node");

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
        let mut data: Vec<Data> = self
            .data
            .values()
            .filter(|data| (Utc::now() - data.timestamp) < self.staleness_threshold)
            .filter(|data| data.data_feed_identifier == data_feed_identifier)
            .cloned()
            .collect();

        // Sort the data by timestamp
        data.sort_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap());

        self.proof_generator.generate_proof(data).await
    }
}

pub mod circom {

    use super::Data;
    use anyhow::anyhow;
    use anyhow::Result;
    use pyth_sdk::UnixTimestamp;
    use serde::Serialize;
    use slog::Logger;
    use std::fmt;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tokio::process::Command;

    type Fee = super::Fee;

    #[derive(Debug)]
    // TODO: add witness to this
    pub struct Proof;

    pub struct ProofGenerator {
        fee: Fee,
        logger: Logger,
    }

    #[derive(Serialize)]
    struct InputData {
        n: usize,
        prices: Vec<super::Price>,
        confs: Vec<super::Conf>,
        timestamps: Vec<UnixTimestamp>,
        fee: Fee,
    }

    impl ProofGenerator {
        pub fn new(fee: Fee, logger: Logger) -> Self {
            ProofGenerator { fee, logger }
        }

        pub async fn generate_proof(&self, data: Vec<Data>) -> Result<Proof> {
            if data.is_empty() {
                info!(self.logger, "no data");
                return Ok(Proof {});
            }

            let mut data_debug = String::new();
            fmt::write(&mut data_debug, format_args!("{:#?}", data))?;
            debug!(self.logger, "generating circom proof"; "data" => data_debug);

            // Generate the input for the proof
            let input_json = self.generate_input(data).await?;
            debug!(self.logger, "input_json"; "data" => input_json.clone());
            let mut input_file = NamedTempFile::new()?;
            write!(input_file, "{}", input_json)?;

            // Generate the witness
            let witness_file = NamedTempFile::new()?;
            let witness_command =
                Command::new("/workspaces/agent/src/prover/circom/build/pyth_cpp/pyth")
                    .arg(self.file_path(&input_file)?)
                    .arg(self.file_path(&witness_file)?)
                    .output()
                    .await?;
            info!(self.logger, "generated witness"; "stdout" => std::str::from_utf8(&witness_command.stdout)?.to_string(), "stderr" => std::str::from_utf8(&witness_command.stderr)?.to_string());

            // Generate the proof
            let proof_file = NamedTempFile::new()?;
            let public_file = NamedTempFile::new()?;
            let proof_generate_command = Command::new("snarkjs")
                .arg("groth16")
                .arg("prove")
                .arg("/workspaces/agent/src/prover/circom/build/pyth_0001.zkey")
                .arg(self.file_path(&witness_file)?)
                .arg(self.file_path(&proof_file)?)
                .arg(self.file_path(&public_file)?)
                .output()
                .await?;
            info!(self.logger, "generated proof"; "stdout" => std::str::from_utf8(&proof_generate_command.stdout)?.to_string(), "stderr" => std::str::from_utf8(&proof_generate_command.stderr)?.to_string());

            // Generate and submit calldata
            debug!(
                self.logger,
                "generating and submitting calldata to verifier contract"
            );
            let generate_and_submit_command = &Command::new("python3")
                .arg("/workspaces/agent/src/prover/circom/generate_and_submit_calldata.py")
                .arg(self.file_path(&public_file)?)
                .arg(self.file_path(&proof_file)?)
                .output()
                .await?;
            info!(self.logger, "generated and submitted calldata"; "stdout" => std::str::from_utf8(&generate_and_submit_command.stdout)?.to_string(), "stderr" => std::str::from_utf8(&generate_and_submit_command.stderr)?.to_string());

            // TODO: return meaningful Proof object
            Ok(Proof {})
        }

        fn file_path(&self, file: &NamedTempFile) -> Result<String> {
            file.path()
                .to_str()
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow!("failed to get path of temporary public file"))
        }

        async fn generate_input(&self, data: Vec<Data>) -> Result<String> {
            debug!(self.logger, "generating input");

            let input_data = InputData {
                n: data.len(),
                prices: data.iter().map(|d| d.price).collect(),
                confs: data.iter().map(|d| d.conf).collect(),
                timestamps: data.iter().map(|d| d.timestamp.timestamp()).collect(),
                fee: self.fee,
            };

            let input_bytes = Command::new("node")
                .arg("/workspaces/agent/src/prover/circom/generate_input.js")
                .arg(serde_json::to_string(&input_data)?)
                .output()
                .await?
                .stdout;

            Ok(std::str::from_utf8(&input_bytes)?.to_string())
        }
    }
}
