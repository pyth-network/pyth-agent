use {
    crate::agent::state,
    anyhow::{
        Result,
        anyhow,
        bail,
    },
    backoff::{
        ExponentialBackoffBuilder,
        backoff::Backoff,
    },
    futures_util::{
        SinkExt,
        stream::{
            SplitSink,
            SplitStream,
            StreamExt,
        },
    },
    http::HeaderValue,
    protobuf::Message as ProtobufMessage,
    pyth_lazer_publisher_sdk::transaction::SignedLazerTransaction,
    reqwest::Client,
    serde::Deserialize,
    std::{
        path::PathBuf,
        sync::Arc,
        time::{
            Duration,
            Instant,
        },
    },
    tokio::{
        net::TcpStream,
        select,
        sync::broadcast,
        task::JoinHandle,
    },
    tokio_tungstenite::{
        MaybeTlsStream,
        WebSocketStream,
        connect_async_with_config,
        tungstenite::{
            Message as TungsteniteMessage,
            client::IntoClientRequest,
        },
    },
    tracing::{
        self,
        instrument,
    },
    url::Url,
};

pub const RELAYER_CHANNEL_CAPACITY: usize = 1000;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub history_url:               Url,
    pub relayer_urls:              Vec<Url>,
    pub authorization_token:       String,
    pub publish_keypair_path:      PathBuf,
    #[serde(with = "humantime_serde", default = "default_publish_interval")]
    pub publish_interval_duration: Duration,
}

fn default_publish_interval() -> Duration {
    Duration::from_millis(200)
}

struct RelayerWsSession {
    ws_sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>,
}

impl RelayerWsSession {
    async fn send_transaction(
        &mut self,
        signed_lazer_transaction: &SignedLazerTransaction,
    ) -> Result<()> {
        tracing::debug!("signed_lazer_transaction: {:?}", signed_lazer_transaction);
        let buf = signed_lazer_transaction.write_to_bytes()?;
        self.ws_sender
            .send(TungsteniteMessage::from(buf.clone()))
            .await?;
        self.ws_sender.flush().await?;
        Ok(())
    }
}

async fn connect_to_relayer(
    mut url: Url,
    token: &str,
) -> Result<(
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
)> {
    tracing::info!("connecting to the relayer at {}", url);
    url.set_path("/v1/transaction");
    let mut req = url.clone().into_client_request()?;
    let headers = req.headers_mut();
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))?,
    );
    let (ws_stream, _) = connect_async_with_config(req, None, true).await?;
    Ok(ws_stream.split())
}

struct RelayerSessionTask {
    // connection state
    url:      Url,
    token:    String,
    receiver: broadcast::Receiver<SignedLazerTransaction>,
}

impl RelayerSessionTask {
    pub async fn run(&mut self) {
        let initial_interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(5);
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(initial_interval)
            .with_max_interval(max_interval)
            .with_max_elapsed_time(None)
            .build();

        const FAILURE_RESET_TIME: Duration = Duration::from_secs(300);
        let mut first_failure_time = Instant::now();
        let mut failure_count = 0;

        loop {
            match self.run_relayer_connection().await {
                Ok(()) => {
                    tracing::info!("relayer session graceful shutdown");
                    return;
                }
                Err(e) => {
                    if first_failure_time.elapsed() > FAILURE_RESET_TIME {
                        failure_count = 0;
                        first_failure_time = Instant::now();
                        backoff.reset();
                    }

                    failure_count += 1;
                    let next_backoff = backoff.next_backoff().unwrap_or(max_interval);
                    tracing::error!(
                        "relayer session failed with error: {:?}, failure_count: {}; retrying in {:?}",
                        e,
                        failure_count,
                        next_backoff
                    );
                    tokio::time::sleep(next_backoff).await;
                }
            }
        }
    }

    pub async fn run_relayer_connection(&mut self) -> Result<()> {
        // Establish relayer connection
        // Relayer will drop the connection if no data received in 5s
        let (relayer_ws_sender, mut relayer_ws_receiver) =
            connect_to_relayer(self.url.clone(), &self.token).await?;
        let mut relayer_ws_session = RelayerWsSession {
            ws_sender: relayer_ws_sender,
        };

        loop {
            select! {
                recv_result = self.receiver.recv() => {
                    match recv_result {
                        Ok(transaction) => {
                            if let Err(e) = relayer_ws_session.send_transaction(&transaction).await {
                                tracing::error!("Error publishing transaction to Lazer relayer: {e:?}");
                                bail!("Failed to publish transaction to Lazer relayer: {e:?}");
                            }
                        },
                        Err(e) => {
                            match e {
                                broadcast::error::RecvError::Closed => {
                                    tracing::error!("transaction broadcast channel closed");
                                    bail!("transaction broadcast channel closed");
                                }
                                broadcast::error::RecvError::Lagged(skipped_count) => {
                                    tracing::warn!("transaction broadcast channel lagged by {skipped_count} messages");
                                }
                            }
                        }
                    }
                }
                // Handle messages from the relayers, such as errors if we send a bad update
                msg = relayer_ws_receiver.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            tracing::debug!("Received message from relayer: {msg:?}");
                        }
                        Some(Err(e)) => {
                            tracing::error!("Error receiving message from at relayer: {e:?}");
                        }
                        None => {
                            tracing::error!("relayer connection closed");
                            bail!("relayer connection closed");
                        }
                    }
                }
            }
        }
    }
}

// TODO: This is copied from history-service; move to Lazer protocol sdk.
#[derive(Deserialize)]
struct SymbolResponse {
    pub pyth_lazer_id:   u32,
    #[serde(rename = "name")]
    pub _name:           String,
    #[serde(rename = "symbol")]
    pub _symbol:         String,
    #[serde(rename = "description")]
    pub _description:    String,
    #[serde(rename = "asset_type")]
    pub _asset_type:     String,
    #[serde(rename = "exponent")]
    pub _exponent:       i16,
    #[serde(rename = "cmc_id")]
    pub _cmc_id:         Option<u32>,
    #[serde(rename = "interval")]
    pub _interval:       Option<String>,
    #[serde(rename = "min_publishers")]
    pub _min_publishers: u16,
    #[serde(rename = "min_channel")]
    pub _min_channel:    String,
    #[serde(rename = "state")]
    pub _state:          String,
    #[serde(rename = "schedule")]
    pub _schedule:       String,
    pub hermes_id:       Option<String>,
}

async fn fetch_symbols(history_url: &Url) -> Result<Vec<SymbolResponse>> {
    let mut url = history_url.clone();
    url.set_scheme("http").map_err(|_| anyhow!("invalid url"))?;
    url.set_path("/history/v1/symbols");
    let client = Client::new();
    let response = client.get(url).send().await?.error_for_status()?;
    let data = response.json().await?;
    Ok(data)
}

#[instrument(skip(config, state))]
pub fn lazer_exporter(config: Config, state: Arc<state::State>) -> Vec<JoinHandle<()>> {
    let mut handles = vec![];

    // can safely drop first receiver for ease of iteration
    let (relayer_sender, _) = broadcast::channel(RELAYER_CHANNEL_CAPACITY);

    for url in config.relayer_urls.iter() {
        let mut task = RelayerSessionTask {
            url:      url.clone(),
            token:    config.authorization_token.to_owned(),
            receiver: relayer_sender.subscribe(),
        };
        handles.push(tokio::spawn(async move { task.run().await }));
    }

    handles.push(tokio::spawn(lazer_exporter::lazer_exporter(
        config.clone(),
        state,
        relayer_sender,
    )));

    handles
}

#[allow(clippy::module_inception)]
mod lazer_exporter {
    use {
        crate::agent::{
            services::lazer_exporter::{
                Config,
                SymbolResponse,
                fetch_symbols,
            },
            state::local::LocalStore,
        },
        anyhow::{
            Context,
            Result,
            bail,
        },
        ed25519_dalek::{
            Signer,
            SigningKey,
        },
        protobuf::{
            Message,
            MessageField,
            well_known_types::timestamp::Timestamp,
        },
        pyth_lazer_publisher_sdk::{
            publisher_update::{
                FeedUpdate,
                PriceUpdate,
                PublisherUpdate,
                feed_update::Update,
            },
            transaction::{
                Ed25519SignatureData,
                LazerTransaction,
                SignatureData,
                SignedLazerTransaction,
                lazer_transaction::Payload,
                signature_data::Data::Ed25519,
            },
        },
        solana_sdk::signer::keypair,
        std::{
            collections::HashMap,
            sync::Arc,
        },
        tokio::sync::broadcast::Sender,
    };

    fn get_signing_key(config: &Config) -> Result<SigningKey> {
        // Read the keypair from the file using Solana SDK because it's the same key used by the Pythnet publisher
        let publish_keypair = match keypair::read_keypair_file(&config.publish_keypair_path) {
            Ok(k) => k,
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    publish_keypair_path = config.publish_keypair_path.display().to_string(),
                    "Reading publish keypair returned an error. ",
                );
                bail!("Reading publish keypair returned an error. ");
            }
        };

        SigningKey::from_keypair_bytes(&publish_keypair.to_bytes())
            .context("Failed to create signing key from keypair")
    }

    pub async fn lazer_exporter<S>(
        config: Config,
        state: Arc<S>,
        relayer_sender: Sender<SignedLazerTransaction>,
    ) where
        S: LocalStore,
        S: Send + Sync + 'static,
    {
        let signing_key = match get_signing_key(&config) {
            Ok(signing_key) => signing_key,
            Err(e) => {
                tracing::error!("lazer_exporter signing key failure: {e:?}");
                return;
            }
        };

        // TODO: Re-fetch on an interval?
        let lazer_symbols: HashMap<pyth_sdk::Identifier, SymbolResponse> =
            match fetch_symbols(&config.history_url).await {
                Ok(symbols) => symbols
                    .into_iter()
                    .filter_map(|symbol| {
                        let hermes_id = symbol.hermes_id.clone()?;
                        match pyth_sdk::Identifier::from_hex(hermes_id.clone()) {
                            Ok(id) => Some((id, symbol)),
                            Err(e) => {
                                tracing::warn!("Failed to parse hermes_id {}: {e:?}", hermes_id);
                                None
                            }
                        }
                    })
                    .collect(),
                Err(e) => {
                    tracing::error!("Failed to fetch Lazer symbols: {e:?}");
                    return;
                }
            };

        let mut publish_interval = tokio::time::interval(config.publish_interval_duration);

        loop {
            tokio::select! {
                _ = publish_interval.tick() => {
                    let publisher_timestamp = MessageField::some(Timestamp::now());
                    let mut publisher_update = PublisherUpdate {
                        updates: vec![],
                        publisher_timestamp,
                        special_fields: Default::default(),
                    };
                    let updates = &mut publisher_update.updates;

                    // TODO: This read locks and clones local::Store::prices, which may not meet performance needs.
                    for (identifier, price_info) in state.get_all_price_infos().await {
                        if let Some(symbol) = lazer_symbols.get(&identifier) {
                            let source_timestamp_micros = price_info.timestamp.and_utc().timestamp_micros();
                            let source_timestamp = MessageField::some(Timestamp {
                                seconds: source_timestamp_micros / 1_000_000,
                                nanos: (source_timestamp_micros % 1_000_000 * 1000) as i32,
                                special_fields: Default::default(),
                            });
                            updates.push(FeedUpdate {
                                feed_id: Some(symbol.pyth_lazer_id),
                                source_timestamp,
                                update: Some(Update::PriceUpdate(PriceUpdate {
                                    price: Some(price_info.price),
                                    ..PriceUpdate::default()
                                })),
                                special_fields: Default::default(),
                            })
                        }
                    }

                    if publisher_update.updates.is_empty() {
                        // nothing to publish
                        continue;
                    }

                    let lazer_transaction = LazerTransaction {
                        payload: Some(Payload::PublisherUpdate(publisher_update)),
                        special_fields: Default::default(),
                    };
                    let buf = match lazer_transaction.write_to_bytes() {
                        Ok(buf) => buf,
                        Err(e) => {
                            tracing::warn!("Failed to encode Lazer transaction to bytes: {:?}", e);
                            continue;
                        }
                    };
                    let signature = signing_key.sign(&buf);
                    let signature_data = SignatureData {
                        data: Some(Ed25519(Ed25519SignatureData {
                            signature: Some(signature.to_bytes().into()),
                            public_key: Some(signing_key.verifying_key().to_bytes().into()),
                            special_fields: Default::default(),
                        })),
                        special_fields: Default::default(),
                    };
                    let signed_lazer_transaction = SignedLazerTransaction {
                        signature_data: MessageField::some(signature_data),
                        payload: Some(buf),
                        special_fields: Default::default(),
                    };
                    match relayer_sender.send(signed_lazer_transaction.clone()) {
                        Ok(_) => (),
                        Err(e) => {
                            tracing::error!("Error sending transaction to relayer receivers: {e}");
                        }
                    }
                }
            }
        }
    }
}
