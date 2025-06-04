use {
    crate::agent::state,
    anyhow::{
        Result,
        anyhow,
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
        time::Duration,
    },
    tokio::{
        net::TcpStream,
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

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub history_url:               Url,
    pub relayer_urls:              Vec<Url>,
    pub publisher_id:              u32,
    pub authorization_token:       String,
    pub publish_keypair_path:      PathBuf,
    #[serde(with = "humantime_serde", default = "default_publish_interval")]
    pub publish_interval_duration: Duration,
}

fn default_publish_interval() -> Duration {
    Duration::from_millis(200)
}

struct RelayerSender {
    ws_senders: Vec<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>>,
}

impl RelayerSender {
    async fn send_price_update(
        &mut self,
        signed_lazer_transaction: &SignedLazerTransaction,
    ) -> Result<()> {
        tracing::debug!("price_update: {:?}", signed_lazer_transaction);
        let buf = signed_lazer_transaction.write_to_bytes()?;
        for sender in self.ws_senders.iter_mut() {
            sender.send(TungsteniteMessage::from(buf.clone())).await?;
            sender.flush().await?;
        }
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
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );
    let (ws_stream, _) = connect_async_with_config(req, None, true).await?;
    Ok(ws_stream.split())
}

async fn connect_to_relayers(
    config: &Config,
) -> Result<(
    RelayerSender,
    Vec<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
)> {
    let mut relayer_senders = Vec::new();
    let mut relayer_receivers = Vec::new();
    for url in config.relayer_urls.clone() {
        let (relayer_sender, relayer_receiver) =
            connect_to_relayer(url, &config.authorization_token).await?;
        relayer_senders.push(relayer_sender);
        relayer_receivers.push(relayer_receiver);
    }
    let sender = RelayerSender {
        ws_senders: relayer_senders,
    };
    tracing::info!("connected to relayers: {:?}", config.relayer_urls);
    Ok((sender, relayer_receivers))
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
    let handles = vec![tokio::spawn(lazer_exporter::lazer_exporter(
        config.clone(),
        state,
    ))];
    handles
}

#[allow(clippy::module_inception)]
mod lazer_exporter {
    use {
        crate::agent::{
            services::lazer_exporter::{
                Config,
                SymbolResponse,
                connect_to_relayers,
                fetch_symbols,
            },
            state::local::LocalStore,
        },
        anyhow::{
            Context,
            bail,
        },
        ed25519_dalek::{
            Signer,
            SigningKey,
        },
        futures_util::StreamExt,
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
            time::Duration,
        },
        tokio_stream::StreamMap,
    };

    pub async fn lazer_exporter<S>(config: Config, state: Arc<S>)
    where
        S: LocalStore,
        S: Send + Sync + 'static,
    {
        let mut failure_count = 0;
        let retry_duration = Duration::from_secs(1);

        loop {
            match run(&config, state.clone()).await {
                Ok(()) => {
                    tracing::info!("lazer_exporter graceful shutdown");
                    return;
                }
                Err(e) => {
                    failure_count += 1;
                    tracing::error!(
                        "lazer_exporter failed with error: {:?}, failure_count: {}; retrying in {:?}",
                        e,
                        failure_count,
                        retry_duration
                    );
                    tokio::time::sleep(retry_duration).await;
                }
            }
        }
    }

    async fn run<S>(config: &Config, state: Arc<S>) -> anyhow::Result<()>
    where
        S: LocalStore,
        S: Send + Sync + 'static,
    {
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
                    bail!("Failed to fetch Lazer symbols: {e:?}");
                }
            };

        // Establish relayer connections
        // Relayer will drop the connection if no data received in 5s
        let (mut relayer_sender, relayer_receivers) = connect_to_relayers(config).await?;
        let mut stream_map = StreamMap::new();
        for (i, receiver) in relayer_receivers.into_iter().enumerate() {
            stream_map.insert(config.relayer_urls[i].clone(), receiver);
        }

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

        let signing_key = SigningKey::from_keypair_bytes(&publish_keypair.to_bytes())
            .context("Failed to create signing key from keypair")?;
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
                    if let Err(e) = relayer_sender.send_price_update(&signed_lazer_transaction).await {
                        tracing::error!("Error publishing update to Lazer relayer: {e:?}");
                        bail!("Failed to publish update to Lazer relayer: {e:?}");
                    }
                }
                // Handle messages from the relayers, such as errors if we send a bad update
                mapped_msg = stream_map.next() => {
                    match mapped_msg {
                        Some((relayer_url, Ok(msg))) => {
                            tracing::debug!("Received message from relayer at {relayer_url}: {msg:?}");
                        }
                        Some((relayer_url, Err(e))) => {
                            tracing::error!("Error receiving message from at relayer {relayer_url}: {e:?}");
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
