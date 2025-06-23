use {
    crate::agent::state,
    anyhow::{
        Context,
        Result,
        bail,
    },
    backoff::{
        ExponentialBackoffBuilder,
        backoff::Backoff,
    },
    base64::{
        Engine,
        prelude::BASE64_STANDARD,
    },
    ed25519_dalek::SigningKey,
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
    serde::{
        Deserialize,
        Serialize,
    },
    solana_sdk::signature::keypair,
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
    pub history_url:                    Url,
    pub relayer_urls:                   Vec<Url>,
    pub publish_keypair_path:           PathBuf,
    #[serde(with = "humantime_serde", default = "default_publish_interval")]
    pub publish_interval_duration:      Duration,
    #[serde(with = "humantime_serde", default = "default_symbol_fetch_interval")]
    pub symbol_fetch_interval_duration: Duration,
}

fn default_publish_interval() -> Duration {
    Duration::from_millis(200)
}

fn default_symbol_fetch_interval() -> Duration {
    Duration::from_secs(60 * 60)
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
                    tracing::warn!(
                        "relayer session ended with error: {:?}, failure_count: {}; retrying in {:?}",
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
                            tracing::warn!("relayer connection closed");
                            bail!("relayer connection closed");
                        }
                    }
                }
            }
        }
    }
}

// TODO: This is copied from history-service; move to Lazer protocol sdk.
#[derive(Debug, Serialize, Deserialize)]
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
    url.set_path("/history/v1/symbols");
    let client = Client::new();
    let response = client.get(url).send().await?.error_for_status()?;
    let data = response.json().await?;
    Ok(data)
}

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

#[instrument(skip(config, state))]
pub fn lazer_exporter(config: Config, state: Arc<state::State>) -> Vec<JoinHandle<()>> {
    let mut handles = vec![];

    let signing_key = match get_signing_key(&config) {
        Ok(signing_key) => signing_key,
        Err(e) => {
            // This is fatal as we can't publish without the key.
            tracing::error!("failed to get Lazer signing key: {e:?}");
            panic!("failed to get Lazer signing key")
        }
    };
    let pubkey_base64 = BASE64_STANDARD.encode(signing_key.verifying_key().to_bytes());
    tracing::info!("Loaded Lazer signing key; pubkey in base64: {pubkey_base64}");

    // can safely drop first receiver for ease of iteration
    let (relayer_sender, _) = broadcast::channel(RELAYER_CHANNEL_CAPACITY);

    for url in config.relayer_urls.iter() {
        let mut task = RelayerSessionTask {
            url:      url.clone(),
            token:    pubkey_base64.clone(),
            receiver: relayer_sender.subscribe(),
        };
        handles.push(tokio::spawn(async move { task.run().await }));
    }

    handles.push(tokio::spawn(lazer_exporter::lazer_exporter(
        config.clone(),
        state,
        relayer_sender,
        signing_key,
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
        std::{
            collections::HashMap,
            sync::Arc,
            time::Duration,
        },
        tokio::sync::{
            broadcast::Sender,
            mpsc,
        },
        url::Url,
    };

    pub async fn lazer_exporter<S>(
        config: Config,
        state: Arc<S>,
        relayer_sender: Sender<SignedLazerTransaction>,
        signing_key: SigningKey,
    ) where
        S: LocalStore,
        S: Send + Sync + 'static,
    {
        // We can't publish to Lazer without symbols, so crash the process if it fails.
        let mut lazer_symbols = match get_lazer_symbol_map(&config.history_url).await {
            Ok(symbol_map) => {
                if symbol_map.is_empty() {
                    panic!("Retrieved zero Lazer symbols from {}", config.history_url);
                }
                symbol_map
            }
            Err(_) => {
                tracing::error!(
                    "Failed to retrieve Lazer symbols from {}",
                    config.history_url
                );
                panic!(
                    "Failed to retrieve Lazer symbols from {}",
                    config.history_url
                );
            }
        };

        tracing::info!(
            "Retrieved {} Lazer feeds with hermes symbols from symbols endpoint: {}",
            lazer_symbols.len(),
            &config.history_url
        );

        let (symbols_sender, mut symbols_receiver) = mpsc::channel(1);
        tokio::spawn(get_lazer_symbols_task(
            config.history_url.clone(),
            config.symbol_fetch_interval_duration,
            symbols_sender,
        ));

        let mut publish_interval = tokio::time::interval(config.publish_interval_duration);
        // consume immediate tick
        publish_interval.tick().await;

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
                },
                latest_symbol_map = symbols_receiver.recv() => {
                    match latest_symbol_map {
                        Some(symbol_map) => {
                            tracing::info!("Refreshing Lazer symbol map with {} symbols", symbol_map.len());
                            lazer_symbols = symbol_map
                        }
                        None => {
                            // agent can continue but will eventually have a stale symbol set unless the process is cycled.
                            tracing::error!("Lazer symbol refresh channel closed")
                        }
                    }
                },
            }
        }
    }

    async fn get_lazer_symbols_task(
        history_url: Url,
        fetch_interval_duration: Duration,
        sender: mpsc::Sender<HashMap<pyth_sdk::Identifier, SymbolResponse>>,
    ) {
        let mut symbol_fetch_interval = tokio::time::interval(fetch_interval_duration);
        // consume immediate tick
        symbol_fetch_interval.tick().await;

        loop {
            tokio::select! {
                _ = symbol_fetch_interval.tick() => {
                    tracing::info!("Refreshing Lazer symbol map from history service...");
                    match get_lazer_symbol_map(&history_url).await {
                        Ok(symbol_map) => {
                            if symbol_map.is_empty() {
                                tracing::error!("Retrieved zero Lazer symbols from {}", history_url);
                                continue;
                            }
                            match sender.send(symbol_map).await {
                                Ok(_) => (),
                                Err(e) => {
                                    // agent can continue but will eventually have a stale symbol set unless the process is cycled.
                                    tracing::error!("Error sending refreshed symbol map to exporter task: {e}");
                                }
                            }
                        },
                        Err(_) => {
                            tracing::error!("Failed to retrieve Lazer symbols from {} in refresh task", history_url);
                        }
                    }
                }
            }
        }
    }

    async fn get_lazer_symbol_map(
        history_url: &Url,
    ) -> anyhow::Result<HashMap<pyth_sdk::Identifier, SymbolResponse>> {
        const NUM_RETRIES: usize = 3;
        const RETRY_INTERVAL: Duration = Duration::from_secs(1);
        let mut retry_count = 0;

        while retry_count < NUM_RETRIES {
            match fetch_symbols(history_url).await {
                Ok(symbols) => {
                    let symbol_map = symbols
                        .into_iter()
                        .filter_map(|symbol| {
                            let hermes_id = symbol.hermes_id.clone()?;
                            match pyth_sdk::Identifier::from_hex(hermes_id.clone()) {
                                Ok(id) => Some((id, symbol)),
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to parse hermes_id {}: {e:?}",
                                        hermes_id
                                    );
                                    None
                                }
                            }
                        })
                        .collect();
                    return Ok(symbol_map);
                }
                Err(e) => {
                    tracing::error!("Failed to fetch Lazer symbols: {e:?}");

                    retry_count += 1;
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
        anyhow::bail!("Lazer symbol map fetch failed after {NUM_RETRIES} attempts");
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::agent::{
            services::lazer_exporter::{
                Config,
                RELAYER_CHANNEL_CAPACITY,
                RelayerSessionTask,
                SymbolResponse,
                lazer_exporter::lazer_exporter,
            },
            state::{
                local,
                local::{
                    LocalStore,
                    PriceInfo,
                },
            },
        },
        ed25519_dalek::{
            Signer,
            SigningKey,
        },
        futures_util::StreamExt,
        prometheus_client::registry::Registry,
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
                feed_update::{
                    self,
                    Update,
                },
            },
            transaction::{
                Ed25519SignatureData,
                LazerTransaction,
                SignatureData,
                SignedLazerTransaction,
                lazer_transaction::{
                    self,
                    Payload,
                },
                signature_data::Data::Ed25519,
            },
        },
        pyth_sdk_solana::state::PriceStatus,
        std::{
            io::Write,
            net::SocketAddr,
            path::PathBuf,
            sync::{
                Arc,
                Once,
            },
            time::Duration,
        },
        tempfile::NamedTempFile,
        tokio::{
            net::TcpListener,
            sync::{
                broadcast::{
                    self,
                    error::TryRecvError,
                },
                mpsc,
            },
        },
        url::Url,
        warp::Filter,
    };

    static INIT: Once = Once::new();

    fn init_tracing() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer() // Send output to test output
                .init();
        });
    }

    pub async fn run_mock_history_server(addr: SocketAddr) {
        let route = warp::path!("history" / "v1" / "symbols")
            .and(warp::get())
            .map(|| {
                let response = vec![SymbolResponse {
                    pyth_lazer_id:   1,
                    _name:           "BTCUSD".to_string(),
                    _symbol:         "Crypto.BTC/USD".to_string(),
                    _description:    "BITCOIN / US DOLLAR".to_string(),
                    _asset_type:     "crypto".to_string(),
                    _exponent:       -8,
                    _cmc_id:         Some(1),
                    _interval:       None,
                    _min_publishers: 1,
                    _min_channel:    "real_time".to_string(),
                    _state:          "stable".to_string(),
                    _schedule:       "America/New_York;O,O,O,O,O,O,O;".to_string(),
                    hermes_id:       Some(
                        "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
                            .to_string(),
                    ),
                }];
                warp::reply::json(&response)
            });
        warp::serve(route).run(addr).await;
    }

    fn get_private_key() -> SigningKey {
        SigningKey::from_keypair_bytes(&[
            105, 175, 146, 91, 32, 145, 164, 199, 37, 111, 139, 255, 44, 225, 5, 247, 154, 170,
            238, 70, 47, 15, 9, 48, 102, 87, 180, 50, 50, 38, 148, 243, 62, 148, 219, 72, 222, 170,
            8, 246, 176, 33, 205, 29, 118, 11, 220, 163, 214, 204, 46, 49, 132, 94, 170, 173, 244,
            39, 179, 211, 177, 70, 252, 31,
        ])
        .unwrap()
    }

    fn get_private_key_file() -> NamedTempFile {
        let private_key_string = "[105,175,146,91,32,145,164,199,37,111,139,255,44,225,5,247,154,170,238,70,47,15,9,48,102,87,180,50,50,38,148,243,62,148,219,72,222,170,8,246,176,33,205,29,118,11,220,163,214,204,46,49,132,94,170,173,244,39,179,211,177,70,252,31]";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file
            .as_file_mut()
            .write(private_key_string.as_bytes())
            .unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    #[tokio::test]
    async fn test_lazer_exporter() {
        init_tracing();

        let history_addr = "127.0.0.1:12345".parse().unwrap();
        tokio::spawn(async move {
            run_mock_history_server(history_addr).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let state = Arc::new(local::Store::new(&mut Registry::default()));
        let (relayer_sender, mut relayer_receiver) = broadcast::channel(RELAYER_CHANNEL_CAPACITY);
        let private_key_file = get_private_key_file();
        let private_key = get_private_key();

        let config = Config {
            history_url:                    Url::parse("http://127.0.0.1:12345").unwrap(),
            relayer_urls:                   vec![Url::parse("http://127.0.0.1:12346").unwrap()],
            publish_keypair_path:           PathBuf::from(private_key_file.path()),
            publish_interval_duration:      Duration::from_secs(1),
            symbol_fetch_interval_duration: Duration::from_secs(60 * 60),
        };
        tokio::spawn(lazer_exporter(
            config,
            state.clone(),
            relayer_sender,
            private_key,
        ));

        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        match relayer_receiver.try_recv() {
            Err(TryRecvError::Empty) => (),
            _ => panic!("channel should be empty"),
        }

        let btc_id = pyth_sdk::Identifier::from_hex(
            "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
        )
        .unwrap();
        let price = PriceInfo {
            status:    PriceStatus::Trading,
            price:     100_000_00000000i64,
            conf:      1_00000000u64,
            timestamp: Default::default(),
        };
        state.update(btc_id, price).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        match relayer_receiver.try_recv() {
            Ok(transaction) => {
                let lazer_transaction =
                    LazerTransaction::parse_from_bytes(transaction.payload.unwrap().as_slice())
                        .unwrap();
                let publisher_update =
                    if let lazer_transaction::Payload::PublisherUpdate(publisher_update) =
                        lazer_transaction.payload.unwrap()
                    {
                        publisher_update
                    } else {
                        panic!("expected publisher_update")
                    };
                assert_eq!(publisher_update.updates.len(), 1);
                let feed_update = &publisher_update.updates[0];
                assert_eq!(feed_update.feed_id, Some(1u32));
                let price_update = if let feed_update::Update::PriceUpdate(price_update) =
                    feed_update.clone().update.unwrap()
                {
                    price_update
                } else {
                    panic!("expected price_update")
                };
                assert_eq!(price_update.price, Some(100_000_00000000i64));
            }
            _ => panic!("channel should have a transaction waiting"),
        }
    }

    pub async fn run_mock_relayer(
        addr: SocketAddr,
        back_sender: mpsc::Sender<SignedLazerTransaction>,
    ) {
        let listener = TcpListener::bind(addr).await.unwrap();

        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                panic!("failed to accept mock relayer websocket connection");
            };
            let ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("handshake failed");
            let (_, mut read) = ws_stream.split();
            while let Some(msg) = read.next().await {
                if let Ok(msg) = msg {
                    if msg.is_binary() {
                        tracing::info!("Received binary message: {msg:?}");
                        let transaction =
                            SignedLazerTransaction::parse_from_bytes(msg.into_data().as_ref())
                                .unwrap();
                        back_sender.clone().send(transaction).await.unwrap();
                    }
                } else {
                    tracing::error!("Received a malformed message: {msg:?}");
                }
            }
        });
    }

    #[tokio::test]
    async fn test_relayer_session() {
        init_tracing();

        let (back_sender, mut back_receiver) = mpsc::channel(RELAYER_CHANNEL_CAPACITY);
        let relayer_addr = "127.0.0.1:12346".parse().unwrap();
        run_mock_relayer(relayer_addr, back_sender).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (relayer_sender, relayer_receiver) = broadcast::channel(RELAYER_CHANNEL_CAPACITY);

        let mut relayer_session_task = RelayerSessionTask {
            // connection state
            url:      Url::parse("ws://127.0.0.1:12346").unwrap(),
            token:    "token1".to_string(),
            receiver: relayer_receiver,
        };
        tokio::spawn(async move { relayer_session_task.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        let transaction = get_signed_lazer_transaction();
        relayer_sender
            .send(transaction.clone())
            .expect("relayer_sender.send failed");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        let received_transaction = back_receiver
            .recv()
            .await
            .expect("back_receiver.recv failed");
        assert_eq!(transaction, received_transaction);
    }

    fn get_signed_lazer_transaction() -> SignedLazerTransaction {
        let publisher_update = PublisherUpdate {
            updates:             vec![FeedUpdate {
                feed_id:          Some(1),
                source_timestamp: MessageField::some(Timestamp::now()),
                update:           Some(Update::PriceUpdate(PriceUpdate {
                    price: Some(1_000_000_000i64),
                    ..PriceUpdate::default()
                })),
                special_fields:   Default::default(),
            }],
            publisher_timestamp: MessageField::some(Timestamp::now()),
            special_fields:      Default::default(),
        };
        let lazer_transaction = LazerTransaction {
            payload:        Some(Payload::PublisherUpdate(publisher_update)),
            special_fields: Default::default(),
        };
        let buf = lazer_transaction.write_to_bytes().unwrap();
        let signing_key = get_private_key();
        let signature = signing_key.sign(&buf);
        let signature_data = SignatureData {
            data:           Some(Ed25519(Ed25519SignatureData {
                signature:      Some(signature.to_bytes().into()),
                public_key:     Some(signing_key.verifying_key().to_bytes().into()),
                special_fields: Default::default(),
            })),
            special_fields: Default::default(),
        };
        SignedLazerTransaction {
            signature_data: MessageField::some(signature_data),
            payload:        Some(buf),
            special_fields: Default::default(),
        }
    }
}
