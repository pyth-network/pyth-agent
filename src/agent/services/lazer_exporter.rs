use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use futures_util::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use http::HeaderValue;
use pyth_lazer_protocol::publisher::PriceFeedDataV1;
use reqwest::Client;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tracing::instrument;
use tracing;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream,
};
use tokio_util::bytes::{BufMut, BytesMut};
use url::Url;
use crate::agent::state;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub history_url: Url,
    pub relayer_urls: Vec<Url>,
    pub authorization_token: String,
    #[serde(with = "humantime_serde")]
    pub publish_interval_duration: Duration,
}

struct RelayerSender {
    ws_senders: Vec<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
}

impl RelayerSender {
    async fn send_price_update(&mut self, price_feed_data: &PriceFeedDataV1) -> Result<()> {
        tracing::debug!("price_update: {:?}", price_feed_data);
        let mut buf = BytesMut::new().writer();
        bincode::serde::encode_into_std_write(
            price_feed_data,
            &mut buf,
            bincode::config::legacy(),
        )?;
        let buf = Message::Binary(buf.into_inner().freeze());
        for sender in self.ws_senders.iter_mut() {
            sender.send(buf.clone()).await?;
            sender.flush().await?;
        }
        Ok(())
    }
}

async fn connect_to_relayer(
    url: &Url,
    token: &str,
) -> Result<(
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
)> {
    tracing::info!("connecting to the relayer at {}", url);
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
            connect_to_relayer(&url, &config.authorization_token).await?;
        relayer_senders.push(relayer_sender);
        relayer_receivers.push(relayer_receiver);
    }
    let sender = RelayerSender { ws_senders: relayer_senders };
    tracing::info!("connected to relayers: {:?}", config.relayer_urls);
    Ok((sender, relayer_receivers))
}

#[derive(Deserialize)]
struct SymbolResponse {
    pub pyth_lazer_id: u32,
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub asset_type: String,
    pub exponent: i32,
    pub cmc_id: Option<u32>,
    pub interval: Option<String>,
    pub min_publishers: u16,
    pub min_channel: String,
    pub state: String,
    pub hermes_id: Option<String>,
}

async fn fetch_symbols(history_url: &Url) -> Result<Vec<SymbolResponse>> {
    let mut url = history_url.clone();
    url.set_scheme("http").unwrap();
    url.set_path("/history/v1/symbols");
    let client = Client::new();
    let response = client
        .get(url)
        .send()
        .await?
        .text()
        .await?;
    Ok(serde_json::from_str(&response)?)
}

#[instrument(skip(config, state))]
pub fn lazer_exporter(config: Config, state: Arc<state::State>) -> Vec<JoinHandle<()>>
{
    // TODO: add loop to handle relayer failure/retry
    let mut handles = Vec::new();
    handles.push(tokio::spawn(lazer_exporter::lazer_exporter(config.clone(), state)));
    handles
}

mod lazer_exporter {
    use std::collections::HashMap;
    use std::num::NonZeroI64;
    use std::sync::Arc;
    use std::time::Duration;
    use futures_util::StreamExt;
    use pyth_lazer_protocol::publisher::PriceFeedDataV1;
    use pyth_lazer_protocol::router::{Price, PriceFeedId, TimestampUs};
    use tokio_stream::StreamMap;
    use crate::agent::services::lazer_exporter::{Config, connect_to_relayers, fetch_symbols, SymbolResponse};
    use crate::agent::state::local::LocalStore;

    pub async fn lazer_exporter<S>(config: Config, state: Arc<S>)
    where
        S: LocalStore,
        S: Send + Sync + 'static,
    {
        let mut failure_count = 0;
        let retry_duration = Duration::from_secs(1);

        loop {
            run(&config, state.clone()).await;

            failure_count += 1;
            tracing::error!("Lazer exporter failed {} times; retrying in {:?}", failure_count, retry_duration);
            tokio::time::sleep(retry_duration).await;

            // TODO: Back off or crash altogether on persistent failure
        }
    }

    async fn run<S>(config: &Config, state: Arc<S>)
    where
        S: LocalStore,
        S: Send + Sync + 'static,
    {
        // TODO: Re-fetch on an interval?
        let lazer_symbols: HashMap<String, SymbolResponse> = match fetch_symbols(&config.history_url).await {
            Ok(symbols) => symbols.into_iter().filter_map(|symbol| {
                symbol.hermes_id.clone().map(|id| (id, symbol))
            }).collect(),
            Err(e) => {
                tracing::error!("Failed to fetch Lazer symbols: {e:?}");
                return;
            }
        };

        // Establish relayer connections
        // Relayer will drop the connection if no data received in 5s
        let (mut relayer_sender, relayer_receivers) = connect_to_relayers(&config)
            .await.expect("failed to connect to relayers");
        let mut stream_map = StreamMap::new();
        for (i, receiver) in relayer_receivers.into_iter().enumerate() {
            stream_map.insert(config.relayer_urls[i].clone(), receiver);
        }

        let mut publish_interval = tokio::time::interval(config.publish_interval_duration);

        loop {
            tokio::select! {
                _ = publish_interval.tick() => {
                    for (identifier, price_info) in state.get_all_price_infos().await {
                        if let Some(symbol) = lazer_symbols.get(&identifier.to_string()) {
                            if let Err(e) = relayer_sender.send_price_update(&PriceFeedDataV1 {
                                price: Some(Price(NonZeroI64::try_from(price_info.price).unwrap())),
                                best_ask_price: None,
                                best_bid_price: None,
                                price_feed_id: PriceFeedId(symbol.pyth_lazer_id),
                                publisher_timestamp_us: TimestampUs::now(),
                                source_timestamp_us: TimestampUs(price_info.timestamp.and_utc().timestamp_micros() as u64),
                            }).await {
                                tracing::error!("Error sending price update to relayer: {e:?}");
                                return;
                            }
                        }
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
                            // TODO: Probably still appropriate to return here, but retry in caller.
                            tracing::error!("relayer connection closed");
                            return;
                        }
                    }
                }
            }
        }
    }
}
