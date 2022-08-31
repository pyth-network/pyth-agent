// This module is responsible for exposing the JRPC-esq websocket API
// documented at https://docs.pyth.network/publish-data/pyth-client-websocket-api
//
// It does not implement the business logic, only exposes a websocket server which
// accepts messages and can return responses in the expected format.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub type PubKey = String;
pub type Attrs = BTreeMap<String, String>;

pub type Price = i64;
pub type Exponent = i64;
pub type Conf = u64;
pub type Slot = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProductAccountMetadata {
    pub account: PubKey,
    pub attr_dict: Attrs,
    pub prices: Vec<PriceAccountMetadata>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PriceAccountMetadata {
    pub account: PubKey,
    pub price_type: String,
    pub price_exponent: Exponent,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProductAccount {
    pub account: PubKey,
    pub attr_dict: Attrs,
    pub price_accounts: Vec<PriceAccount>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PriceAccount {
    pub account: PubKey,
    pub price_type: String,
    pub price_exponent: Exponent,
    pub status: String,
    pub price: Price,
    pub conf: Conf,
    pub twap: Price,
    pub twac: Price,
    pub valid_slot: Slot,
    pub pub_slot: Slot,
    pub prev_slot: Slot,
    pub prev_price: Price,
    pub prev_conf: Conf,
    pub publisher_accounts: Vec<PublisherAccount>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PublisherAccount {
    pub account: PubKey,
    pub status: String,
    pub price: Price,
    pub conf: Conf,
    pub slot: Slot,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NotifyPrice {
    pub subscription: SubscriptionID,
    pub result: PriceUpdate,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NotifyPriceSched {
    pub subscription: SubscriptionID,
}

pub type SubscriptionID = i64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PriceUpdate {
    pub price: Price,
    pub conf: Conf,
    pub status: String,
    pub valid_slot: Slot,
    pub pub_slot: Slot,
}

pub mod rpc {

    use std::fmt::Debug;
    use std::net::SocketAddr;

    use anyhow::{anyhow, Result};
    use futures_util::stream::{SplitSink, SplitStream, StreamExt};
    use futures_util::SinkExt;
    use jrpc::{parse_request, ErrorCode, Id, IdReq, Request, Response, Value};
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};

    use slog::Logger;
    use tokio::sync::{broadcast, mpsc, oneshot};
    use warp::ws::{Message, WebSocket, Ws};
    use warp::Filter;

    use super::super::adapter;
    use super::{Conf, NotifyPrice, NotifyPriceSched, Price, PubKey, SubscriptionID};

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(rename_all = "snake_case")]
    enum Method {
        GetProductList,
        GetProduct,
        GetAllProducts,
        SubscribePrice,
        NotifyPrice,
        SubscribePriceSched,
        NotifyPriceSched,
        UpdatePrice,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct GetProductParams {
        account: PubKey,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct SubscribePriceParams {
        account: PubKey,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct SubscribePriceSchedParams {
        account: PubKey,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct UpdatePriceParams {
        account: PubKey,
        price: Price,
        conf: Conf,
        status: String,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SubscribeResult {
        subscription: SubscriptionID,
    }

    #[derive(thiserror::Error, Debug)]
    enum ConnectionError {
        #[error("websocket connection closed")]
        WebsocketConnectionClosed,
    }

    struct Connection {
        // Channel for communicating with the adapter
        adapter_tx: mpsc::Sender<adapter::Message>,

        // Channel Websocket messages are sent and received on
        ws_tx: SplitSink<WebSocket, Message>,
        ws_rx: SplitStream<WebSocket>,

        // Channel NotifyPrice events are sent and received on
        notify_price_tx: mpsc::Sender<NotifyPrice>,
        notify_price_rx: mpsc::Receiver<NotifyPrice>,

        // Channel NotifyPriceSched events are sent and received on
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
        notify_price_sched_rx: mpsc::Receiver<NotifyPriceSched>,

        logger: Logger,
    }

    impl Connection {
        fn new(
            ws_conn: WebSocket,
            adapter_tx: mpsc::Sender<adapter::Message>,
            notify_price_tx_buffer: usize,
            notify_price_sched_tx_buffer: usize,
            logger: Logger,
        ) -> Self {
            // Create the channels
            let (ws_tx, ws_rx) = ws_conn.split();
            let (notify_price_tx, notify_price_rx) = mpsc::channel(notify_price_tx_buffer);
            let (notify_price_sched_tx, notify_price_sched_rx) =
                mpsc::channel(notify_price_sched_tx_buffer);

            // Create the new connection object
            Connection {
                adapter_tx,
                ws_tx,
                ws_rx,
                notify_price_tx,
                notify_price_rx,
                notify_price_sched_tx,
                notify_price_sched_rx,
                logger,
            }
        }

        async fn consume(&mut self) {
            loop {
                if let Err(err) = self.handle_next().await {
                    if let Some(ConnectionError::WebsocketConnectionClosed) =
                        err.downcast_ref::<ConnectionError>()
                    {
                        info!(self.logger, "websocket connection closed");
                        return;
                    }

                    error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
                }
            }
        }

        async fn handle_next(&mut self) -> Result<()> {
            tokio::select! {
                msg = self.ws_rx.next() => {
                    match msg {
                        Some(body) => self.handle_ws_rx(body).await,
                        None => Err(ConnectionError::WebsocketConnectionClosed)?,
                    }
                }
                Some(notify_price) = self.notify_price_rx.recv() => {
                    self.handle_notify_price(notify_price).await
                }
                Some(notify_price_sched) = self.notify_price_sched_rx.recv() => {
                    self.handle_notify_price_sched(notify_price_sched).await
                }
            }
        }

        async fn handle_ws_rx(&mut self, body: Result<Message, warp::Error>) -> Result<()> {
            match body {
                Ok(msg) => self.handle(msg).await,
                Err(e) => self.send_error(e.into(), None).await,
            }
        }

        async fn handle_notify_price(&mut self, notify_price: NotifyPrice) -> Result<()> {
            self.send_notification(Method::NotifyPrice, Some(notify_price))
                .await
        }

        async fn handle_notify_price_sched(
            &mut self,
            notify_price_sched: NotifyPriceSched,
        ) -> Result<()> {
            self.send_notification(Method::NotifyPriceSched, Some(notify_price_sched))
                .await
        }

        async fn handle(&mut self, msg: Message) -> Result<()> {
            // Ignore control and binary messages
            if !msg.is_text() {
                return Ok(());
            }

            // Parse and dispatch the message
            match self.parse(msg).await {
                Ok(request) => self.dispatch_and_catch_error(&request).await,
                Err(e) => self.send_text(&Response::<Value>::Err(e).to_string()).await,
            }
        }

        async fn dispatch_and_catch_error(
            &mut self,
            request: &Request<Method, Value>,
        ) -> Result<()> {
            if let Err(err) = self.dispatch(request).await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err));
                self.send_error_response(err, request).await?;
            };
            Ok(())
        }

        async fn parse(
            &mut self,
            msg: Message,
        ) -> core::result::Result<Request<Method, Value>, jrpc::Error<Value>> {
            let s = msg.to_str().map_err(|()| {
                // This should never happen, but fail gracefully just in case
                jrpc::Error::new(
                    Id::from(0),
                    jrpc::ErrorCode::ParseError,
                    "non-text websocket message received",
                    None,
                )
            })?;
            parse_request::<Method>(s)
        }

        async fn dispatch(&mut self, request: &Request<Method, Value>) -> Result<()> {
            match request.method {
                Method::GetProductList => self.get_product_list(request).await,
                Method::GetProduct => self.get_product(request).await,
                Method::GetAllProducts => self.get_all_products(request).await,
                Method::SubscribePrice => self.subscribe_price(request).await,
                Method::SubscribePriceSched => self.subscribe_price_sched(request).await,
                Method::UpdatePrice => self.update_price(request).await,
                _ => Err(anyhow!("unsupported method type: {:?}", request.method)),
            }
        }

        async fn get_product_list(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let (result_tx, result_rx) = oneshot::channel();
            self.adapter_tx
                .send(adapter::Message::GetProductList { result_tx })
                .await?;

            self.send_result(request, result_rx.await??).await
        }

        async fn get_product(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: GetProductParams = self.deserialize_params(request.params.clone())?;

            let (result_tx, result_rx) = oneshot::channel();
            self.adapter_tx
                .send(adapter::Message::GetProduct {
                    account: params.account,
                    result_tx,
                })
                .await?;

            self.send_result(request, result_rx.await??).await
        }

        async fn get_all_products(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let (result_tx, result_rx) = oneshot::channel();
            self.adapter_tx
                .send(adapter::Message::GetAllProducts { result_tx })
                .await?;

            self.send_result(request, result_rx.await??).await
        }

        async fn subscribe_price(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: SubscribePriceParams = self.deserialize_params(request.params.clone())?;

            let (result_tx, result_rx) = oneshot::channel();
            self.adapter_tx
                .send(adapter::Message::SubscribePrice {
                    result_tx,
                    account: params.account,
                    notify_price_tx: self.notify_price_tx.clone(),
                })
                .await?;

            self.send_result(
                request,
                SubscribeResult {
                    subscription: result_rx.await??,
                },
            )
            .await
        }

        async fn subscribe_price_sched(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: SubscribePriceSchedParams =
                self.deserialize_params(request.params.clone())?;

            let (result_tx, result_rx) = oneshot::channel();
            self.adapter_tx
                .send(adapter::Message::SubscribePriceSched {
                    result_tx,
                    account: params.account,
                    notify_price_sched_tx: self.notify_price_sched_tx.clone(),
                })
                .await?;

            self.send_result(
                request,
                SubscribeResult {
                    subscription: result_rx.await??,
                },
            )
            .await
        }

        async fn update_price(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: UpdatePriceParams = self.deserialize_params(request.params.clone())?;

            self.adapter_tx
                .send(adapter::Message::UpdatePrice {
                    account: params.account,
                    price: params.price,
                    conf: params.conf,
                    status: params.status,
                })
                .await?;

            self.send_result(request, 0).await
        }

        fn deserialize_params<T>(&self, value: Option<Value>) -> Result<T>
        where
            T: DeserializeOwned,
        {
            serde_json::from_value::<T>(value.ok_or_else(|| anyhow!("Missing request parameters"))?)
                .map_err(|e| e.into())
        }

        async fn send_result<T>(
            &mut self,
            request: &Request<Method, jrpc::Value>,
            result: T,
        ) -> Result<()>
        where
            T: Serialize + DeserializeOwned,
        {
            let id = request.id.clone().to_id().unwrap_or_else(|| Id::from(0));
            let response = Response::success(id, result);
            self.send_text(&response.to_string()).await
        }

        async fn send_error_response(
            &mut self,
            error: anyhow::Error,
            request: &Request<Method, jrpc::Value>,
        ) -> Result<()> {
            self.send_error(error, request.id.clone().to_id()).await
        }

        async fn send_error(&mut self, error: anyhow::Error, id: Option<Id>) -> Result<()> {
            let response: Response<Value> = Response::error(
                id.unwrap_or_else(|| Id::from(0)),
                ErrorCode::InternalError,
                error.to_string(),
                None,
            );
            self.send_text(&response.to_string()).await
        }

        async fn send_notification<T>(&mut self, method: Method, params: Option<T>) -> Result<()>
        where
            T: Sized + Serialize + DeserializeOwned,
        {
            self.send_request(IdReq::Notification, method, params).await
        }

        async fn send_request<I, T>(
            &mut self,
            id: I,
            method: Method,
            params: Option<T>,
        ) -> Result<()>
        where
            I: Into<IdReq>,
            T: Sized + Serialize + DeserializeOwned,
        {
            let request = Request::with_params(id, method, params);
            self.send_text(&request.to_string()).await
        }

        async fn send_text(&mut self, msg: &str) -> Result<()> {
            self.ws_tx
                .send(Message::text(msg.to_string()))
                .await
                .map_err(|e| e.into())
        }
    }

    #[derive(Clone)]
    struct WithLogger {
        logger: Logger,
    }

    #[derive(Clone, Debug, Deserialize)]
    pub struct Config {
        /// The address which the websocket API server will listen on.
        listen_address: String,
        /// Size of the buffer of each Server's channel on which `notify_price` events are
        /// received from the Adapter.
        notify_price_tx_buffer: usize,
        /// Size of the buffer of each Server's channel on which `notify_price_sched` events are
        /// received from the Adapter.
        notify_price_sched_tx_buffer: usize,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                listen_address: "127.0.0.1:8910".to_string(),
                notify_price_tx_buffer: 1000,
                notify_price_sched_tx_buffer: 1000,
            }
        }
    }

    pub struct Server {
        adapter_tx: mpsc::Sender<adapter::Message>,
        config: Config,
        logger: Logger,
    }

    impl Server {
        pub fn new(
            adapter_tx: mpsc::Sender<adapter::Message>,
            config: Config,
            logger: Logger,
        ) -> Self {
            Server {
                adapter_tx,
                config,
                logger,
            }
        }

        pub async fn run(&self, shutdown_rx: broadcast::Receiver<()>) {
            if let Err(err) = self.serve(shutdown_rx).await {
                error!(self.logger, "{:#}", err; "error" => format!("{:?}", err))
            }
        }

        async fn serve(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<()> {
            let adapter_tx = self.adapter_tx.clone();
            let config = self.config.clone();
            let with_logger = WithLogger {
                logger: self.logger.clone(),
            };

            let index = warp::path::end()
                .and(warp::ws())
                .and(warp::any().map(move || adapter_tx.clone()))
                .and(warp::any().map(move || with_logger.clone()))
                .and(warp::any().map(move || config.clone()))
                .map(
                    |ws: Ws,
                     adapter_tx: mpsc::Sender<adapter::Message>,
                     with_logger: WithLogger,
                     config: Config| {
                        ws.on_upgrade(move |conn| async move {
                            Connection::new(
                                conn,
                                adapter_tx,
                                config.notify_price_tx_buffer,
                                config.notify_price_sched_tx_buffer,
                                with_logger.logger,
                            )
                            .consume()
                            .await
                        })
                    },
                );

            let (_, serve) = warp::serve(index).bind_with_graceful_shutdown(
                self.config.listen_address.as_str().parse::<SocketAddr>()?,
                async move {
                    let _ = shutdown_rx.recv().await;
                },
            );

            tokio::task::spawn(serve).await.map_err(|e| e.into())
        }
    }

}
