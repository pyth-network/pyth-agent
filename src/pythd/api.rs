use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use anyhow::Result;
use async_trait::async_trait;

#[cfg(test)]
use mockall::automock;

type PubKey = String;
type Attrs = BTreeMap<String, String>;

type Price = i64;
type Exponent = i64;
type Conf = u64;
type Slot = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ProductAccountMetadata {
    account: PubKey,
    attr_dict: Attrs,
    prices: Vec<PriceAccountMetadata>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PriceAccountMetadata {
    account: PubKey,
    price_type: String,
    price_exponent: Exponent,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ProductAccount {
    account: PubKey,
    attr_dict: Attrs,
    price_accounts: Vec<PriceAccount>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PriceAccount {
    account: PubKey,
    price_type: String,
    price_exponent: Exponent,
    status: String,
    price: Price,
    conf: Conf,
    twap: Price,
    twac: Price,
    valid_slot: Slot,
    pub_slot: Slot,
    prev_slot: Slot,
    prev_price: Price,
    prev_conf: Conf,
    publisher_accounts: Vec<PublisherAccount>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PublisherAccount {
    account: PubKey,
    status: String,
    price: Price,
    conf: Conf,
    slot: Slot,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PriceUpdate {
    price: Price,
    conf: Conf,
    status: String,
    valid_slot: Slot,
    pub_slot: Slot,
}

type Subscription = i64;

// The Pythd JRPC API delegates to structs implementing the Protocol trait
// to process API calls. This allows the business logic to be mocked out.
#[cfg_attr(test, automock)]
#[async_trait]
pub trait Protocol {
    async fn get_product_list(&self) -> Result<Vec<ProductAccountMetadata>>;

    async fn get_product(&self, account: PubKey) -> Result<ProductAccount>;

    async fn get_all_products(&self) -> Result<Vec<ProductAccount>>;

    async fn subscribe_price(&self, account: PubKey) -> Result<Subscription>;

    async fn update_price(
        &self,
        account: PubKey,
        price: Price,
        conf: Conf,
        status: &str,
    ) -> Result<()>;
}

mod rpc {

    use std::fmt::Debug;

    use std::sync::Arc;

    use anyhow::{anyhow, Result};
    use futures_util::stream::{SplitSink, SplitStream, StreamExt};
    use futures_util::SinkExt;
    use jrpc::{parse_request, ErrorCode, Id, Request, Response, Value};
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};

    use warp::ws::{Message, WebSocket, Ws};
    use warp::Filter;

    use super::{Conf, Price, PubKey, Subscription};

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(rename_all = "snake_case")]
    enum Method {
        GetProductList,
        GetProduct,
        GetAllProducts,
        SubscribePrice,
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

    #[derive(Serialize, Deserialize, Debug)]
    struct UpdatePriceParams {
        account: PubKey,
        price: Price,
        conf: Conf,
        status: String,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SubscribeResult {
        subscription: Subscription,
    }

    struct Connection {
        protocol: Arc<Box<dyn super::Protocol + Send + Sync>>,
        ws_tx: SplitSink<WebSocket, Message>,
        ws_rx: SplitStream<WebSocket>,
    }

    impl Connection {
        async fn consume(&mut self) -> Result<()> {
            while let Some(body) = self.ws_rx.next().await {
                match body {
                    Ok(msg) => self.handle(msg).await,
                    Err(e) => self.send_error(e.into(), None).await,
                }?;
            }

            Ok(())
        }

        async fn handle(&mut self, msg: Message) -> Result<()> {
            // Ignore control and binary messages
            if !msg.is_text() {
                return Ok(());
            }

            // Parse and dispatch the message
            match self.parse(msg).await {
                Ok(request) => self.dispatch_and_catch_error(&request).await,
                Err(e) => self.send(Response::<Value>::Err(e)).await,
            }
        }

        async fn dispatch_and_catch_error(
            &mut self,
            request: &Request<Method, Value>,
        ) -> Result<()> {
            if let Err(e) = self.dispatch(request).await {
                self.send_error_response(e, request).await?;
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
                Method::UpdatePrice => self.update_price(request).await,
            }
        }

        async fn get_product_list(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let result = self.protocol.get_product_list().await?;
            self.send_result(request, result).await
        }

        async fn get_product(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: GetProductParams = self.deserialize_params(request.params.clone())?;
            let result = self.protocol.get_product(params.account).await?;
            self.send_result(request, result).await
        }

        async fn get_all_products(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let result = self.protocol.get_all_products().await?;
            self.send_result(request, result).await
        }

        async fn subscribe_price(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: SubscribePriceParams = self.deserialize_params(request.params.clone())?;
            let subscription = self.protocol.subscribe_price(params.account).await?;
            self.send_result(request, SubscribeResult { subscription })
                .await
        }

        async fn update_price(&mut self, request: &Request<Method, Value>) -> Result<()> {
            let params: UpdatePriceParams = self.deserialize_params(request.params.clone())?;
            self.protocol
                .update_price(
                    params.account,
                    params.price,
                    params.conf,
                    params.status.as_str(),
                )
                .await?;
            self.send_result(request, 0).await
        }

        fn deserialize_params<T>(&self, value: Option<Value>) -> Result<T>
        where
            T: DeserializeOwned,
        {
            serde_json::from_value::<T>(value.ok_or(anyhow!("Missing request parameters"))?)
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
            self.send(response).await
        }

        async fn send_error_response(
            &mut self,
            error: anyhow::Error,
            request: &Request<Method, jrpc::Value>,
        ) -> Result<()> {
            self.send_error(error, request.id.clone().to_id()).await
        }

        async fn send_error(&mut self, error: anyhow::Error, id: Option<Id>) -> Result<()> {
            tracing::error!("Error handling websocket request: {:?}", error);
            let response: Response<Value> = Response::error(
                id.unwrap_or_else(|| Id::from(0)),
                ErrorCode::InternalError,
                error.to_string(),
                None,
            );
            self.send(response).await
        }

        async fn send<T>(&mut self, response: Response<T>) -> Result<()>
        where
            T: Serialize + DeserializeOwned,
        {
            self.ws_tx
                .send(Message::text(response.to_string()))
                .await
                .map_err(|e| anyhow!("Failed to send message: {:#?}", e))
        }
    }

    // ConnectionFactory allows objects with complex types necessary for creating
    // new Connection objects to be easily passed to Ws::on_upgrade.
    #[derive(Clone)]
    struct ConnectionFactory {
        protocol: Arc<Box<dyn super::Protocol + Send + Sync>>,
    }

    impl ConnectionFactory {
        fn new_connection(
            &self,
            ws_tx: SplitSink<WebSocket, Message>,
            ws_rx: SplitStream<WebSocket>,
        ) -> Connection {
            Connection {
                protocol: self.protocol.clone(),
                ws_tx,
                ws_rx,
            }
        }
    }

    struct Server;

    impl Server {
        async fn serve(port: u16, connection_factory: ConnectionFactory) {
            let index = warp::path::end()
                .and(warp::ws())
                .and(warp::any().map(move || connection_factory.clone()))
                .map(|ws: Ws, connection_factory: ConnectionFactory| {
                    ws.on_upgrade(move |conn| async move {
                        let (ws_tx, ws_rx) = conn.split();
                        let mut connection = connection_factory.new_connection(ws_tx, ws_rx);
                        if let Err(e) = connection.consume().await {
                            tracing::error!("Error consuming websocket connection: {:?}", e);
                        }
                    })
                });

            warp::serve(index).run(([127, 0, 0, 1], port)).await
        }
    }

    #[cfg(test)]
    mod tests {
        use anyhow::anyhow;
        use jrpc::{Id, Request};
        use mockall::predicate;
        use rand::Rng;
        use serde::de::DeserializeOwned;
        use serde::Serialize;
        use soketto::handshake::{Client, ServerResponse};
        use std::str::from_utf8;
        use std::sync::Arc;
        use tokio::net::TcpStream;
        use tokio_retry::strategy::FixedInterval;
        use tokio_retry::Retry;
        use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

        use super::super::rpc::{GetProductParams, SubscribePriceParams, UpdatePriceParams};
        use super::super::{
            Attrs, MockProtocol, PriceAccount, PriceAccountMetadata, ProductAccount,
            ProductAccountMetadata, PubKey, PublisherAccount, Subscription,
        };
        use super::{ConnectionFactory, Server, SubscribeResult};

        type Sender = soketto::Sender<Compat<TcpStream>>;
        type Receiver = soketto::Receiver<Compat<TcpStream>>;

        async fn start_server(protocol: MockProtocol) -> (Sender, Receiver) {
            let port = portpicker::pick_unused_port().unwrap();

            let connection_factory = ConnectionFactory {
                protocol: Arc::new(Box::new(protocol)),
            };
            tokio::spawn(Server::serve(port, connection_factory));

            // Create a test client, retrying as the server may take some time to start serving requests
            let socket = Retry::spawn(FixedInterval::from_millis(100).take(20), || {
                TcpStream::connect(("127.0.0.1", port))
            })
            .await
            .unwrap();

            // let socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let mut client = Client::new(socket.compat(), "...", "/");

            // Perform the websocket handshake
            let handshake = client.handshake().await.unwrap();
            assert!(matches!(handshake, ServerResponse::Accepted { .. }));
            let (sender, receiver) = client.into_builder().finish();

            (sender, receiver)
        }

        async fn send<T>(mut sender: Sender, request: Request<String, T>)
        where
            T: Serialize + DeserializeOwned,
        {
            sender.send_text(request.to_string()).await.unwrap();
        }

        async fn receive(mut receiver: Receiver) -> Vec<u8> {
            let mut bytes = Vec::new();
            receiver.receive_data(&mut bytes).await.unwrap();
            bytes
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn json_get_product_success_test() {
            // Set expectations on our mock protocol
            let product_account = "some_product_account".to_string();
            let mut protocol = MockProtocol::new();
            protocol
                .expect_get_product()
                .with(predicate::eq(product_account.clone()))
                .times(1)
                .returning(|account| {
                    Ok(ProductAccount {
                        account,
                        attr_dict: Attrs::from(
                            [
                                ("symbol", "BTC/USD"),
                                ("asset_type", "Crypto"),
                                ("country", "US"),
                                ("quote_currency", "USD"),
                                ("tenor", "spot"),
                            ]
                            .map(|(k, v)| (k.to_string(), v.to_string())),
                        ),
                        price_accounts: vec![PriceAccount {
                            account: "some_price_account".to_string(),
                            price_type: "price".to_string(),
                            price_exponent: 8,
                            status: "trading".to_string(),
                            price: 536,
                            conf: 67,
                            twap: 276,
                            twac: 463,
                            valid_slot: 4628,
                            pub_slot: 4736,
                            prev_slot: 3856,
                            prev_price: 400,
                            prev_conf: 45,
                            publisher_accounts: vec![
                                PublisherAccount {
                                    account: "some_publisher_account".to_string(),
                                    status: "trading".to_string(),
                                    price: 500,
                                    conf: 24,
                                    slot: 3563,
                                },
                                PublisherAccount {
                                    account: "another_publisher_account".to_string(),
                                    status: "halted".to_string(),
                                    price: 300,
                                    conf: 683,
                                    slot: 5834,
                                },
                            ],
                        }],
                    })
                });

            // Start and connect to the RPC server
            let (mut sender, receiver) = start_server(protocol).await;

            // Make a binary request, which should be safely ignored
            let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
            sender.send_binary(random_bytes).await.unwrap();

            // Make a request
            send(
                sender,
                Request::with_params(
                    Id::from(1),
                    "get_product".to_string(),
                    GetProductParams {
                        account: product_account,
                    },
                ),
            )
            .await;

            // Wait for the result to come back
            let bytes = receive(receiver).await;
            let received_json = from_utf8(&bytes).unwrap();

            // Check that the JSON representation is correct
            let expected_json = r#"{"jsonrpc":"2.0","result":{"account":"some_product_account","attr_dict":{"asset_type":"Crypto","country":"US","quote_currency":"USD","symbol":"BTC/USD","tenor":"spot"},"price_accounts":[{"account":"some_price_account","price_type":"price","price_exponent":8,"status":"trading","price":536,"conf":67,"twap":276,"twac":463,"valid_slot":4628,"pub_slot":4736,"prev_slot":3856,"prev_price":400,"prev_conf":45,"publisher_accounts":[{"account":"some_publisher_account","status":"trading","price":500,"conf":24,"slot":3563},{"account":"another_publisher_account","status":"halted","price":300,"conf":683,"slot":5834}]}]},"id":1}"#;
            assert_eq!(received_json, expected_json);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn json_unknown_method_error_test() {
            // Start and connect to the server
            let protocol = MockProtocol::new();
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::with_params(
                    Id::from(3),
                    "wrong_method".to_string(),
                    GetProductParams {
                        account: "some_account".to_string(),
                    },
                ),
            )
            .await;

            // Wait for the result to come back
            let bytes = receive(receiver).await;

            // Check that the result is what we expect
            let expected_json = r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"unknown variant `wrong_method`, expected one of `get_product_list`, `get_product`, `get_all_products`, `subscribe_price`, `update_price`","data":null},"id":3}"#;
            let received_json = std::str::from_utf8(&bytes).unwrap();
            assert_eq!(received_json, expected_json);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn json_missing_request_parameters_test() {
            // Start and connect to the server
            let protocol = MockProtocol::new();
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::new(Id::from(5), "update_price".to_string()),
            )
            .await;

            // Wait for the result to come back
            let bytes = receive(receiver).await;
            let received_json = from_utf8(&bytes).unwrap();

            // Check that the result is what we expect
            let expected_json = r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Missing request parameters","data":null},"id":5}"#;
            assert_eq!(received_json, expected_json);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn json_internal_error() {
            // Set expectations on our mock protocol
            let mut protocol = MockProtocol::new();
            protocol
                .expect_get_product()
                .times(1)
                .returning(|_| Err(anyhow!("some internal error")));

            // Start and connect to the JRPC server
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::with_params(
                    Id::from(9),
                    "get_product".to_string(),
                    GetProductParams {
                        account: "some_account".to_string(),
                    },
                ),
            )
            .await;

            // Get the result back
            let bytes = receive(receiver).await;
            let received_json = std::str::from_utf8(&bytes).unwrap();

            // Check that the result is what we expect
            let expected_json = r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"some internal error","data":null},"id":9}"#;
            assert_eq!(expected_json, received_json);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn json_update_price_success() {
            // Set expectations on our mock protocol
            let status = "trading";
            let params = UpdatePriceParams {
                account: PubKey::from("some_price_account"),
                price: 7467,
                conf: 892,
                status: status.to_string(),
            };
            let mut protocol = MockProtocol::new();
            protocol
                .expect_update_price()
                .with(
                    predicate::eq(params.account.clone()),
                    predicate::eq(params.price),
                    predicate::eq(params.conf),
                    predicate::eq(status),
                )
                .times(1)
                .returning(|_, _, _, _| Ok(()));

            // Start and connect to the JRPC server
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::with_params(Id::from(15), "update_price".to_string(), params),
            )
            .await;

            // Get the result back
            let bytes = receive(receiver).await;

            // Assert that the result is what we expect
            let expected_json = r#"{"jsonrpc":"2.0","result":0,"id":15}"#;
            let received_json = from_utf8(&bytes).unwrap();
            assert_eq!(received_json, expected_json);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn get_product_list_success_test() {
            // Set expectations on our mock protocol
            let product_account = PubKey::from("some_product_account");
            let mut protocol = MockProtocol::new();
            let data = vec![ProductAccountMetadata {
                account: product_account.clone(),
                attr_dict: Attrs::from(
                    [
                        ("symbol", "BTC/USD"),
                        ("asset_type", "Crypto"),
                        ("country", "US"),
                        ("quote_currency", "USD"),
                        ("tenor", "spot"),
                    ]
                    .map(|(k, v)| (k.to_string(), v.to_string())),
                ),
                prices: vec![
                    PriceAccountMetadata {
                        account: PubKey::from("some_price_account"),
                        price_type: "price".to_string(),
                        price_exponent: 4,
                    },
                    PriceAccountMetadata {
                        account: PubKey::from("another_price_account"),
                        price_type: "special".to_string(),
                        price_exponent: 6,
                    },
                ],
            }];
            let return_data = data.clone();
            protocol
                .expect_get_product_list()
                .times(1)
                .returning(move || Ok(return_data.clone()));

            // Start and connect to the JRPC server
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::new(Id::from(11), "get_product_list".to_string()),
            )
            .await;

            // Get the result back
            let bytes = receive(receiver).await;

            // Assert that the result is what we expect
            let response: jrpc::Response<Vec<ProductAccountMetadata>> =
                serde_json::from_slice(&bytes).unwrap();
            assert!(matches!(response, jrpc::Response::Ok(success) if success.result == data));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn get_all_products_success() {
            // Set expectations on our mock protocol
            let mut protocol = MockProtocol::new();
            let data = vec![ProductAccount {
                account: PubKey::from("some_product_account"),
                attr_dict: Attrs::from(
                    [
                        ("symbol", "LTC/USD"),
                        ("asset_type", "Crypto"),
                        ("country", "US"),
                        ("quote_currency", "USD"),
                        ("tenor", "spot"),
                    ]
                    .map(|(k, v)| (k.to_string(), v.to_string())),
                ),
                price_accounts: vec![PriceAccount {
                    account: PubKey::from("some_price_account"),
                    price_type: "price".to_string(),
                    price_exponent: 7463,
                    status: "trading".to_string(),
                    price: 6453,
                    conf: 3434,
                    twap: 6454,
                    twac: 365,
                    valid_slot: 3646,
                    pub_slot: 2857,
                    prev_slot: 7463,
                    prev_price: 3784,
                    prev_conf: 9879,
                    publisher_accounts: vec![
                        PublisherAccount {
                            account: PubKey::from("some_publisher_account"),
                            status: "trading".to_string(),
                            price: 756,
                            conf: 8787,
                            slot: 2209,
                        },
                        PublisherAccount {
                            account: PubKey::from("another_publisher_account"),
                            status: "halted".to_string(),
                            price: 0,
                            conf: 0,
                            slot: 6676,
                        },
                    ],
                }],
            }];
            let return_data = data.clone();
            protocol
                .expect_get_all_products()
                .times(1)
                .returning(move || Ok(return_data.clone()));

            // Start and connect to the JRPC server
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::new(Id::from(5), "get_all_products".to_string()),
            )
            .await;

            // Get the result back
            let bytes = receive(receiver).await;

            // Assert that the result is what we expect
            let response: jrpc::Response<Vec<ProductAccount>> =
                serde_json::from_slice(&bytes).unwrap();
            assert!(matches!(response, jrpc::Response::Ok(success) if success.result == data));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn subscribe_price_success() {
            // Set expectations on our mock protocol
            let price_account = PubKey::from("some_price_account");
            let subscription = Subscription::from(16);
            let mut protocol = MockProtocol::new();
            protocol
                .expect_subscribe_price()
                .with(predicate::eq(price_account.clone()))
                .times(1)
                .returning(move |_| Ok(subscription));

            // Start and connect to the JRPC server
            let (sender, receiver) = start_server(protocol).await;

            // Make a request
            send(
                sender,
                Request::with_params(
                    Id::from(13),
                    "subscribe_price".to_string(),
                    SubscribePriceParams {
                        account: price_account,
                    },
                ),
            )
            .await;

            // Get the result back
            let bytes = receive(receiver).await;

            // Assert that the result is what we expect
            let response: jrpc::Response<SubscribeResult> = serde_json::from_slice(&bytes).unwrap();
            assert!(
                matches!(response, jrpc::Response::Ok(success) if success.result == SubscribeResult{ subscription })
            );
        }
    }
}
