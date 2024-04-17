// This module is responsible for exposing the JRPC-esq websocket API
// documented at https://docs.pyth.network/publish-data/pyth-client-websocket-api
//
// It does not implement the business logic, only exposes a websocket server which
// accepts messages and can return responses in the expected format.

use {
    super::{
        super::adapter,
        Conf,
        NotifyPrice,
        NotifyPriceSched,
        Price,
        Pubkey,
        SubscriptionID,
    },
    anyhow::{
        anyhow,
        Result,
    },
    futures_util::{
        stream::{
            SplitSink,
            SplitStream,
            StreamExt,
        },
        SinkExt,
    },
    jrpc::{
        parse_request,
        ErrorCode,
        Id,
        IdReq,
        Request,
        Response,
        Value,
    },
    serde::{
        de::DeserializeOwned,
        Deserialize,
        Serialize,
    },
    serde_this_or_that::{
        as_i64,
        as_u64,
    },
    slog::Logger,
    std::{
        fmt::Debug,
        net::SocketAddr,
    },
    tokio::sync::{
        broadcast,
        mpsc,
    },
    warp::{
        ws::{
            Message,
            WebSocket,
            Ws,
        },
        Filter,
    },
};

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
    account: Pubkey,
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribePriceParams {
    account: Pubkey,
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribePriceSchedParams {
    account: Pubkey,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UpdatePriceParams {
    account: Pubkey,
    #[serde(deserialize_with = "as_i64")]
    price:   Price,
    #[serde(deserialize_with = "as_u64")]
    conf:    Conf,
    status:  String,
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

async fn handle_connection(
    ws_conn: WebSocket,
    adapter_tx: mpsc::Sender<adapter::Message>,
    notify_price_tx_buffer: usize,
    notify_price_sched_tx_buffer: usize,
    logger: Logger,
) {
    // Create the channels
    let (mut ws_tx, mut ws_rx) = ws_conn.split();
    let (mut notify_price_tx, mut notify_price_rx) = mpsc::channel(notify_price_tx_buffer);
    let (mut notify_price_sched_tx, mut notify_price_sched_rx) =
        mpsc::channel(notify_price_sched_tx_buffer);

    loop {
        if let Err(err) = handle_next(
            &logger,
            &adapter_tx,
            &mut ws_tx,
            &mut ws_rx,
            &mut notify_price_tx,
            &mut notify_price_rx,
            &mut notify_price_sched_tx,
            &mut notify_price_sched_rx,
        )
        .await
        {
            if let Some(ConnectionError::WebsocketConnectionClosed) =
                err.downcast_ref::<ConnectionError>()
            {
                info!(logger, "websocket connection closed");
                return;
            }

            error!(logger, "{}", err);
            debug!(logger, "error context"; "context" => format!("{:?}", err));
        }
    }
}

async fn handle_next(
    logger: &Logger,
    adapter_tx: &mpsc::Sender<adapter::Message>,
    ws_tx: &mut SplitSink<WebSocket, Message>,
    ws_rx: &mut SplitStream<WebSocket>,
    notify_price_tx: &mut mpsc::Sender<NotifyPrice>,
    notify_price_rx: &mut mpsc::Receiver<NotifyPrice>,
    notify_price_sched_tx: &mut mpsc::Sender<NotifyPriceSched>,
    notify_price_sched_rx: &mut mpsc::Receiver<NotifyPriceSched>,
) -> Result<()> {
    tokio::select! {
        msg = ws_rx.next() => {
            match msg {
                Some(body) => match body {
                    Ok(msg) => {
                        handle(
                            logger,
                            ws_tx,
                            adapter_tx,
                            notify_price_tx,
                            notify_price_sched_tx,
                            msg,
                        )
                        .await
                    }
                    Err(e) => send_error(ws_tx, e.into(), None).await,
                },
                None => Err(ConnectionError::WebsocketConnectionClosed)?,
            }
        }
        Some(notify_price) = notify_price_rx.recv() => {
            send_notification(ws_tx, Method::NotifyPrice, Some(notify_price))
                .await
        }
        Some(notify_price_sched) = notify_price_sched_rx.recv() => {
            send_notification(ws_tx, Method::NotifyPriceSched, Some(notify_price_sched))
                .await
        }
    }
}

async fn handle(
    logger: &Logger,
    ws_tx: &mut SplitSink<WebSocket, Message>,
    adapter_tx: &mpsc::Sender<adapter::Message>,
    notify_price_tx: &mpsc::Sender<NotifyPrice>,
    notify_price_sched_tx: &mpsc::Sender<NotifyPriceSched>,
    msg: Message,
) -> Result<()> {
    // Ignore control and binary messages
    if !msg.is_text() {
        debug!(logger, "JSON RPC API: skipped non-text message");
        return Ok(());
    }

    // Parse and dispatch the message
    match parse(msg).await {
        Ok((requests, is_batch)) => {
            let mut responses = Vec::with_capacity(requests.len());

            // Perform requests in sequence and gather responses
            for request in requests {
                let response = dispatch_and_catch_error(
                    logger,
                    adapter_tx,
                    notify_price_tx,
                    notify_price_sched_tx,
                    &request,
                )
                .await;
                responses.push(response)
            }

            // Send an array if we're handling a batch
            // request, single response object otherwise
            if is_batch {
                send_text(ws_tx, &serde_json::to_string(&responses)?).await?;
            } else {
                send_text(ws_tx, &serde_json::to_string(&responses[0])?).await?;
            }
        }
        // The top-level parsing errors are fine to share with client
        Err(e) => {
            send_error(ws_tx, e, None).await?;
        }
    }

    Ok(())
}

/// Parse a JSONRPC request object or a batch of them. The
/// bool in result informs request handling whether it needs
/// to respond with a single object or an array, to prevent
/// sending unexpected
/// `[{<just one response, but request was not array>}]`
/// array payloads.
async fn parse(msg: Message) -> Result<(Vec<Request<Method, Value>>, bool)> {
    let s = msg
        .to_str()
        .map_err(|_| anyhow!("Could not parse message as text"))?;

    let json_value: Value = serde_json::from_str(s)?;
    if let Some(array) = json_value.as_array() {
        // Interpret request as JSON-RPC 2.0 batch if value is an array
        let mut requests = Vec::with_capacity(array.len());
        for maybe_request in array {
            // Re-serialize for parse_request(), it's the only
            // jrpc parsing function available and it's taking
            // &str.
            let maybe_request_string = serde_json::to_string(maybe_request)?;
            requests.push(
                parse_request::<Method>(&maybe_request_string)
                    .map_err(|e| anyhow!("Could not parse message: {}", e.error.message))?,
            );
        }

        Ok((requests, true))
    } else {
        // Base single request case
        let single = parse_request::<Method>(s)
            .map_err(|e| anyhow!("Could not parse message: {}", e.error.message))?;
        Ok((vec![single], false))
    }
}

async fn dispatch_and_catch_error(
    logger: &Logger,
    adapter_tx: &mpsc::Sender<adapter::Message>,
    notify_price_tx: &mpsc::Sender<NotifyPrice>,
    notify_price_sched_tx: &mpsc::Sender<NotifyPriceSched>,
    request: &Request<Method, Value>,
) -> Response<serde_json::Value> {
    debug!(
        logger,
        "JSON RPC API: handling request";
        "method" => format!("{:?}", request.method),
    );

    let result = match request.method {
        Method::GetProductList => get_product_list(adapter_tx).await,
        Method::GetProduct => get_product(adapter_tx, request).await,
        Method::GetAllProducts => get_all_products(adapter_tx).await,
        Method::UpdatePrice => update_price(adapter_tx, request).await,
        Method::SubscribePrice => subscribe_price(adapter_tx, notify_price_tx, request).await,
        Method::SubscribePriceSched => {
            subscribe_price_sched(adapter_tx, notify_price_sched_tx, request).await
        }
        Method::NotifyPrice | Method::NotifyPriceSched => {
            Err(anyhow!("unsupported method: {:?}", request.method))
        }
    };

    // Consider errors internal, print details to logs.
    match result {
        Ok(payload) => {
            Response::success(request.id.clone().to_id().unwrap_or(Id::from(0)), payload)
        }
        Err(e) => {
            warn!(
                logger,
                "Error handling JSON RPC request";
                "request" => format!("{:?}", request),
                "error" => format!("{}", e.to_string()),
            );

            Response::error(
                request.id.clone().to_id().unwrap_or(Id::from(0)),
                ErrorCode::InternalError,
                e.to_string(),
                None,
            )
        }
    }
}

mod get_all_products;
mod get_product;
mod get_product_list;
mod subscribe_price;
mod subscribe_price_sched;
mod update_price;
use {
    get_all_products::*,
    get_product::*,
    get_product_list::*,
    subscribe_price::*,
    subscribe_price_sched::*,
    update_price::*,
};

async fn send_error(
    ws_tx: &mut SplitSink<WebSocket, Message>,
    error: anyhow::Error,
    id: Option<Id>,
) -> Result<()> {
    let response: Response<Value> = Response::error(
        id.unwrap_or_else(|| Id::from(0)),
        ErrorCode::InternalError,
        error.to_string(),
        None,
    );
    send_text(ws_tx, &response.to_string()).await
}

async fn send_notification<T>(
    ws_tx: &mut SplitSink<WebSocket, Message>,
    method: Method,
    params: Option<T>,
) -> Result<()>
where
    T: Sized + Serialize + DeserializeOwned,
{
    send_request(ws_tx, IdReq::Notification, method, params).await
}

async fn send_request<I, T>(
    ws_tx: &mut SplitSink<WebSocket, Message>,
    id: I,
    method: Method,
    params: Option<T>,
) -> Result<()>
where
    I: Into<IdReq>,
    T: Sized + Serialize + DeserializeOwned,
{
    let request = Request::with_params(id, method, params);
    send_text(ws_tx, &request.to_string()).await
}

async fn send_text(ws_tx: &mut SplitSink<WebSocket, Message>, msg: &str) -> Result<()> {
    ws_tx
        .send(Message::text(msg.to_string()))
        .await
        .map_err(|e| e.into())
}

#[derive(Clone)]
struct WithLogger {
    logger: Logger,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The address which the websocket API server will listen on.
    pub listen_address:               String,
    /// Size of the buffer of each Server's channel on which `notify_price` events are
    /// received from the Adapter.
    pub notify_price_tx_buffer:       usize,
    /// Size of the buffer of each Server's channel on which `notify_price_sched` events are
    /// received from the Adapter.
    pub notify_price_sched_tx_buffer: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_address:               "127.0.0.1:8910".to_string(),
            notify_price_tx_buffer:       10000,
            notify_price_sched_tx_buffer: 10000,
        }
    }
}

pub async fn run(
    config: Config,
    logger: Logger,
    adapter_tx: mpsc::Sender<adapter::Message>,
    shutdown_rx: broadcast::Receiver<()>,
) {
    if let Err(err) = serve(config, &logger, adapter_tx, shutdown_rx).await {
        error!(logger, "{}", err);
        debug!(logger, "error context"; "context" => format!("{:?}", err));
    }
}

async fn serve(
    config: Config,
    logger: &Logger,
    adapter_tx: mpsc::Sender<adapter::Message>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let adapter_tx = adapter_tx.clone();
    let config = config.clone();
    let with_logger = WithLogger {
        logger: logger.clone(),
    };

    let index = {
        let config = config.clone();
        warp::path::end()
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
                        info!(with_logger.logger, "websocket user connected");
                        handle_connection(
                            conn,
                            adapter_tx,
                            config.notify_price_tx_buffer,
                            config.notify_price_sched_tx_buffer,
                            with_logger.logger,
                        )
                        .await
                    })
                },
            )
    };

    let (_, serve) = warp::serve(index).bind_with_graceful_shutdown(
        config.listen_address.as_str().parse::<SocketAddr>()?,
        async move {
            let _ = shutdown_rx.recv().await;
        },
    );

    info!(logger, "starting api server"; "listen address" => config.listen_address.clone());

    tokio::task::spawn(serve).await.map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                rpc::GetProductParams,
                Attrs,
                PriceAccount,
                PriceAccountMetadata,
                ProductAccount,
                ProductAccountMetadata,
                Pubkey,
                PublisherAccount,
                SubscriptionID,
            },
            Config,
        },
        crate::agent::pythd::{
            adapter,
            api::{
                rpc::{
                    SubscribePriceParams,
                    SubscribePriceSchedParams,
                    UpdatePriceParams,
                },
                NotifyPrice,
                NotifyPriceSched,
                PriceUpdate,
            },
        },
        anyhow::anyhow,
        iobuffer::IoBuffer,
        jrpc::{
            Id,
            Request,
        },
        rand::Rng,
        serde::{
            de::DeserializeOwned,
            Serialize,
        },
        slog_extlog::slog_test,
        soketto::handshake::{
            Client,
            ServerResponse,
        },
        std::str::from_utf8,
        tokio::{
            net::TcpStream,
            sync::{
                broadcast,
                mpsc,
            },
            task::JoinHandle,
        },
        tokio_retry::{
            strategy::FixedInterval,
            Retry,
        },
        tokio_util::compat::{
            Compat,
            TokioAsyncReadCompatExt,
        },
    };

    struct TestAdapter {
        rx: mpsc::Receiver<adapter::Message>,
    }

    impl TestAdapter {
        async fn recv(&mut self) -> adapter::Message {
            self.rx.recv().await.unwrap()
        }
    }

    struct TestServer {
        shutdown_tx: broadcast::Sender<()>,
        jh:          JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            let _ = self.shutdown_tx.send(());
            self.jh.abort();
        }
    }

    struct TestClient {
        sender:   soketto::Sender<Compat<TcpStream>>,
        receiver: soketto::Receiver<Compat<TcpStream>>,
    }

    impl TestClient {
        async fn new(server_port: u16) -> Self {
            // Connect to the server, retrying as the server may take some time to respond to requests initially
            let socket = Retry::spawn(FixedInterval::from_millis(100).take(20), || {
                TcpStream::connect(("127.0.0.1", server_port))
            })
            .await
            .unwrap();
            let mut client = Client::new(socket.compat(), "...", "/");

            // Perform the websocket handshake
            let handshake = client.handshake().await.unwrap();
            assert!(matches!(handshake, ServerResponse::Accepted { .. }));

            let (sender, receiver) = client.into_builder().finish();
            TestClient { sender, receiver }
        }

        async fn send<T>(&mut self, request: Request<String, T>)
        where
            T: Serialize + DeserializeOwned,
        {
            self.sender.send_text(request.to_string()).await.unwrap();
        }

        async fn send_batch(&mut self, requests: Vec<Request<String, serde_json::Value>>) {
            let serialized = serde_json::to_string(&requests).unwrap();
            self.sender.send_text(serialized).await.unwrap()
        }

        async fn recv_json(&mut self) -> String {
            let bytes = self.recv_bytes().await;
            from_utf8(&bytes).unwrap().to_string()
        }

        async fn recv_bytes(&mut self) -> Vec<u8> {
            let mut bytes = Vec::new();
            self.receiver.receive_data(&mut bytes).await.unwrap();
            bytes
        }
    }

    async fn start_server() -> (TestServer, TestClient, TestAdapter, IoBuffer) {
        let listen_port = portpicker::pick_unused_port().unwrap();

        // Create the test adapter
        let (adapter_tx, adapter_rx) = mpsc::channel(100);
        let test_adapter = TestAdapter { rx: adapter_rx };

        // Create and spawn a server (the SUT)
        let (shutdown_tx, shutdown_rx) = broadcast::channel(10);
        let log_buffer = IoBuffer::new();
        let logger = slog_test::new_test_logger(log_buffer.clone());
        let config = Config {
            listen_address: format!("127.0.0.1:{:}", listen_port),
            ..Default::default()
        };
        let jh = tokio::spawn(super::run(config, logger, adapter_tx, shutdown_rx));
        let test_server = TestServer { shutdown_tx, jh };

        // Create a test client to interact with the server
        let test_client = TestClient::new(listen_port).await;

        (test_server, test_client, test_adapter, log_buffer)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_get_product_success_test() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Make a binary request, which should be safely ignored
        let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
        test_client.sender.send_binary(random_bytes).await.unwrap();

        // Define the product account we expect to receive back
        let product_account_key = "some_product_account".to_string();
        let product_account = ProductAccount {
            account:        product_account_key.clone(),
            attr_dict:      Attrs::from(
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
                account:            "some_price_account".to_string(),
                price_type:         "price".to_string(),
                price_exponent:     8,
                status:             "trading".to_string(),
                price:              536,
                conf:               67,
                twap:               276,
                twac:               463,
                valid_slot:         4628,
                pub_slot:           4736,
                prev_slot:          3856,
                prev_price:         400,
                prev_conf:          45,
                publisher_accounts: vec![
                    PublisherAccount {
                        account: "some_publisher_account".to_string(),
                        status:  "trading".to_string(),
                        price:   500,
                        conf:    24,
                        slot:    3563,
                    },
                    PublisherAccount {
                        account: "another_publisher_account".to_string(),
                        status:  "halted".to_string(),
                        price:   300,
                        conf:    683,
                        slot:    5834,
                    },
                ],
            }],
        };

        // Make a request
        test_client
            .send(Request::with_params(
                Id::from(1),
                "get_product".to_string(),
                GetProductParams {
                    account: product_account_key,
                },
            ))
            .await;

        // Expect the adapter to receive the corresponding message and send the product account in return
        if let adapter::Message::GetProduct { result_tx, .. } = test_adapter.recv().await {
            result_tx.send(Ok(product_account.clone())).unwrap();
        }

        // Wait for the result to come back
        let received_json = test_client.recv_json().await;

        // Check that the JSON representation is correct
        let expected = serde_json::json!({
        "jsonrpc":"2.0",
        "result": {
            "account": "some_product_account",
            "attr_dict": {
            "symbol": "BTC/USD",
            "asset_type": "Crypto",
            "country": "US",
            "quote_currency": "USD",
            "tenor": "spot"
            },
            "price_accounts": [
            {
                "account": "some_price_account",
                "price_type": "price",
                "price_exponent": 8,
                "status": "trading",
                "price": 536,
                "conf": 67,
                "twap": 276,
                "twac": 463,
                "valid_slot": 4628,
                "pub_slot": 4736,
                "prev_slot": 3856,
                "prev_price": 400,
                "prev_conf": 45,
                "publisher_accounts": [
                {
                    "account": "some_publisher_account",
                    "status": "trading",
                    "price": 500,
                    "conf": 24,
                    "slot": 3563
                },
                {
                    "account": "another_publisher_account",
                    "status": "halted",
                    "price": 300,
                    "conf": 683,
                    "slot": 5834
                }
                ]

            }
            ]
        },
        "id": 1
        }
        );
        let received: serde_json::Value = serde_json::from_str(&received_json).unwrap();
        assert_eq!(received, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_unknown_method_error_test() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, _, _) = start_server().await;

        // Make a request with an unknown methid
        test_client
            .send(Request::with_params(
                Id::from(3),
                "wrong_method".to_string(),
                GetProductParams {
                    account: "some_account".to_string(),
                },
            ))
            .await;

        // Wait for the result to come back
        let received_json = test_client.recv_json().await;

        // Check that the result is what we expect
        let expected_json = r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Could not parse message: unknown variant `wrong_method`, expected one of `get_product_list`, `get_product`, `get_all_products`, `subscribe_price`, `notify_price`, `subscribe_price_sched`, `notify_price_sched`, `update_price`","data":null},"id":0}"#;
        assert_eq!(received_json, expected_json);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_missing_request_parameters_test() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, _, _) = start_server().await;

        // Make a request with missing parameters
        test_client
            .send(Request::new(Id::from(5), "update_price".to_string()))
            .await;

        // Wait for the result to come back
        let received_json = test_client.recv_json().await;

        // Check that the result is what we expect
        let expected_json = r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Missing request parameters","data":null},"id":5}"#;
        assert_eq!(received_json, expected_json);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_internal_error() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Make a request
        test_client
            .send(Request::with_params(
                Id::from(9),
                "get_product".to_string(),
                GetProductParams {
                    account: "some_account".to_string(),
                },
            ))
            .await;

        // Make the adapter throw an error in return
        if let adapter::Message::GetProduct { result_tx, .. } = test_adapter.recv().await {
            result_tx.send(Err(anyhow!("some internal error"))).unwrap();
        }

        // Get the result back
        let received_json = test_client.recv_json().await;

        // Check that the result is what we expect
        let expected_json = r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"some internal error","data":null},"id":9}"#;
        assert_eq!(expected_json, received_json);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_update_price_success() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Make a request to update the price
        let status = "trading";
        let params = UpdatePriceParams {
            account: Pubkey::from("some_price_account"),
            price:   7467,
            conf:    892,
            status:  status.to_string(),
        };
        test_client
            .send(Request::with_params(
                Id::from(15),
                "update_price".to_string(),
                params.clone(),
            ))
            .await;

        // Assert that the adapter receives this
        assert!(matches!(
            test_adapter.recv().await,
            adapter::Message::UpdatePrice {
                account,
                price,
                conf,
                status
            } if account == params.account && price == params.price && conf == params.conf && status == params.status
        ));

        // Get the result back
        let received_json = test_client.recv_json().await;

        // Assert that the result is what we expect
        let expected_json = r#"{"jsonrpc":"2.0","result":0,"id":15}"#;
        assert_eq!(received_json, expected_json);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_product_list_success_test() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Define the data we are working with
        let product_account = Pubkey::from("some_product_account");
        let data = vec![ProductAccountMetadata {
            account:   product_account.clone(),
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
            price:     vec![
                PriceAccountMetadata {
                    account:        Pubkey::from("some_price_account"),
                    price_type:     "price".to_string(),
                    price_exponent: 4,
                },
                PriceAccountMetadata {
                    account:        Pubkey::from("another_price_account"),
                    price_type:     "special".to_string(),
                    price_exponent: 6,
                },
            ],
        }];

        // Make a GetProductList request
        test_client
            .send(Request::new(Id::from(11), "get_product_list".to_string()))
            .await;

        // Instruct the adapter to send our data back
        if let adapter::Message::GetProductList { result_tx } = test_adapter.recv().await {
            result_tx.send(Ok(data.clone())).unwrap();
        }

        // Get the result back
        let bytes = test_client.recv_bytes().await;

        // Assert that the result is what we expect
        let response: jrpc::Response<Vec<ProductAccountMetadata>> =
            serde_json::from_slice(&bytes).unwrap();
        assert!(matches!(response, jrpc::Response::Ok(success) if success.result == data));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_all_products_success() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Define the data we are working with
        let data = vec![ProductAccount {
            account:        Pubkey::from("some_product_account"),
            attr_dict:      Attrs::from(
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
                account:            Pubkey::from("some_price_account"),
                price_type:         "price".to_string(),
                price_exponent:     7463,
                status:             "trading".to_string(),
                price:              6453,
                conf:               3434,
                twap:               6454,
                twac:               365,
                valid_slot:         3646,
                pub_slot:           2857,
                prev_slot:          7463,
                prev_price:         3784,
                prev_conf:          9879,
                publisher_accounts: vec![
                    PublisherAccount {
                        account: Pubkey::from("some_publisher_account"),
                        status:  "trading".to_string(),
                        price:   756,
                        conf:    8787,
                        slot:    2209,
                    },
                    PublisherAccount {
                        account: Pubkey::from("another_publisher_account"),
                        status:  "halted".to_string(),
                        price:   0,
                        conf:    0,
                        slot:    6676,
                    },
                ],
            }],
        }];

        // Make a GetAllProducts request
        test_client
            .send(Request::new(Id::from(5), "get_all_products".to_string()))
            .await;

        // Instruct the adapter to send our data back
        if let adapter::Message::GetAllProducts { result_tx, .. } = test_adapter.recv().await {
            result_tx.send(Ok(data.clone())).unwrap();
        }

        // Get the result back
        let bytes = test_client.recv_bytes().await;

        // Assert that the result is what we expect
        let response: jrpc::Response<Vec<ProductAccount>> = serde_json::from_slice(&bytes).unwrap();
        assert!(matches!(response, jrpc::Response::Ok(success) if success.result == data));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_price_success() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Make a SubscribePrice request
        let price_account = Pubkey::from("some_price_account");
        test_client
            .send(Request::with_params(
                Id::from(13),
                "subscribe_price".to_string(),
                SubscribePriceParams {
                    account: price_account,
                },
            ))
            .await;

        // Send a subscription ID back, and then a Notify Price update.
        // Check that both are received by the client.
        match test_adapter.recv().await {
            adapter::Message::SubscribePrice {
                account: _,
                notify_price_tx,
                result_tx,
            } => {
                // Send the subscription ID from the adapter to the server
                let subscription_id = SubscriptionID::from(16);
                result_tx.send(Ok(subscription_id)).unwrap();

                // Assert that the client connection receives the subscription ID
                assert_eq!(
                    test_client.recv_json().await,
                    r#"{"jsonrpc":"2.0","result":{"subscription":16},"id":13}"#
                );

                // Send a Notify Price event from the adapter to the server, with the corresponding subscription id
                let notify_price_update = NotifyPrice {
                    subscription: subscription_id,
                    result:       PriceUpdate {
                        price:      74,
                        conf:       24,
                        status:     "trading".to_string(),
                        valid_slot: 6786,
                        pub_slot:   9897,
                    },
                };
                notify_price_tx.send(notify_price_update).await.unwrap();

                // Assert that the client connection receives the notify_price notification
                // with the subscription ID and price update.
                assert_eq!(
                    test_client.recv_json().await,
                    r#"{"jsonrpc":"2.0","method":"notify_price","params":{"subscription":16,"result":{"price":74,"conf":24,"status":"trading","valid_slot":6786,"pub_slot":9897}}}"#
                )
            }
            _ => panic!("Uexpected message received from adapter"),
        };
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_price_sched_success() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, _) = start_server().await;

        // Make a SubscribePriceSched request
        let price_account = Pubkey::from("some_price_account");
        test_client
            .send(Request::with_params(
                Id::from(19),
                "subscribe_price_sched".to_string(),
                SubscribePriceSchedParams {
                    account: price_account,
                },
            ))
            .await;

        // Send a subscription ID back, and then a Notify Price Sched update.
        // Check that both are received by the client.
        match test_adapter.recv().await {
            adapter::Message::SubscribePriceSched {
                account: _,
                notify_price_sched_tx,
                result_tx,
            } => {
                // Send the subscription ID from the adapter to the server
                let subscription_id = SubscriptionID::from(27);
                result_tx.send(Ok(subscription_id)).unwrap();

                // Assert that the client connection receives the subscription ID
                assert_eq!(
                    test_client.recv_json().await,
                    r#"{"jsonrpc":"2.0","result":{"subscription":27},"id":19}"#
                );

                // Send a Notify Price Sched event from the adapter to the server, with the corresponding subscription id
                let notify_price_sched_update = NotifyPriceSched {
                    subscription: subscription_id,
                };
                notify_price_sched_tx
                    .send(notify_price_sched_update)
                    .await
                    .unwrap();

                // Assert that the client connection receives the notify_price notification
                // with the correct subscription ID.
                assert_eq!(
                    test_client.recv_json().await,
                    r#"{"jsonrpc":"2.0","method":"notify_price_sched","params":{"subscription":27}}"#
                )
            }
            _ => panic!("Uexpected message received from adapter"),
        };
    }

    /// Send a batch of requests with one of them mangled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_request_partial_failure() {
        // Start and connect to the JRPC server
        let (_test_server, mut test_client, mut test_adapter, mut log_buffer) =
            start_server().await;

        let product_account_key = "some_product_account".to_string();
        let valid_params = GetProductParams {
            account: product_account_key.clone(),
        };

        test_client
            .send_batch(vec![
                // Should work
                Request::with_params(
                    Id::from(15),
                    "get_product".to_string(),
                    serde_json::to_value(&valid_params).unwrap(),
                ),
                // Should fail
                Request::with_params(
                    Id::from(666), // Note: Spooky
                    "update_price".to_string(),
                    serde_json::json!({}),
                ),
            ])
            .await;

        let product_account = ProductAccount {
            account:        product_account_key,
            attr_dict:      Attrs::from(
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
                account:            "some_price_account".to_string(),
                price_type:         "price".to_string(),
                price_exponent:     8,
                status:             "trading".to_string(),
                price:              536,
                conf:               67,
                twap:               276,
                twac:               463,
                valid_slot:         4628,
                pub_slot:           4736,
                prev_slot:          3856,
                prev_price:         400,
                prev_conf:          45,
                publisher_accounts: vec![
                    PublisherAccount {
                        account: "some_publisher_account".to_string(),
                        status:  "trading".to_string(),
                        price:   500,
                        conf:    24,
                        slot:    3563,
                    },
                    PublisherAccount {
                        account: "another_publisher_account".to_string(),
                        status:  "halted".to_string(),
                        price:   300,
                        conf:    683,
                        slot:    5834,
                    },
                ],
            }],
        };

        // Handle the request in test adapter
        if let adapter::Message::GetProduct { result_tx, .. } = test_adapter.recv().await {
            result_tx.send(Ok(product_account.clone())).unwrap();
        }

        let received: serde_json::Value =
            serde_json::from_str(&test_client.recv_json().await).unwrap();

        let expected_product_account_json = serde_json::json!({
            "account": "some_product_account",
            "attr_dict": {
            "symbol": "BTC/USD",
            "asset_type": "Crypto",
            "country": "US",
            "quote_currency": "USD",
            "tenor": "spot"
            },
            "price_accounts": [
            {
                "account": "some_price_account",
                "price_type": "price",
                "price_exponent": 8,
                "status": "trading",
                "price": 536,
                "conf": 67,
                "twap": 276,
                "twac": 463,
                "valid_slot": 4628,
                "pub_slot": 4736,
                "prev_slot": 3856,
                "prev_price": 400,
                "prev_conf": 45,
                "publisher_accounts": [
                {
                    "account": "some_publisher_account",
                    "status": "trading",
                    "price": 500,
                    "conf": 24,
                    "slot": 3563
                },
                {
                    "account": "another_publisher_account",
                    "status": "halted",
                    "price": 300,
                    "conf": 683,
                    "slot": 5834
                }
                ]

            }
            ]
        }
        );

        let expected = serde_json::json!(
            [
        {
            "jsonrpc": "2.0",
            "id": 15,
            "result": expected_product_account_json,
        },
        {
            "jsonrpc": "2.0",
            "error": {
            "code": -32603,
            "message": "missing field `account`",
            "data": null
            },
            "id": 666
        }
            ]
            );

        println!("Log contents:");
        for line in log_buffer.lines() {
            println!("{}", String::from_utf8(line).unwrap());
        }

        assert_eq!(received, expected);
    }
}
