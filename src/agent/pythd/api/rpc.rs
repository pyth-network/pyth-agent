// This module is responsible for exposing the JRPC-esq websocket API
// documented at https://docs.pyth.network/publish-data/pyth-client-websocket-api
//
// It does not implement the business logic, only exposes a websocket server which
// accepts messages and can return responses in the expected format.

use {
    super::{
        Conf,
        NotifyPrice,
        NotifyPriceSched,
        Price,
        Pubkey,
        SubscriptionID,
    },
    crate::agent::state,
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
        sync::Arc,
    },
    tokio::sync::mpsc,
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

async fn handle_connection<S>(
    ws_conn: WebSocket,
    adapter: Arc<S>,
    notify_price_tx_buffer: usize,
    notify_price_sched_tx_buffer: usize,
    logger: Logger,
) where
    S: state::Prices,
    S: Send,
    S: Sync,
    S: 'static,
{
    // Create the channels
    let (mut ws_tx, mut ws_rx) = ws_conn.split();
    let (mut notify_price_tx, mut notify_price_rx) = mpsc::channel(notify_price_tx_buffer);
    let (mut notify_price_sched_tx, mut notify_price_sched_rx) =
        mpsc::channel(notify_price_sched_tx_buffer);

    loop {
        if let Err(err) = handle_next(
            &logger,
            &*adapter,
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

async fn handle_next<S>(
    logger: &Logger,
    adapter: &S,
    ws_tx: &mut SplitSink<WebSocket, Message>,
    ws_rx: &mut SplitStream<WebSocket>,
    notify_price_tx: &mut mpsc::Sender<NotifyPrice>,
    notify_price_rx: &mut mpsc::Receiver<NotifyPrice>,
    notify_price_sched_tx: &mut mpsc::Sender<NotifyPriceSched>,
    notify_price_sched_rx: &mut mpsc::Receiver<NotifyPriceSched>,
) -> Result<()>
where
    S: state::Prices,
{
    tokio::select! {
        msg = ws_rx.next() => {
            match msg {
                Some(body) => match body {
                    Ok(msg) => {
                        handle(
                            logger,
                            ws_tx,
                            adapter,
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

async fn handle<S>(
    logger: &Logger,
    ws_tx: &mut SplitSink<WebSocket, Message>,
    adapter: &S,
    notify_price_tx: &mpsc::Sender<NotifyPrice>,
    notify_price_sched_tx: &mpsc::Sender<NotifyPriceSched>,
    msg: Message,
) -> Result<()>
where
    S: state::Prices,
{
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
                    adapter,
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

async fn dispatch_and_catch_error<S>(
    logger: &Logger,
    adapter: &S,
    notify_price_tx: &mpsc::Sender<NotifyPrice>,
    notify_price_sched_tx: &mpsc::Sender<NotifyPriceSched>,
    request: &Request<Method, Value>,
) -> Response<serde_json::Value>
where
    S: state::Prices,
{
    debug!(
        logger,
        "JSON RPC API: handling request";
        "method" => format!("{:?}", request.method),
    );

    let result = match request.method {
        Method::GetProductList => get_product_list(adapter).await,
        Method::GetProduct => get_product(adapter, request).await,
        Method::GetAllProducts => get_all_products(adapter).await,
        Method::UpdatePrice => update_price(adapter, request).await,
        Method::SubscribePrice => subscribe_price(adapter, notify_price_tx, request).await,
        Method::SubscribePriceSched => {
            subscribe_price_sched(adapter, notify_price_sched_tx, request).await
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

pub async fn run<S>(config: Config, logger: Logger, adapter: Arc<S>)
where
    S: state::Prices,
    S: Send,
    S: Sync,
    S: 'static,
{
    if let Err(err) = serve(config, &logger, adapter).await {
        error!(logger, "{}", err);
        debug!(logger, "error context"; "context" => format!("{:?}", err));
    }
}

async fn serve<S>(config: Config, logger: &Logger, adapter: Arc<S>) -> Result<()>
where
    S: state::Prices,
    S: Send,
    S: Sync,
    S: 'static,
{
    let config = config.clone();
    let with_logger = WithLogger {
        logger: logger.clone(),
    };

    let index = {
        let config = config.clone();
        warp::path::end()
            .and(warp::ws())
            .and(warp::any().map(move || adapter.clone()))
            .and(warp::any().map(move || with_logger.clone()))
            .and(warp::any().map(move || config.clone()))
            .map(
                |ws: Ws, adapter: Arc<S>, with_logger: WithLogger, config: Config| {
                    ws.on_upgrade(move |conn| async move {
                        info!(with_logger.logger, "websocket user connected");
                        handle_connection(
                            conn,
                            adapter,
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
        async {
            let _ = crate::agent::EXIT.subscribe().changed().await;
        },
    );

    info!(logger, "starting api server"; "listen address" => config.listen_address.clone());

    tokio::task::spawn(serve).await.map_err(|e| e.into())
}
