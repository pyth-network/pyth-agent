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

}
