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

