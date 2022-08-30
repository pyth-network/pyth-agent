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

