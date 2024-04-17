use {
    serde::{
        Deserialize,
        Serialize,
    },
    std::collections::BTreeMap,
};

pub type Pubkey = String;
pub type Attrs = BTreeMap<String, String>;

pub type Price = i64;
pub type Exponent = i64;
pub type Conf = u64;
pub type Slot = u64;

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct ProductAccountMetadata {
    pub account:   Pubkey,
    pub attr_dict: Attrs,
    pub price:     Vec<PriceAccountMetadata>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct PriceAccountMetadata {
    pub account:        Pubkey,
    pub price_type:     String,
    pub price_exponent: Exponent,
}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct ProductAccount {
    pub account:        Pubkey,
    pub attr_dict:      Attrs,
    pub price_accounts: Vec<PriceAccount>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct PriceAccount {
    pub account:            Pubkey,
    pub price_type:         String,
    pub price_exponent:     Exponent,
    pub status:             String,
    pub price:              Price,
    pub conf:               Conf,
    pub twap:               Price,
    pub twac:               Price,
    pub valid_slot:         Slot,
    pub pub_slot:           Slot,
    pub prev_slot:          Slot,
    pub prev_price:         Price,
    pub prev_conf:          Conf,
    pub publisher_accounts: Vec<PublisherAccount>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct PublisherAccount {
    pub account: Pubkey,
    pub status:  String,
    pub price:   Price,
    pub conf:    Conf,
    pub slot:    Slot,
}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct NotifyPrice {
    pub subscription: SubscriptionID,
    pub result:       PriceUpdate,
}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct NotifyPriceSched {
    pub subscription: SubscriptionID,
}

pub type SubscriptionID = i64;

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct PriceUpdate {
    pub price:      Price,
    pub conf:       Conf,
    pub status:     String,
    pub valid_slot: Slot,
    pub pub_slot:   Slot,
}

pub mod rpc;
