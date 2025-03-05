use {
    serde::{
        Deserialize,
        Serialize,
    },
    smol_str::SmolStr,
    std::{
        collections::BTreeMap,
        sync::Arc,
    },
};

pub mod rpc;

pub type Pubkey = SmolStr;
pub type Attrs = BTreeMap<SmolStr, SmolStr>;

pub type Price = i64;
pub type Exponent = i64;
pub type Conf = u64;
pub type Slot = u64;

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct ProductAccountMetadata {
    pub account:   Pubkey,
    pub attr_dict: Attrs,
    pub price:     Arc<[PriceAccountMetadata]>,
}

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct PriceAccountMetadata {
    pub account:        Pubkey,
    pub price_type:     SmolStr,
    pub price_exponent: Exponent,
}

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct ProductAccount {
    pub account:        Pubkey,
    pub attr_dict:      Attrs,
    pub price_accounts: Arc<[PriceAccount]>,
}

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct PriceAccount {
    pub account:            Pubkey,
    pub price_type:         SmolStr,
    pub price_exponent:     Exponent,
    pub status:             SmolStr,
    pub price:              Price,
    pub conf:               Conf,
    pub twap:               Price,
    pub twac:               Price,
    pub valid_slot:         Slot,
    pub pub_slot:           Slot,
    pub prev_slot:          Slot,
    pub prev_price:         Price,
    pub prev_conf:          Conf,
    pub publisher_accounts: Arc<[PublisherAccount]>,
}

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct PublisherAccount {
    pub account: Pubkey,
    pub status:  SmolStr,
    pub price:   Price,
    pub conf:    Conf,
    pub slot:    Slot,
}

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct NotifyPrice {
    pub subscription: SubscriptionID,
    pub result:       PriceUpdate,
}

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct NotifyPriceSched {
    pub subscription: SubscriptionID,
}

pub type SubscriptionID = i64;

#[derive(Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct PriceUpdate {
    pub price:      Price,
    pub conf:       Conf,
    pub status:     SmolStr,
    pub valid_slot: Slot,
    pub pub_slot:   Slot,
}
