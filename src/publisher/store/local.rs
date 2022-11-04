// The Local Store stores a copy of all the price information this local publisher
// is contributing to the network. The Exporters will then take this data and publish
// it to the networks.

use {
    super::PriceIdentifier,
    pyth_sdk::{
        PriceStatus,
        UnixTimestamp,
    },
    std::collections::HashMap,
    tokio::sync::oneshot,
};

#[derive(Clone)]
pub struct PriceInfo {
    pub status:    PriceStatus,
    pub price:     i64,
    pub conf:      u64,
    pub timestamp: UnixTimestamp,
}

pub enum Message {
    Update {
        price_identifier: PriceIdentifier,
        price_info:       PriceInfo,
    },
    LookupAllPriceInfo {
        result_tx: oneshot::Sender<HashMap<PriceIdentifier, PriceInfo>>,
    },
}
