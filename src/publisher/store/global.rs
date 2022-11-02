// This module is the "Global Store": storing a copy of all the information
// held in the global Pyth Network. This enables this data to be easily
// queried by other components.
//
// Note that each Global Store is network-specific: it does not try to combine information
// from Solana and PythNet.

use super::super::solana::oracle::{self, PriceAccount, ProductAccount};
use anyhow::Result;
use pyth_sdk::PriceFeed;
use tokio::sync::oneshot;

use super::PriceIdentifier;

#[derive(Debug)]
pub enum Message {
    ProductAccountUpdate {
        account_key: solana_sdk::pubkey::Pubkey,
        account: ProductAccount,
    },
    PriceAccountUpdate {
        account_key: solana_sdk::pubkey::Pubkey,
        account: PriceAccount,
    },
    LookupPriceFeed {
        identifier: PriceIdentifier,
        result_tx: oneshot::Sender<Result<PriceFeed>>,
    },
    LookupSolanaOracleData {
        result_tx: oneshot::Sender<Result<oracle::Data>>,
    },
}
