// The Global Store stores a copy of all the product and price information held in the Pyth
// on-chain aggregation contracts, across both the primary and secondary networks.
// This enables this data to be easily queried by other components.

use {
    super::{
        super::solana::oracle::{
            self,
            PriceAccount,
            ProductAccount,
        },
        PriceIdentifier,
    },
    anyhow::Result,
    pyth_sdk::PriceFeed,
    std::collections::{
        BTreeMap,
        HashMap,
    },
    tokio::sync::oneshot,
};

/// AllAccountsMetadata contains the metadata for all the price and product accounts,
/// merged from both networks.
///
/// Crucially, this relies on the metadata for all accounts being consistent across both networks.
#[derive(Debug)]
pub struct AllAccountsMetadata {
    pub product_accounts_metadata: HashMap<solana_sdk::pubkey::Pubkey, ProductAccountMetadata>,
    pub price_accounts_metadata:   HashMap<solana_sdk::pubkey::Pubkey, PriceAccountMetadata>,
}

/// ProductAccountMetadata contains the metadata for a product account.
#[derive(Debug)]
pub struct ProductAccountMetadata {
    /// Attribute dictionary
    pub attr_dict:      BTreeMap<String, String>,
    /// Price accounts associated with this product
    pub price_accounts: Vec<solana_sdk::pubkey::Pubkey>,
}

/// PriceAccountMetadata contains the metadata for a price account.
#[derive(Debug)]
pub struct PriceAccountMetadata {
    /// Exponent
    pub expo: i32,
}

#[derive(Debug)]
pub enum Message {
    ProductAccountUpdate {
        account_key: solana_sdk::pubkey::Pubkey,
        account:     ProductAccount,
    },
    PriceAccountUpdate {
        account_key: solana_sdk::pubkey::Pubkey,
        account:     PriceAccount,
    },
    LookupAllAccountsMetadata {
        result_tx: oneshot::Sender<Result<AllAccountsMetadata>>,
    },
    LookupPriceFeed {
        identifier: PriceIdentifier,
        result_tx:  oneshot::Sender<Result<PriceFeed>>,
    },
    LookupSolanaOracleData {
        result_tx: oneshot::Sender<Result<oracle::Data>>,
    },
}
