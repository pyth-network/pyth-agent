// The Global Store stores a copy of all the product and price information held in the Pyth
// on-chain aggregation contracts, across both the primary and secondary networks.
// This enables this data to be easily queried by other components.
use {
    super::super::solana::oracle::{
        self,
        PriceAccount,
        ProductAccount,
    },
    crate::publisher::pythd::adapter,
    anyhow::{
        anyhow,
        Result,
    },
    pyth_sdk::Identifier,
    slog::Logger,
    solana_sdk::pubkey::Pubkey,
    std::collections::{
        BTreeMap,
        HashMap,
    },
    tokio::sync::{
        mpsc,
        oneshot,
    },
};

/// AllAccountsData contains the full data for the price and product accounts, sourced
/// from the primary network.
#[derive(Debug, Clone)]
pub struct AllAccountsData {
    pub product_accounts: HashMap<Pubkey, oracle::ProductAccount>,
    pub price_accounts:   HashMap<Pubkey, oracle::PriceAccount>,
}

/// AllAccountsMetadata contains the metadata for all the price and product accounts.
///
/// Important: this relies on the metadata for all accounts being consistent across both networks.
#[derive(Debug, Clone)]
pub struct AllAccountsMetadata {
    pub product_accounts_metadata: HashMap<Pubkey, ProductAccountMetadata>,
    pub price_accounts_metadata:   HashMap<Pubkey, PriceAccountMetadata>,
}

/// ProductAccountMetadata contains the metadata for a product account.
#[derive(Debug, Clone)]
pub struct ProductAccountMetadata {
    /// Attribute dictionary
    pub attr_dict:      BTreeMap<String, String>,
    /// Price accounts associated with this product
    pub price_accounts: Vec<Pubkey>,
}

impl From<oracle::ProductAccount> for ProductAccountMetadata {
    fn from(product_account: oracle::ProductAccount) -> Self {
        ProductAccountMetadata {
            attr_dict:      product_account
                .account_data
                .iter()
                .map(|(key, val)| (key.to_owned(), val.to_owned()))
                .collect(),
            price_accounts: product_account.price_accounts,
        }
    }
}

/// PriceAccountMetadata contains the metadata for a price account.
#[derive(Debug, Clone)]
pub struct PriceAccountMetadata {
    /// Exponent
    pub expo: i32,
}

impl From<oracle::PriceAccount> for PriceAccountMetadata {
    fn from(price_account: oracle::PriceAccount) -> Self {
        PriceAccountMetadata {
            expo: price_account.expo,
        }
    }
}

#[derive(Debug)]
pub enum Update {
    ProductAccountUpdate {
        account_key: Pubkey,
        account:     ProductAccount,
    },
    PriceAccountUpdate {
        account_key: Pubkey,
        account:     PriceAccount,
    },
}

#[derive(Debug)]
pub enum Lookup {
    LookupAllAccountsMetadata {
        result_tx: oneshot::Sender<Result<AllAccountsMetadata>>,
    },
    LookupAllAccountsData {
        result_tx: oneshot::Sender<Result<AllAccountsData>>,
    },
}
