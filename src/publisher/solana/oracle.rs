// This module is responsible for loading the current state of the
// on-chain Oracle program accounts from Solana.

use {
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

#[derive(Default, Debug, Clone)]
pub struct Data {
    pub mapping_accounts: HashMap<Pubkey, MappingAccount>,
    pub product_accounts: HashMap<Pubkey, ProductAccount>,
    pub price_accounts:   HashMap<Pubkey, PriceAccount>,
}

pub type MappingAccount = pyth_sdk_solana::state::MappingAccount;
#[derive(Debug, Clone)]
pub struct ProductAccount {
    pub account_data:   pyth_sdk_solana::state::ProductAccount,
    pub price_accounts: Vec<Pubkey>,
}
pub type PriceAccount = pyth_sdk_solana::state::PriceAccount;
