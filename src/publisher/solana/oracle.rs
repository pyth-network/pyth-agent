// This module is responsible for loading the current state of the
// on-chain Oracle program accounts from Solana.

use std::collections::HashMap;

use solana_sdk::pubkey::Pubkey;

#[derive(Default, Debug, Clone)]
pub struct Data {
    pub mapping_accounts: MappingAccounts,
    pub product_accounts: ProductAccounts,
    pub price_accounts: PriceAccounts,
}

pub type MappingAccounts = HashMap<Pubkey, MappingAccount>;
pub type ProductAccounts = HashMap<Pubkey, ProductAccount>;
pub type PriceAccounts = HashMap<Pubkey, PriceAccount>;

pub type MappingAccount = pyth_sdk_solana::state::MappingAccount;
#[derive(Debug, Clone)]
pub struct ProductAccount {
    pub account_data: pyth_sdk_solana::state::ProductAccount,
    pub price_accounts: Vec<Pubkey>,
}
pub type PriceAccount = pyth_sdk_solana::state::PriceAccount;
