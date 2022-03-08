// This module is responsible for loading the current state of the
// on-chain Oracle program accounts from Solana.

use std::collections::HashMap;
use std::sync::Weak;

use anyhow::Result;
use async_trait::async_trait;
use pyth_sdk_solana::state::{
    load_mapping_account, load_price_account, load_product_account, AccKey, MappingAccount,
    PriceAccount, ProductAccount,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc};

use crate::publisher::store::global;

type MappingAccounts = HashMap<AccKey, MappingAccount>;
type ProductAccounts = HashMap<AccKey, ProductAccount>;
type PriceAccounts = HashMap<AccKey, PriceAccount>;

// Oracle is responsible for loading data stored in the Oracle.
pub struct Oracle {
    // Configuration
    rpc_client: RpcClient,
    mapping_account_key: AccKey,

    // The actual data
    data: Data,

    // Channel on which to communicate to the global store
    global_store_tx: mpsc::Sender<global::Message>,
}

#[derive(Clone)]
pub enum Update {
    ProductAccount {
        acc_key: AccKey,
        account: ProductAccount,
    },
    PriceAccount {
        acc_key: AccKey,
        account: PriceAccount,
    },
}

#[derive(Default)]
pub struct Data {
    mapping_accounts: MappingAccounts,
    product_accounts: ProductAccounts,
    price_accounts: PriceAccounts,
}

impl Oracle {
    pub fn new(rpc_endpoint: String, mapping_account_key: AccKey) -> Self {
        todo!();
        // Oracle {
        //     // TODO: websocket client
        //     // TODO: HTTPS support
        //     rpc_client: RpcClient::new(rpc_endpoint),
        //     mapping_account_key,
        //     data: Default::default(),
        //     observers: Default::default(),
        // }
    }

    pub fn update(&mut self) -> Result<()> {
        self.data.mapping_accounts = self.fetch_mapping_accounts()?;
        self.data.product_accounts = self.fetch_product_accounts()?;
        self.data.price_accounts = self.fetch_price_accounts()?;
        Ok(())
    }

    fn fetch_mapping_accounts(&self) -> Result<HashMap<AccKey, MappingAccount>> {
        let mut accounts = HashMap::new();

        let mut account_key = self.mapping_account_key;
        while account_key.is_valid() {
            let pub_key = Pubkey::new(&account_key.val);
            let data = self.rpc_client.get_account_data(&pub_key)?;
            let account = *load_mapping_account(&data)?;
            accounts.insert(account_key, account);

            account_key = account.next;
        }

        Ok(accounts)
    }

    fn fetch_product_accounts(&self) -> Result<HashMap<AccKey, ProductAccount>> {
        let mut accounts = HashMap::new();
        for mapping_account in self.data.mapping_accounts.values() {
            accounts.extend(self.fetch_product_accounts_from_mapping_account(mapping_account)?);
        }

        Ok(accounts)
    }

    fn fetch_product_accounts_from_mapping_account(
        &self,
        mapping_account: &MappingAccount,
    ) -> Result<HashMap<AccKey, ProductAccount>> {
        let mut accounts = HashMap::new();

        for account_key in mapping_account.products {
            let pub_key = Pubkey::new(&account_key.val);
            let data = self.rpc_client.get_account_data(&pub_key)?;
            let product_account = *load_product_account(&data)?;

            accounts.insert(account_key, product_account);
        }

        Ok(accounts)
    }

    fn fetch_price_accounts(&self) -> Result<HashMap<AccKey, PriceAccount>> {
        let mut accounts = HashMap::new();
        for product_account in self.data.product_accounts.values() {
            accounts.extend(self.fetch_price_accounts_from_product_account(product_account)?);
        }

        Ok(accounts)
    }

    fn fetch_price_accounts_from_product_account(
        &self,
        product_account: &ProductAccount,
    ) -> Result<HashMap<AccKey, PriceAccount>> {
        let mut accounts = HashMap::new();

        let mut account_key = product_account.px_acc;
        while account_key.is_valid() {
            let pub_key = Pubkey::new(&account_key.val);
            let data = self.rpc_client.get_account_data(&pub_key)?;
            let account = *load_price_account(&data)?;
            accounts.insert(account_key, account);

            account_key = account.next;
        }

        Ok(accounts)
    }
}
