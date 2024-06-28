use {
    super::{
        super::{
            pyth::{
                Conf,
                NotifyPrice,
                NotifyPriceSched,
                Price,
                PriceAccount,
                PriceAccountMetadata,
                PriceUpdate,
                ProductAccount,
                ProductAccountMetadata,
                PublisherAccount,
                SubscriptionID,
            },
            solana::network::Network,
        },
        global::{
            AllAccountsData,
            GlobalStore,
            Update,
        },
        local::{
            self,
            LocalStore,
        },
        oracle::{
            PriceEntry,
            ProductEntry,
        },
        Config,
        NotifyPriceSchedSubscription,
        NotifyPriceSubscription,
        State,
    },
    anyhow::{
        anyhow,
        Result,
    },
    chrono::Utc,
    pyth_sdk::Identifier,
    pyth_sdk_solana::state::{
        PriceComp,
        PriceStatus,
    },
    std::{
        collections::HashMap,
        sync::atomic::AtomicI64,
        time::Duration,
    },
    tokio::sync::{
        mpsc,
        RwLock,
    },
};

// TODO: implement Display on PriceStatus and then just call PriceStatus::to_string
fn price_status_to_str(price_status: PriceStatus) -> String {
    match price_status {
        PriceStatus::Unknown => "unknown",
        PriceStatus::Trading => "trading",
        PriceStatus::Halted => "halted",
        PriceStatus::Auction => "auction",
        PriceStatus::Ignored => "ignored",
    }
    .to_string()
}

fn solana_product_account_to_pythd_api_product_account(
    product_account: &ProductEntry,
    all_accounts_data: &AllAccountsData,
    product_account_key: &solana_sdk::pubkey::Pubkey,
) -> ProductAccount {
    // Extract all the price accounts from the product account
    let price_accounts = product_account
        .price_accounts
        .iter()
        .filter_map(|price_account_key| {
            all_accounts_data
                .price_accounts
                .get(price_account_key)
                .map(|acc| (price_account_key, acc))
        })
        .map(|(price_account_key, price_account)| {
            solana_price_account_to_pythd_api_price_account(price_account_key, price_account)
        })
        .collect();

    // Create the product account metadata struct
    ProductAccount {
        account: product_account_key.to_string(),
        attr_dict: product_account
            .account_data
            .iter()
            .filter(|(key, val)| !key.is_empty() && !val.is_empty())
            .map(|(key, val)| (key.to_owned(), val.to_owned()))
            .collect(),
        price_accounts,
    }
}

fn solana_price_account_to_pythd_api_price_account(
    price_account_key: &solana_sdk::pubkey::Pubkey,
    price_account: &PriceEntry,
) -> PriceAccount {
    PriceAccount {
        account:            price_account_key.to_string(),
        price_type:         "price".to_string(),
        price_exponent:     price_account.expo as i64,
        status:             price_status_to_str(price_account.agg.status),
        price:              price_account.agg.price,
        conf:               price_account.agg.conf,
        twap:               price_account.ema_price.val,
        twac:               price_account.ema_conf.val,
        valid_slot:         price_account.valid_slot,
        pub_slot:           price_account.agg.pub_slot,
        prev_slot:          price_account.prev_slot,
        prev_price:         price_account.prev_price,
        prev_conf:          price_account.prev_conf,
        publisher_accounts: price_account
            .comp
            .iter()
            .filter(|comp| *comp != &PriceComp::default())
            .map(|comp| PublisherAccount {
                account: comp.publisher.to_string(),
                status:  price_status_to_str(comp.agg.status),
                price:   comp.agg.price,
                conf:    comp.agg.conf,
                slot:    comp.agg.pub_slot,
            })
            .collect(),
    }
}

type PriceSubscriptions = HashMap<pyth_sdk::Identifier, Vec<NotifyPriceSubscription>>;
type PriceSchedSubscribtions = HashMap<pyth_sdk::Identifier, Vec<NotifyPriceSchedSubscription>>;

#[derive(Default)]
pub struct PricesState {
    subscription_id_seq:                  AtomicI64,
    notify_price_sched_interval_duration: Duration,
    notify_price_subscriptions:           RwLock<PriceSubscriptions>,
    notify_price_sched_subscriptions:     RwLock<PriceSchedSubscribtions>,
}

impl PricesState {
    pub fn new(config: Config) -> Self {
        Self {
            subscription_id_seq:                  1.into(),
            notify_price_sched_interval_duration: config.notify_price_sched_interval_duration,
            notify_price_subscriptions:           Default::default(),
            notify_price_sched_subscriptions:     Default::default(),
        }
    }
}

#[async_trait::async_trait]
pub trait Prices {
    async fn get_product_list(&self) -> Result<Vec<ProductAccountMetadata>>;
    async fn get_all_products(&self) -> Result<Vec<ProductAccount>>;
    async fn get_product(
        &self,
        product_account_key: &solana_sdk::pubkey::Pubkey,
    ) -> Result<ProductAccount>;
    async fn subscribe_price_sched(
        &self,
        account_pubkey: &solana_sdk::pubkey::Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
    ) -> SubscriptionID;
    async fn subscribe_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
    ) -> SubscriptionID;
    async fn send_notify_price_sched(&self) -> Result<()>;
    async fn drop_closed_subscriptions(&self);
    async fn update_local_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        price: Price,
        conf: Conf,
        status: String,
    ) -> Result<()>;
    async fn update_global_price(&self, network: Network, update: &Update) -> Result<()>;
    fn notify_interval_duration(&self) -> Duration;
    fn next_subscription_id(&self) -> SubscriptionID;
    fn map_status(status: &str) -> Result<PriceStatus>;
}

// Allow downcasting State into Keypairs for functions that depend on the `Keypairs` service.
impl<'a> From<&'a State> for &'a PricesState {
    fn from(state: &'a State) -> &'a PricesState {
        &state.prices
    }
}

#[async_trait::async_trait]
impl<T> Prices for T
where
    for<'a> &'a T: Into<&'a PricesState>,
    T: GlobalStore,
    T: LocalStore,
    T: Sync,
{
    async fn get_product_list(&self) -> Result<Vec<ProductAccountMetadata>> {
        let all_accounts_metadata = GlobalStore::accounts_metadata(self).await?;
        let mut result = Vec::new();
        for (product_account_key, product_account) in
            all_accounts_metadata.product_accounts_metadata
        {
            // Transform the price accounts into the API PriceAccountMetadata structs
            // the API uses.
            let price_accounts_metadata = product_account
                .price_accounts
                .iter()
                .filter_map(|price_account_key| {
                    all_accounts_metadata
                        .price_accounts_metadata
                        .get(price_account_key)
                        .map(|acc| (price_account_key, acc))
                })
                .map(|(price_account_key, price_account)| PriceAccountMetadata {
                    account:        price_account_key.to_string(),
                    price_type:     "price".to_owned(),
                    price_exponent: price_account.expo as i64,
                })
                .collect();

            // Create the product account metadata struct
            result.push(ProductAccountMetadata {
                account:   product_account_key.to_string(),
                attr_dict: product_account.attr_dict,
                price:     price_accounts_metadata,
            })
        }

        Ok(result)
    }

    async fn get_all_products(&self) -> Result<Vec<ProductAccount>> {
        let solana_data = GlobalStore::accounts_data(self, Network::Primary).await?;
        let mut result = Vec::new();
        for (product_account_key, product_account) in &solana_data.product_accounts {
            let product_account_api = solana_product_account_to_pythd_api_product_account(
                product_account,
                &solana_data,
                product_account_key,
            );

            result.push(product_account_api)
        }

        Ok(result)
    }

    async fn get_product(
        &self,
        product_account_key: &solana_sdk::pubkey::Pubkey,
    ) -> Result<ProductAccount> {
        let all_accounts_data = GlobalStore::accounts_data(self, Network::Primary).await?;

        // Look up the product account
        let product_account = all_accounts_data
            .product_accounts
            .get(product_account_key)
            .ok_or_else(|| anyhow!("product account not found"))?;

        Ok(solana_product_account_to_pythd_api_product_account(
            product_account,
            &all_accounts_data,
            product_account_key,
        ))
    }

    async fn subscribe_price_sched(
        &self,
        account_pubkey: &solana_sdk::pubkey::Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
    ) -> SubscriptionID {
        let subscription_id = self.next_subscription_id();
        self.into()
            .notify_price_sched_subscriptions
            .write()
            .await
            .entry(Identifier::new(account_pubkey.to_bytes()))
            .or_default()
            .push(NotifyPriceSchedSubscription {
                subscription_id,
                notify_price_sched_tx,
            });
        subscription_id
    }

    fn next_subscription_id(&self) -> SubscriptionID {
        self.into()
            .subscription_id_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    async fn subscribe_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
    ) -> SubscriptionID {
        let subscription_id = self.next_subscription_id();
        self.into()
            .notify_price_subscriptions
            .write()
            .await
            .entry(Identifier::new(account.to_bytes()))
            .or_default()
            .push(NotifyPriceSubscription {
                subscription_id,
                notify_price_tx,
            });
        subscription_id
    }

    async fn send_notify_price_sched(&self) -> Result<()> {
        for subscription in self
            .into()
            .notify_price_sched_subscriptions
            .read()
            .await
            .values()
            .flatten()
        {
            // Send the notify price sched update without awaiting. This results in raising errors
            // if the channel is full which normally should not happen. This is because we do not
            // want to block the API if the channel is full.
            subscription
                .notify_price_sched_tx
                .try_send(NotifyPriceSched {
                    subscription: subscription.subscription_id,
                })?;
        }

        Ok(())
    }

    async fn drop_closed_subscriptions(&self) {
        for subscriptions in self
            .into()
            .notify_price_subscriptions
            .write()
            .await
            .values_mut()
        {
            subscriptions.retain(|subscription| !subscription.notify_price_tx.is_closed())
        }

        for subscriptions in self
            .into()
            .notify_price_sched_subscriptions
            .write()
            .await
            .values_mut()
        {
            subscriptions.retain(|subscription| !subscription.notify_price_sched_tx.is_closed())
        }
    }

    async fn update_local_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        price: Price,
        conf: Conf,
        status: String,
    ) -> Result<()> {
        LocalStore::update(
            self,
            pyth_sdk::Identifier::new(account.to_bytes()),
            local::PriceInfo {
                status: State::map_status(&status)?,
                price,
                conf,
                timestamp: Utc::now().naive_utc(),
            },
        )
        .await
        .map_err(|_| anyhow!("failed to send update to local store"))
    }

    async fn update_global_price(&self, network: Network, update: &Update) -> Result<()> {
        GlobalStore::update(self, network, update)
            .await
            .map_err(|_| anyhow!("failed to send update to global store"))?;

        // Additionally, if the update is for a PriceAccount, we can notify our
        // subscribers that the account has changed. We only notify when this is
        // an update by the primary network as the account data might differ on
        // the secondary network.
        match (network, update) {
            (
                Network::Primary,
                Update::PriceAccountUpdate {
                    account_key,
                    account,
                },
            ) => {
                // Look up any subcriptions associated with the price identifier
                let empty = Vec::new();
                let subscriptions = self.into().notify_price_subscriptions.read().await;
                let subscriptions = subscriptions
                    .get(&Identifier::new(account_key.to_bytes()))
                    .unwrap_or(&empty);

                // Send the Notify Price update to each subscription
                for subscription in subscriptions {
                    // Send the notify price update without awaiting. This results in raising errors if the
                    // channel is full which normally should not happen. This is because we do not want to
                    // block the APIO if the channel is full.
                    subscription.notify_price_tx.try_send(NotifyPrice {
                        subscription: subscription.subscription_id,
                        result:       PriceUpdate {
                            price:      account.agg.price,
                            conf:       account.agg.conf,
                            status:     price_status_to_str(account.agg.status),
                            valid_slot: account.valid_slot,
                            pub_slot:   account.agg.pub_slot,
                        },
                    })?;
                }

                Ok(())
            }

            _ => Ok(()),
        }
    }

    fn notify_interval_duration(&self) -> Duration {
        self.into().notify_price_sched_interval_duration
    }

    // TODO: implement FromStr method on PriceStatus
    fn map_status(status: &str) -> Result<PriceStatus> {
        match status {
            "unknown" => Ok(PriceStatus::Unknown),
            "trading" => Ok(PriceStatus::Trading),
            "halted" => Ok(PriceStatus::Halted),
            "auction" => Ok(PriceStatus::Auction),
            "ignored" => Ok(PriceStatus::Ignored),
            _ => Err(anyhow!("invalid price status: {:#?}", status)),
        }
    }
}
