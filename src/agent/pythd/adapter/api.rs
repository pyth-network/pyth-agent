use {
    super::{
        Adapter,
        NotifyPriceSchedSubscription,
        NotifyPriceSubscription,
    },
    crate::agent::{
        pythd::api::{
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
        solana::{
            self,
            oracle::PriceEntry,
        },
        store::{
            global::{
                self,
                AllAccountsData,
                AllAccountsMetadata,
            },
            local,
            PriceIdentifier,
        },
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
    std::sync::Arc,
    tokio::sync::{
        broadcast,
        mpsc,
        oneshot,
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
    product_account: &solana::oracle::ProductEntry,
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

#[async_trait::async_trait]
pub trait AdapterApi {
    async fn get_product_list(&self) -> Result<Vec<ProductAccountMetadata>>;
    async fn lookup_all_accounts_metadata(&self) -> Result<AllAccountsMetadata>;
    async fn get_all_products(&self) -> Result<Vec<ProductAccount>>;
    async fn lookup_all_accounts_data(&self) -> Result<AllAccountsData>;
    async fn get_product(
        &self,
        product_account_key: &solana_sdk::pubkey::Pubkey,
    ) -> Result<ProductAccount>;
    async fn subscribe_price_sched(
        &self,
        account_pubkey: &solana_sdk::pubkey::Pubkey,
        notify_price_sched_tx: mpsc::Sender<NotifyPriceSched>,
    ) -> SubscriptionID;
    fn next_subscription_id(&self) -> SubscriptionID;
    async fn subscribe_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
    ) -> SubscriptionID;
    async fn send_notify_price_sched(&self) -> Result<()>;
    async fn drop_closed_subscriptions(&self);
    async fn update_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        price: Price,
        conf: Conf,
        status: String,
    ) -> Result<()>;
    // TODO: implement FromStr method on PriceStatus
    fn map_status(status: &str) -> Result<PriceStatus>;
    async fn global_store_update(
        &self,
        price_identifier: PriceIdentifier,
        price: i64,
        conf: u64,
        status: PriceStatus,
        valid_slot: u64,
        pub_slot: u64,
    ) -> Result<()>;
}

pub async fn notifier(adapter: Arc<Adapter>, mut shutdown_rx: broadcast::Receiver<()>) {
    let mut interval = tokio::time::interval(adapter.notify_price_sched_interval_duration);
    loop {
        adapter.drop_closed_subscriptions().await;
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!(adapter.logger, "shutdown signal received");
                return;
            }
            _ = interval.tick() => {
                if let Err(err) = adapter.send_notify_price_sched().await {
                    error!(adapter.logger, "{}", err);
                    debug!(adapter.logger, "error context"; "context" => format!("{:?}", err));
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AdapterApi for Adapter {
    async fn get_product_list(&self) -> Result<Vec<ProductAccountMetadata>> {
        let all_accounts_metadata = self.lookup_all_accounts_metadata().await?;

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

    // Fetches the Solana-specific Oracle data from the global store
    async fn lookup_all_accounts_metadata(&self) -> Result<global::AllAccountsMetadata> {
        let (result_tx, result_rx) = oneshot::channel();
        self.global_store_lookup_tx
            .send(global::Lookup::LookupAllAccountsMetadata { result_tx })
            .await?;
        result_rx.await?
    }

    async fn get_all_products(&self) -> Result<Vec<ProductAccount>> {
        let solana_data = self.lookup_all_accounts_data().await?;

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

    async fn lookup_all_accounts_data(&self) -> Result<AllAccountsData> {
        let (result_tx, result_rx) = oneshot::channel();
        self.global_store_lookup_tx
            .send(global::Lookup::LookupAllAccountsData {
                network: solana::network::Network::Primary,
                result_tx,
            })
            .await?;
        result_rx.await?
    }

    async fn get_product(
        &self,
        product_account_key: &solana_sdk::pubkey::Pubkey,
    ) -> Result<ProductAccount> {
        let all_accounts_data = self.lookup_all_accounts_data().await?;

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
        self.notify_price_sched_subscriptions
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
        self.subscription_id_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    async fn subscribe_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        notify_price_tx: mpsc::Sender<NotifyPrice>,
    ) -> SubscriptionID {
        let subscription_id = self.next_subscription_id();
        self.notify_price_subscriptions
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
            .notify_price_sched_subscriptions
            .read()
            .await
            .values()
            .flatten()
        {
            // Send the notify price sched update without awaiting. This results in raising errors
            // if the channel is full which normally should not happen. This is because we do not
            // want to block the adapter if the channel is full.
            subscription
                .notify_price_sched_tx
                .try_send(NotifyPriceSched {
                    subscription: subscription.subscription_id,
                })?;
        }

        Ok(())
    }

    async fn drop_closed_subscriptions(&self) {
        for subscriptions in self.notify_price_subscriptions.write().await.values_mut() {
            subscriptions.retain(|subscription| !subscription.notify_price_tx.is_closed())
        }

        for subscriptions in self
            .notify_price_sched_subscriptions
            .write()
            .await
            .values_mut()
        {
            subscriptions.retain(|subscription| !subscription.notify_price_sched_tx.is_closed())
        }
    }

    async fn update_price(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        price: Price,
        conf: Conf,
        status: String,
    ) -> Result<()> {
        self.local_store_tx
            .send(local::Message::Update {
                price_identifier: pyth_sdk::Identifier::new(account.to_bytes()),
                price_info:       local::PriceInfo {
                    status: Adapter::map_status(&status)?,
                    price,
                    conf,
                    timestamp: Utc::now().timestamp(),
                },
            })
            .await
            .map_err(|_| anyhow!("failed to send update to local store"))
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

    async fn global_store_update(
        &self,
        price_identifier: PriceIdentifier,
        price: i64,
        conf: u64,
        status: PriceStatus,
        valid_slot: u64,
        pub_slot: u64,
    ) -> Result<()> {
        // Look up any subcriptions associated with the price identifier
        let empty = Vec::new();
        let subscriptions = self.notify_price_subscriptions.read().await;
        let subscriptions = subscriptions.get(&price_identifier).unwrap_or(&empty);

        // Send the Notify Price update to each subscription
        for subscription in subscriptions {
            // Send the notify price update without awaiting. This results in raising errors if the
            // channel is full which normally should not happen. This is because we do not want to
            // block the adapter if the channel is full.
            subscription.notify_price_tx.try_send(NotifyPrice {
                subscription: subscription.subscription_id,
                result:       PriceUpdate {
                    price,
                    conf,
                    status: price_status_to_str(status),
                    valid_slot,
                    pub_slot,
                },
            })?;
        }

        Ok(())
    }
}
