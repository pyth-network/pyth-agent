use std::{
    collections::{BTreeSet, HashMap, HashSet},
    time::Instant,
};

use pyth_sdk::{Identifier, PriceIdentifier};
use typed_html::{dom::DOMTree, elements::TableContent};
use warp::{
    reply::{self, WithStatus},
    Rejection, Reply,
};

use solana_sdk::pubkey::Pubkey;

use super::{
    solana::oracle::{self, PriceAccount, ProductAccount},
    store::{
        global::{AllAccountsData, AllAccountsMetadata, Lookup, PriceAccountMetadata},
        local::{Message, PriceInfo},
    },
};

use {
    prometheus::Registry,
    std::{collections::BTreeMap, net::SocketAddr, sync::Arc},
    tokio::sync::{mpsc, oneshot, Mutex},
    typed_html::{html, text},
    warp::{hyper::StatusCode, Filter},
};

/// Internal metrics server state, holds state needed for serving
/// dashboard and metrics.
pub struct MetricsServer {
    /// Used to pull the state of all symbols in local store
    local_store_tx: mpsc::Sender<Message>,
    global_store_lookup_tx: mpsc::Sender<Lookup>,
    /// Prometheus registry for enumerating all metrics for clients
    registry: Registry,
    start_time: Instant,
}

impl MetricsServer {
    /// Instantiate a metrics API with a dashboard
    pub async fn spawn(
        addr: impl Into<SocketAddr> + 'static,
        local_store_tx: mpsc::Sender<Message>,
        global_store_lookup_tx: mpsc::Sender<Lookup>,
    ) {
        let server = MetricsServer {
            local_store_tx,
            global_store_lookup_tx,
            registry: Registry::new(),
            start_time: Instant::now(),
        };

        let shared_state = Arc::new(Mutex::new(server));

        let dashboard_route = warp::path("dashboard")
            .and(warp::path::end())
            .and_then(move || {
                let shared_state = shared_state.clone();
                async move {
                    let response = shared_state
                        .lock()
                        .await
                        .render_dashboard()
                        .await
                        .unwrap_or_else(|e| {
                            // Add logging here

                            // Withhold failure details from client
                            "Could not render dashboard!".to_owned()
                        });
                    Result::<Box<dyn Reply>, Rejection>::Ok(Box::new(reply::with_status(
                        reply::html(response),
                        StatusCode::OK,
                    )))
                }
            });

        warp::serve(dashboard_route).bind(addr).await;
    }

    /// Create an HTML view of store data
    async fn render_dashboard(&self) -> Result<String, Box<dyn std::error::Error>> {
        // Prepare response channel for request
        let (local_tx, local_rx) = oneshot::channel();
        let (global_data_tx, global_data_rx) = oneshot::channel();
        let (global_metadata_tx, global_metadata_rx) = oneshot::channel();

        // Request price data from local store
        self.local_store_tx
            .send(Message::LookupAllPriceInfo {
                result_tx: local_tx,
            })
            .await?;

        self.global_store_lookup_tx
            .send(Lookup::LookupAllAccountsData {
                result_tx: global_data_tx,
            })
            .await?;

        self.global_store_lookup_tx
            .send(Lookup::LookupAllAccountsMetadata {
                result_tx: global_metadata_tx,
            })
            .await?;

        // Await the results
        let local_data = local_rx.await?;
        let global_data = global_data_rx.await??;
        let global_metadata = global_metadata_rx.await??;

        let symbol_view = build_dashboard_data(local_data, global_data, global_metadata);

        let mut uptime_seconds = self.start_time.elapsed().as_secs();

	// Shave bigger units off the seconds value. Decide the count
	// and replace with remainder.
        let uptime_days = uptime_seconds / (24 * 3600); // 24h
        uptime_seconds %= 24 * 3600;

        let uptime_hours = uptime_seconds / 3600; // 1h
        uptime_seconds %= 3600;

        let uptime_minutes = uptime_seconds / 60; // 1min
        uptime_seconds %= 60;

        let uptime_string = format!("{}d{}h{}m{}s", uptime_days, uptime_hours, uptime_minutes, uptime_seconds);

        // Build and collect table rows
        let mut rows = vec![];

        for (symbol, data) in symbol_view {
            for (price_pubkey, price_data) in data.prices {
                let price_string = if let Some(global_data) = price_data.global_data {
                    let expo = global_data.expo;
                    let price_with_expo: f64 = (global_data.agg.price as f64 * 10f64).powi(expo);

                    format!("{:.2}", price_with_expo)
                } else {
                    "no data".to_string()
                };

                let row_snippet = html! {
                    <tr>
                        <td>{text!(symbol.clone())}</td>
                <td>{text!(price_pubkey.to_string())}</td>
                <td>{text!(price_string)}</td>
                        <td>"placeholder"</td>
                    </tr>
                    };
                rows.push(row_snippet);
            }
        }
        // for (id, data) in local_sorted.iter() {
        //     let row_snippet = html! {
        //     <tr>
        //         <td>{text!(id.to_string())}</td>
        //         <td>"placeholder"</td>
        //         <td>"placeholder"</td>
        //         <td>"placeholder"</td>
        //     </tr>
        //     };
        //     rows.push(row_snippet);
        // }

        rows.push(html! {
        <tr>
            <td>"Crypto.USDBTC"</td>
            <td>"3024.14"</td>
            <td>"15 minutes, 34 seconds ago"</td>
            <td>"1 seconds ago"</td>
        </tr>
        });
        let title_string = concat!("Pyth Agent Dashboard - ", env!("CARGO_PKG_VERSION"));
        let res_html: DOMTree<String> = html! {
        <html>
            <head>
            <title>{text!(title_string)}</title>
        <style>
            """
table {
  width: 100%;
  border-collapse: collapse;
}
table, th, td {
  border: 1px solid;
}
"""
        </style>
            </head>
            <body>
            <h1>{text!(title_string)}</h1>
        {text!("Uptime: {}", uptime_string)}
            <h2>"State Overview"</h2>
            <table>
            <tr>
                <th>"Symbol"</th>
                <th>"Published Price"</th>
                <th>"Last Updated"</th>
                <th>"Last Published"</th>
            </tr>
            { rows }
        </table>
            </body>
        </html>
        };
        Ok(res_html.to_string())
    }
}

pub struct DashboardSymbolView {
    product: Pubkey,
    prices: BTreeMap<Pubkey, DashboardPriceView>,
}

pub struct DashboardPriceView {
    local_data: Option<PriceInfo>,
    global_data: Option<PriceAccount>,
    global_metadata: Option<PriceAccountMetadata>,
}

/// Turn global/local store state into a single per-symbol view.
///
/// The dashboard data comes from three sources - the global store
/// (observed on-chain state) data, global store metadata and local
/// store data (local state possibly not yet committed to the oracle
/// contract).
///
/// The view is indexed by human-readable symbol name or a stringified
/// public key if symbol name can't be found.
pub fn build_dashboard_data(
    mut local_data: HashMap<PriceIdentifier, PriceInfo>,
    mut global_data: AllAccountsData,
    mut global_metadata: AllAccountsMetadata,
) -> BTreeMap<String, DashboardSymbolView> {
    let mut ret = BTreeMap::new();

    // Learn all the product/price keys in the system,
    let all_product_keys_iter = global_metadata.product_accounts_metadata.keys().cloned();

    let all_product_keys_dedup = all_product_keys_iter.collect::<HashSet<Pubkey>>();

    let all_price_keys_iter = global_data
        .price_accounts
        .keys()
        .chain(global_metadata.price_accounts_metadata.keys())
        .cloned()
        .chain(local_data.keys().map(|identifier| {
            let bytes = identifier.to_bytes();
            Pubkey::new_from_array(bytes)
        }));

    let all_price_keys_dedup = all_price_keys_iter.collect::<HashSet<Pubkey>>();

    // query all the keys and assemvle them into the view

    for product_key in all_product_keys_dedup {
        // global data and metadata; removing marks this symbol as "done"
        let product_data = global_data.product_accounts.remove(&product_key);

        if let Some(mut product_metadata) = global_metadata
            .product_accounts_metadata
            .remove(&product_key)
        {
            let symbol_name = product_metadata
                .attr_dict
                .get("symbol")
                .cloned()
                // Use product key for unnamed products
                .unwrap_or(format!("unnamed product {}", product_key));

            // Sort and deduplicate prices
            let this_product_price_keys_dedup = product_metadata
                .price_accounts
                .drain(0..)
                .collect::<BTreeSet<_>>();

            let mut prices = BTreeMap::new();

            // Extract information about each price
            for price_key in this_product_price_keys_dedup {
                let price_global_data = global_data.price_accounts.remove(&price_key);
                let price_global_metadata =
                    global_metadata.price_accounts_metadata.remove(&price_key);

                let price_identifier = Identifier::new(price_key.clone().to_bytes());
                let price_local_data = local_data.remove(&price_identifier);

                prices.insert(
                    price_key,
                    DashboardPriceView {
                        local_data: price_local_data,
                        global_data: price_global_data,
                        global_metadata: price_global_metadata,
                    },
                );
            }
        } else {
            // TODO(drozdziak1): log a missing product problem, which would suggest . We
            // expect that missing price information is possible if no
            // on-chain queries or publishing took place yet.
        }
    }

    return ret;
}
