use anyhow::{anyhow, Result};
use futures::StreamExt;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Dernier état de prix d'un token.
#[derive(Debug, Clone, Default)]
pub struct TokenPrice {
    pub best_bid: f64,
    pub best_ask: f64,
    /// Midpoint = (best_bid + best_ask) / 2
    pub price: f64,
}

/// Cache partagé du dernier prix par asset_id.
pub type PriceCache = Arc<RwLock<HashMap<String, TokenPrice>>>;

pub fn new_price_cache() -> PriceCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Lance un stream WebSocket Polymarket best_bid_ask (custom_feature_enabled).
/// Calcule le midpoint comme prix et signale le main loop.
pub async fn stream_prices(
    asset_ids: Vec<String>,
    cache: PriceCache,
    tx: mpsc::Sender<()>,
) -> Result<()> {
    let ws_client = WsClient::default();

    info!(
        "[PRICE FEED] Souscription best_bid_ask pour {} tokens",
        asset_ids.len()
    );

    let stream = ws_client
        .subscribe_best_bid_ask(asset_ids.clone())
        .map_err(|e| anyhow!("Erreur souscription best_bid_ask WS: {}", e))?;

    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        match result {
            Ok(bba) => {
                let best_bid: f64 = bba.best_bid.to_string().parse().unwrap_or(0.0);
                let best_ask: f64 = bba.best_ask.to_string().parse().unwrap_or(0.0);

                let price = if best_bid > 0.0 && best_ask > 0.0 {
                    (best_bid + best_ask) / 2.0
                } else if best_ask > 0.0 {
                    best_ask
                } else {
                    best_bid
                };

                debug!(
                    "[PRICE] {} bid={:.2}¢ ask={:.2}¢ mid={:.2}¢",
                    &bba.asset_id[..8.min(bba.asset_id.len())],
                    best_bid * 100.0,
                    best_ask * 100.0,
                    price * 100.0
                );

                if let Ok(mut c) = cache.write() {
                    c.insert(
                        bba.asset_id.clone(),
                        TokenPrice {
                            best_bid,
                            best_ask,
                            price,
                        },
                    );
                }
                let _ = tx.try_send(());
            }
            Err(e) => {
                error!("[PRICE FEED] Erreur WebSocket: {}", e);
                break;
            }
        }
    }

    warn!("[PRICE FEED] Stream fermé");
    Ok(())
}
