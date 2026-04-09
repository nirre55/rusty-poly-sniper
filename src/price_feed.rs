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
    /// Prix du marché (dernier trade réel)
    pub price: f64,
    /// Meilleur bid (prix auquel on peut vendre) — optionnel
    pub best_bid: f64,
    /// Meilleur ask (prix auquel on peut acheter) — optionnel
    pub best_ask: f64,
}

/// Cache partagé du dernier prix par asset_id.
pub type PriceCache = Arc<RwLock<HashMap<String, TokenPrice>>>;

pub fn new_price_cache() -> PriceCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Lance un stream WebSocket Polymarket price_change.
/// Met à jour le PriceCache avec le prix réel et signale le main loop.
pub async fn stream_prices(
    asset_ids: Vec<String>,
    cache: PriceCache,
    tx: mpsc::Sender<()>,
) -> Result<()> {
    let ws_client = WsClient::default();

    info!(
        "[PRICE FEED] Souscription price_change pour {} tokens",
        asset_ids.len()
    );

    let stream = ws_client
        .subscribe_prices(asset_ids.clone())
        .map_err(|e| anyhow!("Erreur souscription price WS: {}", e))?;

    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        match result {
            Ok(price_change) => {
                let mut updated = false;

                for entry in &price_change.price_changes {
                    let price: f64 = entry.price.to_string().parse().unwrap_or(0.0);
                    if price <= 0.0 {
                        continue;
                    }

                    let best_bid: f64 = entry
                        .best_bid
                        .as_ref()
                        .and_then(|d| d.to_string().parse().ok())
                        .unwrap_or(0.0);

                    let best_ask: f64 = entry
                        .best_ask
                        .as_ref()
                        .and_then(|d| d.to_string().parse().ok())
                        .unwrap_or(0.0);

                    debug!(
                        "[PRICE] {} price={:.2}¢ bid={:.2}¢ ask={:.2}¢",
                        &entry.asset_id[..8.min(entry.asset_id.len())],
                        price * 100.0,
                        best_bid * 100.0,
                        best_ask * 100.0
                    );

                    if let Ok(mut c) = cache.write() {
                        let tp = c.entry(entry.asset_id.clone()).or_default();
                        tp.price = price;
                        if best_bid > 0.0 {
                            tp.best_bid = best_bid;
                        }
                        if best_ask > 0.0 {
                            tp.best_ask = best_ask;
                        }
                    }
                    updated = true;
                }

                if updated {
                    let _ = tx.try_send(());
                }
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
