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
    /// Meilleur prix d'achat (best bid) — prix auquel on peut vendre
    pub best_bid: f64,
    /// Meilleur prix de vente (best ask) — prix auquel on peut acheter
    pub best_ask: f64,
    /// Prix milieu (midpoint)
    pub mid: f64,
}

/// Cache partagé du dernier prix par asset_id.
pub type PriceCache = Arc<RwLock<HashMap<String, TokenPrice>>>;

pub fn new_price_cache() -> PriceCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Lance un stream WebSocket Polymarket orderbook.
/// Met à jour le PriceCache avec best_bid/best_ask et signale le main loop.
pub async fn stream_prices(
    asset_ids: Vec<String>,
    cache: PriceCache,
    tx: mpsc::Sender<()>,
) -> Result<()> {
    let ws_client = WsClient::default();

    info!(
        "[PRICE FEED] Souscription orderbook pour {} tokens",
        asset_ids.len()
    );

    let stream = ws_client
        .subscribe_orderbook(asset_ids.clone())
        .map_err(|e| anyhow!("Erreur souscription orderbook WS: {}", e))?;

    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        match result {
            Ok(book) => {
                let best_bid: f64 = book
                    .bids
                    .first()
                    .map(|b| b.price.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0);

                let best_ask: f64 = book
                    .asks
                    .first()
                    .map(|a| a.price.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0);

                let mid = if best_bid > 0.0 && best_ask > 0.0 {
                    (best_bid + best_ask) / 2.0
                } else {
                    best_bid.max(best_ask)
                };

                debug!(
                    "[BOOK] {} bid={:.2}¢ ask={:.2}¢ mid={:.2}¢",
                    &book.asset_id[..8],
                    best_bid * 100.0,
                    best_ask * 100.0,
                    mid * 100.0
                );

                if let Ok(mut c) = cache.write() {
                    c.insert(
                        book.asset_id.clone(),
                        TokenPrice {
                            best_bid,
                            best_ask,
                            mid,
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
