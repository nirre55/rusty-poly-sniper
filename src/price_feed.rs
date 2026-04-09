use anyhow::{anyhow, Result};
use futures::StreamExt;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Événement de prix reçu via WebSocket.
#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub asset_id: String,
    pub price: f64,
    pub timestamp: i64,
}

/// Lance un stream WebSocket Polymarket et envoie les PriceUpdate dans le channel.
/// Retourne quand le stream se ferme (déconnexion ou erreur).
pub async fn stream_prices(
    asset_ids: Vec<String>,
    tx: mpsc::Sender<PriceUpdate>,
) -> Result<()> {
    let ws_client = WsClient::default();

    info!(
        "[PRICE FEED] Souscription aux prix pour {} tokens",
        asset_ids.len()
    );

    let stream = ws_client
        .subscribe_prices(asset_ids.clone())
        .map_err(|e| anyhow!("Erreur souscription prix WS: {}", e))?;

    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        match result {
            Ok(price_change) => {
                for entry in &price_change.price_changes {
                    let price_f64: f64 = match entry.price.to_string().parse() {
                        Ok(p) => p,
                        Err(_) => {
                            warn!(
                                "[PRICE FEED] Prix non parseable: {} pour {}",
                                entry.price, entry.asset_id
                            );
                            continue;
                        }
                    };

                    let update = PriceUpdate {
                        asset_id: entry.asset_id.clone(),
                        price: price_f64,
                        timestamp: price_change.timestamp,
                    };

                    if tx.try_send(update).is_err() {
                        warn!("[PRICE FEED] Channel saturé — prix droppé");
                    }
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
