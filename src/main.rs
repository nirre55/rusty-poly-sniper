use anyhow::Result;
use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use rusty_poly_sniper::config::Config;
use rusty_poly_sniper::logger::{TradeLogger, TradeRecord};
use rusty_poly_sniper::money::MoneyManager;
use rusty_poly_sniper::polymarket::PolymarketClient;
use rusty_poly_sniper::position::{calculate_sl, calculate_tp, OpenPosition, PositionManager};
use rusty_poly_sniper::price_feed::{self, PriceUpdate};

/// Calcule le slug du marché actif à cet instant.
/// Le timestamp correspond au début de la bougie en cours (aligné sur l'intervalle).
fn current_market_slug(prefix: &str, interval_secs: i64) -> String {
    let now_secs = Utc::now().timestamp();
    let candle_open_secs = (now_secs / interval_secs) * interval_secs;
    format!("{}-{}", prefix, candle_open_secs)
}

/// Temps restant (en secondes) avant l'expiration du marché courant.
fn seconds_until_market_end(interval_secs: i64) -> i64 {
    let now_secs = Utc::now().timestamp();
    let candle_open_secs = (now_secs / interval_secs) * interval_secs;
    let candle_end_secs = candle_open_secs + interval_secs;
    candle_end_secs - now_secs
}

/// Slug du marché suivant (celui qui ouvre quand le courant expire).
fn next_market_slug(prefix: &str, interval_secs: i64) -> String {
    let now_secs = Utc::now().timestamp();
    let next_open_secs = (now_secs / interval_secs) * interval_secs + interval_secs;
    format!("{}-{}", prefix, next_open_secs)
}

/// Pré-fetch du marché suivant + warm caches SDK en arrière-plan.
fn spawn_prefetch_next_market(
    poly_client: &Arc<PolymarketClient>,
    prefix: &str,
    interval_secs: i64,
) {
    let poly = poly_client.clone();
    let slug = next_market_slug(prefix, interval_secs);
    tokio::spawn(async move {
        if let Ok(market) = poly.resolve_market(&slug).await {
            poly.warm_sdk_caches(&market).await;
            info!("[PREFETCH] Marché suivant pré-chargé: {}", slug);
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let interval_secs = config.interval_seconds()?;

    info!(
        "Démarrage rusty-poly-sniper | mode={:?} prefix={} interval={} entry={}¢ TP=+{}¢ SL=-{}¢ amount={:.2}USDC",
        config.execution_mode,
        config.polymarket_slug_prefix,
        config.interval,
        (config.entry_threshold * 100.0) as u32,
        config.tp_offset_cents,
        config.sl_offset_cents,
        config.trade_amount_usdc
    );

    let trade_logger = Arc::new(TradeLogger::new(&config.logs_dir)?);
    let poly_client = Arc::new(PolymarketClient::new(config.clone()));
    poly_client.warm_up().await;

    // Keep-alive CLOB
    tokio::spawn({
        let poly = poly_client.clone();
        async move { poly.run_keep_alive_loop().await }
    });

    let money_manager = Arc::new(tokio::sync::Mutex::new(MoneyManager::new(
        config.trade_amount_usdc,
        config.martingale_multiplier,
        config.martingale_max_amount,
        &config.logs_dir,
    )));

    let mut position_mgr = PositionManager::new(&config.logs_dir);

    // ── Boucle de résilience : un marché à la fois ───────────────────────────
    loop {
        let slug = current_market_slug(&config.polymarket_slug_prefix, interval_secs);
        let remaining = seconds_until_market_end(interval_secs);

        if remaining < 5 {
            info!("[MARKET] Marché {} expire dans {}s — attente du suivant", slug, remaining);
            tokio::time::sleep(std::time::Duration::from_secs(remaining as u64 + 1)).await;
            continue;
        }

        info!(
            "[MARKET] Résolution du marché: {} ({}s restants)",
            slug, remaining
        );

        let market = match poly_client.resolve_market(&slug).await {
            Ok(m) => m,
            Err(e) => {
                error!("Impossible de résoudre le marché {}: {}", slug, e);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };

        // Warm SDK caches
        poly_client.warm_sdk_caches(&market).await;

        info!(
            "[MARKET] {} | UP={} DOWN={}",
            market.slug,
            &market.up_token_id[..8],
            &market.down_token_id[..8]
        );

        // Gérer les positions résiduelles d'un slug précédent
        let old_positions = position_mgr.all_positions().to_vec();
        for pos in &old_positions {
            if pos.slug != slug {
                warn!(
                    "[POSITION] Position orpheline du marché {} — suppression",
                    pos.slug
                );
                position_mgr.close(&pos.trade_id);
            }
        }

        // Lancer le price feed WebSocket
        let (tx, mut rx) = mpsc::channel::<PriceUpdate>(256);
        let asset_ids = vec![
            market.up_token_id.clone(),
            market.down_token_id.clone(),
        ];

        let ws_handle = tokio::spawn({
            let ids = asset_ids.clone();
            async move {
                if let Err(e) = price_feed::stream_prices(ids, tx).await {
                    error!("[PRICE FEED] Erreur: {}", e);
                }
            }
        });

        // Timer d'expiration du marché
        let market_end = tokio::time::sleep(std::time::Duration::from_secs(remaining as u64));
        tokio::pin!(market_end);

        // Timer pré-fetch du marché suivant (~30s avant expiration)
        let prefetch_delay = if remaining > 35 { remaining - 30 } else { 0 };
        let prefetch_timer = tokio::time::sleep(std::time::Duration::from_secs(prefetch_delay as u64));
        tokio::pin!(prefetch_timer);
        let mut prefetch_done = false;
        // Tokens déjà tradés (TP/SL atteint) sur ce marché — pas de re-entry
        let mut exited_sides: HashSet<String> = HashSet::new();

        // ── Boucle de trading sur ce marché ──────────────────────────────────
        loop {
            tokio::select! {
                _ = &mut prefetch_timer, if !prefetch_done => {
                    spawn_prefetch_next_market(&poly_client, &config.polymarket_slug_prefix, interval_secs);
                    prefetch_done = true;
                }
                _ = &mut market_end => {
                    info!("[MARKET] Marché {} expiré — passage au suivant", slug);
                    // Fermer les positions restantes (le marché résout automatiquement)
                    let leftover = position_mgr.clear_slug(&slug);
                    for pos in &leftover {
                        warn!(
                            "[POSITION] Position {} non fermée avant expiration — marché résout",
                            pos.trade_id
                        );
                    }
                    break;
                }
                msg = rx.recv() => {
                    let Some(update) = msg else {
                        warn!("[PRICE FEED] Channel fermé — relance");
                        break;
                    };

                    // Identifier le token et le côté
                    let (token_side, token_id) = if update.asset_id == market.up_token_id {
                        ("UP", &market.up_token_id)
                    } else if update.asset_id == market.down_token_id {
                        ("DOWN", &market.down_token_id)
                    } else {
                        continue;
                    };

                    // ── Vérifier TP/SL sur les positions ouvertes ────────
                    let positions_snapshot: Vec<OpenPosition> = position_mgr
                        .positions_for_slug(&slug)
                        .into_iter()
                        .filter(|p| p.token_id == update.asset_id)
                        .cloned()
                        .collect();

                    for pos in positions_snapshot {
                        if update.price >= pos.tp_price {
                            // TAKE PROFIT
                            info!(
                                "[TP HIT] {} prix={:.2}¢ >= TP={:.2}¢ | trade_id={}",
                                pos.token_side,
                                update.price * 100.0,
                                pos.tp_price * 100.0,
                                pos.trade_id
                            );
                            match poly_client.sell_market(&pos.token_id, pos.shares).await {
                                Ok(sell_result) => {
                                    let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                    log_exit(
                                        &trade_logger,
                                        &pos,
                                        "TP",
                                        sell_result.fill_price,
                                        latency,
                                    );
                                    position_mgr.close(&pos.trade_id);
                                    exited_sides.insert(pos.token_side.clone());
                                    money_manager.lock().await.on_outcome("WIN");
                                }
                                Err(e) => error!("[TP SELL] Erreur: {}", e),
                            }
                        } else if update.price <= pos.sl_price {
                            // STOP LOSS
                            info!(
                                "[SL HIT] {} prix={:.2}¢ <= SL={:.2}¢ | trade_id={}",
                                pos.token_side,
                                update.price * 100.0,
                                pos.sl_price * 100.0,
                                pos.trade_id
                            );
                            match poly_client.sell_market(&pos.token_id, pos.shares).await {
                                Ok(sell_result) => {
                                    let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                    log_exit(
                                        &trade_logger,
                                        &pos,
                                        "SL",
                                        sell_result.fill_price,
                                        latency,
                                    );
                                    position_mgr.close(&pos.trade_id);
                                    exited_sides.insert(pos.token_side.clone());
                                    money_manager.lock().await.on_outcome("LOSS");
                                }
                                Err(e) => error!("[SL SELL] Erreur: {}", e),
                            }
                        }
                    }

                    // ── Vérifier entrée ──────────────────────────────────
                    if !position_mgr.has_position_on_slug(&slug)
                        && !exited_sides.contains(token_side)
                        && update.price >= config.entry_threshold
                    {
                        let trade_amount = money_manager.lock().await.current_amount();
                        info!(
                            "[ENTRY SIGNAL] {} prix={:.2}¢ >= seuil={:.2}¢ | montant={:.2}USDC",
                            token_side,
                            update.price * 100.0,
                            config.entry_threshold * 100.0,
                            trade_amount
                        );

                        match poly_client.buy_market(token_id, trade_amount, update.price).await {
                            Ok(buy_result) => {
                                let fill_price = buy_result.fill_price;
                                let shares = buy_result.shares;
                                let tp = calculate_tp(fill_price, config.tp_offset_cents);
                                let sl = calculate_sl(fill_price, config.sl_offset_cents);
                                let trade_id = uuid::Uuid::new_v4().to_string();
                                let latency = (buy_result.ack_at - buy_result.submitted_at).num_milliseconds();

                                info!(
                                    "[ENTRY FILLED] {} | fill={:.2}¢ shares={:.4} TP={:.2}¢ SL={:.2}¢ | {}ms",
                                    token_side,
                                    fill_price * 100.0,
                                    shares,
                                    tp * 100.0,
                                    sl * 100.0,
                                    latency
                                );

                                // Log CSV
                                let _ = trade_logger.log_trade(&TradeRecord {
                                    trade_id: trade_id.clone(),
                                    slug: slug.clone(),
                                    token_side: token_side.to_string(),
                                    action: "BUY".to_string(),
                                    amount_usdc: format!("{:.2}", trade_amount),
                                    shares: format!("{:.4}", shares),
                                    fill_price: format!("{:.4}", fill_price),
                                    tp_price: format!("{:.2}", tp),
                                    sl_price: format!("{:.2}", sl),
                                    exit_price: String::new(),
                                    exit_reason: String::new(),
                                    latency_ms: format!("{}", latency),
                                    timestamp_utc: Utc::now().to_rfc3339(),
                                });

                                position_mgr.open(OpenPosition {
                                    trade_id,
                                    slug: slug.clone(),
                                    token_id: token_id.clone(),
                                    token_side: token_side.to_string(),
                                    fill_price,
                                    shares,
                                    tp_price: tp,
                                    sl_price: sl,
                                    entry_time_utc: Utc::now().to_rfc3339(),
                                });
                            }
                            Err(e) => {
                                error!("[ENTRY BUY] Erreur: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Cleanup
        ws_handle.abort();
        info!("[MARKET] Attente du prochain marché...");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn log_exit(
    logger: &TradeLogger,
    pos: &OpenPosition,
    reason: &str,
    exit_price: f64,
    latency_ms: i64,
) {
    let _ = logger.log_trade(&TradeRecord {
        trade_id: pos.trade_id.clone(),
        slug: pos.slug.clone(),
        token_side: pos.token_side.clone(),
        action: "SELL".to_string(),
        amount_usdc: String::new(),
        shares: format!("{:.4}", pos.shares),
        fill_price: format!("{:.4}", pos.fill_price),
        tp_price: format!("{:.2}", pos.tp_price),
        sl_price: format!("{:.2}", pos.sl_price),
        exit_price: format!("{:.4}", exit_price),
        exit_reason: reason.to_string(),
        latency_ms: format!("{}", latency_ms),
        timestamp_utc: Utc::now().to_rfc3339(),
    });
}
