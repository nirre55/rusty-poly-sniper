use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use rusty_poly_sniper::config::{Config, ExecutionMode};
use rusty_poly_sniper::logger::{TradeLogger, TradeRecord};
use rusty_poly_sniper::money::MoneyManager;
use rusty_poly_sniper::polymarket::{MarketInfo, PolymarketClient};
use rusty_poly_sniper::position::{calculate_sl, calculate_tp, OpenPosition, PositionManager};
use rusty_poly_sniper::price_feed;

/// Calcule le slug du marché actif à cet instant.
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

/// Slug du marché suivant.
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

    // Ignorer le marché en cours au démarrage — attendre le prochain
    {
        let slug = current_market_slug(&config.polymarket_slug_prefix, interval_secs);
        let remaining = seconds_until_market_end(interval_secs);
        info!(
            "[STARTUP] Marché en cours {} ignoré — attente du prochain dans {}s",
            slug, remaining
        );
        tokio::time::sleep(std::time::Duration::from_secs(remaining as u64 + 1)).await;
    }

    // ── Boucle de résilience : un marché à la fois ───────────────────────────
    loop {
        let slug = current_market_slug(&config.polymarket_slug_prefix, interval_secs);
        let remaining = seconds_until_market_end(interval_secs);

        if remaining < 5 {
            info!("[MARKET] Marché {} expire dans {}s — attente du suivant", slug, remaining);
            tokio::time::sleep(std::time::Duration::from_secs(remaining as u64 + 1)).await;
            continue;
        }

        info!("[MARKET] Résolution du marché: {} ({}s restants)", slug, remaining);

        let market = match poly_client.resolve_market(&slug).await {
            Ok(m) => m,
            Err(e) => {
                error!("Impossible de résoudre le marché {}: {}", slug, e);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };

        poly_client.warm_sdk_caches(&market).await;

        info!(
            "[MARKET] {} | UP={} DOWN={}",
            market.slug, &market.up_token_id[..8], &market.down_token_id[..8]
        );

        // Cleanup positions orphelines
        let old_positions = position_mgr.all_positions().to_vec();
        for pos in &old_positions {
            if pos.slug != slug {
                warn!("[POSITION] Position orpheline du marché {} — suppression", pos.slug);
                position_mgr.close(&pos.trade_id);
            }
        }

        // Price cache partagé : le WS écrit, le main loop lit
        let price_cache = price_feed::new_price_cache();
        let (tx, mut rx) = mpsc::channel::<()>(16);

        let ws_handle = tokio::spawn({
            let ids = vec![market.up_token_id.clone(), market.down_token_id.clone()];
            let cache = price_cache.clone();
            async move {
                if let Err(e) = price_feed::stream_prices(ids, cache, tx).await {
                    error!("[PRICE FEED] Erreur: {}", e);
                }
            }
        });

        let market_end = tokio::time::sleep(std::time::Duration::from_secs(remaining as u64));
        tokio::pin!(market_end);

        let prefetch_delay = if remaining > 35 { remaining - 30 } else { 0 };
        let prefetch_timer = tokio::time::sleep(std::time::Duration::from_secs(prefetch_delay as u64));
        tokio::pin!(prefetch_timer);
        let mut prefetch_done = false;

        match config.execution_mode {
            ExecutionMode::Limit => {
                run_limit_loop(
                    &config,
                    &poly_client,
                    &trade_logger,
                    &money_manager,
                    &mut position_mgr,
                    &market,
                    &slug,
                    &price_cache,
                    &mut rx,
                    &mut market_end,
                    &mut prefetch_timer,
                    &mut prefetch_done,
                    interval_secs,
                )
                .await;
            }
            _ => {
                run_market_loop(
                    &config,
                    &poly_client,
                    &trade_logger,
                    &money_manager,
                    &mut position_mgr,
                    &market,
                    &slug,
                    &price_cache,
                    &mut rx,
                    &mut market_end,
                    &mut prefetch_timer,
                    &mut prefetch_done,
                    interval_secs,
                )
                .await;
            }
        }

        ws_handle.abort();
        // En mode limit, annuler les ordres restants en fin de marché
        if config.execution_mode == ExecutionMode::Limit {
            if let Err(e) = poly_client.cancel_all_orders().await {
                warn!("[LIMIT] Erreur annulation ordres fin de marché: {}", e);
            }
        }
        info!("[MARKET] Attente du prochain marché...");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

// ── Mode MARKET (FOK réactif, inchangé) ─────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_market_loop(
    config: &Config,
    poly_client: &Arc<PolymarketClient>,
    trade_logger: &Arc<TradeLogger>,
    money_manager: &Arc<tokio::sync::Mutex<MoneyManager>>,
    position_mgr: &mut PositionManager,
    market: &MarketInfo,
    slug: &str,
    price_cache: &price_feed::PriceCache,
    rx: &mut mpsc::Receiver<()>,
    market_end: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    prefetch_timer: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    prefetch_done: &mut bool,
    interval_secs: i64,
) {
    let mut exited_sides: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            _ = &mut *prefetch_timer, if !*prefetch_done => {
                spawn_prefetch_next_market(poly_client, &config.polymarket_slug_prefix, interval_secs);
                *prefetch_done = true;
            }
            _ = &mut *market_end => {
                info!("[MARKET] Marché {} expiré — passage au suivant", slug);
                let leftover = position_mgr.clear_slug(slug);
                for pos in &leftover {
                    warn!("[POSITION] {} non fermée avant expiration", pos.trade_id);
                }
                break;
            }
            msg = rx.recv() => {
                if msg.is_none() {
                    warn!("[PRICE FEED] Channel fermé — relance");
                    break;
                }

                let prices = match price_cache.read() {
                    Ok(c) => c.clone(),
                    Err(_) => continue,
                };

                // ── Vérifier TP/SL sur les positions ouvertes ────────
                let positions_snapshot: Vec<OpenPosition> = position_mgr
                    .positions_for_slug(slug)
                    .into_iter()
                    .cloned()
                    .collect();

                for pos in positions_snapshot {
                    let Some(tp) = prices.get(&pos.token_id) else { continue };
                    let current_price = tp.price;
                    if current_price <= 0.0 { continue; }

                    if current_price >= pos.tp_price {
                        info!(
                            "[TP HIT] {} price={:.2}¢ >= TP={:.2}¢ | trade_id={}",
                            pos.token_side, current_price * 100.0, pos.tp_price * 100.0, pos.trade_id
                        );
                        match poly_client.sell_market(&pos.token_id, pos.shares).await {
                            Ok(sell_result) => {
                                let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                log_exit(trade_logger, &pos, "TP", sell_result.fill_price, latency);
                                position_mgr.close(&pos.trade_id);
                                exited_sides.insert(pos.token_side.clone());
                                money_manager.lock().await.on_outcome("WIN");
                            }
                            Err(e) => {
                                error!("[TP SELL FAILED] {} — position fermée avec erreur", e);
                                position_mgr.close(&pos.trade_id);
                                exited_sides.insert(pos.token_side.clone());
                            }
                        }
                    } else if current_price <= pos.sl_price {
                        info!(
                            "[SL HIT] {} price={:.2}¢ <= SL={:.2}¢ | trade_id={}",
                            pos.token_side, current_price * 100.0, pos.sl_price * 100.0, pos.trade_id
                        );
                        match poly_client.sell_market(&pos.token_id, pos.shares).await {
                            Ok(sell_result) => {
                                let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                log_exit(trade_logger, &pos, "SL", sell_result.fill_price, latency);
                                position_mgr.close(&pos.trade_id);
                                exited_sides.insert(pos.token_side.clone());
                                money_manager.lock().await.on_outcome("LOSS");
                            }
                            Err(e) => {
                                error!("[SL SELL FAILED] {} — position fermée avec erreur", e);
                                position_mgr.close(&pos.trade_id);
                                exited_sides.insert(pos.token_side.clone());
                            }
                        }
                    }
                }

                // ── Vérifier entrée ──
                if position_mgr.has_position_on_slug(slug) {
                    continue;
                }

                for (token_id, token_side) in [
                    (&market.up_token_id, "UP"),
                    (&market.down_token_id, "DOWN"),
                ] {
                    if exited_sides.contains(token_side) { continue; }

                    let Some(tp) = prices.get(token_id) else { continue };
                    let token_price = tp.price;
                    if token_price <= 0.0 || token_price >= 1.0 { continue; }

                    if token_price < config.entry_threshold {
                        continue;
                    }

                    let trade_amount = money_manager.lock().await.current_amount();
                    info!(
                        "[ENTRY SIGNAL] {} price={:.2}¢ >= seuil={:.2}¢ | montant={:.2}USDC",
                        token_side, token_price * 100.0, config.entry_threshold * 100.0, trade_amount
                    );

                    match poly_client.buy_market(token_id, trade_amount, token_price).await {
                        Ok(buy_result) => {
                            let fill_price = buy_result.fill_price;
                            let shares = buy_result.shares;
                            let tp_price = calculate_tp(fill_price, config.tp_offset_cents);
                            let sl_price = calculate_sl(fill_price, config.sl_offset_cents);
                            let trade_id = uuid::Uuid::new_v4().to_string();
                            let latency = (buy_result.ack_at - buy_result.submitted_at).num_milliseconds();

                            info!(
                                "[ENTRY FILLED] {} | fill={:.2}¢ shares={:.4} TP={:.2}¢ SL={:.2}¢ | {}ms",
                                token_side, fill_price * 100.0, shares, tp_price * 100.0, sl_price * 100.0, latency
                            );

                            let _ = trade_logger.log_trade(&TradeRecord {
                                trade_id: trade_id.clone(),
                                slug: slug.to_string(),
                                token_side: token_side.to_string(),
                                action: "BUY".to_string(),
                                amount_usdc: format!("{:.2}", trade_amount),
                                shares: format!("{:.6}", shares),
                                fill_price: format!("{:.4}", fill_price),
                                tp_price: format!("{:.2}", tp_price),
                                sl_price: format!("{:.2}", sl_price),
                                exit_price: String::new(),
                                exit_reason: String::new(),
                                latency_ms: format!("{}", latency),
                                timestamp_utc: Utc::now().to_rfc3339(),
                            });

                            position_mgr.open(OpenPosition {
                                trade_id,
                                slug: slug.to_string(),
                                token_id: token_id.clone(),
                                token_side: token_side.to_string(),
                                fill_price,
                                shares,
                                tp_price,
                                sl_price,
                                entry_time_utc: Utc::now().to_rfc3339(),
                            });
                        }
                        Err(e) => error!("[ENTRY BUY] Erreur: {}", e),
                    }
                    break;
                }
            }
        }
    }
}

// ── Mode LIMIT (GTC pré-placé) ──────────────────────────────────────────────

/// État d'un ordre GTC limit entry posé sur le book.
struct LimitEntry {
    order_id: String,
    #[allow(dead_code)]
    token_id: String,
    token_side: String,
    limit_price: f64,
    shares: f64,
}

/// État de l'ordre GTC limit TP posé après un fill entry.
struct LimitTp {
    order_id: String,
    tp_price: f64,
}

#[allow(clippy::too_many_arguments)]
async fn run_limit_loop(
    config: &Config,
    poly_client: &Arc<PolymarketClient>,
    trade_logger: &Arc<TradeLogger>,
    money_manager: &Arc<tokio::sync::Mutex<MoneyManager>>,
    position_mgr: &mut PositionManager,
    market: &MarketInfo,
    slug: &str,
    price_cache: &price_feed::PriceCache,
    rx: &mut mpsc::Receiver<()>,
    market_end: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    prefetch_timer: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    prefetch_done: &mut bool,
    interval_secs: i64,
) {
    let trade_amount = money_manager.lock().await.current_amount();

    // ── Placer les 2 ordres GTC limit BUY (UP + DOWN) au seuil d'entrée ────
    let mut pending_entries: HashMap<String, LimitEntry> = HashMap::new();

    for (token_id, token_side) in [
        (&market.up_token_id, "UP"),
        (&market.down_token_id, "DOWN"),
    ] {
        let limit_price = config.entry_threshold;
        match poly_client
            .place_limit_buy(token_id, trade_amount, limit_price)
            .await
        {
            Ok(result) => {
                info!(
                    "[LIMIT] GTC BUY {} placé @ {:.2}¢ | order_id={}",
                    token_side,
                    limit_price * 100.0,
                    &result.order_id
                );
                pending_entries.insert(
                    token_id.clone(),
                    LimitEntry {
                        order_id: result.order_id,
                        token_id: token_id.clone(),
                        token_side: token_side.to_string(),
                        limit_price,
                        shares: result.shares,
                    },
                );
            }
            Err(e) => {
                error!("[LIMIT] Erreur placement GTC BUY {}: {}", token_side, e);
            }
        }
    }

    let mut entry_filled = false;
    let mut tp_order: Option<LimitTp> = None;
    let mut exited_sides: HashSet<String> = HashSet::new();

    // ── Boucle de trading sur ce marché ──────────────────────────────────────
    loop {
        tokio::select! {
            _ = &mut *prefetch_timer, if !*prefetch_done => {
                spawn_prefetch_next_market(poly_client, &config.polymarket_slug_prefix, interval_secs);
                *prefetch_done = true;
            }
            _ = &mut *market_end => {
                info!("[MARKET] Marché {} expiré — passage au suivant", slug);
                let leftover = position_mgr.clear_slug(slug);
                for pos in &leftover {
                    warn!("[POSITION] {} non fermée avant expiration", pos.trade_id);
                }
                break;
            }
            msg = rx.recv() => {
                if msg.is_none() {
                    warn!("[PRICE FEED] Channel fermé — relance");
                    break;
                }

                let prices = match price_cache.read() {
                    Ok(c) => c.clone(),
                    Err(_) => continue,
                };

                // ── Phase 1 : Détecter le fill de l'entry GTC ────────────
                if !entry_filled && !pending_entries.is_empty() {
                    let mut filled_token_id: Option<String> = None;

                    for (token_id, entry) in &pending_entries {
                        let Some(tp) = prices.get(token_id) else { continue };
                        // Le best_ask <= notre limit_price signifie que notre BUY a été fill
                        // (les asks sont descendues jusqu'à notre prix ou en dessous)
                        let best_ask = tp.best_ask;
                        if best_ask > 0.0 && best_ask <= entry.limit_price {
                            let fill_price = entry.limit_price;
                            let shares = entry.shares;
                            let tp_price = calculate_tp(fill_price, config.tp_offset_cents);
                            let sl_price = calculate_sl(fill_price, config.sl_offset_cents);
                            let trade_id = uuid::Uuid::new_v4().to_string();

                            info!(
                                "[LIMIT ENTRY FILLED] {} | fill={:.2}¢ shares={:.4} TP={:.2}¢ SL={:.2}¢",
                                entry.token_side,
                                fill_price * 100.0,
                                shares,
                                tp_price * 100.0,
                                sl_price * 100.0
                            );

                            let _ = trade_logger.log_trade(&TradeRecord {
                                trade_id: trade_id.clone(),
                                slug: slug.to_string(),
                                token_side: entry.token_side.clone(),
                                action: "BUY".to_string(),
                                amount_usdc: format!("{:.2}", trade_amount),
                                shares: format!("{:.6}", shares),
                                fill_price: format!("{:.4}", fill_price),
                                tp_price: format!("{:.2}", tp_price),
                                sl_price: format!("{:.2}", sl_price),
                                exit_price: String::new(),
                                exit_reason: String::new(),
                                latency_ms: "0".to_string(),
                                timestamp_utc: Utc::now().to_rfc3339(),
                            });

                            position_mgr.open(OpenPosition {
                                trade_id,
                                slug: slug.to_string(),
                                token_id: token_id.clone(),
                                token_side: entry.token_side.clone(),
                                fill_price,
                                shares,
                                tp_price,
                                sl_price,
                                entry_time_utc: Utc::now().to_rfc3339(),
                            });

                            filled_token_id = Some(token_id.clone());

                            // Placer le GTC limit SELL au TP
                            match poly_client
                                .place_limit_sell(token_id, shares, tp_price)
                                .await
                            {
                                Ok(tp_result) => {
                                    info!(
                                        "[LIMIT] GTC SELL TP placé @ {:.2}¢ | order_id={}",
                                        tp_price * 100.0,
                                        &tp_result.order_id
                                    );
                                    tp_order = Some(LimitTp {
                                        order_id: tp_result.order_id,
                                        tp_price,
                                    });
                                }
                                Err(e) => {
                                    error!("[LIMIT] Erreur placement GTC SELL TP: {}", e);
                                }
                            }

                            break;
                        }
                    }

                    if let Some(filled_id) = filled_token_id {
                        // Annuler l'autre côté
                        let other_entries: Vec<LimitEntry> = pending_entries
                            .drain()
                            .filter_map(|(k, v)| if k != filled_id { Some(v) } else { None })
                            .collect();

                        for other in &other_entries {
                            info!(
                                "[LIMIT] Annulation GTC BUY {} (autre côté)",
                                other.token_side
                            );
                            if let Err(e) = poly_client.cancel_order(&other.order_id).await {
                                warn!("[LIMIT] Erreur annulation {}: {}", other.token_side, e);
                            }
                        }

                        entry_filled = true;
                        pending_entries.clear();
                    }
                }

                // ── Phase 2 : Surveiller TP (via GTC) et SL (réactif) ────
                let positions_snapshot: Vec<OpenPosition> = position_mgr
                    .positions_for_slug(slug)
                    .into_iter()
                    .cloned()
                    .collect();

                for pos in positions_snapshot {
                    let Some(tp) = prices.get(&pos.token_id) else { continue };
                    let current_price = tp.price;
                    if current_price <= 0.0 { continue; }

                    // TP : détecté via le best_bid >= tp_price (notre sell limit a été fill)
                    if tp_order.is_some() && current_price >= pos.tp_price {
                        let tp_info = tp_order.take().unwrap();
                        info!(
                            "[LIMIT TP FILLED] {} price={:.2}¢ >= TP={:.2}¢ | trade_id={}",
                            pos.token_side,
                            current_price * 100.0,
                            pos.tp_price * 100.0,
                            pos.trade_id
                        );
                        log_exit(trade_logger, &pos, "TP", tp_info.tp_price, 0);
                        position_mgr.close(&pos.trade_id);
                        exited_sides.insert(pos.token_side.clone());
                        money_manager.lock().await.on_outcome("WIN");
                    }
                    // SL : réactif, FOK sell (incompressible ~450ms)
                    else if current_price <= pos.sl_price {
                        info!(
                            "[SL HIT] {} price={:.2}¢ <= SL={:.2}¢ | trade_id={}",
                            pos.token_side,
                            current_price * 100.0,
                            pos.sl_price * 100.0,
                            pos.trade_id
                        );

                        // Annuler l'ordre TP GTC s'il existe
                        if let Some(tp_info) = tp_order.take() {
                            if let Err(e) = poly_client.cancel_order(&tp_info.order_id).await {
                                warn!("[LIMIT] Erreur annulation TP order: {}", e);
                            }
                        }

                        match poly_client.sell_market(&pos.token_id, pos.shares).await {
                            Ok(sell_result) => {
                                let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                log_exit(trade_logger, &pos, "SL", sell_result.fill_price, latency);
                                position_mgr.close(&pos.trade_id);
                                exited_sides.insert(pos.token_side.clone());
                                money_manager.lock().await.on_outcome("LOSS");
                            }
                            Err(e) => {
                                error!("[SL SELL FAILED] {} — position fermée avec erreur", e);
                                position_mgr.close(&pos.trade_id);
                                exited_sides.insert(pos.token_side.clone());
                            }
                        }
                    }
                }

                // Si plus de positions et entry déjà filled, on a fini sur ce marché
                if entry_filled && !position_mgr.has_position_on_slug(slug) {
                    info!("[LIMIT] Plus de positions sur {} — attente fin de marché", slug);
                }
            }
        }
    }
}

fn log_exit(logger: &TradeLogger, pos: &OpenPosition, reason: &str, exit_price: f64, latency_ms: i64) {
    let _ = logger.log_trade(&TradeRecord {
        trade_id: pos.trade_id.clone(),
        slug: pos.slug.clone(),
        token_side: pos.token_side.clone(),
        action: "SELL".to_string(),
        amount_usdc: String::new(),
        shares: format!("{:.6}", pos.shares),
        fill_price: format!("{:.4}", pos.fill_price),
        tp_price: format!("{:.2}", pos.tp_price),
        sl_price: format!("{:.2}", pos.sl_price),
        exit_price: format!("{:.4}", exit_price),
        exit_reason: reason.to_string(),
        latency_ms: format!("{}", latency_ms),
        timestamp_utc: Utc::now().to_rfc3339(),
    });
}
