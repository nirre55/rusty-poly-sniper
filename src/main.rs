use anyhow::Result;
use chrono::{Datelike, Timelike, TimeZone, Utc, Weekday};
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
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let interval_secs = config.interval_seconds()?;

    let tp_display = if config.tp_disabled {
        "disabled (expiration)".to_string()
    } else if let Some(p) = config.tp_price_fixed {
        format!("{:.0}¢ (fixed)", p * 100.0)
    } else {
        format!("+{:.0}¢ (offset)", config.tp_offset_cents)
    };

    let sl_display = if let Some(p) = config.sl_price_fixed {
        format!("{:.0}¢ (fixed)", p * 100.0)
    } else {
        format!("-{:.0}¢ (offset)", config.sl_offset_cents)
    };

    let amount_display = if config.trade_amount_pct > 0.0 {
        format!("{:.1}% du solde", config.trade_amount_pct)
    } else {
        format!("{:.2} USDC", config.trade_amount_usdc)
    };

    info!(
        "Démarrage rusty-poly-sniper | mode={} prefix={} interval={} entry={}¢ | TP={} SL={} | montant={}",
        config.execution_mode.as_str(),
        config.polymarket_slug_prefix,
        config.interval,
        (config.entry_threshold * 100.0) as u32,
        tp_display,
        sl_display,
        amount_display,
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
        config.anti_martingale_multiplier,
        config.martingale_max_amount,
        config.anti_martingale_max_streak,
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

        let market = {
            let market_deadline = Utc::now() + chrono::Duration::seconds(remaining);
            let mut attempt = 0u32;
            let mut resolved = None;
            loop {
                if Utc::now() >= market_deadline {
                    info!("[MARKET] {} introuvable — slot expiré, passage au suivant", slug);
                    break;
                }
                match poly_client.resolve_market(&slug).await {
                    Ok(m) => { resolved = Some(m); break; }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("Aucun marché trouvé") {
                            let delay = match attempt {
                                0 => 5,
                                1 => 10,
                                2 => 15,
                                _ => 20,
                            };
                            if attempt == 0 {
                                info!("[MARKET] {} pas encore disponible — attente {}s", slug, delay);
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                        } else {
                            error!("Impossible de résoudre le marché {}: {}", slug, e);
                            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                        }
                        attempt += 1;
                    }
                }
            }
            match resolved {
                Some(m) => m,
                None => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        };

        // ── Filtre jour de la semaine ────────────────────────────────────────
        if !config.excluded_days.is_empty() {
            // Extraire le timestamp Unix depuis le slug (dernier segment)
            if let Some(ts_str) = slug.rsplit('-').next() {
                if let Ok(ts) = ts_str.parse::<i64>() {
                    let dt = Utc.timestamp_opt(ts, 0).single();
                    if let Some(dt) = dt {
                        let day_str = match dt.weekday() {
                            Weekday::Mon => "mon",
                            Weekday::Tue => "tue",
                            Weekday::Wed => "wed",
                            Weekday::Thu => "thu",
                            Weekday::Fri => "fri",
                            Weekday::Sat => "sat",
                            Weekday::Sun => "sun",
                        };
                        if config.excluded_days.iter().any(|d| d == day_str) {
                            info!(
                                "[FILTRE JOUR] {} ({}) exclu — attente du prochain marché",
                                slug, day_str
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(remaining as u64 + 1)).await;
                            continue;
                        }
                    }
                }
            }
        }

        // ── Filtre heure (UTC) ───────────────────────────────────────────────
        if !config.excluded_hours.is_empty() {
            if let Some(ts_str) = slug.rsplit('-').next() {
                if let Ok(ts) = ts_str.parse::<i64>() {
                    if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
                        let hour = dt.hour();
                        if config.excluded_hours.iter().any(|&(start, end)| hour >= start && hour < end) {
                            info!(
                                "[FILTRE HEURE] {} ({}h UTC) exclu — attente du prochain marché",
                                slug, hour
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(remaining as u64 + 1)).await;
                            continue;
                        }
                    }
                }
            }
        }

        // ── Mode pourcentage : recalcul du montant de base sur le solde USDC ──
        if config.trade_amount_pct > 0.0 && config.execution_mode != ExecutionMode::DryRun {
            match poly_client.get_usdc_balance().await {
                Ok(balance) => {
                    let pct_amount = (balance * config.trade_amount_pct / 100.0 * 100.0).floor() / 100.0;
                    let amount = pct_amount.max(1.0);
                    info!(
                        "[MONEY] Solde USDC = {:.2} | {:.1}% = {:.2} USDC (min 1$)",
                        balance, config.trade_amount_pct, amount
                    );
                    money_manager.lock().await.set_base_amount(amount);
                }
                Err(e) => warn!("[MONEY] Impossible de récupérer le solde USDC — montant inchangé: {}", e),
            }
        }

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
            ExecutionMode::NoTpNoSl => {
                run_no_tp_no_sl_loop(
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
    let mut trade_count: u32 = 0;
    // Deadline = expiration du marché courant (pour retry SELL)
    let market_deadline = Utc::now() + chrono::Duration::seconds(seconds_until_market_end(interval_secs));
    // Cooldown après un 500 pour éviter les doubles positions
    let mut buy_cooldown_until: Option<std::time::Instant> = None;

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
                    // Annuler le limit TP GTC si présent (marché réglé automatiquement)
                    if let Some(ref oid) = pos.tp_order_id {
                        if let Err(e) = poly_client.cancel_order(oid).await {
                            warn!("[TP LIMIT] Annulation fin de marché échouée (déjà fill?): {}", e);
                        }
                    }
                    warn!("[POSITION] {} non fermée — comptée comme WIN (résolution marché)", pos.trade_id);
                    money_manager.lock().await.on_outcome("WIN");
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

                    if !config.tp_disabled && current_price >= pos.tp_price {
                        info!(
                            "[TP HIT] {} price={:.2}¢ >= TP={:.2}¢ | trade_id={}",
                            pos.token_side, current_price * 100.0, pos.tp_price * 100.0, pos.trade_id
                        );
                        // Annuler le limit order GTC (si déjà fill, cancel échoue silencieusement)
                        if let Some(ref oid) = pos.tp_order_id {
                            if let Err(e) = poly_client.cancel_order(oid).await {
                                warn!("[TP LIMIT] Annulation TP échouée (déjà fill?): {}", e);
                            }
                        }
                        match poly_client.sell_market(&pos.token_id, pos.shares, market_deadline).await {
                            Ok(sell_result) => {
                                let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                log_exit(trade_logger, &pos, "TP", sell_result.fill_price, latency);
                                position_mgr.close(&pos.trade_id);
                                trade_count += 1;
                                money_manager.lock().await.on_outcome("WIN");
                                refresh_pct_balance(config, poly_client, money_manager).await;
                            }
                            Err(e) => {
                                error!("[TP SELL FAILED] {} — position fermée avec erreur", e);
                                log_exit(trade_logger, &pos, "TP_SELL_FAILED", current_price, 0);
                                position_mgr.close(&pos.trade_id);
                                trade_count += 1;
                            }
                        }
                    } else if current_price <= pos.sl_price {
                        info!(
                            "[SL HIT] {} price={:.2}¢ <= SL={:.2}¢ | trade_id={}",
                            pos.token_side, current_price * 100.0, pos.sl_price * 100.0, pos.trade_id
                        );
                        // Annuler le limit TP GTC avant de vendre au marché
                        if let Some(ref oid) = pos.tp_order_id {
                            if let Err(e) = poly_client.cancel_order(oid).await {
                                warn!("[TP LIMIT] Annulation SL échouée (déjà fill?): {}", e);
                            }
                        }
                        match poly_client.sell_market(&pos.token_id, pos.shares, market_deadline).await {
                            Ok(sell_result) => {
                                let latency = (sell_result.ack_at - sell_result.submitted_at).num_milliseconds();
                                log_exit(trade_logger, &pos, "SL", sell_result.fill_price, latency);
                                position_mgr.close(&pos.trade_id);
                                trade_count += 1;
                                money_manager.lock().await.on_outcome("LOSS");
                                refresh_pct_balance(config, poly_client, money_manager).await;
                            }
                            Err(e) => {
                                error!("[SL SELL FAILED] {} — position fermée avec erreur", e);
                                log_exit(trade_logger, &pos, "SL_SELL_FAILED", current_price, 0);
                                position_mgr.close(&pos.trade_id);
                                trade_count += 1;
                            }
                        }
                    }
                }

                // ── Vérifier entrée ──
                if position_mgr.has_position_on_slug(slug) {
                    continue;
                }

                if config.market_max_trades > 0 && trade_count >= config.market_max_trades {
                    continue;
                }

                // Cooldown après erreur 500 : évite les doubles positions
                if let Some(until) = buy_cooldown_until {
                    if std::time::Instant::now() < until {
                        continue;
                    }
                    buy_cooldown_until = None;
                }

                for (signal_token_id, signal_side, buy_token_id, buy_side) in [
                    (&market.up_token_id, "UP", if config.reverse_entry { &market.down_token_id } else { &market.up_token_id }, if config.reverse_entry { "DOWN" } else { "UP" }),
                    (&market.down_token_id, "DOWN", if config.reverse_entry { &market.up_token_id } else { &market.down_token_id }, if config.reverse_entry { "UP" } else { "DOWN" }),
                ] {
                    let Some(tp) = prices.get(signal_token_id) else { continue };
                    let token_price = tp.price;
                    if token_price <= 0.0 || token_price >= 1.0 { continue; }

                    if token_price < config.entry_threshold {
                        continue;
                    }

                    let trade_amount = money_manager.lock().await.current_amount();
                    info!(
                        "[ENTRY SIGNAL] {} price={:.2}¢ >= seuil={:.2}¢{} | montant={:.2}USDC",
                        signal_side, token_price * 100.0, config.entry_threshold * 100.0,
                        if config.reverse_entry { format!(" → achat {}", buy_side) } else { String::new() },
                        trade_amount
                    );

                    let token_id = buy_token_id;
                    let token_side = buy_side;
                    match poly_client.buy_market(token_id, trade_amount, token_price).await {
                        Ok(buy_result) => {
                            let fill_price = buy_result.fill_price;
                            let shares = buy_result.shares;
                            let tp_price = config.tp_price_fixed
                                .unwrap_or_else(|| calculate_tp(fill_price, config.tp_offset_cents));
                            let sl_price = config.sl_price_fixed
                                .unwrap_or_else(|| calculate_sl(fill_price, config.sl_offset_cents));
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

                            // Ouvrir la position d'abord (sans tp_order_id)
                            position_mgr.open(OpenPosition {
                                trade_id: trade_id.clone(),
                                slug: slug.to_string(),
                                token_id: token_id.clone(),
                                token_side: token_side.to_string(),
                                fill_price,
                                shares,
                                tp_price,
                                sl_price,
                                entry_time_utc: Utc::now().to_rfc3339(),
                                tp_order_id: None,
                            });

                            // Placer le limit sell GTC au TP seulement si TP activé et shares > 5
                            let tp_limit_price = tp_price.min(0.99);
                            let tp_order_id = if !config.tp_disabled && shares > 5.0 {
                                match poly_client.place_limit_sell(token_id, shares, tp_limit_price).await {
                                    Ok(r) => {
                                        info!(
                                            "[TP LIMIT] GTC limit sell placé à {:.2}¢ | order_id={}",
                                            tp_limit_price * 100.0, r.order_id
                                        );
                                        Some(r.order_id)
                                    }
                                    Err(e) => {
                                        warn!("[TP LIMIT] Placement échoué — fallback monitoring prix: {}", e);
                                        None
                                    }
                                }
                            } else {
                                info!("[TP LIMIT] Skipped — shares={:.4} <= 5 (monitoring prix uniquement)", shares);
                                None
                            };

                            // Mettre à jour la position avec le tp_order_id
                            if let Some(oid) = tp_order_id {
                                position_mgr.set_tp_order_id(&trade_id, oid);
                            }
                        }
                        Err(e) => {
                            error!("[ENTRY BUY] Erreur: {} — cooldown 5s", e);
                            buy_cooldown_until = Some(
                                std::time::Instant::now() + std::time::Duration::from_secs(5)
                            );
                        }
                    }
                    break;
                }
            }
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Rafraîchit le base_amount du MoneyManager depuis la balance USDC réelle (mode PCT uniquement).
/// Appelé immédiatement après un TP ou SL fill pour que le prochain trade utilise le bon capital.
async fn refresh_pct_balance(
    config: &Config,
    poly_client: &Arc<PolymarketClient>,
    money_manager: &Arc<tokio::sync::Mutex<MoneyManager>>,
) {
    if config.trade_amount_pct <= 0.0 || config.execution_mode == ExecutionMode::DryRun {
        return;
    }
    // Retry jusqu'à ce que le CLOB ait crédité l'USDC du sell (max ~2.5s)
    for delay_ms in [300u64, 700, 1500] {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        match poly_client.get_usdc_balance().await {
            Ok(balance) if balance > 0.0 => {
                let amount = (balance * config.trade_amount_pct / 100.0 * 100.0).floor() / 100.0;
                let amount = amount.max(1.0);
                info!("[MONEY] Balance post-trade: {:.2}$ → prochain montant = {:.2}$", balance, amount);
                money_manager.lock().await.set_base_amount(amount);
                return;
            }
            Ok(_) => warn!("[MONEY] Balance USDC encore 0 après {}ms, retry...", delay_ms),
            Err(e) => {
                warn!("[MONEY] Balance refresh post-trade échoué: {}", e);
                return;
            }
        }
    }
    warn!("[MONEY] Balance USDC toujours 0 après retries — montant inchangé");
}

// ── Mode LIMIT ──────────────────────────────────────────────────────────────
//
// Surveille l'entry sur les 2 côtés indépendamment (1 position max par côté).
// Quand UP atteint le seuil → FOK BUY UP.
// Quand DOWN atteint le seuil → FOK BUY DOWN.
// Pas de TP, pas de SL — juste entry.

#[allow(clippy::too_many_arguments)]
async fn run_no_tp_no_sl_loop(
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
    // Côtés touchés (pour martingale win/loss)
    let mut sides_touched: HashSet<String> = HashSet::new();
    // Ping-pong: dernier côté entré + compteur d'entrées
    let mut last_side: Option<String> = None;
    let mut entry_count: u32 = 0;
    // Montant de base pour ce marché (martingale/anti-martingale)
    let base_market_amount = money_manager.lock().await.current_amount();
    let inmarket_mult = config.inmarket_multiplier;
    // Reversal: true quand le ping-pong est terminé et on surveille le dernier côté
    let mut reversal_done = false;

    loop {
        tokio::select! {
            _ = &mut *prefetch_timer, if !*prefetch_done => {
                spawn_prefetch_next_market(poly_client, &config.polymarket_slug_prefix, interval_secs);
                *prefetch_done = true;
            }
            _ = &mut *market_end => {
                info!("[MARKET] Marché {} expiré — passage au suivant", slug);
                let num_sides = sides_touched.len();
                let leftover = position_mgr.clear_slug(slug);
                for pos in &leftover {
                    info!("[POSITION] {} fermée par expiration marché", pos.trade_id);
                }
                // Martingale: 2 côtés touchés = LOSS, 1 côté = WIN, 0 = pas de trade
                if num_sides == 2 {
                    info!("[MARTINGALE] 2 côtés touchés (UP+DOWN) = LOSS");
                    money_manager.lock().await.on_outcome("LOSS");
                } else if num_sides == 1 {
                    info!("[MARTINGALE] 1 côté touché = WIN");
                    money_manager.lock().await.on_outcome("WIN");
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

                // ── Vérifier entry sur chaque côté ───────
                for (token_id, token_side) in [
                    (&market.up_token_id, "UP"),
                    (&market.down_token_id, "DOWN"),
                ] {
                    // Ping-pong désactivé (inmarket_mult == 1.0) : 1 position max par côté
                    if inmarket_mult <= 1.0 {
                        if sides_touched.contains(token_side) { continue; }
                    } else {
                        // Limite d'entrées atteinte
                        if config.inmarket_max_entries > 0 && entry_count >= config.inmarket_max_entries {
                            continue;
                        }
                        // Ping-pong activé : après la 1ère entry, surveiller seulement l'autre côté
                        if let Some(ref ls) = last_side {
                            if ls == token_side { continue; }
                        }
                    }

                    let Some(tp) = prices.get(token_id) else { continue };
                    let token_price = tp.price;
                    if token_price <= 0.0 || token_price >= 1.0 { continue; }

                    if token_price < config.entry_threshold {
                        continue;
                    }

                    // Calcul du montant : base_market_amount × inmarket_mult^(entrées après la 1ère)
                    let trade_amount = if entry_count == 0 || inmarket_mult <= 1.0 {
                        base_market_amount
                    } else {
                        base_market_amount * inmarket_mult.powi(entry_count as i32)
                    };

                    info!(
                        "[ENTRY SIGNAL] {} price={:.2}¢ >= seuil={:.2}¢ | montant={:.2}USDC (entry #{})",
                        token_side, token_price * 100.0, config.entry_threshold * 100.0, trade_amount, entry_count + 1
                    );

                    match poly_client.buy_market(token_id, trade_amount, token_price).await {
                        Ok(buy_result) => {
                            let fill_price = buy_result.fill_price;
                            let shares = buy_result.shares;
                            let trade_id = uuid::Uuid::new_v4().to_string();
                            let latency = (buy_result.ack_at - buy_result.submitted_at).num_milliseconds();

                            info!(
                                "[ENTRY FILLED] {} | fill={:.2}¢ shares={:.4} | {}ms (entry #{})",
                                token_side, fill_price * 100.0, shares, latency, entry_count + 1
                            );

                            let _ = trade_logger.log_trade(&TradeRecord {
                                trade_id: trade_id.clone(),
                                slug: slug.to_string(),
                                token_side: token_side.to_string(),
                                action: "BUY".to_string(),
                                amount_usdc: format!("{:.2}", trade_amount),
                                shares: format!("{:.6}", shares),
                                fill_price: format!("{:.4}", fill_price),
                                tp_price: String::new(),
                                sl_price: String::new(),
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
                                tp_price: 0.0,
                                sl_price: 0.0,
                                entry_time_utc: Utc::now().to_rfc3339(),
                                tp_order_id: None,
                            });

                            sides_touched.insert(token_side.to_string());
                            last_side = Some(token_side.to_string());
                            entry_count += 1;
                        }
                        Err(e) => error!("[ENTRY BUY] {} Erreur: {}", token_side, e),
                    }
                }

                // ── Reversal : surveiller le dernier côté entré ───────
                if !reversal_done
                    && config.reversal_threshold > 0.0
                    && config.reversal_amount > 0.0
                    && entry_count >= 2
                {
                    // Le ping-pong doit être terminé (limite atteinte ou désactivé après 2+)
                    let ping_pong_done = if inmarket_mult <= 1.0 {
                        true
                    } else {
                        config.inmarket_max_entries > 0 && entry_count >= config.inmarket_max_entries
                    };

                    if ping_pong_done {
                        if let Some(ref ls) = last_side {
                            // Token du dernier côté entré
                            let (watch_token_id, watch_side) = if ls == "UP" {
                                (&market.up_token_id, "UP")
                            } else {
                                (&market.down_token_id, "DOWN")
                            };
                            // Côté opposé pour le trade reversal
                            let (reversal_token_id, reversal_side) = if ls == "UP" {
                                (&market.down_token_id, "DOWN")
                            } else {
                                (&market.up_token_id, "UP")
                            };

                            if let Some(tp) = prices.get(watch_token_id) {
                                let watch_price = tp.price;
                                if watch_price > 0.0 && watch_price <= config.reversal_threshold {
                                    info!(
                                        "[REVERSAL] {} redescend à {:.2}¢ <= seuil={:.2}¢ → BUY {} {:.2}USDC",
                                        watch_side, watch_price * 100.0, config.reversal_threshold * 100.0,
                                        reversal_side, config.reversal_amount
                                    );

                                    match poly_client.buy_market(reversal_token_id, config.reversal_amount, prices.get(reversal_token_id).map(|p| p.price).unwrap_or(0.5)).await {
                                        Ok(buy_result) => {
                                            let fill_price = buy_result.fill_price;
                                            let shares = buy_result.shares;
                                            let trade_id = uuid::Uuid::new_v4().to_string();
                                            let latency = (buy_result.ack_at - buy_result.submitted_at).num_milliseconds();

                                            info!(
                                                "[REVERSAL FILLED] {} | fill={:.2}¢ shares={:.4} | {}ms",
                                                reversal_side, fill_price * 100.0, shares, latency
                                            );

                                            let _ = trade_logger.log_trade(&TradeRecord {
                                                trade_id: trade_id.clone(),
                                                slug: slug.to_string(),
                                                token_side: reversal_side.to_string(),
                                                action: "BUY_REVERSAL".to_string(),
                                                amount_usdc: format!("{:.2}", config.reversal_amount),
                                                shares: format!("{:.6}", shares),
                                                fill_price: format!("{:.4}", fill_price),
                                                tp_price: String::new(),
                                                sl_price: String::new(),
                                                exit_price: String::new(),
                                                exit_reason: String::new(),
                                                latency_ms: format!("{}", latency),
                                                timestamp_utc: Utc::now().to_rfc3339(),
                                            });

                                            position_mgr.open(OpenPosition {
                                                trade_id,
                                                slug: slug.to_string(),
                                                token_id: reversal_token_id.clone(),
                                                token_side: reversal_side.to_string(),
                                                fill_price,
                                                shares,
                                                tp_price: 0.0,
                                                sl_price: 0.0,
                                                entry_time_utc: Utc::now().to_rfc3339(),
                                                tp_order_id: None,
                                            });

                                            reversal_done = true;
                                        }
                                        Err(e) => error!("[REVERSAL BUY] {} Erreur: {}", reversal_side, e),
                                    }
                                }
                            }
                        }
                    }
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
