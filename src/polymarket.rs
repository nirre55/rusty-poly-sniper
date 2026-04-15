use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{
    Amount, OrderType as SdkOrderType, Side as SdkSide, SignatureType as SdkSignatureType,
};
use polymarket_client_sdk::clob::{Client as SdkClobClient, Config as SdkConfig};
use polymarket_client_sdk::types::Decimal;
use polymarket_client_sdk::POLYGON;
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::{Config, ExecutionMode};

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_API_BASE: &str = "https://clob.polymarket.com";
const FOK_RETRY_DELAYS_SECS: [u64; 3] = [3, 7, 10];

// ── Types publics ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub slug: String,
    pub order_min_size: f64,
}

#[derive(Debug, Clone)]
pub struct OrderResult {
    pub order_id: String,
    pub status: String,
    pub fill_price: f64,
    pub shares: f64,
    pub submitted_at: DateTime<Utc>,
    pub ack_at: DateTime<Utc>,
}

// ── Types internes ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GammaMarket {
    #[serde(alias = "conditionId")]
    condition_id: String,
    outcomes: String,
    #[serde(alias = "clobTokenIds")]
    clob_token_ids: String,
    #[serde(alias = "orderMinSize", default = "default_order_min_size")]
    order_min_size: f64,
}

fn default_order_min_size() -> f64 {
    5.0
}

// ── Client ───────────────────────────────────────────────────────────────────

pub struct PolymarketClient {
    config: Config,
    http: reqwest::Client,
    market_cache: Mutex<Option<(String, MarketInfo)>>,
    sdk_client: Mutex<Option<SdkClobClient<Authenticated<Normal>>>>,
    sdk_signer: Option<PrivateKeySigner>,
}

impl PolymarketClient {
    pub fn new(config: Config) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .tcp_keepalive(Some(Duration::from_secs(20)))
            .pool_max_idle_per_host(4)
            .pool_idle_timeout(Duration::from_secs(90))
            .http2_keep_alive_interval(Duration::from_secs(15))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let parsed_pk = config.evm_private_key.as_deref().and_then(|pk| {
            let pk = pk.trim_start_matches("0x");
            match PrivateKeySigner::from_str(pk) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!(
                        "POLYMARKET_PRIVATE_KEY invalide — mode réel désactivé: {}",
                        e
                    );
                    None
                }
            }
        });

        let sdk_signer = parsed_pk.map(|s| s.with_chain_id(Some(POLYGON)));

        Self {
            config,
            http,
            market_cache: Mutex::new(None),
            sdk_client: Mutex::new(None),
            sdk_signer,
        }
    }

    /// Pré-chauffe la connexion TCP/TLS et le client SDK.
    pub async fn warm_up(&self) {
        match self.http.get(format!("{}/ok", CLOB_API_BASE)).send().await {
            Ok(_) => info!("Connexion CLOB Polymarket pré-chauffée"),
            Err(e) => warn!("warm_up CLOB échoué (non bloquant): {}", e),
        }
        match self.get_or_create_sdk_client().await {
            Ok(_) => info!("Client SDK Polymarket pré-authentifié"),
            Err(e) => warn!("warm_up SDK échoué (non bloquant): {}", e),
        }
    }

    /// Pré-chauffe les caches SDK (tick_size, fee_rate_bps, neg_risk).
    pub async fn warm_sdk_caches(&self, market: &MarketInfo) {
        let client = match self.get_or_create_sdk_client().await {
            Ok(c) => c,
            Err(_) => return,
        };
        for token_id in [&market.up_token_id, &market.down_token_id] {
            let _ = client.tick_size(token_id).await;
            let _ = client.fee_rate_bps(token_id).await;
            let _ = client.neg_risk(token_id).await;
        }
    }

    /// Keep-alive ping toutes les 20s (notre client HTTP + le client interne SDK).
    pub async fn run_keep_alive_loop(&self) {
        let mut ticker = tokio::time::interval(Duration::from_secs(20));
        loop {
            ticker.tick().await;
            // Keep-alive sur notre client HTTP
            let _ = self.http.get(format!("{}/ok", CLOB_API_BASE)).send().await;
            // Keep-alive sur le client HTTP interne du SDK (même connection pool)
            if let Ok(client) = self.get_or_create_sdk_client().await {
                let _ = client.server_time().await;
            }
        }
    }

    /// Construit le slug : `{prefix}-{unix_seconds}`
    pub fn build_slug(prefix: &str, open_time_ms: i64) -> String {
        let unix_secs = open_time_ms / 1000;
        format!("{}-{}", prefix, unix_secs)
    }

    /// Résout slug → MarketInfo via l'API Gamma.
    pub async fn resolve_market(&self, slug: &str) -> Result<MarketInfo> {
        {
            let cache = self.market_cache.lock().await;
            if let Some((cached_slug, info)) = cache.as_ref() {
                if cached_slug == slug {
                    return Ok(info.clone());
                }
            }
        }

        let url = format!("{}/markets?slug={}", GAMMA_API_BASE, slug);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow!("Gamma API GET échoué: {}", e))?;

        if !resp.status().is_success() {
            return Err(anyhow!("Gamma API {} → HTTP {}", url, resp.status()));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("Gamma API lecture body: {}", e))?;

        let markets: Vec<GammaMarket> = serde_json::from_str(&body).map_err(|e| {
            anyhow!(
                "Gamma API parse JSON: {} | body={}",
                e,
                &body[..body.len().min(300)]
            )
        })?;

        let market = markets
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Aucun marché trouvé pour le slug '{}'", slug))?;

        let outcomes: Vec<String> = serde_json::from_str(&market.outcomes)
            .map_err(|e| anyhow!("Impossible de parser outcomes: {}", e))?;
        let token_ids: Vec<String> = serde_json::from_str(&market.clob_token_ids)
            .map_err(|e| anyhow!("Impossible de parser clobTokenIds: {}", e))?;

        let up_idx = outcomes
            .iter()
            .position(|o| o.eq_ignore_ascii_case("up"))
            .ok_or_else(|| anyhow!("Outcome 'Up' introuvable pour '{}'", slug))?;
        let down_idx = outcomes
            .iter()
            .position(|o| o.eq_ignore_ascii_case("down"))
            .ok_or_else(|| anyhow!("Outcome 'Down' introuvable pour '{}'", slug))?;

        let info = MarketInfo {
            condition_id: market.condition_id,
            up_token_id: token_ids[up_idx].clone(),
            down_token_id: token_ids[down_idx].clone(),
            slug: slug.to_string(),
            order_min_size: market.order_min_size,
        };

        debug!(
            "Marché résolu: slug={} UP={} DOWN={}",
            slug, info.up_token_id, info.down_token_id
        );
        *self.market_cache.lock().await = Some((slug.to_string(), info.clone()));
        Ok(info)
    }

    /// Place un ordre BUY market FOK.
    /// Retourne le fill_price et les shares reçues.
    /// Place un ordre BUY market FOK.
    /// `current_price` est le prix courant du token (utilisé pour simuler le fill en dry-run).
    pub async fn buy_market(
        &self,
        token_id: &str,
        amount_usdc: f64,
        current_price: f64,
    ) -> Result<OrderResult> {
        let submitted_at = Utc::now();

        if self.config.execution_mode == ExecutionMode::DryRun {
            let fill = current_price;
            let shares = if fill > 0.0 { amount_usdc / fill } else { 0.0 };
            info!(
                "[DRY-RUN BUY] token={} amount={:.2} USDC fill={:.2}¢ shares={:.4}",
                &token_id[..8],
                amount_usdc,
                fill * 100.0,
                shares
            );
            return Ok(OrderResult {
                order_id: format!("dry-{}", uuid::Uuid::new_v4()),
                status: "Matched".to_string(),
                fill_price: fill,
                shares,
                submitted_at,
                ack_at: Utc::now(),
            });
        }

        self.submit_buy_with_retry(token_id, submitted_at, amount_usdc)
            .await
    }

    /// Place un ordre SELL market FOK pour toutes les shares.
    pub async fn sell_market(
        &self,
        token_id: &str,
        shares: f64,
    ) -> Result<OrderResult> {
        let submitted_at = Utc::now();

        if self.config.execution_mode == ExecutionMode::DryRun {
            info!(
                "[DRY-RUN SELL] token={} shares={:.4}",
                &token_id[..8],
                shares
            );
            return Ok(OrderResult {
                order_id: format!("dry-{}", uuid::Uuid::new_v4()),
                status: "Matched".to_string(),
                fill_price: 0.0,
                shares,
                submitted_at,
                ack_at: Utc::now(),
            });
        }

        self.submit_sell_with_retry(token_id, submitted_at, shares)
            .await
    }

    // ── Internals ────────────────────────────────────────────────────────────

    async fn get_or_create_sdk_client(&self) -> Result<SdkClobClient<Authenticated<Normal>>> {
        let mut guard = self.sdk_client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let sdk_signer = self
            .sdk_signer
            .as_ref()
            .ok_or_else(|| anyhow!("POLYMARKET_PRIVATE_KEY requis pour le mode Market"))?;

        let auth_builder = SdkClobClient::new(CLOB_API_BASE, SdkConfig::default())
            .map_err(|e| anyhow!("SDK client init: {}", e))?
            .authentication_builder(sdk_signer);

        let client = if let Some(funder) = self.config.polymarket_funder.as_deref() {
            let funder = Address::from_str(funder)
                .map_err(|e| anyhow!("POLYMARKET_FUNDER invalide: {}", e))?;
            let signature_type = match self.config.polymarket_signature_type.unwrap_or(1) {
                0 => SdkSignatureType::Eoa,
                1 => SdkSignatureType::Proxy,
                2 => SdkSignatureType::GnosisSafe,
                other => return Err(anyhow!("POLYMARKET_SIGNATURE_TYPE={} invalide", other)),
            };
            auth_builder
                .funder(funder)
                .signature_type(signature_type)
                .authenticate()
                .await
                .map_err(|e| anyhow!("SDK authenticate avec funder: {}", e))?
        } else {
            auth_builder
                .authenticate()
                .await
                .map_err(|e| anyhow!("SDK authenticate: {}", e))?
        };

        info!("Client SDK Polymarket authentifié et mis en cache");
        *guard = Some(client.clone());
        Ok(client)
    }

    async fn submit_buy_order(
        &self,
        token_id: &str,
        submitted_at: DateTime<Utc>,
        amount_usdc: f64,
    ) -> Result<OrderResult> {
        use std::time::Instant;

        let sdk_signer = self
            .sdk_signer
            .as_ref()
            .ok_or_else(|| anyhow!("POLYMARKET_PRIVATE_KEY requis"))?;

        let t0 = Instant::now();
        let client = self.get_or_create_sdk_client().await?;
        let t_client = t0.elapsed().as_millis();

        let truncated = (amount_usdc * 100.0).floor() / 100.0;
        let amount = Decimal::from_str(&format!("{:.2}", truncated))
            .map_err(|e| anyhow!("Decimal: {}", e))?;
        let max_price =
            Decimal::from_str("0.99").map_err(|e| anyhow!("Decimal: {}", e))?;

        let order = client
            .market_order()
            .token_id(token_id)
            .amount(Amount::usdc(amount).map_err(|e| anyhow!("Amount: {}", e))?)
            .price(max_price)
            .side(SdkSide::Buy)
            .order_type(SdkOrderType::FAK)
            .build()
            .await
            .map_err(|e| anyhow!("SDK build: {}", e))?;
        let t_build = t0.elapsed().as_millis();

        let signed = client
            .sign(sdk_signer, order)
            .await
            .map_err(|e| anyhow!("SDK sign: {}", e))?;
        let t_sign = t0.elapsed().as_millis();

        let resp = client
            .post_order(signed)
            .await
            .map_err(|e| anyhow!("SDK post_order: {}", e))?;

        let ack_at = Utc::now();
        let total_ms = t0.elapsed().as_millis();

        // Calculer le fill price : making_amount (USDC payé) / taking_amount (shares reçues)
        let making: f64 = resp.making_amount.to_string().parse().unwrap_or(0.0);
        let taking: f64 = resp.taking_amount.to_string().parse().unwrap_or(0.0);
        let fill_price = if taking > 0.0 { making / taking } else { 0.0 };

        // FAK : fill partiel possible — si taking == 0, rien n'a été rempli
        if taking <= 0.0 {
            return Err(anyhow!("FAK order zero fill — aucune liquidité disponible"));
        }

        info!(
            "[BUY] token={} amount={:.2}USDC | fill={:.4} shares={:.4} | {}ms (client={}ms build={}ms sign={}ms post={}ms)",
            &token_id[..8], amount_usdc, fill_price, taking, total_ms,
            t_client, t_build - t_client, t_sign - t_build, total_ms - t_sign
        );

        Ok(OrderResult {
            order_id: format!("{:?}", resp.order_id).trim_matches('"').to_string(),
            status: format!("{:?}", resp.status).trim_matches('"').to_string(),
            fill_price,
            shares: taking,
            submitted_at,
            ack_at,
        })
    }

    async fn submit_sell_order(
        &self,
        token_id: &str,
        submitted_at: DateTime<Utc>,
        shares: f64,
    ) -> Result<OrderResult> {
        use std::time::Instant;

        let sdk_signer = self
            .sdk_signer
            .as_ref()
            .ok_or_else(|| anyhow!("POLYMARKET_PRIVATE_KEY requis"))?;

        let t0 = Instant::now();
        let client = self.get_or_create_sdk_client().await?;
        let t_client = t0.elapsed().as_millis();

        // Tronquer les shares à 2 décimales (limite SDK)
        let truncated_shares = (shares * 100.0).floor() / 100.0;
        let share_amount = Decimal::from_str(&format!("{:.2}", truncated_shares))
            .map_err(|e| anyhow!("Decimal shares: {}", e))?;

        // Prix plancher 0.01 pour SELL market (garantir le fill au meilleur bid)
        let min_price =
            Decimal::from_str("0.01").map_err(|e| anyhow!("Decimal: {}", e))?;

        let order = client
            .market_order()
            .token_id(token_id)
            .amount(
                Amount::shares(share_amount).map_err(|e| anyhow!("Amount: {}", e))?,
            )
            .price(min_price)
            .side(SdkSide::Sell)
            .order_type(SdkOrderType::FAK)
            .build()
            .await
            .map_err(|e| anyhow!("SDK build sell: {}", e))?;
        let t_build = t0.elapsed().as_millis();

        let signed = client
            .sign(sdk_signer, order)
            .await
            .map_err(|e| anyhow!("SDK sign sell: {}", e))?;
        let t_sign = t0.elapsed().as_millis();

        let resp = client
            .post_order(signed)
            .await
            .map_err(|e| anyhow!("SDK post_order sell: {}", e))?;

        let ack_at = Utc::now();
        let total_ms = t0.elapsed().as_millis();

        let making: f64 = resp.making_amount.to_string().parse().unwrap_or(0.0);
        let taking: f64 = resp.taking_amount.to_string().parse().unwrap_or(0.0);
        // Pour SELL : making = shares vendues, taking = USDC reçus
        let fill_price = if making > 0.0 { taking / making } else { 0.0 };

        // FAK : si making == 0, rien n'a été vendu
        if making <= 0.0 {
            return Err(anyhow!("FAK sell zero fill — aucune liquidité disponible"));
        }

        info!(
            "[SELL] token={} shares={:.4} | fill={:.4} usdc_received={:.4} | {}ms (client={}ms build={}ms sign={}ms post={}ms)",
            &token_id[..8], shares, fill_price, taking, total_ms,
            t_client, t_build - t_client, t_sign - t_build, total_ms - t_sign
        );

        Ok(OrderResult {
            order_id: format!("{:?}", resp.order_id).trim_matches('"').to_string(),
            status: format!("{:?}", resp.status).trim_matches('"').to_string(),
            fill_price,
            shares: making,
            submitted_at,
            ack_at,
        })
    }

    async fn submit_buy_with_retry(
        &self,
        token_id: &str,
        submitted_at: DateTime<Utc>,
        amount_usdc: f64,
    ) -> Result<OrderResult> {
        let mut attempt = 0usize;
        loop {
            match self
                .submit_buy_order(token_id, submitted_at, amount_usdc)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) if Self::is_fok_unfilled(&e) && attempt < FOK_RETRY_DELAYS_SECS.len() => {
                    let delay = FOK_RETRY_DELAYS_SECS[attempt];
                    warn!(
                        "BUY FOK non rempli — retry {}/{} dans {}s",
                        attempt + 1,
                        FOK_RETRY_DELAYS_SECS.len(),
                        delay
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn submit_sell_with_retry(
        &self,
        token_id: &str,
        submitted_at: DateTime<Utc>,
        shares: f64,
    ) -> Result<OrderResult> {
        let mut current_shares = shares;
        let mut attempt = 0usize;
        loop {
            match self
                .submit_sell_order(token_id, submitted_at, current_shares)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) if Self::is_balance_error(&e) => {
                    if let Some(balance) = Self::extract_balance(&e) {
                        let adjusted = (balance / 1_000_000.0 * 100.0).floor() / 100.0;
                        if adjusted <= 0.0 {
                            // Solde 0 = marché déjà réglé par Polymarket, shares rachetées automatiquement
                            warn!(
                                "[SELL] Solde 0 — marché déjà réglé, position fermée sans vente"
                            );
                            return Ok(OrderResult {
                                order_id: "market-settled".to_string(),
                                status: "settled".to_string(),
                                fill_price: 0.0,
                                shares: 0.0,
                                submitted_at,
                                ack_at: Utc::now(),
                            });
                        }
                        warn!(
                            "[SELL] Balance insuffisante — retry avec solde réel: {:.4} shares (demandé: {:.4})",
                            adjusted, current_shares
                        );
                        current_shares = adjusted;
                        continue;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_fok_unfilled(&e) && attempt < FOK_RETRY_DELAYS_SECS.len() => {
                    let delay = FOK_RETRY_DELAYS_SECS[attempt];
                    warn!(
                        "SELL FOK non rempli — retry {}/{} dans {}s",
                        attempt + 1,
                        FOK_RETRY_DELAYS_SECS.len(),
                        delay
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Place un ordre GTC limit BUY et retourne l'order_id + timing.
    /// Utilisé pour mesurer la latence de placement (vs FOK matching).
    pub async fn place_limit_buy(
        &self,
        token_id: &str,
        amount_usdc: f64,
        limit_price: f64,
    ) -> Result<OrderResult> {
        use std::time::Instant;

        let sdk_signer = self
            .sdk_signer
            .as_ref()
            .ok_or_else(|| anyhow!("POLYMARKET_PRIVATE_KEY requis"))?;

        let t0 = Instant::now();
        let client = self.get_or_create_sdk_client().await?;
        let t_client = t0.elapsed().as_millis();

        let raw_shares = (amount_usdc / limit_price * 100.0).floor() / 100.0;
        // Minimum 5 shares pour les ordres limit sur Polymarket
        let final_shares = raw_shares.max(5.0);
        let price = Decimal::from_str(&format!("{:.2}", limit_price))
            .map_err(|e| anyhow!("Decimal: {}", e))?;
        let shares = Decimal::from_str(&format!("{:.2}", final_shares))
            .map_err(|e| anyhow!("Decimal: {}", e))?;

        let order = client
            .limit_order()
            .token_id(token_id)
            .price(price)
            .size(shares)
            .side(SdkSide::Buy)
            .order_type(SdkOrderType::GTC)
            .build()
            .await
            .map_err(|e| anyhow!("SDK build limit: {}", e))?;
        let t_build = t0.elapsed().as_millis();

        let signed = client
            .sign(sdk_signer, order)
            .await
            .map_err(|e| anyhow!("SDK sign limit: {}", e))?;
        let t_sign = t0.elapsed().as_millis();

        let resp = client
            .post_order(signed)
            .await
            .map_err(|e| anyhow!("SDK post_order limit: {}", e))?;

        let ack_at = Utc::now();
        let total_ms = t0.elapsed().as_millis();

        let order_id = format!("{:?}", resp.order_id).trim_matches('"').to_string();

        info!(
            "[LIMIT BUY PLACED] token={} price={:.2}¢ amount={:.2}USDC | {}ms (client={}ms build={}ms sign={}ms post={}ms) | order_id={}",
            &token_id[..8], limit_price * 100.0, amount_usdc, total_ms,
            t_client, t_build - t_client, t_sign - t_build, total_ms - t_sign,
            &order_id
        );

        Ok(OrderResult {
            order_id,
            status: format!("{:?}", resp.status).trim_matches('"').to_string(),
            fill_price: limit_price,
            shares: shares.to_string().parse().unwrap_or(0.0),
            submitted_at: Utc::now(),
            ack_at,
        })
    }

    /// Place un ordre GTC limit SELL.
    pub async fn place_limit_sell(
        &self,
        token_id: &str,
        shares: f64,
        limit_price: f64,
    ) -> Result<OrderResult> {
        use std::time::Instant;

        let sdk_signer = self
            .sdk_signer
            .as_ref()
            .ok_or_else(|| anyhow!("POLYMARKET_PRIVATE_KEY requis"))?;

        let t0 = Instant::now();
        let client = self.get_or_create_sdk_client().await?;
        let t_client = t0.elapsed().as_millis();

        let truncated_shares = (shares * 100.0).floor() / 100.0;
        let size = Decimal::from_str(&format!("{:.2}", truncated_shares))
            .map_err(|e| anyhow!("Decimal: {}", e))?;
        let price = Decimal::from_str(&format!("{:.2}", limit_price))
            .map_err(|e| anyhow!("Decimal: {}", e))?;

        let order = client
            .limit_order()
            .token_id(token_id)
            .price(price)
            .size(size)
            .side(SdkSide::Sell)
            .order_type(SdkOrderType::GTC)
            .build()
            .await
            .map_err(|e| anyhow!("SDK build limit sell: {}", e))?;
        let t_build = t0.elapsed().as_millis();

        let signed = client
            .sign(sdk_signer, order)
            .await
            .map_err(|e| anyhow!("SDK sign limit sell: {}", e))?;
        let t_sign = t0.elapsed().as_millis();

        let resp = client
            .post_order(signed)
            .await
            .map_err(|e| anyhow!("SDK post_order limit sell: {}", e))?;

        let ack_at = Utc::now();
        let total_ms = t0.elapsed().as_millis();

        let order_id = format!("{:?}", resp.order_id).trim_matches('"').to_string();

        info!(
            "[LIMIT SELL PLACED] token={} price={:.2}¢ shares={:.4} | {}ms (client={}ms build={}ms sign={}ms post={}ms) | order_id={}",
            &token_id[..8], limit_price * 100.0, shares, total_ms,
            t_client, t_build - t_client, t_sign - t_build, total_ms - t_sign,
            &order_id
        );

        Ok(OrderResult {
            order_id,
            status: format!("{:?}", resp.status).trim_matches('"').to_string(),
            fill_price: limit_price,
            shares: truncated_shares,
            submitted_at: Utc::now(),
            ack_at,
        })
    }

    /// Annule un ordre par son ID.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        use std::time::Instant;

        let t0 = Instant::now();
        let client = self.get_or_create_sdk_client().await?;
        client
            .cancel_order(order_id)
            .await
            .map_err(|e| anyhow!("SDK cancel_order: {}", e))?;
        let total_ms = t0.elapsed().as_millis();

        info!("[ORDER CANCELLED] order_id={} | {}ms", order_id, total_ms);
        Ok(())
    }

    /// Annule tous les ordres ouverts.
    pub async fn cancel_all_orders(&self) -> Result<()> {
        use std::time::Instant;

        let t0 = Instant::now();
        let client = self.get_or_create_sdk_client().await?;
        client
            .cancel_all_orders()
            .await
            .map_err(|e| anyhow!("SDK cancel_all_orders: {}", e))?;
        let total_ms = t0.elapsed().as_millis();

        info!("[ALL ORDERS CANCELLED] | {}ms", total_ms);
        Ok(())
    }

    /// Vérifie si un trade BUY a été exécuté sur ce token dans les N dernières secondes.
    /// Utilisé pour détecter les ordres "ghost" après un 500.
    pub async fn fetch_recent_fill(
        &self,
        token_id: &str,
        within_secs: i64,
    ) -> Option<(f64, f64)> {
        let address = self.sdk_signer.as_ref()?.address().to_string().to_lowercase();

        #[derive(Deserialize)]
        struct Trade {
            side: String,
            price: String,
            size: String,
            #[serde(rename = "matchTime")]
            match_time: Option<String>,
        }

        let url = format!(
            "{}/data/trades?maker_address={}&asset_id={}",
            CLOB_API_BASE, address, token_id
        );

        let resp = self.http.get(&url).send().await.ok()?;
        let trades: Vec<Trade> = resp.json().await.ok()?;

        let cutoff = Utc::now().timestamp() - within_secs;
        for trade in &trades {
            if !trade.side.eq_ignore_ascii_case("buy") {
                continue;
            }
            // Vérifier si le trade est récent (si match_time disponible)
            if let Some(ref mt) = trade.match_time {
                if let Ok(ts) = mt.parse::<i64>() {
                    if ts < cutoff {
                        continue;
                    }
                }
            }
            let price: f64 = trade.price.parse().unwrap_or(0.0);
            let size: f64 = trade.size.parse().unwrap_or(0.0);
            if price > 0.0 && size > 0.0 {
                return Some((price, size));
            }
        }
        None
    }

    fn is_fok_unfilled(err: &anyhow::Error) -> bool {
        let msg = err.to_string().to_ascii_lowercase();
        msg.contains("fok orders are fully filled or killed")
            || msg.contains("order couldn't be fully filled")
            || msg.contains("fak order zero fill")
            || msg.contains("could not run the execution")
    }

    fn is_balance_error(err: &anyhow::Error) -> bool {
        err.to_string().contains("not enough balance")
    }

    /// Extrait le solde réel depuis l'erreur "balance: 1232000, order amount: 1250000"
    fn extract_balance(err: &anyhow::Error) -> Option<f64> {
        let msg = err.to_string();
        let marker = "balance: ";
        let start = msg.find(marker)? + marker.len();
        let rest = &msg[start..];
        let end = rest.find(|c: char| !c.is_ascii_digit())?;
        rest[..end].parse::<f64>().ok()
    }
}
