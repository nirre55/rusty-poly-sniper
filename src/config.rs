use anyhow::Result;
use std::env;
use tracing::warn;

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    DryRun,
    Market,
    NoTpNoSl,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionMode::DryRun => "DRY_RUN",
            ExecutionMode::Market => "MARKET",
            ExecutionMode::NoTpNoSl => "NO_TP_NO_SL",
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub execution_mode: ExecutionMode,
    pub polymarket_slug_prefix: String,
    pub interval: String,
    /// Seuil d'entrée : acheter quand prix token >= ce seuil (ex: 0.80 = 80¢)
    pub entry_threshold: f64,
    /// Si true, aucun TP ni ordre limite — la position court jusqu'à expiration du marché.
    pub tp_disabled: bool,
    /// Offset TP en cents (ex: 5 = +5¢ depuis le fill price). Ignoré si tp_price_fixed est défini.
    pub tp_offset_cents: f64,
    /// Prix TP fixe absolu (ex: 0.98 = 98¢). Si défini, remplace tp_offset_cents.
    pub tp_price_fixed: Option<f64>,
    /// Offset SL en cents (ex: 20 = -20¢ depuis le fill price). Ignoré si sl_price_fixed est défini.
    pub sl_offset_cents: f64,
    /// Prix SL fixe absolu (ex: 0.60 = 60¢). Si défini, remplace sl_offset_cents.
    pub sl_price_fixed: Option<f64>,
    /// Montant fixe par trade en USDC (mutuellement exclusif avec trade_amount_pct)
    pub trade_amount_usdc: f64,
    /// Pourcentage du solde USDC par trade (ex: 5.0 = 5%). Minimum 1$. 0.0 = désactivé.
    pub trade_amount_pct: f64,
    /// Clé privée EVM (hex). Requis pour mode market.
    pub evm_private_key: Option<String>,
    /// Adresse funder Polymarket (proxy/safe).
    pub polymarket_funder: Option<String>,
    /// Signature type: 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE.
    pub polymarket_signature_type: Option<u8>,
    /// Multiplicateur Martingale (sur LOSS). 1.0 = désactivé.
    pub martingale_multiplier: f64,
    /// Multiplicateur Anti-Martingale (sur WIN). 1.0 = désactivé.
    pub anti_martingale_multiplier: f64,
    /// Montant max Martingale/Anti-Martingale en USDC. 0.0 = pas de plafond.
    pub martingale_max_amount: f64,
    /// Nombre max de wins consécutifs avant reset (Anti-Martingale). 0 = pas de limite.
    pub anti_martingale_max_streak: u32,
    /// Multiplicateur intra-marché ping-pong. 1.0 = désactivé (1 position max par côté).
    pub inmarket_multiplier: f64,
    /// Nombre max d'entrées ping-pong par marché. 0 = illimité.
    pub inmarket_max_entries: u32,
    /// Nombre max de trades par marché en mode market. 0 = illimité.
    pub market_max_trades: u32,
    /// Jours exclus du trading (ex: ["sat","sun"]). Vide = pas de filtre.
    pub excluded_days: Vec<String>,
    /// Plages horaires exclues UTC (ex: [(0,6),(22,24)]). Vide = pas de filtre.
    pub excluded_hours: Vec<(u32, u32)>,
    /// Entrée inversée : quand UP atteint le seuil, acheter DOWN (et vice-versa).
    pub reverse_entry: bool,
    /// Seuil reversal : si le dernier côté entré redescend à ce prix, on ouvre sur l'opposé. 0.0 = désactivé.
    pub reversal_threshold: f64,
    /// Montant du trade reversal en USDC.
    pub reversal_amount: f64,
    pub logs_dir: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("execution_mode", &self.execution_mode)
            .field("polymarket_slug_prefix", &self.polymarket_slug_prefix)
            .field("interval", &self.interval)
            .field("entry_threshold", &self.entry_threshold)
            .field("tp_disabled", &self.tp_disabled)
            .field("tp_offset_cents", &self.tp_offset_cents)
            .field("tp_price_fixed", &self.tp_price_fixed)
            .field("sl_offset_cents", &self.sl_offset_cents)
            .field("sl_price_fixed", &self.sl_price_fixed)
            .field("trade_amount_usdc", &self.trade_amount_usdc)
            .field("trade_amount_pct", &self.trade_amount_pct)
            .field("evm_private_key", &"[REDACTED]")
            .field("polymarket_funder", &self.polymarket_funder)
            .field("polymarket_signature_type", &self.polymarket_signature_type)
            .field("martingale_multiplier", &self.martingale_multiplier)
            .field("anti_martingale_multiplier", &self.anti_martingale_multiplier)
            .field("martingale_max_amount", &self.martingale_max_amount)
            .field("anti_martingale_max_streak", &self.anti_martingale_max_streak)
            .field("inmarket_multiplier", &self.inmarket_multiplier)
            .field("inmarket_max_entries", &self.inmarket_max_entries)
            .field("market_max_trades", &self.market_max_trades)
            .field("excluded_days", &self.excluded_days)
            .field("excluded_hours", &self.excluded_hours)
            .field("reverse_entry", &self.reverse_entry)
            .field("reversal_threshold", &self.reversal_threshold)
            .field("reversal_amount", &self.reversal_amount)
            .field("logs_dir", &self.logs_dir)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let mode = env::var("EXECUTION_MODE").unwrap_or_else(|_| "dry-run".to_string());
        let execution_mode = match mode.as_str() {
            "market" => ExecutionMode::Market,
            "no-tp-no-sl" => ExecutionMode::NoTpNoSl,
            "dry-run" | "dryrun" => ExecutionMode::DryRun,
            _ => {
                warn!("EXECUTION_MODE '{}' non reconnu — dry-run par défaut", mode);
                ExecutionMode::DryRun
            }
        };

        let trade_amount_pct = env::var("TRADE_AMOUNT_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        if trade_amount_pct != 0.0 && !(0.0 < trade_amount_pct && trade_amount_pct <= 100.0) {
            anyhow::bail!("TRADE_AMOUNT_PCT={} invalide — doit être entre 0 et 100 exclus", trade_amount_pct);
        }

        let pct_explicitly_set = env::var("TRADE_AMOUNT_PCT").is_ok();
        let usdc_explicitly_set = env::var("TRADE_AMOUNT_USDC").is_ok();
        if pct_explicitly_set && usdc_explicitly_set && trade_amount_pct > 0.0 {
            anyhow::bail!("TRADE_AMOUNT_PCT et TRADE_AMOUNT_USDC ne peuvent pas être définis en même temps");
        }

        let raw_amount = env::var("TRADE_AMOUNT_USDC").unwrap_or_else(|_| "1.0".to_string());
        let trade_amount_usdc = match raw_amount.parse::<f64>() {
            Ok(v) if v > 0.0 => v,
            _ => {
                warn!("TRADE_AMOUNT_USDC='{}' invalide — 1.0 par défaut", raw_amount);
                1.0
            }
        };

        let entry_threshold = env::var("ENTRY_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.80);

        let tp_disabled = env::var("TP_DISABLED")
            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);

        let tp_offset_cents = env::var("TP_OFFSET_CENTS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(5.0);

        let tp_price_fixed = match env::var("TP_PRICE") {
            Ok(raw) => match raw.parse::<f64>() {
                Ok(v) if v > 0.0 && v <= 1.0 => Some(v),
                _ => {
                    warn!("TP_PRICE='{}' invalide — doit être entre 0 et 1.0 (ex: 0.99)", raw);
                    None
                }
            },
            Err(_) => None,
        };

        let sl_offset_cents = env::var("SL_OFFSET_CENTS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(20.0);

        let sl_price_fixed = match env::var("SL_PRICE") {
            Ok(raw) => match raw.parse::<f64>() {
                Ok(v) if v > 0.0 && v < 1.0 => Some(v),
                _ => {
                    warn!("SL_PRICE='{}' invalide — doit être entre 0 et 1.0 exclus (ex: 0.60)", raw);
                    None
                }
            },
            Err(_) => None,
        };

        let polymarket_signature_type = match env::var("POLYMARKET_SIGNATURE_TYPE") {
            Ok(raw) => match raw.parse::<u8>() {
                Ok(v @ 0..=2) => Some(v),
                _ => {
                    warn!("POLYMARKET_SIGNATURE_TYPE='{}' invalide — ignoré", raw);
                    None
                }
            },
            Err(_) => None,
        };

        let martingale_multiplier = env::var("MARTINGALE_MULTIPLIER")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0);

        let anti_martingale_multiplier = env::var("ANTI_MARTINGALE_MULTIPLIER")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0);

        // On ne peut pas activer les deux en même temps
        if martingale_multiplier > 1.0 && anti_martingale_multiplier > 1.0 {
            anyhow::bail!(
                "MARTINGALE_MULTIPLIER={} et ANTI_MARTINGALE_MULTIPLIER={} ne peuvent pas être actifs en même temps",
                martingale_multiplier, anti_martingale_multiplier
            );
        }

        let martingale_max_amount = env::var("MARTINGALE_MAX_AMOUNT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        let anti_martingale_max_streak = env::var("ANTI_MARTINGALE_MAX_STREAK")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(4);

        let inmarket_multiplier = env::var("INMARKET_MULTIPLIER")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0);

        let inmarket_max_entries = env::var("INMARKET_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        let market_max_trades = env::var("MARKET_MAX_TRADES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        let excluded_days = env::var("EXCLUDED_DAYS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        // Plages horaires exclues — format: "00h-06h" ou "00h-06h,22h-24h"
        let excluded_hours: Vec<(u32, u32)> = env::var("EXCLUDED_HOURS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|range| {
                let range = range.trim();
                if range.is_empty() { return None; }
                let parts: Vec<&str> = range.splitn(2, '-').collect();
                if parts.len() != 2 { return None; }
                let parse_h = |s: &str| s.trim().trim_end_matches('h').parse::<u32>().ok();
                let start = parse_h(parts[0])?;
                let end = parse_h(parts[1])?;
                if start >= end || end > 24 {
                    warn!("EXCLUDED_HOURS: plage invalide '{}' ignorée", range);
                    return None;
                }
                Some((start, end))
            })
            .collect();

        let reverse_entry = env::var("REVERSE_ENTRY")
            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);

        let reversal_threshold = env::var("REVERSAL_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        let reversal_amount = env::var("REVERSAL_AMOUNT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok(Config {
            execution_mode,
            polymarket_slug_prefix: env::var("POLYMARKET_SLUG_PREFIX")
                .unwrap_or_else(|_| "btc-updown-5m".to_string()),
            interval: env::var("INTERVAL").unwrap_or_else(|_| "5m".to_string()),
            entry_threshold,
            tp_disabled,
            tp_offset_cents,
            tp_price_fixed,
            sl_offset_cents,
            sl_price_fixed,
            trade_amount_usdc,
            trade_amount_pct,
            evm_private_key: env::var("POLYMARKET_PRIVATE_KEY").ok(),
            polymarket_funder: env::var("POLYMARKET_FUNDER").ok(),
            polymarket_signature_type,
            martingale_multiplier,
            anti_martingale_multiplier,
            martingale_max_amount,
            anti_martingale_max_streak,
            inmarket_multiplier,
            inmarket_max_entries,
            market_max_trades,
            excluded_days,
            excluded_hours,
            reverse_entry,
            reversal_threshold,
            reversal_amount,
            logs_dir: env::var("LOGS_DIR").unwrap_or_else(|_| "logs".to_string()),
        })
    }

    /// Durée de l'intervalle en secondes.
    pub fn interval_seconds(&self) -> Result<i64> {
        let s = &self.interval;
        if s.len() < 2 {
            anyhow::bail!("intervalle invalide: {}", s);
        }
        let (value, unit) = s.split_at(s.len() - 1);
        let value: i64 = value.parse()?;
        match unit {
            "m" => Ok(value * 60),
            "h" => Ok(value * 3600),
            "d" => Ok(value * 86400),
            _ => anyhow::bail!("unité d'intervalle non supportée: {}", s),
        }
    }
}
