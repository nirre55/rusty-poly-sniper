use anyhow::Result;
use std::env;
use tracing::warn;

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    DryRun,
    Market,
    Limit,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionMode::DryRun => "DRY_RUN",
            ExecutionMode::Market => "MARKET",
            ExecutionMode::Limit => "LIMIT",
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
    /// Offset TP en cents (ex: 5 = +5¢ depuis le fill price)
    pub tp_offset_cents: f64,
    /// Offset SL en cents (ex: 20 = -20¢ depuis le fill price)
    pub sl_offset_cents: f64,
    /// Montant fixe par trade en USDC
    pub trade_amount_usdc: f64,
    /// Clé privée EVM (hex). Requis pour mode market.
    pub evm_private_key: Option<String>,
    /// Adresse funder Polymarket (proxy/safe).
    pub polymarket_funder: Option<String>,
    /// Signature type: 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE.
    pub polymarket_signature_type: Option<u8>,
    /// Multiplicateur Martingale. 1.0 = désactivé.
    pub martingale_multiplier: f64,
    /// Montant max Martingale en USDC. 0.0 = pas de plafond.
    pub martingale_max_amount: f64,
    pub logs_dir: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("execution_mode", &self.execution_mode)
            .field("polymarket_slug_prefix", &self.polymarket_slug_prefix)
            .field("interval", &self.interval)
            .field("entry_threshold", &self.entry_threshold)
            .field("tp_offset_cents", &self.tp_offset_cents)
            .field("sl_offset_cents", &self.sl_offset_cents)
            .field("trade_amount_usdc", &self.trade_amount_usdc)
            .field("evm_private_key", &"[REDACTED]")
            .field("polymarket_funder", &self.polymarket_funder)
            .field("polymarket_signature_type", &self.polymarket_signature_type)
            .field("martingale_multiplier", &self.martingale_multiplier)
            .field("martingale_max_amount", &self.martingale_max_amount)
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
            "limit" => ExecutionMode::Limit,
            "dry-run" | "dryrun" => ExecutionMode::DryRun,
            _ => {
                warn!("EXECUTION_MODE '{}' non reconnu — dry-run par défaut", mode);
                ExecutionMode::DryRun
            }
        };

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

        let tp_offset_cents = env::var("TP_OFFSET_CENTS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(5.0);

        let sl_offset_cents = env::var("SL_OFFSET_CENTS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(20.0);

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

        let martingale_max_amount = env::var("MARTINGALE_MAX_AMOUNT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok(Config {
            execution_mode,
            polymarket_slug_prefix: env::var("POLYMARKET_SLUG_PREFIX")
                .unwrap_or_else(|_| "btc-updown-5m".to_string()),
            interval: env::var("INTERVAL").unwrap_or_else(|_| "5m".to_string()),
            entry_threshold,
            tp_offset_cents,
            sl_offset_cents,
            trade_amount_usdc,
            evm_private_key: env::var("POLYMARKET_PRIVATE_KEY").ok(),
            polymarket_funder: env::var("POLYMARKET_FUNDER").ok(),
            polymarket_signature_type,
            martingale_multiplier,
            martingale_max_amount,
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
