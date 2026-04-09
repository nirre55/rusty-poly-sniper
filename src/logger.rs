use anyhow::{anyhow, Result};
use csv::WriterBuilder;
use serde::Serialize;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::info;

const CSV_HEADERS: [&str; 13] = [
    "trade_id",
    "slug",
    "token_side",
    "action",
    "amount_usdc",
    "shares",
    "fill_price",
    "tp_price",
    "sl_price",
    "exit_price",
    "exit_reason",
    "latency_ms",
    "timestamp_utc",
];

#[derive(Debug, Serialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub slug: String,
    pub token_side: String,
    pub action: String,
    pub amount_usdc: String,
    pub shares: String,
    pub fill_price: String,
    pub tp_price: String,
    pub sl_price: String,
    pub exit_price: String,
    pub exit_reason: String,
    pub latency_ms: String,
    pub timestamp_utc: String,
}

pub struct TradeLogger {
    csv_path: PathBuf,
    lock: Mutex<()>,
}

impl TradeLogger {
    pub fn new(logs_dir: &str) -> Result<Self> {
        fs::create_dir_all(logs_dir)?;
        let csv_path = PathBuf::from(logs_dir).join("trades.csv");

        let needs_header = !csv_path.exists()
            || fs::metadata(&csv_path)
                .map(|m| m.len() == 0)
                .unwrap_or(true);

        if needs_header {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&csv_path)?;
            let mut wtr = WriterBuilder::new().has_headers(true).from_writer(file);
            wtr.write_record(&CSV_HEADERS)?;
            wtr.flush()?;
        }

        Ok(Self {
            csv_path,
            lock: Mutex::new(()),
        })
    }

    pub fn log_trade(&self, record: &TradeRecord) -> Result<()> {
        let _guard = self
            .lock
            .lock()
            .map_err(|e| anyhow!("CSV lock poisoned: {}", e))?;
        let file = OpenOptions::new().append(true).open(&self.csv_path)?;
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(file);
        wtr.serialize(record)?;
        wtr.flush()?;
        info!(
            "Trade enregistré | id={} action={} side={}",
            record.trade_id, record.action, record.token_side
        );
        Ok(())
    }
}
