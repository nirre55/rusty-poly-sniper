use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct MoneyState {
    consecutive_losses: u32,
}

/// Gestion Martingale progressive de la taille de position.
///
/// Après chaque LOSS : montant = base × multiplier^consecutive_losses
/// Après un WIN : reset à base.
/// Si multiplier == 1.0, la taille est constante (Martingale désactivée).
pub struct MoneyManager {
    base_amount: f64,
    multiplier: f64,
    consecutive_losses: u32,
    max_amount: f64,
    state_path: PathBuf,
}

impl MoneyManager {
    pub fn new(base_amount: f64, multiplier: f64, max_amount: f64, logs_dir: &str) -> Self {
        let state_path = PathBuf::from(logs_dir).join("money_state.json");
        let consecutive_losses = Self::load_state(&state_path);
        if consecutive_losses > 0 {
            info!(
                "[MONEY] État rechargé : {} losses consécutifs, montant courant = {:.2} USDC",
                consecutive_losses,
                base_amount * multiplier.powi(consecutive_losses as i32)
            );
        }
        if max_amount > 0.0 {
            info!("[MONEY] Plafond Martingale = {:.2} USDC", max_amount);
        }
        Self {
            base_amount,
            multiplier,
            consecutive_losses,
            max_amount,
            state_path,
        }
    }

    pub fn current_amount(&self) -> f64 {
        let amount = self.base_amount * self.multiplier.powi(self.consecutive_losses as i32);
        if self.max_amount > 0.0 {
            amount.min(self.max_amount)
        } else {
            amount
        }
    }

    pub fn consecutive_losses(&self) -> u32 {
        self.consecutive_losses
    }

    pub fn on_outcome(&mut self, outcome: &str) {
        match outcome {
            "WIN" => {
                if self.consecutive_losses > 0 {
                    info!(
                        "[MONEY] WIN après {} losses — reset au montant de base {:.2} USDC",
                        self.consecutive_losses, self.base_amount
                    );
                }
                self.consecutive_losses = 0;
            }
            "LOSS" => {
                self.consecutive_losses += 1;
                info!(
                    "[MONEY] LOSS #{} — prochain montant = {:.2} USDC",
                    self.consecutive_losses,
                    self.current_amount()
                );
            }
            _ => return,
        }
        self.save_state();
    }

    fn load_state(state_path: &PathBuf) -> u32 {
        match fs::read_to_string(state_path) {
            Ok(content) => match serde_json::from_str::<MoneyState>(&content) {
                Ok(state) => state.consecutive_losses,
                Err(e) => {
                    warn!("[MONEY] money_state.json invalide: {} — reset à 0", e);
                    0
                }
            },
            Err(_) => 0,
        }
    }

    fn save_state(&self) {
        let state = MoneyState {
            consecutive_losses: self.consecutive_losses,
        };
        match serde_json::to_string_pretty(&state) {
            Ok(body) => {
                if let Err(e) = fs::write(&self.state_path, body) {
                    warn!("[MONEY] Sauvegarde état échouée: {}", e);
                }
            }
            Err(e) => warn!("[MONEY] Sérialisation état échouée: {}", e),
        }
    }
}
