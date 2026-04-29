use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct MoneyState {
    consecutive_count: u32,
}

/// Gestion Martingale / Anti-Martingale de la taille de position.
///
/// Martingale (multiplier > 1.0):
///   LOSS → montant = base × multiplier^consecutive_losses
///   WIN  → reset à base
///
/// Anti-Martingale (anti_multiplier > 1.0):
///   WIN  → montant = base × anti_multiplier^consecutive_wins
///   LOSS → reset à base
///
/// Si les deux == 1.0 → taille constante.
pub struct MoneyManager {
    base_amount: f64,
    multiplier: f64,
    anti_multiplier: f64,
    consecutive_count: u32,
    max_amount: f64,
    max_streak: u32,
    state_path: PathBuf,
}

impl MoneyManager {
    pub fn new(
        base_amount: f64,
        multiplier: f64,
        anti_multiplier: f64,
        max_amount: f64,
        max_streak: u32,
        logs_dir: &str,
    ) -> Self {
        let state_path = PathBuf::from(logs_dir).join("money_state.json");
        let consecutive_count = Self::load_state(&state_path);
        let mode = if multiplier > 1.0 {
            "Martingale"
        } else if anti_multiplier > 1.0 {
            "Anti-Martingale"
        } else {
            "Désactivé"
        };
        if consecutive_count > 0 {
            let active_mult = if multiplier > 1.0 {
                multiplier
            } else {
                anti_multiplier
            };
            info!(
                "[MONEY] État rechargé : {} consécutifs ({}), montant courant = {:.2} USDC",
                consecutive_count,
                mode,
                base_amount * active_mult.powi(consecutive_count as i32)
            );
        }
        if max_amount > 0.0 {
            info!("[MONEY] Plafond = {:.2} USDC", max_amount);
        }
        Self {
            base_amount,
            multiplier,
            anti_multiplier,
            consecutive_count,
            max_amount,
            max_streak,
            state_path,
        }
    }

    pub fn current_amount(&self) -> f64 {
        let active_mult = if self.multiplier > 1.0 {
            self.multiplier
        } else if self.anti_multiplier > 1.0 {
            self.anti_multiplier
        } else {
            1.0
        };
        let amount = self.base_amount * active_mult.powi(self.consecutive_count as i32);
        if self.max_amount > 0.0 {
            amount.min(self.max_amount)
        } else {
            amount
        }
    }

    pub fn set_base_amount(&mut self, amount: f64) {
        self.base_amount = amount;
    }

    pub fn consecutive_losses(&self) -> u32 {
        self.consecutive_count
    }

    pub fn on_outcome(&mut self, outcome: &str) {
        if self.multiplier > 1.0 {
            // Mode Martingale: augmente sur LOSS, reset sur WIN
            match outcome {
                "WIN" => {
                    if self.consecutive_count > 0 {
                        info!(
                            "[MONEY] WIN après {} losses — reset à {:.2} USDC",
                            self.consecutive_count, self.base_amount
                        );
                    }
                    self.consecutive_count = 0;
                }
                "LOSS" => {
                    self.consecutive_count += 1;
                    info!(
                        "[MONEY] LOSS #{} — prochain montant = {:.2} USDC",
                        self.consecutive_count,
                        self.current_amount()
                    );
                }
                _ => return,
            }
        } else if self.anti_multiplier > 1.0 {
            // Mode Anti-Martingale: augmente sur WIN, reset sur LOSS
            match outcome {
                "WIN" => {
                    self.consecutive_count += 1;
                    if self.max_streak > 0 && self.consecutive_count >= self.max_streak {
                        info!(
                            "[MONEY] WIN #{} — max streak atteint ({}) — reset à {:.2} USDC",
                            self.consecutive_count, self.max_streak, self.base_amount
                        );
                        self.consecutive_count = 0;
                    } else {
                        info!(
                            "[MONEY] WIN #{} — prochain montant = {:.2} USDC",
                            self.consecutive_count,
                            self.current_amount()
                        );
                    }
                }
                "LOSS" => {
                    if self.consecutive_count > 0 {
                        info!(
                            "[MONEY] LOSS après {} wins — reset à {:.2} USDC",
                            self.consecutive_count, self.base_amount
                        );
                    }
                    self.consecutive_count = 0;
                }
                _ => return,
            }
        } else {
            return;
        }
        self.save_state();
    }

    fn load_state(state_path: &PathBuf) -> u32 {
        match fs::read_to_string(state_path) {
            Ok(content) => match serde_json::from_str::<MoneyState>(&content) {
                Ok(state) => state.consecutive_count,
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
            consecutive_count: self.consecutive_count,
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
