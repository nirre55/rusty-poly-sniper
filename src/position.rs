use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::{info, warn};

/// Calcule le TP : fill_price + offset, arrondi au cent supérieur.
/// Ex: fill=0.821, offset=5 → 0.821*100+5 = 87.1 → ceil = 88 → 0.88
pub fn calculate_tp(fill_price: f64, tp_offset_cents: f64) -> f64 {
    let raw_cents = fill_price * 100.0 + tp_offset_cents;
    raw_cents.ceil() / 100.0
}

/// Calcule le SL : fill_price - offset, arrondi au cent supérieur.
/// Ex: fill=0.821, offset=20 → 0.821*100-20 = 62.1 → ceil = 63 → 0.63
pub fn calculate_sl(fill_price: f64, sl_offset_cents: f64) -> f64 {
    let raw_cents = fill_price * 100.0 - sl_offset_cents;
    raw_cents.ceil() / 100.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub trade_id: String,
    pub slug: String,
    pub token_id: String,
    /// "UP" ou "DOWN"
    pub token_side: String,
    pub fill_price: f64,
    pub shares: f64,
    pub tp_price: f64,
    pub sl_price: f64,
    pub entry_time_utc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PositionState {
    positions: Vec<OpenPosition>,
}

/// Gère les positions ouvertes avec persistance JSON.
pub struct PositionManager {
    state_path: PathBuf,
    positions: Vec<OpenPosition>,
}

impl PositionManager {
    pub fn new(logs_dir: &str) -> Self {
        let state_path = PathBuf::from(logs_dir).join("positions.json");
        let positions = Self::load(&state_path);
        if !positions.is_empty() {
            info!(
                "[POSITION] {} position(s) rechargée(s) depuis le disque",
                positions.len()
            );
            for p in &positions {
                info!(
                    "[POSITION] {} {} | fill={:.2}¢ TP={:.2}¢ SL={:.2}¢ shares={:.4}",
                    p.token_side,
                    p.slug,
                    p.fill_price * 100.0,
                    p.tp_price * 100.0,
                    p.sl_price * 100.0,
                    p.shares
                );
            }
        }
        Self {
            state_path,
            positions,
        }
    }

    /// Ajoute une position et persiste.
    pub fn open(&mut self, position: OpenPosition) {
        info!(
            "[POSITION OUVERTE] {} {} | fill={:.2}¢ TP={:.2}¢ SL={:.2}¢ shares={:.4}",
            position.token_side,
            position.slug,
            position.fill_price * 100.0,
            position.tp_price * 100.0,
            position.sl_price * 100.0,
            position.shares
        );
        self.positions.push(position);
        self.save();
    }

    /// Retire une position par trade_id et persiste.
    pub fn close(&mut self, trade_id: &str) -> Option<OpenPosition> {
        if let Some(idx) = self.positions.iter().position(|p| p.trade_id == trade_id) {
            let pos = self.positions.remove(idx);
            info!(
                "[POSITION FERMÉE] {} {} | trade_id={}",
                pos.token_side, pos.slug, pos.trade_id
            );
            self.save();
            Some(pos)
        } else {
            None
        }
    }

    /// Retourne les positions ouvertes pour un slug donné.
    pub fn positions_for_slug(&self, slug: &str) -> Vec<&OpenPosition> {
        self.positions.iter().filter(|p| p.slug == slug).collect()
    }

    /// Vérifie si un token_id a déjà une position ouverte sur ce slug.
    pub fn has_position_on_slug(&self, slug: &str) -> bool {
        self.positions.iter().any(|p| p.slug == slug)
    }

    /// Retourne toutes les positions ouvertes.
    pub fn all_positions(&self) -> &[OpenPosition] {
        &self.positions
    }

    /// Supprime toutes les positions d'un slug (ex: marché expiré sans exit).
    pub fn clear_slug(&mut self, slug: &str) -> Vec<OpenPosition> {
        let (to_remove, to_keep): (Vec<_>, Vec<_>) =
            self.positions.drain(..).partition(|p| p.slug == slug);
        self.positions = to_keep;
        if !to_remove.is_empty() {
            self.save();
        }
        to_remove
    }

    fn load(path: &PathBuf) -> Vec<OpenPosition> {
        match fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str::<PositionState>(&content) {
                Ok(state) => state.positions,
                Err(e) => {
                    warn!("[POSITION] positions.json invalide: {} — reset", e);
                    Vec::new()
                }
            },
            Err(_) => Vec::new(),
        }
    }

    fn save(&self) {
        let state = PositionState {
            positions: self.positions.clone(),
        };
        match serde_json::to_string_pretty(&state) {
            Ok(body) => {
                if let Err(e) = fs::write(&self.state_path, body) {
                    warn!("[POSITION] Sauvegarde échouée: {}", e);
                }
            }
            Err(e) => warn!("[POSITION] Sérialisation échouée: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tp_exact_cents() {
        // 82¢ + 5 = 87¢ → pas de décimale → 0.87
        assert_eq!(calculate_tp(0.82, 5.0), 0.87);
    }

    #[test]
    fn test_tp_with_decimal_rounds_up() {
        // 82.1¢ + 5 = 87.1 → ceil = 88 → 0.88
        assert_eq!(calculate_tp(0.821, 5.0), 0.88);
    }

    #[test]
    fn test_sl_exact_cents() {
        // 82¢ - 20 = 62¢ → 0.62
        assert_eq!(calculate_sl(0.82, 20.0), 0.62);
    }

    #[test]
    fn test_sl_with_decimal_rounds_up() {
        // 82.1¢ - 20 = 62.1 → ceil = 63 → 0.63
        assert_eq!(calculate_sl(0.821, 20.0), 0.63);
    }

    #[test]
    fn test_tp_80_plus_5() {
        assert_eq!(calculate_tp(0.80, 5.0), 0.85);
    }

    #[test]
    fn test_sl_80_minus_20() {
        assert_eq!(calculate_sl(0.80, 20.0), 0.60);
    }
}
