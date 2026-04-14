# rusty-poly-sniper

Bot de trading automatique pour les marchés binaires Polymarket (BTC/ETH up/down).

## Prérequis

- Rust (stable)
- Compte Polymarket avec clé privée EVM

## Installation

```bash
cp .env.example .env
# Remplir .env avec vos clés
cargo build --release
```

## Modes d'exécution

| Mode | Description |
|------|-------------|
| `dry-run` | Simulation, aucun ordre réel |
| `market` | Trading réel avec TP et SL |
| `no-tp-no-sl` | Entry only — pas de TP ni SL |

## Configuration (`.env`)

### Base

| Variable | Défaut | Description |
|----------|--------|-------------|
| `EXECUTION_MODE` | `dry-run` | Mode d'exécution |
| `POLYMARKET_SLUG_PREFIX` | `btc-updown-5m` | Préfixe du marché |
| `INTERVAL` | `5m` | Durée d'un marché (`5m`, `15m`, `1h`…) |
| `ENTRY_THRESHOLD` | `0.80` | Prix d'entrée (ex: 0.80 = 80¢) |
| `TP_OFFSET_CENTS` | `5` | Offset Take Profit en cents |
| `SL_OFFSET_CENTS` | `20` | Offset Stop Loss en cents |
| `TRADE_AMOUNT_USDC` | `1.0` | Montant par trade en USDC |
| `LOGS_DIR` | `logs` | Répertoire des logs |

### Authentification

| Variable | Description |
|----------|-------------|
| `POLYMARKET_PRIVATE_KEY` | Clé privée EVM (hex) |
| `POLYMARKET_FUNDER` | Adresse funder (proxy/safe) |
| `POLYMARKET_SIGNATURE_TYPE` | `0`=EOA, `1`=POLY_PROXY, `2`=GNOSIS_SAFE |

### Gestion de mise (mode `market`)

| Variable | Défaut | Description |
|----------|--------|-------------|
| `MARKET_MAX_TRADES` | `0` | Max trades par marché (`0` = illimité) |

### Martingale / Anti-Martingale

Une seule peut être active à la fois. Les deux à `1.0` = désactivé.

| Variable | Défaut | Description |
|----------|--------|-------------|
| `MARTINGALE_MULTIPLIER` | `1.0` | Multiplie la mise après un LOSS |
| `ANTI_MARTINGALE_MULTIPLIER` | `1.0` | Multiplie la mise après un WIN |
| `ANTI_MARTINGALE_MAX_STREAK` | `4` | Reset après N wins consécutifs |
| `MARTINGALE_MAX_AMOUNT` | `0.0` | Plafond en USDC (`0` = illimité) |

### Ping-pong intra-marché (mode `no-tp-no-sl`)

Après chaque entry, surveille l'autre côté avec un montant croissant.

| Variable | Défaut | Description |
|----------|--------|-------------|
| `INMARKET_MULTIPLIER` | `1.0` | Multiplicateur par entry (`1.0` = désactivé) |
| `INMARKET_MAX_ENTRIES` | `0` | Max entries par marché (`0` = illimité) |

**Exemple** avec `INMARKET_MULTIPLIER=2.0` et `INMARKET_MAX_ENTRIES=2` :
- Entry #1 : UP touche 0.80 → BUY 1.00 USDC
- Entry #2 : DOWN touche 0.80 → BUY 2.00 USDC
- Limite atteinte, stop

### Reversal (mode `no-tp-no-sl`)

Après la fin du ping-pong, si le dernier côté entré retombe à un seuil bas, ouvre une position sur le côté opposé.

| Variable | Défaut | Description |
|----------|--------|-------------|
| `REVERSAL_THRESHOLD` | `0.0` | Prix déclencheur (`0` = désactivé) |
| `REVERSAL_AMOUNT` | `0.0` | Montant du trade reversal en USDC |

**Exemple** avec `REVERSAL_THRESHOLD=0.30` et `REVERSAL_AMOUNT=4.0` :
- Dernier entry : DOWN à 0.80
- DOWN redescend à 0.30 → BUY UP 4.00 USDC

## Lancer plusieurs instances en parallèle

Pour trader plusieurs marchés simultanément (ex: BTC et ETH), lancer deux instances dans des terminaux séparés en passant les variables via PowerShell. Le `LOGS_DIR` différent est **obligatoire** pour éviter les conflits sur `positions.json` et `money_state.json`.

**Terminal 1 :**
```powershell
$env:POLYMARKET_SLUG_PREFIX="btc-updown-5m"; $env:LOGS_DIR="logs/btc"; cargo run
```

**Terminal 2 :**
```powershell
$env:POLYMARKET_SLUG_PREFIX="eth-updown-5m"; $env:LOGS_DIR="logs/eth"; cargo run
```

Les variables passées ainsi écrasent celles du `.env`. Le reste de la config (clé privée, thresholds, etc.) est lu depuis le `.env` normalement.

## Logs

- `logs/trades.csv` — historique de tous les trades
- `logs/positions.json` — positions ouvertes (persistées entre redémarrages)
- `logs/money_state.json` — état martingale (pertes consécutives)
