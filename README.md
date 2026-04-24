# The Weather Machine

A live trading system for Kalshi daily temperature markets. Monitors HIGH and LOWT temperature markets across 20 US cities, enters No positions on brackets the forecast and market structure make unlikely to resolve Yes, and manages exits automatically.

---

## How it works

```
┌─────────────────────────────────────────────────────────────────┐
│  Continuous polling loop (5 min default, 3 min peak hours)     │
│                                                                 │
│  1. NWS fetch       →  live forecasts + observed highs         │
│  2. Kalshi scan     →  bracket prices, order book depth        │
│  3. Decision engine →  score signals, size positions           │
│  4. Cascade engine  →  market structure convergence signals    │
│  5. Exit check      →  forecast anchor + Yes ceiling           │
│  6. Portfolio sync  →  update session state                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Strategy

### Core thesis
Temperature markets follow a roughly normal distribution centred on the NWS forecast. Brackets far from the forecast mean (in the tails) have a high probability of settling No. The system bets No on those brackets.

### Main engine (`hight_decision_engine.py`)
Scores each bracket 0–5 across five components:

| Component | Signal |
|---|---|
| `obs_below_floor` | Observed high already below bracket floor |
| `forecast_well_clear` | Corrected forecast > 2°F below bracket floor |
| `momentum_flat_or_down` | Observed high not rising toward bracket |
| `spread_ok` | Bid-ask spread within acceptable range |
| `depth_ok` | Sufficient order book depth on No side |

Minimum score for entry: **2/5**. Position sizing by score:

| Score | Contracts |
|---|---|
| 2 | 2 |
| 3 | 4 |
| 4 | 5 |
| 5 | 6 |

Entry gate: No price must be in **[0.75, 0.92]**.

### Forecast bias correction
NWS morning forecasts have a measurable per-city bias. The system applies a correction to the raw forecast before scoring:

```
corrected_forecast = nws_forecast + city_bias
```

Bias values are computed by `bias_calculator.py` from historical observation data and stored in `data/forecast_bias.json`. Refreshed periodically as more data accumulates.

### Cascade engine (`cascade_engine.py`)
A separate signal path based purely on market structure — no NWS data required.

**Mechanism:** Temperature markets converge from the bottom up during the day as the observed high climbs. When any B bracket crosses No ≥ 0.97, the bracket immediately above it becomes the target. The first trigger locks the direction (bottom-up) for that city-day.

**Entry conditions:**
- No price in **[0.60, 0.90]**
- Skip the forecast bracket (most contested by definition)
- No new cascade starts after 15:00 local
- Max 1 entry if trigger fires after 13:00 local
- Max 2 entries default; 3 if top-T confirms before 13:00

**Contract sizing (by conviction):**

| No price range | Contracts |
|---|---|
| 0.60 – 0.70 | 2 |
| 0.71 – 0.80 | 4 |
| 0.81 – 0.90 | 6 |

**Backtest (Apr 6–23, 18 days):** 108 signals, 86.1% win rate, 1 bad day.

### Exit logic
Single rule: **Yes ≥ 0.60 after 15:00 local + forecast anchor.**

- **Yes ceiling:** If Yes crosses 0.60 after 15:00 local, exit — *unless* the corrected forecast is more than 2°F above the bracket floor (spike is noise, hold).
- **Forecast anchor:** If the observed high is within 1°F of the bracket floor and the corrected forecast is ambiguous, exit regardless of hour.

No stop-loss. Data showed holding almost always beats exiting on price alone.

---

## Capital management

Day-open snapshot system — budget is fixed once per calendar day, not recalculated each poll:

```python
CASCADE_RESERVE  = 30.00   # always kept for cascade engine
MAIN_BUDGET_PCT  = 0.70    # fraction of day-open balance for main engine
```

- Main engine: `deployable = day_open × 0.70 - already_deployed`
- Cascade engine: may draw from balance down to `CASCADE_RESERVE` floor
- Prevents the main engine from exhausting capital before cascade entries fire

---

## Cities monitored

20 US cities with timezone-aware scheduling:

Atlanta · Austin · Boston · Chicago · Dallas · Denver · Houston · Las Vegas · Los Angeles · Miami · Minneapolis · New Orleans · New York · Oklahoma City · Philadelphia · Phoenix · San Antonio · San Francisco · Seattle · Washington DC

---

## Application (`app.py`)

PyQt6 desktop application with five tabs:

### Home
Live city status grid — local time, observed high, forecast high/low, observed low for all 20 cities. Updates on every poll.

### Session
Entries and exits for the current session. Stat cards: total entries, open positions, stopped out, avg score, unrealised PnL. Full position table with timestamp, market, side, qty, entry price, score, unrealised PnL, status.

### Performance
Settlement history fetched from Kalshi. Win rate, net PnL, fee totals. Tabs for All / HIGH / LOWT markets. Early exit tracking: would-have-won vs would-have-lost.

### City History
Per-city dashboard. Select any of the 20 cities to see:
- Rolling 7-day win rate chart
- Cumulative PnL chart
- Average PnL by local entry hour (bar chart)
- Summary stats: win rate, total PnL, positions, avg convergence hour, forecast bias, latest observed high
- Full position history table (Date, Bracket, Engine, Side, Entry, Exit, PnL, Contracts, Outcome)

### Log
Timestamped UTC log of every poll cycle, NWS snapshot (full 20-city table), entries, exits, and errors.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. API keys

Create a `config.json` file in the project root:

```json
{
  "kalshi_api_key_id": "your_key_id",
  "kalshi_api_private_key_path": "path/to/private_key.pem",
  "environment": "production"
}
```

### 3. Run

```bash
python app.py
```

---

## Project structure

```
weathermachine/
│
├── app.py                      # PyQt6 desktop application
├── trader.py                   # Order execution, exit logic, capital management
├── hight_decision_engine.py    # Main signal scoring engine
├── cascade_engine.py           # Market structure convergence engine
├── kalshi_scanner.py           # Kalshi market data fetcher
├── nws_fetcher.py              # NWS forecast and observation fetcher
├── bias_calculator.py          # Per-city forecast bias computation
├── cities.py                   # City registry with timezones
│
├── data/
│   ├── forecast_bias.json      # Per-city NWS bias corrections
│   ├── trade_log.json          # Entry log (one record per order)
│   └── lowt_observations.csv   # Historical market observations
│
├── requirements.txt
├── config.json                 # API keys (not in git)
└── README.md
```

---

## Key files

| File | Purpose |
|---|---|
| `trader.py` | Places orders, checks exits, manages capital snapshot |
| `hight_decision_engine.py` | Scores brackets, gates entries by price + score |
| `cascade_engine.py` | Bottom-up convergence scanner, directional signals |
| `bias_calculator.py` | Computes per-city forecast bias from observation history |
| `kalshi_scanner.py` | Fetches live bracket prices and order book depth |
| `nws_fetcher.py` | Fetches NWS forecasts and observed highs for all cities |

---

## Parameters reference

### Main engine (`hight_decision_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `NO_MIN_ENTRY_PRICE` | 0.75 | Minimum No price for entry |
| `NO_MAX_ENTRY_PRICE` | 0.92 | Maximum No price for entry |
| `MIN_SCORE` | 2 | Minimum signal score to enter |
| `MAX_CONTRACTS` | 6 | Hard cap per signal |

### Cascade engine (`cascade_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `CONV_THRESHOLD` | 0.97 | No price at which bracket is considered confirmed |
| `NO_MIN_ENTRY` | 0.60 | Minimum No price for cascade entry |
| `NO_MAX_ENTRY` | 0.90 | Maximum No price (fees kill upside above this) |
| `START_HOUR_CAP` | 15 | No new cascades at or after this local hour |
| `LATE_HOUR` | 13 | Max 1 entry if trigger fires at or after this hour |

### Exit (`trader.py`)

| Parameter | Value | Description |
|---|---|---|
| `YES_CEILING` | 0.60 | Yes price threshold for exit check |
| `EXIT_HOUR_MIN` | 15 | Exit rule only applies after this local hour |
| `FORECAST_FLOOR_GAP_MAX` | 2.0°F | Suppress exit if forecast is this far above floor |

### Capital management (`trader.py`)

| Parameter | Value | Description |
|---|---|---|
| `CASCADE_RESERVE` | $30.00 | Cash floor always kept for cascade engine |
| `MAIN_BUDGET_PCT` | 0.70 | Fraction of day-open balance for main engine |

---

## Disclaimer

This project is for personal research and live trading purposes. Prediction markets involve risk. Past win rates do not guarantee future performance.
