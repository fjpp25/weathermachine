# The Weather Machine

A live trading system for Kalshi daily temperature markets. Monitors HIGH and LOWT temperature markets across 20 US cities, enters No positions on brackets the forecast and market structure make unlikely to resolve Yes, and manages exits automatically.

---

## How it works

```
┌─────────────────────────────────────────────────────────────────┐
│  Continuous polling loop (5 min default, 3 min 11am–4pm local) │
│                                                                 │
│  1. NWS fetch       →  live forecasts + observed highs/lows    │
│  2. Kalshi scan     →  bracket prices, order book depth        │
│  3. Decision engine →  score signals, gate entries             │
│  4. Cascade engine  →  market structure convergence signals    │
│  5. Exit check      →  forecast anchor + Yes ceiling           │
│  6. Portfolio sync  →  update session state                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Strategy

### Core thesis
Temperature markets follow a roughly normal distribution centred on the NWS forecast. Brackets far from the forecast mean have a high probability of settling No. The system bets No on those brackets.

### Main engine (`hight_decision_engine.py`)
Evaluates each bracket with a score from −1 to +5 across six components:

| Component | Signal | Points |
|---|---|---|
| `obs_eliminates_bracket` | Observed high ≥ bracket cap — bracket physically impossible | +1 |
| `obs_below_floor` | Observed high well below bracket floor — bracket not yet in play | +1 |
| `obs_inside_bracket` | Observed high inside bracket range — bracket may be resolving YES | −1 |
| `forecast_well_clear` | Corrected forecast ≥ 6°F from nearest bracket edge | +1 |
| `momentum_flat_or_down` | No upward price pressure in recent candles | +1 |
| `yes_price_quality` | YES ≤ 0.12 — market strongly agrees with our thesis | +1 |

Minimum score for entry: **1**. All entries are sized at **1 contract**, with a hard cap of **3 contracts per position**.

Entry gate: No price must be in **[0.75, 0.92]**; Yes price must be in **(0.02, 0.25]**.

### Forecast bias correction
NWS morning forecasts have a measurable per-city warm bias. The system applies a correction before scoring:

```
corrected_forecast = nws_forecast - city_bias
```

`city_bias` is positive when NWS overestimates (runs warm), so subtracting it shifts the corrected forecast downward toward the actual observed temperature. Bias values are computed by `bias_calculator.py` from historical observation data and stored in `data/forecast_bias.json`.

### Cascade engine (`cascade_engine.py`)
A separate signal path based purely on market structure — no NWS data required.

**HIGH cascade mechanism:** Temperature markets converge from the bottom up as the observed high climbs. When any B bracket crosses No ≥ 0.97, the bracket immediately above it becomes the target. The first trigger locks the direction for that city-day.

**LOWT cascade mechanism:** LOWT markets converge after the overnight low is set. Bottom-up signals fire when the coldest brackets confirm No first (temperature already well above them). Top-down signals fire on unexpectedly cold nights.

**HIGH entry conditions:**
- No price in **[0.60, 0.90]**
- Skip the forecast bracket (most contested)
- No new cascades after 15:00 local
- Max 1 entry if trigger fires after 13:00 local; max 2 otherwise

**Contract sizing (by No price, conviction-scaled):**

| No price range | Contracts |
|---|---|
| 0.60 – 0.70 | 2 |
| 0.71 – 0.80 | 4 |
| 0.81 – 0.90 | 6 |

**Backtests (18 days, Apr 6–23):**
- HIGH bottom-up: 108 signals, 86.1% WR, 1 bad day
- LOWT bottom-up: 143 signals, 97.2% WR, 0 bad days
- LOWT top-down: 33 signals, 87.9% WR, 3 bad days

### Exit logic

**Yes ceiling (time-gated):** If Yes crosses 0.60 after 15:00 local, exit — *unless* the corrected forecast is more than 2°F above the bracket floor (spike is noise, hold).

**Forecast anchor:** If the observed value is within 1.5°F of the dangerous bracket boundary and the corrected forecast is ambiguous, exit regardless of hour.

**Settlement hold override:** If it is past 15:00 local and the observed value is 5°F clear of the bracket boundary, suppress all exits and hold to settlement.

No price-based stop-loss. Data showed holding almost always beats exiting on price alone.

---

## Capital management

Day-open snapshot system — budget is fixed once per calendar day:

```python
CASCADE_RESERVE  = 30.00   # always kept for cascade engine
MAIN_BUDGET_PCT  = 0.70    # fraction of day-open balance for main engine
```

- Main engine: `deployable = day_open × 0.70 - already_deployed_today`
- Cascade engine: may draw from balance down to the `CASCADE_RESERVE` floor
- Top-up orders (13:00–15:00 local, low-Yes positions) draw from the same main budget
- HIGH and LOWT positions are tracked separately — each type has its own per-city cap

`already_deployed_today` is a persistent cross-poll tracker that resets at midnight. It is not affected by intraday winning settlements, preventing the budget from incorrectly refilling when positions resolve.

---

## Cities monitored

20 US cities with timezone-aware scheduling:

Atlanta · Austin · Boston · Chicago · Dallas · Denver · Houston · Las Vegas · Los Angeles · Miami · Minneapolis · New Orleans · New York · Oklahoma City · Philadelphia · Phoenix · San Antonio · San Francisco · Seattle · Washington DC

HIGH markets active: all 20 cities (Las Vegas, Seattle, San Antonio paused pending sufficient data).
LOWT markets active: 18 cities (San Francisco and Philadelphia excluded — poor historical WR).

---

## Application (`app.py`)

PyQt6 desktop application with five tabs:

### Home
Live city status grid — local time, observed high/low, forecast high/low for all 20 cities. Updates every 5 minutes via NWS.

### Session
Entries and exits for the current session. Stat cards: total entries, open positions, stopped out, avg score, unrealised PnL. Full position table with timestamp, market, engine (MAIN/CASCADE), side, qty, entry price, score, unrealised PnL, status.

### Performance
Settlement history fetched from Kalshi. Win rate, net PnL, fee totals, best/worst day. By-Day table (double-click any row for trade detail). All Settlements table. Rolling 7-day win rate chart and equity curve.

### City History
Per-city dashboard. Select any of the 20 cities to see rolling 7-day win rate, cumulative PnL, PnL by local entry hour, summary stats (win rate, total PnL, positions, avg convergence hour, forecast bias, latest observed high), and full position history.

### Log
Timestamped UTC log of every poll cycle, NWS snapshots, entries, exits, and errors.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

On first launch, `app.py` shows a setup dialog that saves credentials to `data/config.json`. To configure manually:

```json
{
  "key_id":    "your-kalshi-key-id",
  "key_file":  "path/to/kalshi_private_key.pem",
  "live_mode": false
}
```

Set `live_mode: true` for live trading. The app prompts for confirmation before starting.

### 3. Run

```bash
python app.py        # desktop UI
python scheduler.py  # headless scheduler
```

---

## Project structure

```
weathermachine/
│
├── app.py                      # PyQt6 desktop application
├── scheduler.py                # Headless polling loop
│
├── trader.py                   # Order execution, exit logic, capital management
├── hight_decision_engine.py    # Main HIGH signal scoring engine
├── lowt_decision_engine.py     # LOWT signal engine (18 cities)
├── cascade_engine.py           # Market structure convergence engine (HIGH + LOWT)
│
├── kalshi_scanner.py           # Kalshi market data fetcher
├── nws_feed.py                 # NWS forecast and observation fetcher
├── cities.py                   # City registry with timezones and series tickers
│
├── tomorrow_scanner.py         # Next-day T-bracket scanner (observation mode)
├── peak_scanner.py             # Intraday peak confirmation scanner (observation mode)
│
├── bias_calculator.py          # Per-city NWS forecast bias computation
├── city_profiles.py            # NOAA 30-year climate normals fetcher
├── update_city_profiles.py     # Blends observed data into city profiles
├── score_analysis.py           # Win rate / EV analysis by score component
├── entry_window_analysis.py    # Optimal entry window analysis from observations
├── log_setup.py                # Shared logging configuration
│
├── data/
│   ├── forecast_bias.json      # Per-city NWS bias corrections
│   ├── city_profiles.json      # NOAA climate normals (30-year)
│   ├── trade_log.json          # Entry log (one record per order)
│   ├── lowt_observations.csv   # Historical market observations
│   └── config.json             # API credentials (not in git)
│
├── requirements.txt
└── README.md
```

---

## Key files

| File | Purpose |
|---|---|
| `trader.py` | Places orders, checks exits, manages capital snapshot |
| `hight_decision_engine.py` | Scores HIGH brackets, gates entries by price + score |
| `lowt_decision_engine.py` | Scores LOWT brackets, obs-driven elimination signals |
| `cascade_engine.py` | Bottom-up/top-down convergence scanner for HIGH and LOWT |
| `kalshi_scanner.py` | Fetches live bracket prices and order book depth |
| `nws_feed.py` | Fetches NWS forecasts and observed highs/lows for all cities |
| `bias_calculator.py` | Computes per-city NWS forecast bias from observation history |
| `tomorrow_scanner.py` | Monitors next-day T-brackets for pre-market signals (observation) |
| `peak_scanner.py` | Confirms intraday daily high, enters No on unreachable brackets (observation) |

---

## Parameters reference

### Main engine (`hight_decision_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `NO_MIN_ENTRY_PRICE` | 0.75 | Minimum No price for entry |
| `NO_MAX_ENTRY_PRICE` | 0.92 | Maximum No price for entry |
| `MIN_SCORE` | 1 | Minimum signal score to enter |
| `MAX_CONTRACTS` | 3 | Hard cap per position |
| `MAX_NO_PER_CITY` | 2 | Max open HIGH positions per city |
| `FORECAST_WELL_CLEAR` | 6.0°F | Distance required for forecast_well_clear point |
| `YES_HIGH_CONFIDENCE` | 0.12 | YES threshold for yes_price_quality point |

### LOWT engine (`lowt_decision_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `OBS_ELIM_MARGIN` | 15.0°F | Temp must be this far above cap for obs_eliminates_bracket |
| `FORECAST_LOW_MARGIN` | 6.0°F | Tonight's forecast low must be this far above cap |
| `MAX_NO_PER_CITY` | 2 | Max open LOWT positions per city |
| `MAX_CONTRACTS` | 2 | Hard cap per LOWT position |

### Cascade engine (`cascade_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `CONV_THRESHOLD` | 0.97 | No price at which a bracket is considered confirmed |
| `NO_MIN_ENTRY` | 0.60 | Minimum No price for cascade entry |
| `NO_MAX_ENTRY` | 0.90 | Maximum No price for HIGH cascade |
| `NO_MAX_ENTRY_TOPDOWN` | 0.85 | Maximum No price for top-down cascade |
| `START_HOUR_CAP` | 15 | No new cascade starts at or after this local hour |
| `LATE_HOUR` | 13 | Max 1 entry if trigger fires at or after this hour |

### Exit (`trader.py`)

| Parameter | Value | Description |
|---|---|---|
| `NO_YES_CEILING` | 0.60 | Yes price threshold that triggers exit check |
| `NO_YES_CEILING_HOUR` | 15 | Yes ceiling only fires at or after this local hour |
| `FORECAST_FLOOR_GAP_MAX` | 2.0°F | Suppress Yes ceiling exit if forecast is this far above floor |
| `FORECAST_ANCHOR_BUFFER` | 1.5°F | Fire anchor exit when obs is within this of the bracket boundary |
| `SETTLEMENT_CLEAR_BUFFER` | 5.0°F | Hold to settlement if obs is this far clear of the boundary |
| `SETTLEMENT_HOLD_HOUR` | 15 | Settlement hold override only applies after this hour |

### Polling interval (`scheduler.py`)

| Window (local time) | Interval | Reason |
|---|---|---|
| Midnight – 11:00 | 5 min | Overnight bracket elimination, LOWT signals |
| 11:00 – 16:00 | 3 min | Peak through convergence — fastest repricing window |
| 16:00 – midnight | 5 min | Next-market scan, late convergence |

### Capital management (`trader.py`)

| Parameter | Value | Description |
|---|---|---|
| `CASCADE_RESERVE` | $30.00 | Cash floor always kept for cascade engine |
| `MAIN_BUDGET_PCT` | 0.70 | Fraction of day-open balance for main engine |
| `TOPUP_HOUR_START` | 13 | Top-up window opens (local) |
| `TOPUP_HOUR_END` | 15 | Top-up window closes (local) |
| `YES_TOPUP_MAX` | 0.30 | Max Yes price seen during window to qualify for top-up |

---

## Disclaimer

This project is for personal research and live trading purposes. Prediction markets involve risk. Past win rates do not guarantee future performance.
