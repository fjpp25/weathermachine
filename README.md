# The Weather Machine

A live trading system for Kalshi daily temperature markets. Monitors HIGH and LOWT temperature markets across 20 US cities, enters No positions on brackets the forecast and market structure make unlikely to resolve Yes, and holds all positions to settlement.

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
│  5. RATCHET engine  →  steady No-price climb signals (LOWT)    │
│  6. Portfolio sync  →  update session state                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Strategy

### Core thesis
Temperature markets follow a roughly normal distribution centred on the NWS forecast. Brackets far from the forecast mean have a high probability of settling No. The system bets No on those brackets and holds to settlement — analysis of 583 positions showed that exiting early was net negative at every threshold tested.

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

### RATCHET engine (`cascade_engine.py`)
A LOWT-only signal that fires when a bracket's No price climbs non-decreasingly for 5 or more consecutive polls and reaches a floor of No ≥ 0.88.

**Signal logic:** Maintains a per-ticker streak counter across polls. When the streak and price floor conditions are both met, enter No at 3 contracts. Streak resets on any downward tick. Tickers are date-scoped so state expires naturally overnight.

**Entry conditions:**
- B brackets only (LOWT markets)
- Evening window: 18:00–21:00 local (overnight low pricing in)
- Streak ≥ 5 consecutive non-decreasing polls
- No price in **[0.88, 0.92]** at trigger moment

**Backtest (26 days, observations data):** 3,606 qualifying cases in the evening window, **99.2% WR**.

### Exit logic
**Positions are held to settlement.** Analysis of 583 live positions showed the exit system was net −$93 vs holding — equivalent to −$0.65 per exit attempt at every threshold tested (0.60 through 0.95).

The only active safeguard is the **YES_MARKET_CLOSED** guard: if Yes reaches 0.97+, the market has no No-side liquidity and sell orders are skipped to avoid spamming unfillable orders.

The **settlement hold override** remains active as a logging mechanism: if it is past 15:00 local and the observed value is 5°F clear of the bracket boundary, any remaining exit check is suppressed.

---

## Capital management

Session-scoped per-engine budget system — each engine has its own daily tracker that persists across poll cycles and resets at midnight:

```python
ENGINE_ALLOCATIONS = {
    "main":     0.30,   # 30% of day-open balance
    "cascade":  0.35,   # 35% — highest proven EV
    "topup":    0.15,   # 15% — augments existing positions
    "peak":     0.08,   # 8%  — conservative newer signal
    "tomorrow": 0.12,   # 12% — overnight pre-market entries
}
```

- **Main engine:** `deployable = day_open × 0.30 − already_deployed_today`
- **Cascade engine:** `deployable = day_open × 0.35 − already_cascade_deployed_today`
- **Peak / tomorrow:** same pattern with their own session trackers
- Budget is fixed at day-open and is not affected by intraday winning settlements
- HIGH and LOWT positions tracked separately — each type has its own per-city cap

---

## Cities monitored

20 US cities with timezone-aware scheduling:

Atlanta · Austin · Boston · Chicago · Dallas · Denver · Houston · Las Vegas · Los Angeles · Miami · Minneapolis · New Orleans · New York · Oklahoma City · Philadelphia · Phoenix · San Antonio · San Francisco · Seattle · Washington DC

**HIGH markets active:** 15 cities. Austin, Los Angeles, Miami, Phoenix, and San Antonio are paused — data analysis showed near-zero EV on HIGH markets for these cities due to higher temperature variance.

**LOWT markets active:** 18 cities (San Francisco and Philadelphia excluded — poor historical WR).

---

## Application (`app.py`)

PyQt6 desktop application with five tabs:

### Home
Live city status grid — local time, observed high/low, forecast high/low for all 20 cities. Updates every 5 minutes via NWS.

### Session
Entries for the current session. Stat cards: total entries, open positions, avg score, unrealised PnL. Full position table with timestamp, market, engine (MAIN/CASCADE/RATCHET), side, qty, entry price, score, unrealised PnL, status. Entry time is shown as `HH:MM UTC`; pre-session positions recovered from `trade_log.json`.

### Performance
Settlement history fetched from Kalshi. Win rate, net PnL, fee totals, best/worst day. By-Day table (double-click any row for trade detail). All Settlements table. Rolling 7-day win rate chart and equity curve.

### City History
Per-city dashboard. Select any of the 20 cities to see rolling 7-day win rate, cumulative PnL, PnL by local entry hour, summary stats (win rate, total PnL, positions, avg convergence hour, forecast bias, latest observed high), and full position history.

### Log
Timestamped UTC log of every poll cycle, NWS snapshots, entries, and errors.

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
├── trader.py                   # Order execution, capital management
├── hight_decision_engine.py    # Main HIGH signal scoring engine
├── lowt_decision_engine.py     # LOWT signal engine (18 cities)
├── cascade_engine.py           # Cascade + RATCHET signal engines (HIGH + LOWT)
│
├── kalshi_scanner.py           # Kalshi market data fetcher
├── nws_feed.py                 # NWS forecast and observation fetcher
├── cities.py                   # City registry with timezones and series tickers
│
├── tomorrow_scanner.py         # Next-day T-bracket pre-market entries
├── peak_scanner.py             # Intraday peak confirmation entries
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
│   ├── lowt_observations.csv   # Historical market observations (HIGH + LOWT)
│   └── config.json             # API credentials (not in git)
│
├── requirements.txt
└── README.md
```

---

## Key files

| File | Purpose |
|---|---|
| `trader.py` | Places orders, manages session capital budgets, settlement hold |
| `hight_decision_engine.py` | Scores HIGH brackets, gates entries by price + score |
| `lowt_decision_engine.py` | Scores LOWT brackets, obs-driven elimination signals |
| `cascade_engine.py` | Cascade (HIGH + LOWT) and RATCHET signal engines |
| `kalshi_scanner.py` | Fetches live bracket prices and order book depth |
| `nws_feed.py` | Fetches NWS forecasts and observed highs/lows for all cities |
| `bias_calculator.py` | Computes per-city NWS forecast bias from observation history |
| `tomorrow_scanner.py` | Monitors next-day T-brackets for pre-market No entries |
| `peak_scanner.py` | Confirms intraday daily high, enters No on unreachable brackets |

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
| `MAX_NO_PER_CITY` | 2 | Max open LOWT positions per city |
| `MAX_CONTRACTS` | 2 | Hard cap per LOWT position |
| `MIN_MARKET_DIST` | 2 | Minimum bracket distance from market forecast |

### Cascade engine (`cascade_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `CONV_THRESHOLD` | 0.97 | No price at which a bracket is considered confirmed |
| `NO_MIN_ENTRY` | 0.60 | Minimum No price for cascade entry |
| `NO_MAX_ENTRY` | 0.90 | Maximum No price for HIGH cascade |
| `NO_MAX_ENTRY_TOPDOWN` | 0.85 | Maximum No price for top-down cascade |
| `START_HOUR_CAP` | 15 | No new cascade starts at or after this local hour |
| `LATE_HOUR` | 13 | Max 1 entry if trigger fires at or after this hour |

### RATCHET engine (`cascade_engine.py`)

| Parameter | Value | Description |
|---|---|---|
| `RATCHET_MIN_STREAK` | 5 | Consecutive non-decreasing polls required |
| `RATCHET_NO_FLOOR` | 0.88 | No price must reach this at trigger moment |
| `RATCHET_NO_MAX` | 0.92 | Upper bound — don't enter if No has moved past this |
| `RATCHET_CONTRACTS` | 3 | Flat sizing per entry |
| `RATCHET_EVENING_START` | 18 | Entry window opens (local hour) |
| `RATCHET_EVENING_END` | 21 | Entry window closes (local hour) |

### Settlement logic (`trader.py`)

| Parameter | Value | Description |
|---|---|---|
| `YES_MARKET_CLOSED` | 0.97 | Skip exit attempts above this Yes price — no liquidity |
| `SETTLEMENT_CLEAR_BUFFER` | 5.0°F | Suppress any exit check if obs is this far clear of boundary |
| `SETTLEMENT_HOLD_HOUR` | 15 | Settlement hold override applies after this local hour |

### Capital management (`trader.py`)

| Parameter | Value | Description |
|---|---|---|
| `ENGINE_ALLOCATIONS["main"]` | 0.30 | Fraction of day-open balance for main engine |
| `ENGINE_ALLOCATIONS["cascade"]` | 0.35 | Fraction for cascade engine (session-scoped) |
| `ENGINE_ALLOCATIONS["peak"]` | 0.08 | Fraction for peak scanner |
| `ENGINE_ALLOCATIONS["tomorrow"]` | 0.12 | Fraction for tomorrow scanner |
| `TOPUP_HOUR_START` | 13 | Top-up window opens (local) |
| `TOPUP_HOUR_END` | 15 | Top-up window closes (local) |
| `YES_TOPUP_MAX` | 0.30 | Max Yes price seen during window to qualify for top-up |

### Polling interval (`scheduler.py`)

| Window (local time) | Interval | Reason |
|---|---|---|
| Midnight – 11:00 | 5 min | Overnight bracket elimination, LOWT signals |
| 11:00 – 16:00 | 3 min | Peak through convergence — fastest repricing window |
| 16:00 – midnight | 5 min | Next-market scan, late convergence |

---

## Disclaimer

This project is for personal research and live trading purposes. Prediction markets involve risk. Past win rates do not guarantee future performance.
