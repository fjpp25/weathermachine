# The Weather Machine

A live algorithmic trading system for Kalshi daily temperature prediction markets. Monitors HIGH and LOWT temperature markets across 20 US cities, enters No positions on brackets the forecast and observed temperature make unlikely to resolve Yes, and holds all positions to settlement.

Running 24/7 on a Raspberry Pi 5. Public dashboard at [theweathermachine.org](https://theweathermachine.org).

---

## How it works

```
┌──────────────────────────────────────────────────────────────────────┐
│  Continuous polling loop (5 min default, 3 min 11am–1pm local)      │
│                                                                      │
│  1. NWS + AccuWeather fetch  →  forecasts, observed highs/lows      │
│  2. Kalshi scan (HIGH + LOWT) →  bracket prices, order book depth   │
│  3. run_pipeline()            →  main + cascade + LOWT engines      │
│  4. sweep_engine.run_scan()   →  pre-market + sweep + dead bracket  │
│  5. peak_scanner.run_scan()   →  intraday peak confirmation         │
│  6. last_bracket.run_scan()   →  late-day 2-bracket setups          │
│  7. evening_convergence       →  3-bracket evening signals          │
│  8. hourly_nyc_engine         →  NYC hourly temperature markets     │
│  9. lowt_decision_engine      →  structural + forecast LOWT signals │
│  10. exit check + order mgmt  →  manage resting orders              │
└──────────────────────────────────────────────────────────────────────┘
```

All positions are held to settlement. Exit analysis across 583 live positions showed that early exits cost $93 net — equivalent to −$0.65 per exit attempt at every threshold tested.

---

## Strategy

### Core thesis

Temperature prediction markets price brackets around the NWS/AccuWeather forecast. Brackets far from the forecast, or already physically eliminated by observed temperature, have a near-certain probability of settling No. The system bets No on those brackets and collects the premium at settlement.

The edge has two sources:

1. **Physical constraints** — if the observed high has already passed a bracket, or the observed low is already above a bracket cap, the outcome is structurally certain regardless of market price.
2. **Forecast alignment** — brackets far from the corrected forecast (≥2°F from nearest edge) are priced with residual uncertainty the market hasn't fully resolved.

### Forecast sources

**AccuWeather** (primary): updated every 2 hours, city-level forecast high/low. Used as the primary forecast input for the main engine and LOWT engine.

**NWS** (secondary): used as a sanity check. When AccuWeather and NWS diverge by ≥3°F, AccuWeather is used. NWS provides observed high/low temperatures for all cities.

A per-city **forecast bias correction** is applied to NWS data before use:
```
corrected_forecast = nws_forecast - city_bias
```
Bias values are stored in `data/forecast_bias.json` and updated from historical observations.

---

## Engines

### Main engine (`hight_decision_engine.py`)

Evaluates HIGH brackets using AccuWeather as primary forecast with NWS as sanity check. Scores each bracket across several components including observed temperature vs bracket position, forecast distance, order book quality, and spread. Minimum score threshold required for entry.

Entry gate: No price in **[0.85, 0.94]**. Per-city entry window: **09:00–24:00 local**.

### Cascade engine (`cascade_engine.py`)

Detects market structure convergence signals — no NWS data required.

**Mechanism:** As temperatures rise through the day, lower B brackets confirm No sequentially. When enough brackets on one side confirm (high-side or low-side dominant), the next unconfirmed bracket becomes a high-conviction entry.

**Entry conditions:**
- B brackets only (afternoon cascade)
- No price in **[0.60, 0.94]** (conv-scaled sizing)
- High-side or low-side dominance confirmed

Also handles overnight distance signals (OVN_DIST): N+2 or further bracket from NWS forecast, entered pre-market. Backtest: 99.2% WR.

### Sweep engine (`sweep_engine.py`)

Unified pre-market and sweep signal engine. Combines three signal paths:

**Signal A — Directional pre-market:** Fires on next-day markets when T-bracket No prices diverge strongly (one T ≥ 0.80 No, opposing T < 0.60 No). Enters the trigger T bracket and adjacent B bracket. 3 contracts each.

**Signal B — Near-dead sweep:** Any open HIGH market. No price in [city_floor, 0.97). Skips the forecast bracket. Per-city floors calibrated from backtest. 3 contracts. Cities with no clean floor excluded.

**Signal C — Dead bracket:** Any open HIGH market. No price in [0.97, 0.989]. Bracket must be rank-4 or rank-5 by NWS forecast distance. City × month safety filter applied. Backtest: 1,080 signals, 99.2% WR. 5 contracts.

All three paths share one session dedup set and one capital budget.

### LOWT engine (`lowt_decision_engine.py`)

Two signal paths for LOW temperature markets:

**Signal A — Structural elimination:** The observed low for the day is already **above the bracket cap** — the temperature has not reached that bracket and is warming. Physically impossible to resolve Yes. Any hour, any city (except past `trade_end_lowt` deadline). No in [0.75, 0.97). Backtest: 1,914 signals, 99.9% WR.

**Signal B — Forecast distance:** Evening window only (18–23h local). B brackets where the NWS forecast low is ≥3°F above the bracket floor — the overnight temperature is not expected to reach the bracket. No in [0.75, 0.92). Skip Philadelphia and Chicago. 2 contracts.

### Peak scanner (`peak_scanner.py`)

Fires when the observed high reaches a bracket floor before the city's P90 peak hour. Temperature is still rising and will almost certainly break through. No in [0.85, 0.92], 3 contracts. 10 cities with predictable peak timing.

Backtest (Apr–May 2026): 179 signals, 92.2% WR.

### Last bracket (`last_bracket.py`)

Fires when exactly 2 brackets remain open and the upper bracket has No ≥ 0.80, provided the 2-bracket phase first appeared at or after 16h local (fresh signal only). Backtest: 126 signals, 95.2% WR.

### Evening convergence (`evening_convergence.py`)

Fires when exactly 3 brackets remain active after 19h local and a non-forecast B bracket has No ≥ 0.85. With 3 brackets in the evening, the market has already converged around one scenario and flanking brackets are structurally safe. Backtest: 98.7% No-win on qualifying signals.

### Hourly NYC engine (`hourly_nyc_engine.py`)

Trades Kalshi's NYC hourly temperature markets (KXTEMPNYCH series). Signal: AccuWeather hourly forecast says the temperature will **not** reach the threshold (`forecast_resolves_yes = False`). Kalshi settles on AccuWeather current conditions — when the same data source says No, the bracket is structurally dead. No in [0.75, 0.95). 3 contracts.

Backtest (34 days): 311 qualifying signals, 99.4% WR.

---

## Capital management

Session-scoped per-engine budget system. Each engine has an independent daily allocation anchored to the **day-open balance** — not affected by intraday position changes or wins.

```python
ENGINE_ALLOCATIONS = {
    "main":     0.10,   # HIGH main engine
    "cascade":  0.25,   # HIGH cascade — highest proven EV
    "sweep":    0.20,   # Unified sweep: directional + near-dead + dead bracket
    "peak":     0.08,   # Intraday peak confirmation
    "topup":    0.03,   # Augments existing positions (rarely fires)
    "econv":    0.03,   # Evening convergence (rarely fires)
    "hourly":   0.03,   # NYC hourly temperature
    "lowt":     0.12,   # LOWT structural elimination + forecast distance
    # 16% unallocated buffer
}
```

All budgets are anchored to day-open balance and reset at midnight. No engine can borrow from another's allocation.

**Global position cap:** `GLOBAL_MAX_CONTRACTS_PER_TICKER = 7` — hard ceiling across all engines on any single bracket ticker, regardless of how many engines fire on it.

---

## Cities

20 US cities with timezone-aware scheduling:

Atlanta · Austin · Boston · Chicago · Dallas · Denver · Houston · Las Vegas · Los Angeles · Miami · Minneapolis · New Orleans · New York · Oklahoma City · Philadelphia · Phoenix · San Antonio · San Francisco · Seattle · Washington DC

**HIGH markets active:** 15 cities. Austin, Los Angeles, Miami, Phoenix, and San Antonio are paused — insufficient edge from historical data.

**LOWT markets active:** 18 cities. San Francisco and Philadelphia excluded.

---

## Infrastructure

- **Hardware:** Raspberry Pi 5 8GB, NVMe SSD, Argon ONE V3 case
- **OS:** Ubuntu 24, systemd services
- **Public access:** theweathermachine.org via Cloudflare
- **Repo:** github.com/fjpp25/weathermachine (public, master branch)
- **Entity:** Wyoming LLC

### Services

```bash
sudo systemctl restart weathermachine    # main scheduler
sudo systemctl restart dashboard         # Flask web dashboard
sudo systemctl restart lowt-observer     # LOWT market observer
sudo systemctl restart hourly-nyc-observer  # NYC hourly observer
```

### Deploy workflow

```bash
# Edit locally → push to GitHub → pull on Pi → restart
cd ~/weathermachine && git pull
sudo systemctl restart weathermachine dashboard
```

---

## Project structure

```
weathermachine/
│
├── scheduler.py                # Main polling loop — coordinates all engines
├── trader.py                   # Order execution, capital management, EngineCapital
│
├── hight_decision_engine.py    # HIGH bracket scoring engine (AccuWeather-primary)
├── cascade_engine.py           # Cascade + overnight distance signals
├── sweep_engine.py             # Unified sweep: directional, near-dead, dead bracket
├── lowt_decision_engine.py     # LOWT: structural elimination + forecast distance
├── peak_scanner.py             # Intraday peak confirmation entries
├── last_bracket.py             # Late-day 2-bracket signal
├── evening_convergence.py      # 3-bracket evening convergence signal
├── hourly_nyc_engine.py        # NYC hourly temperature market engine
│
├── kalshi_scanner.py           # Kalshi market data fetcher
├── nws_feed.py                 # NWS forecast + observed temp fetcher
├── accuweather_feed.py         # AccuWeather forecast fetcher (primary)
├── accuweather_logger.py       # Background AW forecast logger (for future analysis)
│
├── cities.py                   # City registry: timezones, series tickers, trade windows
├── market_utils.py             # Shared helpers: local_hour, no_price, bracket_val, etc.
├── log_setup.py                # Shared UTC logging configuration
│
├── city_profiles.py            # NOAA 30-year climate normals fetcher (8 cities)
├── dashboard.py                # Flask web dashboard (theweathermachine.org)
├── app.py                      # PyQt6 desktop GUI (less active)
│
├── data/
│   ├── config.json             # API credentials (not in git)
│   ├── forecast_bias.json      # Per-city NWS bias corrections
│   ├── city_profiles.json      # NOAA climate normals cache
│   ├── trade_log.json          # Entry log (one record per order)
│   ├── accuweather_locations.json   # AW location keys (permanent cache)
│   ├── accuweather_forecasts.json   # AW forecast cache (refreshed every 2h)
│   └── accuweather_forecast_log.csv # Historical AW forecast log (for skip zone analysis)
│
├── requirements.txt
└── README.md
```

---

## Key design decisions

**Hold to settlement.** Exit analysis on 583 live positions showed early exits cost $93 net at every threshold tested. The only safeguard is a YES_MARKET_CLOSED guard (threshold 0.97) to avoid placing unfillable orders when there's no No-side liquidity.

**AccuWeather over NWS.** NWS morning forecasts have a measurable per-city warm bias and update less frequently. AccuWeather is used as primary; NWS serves as sanity check with blending to midpoint on divergences ≥3°F.

**Price level over price velocity.** Extensive backtest confirmed that No price jump signals (velocity) have no independent predictive value over the absolute price level. A 5% jump filter reduces WR by 6.2pp and EV by $0.034/contract vs price gate alone.

**Structural signals over statistical signals.** The strongest edges come from physical constraints (obs_low > bracket_cap, obs_high eliminates bracket) rather than forecast-distance statistics. 1,914 LOWT structural elimination signals showed 99.9% WR.

**Day-open anchored budgets.** Capital allocations are fixed at the start of each day, not recalculated as intraday balance changes. This prevents morning deployments from crowding out evening engines (LOWT, sweep) that fire later in the day.

**Sequencing: win rate before contract size.** Contract sizing is flat and conservative (2-5 contracts depending on engine) until win rates are confirmed stable at each capital level. Scaling is a deliberate future step, not a current goal.

---

## Parameters reference

### Main engine

| Parameter | Value | Description |
|---|---|---|
| `NO_MIN_ENTRY` | 0.85 | Minimum No price for entry |
| `NO_MAX_ENTRY` | 0.94 | Maximum No price for entry |
| `MAX_CONTRACTS` | 3 | Hard cap per position |
| `MAX_NO_PER_CITY` | 2 | Max open HIGH positions per city |
| `TRADE_WINDOW_START` | 9 | Entry window opens (local hour) |
| `TRADE_WINDOW_END` | 24 | Entry window closes (local hour) |

### Cascade engine

| Parameter | Value | Description |
|---|---|---|
| `CONV_THRESHOLD` | 0.97 | No price at which a bracket is considered confirmed |
| `NO_MIN_ENTRY` | 0.60 | Minimum No price for cascade entry |
| `NO_MAX_ENTRY` | 0.94 | Maximum No price for cascade entry |
| `OVN_N_MIN` | 2 | Minimum bracket rank above forecast for overnight signal |

### Sweep engine

| Parameter | Value | Description |
|---|---|---|
| `NO_TRIGGER` | 0.80 | T bracket No that fires the directional signal (A) |
| `SWEEP_CEILING` | 0.97 | Upper bound for Signal B — Signal C starts here |
| `DEAD_FLOOR` | 0.97 | Minimum No price for dead bracket signal (C) |
| `DEAD_CEILING` | 0.989 | Above this, Yes is $0.01 — fills collapse |
| `DIRECTIONAL_CONTRACTS` | 3 | Contracts per Signal A entry |
| `SWEEP_CONTRACTS` | 3 | Contracts per Signal B entry |
| `DEAD_CONTRACTS` | 5 | Contracts per Signal C entry |

### LOWT engine

| Parameter | Value | Description |
|---|---|---|
| `A_NO_MIN` | 0.75 | Min No price, Signal A |
| `A_NO_MAX` | 0.97 | Max No price, Signal A |
| `B_NO_MIN` | 0.75 | Min No price, Signal B |
| `B_NO_MAX` | 0.92 | Max No price, Signal B |
| `B_DIST_MIN` | −3.0°F | Bracket floor must be ≥ forecast + this |
| `B_EVENING_START` | 18 | Signal B entry window opens (local hour) |
| `B_EVENING_END` | 23 | Signal B entry window closes (local hour) |
| `MAX_CONTRACTS` | 2 | Flat sizing per entry |
| `MAX_NO_PER_CITY` | 2 | Max open LOWT positions per city |

### Capital management

| Parameter | Value | Description |
|---|---|---|
| `GLOBAL_MAX_CONTRACTS_PER_TICKER` | 7 | Hard ceiling across all engines per ticker |
| `TOPUP_TOTAL_CAP` | 7 | Aligned with global cap |
| `YES_MARKET_CLOSED` | 0.97 | Skip exit attempts above this Yes price |

### Polling interval

| Window (local time) | Interval | Reason |
|---|---|---|
| All other hours | 5 min | Standard polling |
| 11:00–13:00 | 3 min | Peak repricing window |

---

## Disclaimer

This project is for personal research and live trading. Prediction markets involve risk. Past win rates do not guarantee future performance.
