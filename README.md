# WeatherMachine

Algorithmic trading system for Kalshi's daily temperature markets.

## Strategy

WeatherMachine trades **NO** contracts on Kalshi HIGH temperature brackets. When the NWS morning forecast places the day's high well outside a bracket's range, that bracket's NO contract trades at a discount to its true probability. The system buys NO, collects premium, and exits when the market converges or at settlement.

Key constraints calibrated from live data:
- Entry window: **$0.75–$0.92** per NO contract (86.5% win rate across 37+ trades)
- Max **2 contracts** per position
- Max **2 NO positions** per city per day
- Never trade NO on "above X°F" brackets in spring/summer (29% WR historically → banned)
- Forecast bracket itself is never traded

---

## Architecture

```
nws_feed.py          ← Live NWS observations + forecasts (api.weather.gov)
kalshi_scanner.py    ← Kalshi market prices + orderbook
        │
        ▼
decision_engine.py   ← Signal generation (NO trades only)
        │
        ▼
trader.py            ← Order execution + exit monitoring (Kalshi API)
        │
        ▼
app.py               ← PyQt6 desktop UI
scheduler.py         ← CLI alternative to app.py
```

---

## Modules

| File | Purpose |
|---|---|
| `app.py` | PyQt6 desktop UI — Home, Session, Performance, Log tabs |
| `trader.py` | Kalshi RSA-PSS auth client, order placement, exit monitor, trade log |
| `decision_engine.py` | Signal engine — gates, scoring, NO trade decisions |
| `kalshi_scanner.py` | Fetches brackets, orderbook depth, candlestick history from Kalshi |
| `nws_feed.py` | Fetches observed high/low and forecast high/low from api.weather.gov |
| `cities.py` | Single source of truth for all city metadata (station, ICAO, timezone, LST offset, per-city trade start hours) |
| `city_profiles.py` | Fetches and caches 30-year NOAA climate normals per station |
| `bias_calculator.py` | Computes per-city NWS forecast bias from observation history |
| `scheduler.py` | CLI trading loop with dynamic polling interval (alternative to app.py) |
| `lowt_observer.py` | Passive observer — records bracket prices + NWS data every 15 min |
| `lowt_analyzer.py` | Analyzes lowt_observations.json for trade signal review |
| `reconcile.py` | CLI diagnostic — prints recent Kalshi settlement history |

---

## Data files

| Path | Contents |
|---|---|
| `data/config.json` | API key ID, PEM path, live/demo mode (written by app.py settings dialog) |
| `data/city_profiles.json` | Cached NOAA monthly normals (tmax, tmin, stddev) per city |
| `data/forecast_bias.json` | Per-city NWS forecast bias in °F — `{"New York": 1.89, ...}` |
| `data/trade_log.json` | Every placed order with ticker, score, entry price, timestamp |
| `data/nws_grid_cache.json` | NWS gridpoint URLs per city (fetched once, cached permanently) |
| `data/lowt_observations.json` | Rolling observation history (forecast_f, observed_f, bracket prices) |
| `data/entry_snapshots.csv` | CSV entry log for external analysis |

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Get Kalshi API credentials

Generate an RSA key pair in your [Kalshi dashboard](https://kalshi.com/account/keys).
You need the **Key ID** and the **private key PEM file**.

### 3. Build city profiles

Fetches 30-year NOAA climate normals for each trading station. Cached to `data/city_profiles.json` — only needs to run once.

```bash
python city_profiles.py
```

### 4. Compute forecast bias

Requires at least a few days of `lowt_observations.json` data (run `lowt_observer.py` to collect it). Computes per-city NWS warm/cool bias using a trimmed mean.

```bash
python lowt_observer.py     # collect observations (run daily)
python bias_calculator.py   # compute bias → data/forecast_bias.json
```

### 5. Launch

```bash
python app.py
```

On first launch, a setup dialog will prompt for your Key ID and PEM file path. Credentials are saved to `data/config.json`.

---

## App tabs

**Home** — Start/stop the scheduler, monitor city status cards, open positions table (with unrealised PnL), balance and portfolio value.

**Session** — Live view of trades placed in the current run. Shows entry time, market, side, quantity, entry price, signal score, unrealised PnL, and status. Refresh button re-syncs statuses (Won/Lost/Settled) against Kalshi.

**Performance** — Settlement history from Kalshi. By Day table with equity curve, All Settlements table with score column (joined from `trade_log.json`).

**Log** — Raw scheduler output.

---

## Tunable parameters

### `decision_engine.py`

| Parameter | Default | Description |
|---|---|---|
| `TRADE_WINDOW_START` | 0 | Trading window open (local hour) — currently 24/7, timing handled by scheduler |
| `TRADE_WINDOW_END` | 24 | Trading window close — currently disabled |
| `MAX_SPREAD` | $0.05 | Max acceptable bid-ask spread |
| `MIN_DEPTH` | 500 | Min orderbook depth on the side being bought |
| `BOUNDARY_BUFFER_STDDEV_FACTOR` | 0.6 | Scales boundary buffer with city's monthly tmax stddev |
| `BOUNDARY_BUFFER_MIN` | 2.0°F | Floor on dynamic boundary buffer |
| `BOUNDARY_BUFFER_MAX` | 5.0°F | Ceiling on dynamic boundary buffer |
| `BOUNDARY_BUFFER_FALLBACK` | 3.0°F | Used when city profile stddev is unavailable |
| `FORECAST_BIAS_CORRECTION` | -1.0°F | Global fallback bias — overridden per city by `data/forecast_bias.json` |
| `FORECAST_WELL_CLEAR` | 6.0°F | Bracket must be this far from corrected forecast to score the forecast point |
| `NO_MIN_YES_PRICE` | $0.02 | Skip if YES is essentially zero (bracket already dead) |
| `NO_MAX_YES_PRICE` | $0.25 | Skip if YES is above this — too close to uncertain territory |
| `NO_MIN_ENTRY_PRICE` | $0.75 | Never pay less than this for NO — below here the market prices in real uncertainty |
| `NO_MAX_ENTRY_PRICE` | $0.92 | Never pay more than this for NO — 0.75–0.92 gives 86.5% WR |
| `MAX_NO_PER_CITY` | 2 | Max NO positions per city per day |
| `NO_BAN_ABOVE_BRACKETS` | True | Never trade NO on "above X°F" brackets — asymmetric upside risk |
| `MAX_CONTRACTS` | 2 | Hard cap on contracts per position |
| `NO_EXIT_TARGET` | 0.15 | Take profit when NO price rises 15% |

### `trader.py`

| Parameter | Default | Description |
|---|---|---|
| `BASE_CONTRACTS` | 1 | Base contracts per signal |
| `MAX_CONTRACTS_PER_ORDER` | 10 | Hard safety cap on single order size |
| `NO_STOP_LOSS_RISE` | 0.15 | Exit NO if YES rises more than this above entry YES price |
| `MONITOR_INTERVAL` | 60s | Exit monitor poll interval |

---

## Workflow

### Daily (automated via app.py or scheduler.py)

The scheduler polls on a dynamic interval that tightens around peak temperature hours:

| Local time | Interval | Reason |
|---|---|---|
| Midnight–9am | 10 min | Bracket elimination from overnight observations |
| 9am–11am | 5 min | Morning forecast stabilising |
| 11am–1pm | 3 min | Peak — NWS model runs, market reprices fastest |
| 1pm–3pm | 5 min | Post-peak convergence |
| 3pm–midnight | 10 min | Exit monitoring |

### Periodic maintenance

```bash
# Refresh forecast bias (weekly or after 5+ new observation days)
python bias_calculator.py

# Refresh city profiles (annually or after NOAA normal updates)
python city_profiles.py --refresh

# Quick settlement check from terminal
python reconcile.py
```

---

## Forecast bias

`bias_calculator.py` reads `data/lowt_observations.json`, extracts the morning NWS forecast (local hours 9–11) and the late-afternoon observed high (15:00+) for each city-day, and computes a **trimmed mean** error (dropping the single highest and lowest per city when n ≥ 5).

The resulting `data/forecast_bias.json` is loaded by `decision_engine.py` at startup. Cities with no bias data fall back to `FORECAST_BIAS_CORRECTION = -1.0°F`.

Current bias direction (from ~5–6 days of data per city):
- **Positive** (NWS runs warm): New York +1.89°F, Denver +1.87°F, Dallas +1.42°F
- **Near zero**: Atlanta -0.06°F, San Francisco 0.00°F  
- **Negative** (NWS runs cool): Austin -1.83°F, San Antonio -1.84°F

---

## Score tracking

Every placed order is logged to `data/trade_log.json` with:

```json
{
  "ticker": "KXHIGHNY-26APR13-T72",
  "city": "New York",
  "side": "no",
  "score": 3,
  "score_detail": ["obs_eliminates_bracket", "forecast_well_clear", "momentum_flat_or_down"],
  "entry_price": 0.83,
  "contracts": 2,
  "placed_at": "2026-04-13T14:32:11+00:00",
  "paper": false
}
```

The Performance tab joins this against Kalshi settlements by ticker to show per-trade scores. Once enough data accumulates (~20 settled trades per score level), the win-rate breakdown by score will inform whether position sizing should scale with signal strength.

---

## Safety

- `KALSHI_DEMO` defaults to `true` — the app operates in demo mode unless live mode is explicitly enabled in Settings
- The 70% deployable cap (`balance × 0.70`) limits total exposure
- `MAX_CONTRACTS = 2` is a hard cap regardless of signal score
- Paper mode (`--paper` flag or app.py paper toggle) logs trades without placing real orders
