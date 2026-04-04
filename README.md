# The Weather Machine

An algorithmic trading system for Kalshi temperature prediction markets.

## Strategy overview

The system buys NO contracts on temperature brackets that are unlikely to resolve YES, using NWS weather forecasts and historical climate data to identify mispriced markets.

**Entry logic (NO trades):**
- Forecast must place the expected high comfortably outside the target bracket (≥4°F boundary buffer)
- NO contract must be priced between $0.02 and $0.87 (fee-adjusted floor)
- Minimum orderbook depth of 500 contracts
- Bid-ask spread ≤ $0.03
- Only trades today's markets (not tomorrow's pre-listed markets)

**Exit logic:**
- NO trades held to resolution ($1.00 payout) — natural exit
- Stop-loss: if YES price on a held NO bracket rises above $0.40, cut the position early

**Signal scoring (0–3):**
- +1 if NWS forecast high falls in the target bracket
- +1 if today's observed high has already cleared the bracket floor
- +1 if price momentum is upward in recent candles

---

## Settlement stations

Each city maps to the exact NWS ASOS station Kalshi uses for CLI report resolution:

| City | ICAO | Settlement note |
|---|---|---|
| New York | KNYC | Central Park — NOT JFK or LGA |
| Chicago | KMDW | Midway Airport — NOT O'Hare |
| Miami | KMIA | Miami International Airport |
| Austin | KAUS | Bergstrom Airport |
| Los Angeles | KLAX | LAX Airport |
| San Francisco | KSFO | SFO Airport |
| Denver | KDEN | Denver International Airport |
| Philadelphia | KPHL | Philadelphia International Airport |

---

## Key resolution facts

- Kalshi settles on the **NWS CLI (Climatological Report)**, published the morning after
- CLI uses **raw sensor values**, not the F→C→F rounded values shown in real-time feeds
- Reporting period is **midnight to midnight Local Standard Time** (ignores DST)
- Settlement typically happens **6–9 AM ET** the following morning
- NWS forecasts carry a consistent **~1°F warm bias** — corrected in the model

---

## Project structure

```
kalshi_weather/
├── app.py                 # PyQt6 desktop application (preferred entry point)
├── city_profiles.py       # Fetches and caches 30yr NOAA climate normals
├── nws_feed.py            # Live NWS observations + forecast high, LST-aware
├── kalshi_scanner.py      # Live bracket prices, orderbook depth, candlesticks
├── decision_engine.py     # Signal generation — gates, scoring, YES/NO logic
├── trader.py              # Auth, order execution, exit monitor, Kalshi sync
├── scheduler.py           # Terminal entry point — polling loop
├── reconcile.py           # Morning reconciliation (terminal use)
├── pnl_registry.py        # Local CSV reporting (deprecated — app uses Kalshi directly)
├── requirements.txt
├── .gitignore
└── data/
    ├── config.json            # API credentials (created by app on first launch)
    ├── city_profiles.json     # Cached NOAA normals (auto-generated if missing)
    ├── nws_grid_cache.json    # Cached NWS forecast grid endpoints per city
    ├── positions.json         # Local position log (supplementary — Kalshi is source of truth)
    └── trade_log.json         # Local trade history (supplementary)
```

---

## Setup

```bash
pip install -r requirements.txt

cp .env.template .env
# Edit .env — add KALSHI_KEY_ID and KALSHI_KEY_FILE path
# Keep KALSHI_DEMO=true until ready for live trading

# Fetch and cache city climate profiles (run once)
python city_profiles.py
```

---

## Daily workflow

```bash
# Morning — reconcile yesterday's settled positions
python reconcile.py

# Review performance reports
python pnl_registry.py --summary      # daily aggregated metrics
python pnl_registry.py --trades       # per-trade detail
python pnl_registry.py --score-report # performance breakdown by signal score

# Start today's trading loop
python scheduler.py

# Paper mode — signals logged but no real orders placed
python scheduler.py --paper
```

---

## Scheduler behaviour

The scheduler polls on a dynamic interval that tightens around the peak trading window:

| City local time | Interval | Reason |
|---|---|---|
| Before 10am | 15 min | Waiting for window |
| 10am–11am | 5 min | Window just opened |
| 11am–1pm | 3 min | Peak — forecasts updating |
| 1pm–2pm | 5 min | Approaching cutoff |
| After 2pm | 10 min | Exit monitoring only |

The scheduler exits automatically once all cities have passed their 3pm local activity window.

---

## Utility commands

```bash
python trader.py --balance              # check account balance
python trader.py --positions            # show open and closed positions
python trader.py --test-order           # place + immediately cancel a $0.01 test order
python decision_engine.py               # preview signals without executing
python decision_engine.py --city Miami  # single city
python pnl_registry.py                  # regenerate CSVs and print full report
python pnl_registry.py --trades         # per-trade detail only
python pnl_registry.py --summary        # daily summary only
python pnl_registry.py --score-report   # per-score performance breakdown
```

---

## Tunable parameters

All key parameters are at the top of `decision_engine.py` and `trader.py`:

| Parameter | Location | Default | Description |
|---|---|---|---|
| `TRADE_WINDOW_START` | decision_engine | 10 | Trading window open (local hour) |
| `TRADE_WINDOW_END` | decision_engine | 14 | Trading window close (local hour) |
| `BOUNDARY_BUFFER` | decision_engine | 3.0°F | Min distance from bracket edge — applies to YES and NO trades |
| `NO_MAX_ENTRY_PRICE` | decision_engine | $0.87 | Max price to pay for NO contract |
| `NO_MIN_YES_PRICE` | decision_engine | $0.02 | Skip if YES is basically zero (bracket already dead) |
| `NO_MAX_YES_PRICE` | decision_engine | $0.25 | Skip if YES is above this — prevents entering near stop-loss boundary |
| `MAX_NO_PER_CITY` | decision_engine | 2 | Max NO positions per city per day |
| `MAX_SPREAD` | decision_engine | $0.05 | Max bid-ask spread |
| `MIN_DEPTH` | decision_engine | 500 | Min orderbook depth |
| `FORECAST_BIAS_CORRECTION` | decision_engine | -1.0°F | NWS warm bias correction |
| `NO_STOP_LOSS_RISE` | trader | $0.15 | Exit NO if YES rises more than this above entry YES price |
| `BASE_CONTRACTS` | trader | 1 | Contracts per signal |

---

## Parameter history

| Date | Parameter | Old | New | Reason |
|---|---|---|---|---|
| 2026-03-31 | `BOUNDARY_BUFFER` | 2.0°F | 4.0°F | NYC 76–77° bracket loss — forecast sat at bracket edge |
| 2026-04-01 | `BOUNDARY_BUFFER` | 4.0°F | 3.0°F | 4°F too strict — blocks ~67% of bracket range on 2°F brackets |
| 2026-04-01 | `NO_MAX_ENTRY_PRICE` | $0.90 | $0.87 | Reconcile showed positions above $0.93 were fee-neutral |
| 2026-04-01 | `MAX_SPREAD` | $0.03 | $0.05 | Too strict — blocking valid signals on Philadelphia and others |
| 2026-04-01 | `BOUNDARY_BUFFER` | YES only | YES + NO | NO trades adjacent to forecast were entering dangerously close brackets |
| 2026-04-01 | `NO_MAX_YES_PRICE` | $0.20 | removed | Replaced by distance-based boundary buffer on NO trades |
| 2026-04-01 | `NO_MAX_YES_PRICE` | removed | $0.25 | Re-introduced — boundary buffer alone insufficient; prevents entering near stop-loss |
| 2026-04-02 | `NO_STOP_LOSS_YES_THRESHOLD` | $0.40 (absolute) | `NO_STOP_LOSS_RISE` $0.15 (relative) | Absolute threshold had logical gap — relative rise from entry is more meaningful |

---

## Notes on Kalshi API

- Market data endpoints are **public** — no auth needed for prices, orderbook, candlesticks
- Use `portfolio/settlements` for reconciliation — not individual market status fields
- Market status values are: `initialized`, `inactive`, `active`, `closed`, `determined`, `disputed`, `amended`, `finalized` — there is no `settled` status at the market level
- Prices are in dollars ($0.01–$0.99), not cents
- Order body requires only `yes_price` — do not send both `yes_price` and `no_price`
- Date filter: Kalshi pre-lists tomorrow's markets while today's are still active — always filter by today's date suffix in the event ticker
- Kalshi returns `"canceled"` (one L) not `"cancelled"` in order status responses
