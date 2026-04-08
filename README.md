# README.md — Parameter table patch
#
# Replace the existing "Tunable parameters" table with the one below.
# Changes from the old table:
#   - TRADE_WINDOW_START/END: was 10/14, now reflects actual code (0/24, gate disabled)
#   - NO_MAX_ENTRY_PRICE: was $0.87, now $0.92 (matches code)
#   - NO_STOP_LOSS_RISE → split into correct parameter names (YES_STOP_LOSS, NO_EXIT_TARGET)
#   - Added: NO_MIN_ENTRY_PRICE, MAX_CONTRACTS, NO_BAN_ABOVE_BRACKETS, FORECAST_WELL_CLEAR,
#            YES_EXIT_TARGET
#   - BASE_CONTRACTS: kept as-is (trader.py, not verified in this pass)

## Tunable parameters

All key parameters are at the top of `decision_engine.py` and `trader.py`:

| Parameter | Location | Default | Description |
|---|---|---|---|
| `TRADE_WINDOW_START` | decision_engine | 0 | Trading window open (local hour) — currently disabled (0 = 24/7) |
| `TRADE_WINDOW_END` | decision_engine | 24 | Trading window close (local hour) — currently disabled (24 = 24/7) |
| `BOUNDARY_BUFFER` | decision_engine | 3.0°F | Min distance from bracket edge — applies to YES and NO trades |
| `NO_MIN_YES_PRICE` | decision_engine | $0.02 | Skip if YES is basically zero (bracket already dead) |
| `NO_MAX_YES_PRICE` | decision_engine | $0.25 | Skip if YES is above this — prevents entering near stop-loss boundary |
| `NO_MIN_ENTRY_PRICE` | decision_engine | $0.75 | Never pay less than this for a NO contract — below here the market prices in real uncertainty |
| `NO_MAX_ENTRY_PRICE` | decision_engine | $0.92 | Never pay more than this for a NO contract — 0.75–0.92 gives 86.5% WR across 37 trades |
| `MAX_NO_PER_CITY` | decision_engine | 2 | Max NO positions per city per day |
| `NO_BAN_ABOVE_BRACKETS` | decision_engine | True | Never trade NO on "above X°" (T-bracket) HIGH markets — asymmetric upside risk in spring/summer |
| `MAX_CONTRACTS` | decision_engine | 2 | Hard cap on contracts per position — 3-contract losses average -$1.74 each |
| `MAX_SPREAD` | decision_engine | $0.05 | Max bid-ask spread |
| `MIN_DEPTH` | decision_engine | 500 | Min orderbook depth |
| `FORECAST_BIAS_CORRECTION` | decision_engine | -1.0°F | NWS warm bias correction applied to forecast high |
| `FORECAST_WELL_CLEAR` | decision_engine | 6.0°F | Bracket must be this far from forecast to score the forecast point (higher bar than BOUNDARY_BUFFER gate) |
| `YES_EXIT_TARGET` | decision_engine | 0.25 | Take profit on YES when price rises 25% |
| `YES_STOP_LOSS` | decision_engine | 0.40 | Stop loss on YES if price falls 40% from entry |
| `NO_EXIT_TARGET` | decision_engine | 0.15 | Take profit on NO when price rises 15% |
| `BASE_CONTRACTS` | trader | 1 | Contracts per signal |
