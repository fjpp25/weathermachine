#!/usr/bin/env python3
"""
dc_autopsy.py — diagnose WHY a city/engine loses, to pick the right fix.

Run on the Pi after city_performance.py has written resolutions.json:
    python3 dc_autopsy.py trade_log.json resolutions.json
    python3 dc_autopsy.py trade_log.json resolutions.json --city "Washington DC" --tier main

For the selected city/tier it separates wins from losses and asks two questions:
  1. PRICE: are losses concentrated at high entry No-prices, where one loss
     erases many wins? (points to an entry-gate fix)
  2. FORECAST: how far past the bracket did settlement land — narrow misses
     (bad luck / tight gate) vs blowouts (forecast or bias wrong)?

It also reports the breakeven WR implied by the actual entry prices: at mean
entry price p, you need WR > p just to break even. If realized WR is below
that, the engine is structurally underwater there regardless of "noise".
"""
import argparse, json, re, statistics
from collections import Counter


def load(p):
    with open(p) as f:
        return json.load(f)


def parse_bracket(ticker):
    """
    Extract bracket type and numeric edge from a Kalshi temp ticker.
    Examples: KXHIGHTDC-26JUN07-B88.5 -> ('B', 88.5)
              KXHIGHTDC-26JUN06-T92   -> ('T', 92.0)
    B = between/around a value; T = threshold (above/below). Returns (kind, val).
    """
    m = re.search(r'-([BT])(\d+(?:\.\d+)?)$', ticker)
    if not m:
        return (None, None)
    return (m.group(1), float(m.group(2)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("trade_log")
    ap.add_argument("resolutions")
    ap.add_argument("--city", default="Washington DC")
    ap.add_argument("--tier", default="main")
    args = ap.parse_args()

    trades = load(args.trade_log)
    res = {r["ticker"]: str(r.get("result", "")).lower()
           for r in load(args.resolutions)}

    sel = [t for t in trades
           if t.get("city") == args.city
           and t.get("entry_tier") == args.tier
           and res.get(t.get("ticker")) in ("yes", "no")]

    if not sel:
        print(f"No settled {args.city}/{args.tier} trades found.")
        return

    wins, losses = [], []
    for t in sel:
        outcome = res[t["ticker"]]
        side = str(t.get("side", "")).lower()
        (wins if side == outcome else losses).append(t)

    n = len(sel)
    wr = len(wins) / n
    prices = [t["entry_price"] for t in sel]
    mean_p = statistics.mean(prices)
    breakeven = mean_p   # NO bet at price p: need P(win) > p to be +EV

    print(f"=== {args.city} / {args.tier} ===")
    print(f"N={n}  wins={len(wins)}  losses={len(losses)}  WR={wr*100:.1f}%")
    print(f"mean entry No-price = {mean_p:.3f}  -> breakeven WR = {breakeven*100:.1f}%")
    verdict = "ABOVE breakeven (+EV)" if wr > breakeven else "BELOW breakeven (-EV)"
    print(f"realized WR is {verdict}")
    print()

    print("--- PRICE: entry No-price, wins vs losses ---")
    for label, grp in (("wins", wins), ("losses", losses)):
        if grp:
            ps = sorted(t["entry_price"] for t in grp)
            print(f"  {label:7} n={len(grp):3}  "
                  f"min={ps[0]:.2f} median={statistics.median(ps):.2f} max={ps[-1]:.2f}")
    # PnL contribution: a loss at price p costs p; a win at p earns (1-p)
    win_pnl = sum(1 - t["entry_price"] for t in wins)
    loss_pnl = sum(-t["entry_price"] for t in losses)
    print(f"  win PnL/ctr-equiv = +{win_pnl:.2f}   loss PnL/ctr-equiv = {loss_pnl:.2f}   "
          f"net = {win_pnl + loss_pnl:+.2f}")
    print()

    print("--- LOSS DETAIL (ticker, entry, bracket, score) ---")
    for t in sorted(losses, key=lambda x: -x["entry_price"]):
        kind, val = parse_bracket(t["ticker"])
        print(f"  {t['ticker']:28} entry={t['entry_price']:.2f}  "
              f"bracket={kind}{val}  score={t.get('score')}  "
              f"detail={','.join(t.get('score_detail', []))}")
    print()

    # Score distribution: do losses skew toward minimum-score (marginal) entries?
    print("--- SCORE: are losses concentrated in marginal (low-score) entries? ---")
    for label, grp in (("wins", wins), ("losses", losses)):
        sc = Counter(t.get("score") for t in grp)
        print(f"  {label:7} score histogram: {dict(sorted(sc.items()))}")


if __name__ == "__main__":
    main()
