#!/usr/bin/env python3
"""
analytics.__main__ — CLI entry point.

    python3 -m analytics                      # standard suite (engine/city/band)
    python3 -m analytics --by engine city     # 2D interaction slice
    python3 -m analytics --by band --market lowt --min-n 30
    python3 -m analytics --by hour --sort key

Run from the repo root. The package anchors its own paths to the repo root, so
it also works when imported by a process with a different cwd (the dashboard).
"""
from __future__ import annotations

import argparse

from .core import AXES
from .reports import standard_report, one_report


def main():
    ap = argparse.ArgumentParser(prog="analytics")
    ap.add_argument("--by", nargs="+", default=None,
                    help=f"axes to group by (1 or 2 of: {' '.join(sorted(AXES))})")
    ap.add_argument("--market", choices=["high", "lowt"], default=None)
    ap.add_argument("--min-n", type=int, default=0,
                    help="hide cells with fewer than N settled trades")
    ap.add_argument("--sort", choices=["pnl", "wr", "n", "key"], default="pnl")
    args = ap.parse_args()

    if args.by:
        print(one_report(args.by, market_type=args.market,
                         min_n=args.min_n, sort=args.sort))
    else:
        print(standard_report(market_type=args.market))


if __name__ == "__main__":
    main()
