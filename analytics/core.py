#!/usr/bin/env python3
"""
analytics.core — the canonical enriched-trade model and aggregation.

ONE source of truth for "how are we doing": loads the trade log, joins to the
AUTHORITATIVE settlements table (never observation-derived outcomes), and
produces enriched per-trade records. Aggregation along any axis — or any PAIR
of axes (the 2D interaction slicing) — with a Wilson lower bound on every cell.

DESIGN RULES (earned the hard way):
  1. AUTHORITATIVE ONLY. `result` comes from the settlements table. The ~12%
     observation-vs-authoritative gap means mixing sources corrupts every number.
     Unsettled trades carry result=None: counted (for coverage) but excluded from
     win/PnL aggregates.
  2. WILSON ON EVERYTHING. No cell reports a raw rate without its lower bound.
     Fine slicing breeds mirages; the Wilson floor is the guard.
  3. COMPUTE RETURNS DATA; RENDER IS SEPARATE. aggregate() returns Cells; the
     formatter (reports.py) prints them. This is what makes the dashboard tab
     (Step 4) reuse the same model with no rewrite.

Step 1: settled data only. Fills (Step 2) and capital/per-dollar-EV (Step 3)
layer on later without changing this core.
"""
from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass, field

from . import TRADE_LOG, OBS_DB
from . import wm_time

BANDS = [(0.00, 0.70), (0.70, 0.75), (0.75, 0.80), (0.80, 0.85),
         (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]


# ── shared math (the canonical copies — stop re-pasting these everywhere) ────

def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    return (p + z*z/(2*n) - z*math.sqrt((p*(1-p) + z*z/(4*n))/n)) / (1 + z*z/n)


def fee(price: float) -> float:
    """Kalshi fee per contract: round_up_to_cent(0.07 * p * (1-p))."""
    return math.ceil(0.07 * price * (1-price) * 100) / 100.0


def band_of(p: float):
    for lo, hi in BANDS:
        if lo <= p < hi:
            return (lo, hi)
    return None


def band_label(p: float) -> str:
    b = band_of(p)
    return f"[{b[0]:.2f},{b[1]:.2f})" if b else "?"


# ── canonical enriched trade ────────────────────────────────────────────────

@dataclass
class Trade:
    ticker: str
    engine: str               # entry_tier
    city: str
    market_type: str          # 'high' | 'lowt'
    market_date: str | None   # ISO, from ticker
    entry_local_hour: int | None
    entry_price: float
    contracts: int
    side: str
    result: str | None        # 'no' | 'yes' | None  (AUTHORITATIVE table only)

    @property
    def settled(self) -> bool:
        return self.result in ("no", "yes")

    @property
    def won(self) -> bool | None:
        if not self.settled:
            return None
        return self.result == self.side   # we enter 'no'; win if it settled 'no'

    @property
    def band(self) -> str:
        return band_label(self.entry_price)

    @property
    def price_cent(self) -> str:
        """Entry price as a 1-cent bucket label, e.g. '0.91'. Kalshi prices are
        already cent-denominated, so this is a label, not new binning logic —
        unlike `band` (5-cent buckets), it lets us see exactly where, within a
        single engine's tradable range, EV crosses zero (e.g. main's 0.90-0.92
        gate collapses into one 5-cent band; this resolves it to the cent)."""
        return f"{round(self.entry_price, 2):.2f}"

    @property
    def net_pnl(self) -> float | None:
        """Per-position PnL, fee-adjusted. None if unsettled."""
        if not self.settled:
            return None
        f = fee(self.entry_price) * self.contracts
        if self.won:
            return (1 - self.entry_price) * self.contracts - f
        return -self.entry_price * self.contracts - f


def load_trades(market_type: str | None = None,
                include_paper: bool = False) -> list[Trade]:
    """Load trade log, join to authoritative settlements, return enriched trades.
    Paths come from the package (anchored to repo root, not cwd)."""
    raw = json.loads(TRADE_LOG.read_text())
    con = sqlite3.connect(f"file:{OBS_DB}?mode=ro", uri=True)
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))
    con.close()

    trades: list[Trade] = []
    for t in raw:
        if t.get("paper") and not include_paper:
            continue
        ticker = t.get("ticker", "")
        price = t.get("entry_price")
        if not ticker or price is None:
            continue
        mt = t.get("market_type", "")
        if market_type and mt != market_type:
            continue
        city = t.get("city", "")
        trades.append(Trade(
            ticker=ticker,
            engine=t.get("entry_tier") or "main",
            city=city,
            market_type=mt,
            market_date=wm_time.market_date_iso(ticker),
            entry_local_hour=wm_time.local_hour(t.get("placed_at", ""), city),
            entry_price=float(price),
            contracts=int(t.get("contracts") or 1),
            side=str(t.get("side", "no")).lower(),
            result=settled.get(ticker),
        ))
    return trades


# ── aggregation (N-D), Wilson-gated ─────────────────────────────────────────

@dataclass
class Cell:
    key: tuple
    n_total: int = 0          # all trades in cell (settled + unsettled)
    n_settled: int = 0
    wins: int = 0
    losses: int = 0
    pnl: float = 0.0
    contracts: int = 0
    # Per-market-date LOSS pnl (only losing trades' negative pnl), for
    # concentration analysis: distinguishes "one bad day" (a healed-bug scar) from
    # "steady bleeding". A −$10 pocket from one day and from forty days look
    # identical in pnl alone but demand opposite actions. This breaks that tie.
    loss_by_day: dict = field(default_factory=dict)

    @property
    def wr(self) -> float | None:
        return self.wins / self.n_settled if self.n_settled else None

    @property
    def wilson(self) -> float | None:
        return wilson_lower(self.wins, self.n_settled) if self.n_settled else None

    @property
    def pnl_per_contract(self) -> float | None:
        return self.pnl / self.contracts if self.contracts else None

    @property
    def n_loss_days(self) -> int:
        return len(self.loss_by_day)

    @property
    def worst_day(self) -> tuple | None:
        """(date, pnl) of the single worst loss-day, or None if no losses."""
        if not self.loss_by_day:
            return None
        d = min(self.loss_by_day, key=lambda k: self.loss_by_day[k])
        return (d, self.loss_by_day[d])

    @property
    def loss_concentration(self) -> float | None:
        """Fraction of TOTAL loss dollars coming from the single worst day.
        ~1.0 = essentially all losses are one day (scar); ~0 = diffuse.
        None if the cell has no losses."""
        total_loss = sum(v for v in self.loss_by_day.values())  # negative
        if total_loss == 0:
            return None
        worst = self.worst_day[1]
        return worst / total_loss  # both negative -> positive ratio in [0,1]

    @property
    def is_scar(self) -> bool:
        """Heuristic: a net-negative cell whose loss is dominated by one day.
        Flags the 'healed bug-day' pattern so it isn't mistaken for chronic
        underperformance. Requires net-negative PnL, the worst day accounting for
        >=70% of all loss dollars, and >=3 losses (so tiny cells don't trip it)."""
        if self.pnl >= 0 or self.n_loss_days < 1:
            return False
        conc = self.loss_concentration
        return conc is not None and conc >= 0.70 and self.losses >= 3


# axis name -> extractor. Add axes here; reports & CLI pick them up automatically.
AXES = {
    "engine": lambda t: t.engine,
    "city":   lambda t: t.city,
    "band":   lambda t: t.band,
    "price_cent": lambda t: t.price_cent,
    "hour":   lambda t: t.entry_local_hour,
    "market": lambda t: t.market_type,
    "date":   lambda t: t.market_date,
}


def aggregate(trades: list[Trade], by: list[str]) -> list[Cell]:
    """Group trades by one or more axes; return Wilson-gated cells.
    Settled-only stats (wr/pnl) over settled trades; n_total shows coverage."""
    for a in by:
        if a not in AXES:
            raise ValueError(f"unknown axis '{a}'. choices: {sorted(AXES)}")
    getters = [AXES[a] for a in by]
    cells: dict[tuple, Cell] = {}
    for t in trades:
        key = tuple(g(t) for g in getters)
        c = cells.get(key)
        if c is None:
            c = cells[key] = Cell(key=key)
        c.n_total += 1
        if t.settled:
            c.n_settled += 1
            c.contracts += t.contracts
            if t.won:
                c.wins += 1
            else:
                c.losses += 1
                # record this loss against its market-date for concentration
                d = t.market_date or "?"
                c.loss_by_day[d] = c.loss_by_day.get(d, 0.0) + t.net_pnl
            c.pnl += t.net_pnl
    return list(cells.values())
