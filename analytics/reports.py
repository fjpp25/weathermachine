#!/usr/bin/env python3
"""
analytics.reports — rendering and standard report suite.

Pure formatting + composition. All computation lives in analytics.core; this
turns Cells into CLI tables. Because compute is separate, the future dashboard
tab (Step 4) calls core.aggregate() directly and renders to HTML instead —
nothing here needs to change for that.
"""
from __future__ import annotations

from .core import Cell, Trade, aggregate, load_trades


def render_cells(cells: list[Cell], by: list[str], min_n: int = 0,
                 sort: str = "pnl") -> str:
    """Format aggregated cells as a CLI table. Pure formatting, no computation."""
    shown = [c for c in cells if c.n_settled >= min_n]
    if sort == "pnl":
        shown.sort(key=lambda c: c.pnl)
    elif sort == "wr":
        shown.sort(key=lambda c: (c.wilson if c.wilson is not None else -1))
    elif sort == "n":
        shown.sort(key=lambda c: -c.n_settled)
    elif sort == "key":
        shown.sort(key=lambda c: tuple(str(x) for x in c.key))

    out = [
        f"{' x '.join(by):28}{'set/tot':>9}{'win':>5}{'loss':>5}"
        f"{'WR':>6}{'Wil_LB':>8}{'PnL':>9}{'PnL/ct':>9}",
        "-" * 79,
    ]
    tot = Cell(key=())
    for c in shown:
        keystr = " | ".join(str(x) for x in c.key)
        wr = f"{c.wr*100:.0f}%" if c.wr is not None else "—"
        wl = f"{c.wilson*100:.0f}%" if c.wilson is not None else "—"
        ppc = f"${c.pnl_per_contract:+.3f}" if c.pnl_per_contract is not None else "—"
        out.append(f"{keystr:28}{f'{c.n_settled}/{c.n_total}':>9}{c.wins:>5}"
                   f"{c.losses:>5}{wr:>6}{wl:>8}{c.pnl:>+9.2f}{ppc:>9}")
        tot.n_total += c.n_total; tot.n_settled += c.n_settled
        tot.wins += c.wins; tot.losses += c.losses
        tot.pnl += c.pnl; tot.contracts += c.contracts
    out.append("-" * 79)
    twr = f"{tot.wr*100:.0f}%" if tot.wr is not None else "—"
    twl = f"{tot.wilson*100:.0f}%" if tot.wilson is not None else "—"
    out.append(f"{'TOTAL':28}{f'{tot.n_settled}/{tot.n_total}':>9}{tot.wins:>5}"
               f"{tot.losses:>5}{twr:>6}{twl:>8}{tot.pnl:>+9.2f}")
    if min_n:
        out.append(f"(cells with < {min_n} settled hidden)")
    return "\n".join(out)


def standard_report(market_type: str | None = None) -> str:
    """The periodic health check: per-engine, per-city, per-band at a glance."""
    trades = load_trades(market_type=market_type)
    n_set = sum(1 for t in trades if t.settled)
    out = [
        "Weather Machine — analytics (authoritative settlement)",
        f"trades: {len(trades)} total, {n_set} settled, "
        f"{len(trades)-n_set} unsettled"
        + (f"  [market={market_type}]" if market_type else ""),
        "",
    ]
    for by in (["engine"], ["city"], ["band"]):
        out.append(render_cells(aggregate(trades, by), by, sort="pnl"))
        out.append("")
    return "\n".join(out)


def one_report(by: list[str], market_type: str | None = None,
               min_n: int = 0, sort: str = "pnl") -> str:
    """A single ad-hoc slice (1-D or 2-D)."""
    trades = load_trades(market_type=market_type)
    return render_cells(aggregate(trades, by), by, min_n=min_n, sort=sort)
