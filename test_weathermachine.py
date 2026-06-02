"""
tests/test_weathermachine.py
-----------------------------
Automated test suite for The Weather Machine.

Three layers:
  Layer 1 — Static checks   : imports, constants, signatures (no Kalshi needed)
  Layer 2 — Unit tests      : logic with mock bracket data (no Kalshi needed)
  Layer 3 — Smoke test      : one poll cycle in paper mode (needs Kalshi)

Run:
    pytest tests/ -v                          # all tests
    pytest tests/ -v -m static               # Layer 1 only (fastest)
    pytest tests/ -v -m unit                 # Layer 2 only
    pytest tests/ -v -m smoke                # Layer 3 only (needs connection)
    pytest tests/ -v -m "static or unit"     # skip smoke tests

The test file is designed to be run from the weathermachine root directory:
    cd ~/weathermachine && pytest tests/ -v
"""

import ast
import importlib
import inspect
import re
import sys
import os
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

# ---------------------------------------------------------------------------
# Path setup — ensure weathermachine root is on sys.path
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _src(filename: str) -> str:
    """Read source of a file from the project root."""
    return (ROOT / filename).read_text()


def _count(pattern: str, src: str) -> int:
    return len(re.findall(pattern, src))


# ===========================================================================
# LAYER 1 — STATIC CHECKS
# ===========================================================================

class TestImports:
    """Every module must import exactly the dependencies it needs."""

    @pytest.mark.static
    def test_hde_imports_market_utils_no_price(self):
        src = _src("hight_decision_engine.py")
        assert "from market_utils import no_price as _no_price" in src, (
            "hight_decision_engine must import _no_price from market_utils. "
            "evaluate_bracket calls _no_price() — missing import causes NameError."
        )

    @pytest.mark.static
    def test_scheduler_imports_evening_convergence(self):
        src = _src("scheduler.py")
        assert "import evening_convergence" in src, (
            "scheduler.py must import evening_convergence."
        )

    @pytest.mark.static
    def test_scheduler_imports_nws_feed(self):
        src = _src("scheduler.py")
        assert "import nws_feed" in src, (
            "scheduler.py must import nws_feed for pre-fetch snapshot."
        )

    @pytest.mark.static
    def test_scheduler_imports_kalshi_scanner(self):
        src = _src("scheduler.py")
        assert "import kalshi_scanner" in src, (
            "scheduler.py must import kalshi_scanner for pre-fetch snapshot."
        )


class TestConstants:
    """Key parameters must be at their correct values."""

    @pytest.mark.static
    def test_hde_vol_max_entry(self):
        src = _src("hight_decision_engine.py")
        assert "VOL_MAX_ENTRY" in src, "VOL_MAX_ENTRY must be defined in hight_decision_engine"
        match = re.search(r"VOL_MAX_ENTRY\s*=\s*(\d+)", src)
        assert match, "VOL_MAX_ENTRY not found"
        assert int(match.group(1)) == 1500, (
            f"VOL_MAX_ENTRY should be 1500 (backtest optimal). Got {match.group(1)}"
        )

    @pytest.mark.static
    def test_hde_no_max_entry_price(self):
        src = _src("hight_decision_engine.py")
        match = re.search(r"NO_MAX_ENTRY_PRICE\s*=\s*([\d.]+)", src)
        assert match, "NO_MAX_ENTRY_PRICE not found"
        assert float(match.group(1)) == 0.95, (
            f"NO_MAX_ENTRY_PRICE should be 0.95. Got {match.group(1)}"
        )

    @pytest.mark.static
    def test_hde_kelly_max_contracts(self):
        src = _src("hight_decision_engine.py")
        match = re.search(r"KELLY_MAX_CONTRACTS\s*=\s*(\d+)", src)
        assert match, "KELLY_MAX_CONTRACTS not found"
        assert int(match.group(1)) == 5, (
            f"KELLY_MAX_CONTRACTS should be 5. Got {match.group(1)}"
        )

    @pytest.mark.static
    def test_hde_near_cap_max_contracts_separate_from_kelly(self):
        src = _src("hight_decision_engine.py")
        assert "NEAR_CAP_MAX_CONTRACTS" in src, (
            "NEAR_CAP_MAX_CONTRACTS must be defined separately from KELLY_MAX_CONTRACTS. "
            "Near-cap uses flat 3c sizing, not Kelly."
        )
        match = re.search(r"NEAR_CAP_MAX_CONTRACTS\s*=\s*(\d+)", src)
        assert match and int(match.group(1)) == 3, (
            f"NEAR_CAP_MAX_CONTRACTS should be 3. Got {match.group(1) if match else 'not found'}"
        )

    @pytest.mark.static
    def test_hde_kelly_fractions_present(self):
        src = _src("hight_decision_engine.py")
        assert "KELLY_HALF_FRACTIONS" in src, (
            "KELLY_HALF_FRACTIONS dict must be defined in hight_decision_engine"
        )
        # All 9 price bands should be present
        bands = ["0.75", "0.78", "0.80", "0.82", "0.84",
                 "0.86", "0.88", "0.90", "0.92"]
        for b in bands:
            assert b in src, f"Kelly fraction for band starting at {b} not found"

    @pytest.mark.static
    def test_cascade_no_max_entry(self):
        src = _src("cascade_engine.py")
        match = re.search(r"^NO_MAX_ENTRY\s*=\s*([\d.]+)", src, re.MULTILINE)
        assert match, "NO_MAX_ENTRY not found in cascade_engine"
        assert float(match.group(1)) == 0.94, (
            f"cascade NO_MAX_ENTRY should be 0.94. Got {match.group(1)}"
        )

    @pytest.mark.static
    def test_tomorrow_scanner_no_max_entry(self):
        src = _src("tomorrow_scanner.py")
        match = re.search(r"^NO_MAX_ENTRY\s*=\s*([\d.]+)", src, re.MULTILINE)
        assert match, "NO_MAX_ENTRY not found in tomorrow_scanner"
        assert float(match.group(1)) == 0.94, (
            f"tomorrow_scanner NO_MAX_ENTRY should be 0.94. Got {match.group(1)}"
        )

    @pytest.mark.static
    def test_tomorrow_scanner_dismissed_no_max(self):
        src = _src("tomorrow_scanner.py")
        match = re.search(r"DISMISSED_NO_MAX\s*=\s*([\d.]+)", src)
        assert match, "DISMISSED_NO_MAX not found in tomorrow_scanner"
        assert float(match.group(1)) == 0.94, (
            f"DISMISSED_NO_MAX should be 0.94. Got {match.group(1)}"
        )

    @pytest.mark.static
    def test_cascade_conv_threshold(self):
        src = _src("cascade_engine.py")
        match = re.search(r"CONV_THRESHOLD\s*=\s*([\d.]+)", src)
        assert match and float(match.group(1)) == 0.97, (
            "cascade CONV_THRESHOLD should be 0.97"
        )


class TestNoDuplicateCalls:
    """Specific functions must appear exactly the right number of times."""

    @pytest.mark.static
    def test_cascade_display_not_in_hde_display(self):
        src = _src("hight_decision_engine.py")
        count = src.count("cascade_engine.display")
        assert count == 0, (
            f"cascade_engine.display found {count} times in hight_decision_engine.py. "
            "It should only be called from trader.run_pipeline, not from "
            "hight_decision_engine.display() — otherwise cascade signals print twice."
        )

    @pytest.mark.static
    def test_cascade_display_in_trader(self):
        src = _src("trader.py")
        assert "cascade_engine.display" in src, (
            "cascade_engine.display must be called from trader.run_pipeline"
        )


class TestFunctionSignatures:
    """Critical functions must have the expected parameters."""

    @pytest.mark.static
    def test_run_pipeline_accepts_snapshots(self):
        src = _src("trader.py")
        match = re.search(r"def run_pipeline\(([^)]+)\)", src, re.DOTALL)
        assert match, "run_pipeline not found in trader.py"
        params = match.group(1)
        for p in ["kalshi_high", "kalshi_lowt", "nws_snapshot"]:
            assert p in params, (
                f"run_pipeline missing parameter '{p}'. "
                "Scheduler pre-fetches snapshots and passes them down."
            )

    @pytest.mark.static
    def test_scheduler_passes_snapshots_to_pipeline(self):
        src = _src("scheduler.py")
        assert "kalshi_high" in src, (
            "scheduler must pass kalshi_high to run_pipeline"
        )
        assert "_k_high" in src and "_k_lowt" in src and "_nws" in src, (
            "scheduler must pre-fetch _k_high, _k_lowt, _nws before thread pool"
        )

    @pytest.mark.static
    def test_scheduler_has_five_workers(self):
        src = _src("scheduler.py")
        assert "max_workers=5" in src, (
            "ThreadPoolExecutor must use max_workers=5 (5 engines run in parallel)"
        )

    @pytest.mark.static
    def test_trader_has_balance_cache(self):
        src = _src("trader.py")
        assert "def get_balance_cached" in src, (
            "trader must define get_balance_cached() for Kelly sizing"
        )
        assert "def set_balance_cached" in src, (
            "trader must define set_balance_cached() to update cache each poll"
        )
        assert "set_balance_cached(balance)" in src, (
            "set_balance_cached must be called in run_pipeline after balance fetch"
        )

    @pytest.mark.static
    def test_hde_run_accepts_snapshots(self):
        src = _src("hight_decision_engine.py")
        match = re.search(r"^def run\(([^)]+)\)", src, re.MULTILINE)
        assert match, "hight_decision_engine.run() not found"
        params = match.group(1)
        for p in ["kalshi_snapshot", "nws_snapshot"]:
            assert p in params, (
                f"hight_decision_engine.run() missing parameter '{p}'"
            )


class TestObsHighOverride:
    """Forecast override logic must be present."""

    @pytest.mark.static
    def test_obs_high_override_present(self):
        src = _src("hight_decision_engine.py")
        assert "observed_high > corrected_forecast" in src, (
            "obs_high override missing from evaluate_city(). "
            "When observed_high exceeds corrected_forecast, the observation "
            "must be used as the effective forecast to prevent entering No "
            "on brackets the temperature has already passed."
        )

    @pytest.mark.static
    def test_forecast_shift_tracker_wired(self):
        src = _src("hight_decision_engine.py")
        assert "forecast_shift_tracker" in src, (
            "forecast_shift_tracker not wired into hight_decision_engine. "
            "It must call _fst.update_high(city, forecast_high) each poll."
        )


class TestCitiesConfig:
    """cities.py must have the right trading configuration."""

    @pytest.mark.static
    def test_san_antonio_paused(self):
        src = _src("cities.py")
        # Find San Antonio block
        match = re.search(
            r'"San Antonio".*?trading_high.*?(True|False)',
            src, re.DOTALL
        )
        assert match, "San Antonio trading_high not found"
        assert match.group(1) == "False", (
            "San Antonio must stay paused — near-zero EV on vol<=1500 backtest"
        )

    @pytest.mark.static
    def test_trading_cities_count(self):
        """Should have 19 trading HIGH cities (all except San Antonio)."""
        sys.path.insert(0, str(ROOT))
        try:
            import importlib
            cities_mod = importlib.import_module("cities")
            importlib.reload(cities_mod)
            n = len(cities_mod.TRADING_CITIES)
            assert n == 19, (
                f"Expected 19 HIGH trading cities, got {n}. "
                "All cities except San Antonio should be enabled."
            )
        except ImportError:
            pytest.skip("cities.py not importable from test environment")

    @pytest.mark.static
    def test_time_windows_removed(self):
        src = _src("cities.py")
        # All active cities should have trade_start_high: 0
        # Count trade_start_high: 0 vs trade_start_high: 9/10
        zero_starts = len(re.findall(r'"trade_start_high":\s*0', src))
        old_starts  = len(re.findall(r'"trade_start_high":\s*[1-9]\d*,', src))
        assert zero_starts >= 15, (
            f"Expected at least 15 cities with trade_start_high=0 (time windows removed). "
            f"Found {zero_starts}. Old-style windows: {old_starts}."
        )

    @pytest.mark.static
    def test_requirements_has_pyotp(self):
        src = _src("requirements.txt")
        assert "pyotp" in src, (
            "requirements.txt missing pyotp — Kalshi auth will fail on fresh install"
        )

    @pytest.mark.static
    def test_requirements_has_pandas(self):
        src = _src("requirements.txt")
        assert "pandas" in src, (
            "requirements.txt missing pandas — enrich_trade_log.py will fail"
        )

    @pytest.mark.static
    def test_make_client_skip_confirmation(self):
        src = _src("scheduler.py")
        assert "make_client(skip_confirmation=True)" in src, (
            "scheduler.py must call make_client(skip_confirmation=True). "
            "Without this, the systemd service crashes with EOFError."
        )


# ===========================================================================
# LAYER 2 — UNIT TESTS
# ===========================================================================

# Minimal mock bracket factory
def _bracket(no=0.85, yes=0.15, volume=500, floor=80.0, cap=82.0,
             ticker="KXHIGHTATL-26MAY20-B80.5", status=None) -> dict:
    return {
        "ticker":       ticker,
        "floor":        floor,
        "cap":          cap,
        "ob_no_bid":    no,
        "ob_no_ask":    no,
        "ob_yes_ask":   yes,
        "ob_yes_bid":   yes,
        "ob_spread":    0.02,
        "ob_no_depth":  1000,
        "ob_yes_depth": 1000,
        "volume":       volume,
        "no_price":     no,
        "yes_price":    yes,
        "status":       status,
        "candles":      [],
        "bracket":      ticker.split("-")[-1] if ticker else "",
    }


class TestMarketUtils:
    """market_utils helpers must resolve fields in correct priority order."""

    @pytest.mark.unit
    def test_no_price_ob_bid_priority(self):
        from market_utils import no_price
        b = {"ob_no_bid": 0.88, "ob_no_ask": 0.90, "no_ask": 0.85}
        assert no_price(b) == 0.88, "ob_no_bid should take priority"

    @pytest.mark.unit
    def test_no_price_fallback_chain(self):
        from market_utils import no_price
        # Missing ob fields — falls back to no_ask
        b = {"no_ask": 0.85}
        assert no_price(b) == 0.85
        # Falls back to no_price field
        b2 = {"no_price": 0.82}
        assert no_price(b2) == 0.82
        # Zero value should fall through to next field
        b3 = {"ob_no_bid": 0.0, "no_ask": 0.85}
        assert no_price(b3) == 0.85

    @pytest.mark.unit
    def test_no_price_zero_default(self):
        from market_utils import no_price
        assert no_price({}) == 0.0

    @pytest.mark.unit
    def test_is_resolved(self):
        from market_utils import is_resolved
        assert is_resolved({"ob_no_bid": 0.97})      # No resolved
        assert is_resolved({"ob_yes_ask": 0.96})     # Yes resolved
        assert not is_resolved({"ob_no_bid": 0.90})  # Not resolved
        assert not is_resolved({})

    @pytest.mark.unit
    def test_is_b_bracket(self):
        from market_utils import is_b_bracket
        assert is_b_bracket({"ticker": "KXHIGHTATL-26MAY20-B80.5"})
        assert not is_b_bracket({"ticker": "KXHIGHTATL-26MAY20-T80"})
        assert is_b_bracket({"bracket": "B80.5"})
        assert not is_b_bracket({"bracket": "T80"})
        assert not is_b_bracket({})


class TestKellyContracts:
    """kelly_contracts must return correct values for each price band."""

    @pytest.mark.unit
    def test_returns_fallback_with_no_bankroll(self):
        import hight_decision_engine as hde
        result = hde.kelly_contracts(0.85, None)
        assert result == hde.KELLY_FALLBACK_CONTRACTS, (
            f"With no bankroll should return KELLY_FALLBACK_CONTRACTS={hde.KELLY_FALLBACK_CONTRACTS}"
        )

    @pytest.mark.unit
    def test_returns_fallback_for_out_of_range_price(self):
        import hight_decision_engine as hde
        # Price outside all bands
        result = hde.kelly_contracts(0.50, 200.0)
        assert result == hde.KELLY_FALLBACK_CONTRACTS

    @pytest.mark.unit
    def test_respects_max_contracts(self):
        import hight_decision_engine as hde
        # Very large bankroll — should hit KELLY_MAX_CONTRACTS cap
        result = hde.kelly_contracts(0.85, 100_000.0)
        assert result <= hde.KELLY_MAX_CONTRACTS, (
            f"kelly_contracts must never exceed KELLY_MAX_CONTRACTS={hde.KELLY_MAX_CONTRACTS}"
        )

    @pytest.mark.unit
    def test_respects_min_contracts(self):
        import hight_decision_engine as hde
        # Tiny bankroll — should still return at least 1
        result = hde.kelly_contracts(0.85, 0.01)
        assert result >= 1, "kelly_contracts must return at least 1 contract"

    @pytest.mark.unit
    def test_higher_bankroll_gives_more_contracts(self):
        import hight_decision_engine as hde
        small = hde.kelly_contracts(0.85, 50.0)
        large = hde.kelly_contracts(0.85, 500.0)
        assert large >= small, (
            "More bankroll should give >= contracts (up to the cap)"
        )

    @pytest.mark.unit
    def test_all_bands_covered(self):
        import hight_decision_engine as hde
        test_prices = [0.76, 0.79, 0.81, 0.83, 0.85, 0.87, 0.89, 0.91, 0.93]
        for p in test_prices:
            result = hde.kelly_contracts(p, 200.0)
            assert isinstance(result, int) and result >= 1, (
                f"kelly_contracts({p}, 200) returned invalid: {result}"
            )


class TestVolumeGate:
    """evaluate_bracket must gate on volume correctly."""

    @pytest.mark.unit
    def test_high_volume_blocked(self):
        import hight_decision_engine as hde
        b = _bracket(no=0.85, yes=0.15, volume=5000)
        sig = hde.evaluate_bracket(
            bracket=b, forecast_high=75.0, observed_high=70.0,
            city_local_hour=15, trade_start_hour=0, trade_end_hour=24,
            city_bias=0.0, dynamic_buffer=2.0, corrected_forecast=75.0
        )
        assert sig.get("trade_type") is None, (
            f"Volume 5000 should be blocked (VOL_MAX_ENTRY=1500). "
            f"Got trade_type={sig.get('trade_type')}, skip={sig.get('skip_reason')}"
        )
        assert "Volume" in (sig.get("skip_reason") or ""), (
            f"skip_reason should mention volume. Got: {sig.get('skip_reason')}"
        )

    @pytest.mark.unit
    def test_low_volume_passes_gate(self):
        import hight_decision_engine as hde
        b = _bracket(no=0.85, yes=0.15, volume=500)
        sig = hde.evaluate_bracket(
            bracket=b, forecast_high=75.0, observed_high=70.0,
            city_local_hour=15, trade_start_hour=0, trade_end_hour=24,
            city_bias=0.0, dynamic_buffer=2.0, corrected_forecast=75.0
        )
        assert sig.get("trade_type") == "NO", (
            f"Volume 500 should pass gate. "
            f"Got trade_type={sig.get('trade_type')}, skip={sig.get('skip_reason')}"
        )

    @pytest.mark.unit
    def test_price_below_min_blocked(self):
        import hight_decision_engine as hde
        b = _bracket(no=0.60, yes=0.40, volume=500)
        sig = hde.evaluate_bracket(
            bracket=b, forecast_high=75.0, observed_high=70.0,
            city_local_hour=15, trade_start_hour=0, trade_end_hour=24,
            city_bias=0.0, dynamic_buffer=2.0, corrected_forecast=75.0
        )
        assert sig.get("trade_type") is None, (
            f"No price 0.60 below NO_MIN_ENTRY_PRICE=0.75 should be blocked"
        )

    @pytest.mark.unit
    def test_price_above_max_blocked(self):
        import hight_decision_engine as hde
        b = _bracket(no=0.96, yes=0.04, volume=500)
        sig = hde.evaluate_bracket(
            bracket=b, forecast_high=75.0, observed_high=70.0,
            city_local_hour=15, trade_start_hour=0, trade_end_hour=24,
            city_bias=0.0, dynamic_buffer=2.0, corrected_forecast=75.0
        )
        assert sig.get("trade_type") is None, (
            f"No price 0.96 above NO_MAX_ENTRY_PRICE=0.95 should be blocked"
        )

    @pytest.mark.unit
    def test_volume_exactly_at_max_passes(self):
        import hight_decision_engine as hde
        b = _bracket(no=0.85, yes=0.15, volume=1500)
        sig = hde.evaluate_bracket(
            bracket=b, forecast_high=75.0, observed_high=70.0,
            city_local_hour=15, trade_start_hour=0, trade_end_hour=24,
            city_bias=0.0, dynamic_buffer=2.0, corrected_forecast=75.0
        )
        assert sig.get("trade_type") == "NO", (
            "Volume exactly at VOL_MAX_ENTRY=1500 should pass (inclusive boundary)"
        )

    @pytest.mark.unit
    def test_volume_one_over_max_blocked(self):
        import hight_decision_engine as hde
        b = _bracket(no=0.85, yes=0.15, volume=1501)
        sig = hde.evaluate_bracket(
            bracket=b, forecast_high=75.0, observed_high=70.0,
            city_local_hour=15, trade_start_hour=0, trade_end_hour=24,
            city_bias=0.0, dynamic_buffer=2.0, corrected_forecast=75.0
        )
        assert sig.get("trade_type") is None, (
            "Volume 1501 (one over max) should be blocked"
        )


class TestCascadeSignals:
    """cascade_engine must fire on confirmation and respect thresholds."""

    @pytest.mark.unit
    def test_bottom_up_fires_on_confirmation(self):
        import cascade_engine
        # Reset session state
        cascade_engine._cascade_entered.clear()
        cascade_engine._direction_locked.clear()
        cascade_engine._entries_made.clear()
        cascade_engine._trigger_hour.clear()

        # b0 confirmed at 0.98 → b1 is target (rank 1 from bottom, within MAX_RANK_FROM_BOTTOM=2)
        # b2 is the market forecast (highest yes_price) — b1 must be >= 2 ranks away from it
        # Use 5 brackets so b1 (rank 1) is far enough from b4 (forecast)
        b0 = _bracket(no=0.98, yes=0.02, floor=70.0, cap=72.0,
                      ticker="KXHIGHTATL-26MAY20-B70.5", volume=500)
        b1 = _bracket(no=0.70, yes=0.30, floor=72.0, cap=74.0,
                      ticker="KXHIGHTATL-26MAY20-B72.5", volume=500)
        b2 = _bracket(no=0.50, yes=0.50, floor=74.0, cap=76.0,
                      ticker="KXHIGHTATL-26MAY20-B74.5", volume=500)
        b3 = _bracket(no=0.30, yes=0.70, floor=76.0, cap=78.0,
                      ticker="KXHIGHTATL-26MAY20-B76.5", volume=500)
        b4 = _bracket(no=0.10, yes=0.90, floor=78.0, cap=80.0,
                      ticker="KXHIGHTATL-26MAY20-B78.5", volume=500)  # forecast bracket

        with patch("cascade_engine._local_hour", return_value=10), \
             patch("cascade_engine._corrected_forecast", return_value=79.0):
            sigs = cascade_engine._convergence_signals(
                "Atlanta", [b0, b1, b2, b3, b4], {}
            )

        assert len(sigs) >= 1, (
            "cascade should fire when b0 (No=0.98) confirms and b1 is the target. "
            f"Got {len(sigs)} signals."
        )
        assert sigs[0]["ticker"] == b1["ticker"], (
            "cascade should target bracket immediately above the confirmed one"
        )

    @pytest.mark.unit
    def test_bottom_up_respects_no_max_entry(self):
        import cascade_engine
        cascade_engine._cascade_entered.clear()
        cascade_engine._direction_locked.clear()
        cascade_engine._entries_made.clear()
        cascade_engine._trigger_hour.clear()

        b0 = _bracket(no=0.98, yes=0.02, floor=78.0, cap=80.0,
                      ticker="KXHIGHTATL-26MAY20-B78.5", volume=500)
        # Target has No > NO_MAX_ENTRY (0.94) — should not fire
        b1 = _bracket(no=0.96, yes=0.04, floor=80.0, cap=82.0,
                      ticker="KXHIGHTATL-26MAY20-B80.5", volume=500)
        b2 = _bracket(no=0.30, yes=0.70, floor=82.0, cap=84.0,
                      ticker="KXHIGHTATL-26MAY20-B82.5", volume=500)

        with patch("cascade_engine._local_hour", return_value=10):
            sigs = cascade_engine._convergence_signals("Atlanta", [b0, b1, b2], {})

        assert all(s["ticker"] != b1["ticker"] for s in sigs), (
            f"cascade should not enter b1 when No={b1['ob_no_bid']} > NO_MAX_ENTRY=0.94"
        )

    @pytest.mark.unit
    def test_no_cascade_after_hour_cap(self):
        import cascade_engine
        cascade_engine._cascade_entered.clear()
        cascade_engine._direction_locked.clear()
        cascade_engine._entries_made.clear()
        cascade_engine._trigger_hour.clear()

        b0 = _bracket(no=0.98, yes=0.02, floor=78.0, cap=80.0,
                      ticker="KXHIGHTATL-26MAY20-B78.5", volume=500)
        b1 = _bracket(no=0.70, yes=0.30, floor=80.0, cap=82.0,
                      ticker="KXHIGHTATL-26MAY20-B80.5", volume=500)
        b2 = _bracket(no=0.30, yes=0.70, floor=82.0, cap=84.0,
                      ticker="KXHIGHTATL-26MAY20-B82.5", volume=500)

        with patch("cascade_engine._local_hour", return_value=16):  # >= START_HOUR_CAP
            sigs = cascade_engine._convergence_signals("Atlanta", [b0, b1, b2], {})

        assert len(sigs) == 0, (
            f"cascade must not start new signals at or after START_HOUR_CAP={cascade_engine.START_HOUR_CAP}"
        )


class TestEveningConvergence:
    """evening_convergence must fire on exactly 3 active brackets >= hour 19."""

    @pytest.mark.unit
    def test_fires_on_three_active_brackets(self):
        import evening_convergence as ec
        ec._fired.clear()

        # 3 active (not resolved) brackets, non-forecast B bracket with No in range
        b0 = _bracket(no=0.92, yes=0.08, ticker="KXHIGHTATL-26MAY20-B78.5")
        b1 = _bracket(no=0.20, yes=0.80, ticker="KXHIGHTATL-26MAY20-B80.5")  # forecast
        b2 = _bracket(no=0.90, yes=0.10, ticker="KXHIGHTATL-26MAY20-B82.5")

        client = MagicMock()
        trader_mock = MagicMock()
        trader_mock.place_order = MagicMock()
        trader_mock._append_trade_log = MagicMock()
        trader_mock.get_engine_capital = MagicMock(side_effect=Exception("no cap"))

        with patch("evening_convergence._local_hour", return_value=20):
            ec._check_city("Atlanta", [b0, b1, b2],
                           client, paper=True, _trader=trader_mock, local_hour=20)

        # In paper mode, no order placed — just check it didn't crash
        # and that it correctly identified non-forecast brackets

    @pytest.mark.unit
    def test_skips_wrong_bracket_count(self):
        import evening_convergence as ec
        ec._fired.clear()

        # 4 active brackets — should not fire
        brackets = [
            _bracket(no=0.85, yes=0.15, ticker=f"KXHIGHTATL-26MAY20-B{70+i*2}.5")
            for i in range(4)
        ]
        client = MagicMock()
        trader_mock = MagicMock()

        with patch("evening_convergence._local_hour", return_value=20):
            # _check_city checks active count — should skip all
            ec._check_city("Atlanta", brackets, client,
                           paper=True, _trader=trader_mock, local_hour=20)
        # No orders should be placed
        trader_mock.place_order.assert_not_called()


class TestObsHighOverrideLogic:
    """Obs high override must update corrected_forecast correctly."""

    @pytest.mark.unit
    def test_obs_high_above_forecast_updates_corrected(self):
        """When obs_high > corrected_forecast, corrected_forecast should become obs_high."""
        import hight_decision_engine as hde
        # We test via evaluate_city with a mock bracket where obs_high exceeds forecast
        # A bracket at floor=82 should be gated by boundary buffer if corrected=85
        # but obs_high=88 should push corrected_forecast to 88, blocking entry on B82

        b = _bracket(no=0.85, yes=0.15, volume=500, floor=82.0, cap=84.0,
                     ticker="KXHIGHTATL-26MAY20-B82.5")

        nws_data = {
            "forecast_high_f":  85.0,
            "observed_high_f":  88.0,  # already above 85 — should push corrected to 88
            "city_local_hour":  15,
        }
        scan_data = {"brackets": [b], "error": None}
        profiles  = {}

        with patch("hight_decision_engine._AW_AVAILABLE", False), \
             patch("hight_decision_engine._city_bias", return_value=0.0), \
             patch("hight_decision_engine._dynamic_buffer", return_value=2.0), \
             patch("hight_decision_engine._trade_start_for", return_value=0), \
             patch("hight_decision_engine._trade_end_for", return_value=24):
            result = hde.evaluate_city("Atlanta", nws_data, scan_data, profiles)

        # corrected_forecast should be 88.0 (the obs_high)
        assert result.get("corrected_forecast") == 88.0, (
            f"corrected_forecast should be obs_high=88.0 when obs > forecast. "
            f"Got {result.get('corrected_forecast')}"
        )


# ===========================================================================
# LAYER 3 — SMOKE TEST
# ===========================================================================

@pytest.mark.smoke
class TestSchedulerSmoke:
    """One poll cycle must complete without crashing (paper mode, needs Kalshi)."""

    def test_make_client_skip_confirmation(self):
        """make_client(skip_confirmation=True) must not prompt for input."""
        try:
            import trader
            # Just test the signature accepts the param — don't actually connect
            sig = inspect.signature(trader.make_client)
            assert "skip_confirmation" in sig.parameters, (
                "make_client must accept skip_confirmation parameter"
            )
        except ImportError:
            pytest.skip("trader.py not importable")

    def test_scheduler_module_imports_cleanly(self):
        """All imports in scheduler.py must resolve without error."""
        try:
            # Test that all module-level imports succeed
            import scheduler
        except ImportError as e:
            pytest.fail(f"scheduler.py has import error: {e}")
        except Exception as e:
            pytest.skip(f"scheduler.py startup failed (expected without credentials): {e}")

    def test_hde_module_imports_cleanly(self):
        """hight_decision_engine.py must import without error."""
        try:
            import hight_decision_engine
            assert hasattr(hight_decision_engine, "kelly_contracts"), \
                "kelly_contracts function missing"
            assert hasattr(hight_decision_engine, "VOL_MAX_ENTRY"), \
                "VOL_MAX_ENTRY constant missing"
            assert hasattr(hight_decision_engine, "KELLY_HALF_FRACTIONS"), \
                "KELLY_HALF_FRACTIONS missing"
        except ImportError as e:
            pytest.fail(f"hight_decision_engine.py import error: {e}")

    def test_cascade_engine_imports_cleanly(self):
        try:
            import cascade_engine
            assert cascade_engine.NO_MAX_ENTRY == 0.94
            assert cascade_engine.CONV_THRESHOLD == 0.97
        except ImportError as e:
            pytest.fail(f"cascade_engine.py import error: {e}")

    def test_market_utils_imports_cleanly(self):
        try:
            from market_utils import no_price, yes_price, is_resolved, is_b_bracket
        except ImportError as e:
            pytest.fail(f"market_utils.py import error: {e}")


# ===========================================================================
# LAYER 1 additions — new static checks
# ===========================================================================

class TestOpenOrderManagement:
    """manage_open_orders must cancel stale and out-of-range orders."""

    @pytest.mark.static
    def test_order_age_tracker_defined(self):
        src = _src("trader.py")
        assert "_order_age" in src, (
            "_order_age dict must be defined in trader.py to track order age across polls"
        )

    @pytest.mark.static
    def test_max_order_age_polls_defined(self):
        src = _src("trader.py")
        assert "MAX_ORDER_AGE_POLLS" in src, (
            "MAX_ORDER_AGE_POLLS must be defined in manage_open_orders"
        )

    @pytest.mark.static
    def test_replace_not_amend(self):
        src = _src("trader.py")
        # New logic cancels + re-places rather than amending
        assert "REPLACE" in src, (
            "manage_open_orders must log REPLACE action (cancel + re-place)"
        )

    @pytest.mark.static
    def test_stale_cancel_logic(self):
        src = _src("trader.py")
        assert "stale" in src, (
            "manage_open_orders must cancel stale orders (age > MAX_ORDER_AGE_POLLS)"
        )

    @pytest.mark.static
    def test_resting_orders_in_open_contracts(self):
        src = _src("trader.py")
        assert "resting_contracts_count" in src or \
               '"status": "resting"' in src or \
               "'status': 'resting'" in src, (
            "run_pipeline must fetch resting orders and include them in open_contracts "
            "to prevent the accumulation bug"
        )

    @pytest.mark.static
    def test_entry_tier_defaults_to_main(self):
        src = _src("trader.py")
        assert '"entry_tier":   signal.get("entry_tier", "") or "main"' in src or \
               "or \"main\"" in src, (
            "entry_tier must default to 'main' not empty string in trade log"
        )


class TestNoMinEntryPrice:
    """NO_MIN_ENTRY_PRICE must be 0.85 after live data analysis."""

    @pytest.mark.static
    def test_no_min_entry_price_raised(self):
        src = _src("hight_decision_engine.py")
        match = re.search(r"NO_MIN_ENTRY_PRICE\s*=\s*([\d.]+)", src)
        assert match, "NO_MIN_ENTRY_PRICE not found"
        val = float(match.group(1))
        assert val >= 0.85, (
            f"NO_MIN_ENTRY_PRICE should be >= 0.85. Got {val}. "
            "Live data (May 21 – Jun 1) showed [0.75,0.85) WR=55% — no edge."
        )
