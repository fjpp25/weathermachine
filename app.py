"""
app.py
------
WeatherMachine desktop application — PyQt6 UI for the Kalshi temperature trading system.

Tabs:
  Home   — Start/stop scheduler, live city status, open positions, countdown, balance
  PnL    — Daily summary table, equity curve, score breakdown

Requirements:
  pip install PyQt6 pyqtgraph

Run:
  python app.py
"""

import sys
import os
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from cities import TRADING_CITIES as _CITY_REGISTRY, SERIES_TO_CITY as _SERIES_TO_CITY, CITIES_WEST_TO_EAST as _CITIES_ORDERED, CITIES as _ALL_CITIES

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout, QLabel,
    QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QFrame, QScrollArea, QSizePolicy,
    QTextEdit, QSplitter, QMessageBox, QLineEdit,
    QFileDialog, QDialog, QDialogButtonBox, QCheckBox,
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QObject, QMetaObject, Q_ARG, pyqtSlot
)
from PyQt6.QtGui import QFont, QColor, QPalette, QIcon

try:
    import pyqtgraph as pg
    HAS_PYQTGRAPH = True
except ImportError:
    HAS_PYQTGRAPH = False

# Pre-import trading modules so they're ready before any button is clicked
import trader as _trader_preload

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR    = Path("data")
CONFIG_FILE = DATA_DIR / "config.json"

def _fmt_bracket(bracket: str, market_type: str = "HIGH") -> str:
    """
    Convert raw bracket code to a readable label.
    B50.5 → '50–52°'  (between, next bracket inferred from Kalshi 2° spacing)
    T55   → '>55°'    (above threshold for HIGH)
    T31   → '<31°'    (below threshold for LOWT)
    """
    if not bracket:
        return bracket
    try:
        if bracket.startswith("B"):
            floor = float(bracket[1:])
            cap   = round(floor + 2, 1)
            return f"{floor:.0f}–{cap:.0f}°"
        elif bracket.startswith("T"):
            val = float(bracket[1:])
            if market_type == "LOW":
                return f"<{val:.0f}°"
            else:
                return f">{val:.0f}°"
    except ValueError:
        pass
    return bracket


def _city_from_ticker(ticker: str, bare: bool = False) -> str | None:
    """Extract city name from a Kalshi temperature ticker.
    bare=True returns just the city name; bare=False appends (HIGH) or (LOW).
    """
    prefix = ticker.split("-")[0]
    city = _SERIES_TO_CITY.get(prefix)
    if city:
        if bare:
            return city
        mtype = "HIGH" if "HIGH" in prefix else "LOW"
        return f"{city} ({mtype})"
    return None


BG_DARK     = "#0d0f14"
BG_PANEL    = "#141720"
BG_ROW_ALT = "#181c24"
ACCENT      = "#00d4a0"       # teal green
ACCENT_DIM  = "#00896a"
RED         = "#ff4d6a"
YELLOW      = "#f5c842"
TEXT_PRI    = "#e8eaf0"
TEXT_SEC    = "#7a8099"
BORDER      = "#232736"

DEFAULT_FONT_SIZE = 13

def build_stylesheet(font_size: int = DEFAULT_FONT_SIZE) -> str:
    return f"""
QMainWindow, QWidget {{
    background-color: {BG_DARK};
    color: {TEXT_PRI};
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: {font_size}px;
}}
QTabWidget::pane {{
    border: 1px solid {BORDER};
    background: {BG_PANEL};
}}
QTabBar::tab {{
    background: {BG_DARK};
    color: {TEXT_SEC};
    padding: 10px 28px;
    border: 1px solid {BORDER};
    border-bottom: none;
    font-size: {font_size}px;
    letter-spacing: 1px;
}}
QTabBar::tab:selected {{
    background: {BG_PANEL};
    color: {ACCENT};
    border-top: 2px solid {ACCENT};
}}
QTableWidget {{
    background: {BG_PANEL};
    gridline-color: {BORDER};
    border: none;
    selection-background-color: #1e2535;
}}
QTableWidget::item {{
    padding: 6px 10px;
    border: none;
}}
QHeaderView::section {{
    background: {BG_DARK};
    color: {TEXT_SEC};
    padding: 8px 10px;
    border: none;
    border-bottom: 1px solid {BORDER};
    font-size: {max(font_size - 2, 9)}px;
    letter-spacing: 1px;
    text-transform: uppercase;
}}
QScrollBar:vertical {{
    background: {BG_DARK};
    width: 6px;
}}
QScrollBar::handle:vertical {{
    background: {BORDER};
    border-radius: 3px;
}}
QTextEdit {{
    background: {BG_DARK};
    color: {TEXT_SEC};
    border: 1px solid {BORDER};
    font-family: 'Consolas', monospace;
    font-size: {max(font_size - 2, 9)}px;
}}
QFrame[frameShape="4"], QFrame[frameShape="5"] {{
    color: {BORDER};
}}
"""

STYLESHEET = build_stylesheet(DEFAULT_FONT_SIZE)


# ---------------------------------------------------------------------------
# Config helpers — persist credentials in data/config.json
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Load saved credentials from config file."""
    DATA_DIR.mkdir(exist_ok=True)
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_config(config: dict):
    """Save credentials to config file."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def apply_config(config: dict):
    """Apply config values to environment variables."""
    if config.get("key_id"):
        os.environ["KALSHI_KEY_ID"] = config["key_id"]
    if config.get("key_file"):
        os.environ["KALSHI_KEY_FILE"] = config["key_file"]
    # demo=false means live trading
    os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"


# ---------------------------------------------------------------------------
# Credential setup dialog
# ---------------------------------------------------------------------------

class CredentialDialog(QDialog):
    """
    Shown on first launch or when credentials are missing.
    Collects Key ID, PEM file path, and live/demo mode.
    Saves to data/config.json for future launches.
    """
    def __init__(self, config: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("WeatherMachine — Setup")
        self.setMinimumWidth(520)
        self.setStyleSheet(f"""
            QDialog {{
                background: {BG_PANEL};
            }}
            QLabel {{
                color: {TEXT_PRI};
            }}
            QLineEdit {{
                background: {BG_DARK};
                color: {TEXT_PRI};
                border: 1px solid {BORDER};
                border-radius: 4px;
                padding: 8px;
                font-family: 'Consolas', monospace;
                font-size: 12px;
            }}
            QLineEdit:focus {{
                border-color: {ACCENT};
            }}
            QPushButton {{
                background: {BG_DARK};
                color: {TEXT_PRI};
                border: 1px solid {BORDER};
                border-radius: 4px;
                padding: 6px 16px;
                min-width: 80px;
            }}
            QPushButton:hover {{
                border-color: {ACCENT};
                color: {ACCENT};
            }}
            QCheckBox {{
                color: {TEXT_PRI};
                spacing: 8px;
            }}
            QCheckBox::indicator {{
                width: 16px;
                height: 16px;
                border: 1px solid {BORDER};
                border-radius: 3px;
                background: {BG_DARK};
            }}
            QCheckBox::indicator:checked {{
                background: {ACCENT};
                border-color: {ACCENT};
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 28, 32, 24)
        layout.setSpacing(20)

        # Title
        title = QLabel("Kalshi API Credentials")
        title.setStyleSheet(f"color: {TEXT_PRI}; font-size: 18px; font-weight: bold;")
        subtitle = QLabel(
            "Your credentials are stored locally in data/config.json\n"
            "and never transmitted anywhere except to Kalshi's API."
        )
        subtitle.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")
        layout.addWidget(title)
        layout.addWidget(subtitle)

        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color: {BORDER};")
        layout.addWidget(sep)

        # Key ID field
        layout.addWidget(self._field_label("API Key ID"))
        self.key_id_edit = QLineEdit(config.get("key_id", ""))
        self.key_id_edit.setPlaceholderText("e.g. 4a1f9cc9-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
        layout.addWidget(self.key_id_edit)

        # PEM file field
        layout.addWidget(self._field_label("Private Key File  (.pem)"))
        pem_row = QHBoxLayout()
        self.pem_edit = QLineEdit(config.get("key_file", ""))
        self.pem_edit.setPlaceholderText("Path to your kalshi_private_key.pem file")
        browse_btn = QPushButton("Browse...")
        browse_btn.setFixedWidth(90)
        browse_btn.clicked.connect(self._browse_pem)
        pem_row.addWidget(self.pem_edit)
        pem_row.addWidget(browse_btn)
        layout.addLayout(pem_row)

        # Live mode toggle
        self.live_check = QCheckBox("Enable live trading  (uncheck for demo/paper mode)")
        self.live_check.setChecked(config.get("live_mode", False))
        self.live_check.setStyleSheet(
            self.live_check.styleSheet() +
            f"QCheckBox {{ color: {YELLOW}; }}"
        )
        layout.addWidget(self.live_check)

        # Warning
        self.warning = QLabel("⚠  Live mode will use real money.")
        self.warning.setStyleSheet(f"color: {YELLOW}; font-size: 11px;")
        self.warning.setVisible(self.live_check.isChecked())
        self.live_check.toggled.connect(self.warning.setVisible)
        layout.addWidget(self.warning)

        # Separator
        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.HLine)
        sep2.setStyleSheet(f"color: {BORDER};")
        layout.addWidget(sep2)

        # Appearance — font size
        layout.addWidget(self._field_label("Appearance  —  Font Size"))
        font_row = QHBoxLayout()
        self.font_size_label = QLabel(f"{config.get('font_size', DEFAULT_FONT_SIZE)}px")
        self.font_size_label.setStyleSheet(f"color: {ACCENT}; font-size: 13px; min-width: 36px;")

        from PyQt6.QtWidgets import QSlider
        self.font_slider = QSlider(Qt.Orientation.Horizontal)
        self.font_slider.setMinimum(10)
        self.font_slider.setMaximum(18)
        self.font_slider.setValue(config.get("font_size", DEFAULT_FONT_SIZE))
        self.font_slider.setTickInterval(1)
        self.font_slider.setStyleSheet(f"""
            QSlider::groove:horizontal {{
                background: {BORDER}; height: 4px; border-radius: 2px;
            }}
            QSlider::handle:horizontal {{
                background: {ACCENT}; width: 14px; height: 14px;
                margin: -5px 0; border-radius: 7px;
            }}
            QSlider::sub-page:horizontal {{ background: {ACCENT}; border-radius: 2px; }}
        """)
        self.font_slider.valueChanged.connect(
            lambda v: self.font_size_label.setText(f"{v}px")
        )

        small_lbl = QLabel("A")
        small_lbl.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px;")
        large_lbl = QLabel("A")
        large_lbl.setStyleSheet(f"color: {TEXT_SEC}; font-size: 16px;")

        font_row.addWidget(small_lbl)
        font_row.addWidget(self.font_slider, stretch=1)
        font_row.addWidget(large_lbl)
        font_row.addSpacing(8)
        font_row.addWidget(self.font_size_label)
        layout.addLayout(font_row)

        # Buttons
        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok |
            QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        # Style the OK button as accent
        ok_btn = btns.button(QDialogButtonBox.StandardButton.Ok)
        ok_btn.setStyleSheet(f"""
            QPushButton {{
                background: {ACCENT};
                color: {BG_DARK};
                border: none;
                border-radius: 4px;
                padding: 6px 20px;
                font-weight: bold;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; }}
        """)
        layout.addWidget(btns)

    def _field_label(self, text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 1px;")
        return lbl

    def _browse_pem(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Private Key File", "",
            "PEM Files (*.pem);;All Files (*)"
        )
        if path:
            self.pem_edit.setText(path)

    def _on_accept(self):
        key_id   = self.key_id_edit.text().strip()
        key_file = self.pem_edit.text().strip()

        if not key_id:
            QMessageBox.warning(self, "Missing Field", "Please enter your API Key ID.")
            return
        if not key_file:
            QMessageBox.warning(self, "Missing Field", "Please select your private key file.")
            return
        if not Path(key_file).exists():
            QMessageBox.warning(self, "File Not Found",
                                f"Private key file not found:\n{key_file}")
            return

        config = {
            "key_id":    key_id,
            "key_file":  key_file,
            "live_mode": self.live_check.isChecked(),
            "font_size": self.font_slider.value(),
        }
        save_config(config)
        apply_config(config)
        self.accept()

    def get_config(self) -> dict:
        return {
            "key_id":    self.key_id_edit.text().strip(),
            "key_file":  self.pem_edit.text().strip(),
            "live_mode": self.live_check.isChecked(),
            "font_size": self.font_slider.value(),
        }


# ---------------------------------------------------------------------------
# Generic background worker — uses Qt signals for thread-safe UI updates
# ---------------------------------------------------------------------------

class BackgroundWorker(QObject):
    """
    Runs a callable in a QThread and emits result/error back to the main thread.
    Safer than threading.Thread + QTimer.singleShot on Windows.
    """
    finished = pyqtSignal(object)   # emits result
    errored  = pyqtSignal(str)      # emits error message

    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self._fn     = fn
        self._args   = args
        self._kwargs = kwargs

    def run(self):
        try:
            result = self._fn(*self._args, **self._kwargs)
            self.finished.emit(result)
        except Exception as e:
            import traceback
            self.errored.emit(f"{e}\n{traceback.format_exc()}")


# Module-level set to keep thread references alive until they finish
_active_threads: set = set()


def run_in_background(fn, on_done, on_error=None, *args, **kwargs):
    """
    Helper: run fn(*args, **kwargs) in a QThread,
    call on_done(result) on success, on_error(msg) on failure.
    Thread is kept alive in _active_threads until it finishes.
    """
    worker = BackgroundWorker(fn, *args, **kwargs)
    thread = QThread()
    worker.moveToThread(thread)
    thread.started.connect(worker.run)
    worker.finished.connect(on_done)
    worker.finished.connect(lambda _: thread.quit())
    worker.errored.connect(on_error or (lambda e: None))
    worker.errored.connect(lambda _: thread.quit())

    # Keep reference alive until thread finishes, then clean up
    _active_threads.add(thread)
    thread.finished.connect(lambda: _active_threads.discard(thread))

    thread.start()
    return thread, worker


# ---------------------------------------------------------------------------
# Scheduler worker thread
# ---------------------------------------------------------------------------

class SchedulerWorker(QObject):
    """
    Runs the scheduler loop in a background thread.
    Emits signals to update the UI safely from the main thread.
    """
    log_line          = pyqtSignal(str)
    poll_started      = pyqtSignal(int)
    poll_finished     = pyqtSignal(int, int)
    positions_updated = pyqtSignal()
    balance_updated   = pyqtSignal(float, float)
    client_ready      = pyqtSignal(object)
    session_entry     = pyqtSignal(dict)   # emitted each time an order is placed
    session_exit      = pyqtSignal(str, str) # ticker, reason ("Stopped Out" | "Settled")
    stopped           = pyqtSignal()

    def __init__(self, paper: bool = False):
        super().__init__()
        self.paper      = paper
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def is_running(self):
        return not self._stop_event.is_set()

    def sleep_interruptible(self, seconds: int):
        """Sleep for up to `seconds`, waking immediately if stop() is called."""
        self._stop_event.wait(timeout=seconds)

    def run(self):
        self._stop_event.clear()

        try:
            import trader
            import hight_decision_engine

            client = trader.make_client(skip_confirmation=True)
            self.client_ready.emit(client)
            self.log_line.emit("  Scheduler started.")

            def dynamic_interval():
                # Mirrors scheduler.py logic — uses _ALL_CITIES from cities.py
                min_secs = 10 * 60  # default 10 min overnight
                for meta in _ALL_CITIES.values():
                    tz = meta.get("tz")
                    if not tz:
                        continue
                    h = datetime.now(ZoneInfo(tz)).hour
                    if 11 <= h < 13:
                        min_secs = min(min_secs, 3 * 60)
                    elif h in (9, 10, 13, 14):
                        min_secs = min(min_secs, 5 * 60)
                return min_secs

            poll_count = 0

            while self.is_running():

                interval_secs = dynamic_interval()
                poll_count += 1
                self.poll_started.emit(poll_count)

                now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
                self.log_line.emit(f"\n[{now_str}] Poll #{poll_count}  ({interval_secs//60} min interval)")

                # ── Single sync fetch shared across pipeline + exit check ──────
                try:
                    live_pos = trader.sync_from_kalshi(client)
                except Exception as e:
                    self.log_line.emit(f"Sync error: {e}")
                    live_pos = []

                tickers_before = {
                    p["ticker"] for p in live_pos
                    if "HIGH" in p["ticker"] or "LOWT" in p["ticker"]
                }

                # Run pipeline
                try:
                    evaluations = trader.run_pipeline(client=client, paper=self.paper)
                    # Reset scores each poll so stale entries never mask missing scores
                    self._last_scores = {}
                    for ev in (evaluations or []):
                        for sig in ev.get("signals", []):
                            t = sig.get("ticker")
                            s = sig.get("score")
                            if t and s is not None:
                                self._last_scores[t] = s
                    self.positions_updated.emit()
                except Exception as e:
                    self.log_line.emit(f"Pipeline error: {e}")

                if not self.is_running():
                    break

                # Check exits — pass pre-fetched positions to avoid redundant sync
                try:
                    if live_pos:
                        self.log_line.emit(f"Checking exits ({len(live_pos)} open)...")
                    exited = trader.check_exits(
                        client         = client,
                        paper          = self.paper,
                        live_positions = live_pos,
                    )
                    # Detect newly entered positions (post-pipeline) and emit for session tab
                    try:
                        live_pos_after = trader.sync_from_kalshi(client)
                        tickers_after  = {
                            p["ticker"] for p in live_pos_after
                            if "HIGH" in p["ticker"] or "LOWT" in p["ticker"]
                        }
                        for pos in live_pos_after:
                            t = pos["ticker"]
                            if t not in tickers_before and ("HIGH" in t or "LOWT" in t):
                                score = self._last_scores.get(t, "?")
                                self.session_entry.emit({
                                    **pos,
                                    "entered_at": datetime.now(timezone.utc).strftime("%H:%M UTC"),
                                    "score": score,
                                })
                        # Detect closed positions
                        for closed_ticker in tickers_before - tickers_after:
                            reason = exited.get(closed_ticker, "Settled")
                            self.session_exit.emit(closed_ticker, reason)
                    except Exception as e:
                        self.log_line.emit(f"Post-pipeline sync error: {e}")
                    self.positions_updated.emit()
                except Exception as e:
                    self.log_line.emit(f"Exit check error: {e}")

                # Update balance
                try:
                    bal = trader.get_balance(client)
                    self.balance_updated.emit(bal, bal * 0.70)
                except Exception:
                    pass

                self.poll_finished.emit(poll_count, interval_secs)

                # Sleep — wakes immediately if stop() is called
                self.sleep_interruptible(interval_secs)

        except Exception as e:
            self.log_line.emit(f"Scheduler error: {e}")
        finally:
            self.stopped.emit()


# ---------------------------------------------------------------------------
# City status card
# ---------------------------------------------------------------------------

class CityCard(QFrame):
    clicked = pyqtSignal(str)  # emits city name when clicked

    def __init__(self, city: str):
        super().__init__()
        self.city = city
        self.setFixedSize(200, 82)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setStyleSheet(f"""
            QFrame {{
                background: {BG_PANEL};
                border: 1px solid {BORDER};
                border-radius: 6px;
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 5, 10, 5)
        layout.setSpacing(1)

        self.name_label = QLabel(city)
        self.name_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px; letter-spacing: 1px;")

        self.time_label = QLabel("--:-- --")
        self.time_label.setStyleSheet(f"color: {TEXT_PRI}; font-size: 12px; font-weight: bold;")

        self.hi_label = QLabel("hi: --°  fcst: --°")
        self.hi_label.setStyleSheet(f"color: {ACCENT}; font-size: 11px;")

        self.lo_label = QLabel("lo: --°  fcst: --°")
        self.lo_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

        self.status_label = QLabel("—")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

        layout.addWidget(self.name_label)
        layout.addWidget(self.time_label)
        layout.addWidget(self.hi_label)
        layout.addWidget(self.lo_label)
        layout.addWidget(self.status_label)

    def mousePressEvent(self, event):
        self.clicked.emit(self.city)
        super().mousePressEvent(event)

    def update_data(self, local_time: str, curr: float, obs_hi: float,
                    fcst_hi: float, active: bool,
                    obs_lo: float = None, fcst_lo: float = None,
                    local_hour: int = None):
        self.time_label.setText(local_time)

        hi_str  = f"{obs_hi:.0f}°"  if obs_hi  else "--°"
        lo_str  = f"{obs_lo:.0f}°"  if obs_lo  else "--°"
        fhi_str = f"{fcst_hi:.0f}°" if fcst_hi else "--°"
        flo_str = f"{fcst_lo:.0f}°" if fcst_lo else "--°"

        self.hi_label.setText(f"hi: {hi_str}  fcst: {fhi_str}")
        self.lo_label.setText(f"lo: {lo_str}  fcst: {flo_str}")

        # Determine HIGH and LOWT activity windows from observer data:
        # HIGH: 9am–3pm local (temperature rising toward peak)
        # LOWT: midnight–8am local (overnight low being recorded)
        if local_hour is not None:
            high_active = 9 <= local_hour < 15
            lowt_active = local_hour < 8 or local_hour >= 22

            parts  = []
            colors = []
            if high_active:
                parts.append("HIGH ▲")
                colors.append(ACCENT)
            if lowt_active:
                parts.append("LOWT ▼")
                colors.append("#5599ff")   # blue tint for low temp

            if parts:
                status_str = "  ".join(parts)
                # Use the more prominent color
                status_color = ACCENT if high_active else "#5599ff"
            else:
                status_str   = "between windows"
                status_color = TEXT_SEC

            self.status_label.setText(status_str)
            self.status_label.setStyleSheet(f"color: {status_color}; font-size: 11px;")
            border_color = ACCENT_DIM if (high_active or lowt_active) else BORDER
        else:
            self.status_label.setText("—")
            self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")
            border_color = BORDER

        self.setStyleSheet(f"""
            QFrame {{
                background: {BG_PANEL};
                border: 1px solid {border_color};
                border-radius: 6px;
            }}
        """)


# ---------------------------------------------------------------------------
# City detail dialog — shown when clicking a city card
# ---------------------------------------------------------------------------

class CityDetailDialog(QDialog):
    def __init__(self, city: str, positions: list, client, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"{city} — Detail")
        self.setMinimumWidth(620)
        self.setMinimumHeight(460)
        self.setStyleSheet(f"""
            QDialog {{ background: {BG_DARK}; color: {TEXT_PRI}; }}
            QLabel  {{ color: {TEXT_PRI}; }}
            QTableWidget {{
                background: {BG_PANEL};
                color: {TEXT_PRI};
                gridline-color: {BORDER};
                border: 1px solid {BORDER};
            }}
            QHeaderView::section {{
                background: {BG_DARK};
                color: {TEXT_SEC};
                border: none;
                padding: 4px;
                font-size: 11px;
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(14)

        # Title
        title = QLabel(f"⛅  {city}")
        title.setStyleSheet(f"color: {ACCENT}; font-size: 16px; font-weight: bold;")
        layout.addWidget(title)

        # ── Open positions ────────────────────────────────────────────────
        pos_hdr = QLabel("OPEN POSITIONS")
        pos_hdr.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        layout.addWidget(pos_hdr)

        city_positions = [
            p for p in positions
            if _city_from_ticker(p.get("ticker", ""), bare=True) == city
        ]

        if city_positions:
            pos_table = QTableWidget()
            pos_table.setColumnCount(5)
            pos_table.setHorizontalHeaderLabels(["Market", "Side", "Qty", "Avg Cost", "Unreal. PnL"])
            pos_table.setRowCount(len(city_positions))
            pos_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
            pos_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
            pos_table.horizontalHeader().setStretchLastSection(True)
            pos_table.verticalHeader().setVisible(False)
            for col, w in enumerate([190, 75, 65, 100, 70]):
                pos_table.setColumnWidth(col, w)

            for row, pos in enumerate(city_positions):
                ticker  = pos.get("ticker", "")
                side    = pos.get("side", "").upper()
                qty     = pos.get("contracts", 1)
                cost    = pos.get("avg_cost", 0)
                unreal  = pos.get("unrealised_pnl", 0)
                sign    = "+" if unreal >= 0 else ""
                bracket = ticker.split("-")[-1] if "-" in ticker else ticker
                mtype   = "HIGH" if "HIGH" in ticker else "LOW"
                label   = f"{mtype} {_fmt_bracket(bracket, mtype)}"
                for col, (val, color) in enumerate([
                    (label,                       TEXT_PRI),
                    (side,                        ACCENT if side == "NO" else YELLOW),
                    (str(qty),                    TEXT_PRI),
                    (f"${cost:.2f}",              TEXT_PRI),
                    (f"{sign}${unreal:.2f}",      ACCENT if unreal >= 0 else RED),
                ]):
                    item = QTableWidgetItem(val)
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    item.setForeground(QColor(color))
                    pos_table.setItem(row, col, item)

            pos_table.setFixedHeight(min(len(city_positions) * 30 + 34, 180))
            layout.addWidget(pos_table)
        else:
            no_pos = QLabel("No open positions for this city.")
            no_pos.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
            layout.addWidget(no_pos)

        # ── Historical stats ──────────────────────────────────────────────
        stats_hdr = QLabel("HISTORICAL PERFORMANCE")
        stats_hdr.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        layout.addWidget(stats_hdr)

        self.stats_label = QLabel("Loading...")
        self.stats_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
        layout.addWidget(self.stats_label)

        self.hist_tabs = QTabWidget()
        self.hist_tabs.setStyleSheet(
            "QTabBar::tab { padding: 5px 16px; font-size: 11px; }"
        )
        self.hist_table_all  = self._make_hist_table()
        self.hist_table_high = self._make_hist_table()
        self.hist_table_low  = self._make_hist_table()
        self.hist_tabs.addTab(self.hist_table_all,  "All")
        self.hist_tabs.addTab(self.hist_table_high, "High")
        self.hist_tabs.addTab(self.hist_table_low,  "Low")
        layout.addWidget(self.hist_tabs, stretch=1)

        # Close button
        close_btn = QPushButton("Close")
        close_btn.setFixedWidth(80)
        close_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {TEXT_SEC};
                border: 1px solid {BORDER};
                border-radius: 4px;
                padding: 6px 12px;
            }}
            QPushButton:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}
        """)
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)

        # Fetch historical stats in background if client available
        if client:
            self._city = city
            self._client = client
            threading.Thread(target=self._load_stats, daemon=True).start()
        else:
            self.stats_label.setText("No client connected — start trading to see stats.")

    def _make_hist_table(self) -> QTableWidget:
        t = QTableWidget()
        t.setColumnCount(5)
        t.setHorizontalHeaderLabels(["Date", "Bracket", "Side", "Result", "PnL"])
        t.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        t.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        t.setAlternatingRowColors(True)
        t.horizontalHeader().setStretchLastSection(True)
        t.verticalHeader().setVisible(False)
        t.setStyleSheet(f"QTableWidget {{ alternate-background-color: {BG_ROW_ALT}; }}")
        for col, w in enumerate([120, 175, 70, 95, 70]):
            t.setColumnWidth(col, w)
        return t

    def _fill_hist_table(self, table: QTableWidget, rows: list):
        """Populate a single history table from a list of enriched trade dicts."""
        sorted_rows = sorted(rows, key=lambda x: x["date"], reverse=True)
        table.setRowCount(len(sorted_rows))
        for row, e in enumerate(sorted_rows):
            won = e["won"]
            for col, (val, color) in enumerate([
                (e["date"],                TEXT_SEC),
                (e["bracket"],             TEXT_PRI),
                (e["side"],                ACCENT if e["side"] == "NO" else YELLOW),
                ("Win" if won else "Loss", ACCENT if won else RED),
                (f"${e['net_pnl']:+.2f}", ACCENT if won else RED),
            ]):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item.setForeground(QColor(color))
                table.setItem(row, col, item)

    def _load_stats(self):
        """Fetch settlements + fills for this city and compute stats."""
        try:
            # Fetch all settlements
            all_settlements = []
            cursor = None
            for _ in range(10):
                params = {"limit": 200, "settlement_status": "settled"}
                if cursor:
                    params["cursor"] = cursor
                data  = self._client.get("portfolio/settlements", params=params)
                batch = data.get("settlements", [])
                all_settlements.extend(batch)
                cursor = data.get("cursor")
                if not cursor or len(batch) < 200:
                    break

            # Filter to this city's temperature markets
            city_settlements = [
                s for s in all_settlements
                if _city_from_ticker(s.get("ticker", ""), bare=True) == self._city
                and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
            ]

            if not city_settlements:
                QMetaObject.invokeMethod(
                    self, "_show_stats",
                    Qt.ConnectionType.QueuedConnection,
                    Q_ARG("PyQt_PyObject", []),
                )
                return

            # Fetch fills for these tickers
            tickers = list({s.get("ticker") for s in city_settlements})
            all_fills = []
            cursor = None
            for _ in range(10):
                params = {"limit": 200}
                if cursor:
                    params["cursor"] = cursor
                data  = self._client.get("portfolio/fills", params=params)
                batch = data.get("fills", [])
                all_fills.extend(batch)
                cursor = data.get("cursor")
                if not cursor or len(batch) < 200:
                    break

            fills_by_ticker = {}
            for f in all_fills:
                t = f.get("ticker", "")
                if t in tickers:
                    fills_by_ticker.setdefault(t, []).append(f)

            # Enrich settlements using same logic as PnL tab
            enriched = []
            for s in city_settlements:
                ticker = s.get("ticker", "")
                result = s.get("market_result", "").lower()
                fee    = float(s.get("fee_cost") or 0)
                date   = s.get("settled_time", "")[:10]

                buy_fills = [f for f in fills_by_ticker.get(ticker, [])
                             if f.get("action") == "buy"]
                if not buy_fills:
                    continue

                sides    = [f.get("side") for f in buy_fills]
                our_side = max(set(sides), key=sides.count)
                our_fills = [f for f in buy_fills if f.get("side") == our_side]
                contracts = int(sum(float(f.get("count_fp") or 0) for f in our_fills))
                cost = round(sum(
                    (float(f.get("yes_price_dollars") or 0) if our_side == "yes"
                     else (1.0 - float(f.get("yes_price_dollars") or 0)))
                    * float(f.get("count_fp") or 0)
                    for f in our_fills
                ), 4)

                if contracts == 0 or cost == 0:
                    continue

                won     = (result == our_side)
                net_pnl = round(contracts * 1.0 - cost - fee, 4) if won else round(-cost - fee, 4)
                bracket = ticker.split("-")[-1] if "-" in ticker else ticker
                mtype   = "HIGH" if "HIGH" in ticker else "LOW"

                enriched.append({
                    "date":    date,
                    "bracket": f"{mtype} {_fmt_bracket(bracket, mtype)}",
                    "side":    our_side.upper(),
                    "won":     won,
                    "net_pnl": net_pnl,
                    "mtype":   mtype,
                })

            QMetaObject.invokeMethod(
                self, "_show_stats",
                Qt.ConnectionType.QueuedConnection,
                Q_ARG("PyQt_PyObject", enriched),
            )
        except Exception as e:
            QMetaObject.invokeMethod(
                self, "_show_stats",
                Qt.ConnectionType.QueuedConnection,
                Q_ARG("PyQt_PyObject", []),
            )

    @pyqtSlot(object)
    def _show_stats(self, enriched):
        if not enriched:
            self.stats_label.setText("No settled trades for this city yet.")
            return

        total   = len(enriched)
        wins    = sum(1 for e in enriched if e["won"])
        win_pct = round(wins / total * 100)
        net_pnl = sum(e["net_pnl"] for e in enriched)

        self.stats_label.setText(
            f"{total} settled trades  ·  {wins} wins  ·  {win_pct}% win rate  ·  Net PnL: ${net_pnl:+.2f}"
        )
        self.stats_label.setStyleSheet(
            f"color: {ACCENT if net_pnl >= 0 else RED}; font-size: 12px; font-weight: bold;"
        )

        high_rows = [e for e in enriched if e.get("mtype") == "HIGH"]
        low_rows  = [e for e in enriched if e.get("mtype") == "LOW"]

        self._fill_hist_table(self.hist_table_all,  enriched)
        self._fill_hist_table(self.hist_table_high, high_rows)
        self._fill_hist_table(self.hist_table_low,  low_rows)

        # Update tab labels with counts so it's clear at a glance
        self.hist_tabs.setTabText(0, f"All ({total})")
        self.hist_tabs.setTabText(1, f"High ({len(high_rows)})")
        self.hist_tabs.setTabText(2, f"Low ({len(low_rows)})")


# ---------------------------------------------------------------------------
# Home tab
# ---------------------------------------------------------------------------

class HomeTab(QWidget):
    log_line  = pyqtSignal(str)    # emitted on every log append → LogTab listens

    def __init__(self):
        super().__init__()
        self._worker       = None
        self._thread       = None
        self._running      = False
        self._client       = None
        self._next_poll_ts = None
        self._last_balance = 0.0

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(16)

        # ── Top bar: controls + balance ──────────────────────────────────
        top_bar = QHBoxLayout()

        self.mode_label = QLabel("LIVE" if os.environ.get("KALSHI_DEMO", "true") == "false" else "DEMO")
        live = os.environ.get("KALSHI_DEMO", "true") == "false"
        _mode_color = YELLOW if live else ACCENT
        self.mode_label.setStyleSheet(f"""
            color: {_mode_color}; font-size: 11px; letter-spacing: 2px;
            padding: 4px 10px;
            border: 1px solid {_mode_color};
            border-radius: 4px;
        """)

        self.status_label = QLabel("Idle")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")

        self.countdown_label = QLabel("")
        self.countdown_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")

        self.sync_btn = QPushButton("⟳  Sync")
        self.sync_btn.setFixedHeight(36)
        self.sync_btn.setFixedWidth(100)
        self.sync_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {TEXT_SEC};
                border: 1px solid {BORDER};
                border-radius: 6px;
                font-size: 13px;
            }}
            QPushButton:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}
        """)
        self.sync_btn.clicked.connect(self.sync_positions_from_kalshi)

        top_bar.addWidget(self.sync_btn)
        top_bar.addSpacing(12)
        top_bar.addWidget(self.mode_label)
        top_bar.addSpacing(16)
        top_bar.addWidget(self.status_label)
        top_bar.addStretch()
        top_bar.addWidget(self.countdown_label)

        # ── Balance bar ──────────────────────────────────────────────────
        bal_bar = QHBoxLayout()

        self.balance_label = QLabel("Balance  —")
        self.balance_label.setStyleSheet(f"color: {TEXT_PRI}; font-size: 20px; font-weight: bold;")

        self.deployable_label = QLabel("Deployable  —")
        self.deployable_label.setStyleSheet(f"color: {ACCENT}; font-size: 14px;")

        self.portfolio_label = QLabel("Portfolio  —")
        self.portfolio_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 14px;")

        self.pnl_label = QLabel("Unrealised  —")
        self.pnl_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 14px;")

        bal_bar.addWidget(self.balance_label)
        bal_bar.addSpacing(32)
        bal_bar.addWidget(self.deployable_label)
        bal_bar.addSpacing(32)
        bal_bar.addWidget(self.portfolio_label)
        bal_bar.addSpacing(32)
        bal_bar.addWidget(self.pnl_label)
        bal_bar.addStretch()

        # ── City grid ────────────────────────────────────────────────────
        city_label = QLabel("CITY STATUS")
        city_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")

        city_grid = QGridLayout()
        city_grid.setSpacing(10)
        self.city_cards = {}
        for i, city in enumerate(_CITIES_ORDERED):
            card = CityCard(city)
            card.clicked.connect(self._on_city_card_clicked)
            self.city_cards[city] = card
            city_grid.addWidget(card, i % 4, i // 4)

        # Positions table
        pos_frame = QFrame()
        pos_layout = QVBoxLayout(pos_frame)
        pos_layout.setContentsMargins(0, 0, 0, 0)
        pos_layout.setSpacing(6)

        pos_hdr = QLabel("OPEN POSITIONS")
        pos_hdr.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        pos_layout.addWidget(pos_hdr)

        self.pos_table = QTableWidget()
        self.pos_table.setColumnCount(8)
        self.pos_table.setHorizontalHeaderLabels(
            ["Market", "Side", "Qty", "Avg Cost", "Current", "Unreal. PnL", "Opened", "Status"]
        )
        hdr = self.pos_table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        hdr.setStretchLastSection(True)
        # Ticker, Side, Qty, Avg Cost, Current, Unreal PnL, Opened, Status
        for col, width in enumerate([240, 85, 75, 110, 110, 130, 155, 65]):
            self.pos_table.setColumnWidth(col, width)
        self.pos_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.pos_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.pos_table.setAlternatingRowColors(True)
        self.pos_table.setStyleSheet(
            self.pos_table.styleSheet() +
            f"QTableWidget {{ alternate-background-color: {BG_ROW_ALT}; }}"
        )
        pos_layout.addWidget(self.pos_table)

        # ── Assemble ─────────────────────────────────────────────────────
        main_layout.addLayout(top_bar)
        main_layout.addLayout(bal_bar)
        main_layout.addWidget(self._hline())
        main_layout.addWidget(city_label)
        main_layout.addLayout(city_grid)
        main_layout.addWidget(self._hline())
        main_layout.addWidget(pos_frame, stretch=1)

        # Timers
        self._city_timer = QTimer()
        self._city_timer.timeout.connect(self._refresh_cities)
        self._city_timer.start(30_000)
        self._refresh_cities()

        # NWS data fetch — every 5 minutes, fetch immediately on startup
        self._nws_timer = QTimer()
        self._nws_timer.timeout.connect(self._refresh_nws_data)
        self._nws_timer.start(300_000)
        QTimer.singleShot(500, self._refresh_nws_data)  # slight delay so UI renders first

        self._countdown_timer = QTimer()
        self._countdown_timer.timeout.connect(self._tick_countdown)
        self._countdown_timer.start(1_000)

        self._pos_timer = QTimer()
        self._pos_timer.timeout.connect(self.sync_positions_from_kalshi)
        self._pos_timer.start(15_000)

    def _hline(self):
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setStyleSheet(f"color: {BORDER};")
        return line

    def _on_city_card_clicked(self, city: str):
        """Open city detail dialog when a card is clicked."""
        positions = getattr(self, '_last_positions', [])
        dlg = CityDetailDialog(city, positions, self._client, parent=self)
        dlg.exec()

    def toggle_scheduler(self):
        if self._running:
            self._stop_scheduler()
        else:
            # Check if live mode — show confirmation dialog
            demo = os.environ.get("KALSHI_DEMO", "true").lower() != "false"
            if not demo:
                dlg = QMessageBox(self)
                dlg.setWindowTitle("Live Trading Confirmation")
                dlg.setText("⚠  LIVE TRADING MODE")
                dlg.setInformativeText(
                    "Real money will be deployed.\n\n"
                    "Your account balance and the 70% deployable cap apply.\n\n"
                    "Are you sure you want to start?"
                )
                dlg.setStandardButtons(
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
                )
                dlg.setDefaultButton(QMessageBox.StandardButton.Cancel)
                dlg.setStyleSheet(f"""
                    QMessageBox {{
                        background: {BG_PANEL};
                        color: {TEXT_PRI};
                    }}
                    QLabel {{ color: {TEXT_PRI}; }}
                    QPushButton {{
                        background: {BG_DARK};
                        color: {TEXT_PRI};
                        border: 1px solid {BORDER};
                        border-radius: 4px;
                        padding: 6px 16px;
                        min-width: 80px;
                    }}
                    QPushButton:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}
                """)
                if dlg.exec() != QMessageBox.StandardButton.Yes:
                    return
            self._start_scheduler()

    def _start_scheduler(self):
        self._running = True
        self._client  = None   # will be set once worker connects
        self.status_label.setText("Starting...")
        self.status_label.setStyleSheet(f"color: {ACCENT}; font-size: 12px;")

        self._thread = QThread()
        self._worker = SchedulerWorker(paper=False)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.log_line.connect(self.append_log)
        self._worker.poll_started.connect(self._on_poll_started)
        self._worker.poll_finished.connect(self._on_poll_finished)
        self._worker.client_ready.connect(self._on_client_ready)
        self._worker.positions_updated.connect(self.sync_positions_from_kalshi)
        self._worker.balance_updated.connect(self._on_balance_updated)
        self._worker.stopped.connect(self._on_worker_stopped)

        self._thread.start()

        # Notify registered start callbacks (e.g. session tab wiring)
        for cb in getattr(self, '_start_trading_callbacks', []):
            cb()

    def _stop_scheduler(self):
        if self._worker:
            self._worker.stop()
        self.status_label.setText("Stopping — finishing current operation...")
        self.status_label.setStyleSheet(f"color: {YELLOW}; font-size: 12px;")
        self.append_log("Stop requested — will finish current operation then exit.")

    def _on_worker_stopped(self):
        self._running = False
        self._thread.quit()
        self._thread.wait()
        self.status_label.setText("Idle")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
        self._next_poll_ts = None
        self.append_log("Scheduler stopped.")
        # Notify registered stop callbacks (e.g. global header button)
        for cb in getattr(self, '_stop_trading_callbacks', []):
            cb()

    def _on_client_ready(self, client):
        self._client = client
        # Notify any registered callbacks (e.g. PnL tab)
        for cb in getattr(self, '_client_ready_callbacks', []):
            cb(client)
        self.sync_positions_from_kalshi()

    def _on_poll_started(self, poll_num: int):
        self.status_label.setText(f"Poll #{poll_num} running...")
        self.status_label.setStyleSheet(f"color: {ACCENT}; font-size: 12px;")

    def _on_poll_finished(self, poll_num: int, next_secs: int):
        self._next_poll_ts = time.time() + next_secs
        self.status_label.setText(f"Sleeping — next poll in {next_secs//60} min")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
        self.sync_positions_from_kalshi()

    def _on_balance_updated(self, bal: float, dep: float):
        self._last_balance = bal
        self.balance_label.setText(f"Balance  ${bal:.2f}")
        self.deployable_label.setText(f"Deployable  ${dep:.2f}")

    def _tick_countdown(self):
        if self._next_poll_ts is None:
            self.countdown_label.setText("")
            return
        remaining = int(self._next_poll_ts - time.time())
        if remaining <= 0:
            self.countdown_label.setText("polling...")
        else:
            m, s = divmod(remaining, 60)
            self.countdown_label.setText(f"next poll in {m:02d}:{s:02d}")

    @pyqtSlot(str)
    def append_log(self, text: str):
        self.log_line.emit(text)

    def sync_positions_from_kalshi(self):
        """Fetch live positions and balance directly from Kalshi and update the UI."""
        if not hasattr(self, '_client') or self._client is None:
            return  # no client yet — wait for scheduler to connect

        self.sync_btn.setEnabled(False)
        self.sync_btn.setText("Syncing...")
        client = self._client

        def fetch():
            positions = _trader_preload.sync_from_kalshi(client)
            balance   = _trader_preload.get_balance(client)
            return positions, balance

        def on_done(result):
            positions, balance = result
            self._last_balance = balance
            self.balance_label.setText(f"Balance  ${balance:.2f}")
            self.deployable_label.setText(f"Deployable  ${balance * 0.70:.2f}")
            self._update_positions_table(positions)
            self.sync_btn.setEnabled(True)
            self.sync_btn.setText("⟳  Sync")

        def on_error(msg):
            self.append_log(f"Sync error: {msg}")
            self.sync_btn.setEnabled(True)
            self.sync_btn.setText("⟳  Sync")

        # Keep reference to prevent GC
        self._sync_thread, self._sync_worker = run_in_background(
            fetch, on_done, on_error
        )

    def _update_positions_table(self, positions: list):
        """Update the positions table from a list of enriched position dicts."""
        self._last_positions = positions  # cache for city detail dialog
        # Sort oldest first (chronological order)
        sorted_positions = sorted(
            positions,
            key=lambda p: p.get("last_updated", ""),
        )
        self.pos_table.setRowCount(len(sorted_positions))
        total_unrealised = 0.0

        for row, pos in enumerate(sorted_positions):
            ticker    = pos.get("ticker", "")
            side      = pos.get("side", "").upper()
            qty       = pos.get("contracts", 1)
            avg_cost  = pos.get("avg_cost", 0)
            current   = pos.get("current_price", 0)
            unreal    = pos.get("unrealised_pnl", 0)
            updated   = pos.get("last_updated", "")
            is_live   = pos.get("live", True)
            total_unrealised += unreal

            # Derive city name from ticker prefix
            city_display = _city_from_ticker(ticker) or ticker

            pnl_color    = ACCENT if unreal >= 0 else RED
            sign         = "+" if unreal >= 0 else ""
            status_str   = "Live" if is_live else "Settling"
            status_color = ACCENT if is_live else YELLOW

            items = [
                (city_display,          TEXT_PRI),
                (side,                  ACCENT if side == "NO" else YELLOW),
                (str(qty),              TEXT_PRI),
                (f"${avg_cost:.2f}",    TEXT_PRI),
                (f"${current:.2f}",     TEXT_PRI),
                (f"{sign}${unreal:.2f}", pnl_color),
                (updated,               TEXT_SEC),
                (status_str,            status_color),
            ]

            for col, (val, color) in enumerate(items):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item.setForeground(QColor(color))
                self.pos_table.setItem(row, col, item)

        # Update unrealised PnL and portfolio value
        total_current = sum(
            pos.get("current_price", 0) * pos.get("contracts", 1)
            for pos in sorted_positions
        )
        sign  = "+" if total_unrealised >= 0 else ""
        color = ACCENT if total_unrealised >= 0 else RED
        self.pnl_label.setText(f"Unrealised  {sign}${total_unrealised:.2f}")
        self.pnl_label.setStyleSheet(f"color: {color}; font-size: 14px;")

        # Portfolio = cash balance + current market value of open positions
        bal = getattr(self, '_last_balance', 0.0)
        portfolio = bal + total_current
        self.portfolio_label.setText(f"Portfolio  ${portfolio:.2f}")
        self.portfolio_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 14px;")

    def _refresh_cities(self):
        """Update city card times only — called every 30s."""
        for city in _CITIES_ORDERED:
            card = self.city_cards.get(city)
            if not card:
                continue
            tz = _ALL_CITIES[city]["tz"]
            now = datetime.now(ZoneInfo(tz))
            h    = now.hour
            active = True  # 24/7 window
            card.update_data(
                local_time  = now.strftime("%H:%M %Z"),
                curr        = None,
                obs_hi      = getattr(card, '_obs_hi', None),
                fcst_hi     = getattr(card, '_fcst_hi', None),
                obs_lo      = getattr(card, '_obs_lo', None),
                fcst_lo     = getattr(card, '_fcst_lo', None),
                active      = True,
                local_hour  = h,
            )

    def _refresh_nws_data(self):
        """Fetch NWS temperatures in background — called every 5 minutes."""
        import threading

        def fetch():
            try:
                import nws_feed
                results = nws_feed.snapshot()
                # Post back to main thread safely
                QMetaObject.invokeMethod(
                    self, "_on_nws_ready",
                    Qt.ConnectionType.QueuedConnection,
                    Q_ARG("PyQt_PyObject", results),
                )
            except Exception as e:
                QMetaObject.invokeMethod(
                    self, "append_log",
                    Qt.ConnectionType.QueuedConnection,
                    Q_ARG(str, f"NWS fetch error: {e}"),
                )

        threading.Thread(target=fetch, daemon=True).start()

    @pyqtSlot(object)
    def _on_nws_ready(self, results):
        """Called on main thread when NWS snapshot completes."""
        if "_error" in results:
            self.log_line.emit(f"NWS fetch error: {results['_error']}")
            return

        # Log a sample to confirm data is arriving
        sample = next(iter(results.values()), {})
        self.log_line.emit(
            f"NWS data received — "
            f"obs_hi={sample.get('observed_high_f')} "
            f"obs_lo={sample.get('observed_low_f')} "
            f"fcst_hi={sample.get('forecast_high_f')} "
            f"fcst_lo={sample.get('forecast_low_f')}"
        )

        for city in _CITIES_ORDERED:
            card = self.city_cards.get(city)
            if not card:
                continue
            tz = _ALL_CITIES[city]["tz"]
            now = datetime.now(ZoneInfo(tz))
            active = True  # 24/7 window
            data    = results.get(city, {})
            obs_hi  = data.get("observed_high_f")
            fcst_hi = data.get("forecast_high_f")
            obs_lo  = data.get("observed_low_f")
            fcst_lo = data.get("forecast_low_f")
            # Cache on card so clock ticks preserve the values
            card._obs_hi  = obs_hi
            card._fcst_hi = fcst_hi
            card._obs_lo  = obs_lo
            card._fcst_lo = fcst_lo
            card.update_data(
                local_time  = now.strftime("%H:%M %Z"),
                curr        = None,
                obs_hi      = obs_hi,
                fcst_hi     = fcst_hi,
                obs_lo      = obs_lo,
                fcst_lo     = fcst_lo,
                active      = True,
                local_hour  = now.hour,
            )



# ---------------------------------------------------------------------------
# Day detail dialog — shown when clicking a row in the By Day table
# ---------------------------------------------------------------------------

class DayDetailDialog(QDialog):
    """Shows all trades for a given day when a By Day row is clicked."""

    def __init__(self, date: str, trades: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Trades  —  {date}")
        self.setMinimumWidth(680)
        self.setMinimumHeight(360)
        self.setStyleSheet(f"""
            QDialog   {{ background: {BG_DARK}; color: {TEXT_PRI}; }}
            QLabel    {{ color: {TEXT_PRI}; }}
            QTableWidget {{
                background: {BG_PANEL}; color: {TEXT_PRI};
                gridline-color: {BORDER}; border: 1px solid {BORDER};
            }}
            QHeaderView::section {{
                background: {BG_DARK}; color: {TEXT_SEC};
                border: none; padding: 6px; font-size: 11px;
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(14)

        # ── Header ───────────────────────────────────────────────────────
        wins    = sum(1 for t in trades if t["won"])
        losses  = sum(1 for t in trades if not t["won"] and t.get("result") != "EARLY EXIT")
        stopped = sum(1 for t in trades if t.get("result") == "EARLY EXIT")
        net_pnl = sum(t["net_pnl"] for t in trades)
        sign    = "+" if net_pnl >= 0 else ""
        color   = ACCENT if net_pnl >= 0 else RED

        summary = QLabel(
            f"{len(trades)} trades  ·  {wins}W  {losses}L  {stopped} stopped  "
            f"·  Net PnL: {sign}${net_pnl:.2f}"
        )
        summary.setStyleSheet(f"color: {color}; font-size: 13px; font-weight: bold;")
        layout.addWidget(summary)

        # ── Trades table ─────────────────────────────────────────────────
        table = QTableWidget()
        table.setColumnCount(6)
        table.setHorizontalHeaderLabels(["Market", "Side", "Qty", "Result", "Fee", "Net PnL"])
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setAlternatingRowColors(True)
        table.setStyleSheet(
            f"QTableWidget {{ alternate-background-color: {BG_ROW_ALT}; }}"
        )
        table.horizontalHeader().setStretchLastSection(True)
        table.verticalHeader().setVisible(False)
        for col, w in enumerate([240, 80, 65, 100, 90, 100]):
            table.setColumnWidth(col, w)

        table.setRowCount(len(trades))
        for ri, e in enumerate(sorted(trades, key=lambda x: x.get("ticker", ""))):
            result = e.get("result", "")
            if result == "EARLY EXIT":
                result_str   = "EXIT ↩"
                result_color = YELLOW
            elif e["won"]:
                result_str   = "WON ✓"
                result_color = ACCENT
            else:
                result_str   = "LOST ✗"
                result_color = RED

            market = _city_from_ticker(e["ticker"]) or e["ticker"]
            side   = e.get("side", "")
            pnl    = e["net_pnl"]

            vals = [
                (market,                           TEXT_PRI),
                (side,                             ACCENT if side == "NO" else YELLOW),
                (str(e.get("contracts", 1)),       TEXT_PRI),
                (result_str,                       result_color),
                (f"${e.get('fee', 0):.2f}",       TEXT_SEC),
                (f"${pnl:+.2f}",                  ACCENT if pnl >= 0 else RED),
            ]
            for ci, (val, color) in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item.setForeground(QColor(color))
                table.setItem(ri, ci, item)

        layout.addWidget(table, stretch=1)

        # ── Close button ─────────────────────────────────────────────────
        close_btn = QPushButton("Close")
        close_btn.setFixedWidth(80)
        close_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL}; color: {TEXT_SEC};
                border: 1px solid {BORDER}; border-radius: 4px; padding: 6px 12px;
            }}
            QPushButton:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}
        """)
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)

# ---------------------------------------------------------------------------
# PnL tab
# ---------------------------------------------------------------------------

class PnLTab(QWidget):
    def __init__(self):
        super().__init__()
        self._client = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(16)

        # ── Header ───────────────────────────────────────────────────────
        hdr_row = QHBoxLayout()
        title = QLabel("PERFORMANCE  //  Temperature Markets")
        title.setStyleSheet(f"color: {TEXT_PRI}; font-size: 16px; font-weight: bold; letter-spacing: 1px;")

        self.refresh_btn = QPushButton("↻  Refresh")
        self.refresh_btn.setFixedWidth(120)
        self.refresh_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {ACCENT};
                border: 1px solid {ACCENT_DIM};
                border-radius: 4px;
                padding: 6px 12px;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; color: {BG_DARK}; }}
            QPushButton:disabled {{ border-color: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.refresh_btn.clicked.connect(self.load_data)

        self.status_label = QLabel("Connect to Kalshi to load performance data")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

        hdr_row.addWidget(title)
        hdr_row.addSpacing(16)
        hdr_row.addWidget(self.status_label)
        hdr_row.addStretch()
        hdr_row.addWidget(self.refresh_btn)
        layout.addLayout(hdr_row)

        # ── Summary stats row ─────────────────────────────────────────────
        stats_row = QHBoxLayout()
        self.stat_labels = {}
        for key in ["Settled Trades", "Win Rate", "Net PnL", "Total Fees", "Best Day", "Worst Day"]:
            frame = QFrame()
            frame.setStyleSheet(f"""
                QFrame {{
                    background: {BG_PANEL};
                    border: 1px solid {BORDER};
                    border-radius: 6px;
                }}
            """)
            fl = QVBoxLayout(frame)
            fl.setContentsMargins(16, 10, 16, 10)
            lbl_key = QLabel(key.upper())
            lbl_key.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px; letter-spacing: 1px;")
            lbl_val = QLabel("—")
            lbl_val.setStyleSheet(f"color: {TEXT_PRI}; font-size: 22px; font-weight: bold;")
            fl.addWidget(lbl_key)
            fl.addWidget(lbl_val)
            self.stat_labels[key] = lbl_val
            stats_row.addWidget(frame)
        layout.addLayout(stats_row)

        # ── Charts row — equity curve + win rate ──────────────────────────
        charts_row = QHBoxLayout()
        charts_row.setSpacing(12)

        if HAS_PYQTGRAPH:
            pg.setConfigOptions(antialias=True, background=BG_PANEL, foreground=TEXT_SEC)

            # Left: equity curve
            pnl_col = QVBoxLayout()
            pnl_col.setSpacing(4)
            curve_label = QLabel("EQUITY CURVE  —  Cumulative Net PnL")
            curve_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 20px; letter-spacing: 2px;")
            self.chart = pg.PlotWidget()
            self.chart.setMinimumHeight(150)
            self.chart.setMaximumHeight(200)
            self.chart.showGrid(x=False, y=True, alpha=0.15)
            self.chart.getAxis("left").setTextPen(TEXT_SEC)
            self.chart.getAxis("bottom").setTextPen(TEXT_SEC)
            self.chart.setLabel("left", "Net PnL ($)")
            self.chart.getPlotItem().hideAxis("top")
            self.chart.getPlotItem().hideAxis("right")
            pnl_col.addWidget(curve_label)
            pnl_col.addWidget(self.chart)

            # Right: rolling win rate
            wr_col = QVBoxLayout()
            wr_col.setSpacing(4)
            wr_label = QLabel("WIN RATE  —  Rolling 7-day %")
            wr_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 20px; letter-spacing: 2px;")
            self.wr_chart = pg.PlotWidget()
            self.wr_chart.setMinimumHeight(150)
            self.wr_chart.setMaximumHeight(200)
            self.wr_chart.showGrid(x=False, y=True, alpha=0.15)
            self.wr_chart.getAxis("left").setTextPen(TEXT_SEC)
            self.wr_chart.getAxis("bottom").setTextPen(TEXT_SEC)
            self.wr_chart.setLabel("left", "Win %")
            self.wr_chart.getPlotItem().hideAxis("top")
            self.wr_chart.getPlotItem().hideAxis("right")
            self.wr_chart.setYRange(0, 100)
            wr_col.addWidget(wr_label)
            wr_col.addWidget(self.wr_chart)

            charts_row.addLayout(pnl_col)
            charts_row.addLayout(wr_col)
        else:
            no_chart = QLabel("Install pyqtgraph for charts:  pip install pyqtgraph")
            no_chart.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
            no_chart.setAlignment(Qt.AlignmentFlag.AlignCenter)
            charts_row.addWidget(no_chart)

        layout.addLayout(charts_row)

        # ── Settlements table ─────────────────────────────────────────────
        inner_tabs = QTabWidget()
        inner_tabs.setStyleSheet("QTabBar::tab { padding: 6px 18px; font-size: 11px; }")

        self.daily_table = self._make_table()
        self.daily_table.setCursor(Qt.CursorShape.PointingHandCursor)
        self.daily_table.cellDoubleClicked.connect(self._on_day_row_clicked)
        inner_tabs.addTab(self.daily_table, "By Day")

        self.settlements_table = self._make_table()
        inner_tabs.addTab(self.settlements_table, "All Settlements")

        layout.addWidget(inner_tabs, stretch=1)

    def _on_day_row_clicked(self, row: int, _col: int):
        """Open DayDetailDialog for the clicked day row."""
        date_item = self.daily_table.item(row, 0)
        if not date_item:
            return
        date = date_item.text()
        trades = self._by_day.get(date, [])
        if not trades:
            return
        dlg = DayDetailDialog(date, trades, parent=self)
        dlg.exec()

    def _make_table(self) -> QTableWidget:
        t = QTableWidget()
        t.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        t.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        t.setAlternatingRowColors(True)
        t.setStyleSheet(f"QTableWidget {{ alternate-background-color: {BG_ROW_ALT}; }}")
        return t

    def set_client(self, client):
        self._client = client
        self.status_label.setText("Kalshi connected — click Refresh to load")
        self.status_label.setStyleSheet(f"color: {ACCENT}; font-size: 11px;")

    def load_data(self):
        if self._client is None:
            self.status_label.setText("Not connected — start the scheduler first")
            return

        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("Loading...")
        self.status_label.setText("Fetching settlements from Kalshi...")
        client = self._client

        def fetch():
            # Fetch settlements
            all_settlements = []
            cursor = None
            while True:
                params = {"limit": 200}
                if cursor:
                    params["cursor"] = cursor
                data   = client.get("portfolio/settlements", params=params)
                batch  = data.get("settlements", [])
                all_settlements.extend(batch)
                cursor = data.get("cursor")
                if not cursor or not batch:
                    break

            temp = [
                s for s in all_settlements
                if s.get("ticker", "").startswith("KX")
                and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
            ]
            settled_tickers = {s["ticker"] for s in temp}

            # Fetch fills for accurate entry prices + early exit detection
            all_fills = []
            cursor = None
            while True:
                params = {"limit": 200}
                if cursor:
                    params["cursor"] = cursor
                data  = client.get("portfolio/fills", params=params)
                batch = data.get("fills", [])
                all_fills.extend(batch)
                cursor = data.get("cursor")
                if not cursor or not batch:
                    break

            # Filter fills to temperature markets only
            temp_fills = [
                f for f in all_fills
                if f.get("ticker", "").startswith("KX")
                and ("HIGH" in f.get("ticker", "") or "LOWT" in f.get("ticker", ""))
            ]

            # Index fills by ticker
            from collections import defaultdict
            fills_by_ticker = defaultdict(list)
            for f in temp_fills:
                fills_by_ticker[f.get("ticker", "")].append(f)

            # Build settlement date index for comparison
            settled_dates = {s["ticker"]: s.get("settled_time", "")
                             for s in all_settlements
                             if s.get("ticker", "").startswith("KX")}

            # Detect early exits: tickers with sell fills BEFORE settlement
            # These are stop-losses — we exited before the market resolved
            early_exits = []
            for ticker, fills in fills_by_ticker.items():
                buy_fills  = [f for f in fills if f.get("action") == "buy"]
                sell_fills = [f for f in fills if f.get("action") == "sell"]

                if not buy_fills or not sell_fills:
                    continue

                # Check if any sell fill happened before the settlement time
                settle_time = settled_dates.get(ticker, "")
                early_sells = [f for f in sell_fills
                               if not settle_time or
                               f.get("created_time", "") < settle_time]

                if not early_sells:
                    continue  # sells were at/after settlement — not an early exit

                # Our side = what we bought
                sides    = [f.get("side") for f in buy_fills]
                our_side = max(set(sides), key=sides.count)

                our_buys = [f for f in buy_fills if f.get("side") == our_side]

                # Exit fills: action=sell on the SAME side as our position.
                # Both programmatic exits (place_order action=sell) and manual
                # Kalshi UI exits record side=our_side with action=sell.
                our_early_sells = [f for f in early_sells
                                   if f.get("side") == our_side]

                # Fallback: if no same-side sells found, try opposite side
                # (handles legacy fills from before the exit bug was fixed)
                if not our_early_sells:
                    legacy_side     = "yes" if our_side == "no" else "no"
                    our_early_sells = [f for f in early_sells
                                       if f.get("side") == legacy_side]

                if not our_early_sells:
                    continue

                # Kalshi fills only carry yes_price_dollars regardless of side.
                # For NO positions: actual price = 1.0 - yes_price_dollars.
                # Both buy and sell fills are interpreted from our_side's perspective —
                # closing a NO position records exit_side="yes" but the economic value
                # to the NO holder is still 1.0 - yes_price_dollars.
                def _fill_price(f):
                    yp = float(f.get("yes_price_dollars") or 0)
                    return yp if our_side == "yes" else (1.0 - yp)

                buy_contracts  = sum(float(f.get("count_fp") or 0) for f in our_buys)
                sell_contracts = sum(float(f.get("count_fp") or 0)
                                     for f in our_early_sells)

                if buy_contracts == 0:
                    continue

                avg_buy_price  = sum(
                    _fill_price(f) * float(f.get("count_fp") or 0)
                    for f in our_buys
                ) / buy_contracts

                avg_sell_price = sum(
                    _fill_price(f) * float(f.get("count_fp") or 0)
                    for f in our_early_sells
                ) / max(sell_contracts, 1)

                contracts = int(min(buy_contracts, sell_contracts))
                cost      = round(avg_buy_price  * contracts, 4)
                proceeds  = round(avg_sell_price * contracts, 4)
                fee       = sum(float(f.get("fee_cost") or 0)
                                for f in our_buys + our_early_sells)
                net_pnl   = round(proceeds - cost - fee, 4)

                # Use entry date (earliest buy fill) so early exits and settled wins
                # from the same trading day appear on the same row in the By Day table.
                date = sorted(our_buys,
                              key=lambda f: f.get("created_time", ""))[0].get(
                                  "created_time", "")[:10]

                early_exits.append({
                    "ticker":    ticker,
                    "date":      date,
                    "side":      our_side.upper(),
                    "contracts": contracts,
                    "avg_buy":   round(avg_buy_price, 4),
                    "avg_sell":  round(avg_sell_price, 4),
                    "fee":       round(fee, 4),
                    "net_pnl":   net_pnl,
                    "exit_type": "early_exit",
                })

            # Remove early exit tickers from settled list to avoid double-counting
            early_exit_tickers = {e["ticker"] for e in early_exits}
            temp = [s for s in temp if s["ticker"] not in early_exit_tickers]

            return temp, dict(fills_by_ticker), early_exits

        def on_done(result):
            settlements, fills_by_ticker, early_exits = result
            self._populate(settlements, fills_by_ticker, early_exits)
            self.refresh_btn.setEnabled(True)
            self.refresh_btn.setText("↻  Refresh")
            n_settled = len(settlements)
            n_exits   = len(early_exits)
            self.status_label.setText(
                f"Loaded {n_settled} settlements + {n_exits} early exits  •  "
                f"Last updated {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
            )
            self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

        def on_error(msg):
            self.refresh_btn.setEnabled(True)
            self.refresh_btn.setText("↻  Refresh")
            self.status_label.setText(f"Error loading data")
            self.status_label.setStyleSheet(f"color: {RED}; font-size: 11px;")

        self._pnl_thread, self._pnl_worker = run_in_background(
            fetch, on_done, on_error
        )

    def _populate(self, settlements: list, fills_by_ticker: dict = None,
                  early_exits: list = None):
        """Build all views from raw settlement data."""
        from collections import defaultdict

        # ── Per-settlement enrichment ─────────────────────────────────
        enriched = []
        for s in settlements:
            ticker   = s.get("ticker", "")
            result   = s.get("market_result", "").lower()
            fee      = float(s.get("fee_cost") or 0)

            # Determine our side and cost purely from fills
            # Settlement fields for cost are unreliable — they reflect both sides
            if not fills_by_ticker or ticker not in fills_by_ticker:
                continue   # skip if no fill data available

            buy_fills = [f for f in fills_by_ticker[ticker]
                         if f.get("action") == "buy"]
            if not buy_fills:
                continue

            # Use entry date (earliest buy fill) so that settled wins and early
            # exits from the same trading day land on the same row in the By Day
            # table. Using settled_time caused wins to appear a day after their
            # corresponding stops (settlement runs overnight after market close).
            entry_date = sorted(buy_fills,
                                key=lambda f: f.get("created_time", ""))[0].get(
                                    "created_time", "")[:10]

            # Our side = what we bought
            sides = [f.get("side") for f in buy_fills]
            our_side = max(set(sides), key=sides.count)

            our_fills = [f for f in buy_fills if f.get("side") == our_side]
            contracts = int(sum(float(f.get("count_fp") or 0) for f in our_fills))

            cost = round(sum(
                (float(f.get("yes_price_dollars") or 0) if our_side == "yes"
                 else (1.0 - float(f.get("yes_price_dollars") or 0)))
                * float(f.get("count_fp") or 0)
                for f in our_fills
            ), 4)

            if contracts == 0 or cost == 0:
                continue

            won    = (result == our_side)

            # Payout: if we won, we receive $1.00 per contract minus fee
            # revenue from API is unreliable (sometimes 0 even for wins)
            if won:
                payout  = contracts * 1.0
                net_pnl = round(payout - cost - fee, 4)
            else:
                net_pnl = round(-cost - fee, 4)

            enriched.append({
                "ticker":    ticker,
                "date":      entry_date,
                "side":      our_side.upper(),
                "contracts": contracts,
                "result":    result.upper(),
                "won":       won,
                "cost":      cost,
                "fee":       fee,
                "net_pnl":   net_pnl,
            })

        # Add early exits (stop-losses, manual closes) to enriched list
        for ex in (early_exits or []):
            enriched.append({
                "ticker":    ex["ticker"],
                "date":      ex["date"],
                "side":      ex["side"],
                "contracts": ex["contracts"],
                "result":    "EARLY EXIT",
                "won":       ex["net_pnl"] > 0,
                "cost":      ex["avg_buy"] * ex["contracts"],
                "fee":       ex["fee"],
                "net_pnl":   ex["net_pnl"],
            })

        if not enriched:
            return

        # ── Summary stats ─────────────────────────────────────────────
        total      = len(enriched)
        wins       = [e for e in enriched if e["won"]]
        win_rate   = round(len(wins) / total * 100, 1) if total else 0
        net_pnl    = round(sum(e["net_pnl"] for e in enriched), 2)
        total_fees = round(sum(e["fee"] for e in enriched), 2)

        by_day = defaultdict(list)
        for e in enriched:
            by_day[e["date"]].append(e)
        daily_pnls = [round(sum(t["net_pnl"] for t in v), 2) for v in by_day.values()]
        best_day   = max(daily_pnls) if daily_pnls else 0
        worst_day  = min(daily_pnls) if daily_pnls else 0

        self.stat_labels["Settled Trades"].setText(str(total))
        self.stat_labels["Win Rate"].setText(f"{win_rate}%")
        self.stat_labels["Win Rate"].setStyleSheet(
            f"color: {ACCENT if win_rate >= 70 else YELLOW if win_rate >= 50 else RED}; "
            f"font-size: 18px; font-weight: bold;"
        )
        sign  = "+" if net_pnl >= 0 else ""
        color = ACCENT if net_pnl >= 0 else RED
        self.stat_labels["Net PnL"].setText(f"{sign}${net_pnl:.2f}")
        self.stat_labels["Net PnL"].setStyleSheet(f"color: {color}; font-size: 18px; font-weight: bold;")
        self.stat_labels["Total Fees"].setText(f"${total_fees:.2f}")
        self.stat_labels["Best Day"].setText(f"+${best_day:.2f}")
        self.stat_labels["Best Day"].setStyleSheet(f"color: {ACCENT}; font-size: 18px; font-weight: bold;")
        self.stat_labels["Worst Day"].setText(f"${worst_day:.2f}")
        self.stat_labels["Worst Day"].setStyleSheet(f"color: {RED}; font-size: 18px; font-weight: bold;")

        # ── Equity curve ─────────────────────────────────────────────
        if HAS_PYQTGRAPH:
            sorted_days = sorted(by_day.keys())
            cum, curve  = 0.0, []
            for day in sorted_days:
                cum += sum(e["net_pnl"] for e in by_day[day])
                curve.append(round(cum, 4))

            self.chart.clear()
            x   = list(range(len(curve)))
            pen = pg.mkPen(color=ACCENT, width=2)
            self.chart.plot(x, curve, pen=pen)
            fill_color = QColor(ACCENT)
            fill_color.setAlpha(30)
            fill = pg.FillBetweenItem(
                self.chart.plot(x, [0]*len(x), pen=pg.mkPen(None)),
                self.chart.plot(x, curve, pen=pen),
                brush=fill_color,
            )
            self.chart.addItem(fill)

            # ── Rolling 7-day win rate chart ──────────────────────────
            if hasattr(self, 'wr_chart'):
                from collections import deque
                self.wr_chart.clear()
                window = deque()
                wr_x, wr_y = [], []
                for i, day in enumerate(sorted_days):
                    window.append(by_day[day])
                    # Keep only last 7 days in the window
                    if len(window) > 7:
                        window.popleft()
                    day_trades = [t for batch in window for t in batch]
                    if day_trades:
                        day_wins = sum(1 for t in day_trades if t["won"])
                        wr_x.append(i)
                        wr_y.append(round(day_wins / len(day_trades) * 100, 1))

                if wr_x:
                    wr_pen = pg.mkPen(color=YELLOW, width=2)
                    self.wr_chart.plot(wr_x, wr_y, pen=wr_pen)
                    # 70% reference line
                    ref_pen = pg.mkPen(color=ACCENT_DIM, width=1, style=Qt.PenStyle.DashLine)
                    self.wr_chart.addItem(pg.InfiniteLine(
                        pos=70, angle=0, pen=ref_pen, label="70%",
                        labelOpts={"color": ACCENT_DIM, "position": 0.95},
                    ))
                    # Fill under the line
                    wr_fill_color = QColor(YELLOW)
                    wr_fill_color.setAlpha(20)
                    wr_fill = pg.FillBetweenItem(
                        self.wr_chart.plot(wr_x, [0] * len(wr_x), pen=pg.mkPen(None)),
                        self.wr_chart.plot(wr_x, wr_y, pen=wr_pen),
                        brush=wr_fill_color,
                    )
                    self.wr_chart.addItem(wr_fill)
                    self.wr_chart.setYRange(0, 100)

        # ── By-day table ─────────────────────────────────────────────
        day_rows = []
        cum = 0.0
        for day in sorted(by_day.keys()):   # oldest first for correct accumulation
            trades    = by_day[day]
            day_wins  = [t for t in trades if t["won"]]
            day_pnl   = round(sum(t["net_pnl"] for t in trades), 2)
            day_fees  = round(sum(t["fee"] for t in trades), 2)
            day_settled_losses = sum(1 for t in trades
                                     if not t["won"] and t.get("result") != "EARLY EXIT")
            day_stopped        = sum(1 for t in trades
                                     if t.get("result") == "EARLY EXIT")
            cum      += day_pnl
            day_rows.append({
                "date":            day,
                "trades":          len(trades),
                "wins":            len(day_wins),
                "losses":          day_settled_losses,
                "stopped":         day_stopped,
                "win%":            f"{round(len(day_wins)/len(trades)*100,1)}%",
                "fees":            f"${day_fees:.2f}",
                "net_pnl":         day_pnl,
                "cum_pnl":         round(cum, 2),
            })
        day_rows.reverse()   # newest first for display
        self._by_day = dict(by_day)  # store for day detail dialog

        hdrs = ["Date", "Trades", "Wins", "Losses", "Stopped", "Win%", "Fees", "Net PnL", "Cum PnL"]
        self.daily_table.setColumnCount(len(hdrs))
        self.daily_table.setHorizontalHeaderLabels(hdrs)
        self.daily_table.setRowCount(len(day_rows))
        dh = self.daily_table.horizontalHeader()
        dh.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        dh.setStretchLastSection(True)
        # Date, Trades, Wins, Losses, Stopped, Win%, Fees, Net PnL, Cum PnL
        for col, width in enumerate([165, 90, 85, 90, 90, 80, 110, 130, 100]):
            self.daily_table.setColumnWidth(col, width)

        for ri, row in enumerate(day_rows):
            vals = [row["date"], str(row["trades"]), str(row["wins"]),
                    str(row["losses"]), str(row["stopped"]), row["win%"], row["fees"],
                    f"${row['net_pnl']:+.2f}", f"${row['cum_pnl']:+.2f}"]
            for ci, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if ci == 4:   # Stopped — highlight in yellow if non-zero
                    item.setForeground(QColor(YELLOW if row["stopped"] > 0 else TEXT_SEC))
                if ci == 7:
                    item.setForeground(QColor(ACCENT if row["net_pnl"] >= 0 else RED))
                if ci == 8:
                    item.setForeground(QColor(ACCENT if row["cum_pnl"] >= 0 else RED))
                self.daily_table.setItem(ri, ci, item)

        # ── All settlements table ─────────────────────────────────────
        s_hdrs = ["Date", "Market", "Side", "Qty", "Result", "Fee", "Net PnL"]
        self.settlements_table.setColumnCount(len(s_hdrs))
        self.settlements_table.setHorizontalHeaderLabels(s_hdrs)
        self.settlements_table.setRowCount(len(enriched))
        sh = self.settlements_table.horizontalHeader()
        sh.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        sh.setStretchLastSection(True)
        # Date, Market, Side, Qty, Result, Fee, Net PnL
        for col, width in enumerate([165, 240, 80, 70, 100, 110, 100]):
            self.settlements_table.setColumnWidth(col, width)

        for ri, e in enumerate(sorted(enriched, key=lambda x: x["date"], reverse=True)):
            result  = e.get("result", "")
            if result == "EARLY EXIT":
                result_str = "EXIT ↩"
                result_color = YELLOW
            elif e["won"]:
                result_str = "WON ✓"
                result_color = ACCENT
            else:
                result_str = "LOST ✗"
                result_color = RED

            market = _city_from_ticker(e["ticker"]) or e["ticker"]
            vals = [e["date"], market, e["side"], str(e["contracts"]),
                    result_str, f"${e['fee']:.2f}", f"${e['net_pnl']:+.2f}"]
            for ci, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if ci == 2:
                    item.setForeground(QColor(ACCENT if e["side"] == "NO" else YELLOW))
                if ci == 4:
                    item.setForeground(QColor(result_color))
                if ci == 6:
                    item.setForeground(QColor(ACCENT if e["net_pnl"] >= 0 else RED))
                self.settlements_table.setItem(ri, ci, item)


# ---------------------------------------------------------------------------
# Session Activity tab
# ---------------------------------------------------------------------------

class SessionTab(QWidget):
    def __init__(self):
        super().__init__()
        self._entries = []
        self._client  = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(16)

        # ── Header ───────────────────────────────────────────────────────
        hdr = QHBoxLayout()
        title = QLabel("SESSION ACTIVITY")
        title.setStyleSheet(f"color: {TEXT_PRI}; font-size: 16px; font-weight: bold; letter-spacing: 1px;")

        self.refresh_btn = QPushButton("⟳  Refresh")
        self.refresh_btn.setFixedWidth(100)
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {TEXT_SEC};
                border: 1px solid {BORDER};
                border-radius: 4px;
                padding: 6px 12px;
            }}
            QPushButton:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}
            QPushButton:disabled {{ border-color: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.refresh_btn.clicked.connect(self.refresh_statuses)

        self.clear_btn = QPushButton("✕  Clear")
        self.clear_btn.setFixedWidth(100)
        self.clear_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {TEXT_SEC};
                border: 1px solid {BORDER};
                border-radius: 4px;
                padding: 6px 12px;
            }}
            QPushButton:hover {{ border-color: {RED}; color: {RED}; }}
        """)
        self.clear_btn.clicked.connect(self.clear)

        self.count_label = QLabel("No entries this session")
        self.count_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

        hdr.addWidget(title)
        hdr.addSpacing(16)
        hdr.addWidget(self.count_label)
        hdr.addStretch()
        hdr.addWidget(self.refresh_btn)
        hdr.addSpacing(8)
        hdr.addWidget(self.clear_btn)
        layout.addLayout(hdr)

        # ── Summary bar ───────────────────────────────────────────────────
        summary_row = QHBoxLayout()
        self.stat_labels = {}
        for key in ["Entries", "Open", "Stopped Out", "Avg Score", "Unrealised"]:
            frame = QFrame()
            frame.setStyleSheet(f"""
                QFrame {{
                    background: {BG_PANEL};
                    border: 1px solid {BORDER};
                    border-radius: 6px;
                }}
            """)
            fl = QVBoxLayout(frame)
            fl.setContentsMargins(16, 8, 16, 8)
            lbl_key = QLabel(key.upper())
            lbl_key.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 1px;")
            lbl_val = QLabel("—")
            lbl_val.setStyleSheet(f"color: {TEXT_PRI}; font-size: 18px; font-weight: bold;")
            fl.addWidget(lbl_key)
            fl.addWidget(lbl_val)
            self.stat_labels[key] = lbl_val
            summary_row.addWidget(frame)
        layout.addLayout(summary_row)

        # ── Table ─────────────────────────────────────────────────────────
        self.table = QTableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels(
            ["Time", "Market", "Side", "Qty", "Entry", "Score", "Unreal. PnL", "Status"]
        )
        th = self.table.horizontalHeader()
        th.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        th.setStretchLastSection(True)
        # Time, Market, Side, Qty, Entry, Score, Unreal. PnL, Status
        for col, width in enumerate([120, 220, 80, 70, 90, 80, 110, 65]):
            self.table.setColumnWidth(col, width)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(
            f"QTableWidget {{ alternate-background-color: {BG_ROW_ALT}; }}"
        )
        layout.addWidget(self.table, stretch=1)

    def set_client(self, client):
        """Store Kalshi client — enables the Refresh button."""
        self._client = client
        self.refresh_btn.setEnabled(True)

    def refresh_statuses(self):
        """
        Re-sync Open entry statuses against live Kalshi data.
        Resolves each as Won / Lost using the settlements endpoint.
        """
        if not self._client:
            return
        self.refresh_btn.setEnabled(False)
        self.refresh_btn.setText("...")
        client = self._client

        def fetch():
            import trader as _t
            live         = _t.sync_from_kalshi(client)
            live_tickers = {p["ticker"] for p in live}
            data         = client.get("portfolio/settlements",
                                      params={"limit": 200})
            settled      = {
                s["ticker"]: s.get("market_result", "").lower()
                for s in data.get("settlements", [])
            }
            return live_tickers, settled

        def on_done(result):
            live_tickers, settled = result
            for entry in self._entries:
                if entry.get("status") != "Open":
                    continue
                ticker = entry.get("ticker", "")
                if ticker in live_tickers:
                    continue
                if ticker in settled:
                    our_side = entry.get("side", "").lower()
                    entry["status"] = "Won" if settled[ticker] == our_side else "Lost"
                else:
                    entry["status"] = "Settled"
            self._rebuild()
            self.refresh_btn.setEnabled(True)
            self.refresh_btn.setText("⟳  Refresh")

        def on_error(msg):
            self.refresh_btn.setEnabled(True)
            self.refresh_btn.setText("⟳  Refresh")

        self._ref_thread, self._ref_worker = run_in_background(
            fetch, on_done, on_error
        )

    def add_entry(self, pos: dict):
        """Add a new position entry from this session."""
        self._entries.append({**pos, "status": "Open"})
        self._rebuild()

    def update_pnl(self, live_positions: list):
        """Refresh unrealised_pnl for open entries from latest live positions."""
        pnl_by_ticker = {
            p["ticker"]: p.get("unrealised_pnl", 0)
            for p in live_positions
        }
        updated = False
        for entry in self._entries:
            if entry.get("status") == "Open":
                ticker = entry.get("ticker", "")
                if ticker in pnl_by_ticker:
                    entry["unrealised_pnl"] = pnl_by_ticker[ticker]
                    updated = True
        if updated:
            self._rebuild()

    def update_status(self, ticker: str, status: str):
        """Update status of an existing entry (e.g. 'Stopped Out')."""
        for entry in self._entries:
            if entry["ticker"] == ticker:
                entry["status"] = status
        self._rebuild()

    def clear(self):
        self._entries.clear()
        self._rebuild()

    def _rebuild(self):
        entries = list(reversed(self._entries))  # newest first
        self.table.setRowCount(len(entries))

        open_count    = sum(1 for e in entries if e["status"] == "Open")
        stopped_count = sum(1 for e in entries if e["status"] == "Stopped Out")
        scores        = [e.get("score", 0) for e in entries if isinstance(e.get("score"), (int, float))]
        avg_score     = f"{sum(scores)/len(scores):.1f}/3" if scores else "—"

        self.stat_labels["Entries"].setText(str(len(entries)))
        self.stat_labels["Open"].setText(str(open_count))
        self.stat_labels["Open"].setStyleSheet(f"color: {ACCENT}; font-size: 18px; font-weight: bold;")
        self.stat_labels["Stopped Out"].setText(str(stopped_count))
        self.stat_labels["Stopped Out"].setStyleSheet(
            f"color: {RED if stopped_count else TEXT_PRI}; font-size: 18px; font-weight: bold;"
        )
        self.stat_labels["Avg Score"].setText(avg_score)

        total_unreal = sum(e.get("unrealised_pnl", 0) or 0
                         for e in entries if e.get("status") == "Open")
        unreal_sign  = "+" if total_unreal >= 0 else ""
        unreal_color = ACCENT if total_unreal > 0 else (RED if total_unreal < 0 else TEXT_SEC)
        unreal_lbl   = self.stat_labels.get("Unrealised")
        if unreal_lbl:
            unreal_lbl.setText(f"{unreal_sign}${total_unreal:.2f}")
            unreal_lbl.setStyleSheet(
                f"color: {unreal_color}; font-size: 18px; font-weight: bold;"
            )

        self.count_label.setText(
            f"{len(entries)} entr{'y' if len(entries)==1 else 'ies'} this session"
        )

        for row, e in enumerate(entries):
            status   = e.get("status", "Open")
            side     = e.get("side", "").upper()
            score    = e.get("score", "?")
            avg_cost = e.get("avg_cost", 0)

            if status == "Open":
                status_color = ACCENT
            elif status in ("Stopped Out", "Lost"):
                status_color = RED
            elif status in ("Take Profit", "Won"):
                status_color = ACCENT
            else:  # Settled
                status_color = TEXT_SEC

            unreal     = e.get("unrealised_pnl", 0) or 0
            unreal_sign  = "+" if unreal >= 0 else ""
            unreal_color = ACCENT if unreal > 0 else (RED if unreal < 0 else TEXT_SEC)
            unreal_str   = f"{unreal_sign}${unreal:.2f}" if status == "Open" else "—"

            vals = [
                (e.get("entered_at", "—"),                          TEXT_SEC),
                (_city_from_ticker(e.get("ticker","")) or e.get("ticker",""), TEXT_PRI),
                (side,                        ACCENT if side == "NO" else YELLOW),
                (str(e.get("contracts", 1)), TEXT_PRI),
                (f"${avg_cost:.2f}",          TEXT_PRI),
                (f"{score}/3",                TEXT_PRI),
                (unreal_str,                  unreal_color),
                (status,                      status_color),
            ]

            for col, (val, color) in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item.setForeground(QColor(color))
                self.table.setItem(row, col, item)


# ---------------------------------------------------------------------------
# Log tab
# ---------------------------------------------------------------------------

class LogTab(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(8)

        hdr = QLabel("ACTIVITY LOG")
        hdr.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        layout.addWidget(hdr)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setStyleSheet(f"""
            QTextEdit {{
                background: {BG_DARK};
                color: {TEXT_PRI};
                border: 1px solid {BORDER};
                border-radius: 4px;
                font-family: monospace;
                font-size: 12px;
            }}
        """)
        layout.addWidget(self.log_box, stretch=1)

        clear_btn = QPushButton("Clear Log")
        clear_btn.setFixedWidth(100)
        clear_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {TEXT_SEC};
                border: 1px solid {BORDER};
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 11px;
            }}
            QPushButton:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}
        """)
        clear_btn.clicked.connect(self.log_box.clear)
        layout.addWidget(clear_btn, alignment=Qt.AlignmentFlag.AlignRight)

    def append(self, text: str):
        self.log_box.append(text)
        sb = self.log_box.verticalScrollBar()
        sb.setValue(sb.maximum())


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self, config: dict):
        super().__init__()
        self._config = config
        self.setWindowTitle("WeatherMachine  //  Kalshi Temperature Trader")
        self.setMinimumSize(1100, 760)

        # ── Central widget with header + tabs ────────────────────────────
        central = QWidget()
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Header bar
        header = QFrame()
        header.setFixedHeight(52)
        header.setStyleSheet(f"""
            QFrame {{
                background: {BG_PANEL};
                border-bottom: 1px solid {BORDER};
            }}
        """)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(20, 0, 20, 0)
        logo = QLabel("⛅  The Weather Machine")
        logo.setStyleSheet(f"""
            color: {ACCENT};
            font-size: 18px;
            font-weight: bold;
            letter-spacing: 2px;
        """)
        subtitle = QLabel("Kalshi Temperature Markets")
        subtitle.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px; letter-spacing: 1px;")

        # Global start/stop button
        self.global_start_btn = QPushButton("▶  Start Trading")
        self.global_start_btn.setFixedHeight(34)
        self.global_start_btn.setFixedWidth(150)
        self.global_start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {ACCENT};
                color: {BG_DARK};
                border: none;
                border-radius: 6px;
                font-size: 12px;
                font-weight: bold;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; }}
            QPushButton:disabled {{ background: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.global_start_btn.clicked.connect(self._global_toggle)

        self.global_status_label = QLabel("Idle")
        self.global_status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

        header_layout.addWidget(logo)
        header_layout.addSpacing(12)
        header_layout.addWidget(subtitle)
        header_layout.addStretch()
        header_layout.addWidget(self.global_status_label)
        header_layout.addSpacing(12)
        header_layout.addWidget(self.global_start_btn)
        header_layout.addSpacing(8)
        main_layout.addWidget(header)

        # Tabs
        tabs = QTabWidget()
        self.home_tab    = HomeTab()
        self.session_tab = SessionTab()
        self.pnl_tab     = PnLTab()
        self.log_tab     = LogTab()

        # Wire home tab log signal → log tab
        self.home_tab.log_line.connect(self.log_tab.append)

        settings_btn = QPushButton("⚙  Settings")
        settings_btn.setStyleSheet(f"""
            QPushButton {{
                background: transparent;
                color: {TEXT_SEC};
                border: none;
                padding: 6px 12px;
                font-size: 11px;
            }}
            QPushButton:hover {{ color: {ACCENT}; }}
        """)
        settings_btn.clicked.connect(self._open_settings)
        tabs.setCornerWidget(settings_btn, Qt.Corner.TopRightCorner)

        tabs.addTab(self.home_tab,    "  Home  ")
        tabs.addTab(self.session_tab, "  Session  ")
        tabs.addTab(self.pnl_tab,     "  Performance  ")
        tabs.addTab(self.log_tab,     "  Log  ")
        tabs.currentChanged.connect(self._on_tab_changed)
        main_layout.addWidget(tabs)

        # Client ready → pass to PnL tab
        self.home_tab._client_ready_callbacks = [
            self.pnl_tab.set_client,
            self.session_tab.set_client,
        ]
        # Wire session tab when scheduler starts
        self.home_tab._start_trading_callbacks = [
            self._wire_session_signals,
            self._sync_global_btn_running,
        ]
        # Sync global button when home tab stops
        self.home_tab._stop_trading_callbacks = [self._sync_global_btn_stopped]

        self.setCentralWidget(central)

    def _global_toggle(self):
        """Delegate start/stop to the home tab's toggle_scheduler method."""
        self.home_tab.toggle_scheduler()

    def _sync_global_btn_running(self):
        """Update global button to reflect running state."""
        self.global_start_btn.setText("■  Stop Trading")
        self.global_start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {RED};
                color: white;
                border: none;
                border-radius: 6px;
                font-size: 12px;
                font-weight: bold;
            }}
            QPushButton:hover {{ background: #cc2222; }}
            QPushButton:disabled {{ background: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.global_status_label.setText("Trading active")
        self.global_status_label.setStyleSheet(f"color: {ACCENT}; font-size: 11px;")

    def _sync_global_btn_stopped(self):
        """Update global button to reflect stopped state."""
        self.global_start_btn.setText("▶  Start Trading")
        self.global_start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {ACCENT};
                color: {BG_DARK};
                border: none;
                border-radius: 6px;
                font-size: 12px;
                font-weight: bold;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; }}
            QPushButton:disabled {{ background: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.global_status_label.setText("Idle")
        self.global_status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")

    def _wire_session_signals(self):
        """Connect session signals and clear the session tab."""
        self.session_tab.clear()
        worker = self.home_tab._worker
        if worker:
            worker.session_entry.connect(self.session_tab.add_entry)
            worker.session_exit.connect(
                lambda ticker, reason: self.session_tab.update_status(ticker, reason)
            )
            # Refresh unrealised PnL on every positions update
            worker.positions_updated.connect(self._refresh_session_pnl)

    def _refresh_session_pnl(self):
        """Fetch latest positions and push unrealised PnL to session tab."""
        client = self.home_tab._client
        if client is None:
            return
        try:
            import trader as _t
            live = _t.sync_from_kalshi(client)
            self.session_tab.update_pnl(live)
        except Exception:
            pass

    def _open_settings(self):
        """Open credential dialog — only when scheduler is not running."""
        if self.home_tab._running:
            QMessageBox.information(
                self, "Scheduler Running",
                "Please stop the scheduler before changing credentials."
            )
            return
        dlg = CredentialDialog(self._config, parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._config = load_config()
            apply_config(self._config)
            # Apply font size
            font_size = self._config.get("font_size", DEFAULT_FONT_SIZE)
            QApplication.instance().setStyleSheet(build_stylesheet(font_size))
            # Update live/demo indicator
            mode  = "LIVE" if self._config.get("live_mode") else "DEMO"
            color = YELLOW if self._config.get("live_mode") else ACCENT
            self.home_tab.mode_label.setText(mode)
            self.home_tab.mode_label.setStyleSheet(f"""
                color: {color}; font-size: 11px; letter-spacing: 2px;
                padding: 4px 10px;
                border: 1px solid {color};
                border-radius: 4px;
            """)

    def _on_tab_changed(self, idx: int):
        if idx == 2 and self.pnl_tab._client is not None:
            self.pnl_tab.load_data()

    def closeEvent(self, event):
        if self.home_tab._worker:
            self.home_tab._worker.stop()
        event.accept()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Load saved credentials first so we can read font size
    config = load_config()
    font_size = config.get("font_size", DEFAULT_FONT_SIZE)
    app.setStyleSheet(build_stylesheet(font_size))

    # Dark palette
    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window,          QColor(BG_DARK))
    palette.setColor(QPalette.ColorRole.WindowText,      QColor(TEXT_PRI))
    palette.setColor(QPalette.ColorRole.Base,            QColor(BG_PANEL))
    palette.setColor(QPalette.ColorRole.AlternateBase,   QColor(BG_ROW_ALT))
    palette.setColor(QPalette.ColorRole.Text,            QColor(TEXT_PRI))
    palette.setColor(QPalette.ColorRole.Button,          QColor(BG_PANEL))
    palette.setColor(QPalette.ColorRole.ButtonText,      QColor(TEXT_PRI))
    palette.setColor(QPalette.ColorRole.Highlight,       QColor(ACCENT_DIM))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor(BG_DARK))
    app.setPalette(palette)

    # Show setup dialog if credentials are missing or incomplete
    needs_setup = not config.get("key_id") or not config.get("key_file")
    if needs_setup:
        dlg = CredentialDialog(config)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            sys.exit(0)
        config = load_config()

    # Apply credentials to environment
    apply_config(config)

    window = MainWindow(config)
    window.showMaximized()
    sys.exit(app.exec())
