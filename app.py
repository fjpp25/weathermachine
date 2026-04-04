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

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout, QLabel,
    QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QFrame, QScrollArea, QSizePolicy,
    QTextEdit, QSplitter, QMessageBox, QLineEdit,
    QFileDialog, QDialog, QDialogButtonBox, QCheckBox,
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QObject
)
from PyQt6.QtGui import QFont, QColor, QPalette, QIcon

try:
    import pyqtgraph as pg
    HAS_PYQTGRAPH = True
except ImportError:
    HAS_PYQTGRAPH = False

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR        = Path("data")
CONFIG_FILE     = DATA_DIR / "config.json"   # stores credentials locally
POSITIONS_FILE  = DATA_DIR / "positions.json"
SUMMARY_CSV     = DATA_DIR / "daily_summary.csv"
TRADES_CSV      = DATA_DIR / "trades.csv"

CITY_TIMEZONES = {
    "New York":      "America/New_York",
    "Chicago":       "America/Chicago",
    "Miami":         "America/New_York",
    "Austin":        "America/Chicago",
    "Los Angeles":   "America/Los_Angeles",
    "San Francisco": "America/Los_Angeles",
    "Denver":        "America/Denver",
    "Philadelphia":  "America/New_York",
}

# ---------------------------------------------------------------------------
# Color palette — dark financial terminal aesthetic
# ---------------------------------------------------------------------------
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

STYLESHEET = f"""
QMainWindow, QWidget {{
    background-color: {BG_DARK};
    color: {TEXT_PRI};
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: 13px;
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
    font-size: 13px;
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
    font-size: 11px;
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
    font-size: 11px;
}}
QFrame[frameShape="4"], QFrame[frameShape="5"] {{
    color: {BORDER};
}}
"""


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
        }
        save_config(config)
        apply_config(config)
        self.accept()

    def get_config(self) -> dict:
        return {
            "key_id":    self.key_id_edit.text().strip(),
            "key_file":  self.pem_edit.text().strip(),
            "live_mode": self.live_check.isChecked(),
        }


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
            import decision_engine

            client = trader.make_client(skip_confirmation=True)
            self.client_ready.emit(client)

            ACTIVITY_START = 9
            ACTIVITY_END   = 15

            def local_hour(tz):
                return datetime.now(ZoneInfo(tz)).hour

            def any_active():
                return any(
                    ACTIVITY_START <= local_hour(tz) < ACTIVITY_END
                    for tz in CITY_TIMEZONES.values()
                )

            def all_done():
                return all(
                    local_hour(tz) >= ACTIVITY_END
                    for tz in CITY_TIMEZONES.values()
                )

            def dynamic_interval():
                min_secs = 15 * 60
                for tz in CITY_TIMEZONES.values():
                    h = local_hour(tz)
                    if 11 <= h < 13:
                        min_secs = min(min_secs, 3 * 60)
                    elif h in (10, 13):
                        min_secs = min(min_secs, 5 * 60)
                    elif 14 <= h < ACTIVITY_END:
                        min_secs = min(min_secs, 10 * 60)
                return min_secs

            poll_count = 0

            while self.is_running():
                if not any_active():
                    if all_done():
                        self.log_line.emit("All cities past activity window. Done for today.")
                        break
                    self.log_line.emit("No city active yet — waiting...")
                    self.sleep_interruptible(60)
                    continue

                interval_secs = dynamic_interval()
                poll_count += 1
                self.poll_started.emit(poll_count)

                now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
                self.log_line.emit(f"\n[{now_str}] Poll #{poll_count}  ({interval_secs//60} min interval)")

                # Run pipeline
                try:
                    trader.run_pipeline(client=client, paper=self.paper)
                    self.positions_updated.emit()
                except Exception as e:
                    self.log_line.emit(f"Pipeline error: {e}")

                if not self.is_running():
                    break

                # Check exits — use live Kalshi positions, not local file
                try:
                    live_pos = trader.sync_from_kalshi(client)
                    if live_pos:
                        self.log_line.emit(f"Checking exits ({len(live_pos)} open)...")
                        trader.check_exits(client, paper=self.paper)
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
    def __init__(self, city: str):
        super().__init__()
        self.city = city
        self.setFixedSize(180, 90)
        self.setStyleSheet(f"""
            QFrame {{
                background: {BG_PANEL};
                border: 1px solid {BORDER};
                border-radius: 6px;
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(2)

        self.name_label = QLabel(city)
        self.name_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px; letter-spacing: 1px;")

        self.time_label = QLabel("--:-- --")
        self.time_label.setStyleSheet(f"color: {TEXT_PRI}; font-size: 13px; font-weight: bold;")

        self.temp_label = QLabel("curr: --°  hi: --°")
        self.temp_label.setStyleSheet(f"color: {ACCENT}; font-size: 11px;")

        self.status_label = QLabel("waiting")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px;")

        layout.addWidget(self.name_label)
        layout.addWidget(self.time_label)
        layout.addWidget(self.temp_label)
        layout.addWidget(self.status_label)

    def update_data(self, local_time: str, curr: float, obs_hi: float,
                    fcst_hi: float, active: bool):
        self.time_label.setText(local_time)
        self.temp_label.setText(
            f"curr: {curr:.0f}°  hi: {obs_hi:.0f}°  fcst: {fcst_hi:.0f}°"
        )
        color = ACCENT if active else TEXT_SEC
        self.status_label.setText("active" if active else "outside window")
        self.status_label.setStyleSheet(f"color: {color}; font-size: 10px;")
        self.setStyleSheet(f"""
            QFrame {{
                background: {BG_PANEL};
                border: 1px solid {"" + ACCENT_DIM if active else BORDER};
                border-radius: 6px;
            }}
        """)


# ---------------------------------------------------------------------------
# Home tab
# ---------------------------------------------------------------------------

class HomeTab(QWidget):
    def __init__(self):
        super().__init__()
        self._worker  = None
        self._thread  = None
        self._running = False
        self._client  = None
        self._next_poll_ts = None

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(16)

        # ── Top bar: controls + balance ──────────────────────────────────
        top_bar = QHBoxLayout()

        self.start_btn = QPushButton("▶  Start Trading")
        self.start_btn.setFixedHeight(44)
        self.start_btn.setFixedWidth(180)
        self.start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {ACCENT};
                color: {BG_DARK};
                border: none;
                border-radius: 6px;
                font-size: 13px;
                font-weight: bold;
                letter-spacing: 1px;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; }}
            QPushButton:disabled {{ background: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.start_btn.clicked.connect(self.toggle_scheduler)

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

        self.reconcile_btn = QPushButton("↻  Reconcile")
        self.reconcile_btn.setFixedHeight(44)
        self.reconcile_btn.setFixedWidth(140)
        self.reconcile_btn.setStyleSheet(f"""
            QPushButton {{
                background: {BG_PANEL};
                color: {ACCENT};
                border: 1px solid {ACCENT_DIM};
                border-radius: 6px;
                font-size: 13px;
                letter-spacing: 1px;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; color: {BG_DARK}; }}
            QPushButton:disabled {{ border-color: {BORDER}; color: {TEXT_SEC}; }}
        """)
        self.reconcile_btn.clicked.connect(self.run_reconcile)

        self.sync_btn = QPushButton("⟳  Sync")
        self.sync_btn.setFixedHeight(44)
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

        top_bar.addWidget(self.start_btn)
        top_bar.addSpacing(8)
        top_bar.addWidget(self.reconcile_btn)
        top_bar.addSpacing(8)
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

        self.pnl_label = QLabel("Session PnL  —")
        self.pnl_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 14px;")

        bal_bar.addWidget(self.balance_label)
        bal_bar.addSpacing(32)
        bal_bar.addWidget(self.deployable_label)
        bal_bar.addSpacing(32)
        bal_bar.addWidget(self.pnl_label)
        bal_bar.addStretch()

        # ── City grid ────────────────────────────────────────────────────
        city_label = QLabel("CITY STATUS")
        city_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")

        city_grid = QGridLayout()
        city_grid.setSpacing(10)
        self.city_cards = {}
        cities = list(CITY_TIMEZONES.keys())
        for i, city in enumerate(cities):
            card = CityCard(city)
            self.city_cards[city] = card
            city_grid.addWidget(card, i // 4, i % 4)

        # ── Splitter: positions table + log ──────────────────────────────
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Positions table
        pos_frame = QFrame()
        pos_layout = QVBoxLayout(pos_frame)
        pos_layout.setContentsMargins(0, 0, 0, 0)
        pos_layout.setSpacing(6)

        pos_hdr = QLabel("OPEN POSITIONS")
        pos_hdr.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        pos_layout.addWidget(pos_hdr)

        self.pos_table = QTableWidget()
        self.pos_table.setColumnCount(7)
        self.pos_table.setHorizontalHeaderLabels(
            ["Ticker", "Side", "Qty", "Avg Cost", "Current", "Unreal. PnL", "Updated"]
        )
        self.pos_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.pos_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        self.pos_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.pos_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.pos_table.setAlternatingRowColors(True)
        self.pos_table.setStyleSheet(
            self.pos_table.styleSheet() +
            f"QTableWidget {{ alternate-background-color: {BG_ROW_ALT}; }}"
        )
        pos_layout.addWidget(self.pos_table)

        # Log panel
        log_frame = QFrame()
        log_layout = QVBoxLayout(log_frame)
        log_layout.setContentsMargins(0, 0, 0, 0)
        log_layout.setSpacing(6)

        log_hdr = QLabel("ACTIVITY LOG")
        log_hdr.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        log_layout.addWidget(log_hdr)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMinimumHeight(120)
        log_layout.addWidget(self.log_box)

        splitter.addWidget(pos_frame)
        splitter.addWidget(log_frame)
        splitter.setSizes([300, 180])

        # ── Assemble ─────────────────────────────────────────────────────
        main_layout.addLayout(top_bar)
        main_layout.addLayout(bal_bar)
        main_layout.addWidget(self._hline())
        main_layout.addWidget(city_label)
        main_layout.addLayout(city_grid)
        main_layout.addWidget(self._hline())
        main_layout.addWidget(splitter, stretch=1)

        # Timers
        self._city_timer = QTimer()
        self._city_timer.timeout.connect(self._refresh_cities)
        self._city_timer.start(30_000)
        self._refresh_cities()

        self._countdown_timer = QTimer()
        self._countdown_timer.timeout.connect(self._tick_countdown)
        self._countdown_timer.start(1_000)

        self._pos_timer = QTimer()
        self._pos_timer.timeout.connect(self.refresh_positions)
        self._pos_timer.start(15_000)
        self.refresh_positions()

    def _hline(self):
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setStyleSheet(f"color: {BORDER};")
        return line

    def toggle_scheduler(self):
        if self._running:
            self._stop_scheduler()
        else:
            # Check if live mode — show confirmation dialog
            demo = os.environ.get("KALSHI_DEMO", "true").lower() != "false"            if not demo:
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
        self.start_btn.setText("■  Stop Trading")
        self.start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {RED};
                color: white;
                border: none;
                border-radius: 6px;
                font-size: 13px;
                font-weight: bold;
            }}
            QPushButton:hover {{ background: #cc3d55; }}
        """)
        self.status_label.setText("Starting...")
        self.status_label.setStyleSheet(f"color: {ACCENT}; font-size: 12px;")
        self.append_log("Scheduler started.")

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

    def _stop_scheduler(self):
        if self._worker:
            self._worker.stop()
        self.start_btn.setEnabled(False)
        self.start_btn.setText("■  Stopping...")
        self.status_label.setText("Stopping — finishing current operation...")
        self.status_label.setStyleSheet(f"color: {YELLOW}; font-size: 12px;")
        self.append_log("Stop requested — will finish current operation then exit.")

    def _on_worker_stopped(self):
        self._running = False
        self._thread.quit()
        self._thread.wait()
        self.start_btn.setEnabled(True)
        self.start_btn.setText("▶  Start Trading")
        self.start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {ACCENT};
                color: {BG_DARK};
                border: none;
                border-radius: 6px;
                font-size: 13px;
                font-weight: bold;
            }}
            QPushButton:hover {{ background: {ACCENT_DIM}; }}
        """)
        self.status_label.setText("Idle")
        self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
        self._next_poll_ts = None
        self.append_log("Scheduler stopped.")

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
        self.refresh_positions()

    def _on_balance_updated(self, bal: float, dep: float):
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

    def append_log(self, text: str):
        self.log_box.append(text)
        sb = self.log_box.verticalScrollBar()
        sb.setValue(sb.maximum())

    def run_reconcile(self):
        """Run reconciliation against Kalshi settlements and refresh positions."""
        self.reconcile_btn.setEnabled(False)
        self.reconcile_btn.setText("Reconciling...")
        self.append_log("Running reconciliation...")

        def do_reconcile():
            try:
                import reconcile
                import trader

                client = trader.make_client(skip_confirmation=True)
                settlements = reconcile.fetch_settlements(client)
                settled_by_ticker = {s["ticker"]: s for s in settlements}

                local_open = [p for p in trader.load_positions()
                              if p["status"] == "open"]
                reconciled = 0
                for pos in local_open:
                    ticker = pos["ticker"]
                    if ticker not in settled_by_ticker:
                        continue
                    s        = settled_by_ticker[ticker]
                    result   = s.get("market_result", "").lower()
                    fee_cost = float(s.get("fee_cost") or 0)
                    won      = (result == pos["side"])
                    exit_price = 1.00 if won else 0.00
                    trader.record_exit(pos["id"], exit_price, f"settled_{result}")
                    trader.log_trade("settled", {
                        "ticker":      ticker,
                        "side":        pos["side"],
                        "entry_price": pos["entry_price"],
                        "exit_price":  exit_price,
                        "contracts":   pos["contracts"],
                        "result":      result,
                        "pnl":         round((exit_price - pos["entry_price"]) * pos["contracts"] - fee_cost, 2),
                        "fee_cost":    fee_cost,
                    })
                    reconciled += 1

                return reconciled, len(local_open)

            except Exception as e:
                return None, str(e)

        # Run in thread to avoid blocking UI
        import threading
        def worker():
            result, info = do_reconcile()
            # Use QTimer to update UI from main thread
            from PyQt6.QtCore import QTimer
            def finish():
                self.reconcile_btn.setEnabled(True)
                self.reconcile_btn.setText("↻  Reconcile")
                if result is None:
                    self.append_log(f"Reconcile error: {info}")
                else:
                    self.append_log(f"Reconcile complete — {result}/{info} positions settled.")
                self.refresh_positions()
            QTimer.singleShot(0, finish)

        threading.Thread(target=worker, daemon=True).start()

    def sync_positions_from_kalshi(self):
        """Fetch live positions directly from Kalshi and update the table."""
        if not hasattr(self, '_client') or self._client is None:
            # No client yet — load from local file as fallback
            self.refresh_positions()
            return

        self.sync_btn.setEnabled(False)
        self.sync_btn.setText("Syncing...")

        import threading
        def worker():
            try:
                import trader
                positions = trader.sync_from_kalshi(self._client)
                from PyQt6.QtCore import QTimer
                def finish():
                    self._update_positions_table(positions)
                    self.sync_btn.setEnabled(True)
                    self.sync_btn.setText("⟳  Sync")
                QTimer.singleShot(0, finish)
            except Exception as e:
                from PyQt6.QtCore import QTimer
                def finish():
                    self.append_log(f"Sync error: {e}")
                    self.sync_btn.setEnabled(True)
                    self.sync_btn.setText("⟳  Sync")
                QTimer.singleShot(0, finish)

        threading.Thread(target=worker, daemon=True).start()

    def _update_positions_table(self, positions: list):
        """Update the positions table from a list of enriched position dicts."""
        self.pos_table.setRowCount(len(positions))
        total_unrealised = 0.0

        for row, pos in enumerate(positions):
            ticker    = pos.get("ticker", "")
            side      = pos.get("side", "").upper()
            qty       = pos.get("contracts", 1)
            avg_cost  = pos.get("avg_cost", 0)
            current   = pos.get("current_price", 0)
            unreal    = pos.get("unrealised_pnl", 0)
            updated   = pos.get("last_updated", "")
            total_unrealised += unreal

            pnl_color = ACCENT if unreal >= 0 else RED
            sign      = "+" if unreal >= 0 else ""

            items = [
                (ticker, TEXT_PRI),
                (side, ACCENT if side == "NO" else YELLOW),
                (str(qty), TEXT_PRI),
                (f"${avg_cost:.2f}", TEXT_PRI),
                (f"${current:.2f}", TEXT_PRI),
                (f"{sign}${unreal:.2f}", pnl_color),
                (updated, TEXT_SEC),
            ]

            for col, (val, color) in enumerate(items):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                item.setForeground(QColor(color))
                self.pos_table.setItem(row, col, item)

        # Update session PnL from unrealised
        sign  = "+" if total_unrealised >= 0 else ""
        color = ACCENT if total_unrealised >= 0 else RED
        self.pnl_label.setText(f"Unrealised  {sign}${total_unrealised:.2f}")
        self.pnl_label.setStyleSheet(f"color: {color}; font-size: 14px;")

    def refresh_positions(self):
        """Fallback: load from local positions.json when no Kalshi client available."""
        if not POSITIONS_FILE.exists():
            return
        try:
            with open(POSITIONS_FILE) as f:
                positions = json.load(f)
        except Exception:
            return

        open_pos = [p for p in positions if p["status"] == "open"]

        # Convert local format to display format
        display = []
        for pos in open_pos:
            display.append({
                "ticker":        pos.get("ticker", ""),
                "side":          pos.get("side", ""),
                "contracts":     pos.get("contracts", 1),
                "avg_cost":      pos.get("entry_price", 0),
                "current_price": 0,
                "unrealised_pnl":0,
                "last_updated":  pos.get("opened_at", "")[:16].replace("T", " "),
            })
        self._update_positions_table(display)

    def _refresh_cities(self):
        for city, tz in CITY_TIMEZONES.items():
            card = self.city_cards[city]
            now  = datetime.now(ZoneInfo(tz))
            h    = now.hour
            active = 9 <= h < 15
            card.update_data(
                local_time = now.strftime("%H:%M %Z"),
                curr       = 0,
                obs_hi     = 0,
                fcst_hi    = 0,
                active     = active,
            )


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
            lbl_key.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 1px;")
            lbl_val = QLabel("—")
            lbl_val.setStyleSheet(f"color: {TEXT_PRI}; font-size: 18px; font-weight: bold;")
            fl.addWidget(lbl_key)
            fl.addWidget(lbl_val)
            self.stat_labels[key] = lbl_val
            stats_row.addWidget(frame)
        layout.addLayout(stats_row)

        # ── Equity curve ──────────────────────────────────────────────────
        curve_label = QLabel("EQUITY CURVE  —  Cumulative Net PnL  (temperature markets only)")
        curve_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 10px; letter-spacing: 2px;")
        layout.addWidget(curve_label)

        if HAS_PYQTGRAPH:
            pg.setConfigOptions(antialias=True, background=BG_PANEL, foreground=TEXT_SEC)
            self.chart = pg.PlotWidget()
            self.chart.setMinimumHeight(200)
            self.chart.showGrid(x=False, y=True, alpha=0.15)
            self.chart.getAxis("left").setTextPen(TEXT_SEC)
            self.chart.getAxis("bottom").setTextPen(TEXT_SEC)
            self.chart.setLabel("left", "Net PnL ($)")
            self.chart.getPlotItem().hideAxis("top")
            self.chart.getPlotItem().hideAxis("right")
            layout.addWidget(self.chart)
        else:
            no_chart = QLabel("Install pyqtgraph for equity curve:  pip install pyqtgraph")
            no_chart.setStyleSheet(f"color: {TEXT_SEC}; font-size: 12px;")
            no_chart.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(no_chart)

        # ── Settlements table ─────────────────────────────────────────────
        inner_tabs = QTabWidget()
        inner_tabs.setStyleSheet("QTabBar::tab { padding: 6px 18px; font-size: 11px; }")

        self.daily_table = self._make_table()
        inner_tabs.addTab(self.daily_table, "By Day")

        self.settlements_table = self._make_table()
        inner_tabs.addTab(self.settlements_table, "All Settlements")

        layout.addWidget(inner_tabs, stretch=1)

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

        import threading
        def worker():
            try:
                # Fetch all settlements, paginate if needed
                all_settlements = []
                cursor = None
                while True:
                    params = {"limit": 200}
                    if cursor:
                        params["cursor"] = cursor
                    data   = self._client.get("portfolio/settlements", params=params)
                    batch  = data.get("settlements", [])
                    all_settlements.extend(batch)
                    cursor = data.get("cursor")
                    if not cursor or not batch:
                        break

                # Filter to temperature markets only
                temp = [
                    s for s in all_settlements
                    if s.get("ticker", "").startswith("KX")
                    and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
                ]

                from PyQt6.QtCore import QTimer
                def finish():
                    self._populate(temp)
                    self.refresh_btn.setEnabled(True)
                    self.refresh_btn.setText("↻  Refresh")
                    self.status_label.setText(
                        f"Loaded {len(temp)} temperature settlements  •  "
                        f"Last updated {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
                    )
                    self.status_label.setStyleSheet(f"color: {TEXT_SEC}; font-size: 11px;")
                QTimer.singleShot(0, finish)

            except Exception as e:
                from PyQt6.QtCore import QTimer
                def finish():
                    self.refresh_btn.setEnabled(True)
                    self.refresh_btn.setText("↻  Refresh")
                    self.status_label.setText(f"Error: {e}")
                    self.status_label.setStyleSheet(f"color: {RED}; font-size: 11px;")
                QTimer.singleShot(0, finish)

        threading.Thread(target=worker, daemon=True).start()

    def _populate(self, settlements: list):
        """Build all views from raw settlement data."""
        from collections import defaultdict

        # ── Per-settlement enrichment ─────────────────────────────────
        enriched = []
        for s in settlements:
            ticker   = s.get("ticker", "")
            result   = s.get("market_result", "").lower()
            yes_fp   = float(s.get("yes_count_fp") or 0)
            no_fp    = float(s.get("no_count_fp") or 0)
            fee      = float(s.get("fee_cost") or 0)
            revenue  = float(s.get("revenue") or 0) / 100   # revenue in cents
            settled  = s.get("settled_time", "")[:10]

            # Determine side held and contracts
            if yes_fp > 0:
                side      = "yes"
                contracts = int(yes_fp)
            elif no_fp > 0:
                side      = "no"
                contracts = int(no_fp)
            else:
                continue

            won     = (result == side)
            payout  = revenue - fee   # actual cash received net of fees
            # Cost = total paid for the contracts
            cost    = float(s.get("yes_total_cost") or s.get("no_total_cost") or 0) / 100
            net_pnl = round(payout - cost, 4) if won else round(-cost - fee, 4)

            enriched.append({
                "ticker":    ticker,
                "date":      settled,
                "side":      side.upper(),
                "contracts": contracts,
                "result":    result.upper(),
                "won":       won,
                "cost":      cost,
                "payout":    round(payout, 4),
                "fee":       fee,
                "net_pnl":   net_pnl,
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

        # ── By-day table ─────────────────────────────────────────────
        day_rows = []
        cum = 0.0
        for day in sorted(by_day.keys(), reverse=True):
            trades    = by_day[day]
            day_wins  = [t for t in trades if t["won"]]
            day_pnl   = round(sum(t["net_pnl"] for t in trades), 2)
            day_fees  = round(sum(t["fee"] for t in trades), 2)
            cum      += day_pnl
            day_rows.append({
                "date":    day,
                "trades":  len(trades),
                "wins":    len(day_wins),
                "losses":  len(trades) - len(day_wins),
                "win%":    f"{round(len(day_wins)/len(trades)*100,1)}%",
                "fees":    f"${day_fees:.2f}",
                "net_pnl": day_pnl,
                "cum_pnl": round(cum, 2),
            })

        hdrs = ["Date", "Trades", "Wins", "Losses", "Win%", "Fees", "Net PnL", "Cum PnL"]
        self.daily_table.setColumnCount(len(hdrs))
        self.daily_table.setHorizontalHeaderLabels(hdrs)
        self.daily_table.setRowCount(len(day_rows))
        self.daily_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        for ri, row in enumerate(day_rows):
            vals = [row["date"], str(row["trades"]), str(row["wins"]),
                    str(row["losses"]), row["win%"], row["fees"],
                    f"${row['net_pnl']:+.2f}", f"${row['cum_pnl']:+.2f}"]
            for ci, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if ci == 6:
                    item.setForeground(QColor(ACCENT if row["net_pnl"] >= 0 else RED))
                if ci == 7:
                    item.setForeground(QColor(ACCENT if row["cum_pnl"] >= 0 else RED))
                self.daily_table.setItem(ri, ci, item)

        # ── All settlements table ─────────────────────────────────────
        s_hdrs = ["Date", "Ticker", "Side", "Qty", "Result", "Fee", "Net PnL"]
        self.settlements_table.setColumnCount(len(s_hdrs))
        self.settlements_table.setHorizontalHeaderLabels(s_hdrs)
        self.settlements_table.setRowCount(len(enriched))
        self.settlements_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        for col in [0, 2, 3, 4, 5, 6]:
            self.settlements_table.horizontalHeader().setSectionResizeMode(
                col, QHeaderView.ResizeMode.ResizeToContents
            )

        for ri, e in enumerate(sorted(enriched, key=lambda x: x["date"], reverse=True)):
            won_str = "WON ✓" if e["won"] else "LOST ✗"
            vals    = [e["date"], e["ticker"], e["side"], str(e["contracts"]),
                       won_str, f"${e['fee']:.2f}", f"${e['net_pnl']:+.2f}"]
            for ci, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if ci == 2:
                    item.setForeground(QColor(ACCENT if e["side"] == "NO" else YELLOW))
                if ci == 4:
                    item.setForeground(QColor(ACCENT if e["won"] else RED))
                if ci == 6:
                    item.setForeground(QColor(ACCENT if e["net_pnl"] >= 0 else RED))
                self.settlements_table.setItem(ri, ci, item)


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self, config: dict):
        super().__init__()
        self._config = config
        self.setWindowTitle("WeatherMachine  //  Kalshi Temperature Trader")
        self.setMinimumSize(1100, 760)

        tabs = QTabWidget()
        self.home_tab = HomeTab()
        self.pnl_tab  = PnLTab()

        # Settings button in the tab bar corner
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

        tabs.addTab(self.home_tab, "  Home  ")
        tabs.addTab(self.pnl_tab,  "  Performance  ")
        tabs.currentChanged.connect(self._on_tab_changed)

        # Pass client to PnL tab when scheduler connects
        self.home_tab._client_ready_callbacks = [self.pnl_tab.set_client]

        self.setCentralWidget(tabs)

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
            # Update live/demo indicator on home tab
            mode = "LIVE" if self._config.get("live_mode") else "DEMO"
            self.home_tab.mode_label.setText(mode)
            color = YELLOW if self._config.get("live_mode") else ACCENT
            self.home_tab.mode_label.setStyleSheet(f"""
                color: {color}; font-size: 11px; letter-spacing: 2px;
                padding: 4px 10px;
                border: 1px solid {color};
                border-radius: 4px;
            """)

    def _on_tab_changed(self, idx: int):
        if idx == 1 and self.pnl_tab._client is not None:
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
    app.setStyleSheet(STYLESHEET)

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

    # Load saved credentials
    config = load_config()

    # Show setup dialog if credentials are missing or incomplete
    needs_setup = not config.get("key_id") or not config.get("key_file")
    if needs_setup:
        dlg = CredentialDialog(config)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            sys.exit(0)   # user cancelled — exit cleanly
        config = load_config()   # reload after save

    # Apply credentials to environment
    apply_config(config)

    # Show main window with a settings button to re-open credential dialog
    window = MainWindow(config)
    window.show()
    sys.exit(app.exec())
