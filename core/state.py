"""Single shared BotState (asyncio.Lock-guarded) + JSON crash-recovery snapshot.

Bot states:  BOOT → RECONCILE → RUNNING ⇄ PAUSED; RUNNING → COOLDOWN(t) → RUNNING;
RUNNING → HALTED_DAILY → (00:00 UTC) → RUNNING; any → STOPPED → /resume → RECONCILE.
Position substates: FLAT → PENDING_ENTRY → IN_POSITION → EXITING → FLAT.

A JSON snapshot is persisted on every transition so a crash mid-position can be
recovered (live truth is still reconciled from the exchange on boot).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

log = logging.getLogger("flowscalp.state")


class Bot(str, Enum):
    BOOT = "BOOT"
    RECONCILE = "RECONCILE"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COOLDOWN = "COOLDOWN"
    HALTED_DAILY = "HALTED_DAILY"
    STOPPED = "STOPPED"


class Pos(str, Enum):
    FLAT = "FLAT"
    PENDING_ENTRY = "PENDING_ENTRY"
    IN_POSITION = "IN_POSITION"
    EXITING = "EXITING"


@dataclass
class Position:
    side: str                  # long | short
    entry_px: float            # signal px until filled, then volume-weighted fill px
    size_btc: float
    size_usd: float
    sl_px: float
    tp_px: float
    swept_level: float = 0.0
    reason: str = ""
    ts_open: int = 0           # entry fill time, ms
    entry_risk_usd: float = 0.0
    trade_id: Optional[int] = None
    oids: dict = field(default_factory=dict)   # "entry" | "tp" | "sl" → order id str
    filled_sz: float = 0.0
    exit_sz: float = 0.0
    exit_px: float = 0.0       # volume-weighted
    fees_usd: float = 0.0
    placed_ts: int = 0         # entry order placement, ms
    taker_sent_ts: int = 0     # IOC fallback entry sent, ms (0 = none)
    pending_exit_reason: str = ""
    sl_cross_ts: int = 0       # engine-side backup stop bookkeeping
    tp_is_manual: bool = False # TP was pulled to mid by the eco-close button
    entry_tx: str = ""         # on-chain proof hashes (live fills)
    exit_tx: str = ""
    entry_tx_crossed: bool = False  # our order was the taker in that tx
    exit_tx_crossed: bool = False
    coin: str = "BTC"          # symbol this position belongs to


def utc_day(ts_ms: Optional[int] = None) -> str:
    dt = datetime.fromtimestamp((ts_ms or time.time() * 1000) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def utc_midnight_ms(ts_ms: Optional[int] = None) -> int:
    dt = datetime.fromtimestamp((ts_ms or time.time() * 1000) / 1000, tz=timezone.utc)
    mid = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(mid.timestamp() * 1000)


class BotState:
    def __init__(self, snapshot_path: str, mode: str = "paper"):
        self.lock = asyncio.Lock()
        self.snapshot_path = snapshot_path
        self.bot_state: Bot = Bot.BOOT
        self.pos_state: Pos = Pos.FLAT
        self.position: Optional[Position] = None
        self.mode: str = mode
        self.day_utc: str = utc_day()
        self.day_start_ts: int = utc_midnight_ms()
        self.day_start_equity: float = 0.0
        self.day_realized_pnl: float = 0.0
        self.day_realized_r: float = 0.0
        self.day_trades: int = 0
        self.consec_losses: int = 0
        self.cooldown_until: int = 0           # ms
        self.last_exit_bar_ts: int = 0         # open-ts of the 1m bar containing the last exit
        self.last_swept_level: dict = {"long": None, "short": None}
        self.last_tick_ts: int = 0
        self.boot_ts: int = int(time.time() * 1000)
        self.halted_reason: str = ""

    # -- transitions -----------------------------------------------------------
    def set_bot_state(self, new: Bot) -> None:
        if new != self.bot_state:
            log.info("state %s → %s", self.bot_state.value, new.value)
        self.bot_state = new
        self.save_snapshot()

    def set_pos_state(self, new: Pos) -> None:
        if new != self.pos_state:
            log.info("pos %s → %s", self.pos_state.value, new.value)
        self.pos_state = new
        self.save_snapshot()

    # -- snapshot ----------------------------------------------------------------
    def snapshot_dict(self) -> dict:
        return {
            "ts": int(time.time() * 1000),
            "bot_state": self.bot_state.value,
            "pos_state": self.pos_state.value,
            "position": asdict(self.position) if self.position else None,
            "mode": self.mode,
            "day_utc": self.day_utc,
            "day_start_ts": self.day_start_ts,
            "day_start_equity": self.day_start_equity,
            "day_realized_pnl": self.day_realized_pnl,
            "day_realized_r": self.day_realized_r,
            "day_trades": self.day_trades,
            "consec_losses": self.consec_losses,
            "cooldown_until": self.cooldown_until,
            "last_exit_bar_ts": self.last_exit_bar_ts,
            "last_swept_level": self.last_swept_level,
            "halted_reason": self.halted_reason,
        }

    def save_snapshot(self) -> None:
        try:
            d = os.path.dirname(self.snapshot_path)
            if d:
                os.makedirs(d, exist_ok=True)
            tmp = self.snapshot_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.snapshot_dict(), f)
            os.replace(tmp, self.snapshot_path)
        except OSError:
            log.exception("state snapshot write failed")

    @staticmethod
    def load_snapshot(path: str) -> Optional[dict]:
        try:
            with open(path) as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    def restore(self, snap: dict) -> None:
        """Restore day counters / anti-overtrade memory (same UTC day only) and
        any open position recorded before the crash."""
        if snap.get("day_utc") == self.day_utc:
            self.day_start_equity = snap.get("day_start_equity", 0.0)
            self.day_realized_pnl = snap.get("day_realized_pnl", 0.0)
            self.day_realized_r = snap.get("day_realized_r", 0.0)
            self.day_trades = snap.get("day_trades", 0)
            self.consec_losses = snap.get("consec_losses", 0)
            self.halted_reason = snap.get("halted_reason", "")
            if snap.get("bot_state") == Bot.HALTED_DAILY.value:
                self.bot_state = Bot.HALTED_DAILY
        self.cooldown_until = snap.get("cooldown_until", 0)
        if self.cooldown_until > int(time.time() * 1000):
            self.bot_state = Bot.COOLDOWN
        self.last_exit_bar_ts = snap.get("last_exit_bar_ts", 0)
        self.last_swept_level = snap.get("last_swept_level", {"long": None, "short": None})
        p = snap.get("position")
        if p and snap.get("pos_state") in (Pos.IN_POSITION.value, Pos.EXITING.value):
            self.position = Position(**p)
            self.pos_state = Pos.IN_POSITION
