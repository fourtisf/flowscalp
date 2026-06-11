"""Position sizing (reference implementation, §6.4) and engine-level entry gates.

R accounting on close: r_multiple = pnl_usd / entry_risk_usd, where
entry_risk_usd = size_usd * sl_dist is captured at entry. A trade counts as a
loss for streak purposes when r_multiple < 0.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core import timewin
from core.strategy import Signal

MIN_NOTIONAL_USD = 10.0   # verify against current Hyperliquid minimum at boot if exposed

STALE_FEED_MS = 30_000
POST_EXIT_BAR_SPACING = 3


@dataclass
class SizeResult:
    ok: bool
    size_btc: float = 0.0
    size_usd: float = 0.0
    effective_lev: float = 0.0
    lev_capped: bool = False
    skip_reason: str = ""


def floor_to_decimals(x: float, d: int) -> float:
    f = 10 ** d
    return math.floor(x * f) / f


def position_size(equity: float, entry: float, sl: float, s, sz_decimals: int) -> SizeResult:
    risk_usd = equity * s.risk_pct / 100
    sl_dist = abs(entry - sl) / entry
    if sl_dist <= 0:
        return SizeResult(False, skip_reason="zero SL distance")
    pos_usd = risk_usd / sl_dist
    lev = pos_usd / equity
    capped = False
    if lev > s.leverage_cap:
        pos_usd = equity * s.leverage_cap
        lev = s.leverage_cap
        capped = True   # realized risk < risk_pct; log it
    size_btc = floor_to_decimals(pos_usd / entry, sz_decimals)
    size_usd = size_btc * entry
    if size_usd < MIN_NOTIONAL_USD:
        return SizeResult(False, skip_reason=f"below min notional (${size_usd:.2f})")
    return SizeResult(True, size_btc, size_usd, lev, capped)


def can_enter(state, s, now_utc: datetime, bar_ts: Optional[int] = None,
              sig: Optional[Signal] = None) -> tuple[bool, str]:
    """Engine-level gates, checked in order. `state` is the shared BotState.
    With `sig` supplied, additionally enforces the swept-level dedupe."""
    if state.bot_state != "RUNNING":
        return False, f"state {state.bot_state}"

    minute = now_utc.hour * 60 + now_utc.minute
    if s.blackout_windows and timewin.in_any_window(minute, s.blackout_windows):
        return False, "blackout window"

    now_ms = int(now_utc.timestamp() * 1000)
    if not state.last_tick_ts or now_ms - state.last_tick_ts > STALE_FEED_MS:
        return False, "stale feed"

    if state.day_start_equity > 0:
        limit = s.daily_loss_limit_pct / 100 * state.day_start_equity
        if state.day_realized_pnl <= -limit:
            return False, "daily loss limit"

    if state.consec_losses >= s.max_consec_losses:
        return False, "max consec losses"

    if bar_ts is not None and state.last_exit_bar_ts:
        bars_since = (bar_ts - state.last_exit_bar_ts) // 60_000
        if bars_since < POST_EXIT_BAR_SPACING:
            return False, f"post-exit spacing ({bars_since} bars)"

    if sig is not None and state.last_swept_level.get(sig.side) == sig.swept_level:
        return False, f"same swept level {sig.swept_level:.0f} ({sig.side})"

    return True, ""
