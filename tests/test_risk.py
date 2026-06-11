from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from config import Settings
from core.risk import can_enter, floor_to_decimals, position_size
from core.strategy import Signal

S = Settings()  # risk_pct 0.5, leverage_cap 5
SZ_DEC = 5      # BTC szDecimals on Hyperliquid


def test_position_size_exact_numbers():
    r = position_size(10_000, 100_000, 99_600, S, SZ_DEC)
    assert r.ok
    assert r.size_usd == pytest.approx(12_500)
    assert r.effective_lev == pytest.approx(1.25)
    assert r.size_btc == pytest.approx(0.125)
    assert not r.lev_capped


def test_leverage_cap():
    r = position_size(10_000, 100_000, 99_900, S, SZ_DEC)  # 0.1% SL → lev exactly 5.0
    assert r.ok and r.effective_lev == pytest.approx(5.0) and not r.lev_capped
    r = position_size(10_000, 100_000, 99_950, S, SZ_DEC)  # 0.05% SL → uncapped lev 10
    assert r.ok and r.lev_capped
    assert r.effective_lev == pytest.approx(5.0)
    assert r.size_usd == pytest.approx(50_000)
    assert r.size_btc == pytest.approx(0.5)


def test_min_notional_skip():
    r = position_size(50, 100_000, 96_000, S, SZ_DEC)
    assert not r.ok and "below min notional" in r.skip_reason


def test_zero_sl_distance_skip():
    r = position_size(10_000, 100_000, 100_000, S, SZ_DEC)
    assert not r.ok and r.skip_reason == "zero SL distance"


def test_floor_to_decimals():
    assert floor_to_decimals(0.123456789, 5) == 0.12345
    assert floor_to_decimals(0.9999999, 3) == 0.999
    assert floor_to_decimals(1.0, 0) == 1.0


def _state(**over):
    now_ms = int(datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    base = dict(bot_state="RUNNING", last_tick_ts=now_ms - 1000, day_start_equity=10_000.0,
                day_realized_pnl=0.0, consec_losses=0, last_exit_bar_ts=0,
                last_swept_level={"long": None, "short": None})
    base.update(over)
    return SimpleNamespace(**base)


NOW = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)


def test_can_enter_happy_path():
    ok, why = can_enter(_state(), S, NOW)
    assert ok, why


def test_can_enter_blocks_non_running_states():
    for st in ("PAUSED", "STOPPED", "COOLDOWN", "HALTED_DAILY", "RECONCILE"):
        ok, why = can_enter(_state(bot_state=st), S, NOW)
        assert not ok and st in why


def test_can_enter_blocks_blackout_and_stale():
    ok, why = can_enter(_state(), S, datetime(2026, 1, 5, 13, 20, tzinfo=timezone.utc))
    assert not ok and why == "blackout window"
    stale = _state(last_tick_ts=int(NOW.timestamp() * 1000) - 31_000)
    ok, why = can_enter(stale, S, NOW)
    assert not ok and why == "stale feed"


def test_can_enter_daily_loss_and_streak():
    ok, why = can_enter(_state(day_realized_pnl=-300.0), S, NOW)  # 3% of 10k
    assert not ok and why == "daily loss limit"
    ok, why = can_enter(_state(consec_losses=4), S, NOW)
    assert not ok and why == "max consec losses"
    ok, _ = can_enter(_state(consec_losses=3), S, NOW)
    assert ok


def test_can_enter_post_exit_spacing_and_swept_dedupe():
    bar_ts = int(NOW.timestamp() * 1000)
    st = _state(last_exit_bar_ts=bar_ts - 2 * 60_000)
    ok, why = can_enter(st, S, NOW, bar_ts=bar_ts)
    assert not ok and "post-exit spacing" in why
    st = _state(last_exit_bar_ts=bar_ts - 3 * 60_000)
    ok, _ = can_enter(st, S, NOW, bar_ts=bar_ts)
    assert ok
    sig = Signal("long", 100_000, 99_800, 100_360, swept_level=99_900, reason="t")
    st = _state(last_swept_level={"long": 99_900, "short": None})
    ok, why = can_enter(st, S, NOW, sig=sig)
    assert not ok and "same swept level" in why
    st = _state(last_swept_level={"long": 99_800, "short": None})
    ok, _ = can_enter(st, S, NOW, sig=sig)
    assert ok
