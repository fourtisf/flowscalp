"""Log scrubber, boot-mode policy, crash-snapshot restore."""
import asyncio
import logging
from collections import deque

import pytest

from config import SettingsStore
from core.engine import Engine
from core.state import Bot, BotState, Pos
from db import Database
from main import RedactFilter, decide_boot_mode
from tests.synth import flat_15m, long_sweep_same_bar
from tests.test_integration_cycle import FakeFeed, StubNotifier, _Env


def _records(msgs):
    out = []
    h = logging.Handler()
    h.emit = lambda r: out.append(r.getMessage())
    h.addFilter(RedactFilter())
    lg = logging.Logger("t")
    lg.addHandler(h)
    for m in msgs:
        lg.info(m)
    return out


def test_redact_filter_scrubs_key_material():
    key = "0x" + "ab" * 32
    bare = "cd" * 32
    out = _records([f"loaded key {key} ok", f"sig {bare} done", "normal message",
                    "two %s keys", f"prefix {key} and {bare}"])
    joined = "\n".join(out)
    assert key not in joined and bare not in joined
    assert joined.count("[REDACTED_KEY]") >= 3
    assert "normal message" in joined


def test_boot_mode_policy():
    class E:
        mode = "paper"
        hl_agent_private_key = "k"
        hl_main_wallet_address = "a"

    # no snapshot → paper
    assert decide_boot_mode(E(), None)[0] == "paper"
    # MODE=live in env alone is ignored (confirm flow required)
    e = E()
    e.mode = "live"
    mode, note = decide_boot_mode(e, None)
    assert mode == "paper" and "confirm" in note
    # live snapshot WITHOUT open position → paper
    assert decide_boot_mode(E(), {"mode": "live", "position": None})[0] == "paper"
    # live snapshot WITH open position → crash recovery resumes live
    snap = {"mode": "live", "position": {"side": "long"}}
    mode, note = decide_boot_mode(E(), snap)
    assert mode == "live" and "crash recovery" in note
    # …but only when credentials exist
    e2 = E()
    e2.hl_agent_private_key = ""
    assert decide_boot_mode(e2, snap)[0] == "paper"


async def test_paper_position_survives_restart(tmp_path):
    """kill -9 mid-position (paper): restart restores the position from the
    snapshot and re-arms exits — same trade closes normally afterwards."""
    db = Database(str(tmp_path / "r.db"))
    await db.connect()
    try:
        store = SettingsStore(db)
        await store.load()
        c1m = long_sweep_same_bar(vol_sweep=200.0)
        feed = FakeFeed(c1m[:-1], flat_15m())
        snap_path = str(tmp_path / "snap.json")
        state = BotState(snap_path, mode="paper")
        engine = Engine(_Env(), db, store, state, feed, asyncio.Queue(), StubNotifier())
        await engine.boot()

        t = c1m[-1].ts
        from core.strategy import Candle
        feed.push_bar(c1m[-1])
        await engine._dispatch("candle_1m_closed", c1m[-1])
        fill = Candle(t + 60_000, 99_950.0, 99_990.0, 99_940.0, 99_970.0, 100.0)
        feed.push_bar(fill)
        await engine._dispatch("candle_1m_closed", fill)
        assert state.pos_state == Pos.IN_POSITION
        trade_id, tp = state.position.trade_id, state.position.tp_px
        # no clean shutdown — snapshot on disk is all that survives

        state2 = BotState(snap_path, mode="paper")
        notifier2 = StubNotifier()
        engine2 = Engine(_Env(), db, store, state2, feed, asyncio.Queue(), notifier2)
        await engine2.boot()
        assert state2.bot_state == Bot.RUNNING
        assert state2.pos_state == Pos.IN_POSITION
        assert state2.position.trade_id == trade_id

        tp_bar = Candle(t + 5 * 60_000, 99_980.0, tp + 50, 99_975.0, tp + 10, 100.0)
        feed.push_bar(tp_bar)
        await engine2._dispatch("candle_1m_closed", tp_bar)
        assert state2.pos_state == Pos.FLAT
        rows = await db.recent_trades("paper", 5)
        assert len(rows) == 1 and rows[0]["id"] == trade_id and rows[0]["exit_reason"] == "tp"
    finally:
        await db.close()
