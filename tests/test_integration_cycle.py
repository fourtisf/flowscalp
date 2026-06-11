"""Full paper trade cycle through the real Engine + PaperExecutor + DB.

Replaces the live-feed soak (network is unavailable in CI): a synthetic feed
drives signal → maker fill → exit and the test asserts DB rows, R math,
alerts, circuit breakers and the kill switch.
"""
import asyncio
from collections import deque

import pytest

from config import SettingsStore
from core.engine import Engine
from core.state import Bot, BotState, Pos
from core.strategy import Candle
from db import Database
from tests.synth import flat_15m, long_sweep_same_bar

MIN = 60_000


class _Env:
    paper_start_equity = 10_000.0
    hl_main_wallet_address = ""
    hl_subaccount_address = None
    hl_agent_private_key = ""
    api_url = "https://api.hyperliquid.xyz"
    trading_address = ""
    mode = "paper"


class FakeFeed:
    def __init__(self, c1m, c15m):
        self.candles_1m = deque(c1m, maxlen=600)
        self.deltas_1m = deque([0.0] * len(c1m), maxlen=600)
        self.candles_15m = deque(c15m, maxlen=300)
        self.mid = c1m[-1].close
        self.last_tick_ts = 0
        self.current_funding_hourly = None

    def enable_user_fills(self, addr):
        pass

    def push_bar(self, bar):
        self.candles_1m.append(bar)
        self.deltas_1m.append(0.0)
        self.mid = bar.close
        import time
        self.last_tick_ts = int(time.time() * 1000)


class StubNotifier:
    def __init__(self):
        self.msgs = []
        self.errors = []

    async def channel(self, text):
        self.msgs.append(text)

    async def error(self, text):
        self.errors.append(text)


@pytest.fixture
async def harness(tmp_path):
    dbs = []

    async def make(**setting_overrides):
        db = Database(str(tmp_path / f"i{len(dbs)}.db"))
        await db.connect()
        dbs.append(db)
        store = SettingsStore(db)
        await store.load()
        for k, v in setting_overrides.items():
            ok, _, err = await store.set(k, v)
            assert ok, err
        c1m = long_sweep_same_bar(vol_sweep=200.0)  # 2x volume → passes default 1.7x gate
        feed = FakeFeed(c1m[:-1], flat_15m())
        state = BotState(str(tmp_path / f"snap{len(dbs)}.json"), mode="paper")
        notifier = StubNotifier()
        engine = Engine(_Env(), db, store, state, feed, asyncio.Queue(), notifier)
        await engine.boot()
        assert state.bot_state == Bot.RUNNING
        return engine, db, feed, state, notifier, c1m[-1]

    yield make
    for db in dbs:
        await db.close()


async def _drive_bar(engine, feed, bar):
    feed.push_bar(bar)
    await engine._dispatch("candle_1m_closed", bar)


async def test_full_cycle_signal_fill_tp_exit(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    t = sig_bar.ts

    await _drive_bar(engine, feed, sig_bar)             # signal bar → entry placed
    assert state.pos_state == Pos.PENDING_ENTRY
    pos = state.position
    entry, sl, tp = pos.entry_px, pos.sl_px, pos.tp_px
    assert entry == 99_960.0
    assert sl == pytest.approx(99_845.0 * 0.9995)
    assert tp == pytest.approx(entry + 1.8 * (entry - sl))
    assert any("placing LONG" in m for m in notifier.msgs)

    fill_bar = Candle(t + MIN, 99_950.0, 99_990.0, 99_940.0, 99_970.0, 100.0)  # trades through
    await _drive_bar(engine, feed, fill_bar)
    assert state.pos_state == Pos.IN_POSITION
    pos = state.position
    assert pos.filled_sz > 0 and pos.trade_id is not None
    assert pos.entry_risk_usd == pytest.approx(pos.size_usd * (entry - sl) / entry)
    assert any("LONG BTC" in m and "(paper)" in m for m in notifier.msgs)

    size, risk_usd, fees_entry = pos.filled_sz, pos.entry_risk_usd, pos.fees_usd
    tp_bar = Candle(t + 2 * MIN, 99_980.0, tp + 50, 99_975.0, tp + 10, 100.0)
    await _drive_bar(engine, feed, tp_bar)
    assert state.pos_state == Pos.FLAT and state.position is None

    rows = await db.recent_trades("paper", 10)
    assert len(rows) == 1
    tr = rows[0]
    expected_fees = fees_entry + tp * size * engine.executor.maker_fee
    expected_pnl = (tp - entry) * size - expected_fees
    assert tr["exit_reason"] == "tp"
    assert tr["exit_px"] == pytest.approx(tp)
    assert tr["pnl_usd"] == pytest.approx(expected_pnl, rel=1e-9)
    assert tr["r_multiple"] == pytest.approx(expected_pnl / risk_usd, rel=1e-9)
    assert 1.6 < tr["r_multiple"] < 1.8                  # ~1.8R minus fees
    assert await engine.executor.equity() == pytest.approx(10_000.0 + expected_pnl)
    assert state.day_realized_pnl == pytest.approx(expected_pnl)
    assert state.consec_losses == 0
    assert any("CLOSED LONG BTC" in m for m in notifier.msgs)
    # equity snapshot written on close
    curve = await db.equity_curve("paper", 0)
    assert curve and curve[-1]["equity"] == pytest.approx(10_000.0 + expected_pnl)


async def test_sl_exit_daily_loss_halt_and_rollover(harness):
    engine, db, feed, state, notifier, sig_bar = await harness(
        risk_pct="2.0", daily_loss_limit_pct="1.0")
    t = sig_bar.ts

    await _drive_bar(engine, feed, sig_bar)
    await _drive_bar(engine, feed, Candle(t + MIN, 99_950.0, 99_990.0, 99_940.0, 99_970.0, 100.0))
    assert state.pos_state == Pos.IN_POSITION
    sl = state.position.sl_px

    await _drive_bar(engine, feed, Candle(t + 2 * MIN, 99_940.0, 99_950.0, sl - 30, sl - 10, 100.0))
    assert state.pos_state == Pos.FLAT
    tr = (await db.recent_trades("paper", 1))[0]
    assert tr["exit_reason"] == "sl" and tr["r_multiple"] < 0
    assert state.consec_losses == 1
    # −2% realized vs 1% daily limit → halted
    assert state.bot_state == Bot.HALTED_DAILY
    assert any("HALTED_DAILY" in m for m in notifier.msgs)

    # halted: a fresh signal bar must not open anything
    sig2 = Candle(t + 5 * MIN, 99_950.0, 100_000.0, 99_845.0, 99_960.0, 200.0)
    await _drive_bar(engine, feed, sig2)
    assert state.pos_state == Pos.FLAT and state.position is None

    # next UTC day → halt cleared, day counters reset, report posted
    await engine._dispatch("timer", t + 36 * 60 * MIN)
    assert state.bot_state == Bot.RUNNING
    assert state.day_realized_pnl == 0.0
    assert any("DAILY REPORT" in m for m in notifier.msgs)


async def test_kill_switch_flattens_and_stops(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    t = sig_bar.ts
    await _drive_bar(engine, feed, sig_bar)
    await _drive_bar(engine, feed, Candle(t + MIN, 99_950.0, 99_990.0, 99_940.0, 99_970.0, 100.0))
    assert state.pos_state == Pos.IN_POSITION
    feed.mid = 99_980.0

    res = await engine._cmd_stop()
    assert "stopped" in res
    assert state.bot_state == Bot.STOPPED and state.pos_state == Pos.FLAT
    tr = (await db.recent_trades("paper", 1))[0]
    assert tr["exit_reason"] == "kill"
    assert tr["exit_px"] == pytest.approx(99_980.0 * 0.9999)  # mid − 1bp adverse
    assert any("KILL SWITCH" in m for m in notifier.msgs)
    assert state.last_swept_level["long"] == 99_900

    # while STOPPED a signal bar must not enter
    sig2 = Candle(t + 2 * MIN, 99_950.0, 100_000.0, 99_845.0, 99_960.0, 200.0)
    await _drive_bar(engine, feed, sig2)
    assert state.pos_state == Pos.FLAT

    # /resume goes back to RUNNING through RECONCILE
    res = await engine._cmd_resume()
    assert res == "running" and state.bot_state == Bot.RUNNING

    # anti-overtrade: an identical sweep of the SAME level (after the old sweep
    # bar ages out of the lookback window and spacing has passed) is deduped
    state.last_exit_bar_ts = 0  # neutralize wall-clock kill timestamp for replay
    last_ts = sig2.ts
    for i in range(1, 33):
        b = Candle(last_ts + i * MIN, 100_000.0, 100_100.0, 99_900.0, 100_000.0, 100.0)
        await _drive_bar(engine, feed, b)
    sweep_again = Candle(last_ts + 33 * MIN, 99_950.0, 100_000.0, 99_845.0, 99_960.0, 200.0)
    await _drive_bar(engine, feed, sweep_again)
    assert state.pos_state == Pos.FLAT
    ev = await db.fetchall("SELECT msg FROM events WHERE msg LIKE '%same swept level%'")
    assert ev, "swept-level dedupe should have been logged"


async def test_time_stop_via_timer(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    t = sig_bar.ts
    await _drive_bar(engine, feed, sig_bar)
    await _drive_bar(engine, feed, Candle(t + MIN, 99_950.0, 99_990.0, 99_940.0, 99_970.0, 100.0))
    assert state.pos_state == Pos.IN_POSITION
    feed.mid = 99_985.0
    ts_open = state.position.ts_open

    await engine._dispatch("timer", ts_open + 14 * MIN)   # before time stop
    assert state.pos_state == Pos.IN_POSITION
    await engine._dispatch("timer", ts_open + 15 * MIN)   # 15m default → flatten
    assert state.pos_state == Pos.FLAT
    tr = (await db.recent_trades("paper", 1))[0]
    assert tr["exit_reason"] == "time_stop"
    assert any("⏱" in m for m in notifier.msgs)


async def test_testtrade_injects_full_cycle(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    feed.mid = 100_000.0
    res = await engine._cmd_testtrade()
    assert "test trade dipasang" in res
    assert state.pos_state == Pos.PENDING_ENTRY
    entry_px = state.position.entry_px
    await engine._dispatch("mid", entry_px - 5)       # tick through → maker fill
    assert state.pos_state == Pos.IN_POSITION
    assert any("LONG BTC" in m for m in notifier.msgs)
    res2 = await engine._cmd_testtrade()
    assert "tidak bisa" in res2                       # one position at a time
    await engine._cmd_stop()                          # close it out
    assert (await db.recent_trades("paper", 1))[0]["exit_reason"] == "kill"
    await engine._cmd_resume()
    state.mode = "live"
    assert "butuh konfirmasi" in await engine._cmd_testtrade()


async def test_manual_close_button_path(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    feed.mid = 100_000.0
    await engine._cmd_testtrade()
    await engine._dispatch("mid", state.position.entry_px - 5)   # fill entry
    assert state.pos_state == Pos.IN_POSITION
    feed.mid = state.position.entry_px + 40
    res = await engine._cmd_closepos()
    assert "ditutup" in res and state.pos_state == Pos.FLAT
    tr = (await db.recent_trades("paper", 1))[0]
    assert tr["exit_reason"] == "manual"
    assert "tidak ada posisi" in await engine._cmd_closepos()


async def test_eco_close_fills_as_maker_and_books_manual(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    feed.mid = 100_000.0
    await engine._cmd_testtrade()
    await engine._dispatch("mid", state.position.entry_px - 5)   # fill entry
    assert state.pos_state == Pos.IN_POSITION
    entry = state.position.entry_px

    feed.mid = entry + 30
    res = await engine._cmd_closemaker()
    assert "limit penutup dipasang" in res
    assert state.position.tp_px == pytest.approx(entry + 30)

    # a bar trading through the pulled limit fills it as MAKER
    maker_fee = engine.executor.maker_fee
    bar = Candle(sig_bar.ts + 5 * MIN, entry, entry + 60, entry - 10, entry + 40, 100.0)
    await engine._dispatch("candle_1m_closed", bar)
    assert state.pos_state == Pos.FLAT
    tr = (await db.recent_trades("paper", 1))[0]
    assert tr["exit_reason"] == "manual"                  # eco close, honest label
    assert tr["exit_px"] == pytest.approx(entry + 30)
    expected_exit_fee = (entry + 30) * tr["size_btc"] * maker_fee
    assert tr["fees_usd"] == pytest.approx(
        entry * tr["size_btc"] * maker_fee + expected_exit_fee, rel=1e-9)


async def test_live_testtrade_requires_confirmation_and_uses_minimal_size(harness):
    engine, db, feed, state, notifier, sig_bar = await harness()
    feed.mid = 100_000.0
    state.mode = "live"   # paper executor underneath: exercises the code path safely

    res = await engine._cmd_testtrade()                  # no confirmation
    assert "butuh konfirmasi" in res
    assert state.pos_state == Pos.FLAT

    res = await engine._cmd_testtrade(live_ok=True)
    assert "REAL" in res and state.pos_state == Pos.PENDING_ENTRY
    pos = state.position
    assert pos.size_btc == pytest.approx(0.00012, abs=1e-9)   # ceil($11/$99,990)
    assert pos.size_btc * pos.entry_px == pytest.approx(11.9988, abs=0.05)
    assert "REAL" in pos.reason


async def test_live_fill_hash_appears_as_tx_proof(harness):
    """Fills carrying an L1 hash (live) put an explorer link in both alerts."""
    from core.executor import Fill
    engine, db, feed, state, notifier, sig_bar = await harness()
    feed.mid = 100_000.0
    await engine._cmd_testtrade()
    pos = state.position
    entry_fill = Fill(oid=pos.oids["entry"], px=pos.entry_px, sz=pos.size_btc,
                      side="B", time=1, fee_usd=0.1, kind="entry", tx_hash="0xentryhash")
    await engine.handle_fill(entry_fill)
    assert state.pos_state == Pos.IN_POSITION
    assert any("explorer/tx/0xentryhash" in m for m in notifier.msgs)
    # maker fill (crossed=False) → counterparty caveat present
    assert any("explorer/tx/0xentryhash" in m and "LAWAN" in m for m in notifier.msgs)
    exit_fill = Fill(oid=pos.oids["sl"], px=pos.sl_px, sz=pos.filled_sz,
                     side="A", time=2, fee_usd=0.1, kind="sl", tx_hash="0xexithash",
                     crossed=True)
    await engine.handle_fill(exit_fill)
    assert state.pos_state == Pos.FLAT
    # taker fill (our own action hash) → clean link, no caveat
    exit_msg = next(m for m in notifier.msgs if "explorer/tx/0xexithash" in m)
    assert "LAWAN" not in exit_msg
    # paper fills carry no hash → no link line
    assert not any("explorer/tx/\n" in m for m in notifier.msgs)
