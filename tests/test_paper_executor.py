import pytest

from config import FALLBACK_MAKER_FEE, FALLBACK_TAKER_FEE
from core.executor import PaperExecutor
from core.risk import SizeResult
from core.state import Position
from core.strategy import Candle, Signal
from db import Database


class _Env:
    paper_start_equity = 10_000.0
    hl_main_wallet_address = ""
    hl_subaccount_address = None
    api_url = "https://api.hyperliquid.xyz"
    trading_address = ""


T0 = 1_700_000_000_000


def bar(low, high, ts=T0, o=None, c=None):
    return Candle(ts, o or (low + high) / 2, high, low, c or (low + high) / 2, 100.0)


@pytest.fixture
async def ex(tmp_path):
    db = Database(str(tmp_path / "p.db"))
    await db.connect()
    fills = []

    async def collect(f):
        fills.append(f)

    e = PaperExecutor(_Env(), db, mid_provider=lambda: 100_000.0, on_fill=collect)
    await e.boot()  # fee fetch fails offline → fallback constants, logged
    e.fills = fills
    yield e
    await db.close()


SIG = Signal("long", 100_000.0, 99_800.0, 100_360.0, swept_level=99_900, reason="t")
SIZE = SizeResult(True, size_btc=0.1, size_usd=10_000.0, effective_lev=1.0)


async def test_maker_entry_touch_is_not_a_fill(ex):
    await ex.place_entry(SIG, SIZE)
    await ex.on_bar_closed(bar(low=100_000.0, high=100_050.0))   # touches exactly
    assert ex.fills == []
    await ex.on_bar_closed(bar(low=99_999.0, high=100_050.0))    # trades through
    assert len(ex.fills) == 1
    f = ex.fills[0]
    assert f.kind == "entry" and f.px == 100_000.0 and f.sz == 0.1
    assert f.fee_usd == pytest.approx(100_000.0 * 0.1 * FALLBACK_MAKER_FEE)


async def test_maker_entry_mid_path_strictly_through(ex):
    await ex.place_entry(SIG, SIZE)
    await ex.on_mid(100_000.0)      # touch
    assert ex.fills == []
    await ex.on_mid(99_999.5)       # through
    assert len(ex.fills) == 1 and ex.fills[0].kind == "entry"


async def test_short_entry_mirror(ex):
    sig = Signal("short", 100_000.0, 100_200.0, 99_640.0, swept_level=100_100, reason="t")
    await ex.place_entry(sig, SIZE)
    await ex.on_bar_closed(bar(low=99_900.0, high=100_000.0))    # touch from below
    assert ex.fills == []
    await ex.on_bar_closed(bar(low=99_900.0, high=100_001.0))
    assert len(ex.fills) == 1 and ex.fills[0].side == "A"


def _pos(side="long", entry=100_000.0, sl=99_800.0, tp=100_300.0, sz=0.1):
    return Position(side=side, entry_px=entry, size_btc=sz, size_usd=entry * sz,
                    sl_px=sl, tp_px=tp, ts_open=T0, filled_sz=sz,
                    oids={"entry": "P0"})


async def test_bar_containing_both_sl_and_tp_fills_sl_first(ex):
    pos = _pos()
    await ex.arm_exits(pos)
    await ex.on_bar_closed(bar(low=99_700.0, high=100_400.0))    # both inside the bar
    assert len(ex.fills) == 1
    f = ex.fills[0]
    assert f.kind == "sl" and f.px == 99_800.0 and f.oid == pos.oids["sl"]
    assert f.fee_usd == pytest.approx(99_800.0 * 0.1 * FALLBACK_TAKER_FEE)


async def test_tp_fill_when_only_tp_hit_pays_maker_fee(ex):
    pos = _pos()
    await ex.arm_exits(pos)
    await ex.on_bar_closed(bar(low=99_900.0, high=100_350.0))
    f = ex.fills[0]
    assert f.kind == "tp" and f.px == 100_300.0
    assert f.fee_usd == pytest.approx(100_300.0 * 0.1 * FALLBACK_MAKER_FEE)


async def test_short_exit_mirror_sl_first(ex):
    pos = _pos(side="short", entry=100_000.0, sl=100_200.0, tp=99_700.0)
    await ex.arm_exits(pos)
    await ex.on_bar_closed(bar(low=99_600.0, high=100_250.0))
    f = ex.fills[0]
    assert f.kind == "sl" and f.px == 100_200.0 and f.side == "B"


async def test_time_stop_flatten_at_mid_with_1bp_slip_and_taker_fee(ex):
    pos = _pos()
    await ex.arm_exits(pos)
    await ex.flatten("time_stop")
    f = ex.fills[0]
    expected_px = 100_000.0 * (1 - 0.0001)   # 1bp adverse for a long exit
    assert f.kind == "flatten" and f.reason == "time_stop"
    assert f.px == pytest.approx(expected_px)
    assert f.sz == pytest.approx(0.1)
    assert f.fee_usd == pytest.approx(expected_px * 0.1 * FALLBACK_TAKER_FEE)
    # nothing left armed: another bar produces no further fills
    await ex.on_bar_closed(bar(low=99_000.0, high=101_000.0))
    assert len(ex.fills) == 1


async def test_equity_ledger_persists(ex):
    assert await ex.equity() == 10_000.0
    await ex.apply_realized(+123.45)
    assert await ex.equity() == pytest.approx(10_123.45)
    # a fresh executor on the same DB restores the ledger
    e2 = PaperExecutor(_Env(), ex.db, lambda: 0.0, ex.on_fill)
    await e2.boot()
    assert await e2.equity() == pytest.approx(10_123.45)
