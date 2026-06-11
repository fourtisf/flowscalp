"""Feed message-handling logic, driven without a network connection."""
import asyncio

import pytest

from core.feed import Feed, ONE_MIN_MS


class _Env:
    api_url = "https://api.hyperliquid.xyz"


@pytest.fixture
def feed():
    f = Feed(_Env(), asyncio.Queue())
    f._delta_live_from = 1_000_000 // ONE_MIN_MS * ONE_MIN_MS  # pretend trades stream is live
    return f


def _candle_msg(ts, px=100_000.0, interval="1m", vol=5.0):
    return {"t": ts, "T": ts + ONE_MIN_MS, "s": "BTC", "i": interval,
            "o": str(px), "h": str(px + 10), "l": str(px - 10), "c": str(px), "v": str(vol)}


async def test_candle_close_on_rollover_and_delta_alignment(feed):
    t0 = feed._delta_live_from
    feed._on_trades([{"coin": "BTC", "side": "B", "sz": "1.5", "time": t0 + 1000},
                     {"coin": "BTC", "side": "A", "sz": "0.5", "time": t0 + 2000}])
    feed._on_candle(_candle_msg(t0))
    feed._on_candle(_candle_msg(t0, px=100_005.0))      # live bar update, no close
    assert feed.queue.qsize() == 0 and len(feed.candles_1m) == 0
    feed._on_candle(_candle_msg(t0 + ONE_MIN_MS))       # rollover → t0 bar closed
    kind, bar = feed.queue.get_nowait()
    assert kind == "candle_1m_closed" and bar.ts == t0 and bar.close == 100_005.0
    assert list(feed.deltas_1m) == [1.0]                # 1.5 buy − 0.5 sell


async def test_pre_stream_bars_get_none_delta(feed):
    t0 = feed._delta_live_from
    feed._on_candle(_candle_msg(t0 - ONE_MIN_MS))       # bar predating live trades
    feed._on_candle(_candle_msg(t0))
    assert list(feed.deltas_1m) == [None]


async def test_duplicate_and_stale_candles_ignored(feed):
    t0 = feed._delta_live_from
    feed._on_candle(_candle_msg(t0))
    feed._on_candle(_candle_msg(t0 + ONE_MIN_MS))
    feed._on_candle(_candle_msg(t0))                    # stale resend
    assert len(feed.candles_1m) == 1
    assert feed._live_1m.ts == t0 + ONE_MIN_MS


async def test_finalizer_closes_dead_tape_bar(feed):
    t_old = (feed._delta_live_from // ONE_MIN_MS - 100) * ONE_MIN_MS
    feed._live_1m = None
    feed._on_candle(_candle_msg(t_old))                 # bar way in the past, no successor
    feed._stop.set()  # finalizer runs one sweep below
    live = feed._live_1m
    assert live is not None
    feed._live_1m = None
    feed._finalize_1m(live)
    kind, bar = feed.queue.get_nowait()
    assert kind == "candle_1m_closed" and bar.ts == t_old
    assert list(feed.deltas_1m) == [None]               # before delta_live_from? no — t_old < live_from


async def test_mid_and_userfills_routing(feed):
    feed._on_mids({"mids": {"BTC": "100123.5", "ETH": "4000"}})
    kind, mid = feed.queue.get_nowait()
    assert kind == "mid" and mid == 100_123.5 and feed.mid == 100_123.5
    feed._on_user_fills({"isSnapshot": True, "fills": [{"coin": "BTC", "px": "1"}]})
    assert feed.queue.qsize() == 0                      # snapshots skipped
    feed._on_user_fills({"fills": [{"coin": "BTC", "px": "100000", "sz": "0.1"},
                                   {"coin": "ETH", "px": "4000", "sz": "1"}]})
    kind, fill = feed.queue.get_nowait()
    assert kind == "fill" and fill["coin"] == "BTC"
    assert feed.queue.qsize() == 0                      # non-BTC dropped
