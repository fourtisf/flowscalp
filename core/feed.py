"""Market data feed: REST bootstrap + websocket subscriptions with supervision.

Multi-coin: every coin in env.coins gets its own buffers ("book"); the engine
still opens at most ONE position across all of them. Events carry the coin:

  ("candle_1m_closed", (coin, Candle)), ("candle_15m_closed", (coin, Candle)),
  ("mid", (coin, float)), ("fill", raw_userfill_dict)

Legacy single-coin attributes (candles_1m, deltas_1m, candles_15m, mid)
expose the PRIMARY (first) coin for older callers.

Design notes (verified against hyperliquid-python-sdk 0.24): the SDK
WebsocketManager thread has no reconnect — the supervisor here rebuilds it
with backoff, resubscribes every coin and re-bootstraps candle gaps via REST.
Trade messages carry the taker side ("B" buy / "A" sell); per-1m delta =
Σ(taker-buy sz) − Σ(taker-sell sz); bars predating the live stream get None.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Optional

from hyperliquid.info import Info
from hyperliquid.websocket_manager import WebsocketManager

from core.strategy import Candle

log = logging.getLogger("flowscalp.feed")

ONE_MIN_MS = 60_000
FIFTEEN_MIN_MS = 900_000
CANDLE_GRACE_MS = 3_000
WS_STALE_MS = 75_000
BOOTSTRAP_1M = 620
BOOTSTRAP_15M = 310


def now_ms() -> int:
    return int(time.time() * 1000)


def _to_candle(d: dict) -> Candle:
    return Candle(ts=int(d["t"]), open=float(d["o"]), high=float(d["h"]),
                  low=float(d["l"]), close=float(d["c"]), volume=float(d["v"]))


class CoinBook:
    """Per-coin market state."""

    def __init__(self) -> None:
        self.candles_1m: deque[Candle] = deque(maxlen=600)
        self.deltas_1m: deque[Optional[float]] = deque(maxlen=600)
        self.candles_15m: deque[Candle] = deque(maxlen=300)
        self.live_1m: Optional[Candle] = None
        self.live_15m: Optional[Candle] = None
        self.delta_acc: dict[int, float] = {}
        self.delta_live_from: Optional[int] = None
        self.funding_hourly: Optional[float] = None
        self.open_interest: Optional[float] = None
        self.asset_idx: Optional[int] = None


class Feed:
    def __init__(self, env, queue: asyncio.Queue, db=None):
        self.env = env
        self.coins: tuple = tuple(getattr(env, "coins", None) or (getattr(env, "coin", "BTC"),))
        self.queue = queue
        self.db = db
        self.books: dict[str, CoinBook] = {c: CoinBook() for c in self.coins}
        self.mids: dict[str, float] = {c: 0.0 for c in self.coins}
        self.last_tick_ts: int = 0

        self._rest: Optional[Info] = None
        self._ws: Optional[WebsocketManager] = None
        self._user_fills_addr: Optional[str] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    # -- legacy single-coin views (primary coin) --------------------------------
    @property
    def primary(self) -> str:
        return self.coins[0]

    def book(self, coin: str) -> CoinBook:
        return self.books[coin]

    def mid_of(self, coin: str) -> float:
        return self.mids.get(coin, 0.0)

    @property
    def candles_1m(self):
        return self.books[self.primary].candles_1m

    @property
    def deltas_1m(self):
        return self.books[self.primary].deltas_1m

    @property
    def candles_15m(self):
        return self.books[self.primary].candles_15m

    @property
    def mid(self) -> float:
        return self.mids.get(self.primary, 0.0)

    @property
    def current_funding_hourly(self) -> Optional[float]:
        return self.books[self.primary].funding_hourly

    # -- lifecycle -------------------------------------------------------------
    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._rest = await asyncio.to_thread(Info, self.env.api_url, True)
        await self._bootstrap_candles()
        self._tasks = [
            asyncio.create_task(self._ws_supervisor(), name="feed-ws"),
            asyncio.create_task(self._funding_poll(), name="feed-funding"),
            asyncio.create_task(self._finalizer(), name="feed-finalizer"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._ws is not None:
            await asyncio.to_thread(self._teardown_ws)

    def enable_user_fills(self, address: str) -> None:
        self._user_fills_addr = address
        if self._ws is not None:
            self._ws.subscribe({"type": "userFills", "user": address}, self._cb_user_fills)

    # -- REST bootstrap ---------------------------------------------------------
    async def _bootstrap_candles(self) -> None:
        end = now_ms()
        for coin in self.coins:
            b = self.books[coin]
            for bar in await self._rest_candles(coin, "1m", end - BOOTSTRAP_1M * ONE_MIN_MS, end):
                if bar.ts + ONE_MIN_MS <= end:
                    self._append_1m(coin, b, bar, delta=None, emit=False)
            for bar in await self._rest_candles(coin, "15m", end - BOOTSTRAP_15M * FIFTEEN_MIN_MS, end):
                if bar.ts + FIFTEEN_MIN_MS <= end:
                    self._append_15m(coin, b, bar, emit=False)
            log.info("bootstrap %s: %d x 1m, %d x 15m", coin, len(b.candles_1m), len(b.candles_15m))
        await self._log_event("INFO", f"feed bootstrap {', '.join(self.coins)};"
                                      " CVD delta warm-up starts now")

    async def _rest_candles(self, coin: str, interval: str, start: int, end: int) -> list[Candle]:
        raw = await asyncio.to_thread(self._rest.candles_snapshot, coin, interval, start, end)
        return [_to_candle(d) for d in raw]

    async def _rebootstrap_gap(self) -> None:
        end = now_ms()
        for coin in self.coins:
            b = self.books[coin]
            if b.candles_1m:
                start = b.candles_1m[-1].ts + ONE_MIN_MS
                if end - start > ONE_MIN_MS:
                    for bar in await self._rest_candles(coin, "1m", start, end):
                        if bar.ts + ONE_MIN_MS <= end:
                            self._append_1m(coin, b, bar, delta=None, emit=False)
            if b.candles_15m:
                start = b.candles_15m[-1].ts + FIFTEEN_MIN_MS
                if end - start > FIFTEEN_MIN_MS:
                    for bar in await self._rest_candles(coin, "15m", start, end):
                        if bar.ts + FIFTEEN_MIN_MS <= end:
                            self._append_15m(coin, b, bar, emit=False)

    # -- websocket supervision ---------------------------------------------------
    async def _ws_supervisor(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await asyncio.to_thread(self._build_ws)
                await self._rebootstrap_gap()
                await self._log_event("INFO", "feed websocket connected")
                backoff = 1.0
                while not self._stop.is_set():
                    await asyncio.sleep(2)
                    if not self._ws_healthy():
                        raise ConnectionError("websocket dead or stale")
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("feed ws: %s — reconnecting in %.0fs", e, backoff)
                await self._log_event("WARN", f"feed ws reconnect: {e}")
                await asyncio.to_thread(self._teardown_ws)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 30.0)

    def _ws_healthy(self) -> bool:
        if self._ws is None or not self._ws.is_alive() or not self._ws.ws.keep_running:
            return False
        return now_ms() - self.last_tick_ts < WS_STALE_MS

    def _build_ws(self) -> None:
        ws = WebsocketManager(self.env.api_url)
        ws.daemon = True
        ws.start()
        for coin in self.coins:
            ws.subscribe({"type": "candle", "coin": coin, "interval": "1m"}, self._cb_candle)
            ws.subscribe({"type": "candle", "coin": coin, "interval": "15m"}, self._cb_candle)
            ws.subscribe({"type": "trades", "coin": coin}, self._cb_trades)
        ws.subscribe({"type": "allMids"}, self._cb_mids)
        if self._user_fills_addr:
            ws.subscribe({"type": "userFills", "user": self._user_fills_addr}, self._cb_user_fills)
        self._ws = ws
        live_from = (now_ms() // ONE_MIN_MS + 1) * ONE_MIN_MS
        for b in self.books.values():
            b.delta_live_from = live_from
            b.delta_acc.clear()
        self.last_tick_ts = now_ms()

    def _teardown_ws(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                ws.stop()
            except Exception:  # noqa: BLE001
                pass

    # -- ws callbacks (manager thread → loop) -------------------------------------
    def _cb_candle(self, msg: dict) -> None:
        self._loop.call_soon_threadsafe(self._on_candle, msg.get("data", {}))

    def _cb_mids(self, msg: dict) -> None:
        self._loop.call_soon_threadsafe(self._on_mids, msg.get("data", {}))

    def _cb_trades(self, msg: dict) -> None:
        self._loop.call_soon_threadsafe(self._on_trades, msg.get("data", []))

    def _cb_user_fills(self, msg: dict) -> None:
        self._loop.call_soon_threadsafe(self._on_user_fills, msg.get("data", {}))

    # -- message handlers (loop thread) -------------------------------------------
    def _touch(self) -> None:
        self.last_tick_ts = now_ms()

    def _on_candle(self, d: dict) -> None:
        self._touch()
        coin = d.get("s")
        if not d or coin not in self.books:
            return
        b = self.books[coin]
        bar = _to_candle(d)
        if d.get("i") == "1m":
            if b.live_1m is not None and bar.ts > b.live_1m.ts:
                self._finalize_1m(coin, b, b.live_1m)
            if b.candles_1m and bar.ts <= b.candles_1m[-1].ts:
                return
            b.live_1m = bar
        elif d.get("i") == "15m":
            if b.live_15m is not None and bar.ts > b.live_15m.ts:
                self._append_15m(coin, b, b.live_15m, emit=True)
            if b.candles_15m and bar.ts <= b.candles_15m[-1].ts:
                return
            b.live_15m = bar

    def _on_mids(self, d: dict) -> None:
        self._touch()
        mids = d.get("mids", {})
        for coin in self.coins:
            px = mids.get(coin)
            if px is not None:
                self.mids[coin] = float(px)
                self.queue.put_nowait(("mid", (coin, self.mids[coin])))

    def _on_trades(self, trades: list) -> None:
        self._touch()
        for t in trades:
            coin = t.get("coin")
            if coin not in self.books:
                continue
            b = self.books[coin]
            sz = float(t["sz"])
            bucket = int(t["time"]) // ONE_MIN_MS * ONE_MIN_MS
            b.delta_acc[bucket] = b.delta_acc.get(bucket, 0.0) + (sz if t["side"] == "B" else -sz)

    def _on_user_fills(self, d: dict) -> None:
        self._touch()
        if d.get("isSnapshot"):
            return
        for f in d.get("fills", []):
            if f.get("coin") in self.books:
                self.queue.put_nowait(("fill", f))

    # -- bar finalization -----------------------------------------------------------
    def _finalize_1m(self, coin: str, b: CoinBook, bar: Candle) -> None:
        if b.candles_1m and bar.ts <= b.candles_1m[-1].ts:
            return
        if b.delta_live_from is not None and bar.ts >= b.delta_live_from:
            delta = b.delta_acc.pop(bar.ts, 0.0)
        else:
            delta = None
        for k in [k for k in b.delta_acc if k <= bar.ts - 5 * ONE_MIN_MS]:
            b.delta_acc.pop(k, None)
        self._append_1m(coin, b, bar, delta=delta, emit=True)

    def _append_1m(self, coin: str, b: CoinBook, bar: Candle,
                   delta: Optional[float], emit: bool) -> None:
        if b.candles_1m and bar.ts <= b.candles_1m[-1].ts:
            return
        b.candles_1m.append(bar)
        b.deltas_1m.append(delta)
        if emit:
            self.queue.put_nowait(("candle_1m_closed", (coin, bar)))

    def _append_15m(self, coin: str, b: CoinBook, bar: Candle, emit: bool) -> None:
        if b.candles_15m and bar.ts <= b.candles_15m[-1].ts:
            return
        b.candles_15m.append(bar)
        if emit:
            self.queue.put_nowait(("candle_15m_closed", (coin, bar)))

    async def _finalizer(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(1)
            t = now_ms()
            for coin, b in self.books.items():
                if b.live_1m is not None and t > b.live_1m.ts + ONE_MIN_MS + CANDLE_GRACE_MS:
                    live, b.live_1m = b.live_1m, None
                    self._finalize_1m(coin, b, live)
                if b.live_15m is not None and t > b.live_15m.ts + FIFTEEN_MIN_MS + CANDLE_GRACE_MS:
                    live15, b.live_15m = b.live_15m, None
                    self._append_15m(coin, b, live15, emit=True)

    # -- funding / OI poll ------------------------------------------------------------
    async def _funding_poll(self) -> None:
        while not self._stop.is_set():
            try:
                meta, ctxs = await asyncio.to_thread(self._rest.meta_and_asset_ctxs)
                for coin, b in self.books.items():
                    if b.asset_idx is None:
                        b.asset_idx = next((i for i, a in enumerate(meta["universe"])
                                            if a["name"] == coin), None)
                    if b.asset_idx is not None:
                        ctx = ctxs[b.asset_idx]
                        b.funding_hourly = float(ctx["funding"])
                        b.open_interest = float(ctx["openInterest"])
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("funding poll failed: %s", e)
            await asyncio.sleep(60)

    async def _log_event(self, level: str, msg: str) -> None:
        if self.db is not None:
            try:
                await self.db.log_event(level, msg)
            except Exception:  # noqa: BLE001
                log.exception("event log failed")
