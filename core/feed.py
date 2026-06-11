"""Market data feed: REST bootstrap + websocket subscriptions with supervision.

Pushes events onto the engine queue:
  ("candle_1m_closed", Candle), ("candle_15m_closed", Candle),
  ("mid", float), ("fill", raw_userfill_dict)

Design notes (verified against hyperliquid-python-sdk 0.24 source):
- The SDK WebsocketManager is a thread wrapping websocket-client's run_forever
  with NO reconnect: when the socket drops, the thread exits. The supervisor
  task here detects that (thread death or message staleness) and rebuilds the
  manager with exponential backoff 1s→30s, resubscribes, and re-bootstraps the
  candle gap via REST candleSnapshot.
- WS callbacks fire on the manager thread; they are bridged onto the asyncio
  loop with call_soon_threadsafe, so all buffer mutations happen on the loop.
- Hyperliquid candle messages update the LIVE bar in place; a bar is treated
  as closed when a message with a newer open-ts arrives (timestamp rollover)
  or when the wall clock passes its close time + grace (dead-tape minutes).
- Trade messages carry the taker side in the "side" field: "B" = taker buy,
  "A" = taker sell. Per-1m delta = Σ(taker-buy sz) − Σ(taker-sell sz).
  Bars that predate the live trades stream have delta=None (CVD warm-up).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any, Optional

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


class Feed:
    def __init__(self, env, queue: asyncio.Queue, db=None):
        self.env = env
        self.queue = queue
        self.db = db
        self.candles_1m: deque[Candle] = deque(maxlen=600)
        self.deltas_1m: deque[Optional[float]] = deque(maxlen=600)  # aligned 1:1 with candles_1m
        self.candles_15m: deque[Candle] = deque(maxlen=300)
        self.mid: float = 0.0
        self.last_tick_ts: int = 0          # last ws message of any kind, ms
        self.current_funding_hourly: Optional[float] = None
        self.open_interest: Optional[float] = None

        self._live_1m: Optional[Candle] = None
        self._live_15m: Optional[Candle] = None
        self._delta_acc: dict[int, float] = {}
        self._delta_live_from: Optional[int] = None  # first 1m bucket with complete trade data
        self._rest: Optional[Info] = None
        self._ws: Optional[WebsocketManager] = None
        self._user_fills_addr: Optional[str] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._btc_asset_idx: Optional[int] = None

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
        """Subscribe userFills for the trading account (live mode)."""
        self._user_fills_addr = address
        if self._ws is not None:
            self._ws.subscribe({"type": "userFills", "user": address}, self._cb_user_fills)

    # -- REST bootstrap ---------------------------------------------------------
    async def _bootstrap_candles(self) -> None:
        end = now_ms()
        c1 = await self._rest_candles("1m", end - BOOTSTRAP_1M * ONE_MIN_MS, end)
        c15 = await self._rest_candles("15m", end - BOOTSTRAP_15M * FIFTEEN_MIN_MS, end)
        for bar in c1:
            if bar.ts + ONE_MIN_MS <= end:  # closed only
                self._append_1m(bar, delta=None, emit=False)
        for bar in c15:
            if bar.ts + FIFTEEN_MIN_MS <= end:
                self._append_15m(bar, emit=False)
        log.info("bootstrap: %d x 1m, %d x 15m closed candles", len(self.candles_1m), len(self.candles_15m))
        await self._log_event("INFO", f"feed bootstrap {len(self.candles_1m)}x1m {len(self.candles_15m)}x15m;"
                                      " CVD delta warm-up starts now")

    async def _rest_candles(self, interval: str, start: int, end: int) -> list[Candle]:
        raw = await asyncio.to_thread(self._rest.candles_snapshot, "BTC", interval, start, end)
        return [_to_candle(d) for d in raw]

    async def _rebootstrap_gap(self) -> None:
        """After a reconnect, backfill 1m/15m bars missed during the outage."""
        end = now_ms()
        if self.candles_1m:
            start = self.candles_1m[-1].ts + ONE_MIN_MS
            if end - start > ONE_MIN_MS:
                for bar in await self._rest_candles("1m", start, end):
                    if bar.ts + ONE_MIN_MS <= end:
                        self._append_1m(bar, delta=None, emit=False)  # gap bars: no trade data
        else:
            await self._bootstrap_candles()
        if self.candles_15m:
            start = self.candles_15m[-1].ts + FIFTEEN_MIN_MS
            if end - start > FIFTEEN_MIN_MS:
                for bar in await self._rest_candles("15m", start, end):
                    if bar.ts + FIFTEEN_MIN_MS <= end:
                        self._append_15m(bar, emit=False)

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
            except Exception as e:
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
        """Runs in a worker thread: construct manager + queue subscriptions."""
        ws = WebsocketManager(self.env.api_url)
        ws.daemon = True
        ws.start()
        ws.subscribe({"type": "candle", "coin": "BTC", "interval": "1m"}, self._cb_candle)
        ws.subscribe({"type": "candle", "coin": "BTC", "interval": "15m"}, self._cb_candle)
        ws.subscribe({"type": "allMids"}, self._cb_mids)
        ws.subscribe({"type": "trades", "coin": "BTC"}, self._cb_trades)
        if self._user_fills_addr:
            ws.subscribe({"type": "userFills", "user": self._user_fills_addr}, self._cb_user_fills)
        self._ws = ws
        # trades from a partial first minute would under-count delta: live from next boundary
        self._delta_live_from = (now_ms() // ONE_MIN_MS + 1) * ONE_MIN_MS
        self._delta_acc.clear()
        self.last_tick_ts = now_ms()

    def _teardown_ws(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                ws.stop()
            except Exception:
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
        if not d or d.get("s") != "BTC":
            return
        bar = _to_candle(d)
        if d.get("i") == "1m":
            live = self._live_1m
            if live is not None and bar.ts > live.ts:
                self._finalize_1m(live)
            if self.candles_1m and bar.ts <= self.candles_1m[-1].ts:
                return  # stale update for an already-closed bar
            self._live_1m = bar
        elif d.get("i") == "15m":
            live = self._live_15m
            if live is not None and bar.ts > live.ts:
                self._append_15m(live, emit=True)
            if self.candles_15m and bar.ts <= self.candles_15m[-1].ts:
                return
            self._live_15m = bar

    def _on_mids(self, d: dict) -> None:
        self._touch()
        px = d.get("mids", {}).get("BTC")
        if px is not None:
            self.mid = float(px)
            self.queue.put_nowait(("mid", self.mid))

    def _on_trades(self, trades: list) -> None:
        self._touch()
        for t in trades:
            if t.get("coin") != "BTC":
                continue
            sz = float(t["sz"])
            bucket = int(t["time"]) // ONE_MIN_MS * ONE_MIN_MS
            self._delta_acc[bucket] = self._delta_acc.get(bucket, 0.0) + (sz if t["side"] == "B" else -sz)

    def _on_user_fills(self, d: dict) -> None:
        self._touch()
        if d.get("isSnapshot"):
            return  # historical snapshot on subscribe, not new fills
        for f in d.get("fills", []):
            if f.get("coin") == "BTC":
                self.queue.put_nowait(("fill", f))

    # -- bar finalization -----------------------------------------------------------
    def _finalize_1m(self, bar: Candle) -> None:
        if self.candles_1m and bar.ts <= self.candles_1m[-1].ts:
            return
        if self._delta_live_from is not None and bar.ts >= self._delta_live_from:
            delta = self._delta_acc.pop(bar.ts, 0.0)
        else:
            delta = None
        for k in [k for k in self._delta_acc if k <= bar.ts - 5 * ONE_MIN_MS]:
            self._delta_acc.pop(k, None)
        self._append_1m(bar, delta=delta, emit=True)

    def _append_1m(self, bar: Candle, delta: Optional[float], emit: bool) -> None:
        if self.candles_1m and bar.ts <= self.candles_1m[-1].ts:
            return
        self.candles_1m.append(bar)
        self.deltas_1m.append(delta)
        if emit:
            self.queue.put_nowait(("candle_1m_closed", bar))

    def _append_15m(self, bar: Candle, emit: bool) -> None:
        if self.candles_15m and bar.ts <= self.candles_15m[-1].ts:
            return
        self.candles_15m.append(bar)
        if emit:
            self.queue.put_nowait(("candle_15m_closed", bar))

    async def _finalizer(self) -> None:
        """Close out live bars on dead-tape minutes (no rollover message arrives)."""
        while not self._stop.is_set():
            await asyncio.sleep(1)
            t = now_ms()
            live = self._live_1m
            if live is not None and t > live.ts + ONE_MIN_MS + CANDLE_GRACE_MS:
                self._live_1m = None
                self._finalize_1m(live)
            live15 = self._live_15m
            if live15 is not None and t > live15.ts + FIFTEEN_MIN_MS + CANDLE_GRACE_MS:
                self._live_15m = None
                self._append_15m(live15, emit=True)

    # -- funding / OI poll ------------------------------------------------------------
    async def _funding_poll(self) -> None:
        while not self._stop.is_set():
            try:
                meta, ctxs = await asyncio.to_thread(self._rest.meta_and_asset_ctxs)
                if self._btc_asset_idx is None:
                    self._btc_asset_idx = next(i for i, a in enumerate(meta["universe"]) if a["name"] == "BTC")
                ctx = ctxs[self._btc_asset_idx]
                self.current_funding_hourly = float(ctx["funding"])
                self.open_interest = float(ctx["openInterest"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("funding poll failed: %s", e)
            await asyncio.sleep(60)

    async def _log_event(self, level: str, msg: str) -> None:
        if self.db is not None:
            try:
                await self.db.log_event(level, msg)
            except Exception:
                log.exception("event log failed")
