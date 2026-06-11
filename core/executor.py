"""Order execution: paper simulator and Hyperliquid live executor.

Both implement the same contract (§6.5). All exchange SDK calls are sync
(requests/websocket-client) and are run via asyncio.to_thread with jittered
retries — nothing blocks the event loop.

Verified against hyperliquid-python-sdk 0.24:
- post-only entry  → order_type {"limit": {"tif": "Alo"}}
- IOC              → {"limit": {"tif": "Ioc"}}
- stop trigger     → {"trigger": {"triggerPx": px, "isMarket": True, "tpsl": "sl"}}
- agent wallet     → Exchange(wallet=agent_key, account_address=main_wallet)
- subaccount       → Exchange(..., vault_address=subaccount_addr); info queries
                     (user_state / open_orders / userFills) target the subaccount.
- fee rates        → info.user_fees(addr): userAddRate = maker, userCrossRate = taker
- price rule       → ≤5 significant figures and ≤(6−szDecimals) decimals;
                     integer prices always allowed (relevant for BTC ≥ 100k).
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Protocol

from config import FALLBACK_MAKER_FEE, FALLBACK_TAKER_FEE
from core.risk import SizeResult
from core.state import Position
from core.strategy import Candle, Signal

log = logging.getLogger("flowscalp.executor")

TAKER_SLIP = 0.0001          # 1 bp adverse slippage on simulated taker fills
FLATTEN_BOUND = 0.002        # IOC limit bound for flatten/kill: mid ± 0.2 %
TAKER_ENTRY_BOUND = 0.001    # IOC bound for taker entry fallback: mid ± 0.1 %
SL_TRIGGER_BOUND = 0.05      # exchange-side stop-market limit guard: ± 5 %
PAPER_EQUITY_KEY = "paper_equity"


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Fill:
    oid: str
    px: float
    sz: float
    side: str        # "B" buy | "A" sell
    time: int        # ms
    fee_usd: float
    kind: str = ""   # entry | tp | sl | flatten | taker_entry ("" for raw live fills)
    reason: str = ""
    tx_hash: str = ""  # HyperCore L1 hash (live fills) — public proof
    crossed: bool = False  # True = our order was the taker (hash is OUR action)
    coin: str = ""


class ExecError(Exception):
    pass


class BaseExecutor(Protocol):
    name: str
    sz_decimals: int
    maker_fee: float
    taker_fee: float

    async def boot(self) -> None: ...                # fee rates, szDecimals, reconcile
    async def equity(self) -> float: ...
    async def place_entry(self, sig: Signal, size: SizeResult) -> str: ...
    async def cancel_entry(self, oid: str) -> None: ...
    async def arm_exits(self, pos: Position) -> None: ...   # TP limit + SL trigger
    async def flatten(self, reason: str) -> None: ...       # IOC, reduce-only
    async def cancel_all(self) -> None: ...


def round_px(px: float, sz_decimals: int) -> float:
    """Hyperliquid price rule: ≤5 significant figures, ≤(6−szDecimals)
    decimals; integer prices are always allowed."""
    if px >= 10 ** 5:
        return float(round(px))
    return round(float(f"{px:.5g}"), 6 - sz_decimals)


# =============================================================================
# Paper
# =============================================================================
@dataclass
class _PendingEntry:
    oid: str
    side: str       # long | short
    px: float
    sz: float
    coin: str = ""


class PaperExecutor:
    """Simulates fills from the candle/mid stream.

    - Maker entry fills only when price trades THROUGH the limit (mid < px for
      longs, or a later 1m bar with low < px) — a touch is not a fill.
    - Exits intra-bar on each closed 1m candle; if SL and TP both lie inside
      the bar's range, SL is assumed filled first (conservative).
    - Taker fills (time stop, kill, taker entry) at current mid with 1 bp
      adverse slippage. Fees per side from live rates when fetchable, else
      config fallback constants (logged).
    """
    name = "paper"

    def __init__(self, env, db, mid_provider: Callable[[], float],
                 on_fill: Callable[[Fill], Awaitable[None]]):
        self.env = env
        self.coin = getattr(env, "coin", "BTC")
        self.coins: tuple = tuple(getattr(env, "coins", None) or (self.coin,))
        self.db = db
        self.mid = mid_provider
        self.on_fill = on_fill
        self.sz_decimals = 5
        self._szd: dict = {c: 5 for c in self.coins}
        self.maker_fee = FALLBACK_MAKER_FEE
        self.taker_fee = FALLBACK_TAKER_FEE
        self._equity = env.paper_start_equity
        self._pending: Optional[_PendingEntry] = None
        self._armed: Optional[Position] = None
        self._oid_seq = 0
        # unique per executor instance so ids never collide with oids restored
        # from a pre-restart snapshot (collision once mislabeled a manual
        # close as an SL exit)
        self._oid_ns = f"P{now_ms() % 100_000_000}"

    def _next_oid(self) -> str:
        self._oid_seq += 1
        return f"{self._oid_ns}-{self._oid_seq}"

    async def boot(self) -> None:
        stored = await self.db.get_setting(PAPER_EQUITY_KEY)
        if stored is not None:
            self._equity = float(stored)
        try:
            await self._fetch_live_rates()
        except Exception as e:
            log.warning("paper: live fee fetch failed (%s); using fallback maker %.4f%% taker %.4f%%",
                        e, self.maker_fee * 100, self.taker_fee * 100)
            await self.db.log_event("WARN", f"paper fee rates fallback (fetch failed: {e})")

    async def _fetch_live_rates(self) -> None:
        if not self.env.hl_main_wallet_address:
            raise ExecError("no wallet address configured")
        from hyperliquid.info import Info
        info = await asyncio.to_thread(Info, self.env.api_url, True)
        fees = await asyncio.to_thread(info.user_fees, self.env.trading_address)
        self.maker_fee = float(fees["userAddRate"])
        self.taker_fee = float(fees["userCrossRate"])
        meta = await asyncio.to_thread(info.meta)
        for a in meta["universe"]:
            if a["name"] in self._szd:
                self._szd[a["name"]] = int(a["szDecimals"])
        self.sz_decimals = self._szd.get(self.coin, 5)
        log.info("paper: live rates maker %.4f%% taker %.4f%%, szDecimals %d",
                 self.maker_fee * 100, self.taker_fee * 100, self.sz_decimals)

    def sz_dec(self, coin: str) -> int:
        return self._szd.get(coin, self.sz_decimals)

    def _mid(self, coin: str) -> float:
        try:
            return self.mid(coin)
        except TypeError:           # legacy no-arg provider (tests)
            return self.mid()

    async def equity(self) -> float:
        return self._equity

    async def apply_realized(self, pnl_usd: float) -> None:
        self._equity += pnl_usd
        await self.db.set_setting(PAPER_EQUITY_KEY, repr(self._equity))

    async def place_entry(self, sig: Signal, size: SizeResult, coin: str = "") -> str:
        oid = self._next_oid()
        self._pending = _PendingEntry(oid, sig.side, sig.entry_px, size.size_btc,
                                      coin or self.coin)
        return oid

    async def cancel_entry(self, oid: str) -> None:
        if self._pending and self._pending.oid == oid:
            self._pending = None

    async def taker_entry(self, sig: Signal, sz: float, coin: str = "") -> None:
        """IOC fallback after maker timeout: fill at mid with 1bp adverse slip."""
        coin = coin or self.coin
        mid = self._mid(coin) or sig.entry_px
        px = mid * (1 + TAKER_SLIP) if sig.side == "long" else mid * (1 - TAKER_SLIP)
        oid = self._next_oid()
        await self.on_fill(Fill(oid, px, sz, "B" if sig.side == "long" else "A",
                                now_ms(), px * sz * self.taker_fee, kind="taker_entry",
                                coin=coin))

    async def arm_exits(self, pos: Position) -> None:
        if "tp" not in pos.oids:
            pos.oids["tp"] = self._next_oid()
        if "sl" not in pos.oids:
            pos.oids["sl"] = self._next_oid()
        self._armed = pos

    async def cancel_all(self) -> None:
        self._pending = None

    async def move_tp(self, pos: Position, new_px: float) -> None:
        self._armed = pos  # px already updated on the shared Position

    def disarm(self) -> None:
        self._armed = None

    async def flatten(self, reason: str, coin: str = "") -> None:
        pos = self._armed
        if pos is None:
            return
        remaining = pos.filled_sz - pos.exit_sz
        if remaining <= 0:
            return
        mid = self._mid(pos.coin or coin or self.coin) or pos.entry_px
        px = mid * (1 - TAKER_SLIP) if pos.side == "long" else mid * (1 + TAKER_SLIP)
        self._armed = None
        await self.on_fill(Fill(self._next_oid(), px, remaining,
                                "A" if pos.side == "long" else "B",
                                now_ms(), px * remaining * self.taker_fee,
                                kind="flatten", reason=reason, coin=pos.coin))

    # -- simulation drivers (called by the engine) ------------------------------
    async def on_mid(self, mid: float, coin: str = "") -> None:
        p = self._pending
        if p is None or (coin and p.coin and p.coin != coin):
            return
        if (p.side == "long" and mid < p.px) or (p.side == "short" and mid > p.px):
            await self._fill_entry(p, now_ms())

    async def on_bar_closed(self, bar: Candle, coin: str = "") -> None:
        # the engine drives bars through here BEFORE evaluating/placing entries,
        # so a pending entry is only ever tested against bars after its signal bar
        p = self._pending
        if p is not None and not (coin and p.coin and p.coin != coin):
            traded_through = bar.low < p.px if p.side == "long" else bar.high > p.px
            if traded_through:
                await self._fill_entry(p, bar.ts + 60_000)
        await self._check_exits(bar, coin)

    async def _fill_entry(self, p: _PendingEntry, ts: int) -> None:
        self._pending = None
        await self.on_fill(Fill(p.oid, p.px, p.sz, "B" if p.side == "long" else "A",
                                ts, p.px * p.sz * self.maker_fee, kind="entry",
                                coin=p.coin))

    async def _check_exits(self, bar: Candle, coin: str = "") -> None:
        pos = self._armed
        if pos is None or (coin and pos.coin and pos.coin != coin):
            return
        remaining = pos.filled_sz - pos.exit_sz
        if remaining <= 0:
            return
        ts = bar.ts + 60_000
        if pos.side == "long":
            sl_hit, tp_hit = bar.low <= pos.sl_px, bar.high >= pos.tp_px
        else:
            sl_hit, tp_hit = bar.high >= pos.sl_px, bar.low <= pos.tp_px
        if sl_hit:  # SL-first when both lie inside the bar (conservative)
            self._armed = None
            await self.on_fill(Fill(pos.oids["sl"], pos.sl_px, remaining,
                                    "A" if pos.side == "long" else "B",
                                    ts, pos.sl_px * remaining * self.taker_fee, kind="sl",
                                    coin=pos.coin))
        elif tp_hit:
            self._armed = None
            await self.on_fill(Fill(pos.oids["tp"], pos.tp_px, remaining,
                                    "A" if pos.side == "long" else "B",
                                    ts, pos.tp_px * remaining * self.maker_fee, kind="tp",
                                    coin=pos.coin))


# =============================================================================
# Live (Hyperliquid)
# =============================================================================
class LiveExecutor:
    name = "live"

    def __init__(self, env, db, mid_provider: Callable[[], float]):
        self.env = env
        self.coin = getattr(env, "coin", "BTC")
        self.coins: tuple = tuple(getattr(env, "coins", None) or (self.coin,))
        self.db = db
        self.mid = mid_provider
        self.sz_decimals = 5
        self._szd: dict = {c: 5 for c in self.coins}
        self.maker_fee = FALLBACK_MAKER_FEE
        self.taker_fee = FALLBACK_TAKER_FEE
        self._info = None
        self._exchange = None

    def sz_dec(self, coin: str) -> int:
        return self._szd.get(coin, self.sz_decimals)

    def _mid(self, coin: str) -> float:
        try:
            return self.mid(coin)
        except TypeError:
            return self.mid()

    # -- plumbing -----------------------------------------------------------------
    async def _retry(self, label: str, fn, *args, **kw):
        last = None
        for attempt in range(3):
            try:
                return await asyncio.to_thread(fn, *args, **kw)
            except Exception as e:  # noqa: BLE001 — network/SDK errors retried
                last = e
                wait = (0.3 * (2 ** attempt)) * (1 + random.random())
                log.warning("%s failed (attempt %d/3): %s", label, attempt + 1, e)
                await asyncio.sleep(wait)
        await self.db.log_event("ERROR", f"{label} failed after retries: {last}")
        raise ExecError(f"{label}: {last}") from last

    @staticmethod
    def _oid_from_response(resp: dict) -> tuple[str, Optional[dict]]:
        """Returns (oid, immediate_fill_or_None). Raises ExecError on rejection."""
        if resp.get("status") != "ok":
            raise ExecError(f"order rejected: {resp}")
        statuses = resp["response"]["data"]["statuses"]
        st = statuses[0]
        if "error" in st:
            raise ExecError(f"order error: {st['error']}")
        if "resting" in st:
            return str(st["resting"]["oid"]), None
        if "filled" in st:
            return str(st["filled"]["oid"]), st["filled"]
        raise ExecError(f"unexpected order status: {st}")

    # -- contract --------------------------------------------------------------------
    async def boot(self) -> None:
        import eth_account
        from hyperliquid.exchange import Exchange
        from hyperliquid.info import Info

        if not self.env.hl_agent_private_key:
            raise ExecError("API key agent belum diisi — set lewat /setkey")
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", self.env.trading_address or ""):
            raise ExecError("alamat wallet utama belum diisi/salah — set lewat /setwallet"
                            " (alamat 0x… dari pojok kanan atas app Hyperliquid)")
        wallet = eth_account.Account.from_key(self.env.hl_agent_private_key)
        self._info = await asyncio.to_thread(Info, self.env.api_url, True)
        self._exchange = await asyncio.to_thread(
            Exchange, wallet, self.env.api_url, None,
            self.env.hl_subaccount_address or None,            # vault_address → subaccount
            self.env.hl_main_wallet_address or None,           # account_address → master
        )
        meta = await self._retry("meta", self._info.meta)
        for a in meta["universe"]:
            if a["name"] in self._szd:
                self._szd[a["name"]] = int(a["szDecimals"])
        self.sz_decimals = self._szd.get(self.coin, 5)
        fees = await self._retry("user_fees", self._info.user_fees, self.env.trading_address)
        self.maker_fee = float(fees["userAddRate"])
        self.taker_fee = float(fees["userCrossRate"])
        log.info("live boot: szDecimals %d, maker %.4f%%, taker %.4f%%, account %s%s",
                 self.sz_decimals, self.maker_fee * 100, self.taker_fee * 100,
                 self.env.trading_address[:10],
                 " (subaccount)" if self.env.hl_subaccount_address else "")

    async def reconcile_raw(self, coin: str = "") -> tuple[float, float, list]:
        """Returns (position szi, entry px, open orders) for one coin."""
        coin = coin or self.coin
        st = await self._retry("user_state", self._info.user_state, self.env.trading_address)
        szi, entry_px = 0.0, 0.0
        for ap in st.get("assetPositions", []):
            p = ap["position"]
            if p["coin"] == coin and float(p["szi"]) != 0:
                szi = float(p["szi"])
                entry_px = float(p["entryPx"] or 0)
        orders = await self._retry("open_orders", self._info.open_orders, self.env.trading_address)
        return szi, entry_px, [o for o in orders if o["coin"] == coin]

    async def equity(self) -> float:
        st = await self._retry("user_state", self._info.user_state, self.env.trading_address)
        return float(st["marginSummary"]["accountValue"])

    async def spot_usdc(self) -> float:
        """USDC sitting in the SPOT account (not tradeable for perps)."""
        try:
            st = await self._retry("spot_state", self._info.spot_user_state, self.env.trading_address)
            return sum(float(b.get("total", 0) or 0)
                       for b in st.get("balances", []) if b.get("coin") == "USDC")
        except Exception:  # noqa: BLE001 — diagnostic only
            return 0.0

    async def apply_realized(self, pnl_usd: float) -> None:
        pass  # live equity comes from the exchange

    async def place_entry(self, sig: Signal, size: SizeResult, coin: str = "") -> str:
        coin = coin or self.coin
        px = round_px(sig.entry_px, self.sz_dec(coin))
        resp = await self._retry(
            "place_entry", self._exchange.order, coin, sig.side == "long",
            size.size_btc, px, {"limit": {"tif": "Alo"}}, False)
        oid, _ = self._oid_from_response(resp)
        return oid

    async def cancel_entry(self, oid: str, coin: str = "") -> None:
        try:
            await self._retry("cancel", self._exchange.cancel, coin or self.coin, int(oid))
        except ExecError as e:
            log.info("cancel_entry %s: %s (likely already filled/canceled)", oid, e)

    async def taker_entry(self, sig: Signal, sz: float, coin: str = "") -> None:
        """IOC limit at mid ± 0.1% bound. Fill arrives via the userFills stream."""
        coin = coin or self.coin
        mid = self._mid(coin)
        if not mid:
            raise ExecError("no mid for taker entry")
        bound = mid * (1 + TAKER_ENTRY_BOUND) if sig.side == "long" else mid * (1 - TAKER_ENTRY_BOUND)
        resp = await self._retry(
            "taker_entry", self._exchange.order, coin, sig.side == "long",
            sz, round_px(bound, self.sz_dec(coin)), {"limit": {"tif": "Ioc"}}, False)
        self._oid_from_response(resp)

    async def arm_exits(self, pos: Position) -> None:
        coin = pos.coin or self.coin
        dec = self.sz_dec(coin)
        is_buy_exit = pos.side == "short"
        sz = pos.filled_sz or pos.size_btc
        tp_px = round_px(pos.tp_px, dec)
        resp = await self._retry(
            "arm_tp", self._exchange.order, coin, is_buy_exit, sz, tp_px,
            {"limit": {"tif": "Gtc"}}, True)
        pos.oids["tp"], _ = self._oid_from_response(resp)

        sl_px = round_px(pos.sl_px, dec)
        guard = sl_px * (1 + SL_TRIGGER_BOUND) if is_buy_exit else sl_px * (1 - SL_TRIGGER_BOUND)
        resp = await self._retry(
            "arm_sl", self._exchange.order, coin, is_buy_exit, sz,
            round_px(guard, dec),
            {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}}, True)
        pos.oids["sl"], _ = self._oid_from_response(resp)
        await self.db.log_event("INFO", f"exits armed tp {tp_px} (oid {pos.oids['tp']})"
                                        f" sl {sl_px} (oid {pos.oids['sl']})")

    async def cancel_all(self) -> None:
        all_orders = await self._retry("open_orders", self._info.open_orders,
                                       self.env.trading_address)
        cancels = [{"coin": o["coin"], "oid": o["oid"]}
                   for o in all_orders if o["coin"] in self.coins]
        if cancels:
            await self._retry("cancel_all", self._exchange.bulk_cancel, cancels)

    async def move_tp(self, pos: Position, new_px: float) -> None:
        """Re-point the reduce-only TP limit (eco close pulls it to mid)."""
        coin = pos.coin or self.coin
        old = pos.oids.get("tp")
        if old:
            await self.cancel_entry(old, coin)
        is_buy_exit = pos.side == "short"
        sz = pos.filled_sz - pos.exit_sz
        resp = await self._retry(
            "move_tp", self._exchange.order, coin, is_buy_exit, sz,
            round_px(new_px, self.sz_dec(coin)), {"limit": {"tif": "Gtc"}}, True)
        pos.oids["tp"], _ = self._oid_from_response(resp)

    async def flatten(self, reason: str, coin: str = "") -> None:
        """Cancel all, then IOC reduce-only at mid ± 0.2%; retry once on partial."""
        coin = coin or self.coin
        await self.cancel_all()
        for _ in range(2):
            szi, _, _ = await self.reconcile_raw(coin)
            if szi == 0:
                return
            is_buy = szi < 0
            mid = self._mid(coin)
            if not mid:
                st = await self._retry("all_mids", self._info.all_mids)
                mid = float(st[coin])
            bound = mid * (1 + FLATTEN_BOUND) if is_buy else mid * (1 - FLATTEN_BOUND)
            resp = await self._retry(
                "flatten", self._exchange.order, coin, is_buy, abs(szi),
                round_px(bound, self.sz_dec(coin)), {"limit": {"tif": "Ioc"}}, True)
            if resp.get("status") != "ok":
                await self.db.log_event("ERROR", f"flatten attempt failed: {resp}")
        szi, _, _ = await self.reconcile_raw(coin)
        if szi != 0:
            await self.db.log_event("ERROR", f"flatten incomplete, szi={szi}")
            raise ExecError(f"flatten incomplete, szi={szi}")

    def to_fill(self, raw: dict) -> Fill:
        """Normalize a userFills websocket entry."""
        return Fill(oid=str(raw["oid"]), px=float(raw["px"]), sz=float(raw["sz"]),
                    side=raw["side"], time=int(raw["time"]),
                    fee_usd=float(raw.get("fee", 0) or 0),
                    tx_hash=str(raw.get("hash", "") or ""),
                    crossed=bool(raw.get("crossed", False)),
                    coin=str(raw.get("coin", "") or ""))
