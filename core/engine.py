"""Event-driven engine: consumes the feed queue, owns all state mutations.

Telegram commands are serialized through the same queue (("cmd", ...)) so one
consumer task mutates BotState — no re-entrant locking. Strategy is evaluated
only on closed 1m candles while FLAT and entries are allowed.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional

from config import Settings
from core import risk
from core.executor import Fill, LiveExecutor, PaperExecutor
from core.state import Bot, BotState, Pos, Position, utc_day, utc_midnight_ms
from core.strategy import Candle, Signal, atr, evaluate_ex
from tg import messages

log = logging.getLogger("flowscalp.engine")

RECONCILE_EVERY_MS = 30_000
STALE_ALERT_MS = 60_000
BACKUP_STOP_DELAY_MS = 2_000
TAKER_GIVEUP_MS = 10_000
FOUR_H_MS = 14_400_000
FIFTEEN_MIN_MS = 900_000
TREND_ATR_PERIOD = 14


def now_ms() -> int:
    return int(time.time() * 1000)


class Engine:
    def __init__(self, env, db, store, state: BotState, feed, queue: asyncio.Queue, notifier):
        self.env = env
        self.db = db
        self.store = store
        self.state = state
        self.feed = feed
        self.queue = queue
        self.notifier = notifier
        self.executor = None
        self._last_reconcile = 0
        self._last_stale_alert = 0
        self._sz_eps = 1e-6

    # ------------------------------------------------------------------ boot
    async def boot(self) -> None:
        """BOOT → RECONCILE → RUNNING. Restores the crash snapshot; in live
        mode the exchange is the source of truth (adopt or flatten)."""
        st = self.state
        snap = BotState.load_snapshot(st.snapshot_path)
        st.set_bot_state(Bot.RECONCILE)
        if snap:
            st.restore(snap)
        if st.mode == "live":
            self.executor = LiveExecutor(self.env, self.db, self._mid_of)
            await self.executor.boot()
            await self._reconcile_boot()
            self.feed.enable_user_fills(self.env.trading_address)
        else:
            self.executor = PaperExecutor(self.env, self.db, self._mid_of, self.handle_fill)
            await self.executor.boot()
            if st.position is not None and st.pos_state == Pos.IN_POSITION:
                await self.executor.arm_exits(st.position)
                await self.db.log_event("INFO", "paper position restored from snapshot")
        self._sz_eps = 10 ** -self.executor.sz_decimals / 2
        if st.day_start_equity <= 0:
            st.day_start_equity = await self.executor.equity()
        if st.bot_state == Bot.RECONCILE:
            st.set_bot_state(Bot.RUNNING)
        await self.db.log_event("INFO", f"engine boot complete: {st.bot_state.value} ({st.mode})")

    async def _reconcile_boot(self) -> None:
        """Live boot: compare exchange positions (all coins) vs the snapshot."""
        st = self.state
        for coin in getattr(self.executor, "coins", (self._primary(),)):
            await self._reconcile_boot_coin(coin)
        if st.position is None and st.pos_state != Pos.FLAT:
            st.set_pos_state(Pos.FLAT)

    async def _reconcile_boot_coin(self, coin: str) -> None:
        st = self.state
        szi, ex_entry, orders = await self.executor.reconcile_raw(coin)
        pos = st.position if (st.position and (st.position.coin or self._primary()) == coin) else None
        if szi != 0:
            side = "long" if szi > 0 else "short"
            if pos is not None and pos.side == side and abs(abs(szi) - pos.filled_sz) <= 2 * self._sz_eps:
                # adopt: cancel whatever orders remain, then re-arm exits exactly once
                await self.executor.cancel_all()
                pos.oids = {k: v for k, v in pos.oids.items() if k == "entry"}
                await self.executor.arm_exits(pos)
                st.pos_state = Pos.IN_POSITION
                await self._notify(f"♻️ reconciled: adopted open {side.upper()} {abs(szi):.4f} {coin},"
                                   f" exits re-armed (sl {messages.fmt_px(pos.sl_px)},"
                                   f" tp {messages.fmt_px(pos.tp_px)})")
            else:
                await self._notify(f"⚠️ reconcile: unknown {side.upper()} {abs(szi):.4f} {coin}"
                                   f" @ {messages.fmt_px(ex_entry)} on exchange — flattening for safety")
                if pos is not None:
                    st.position = None
                    st.set_pos_state(Pos.FLAT)
                await self.executor.flatten("manual", coin=coin)
                await self.db.log_event("WARN", f"boot reconcile flattened unknown {coin} szi={szi}")
        else:
            if pos is not None:
                # we thought we had a position; it closed while we were down
                await self._close_unseen_exit(pos)
            elif orders:
                await self.executor.cancel_all()
                await self.db.log_event("WARN", f"boot reconcile canceled {len(orders)} orphan orders")

    async def _close_unseen_exit(self, pos: Position) -> None:
        """Position closed while the bot was down: recover exit details
        (best effort from recent fills, else current mid) and book the trade."""
        exit_px, reason = self._mid_of(pos.coin or self._primary()) or pos.entry_px, "manual"
        fees = 0.0
        try:
            raw = await asyncio.to_thread(self.executor._info.user_fills, self.env.trading_address)
            closes = [f for f in raw if f.get("coin") == (pos.coin or self._primary()) and int(f["time"]) >= pos.ts_open
                      and str(f.get("oid")) != pos.oids.get("entry")]
            if closes:
                sz = sum(float(f["sz"]) for f in closes)
                exit_px = sum(float(f["px"]) * float(f["sz"]) for f in closes) / sz
                fees = sum(float(f.get("fee", 0) or 0) for f in closes)
                oid_set = {str(f["oid"]) for f in closes}
                if pos.oids.get("tp") in oid_set:
                    reason = "tp"
                elif pos.oids.get("sl") in oid_set:
                    reason = "sl"
        except Exception as e:  # noqa: BLE001 — best-effort recovery
            log.warning("unseen-exit fill lookup failed: %s", e)
        pos.exit_px, pos.exit_sz, pos.fees_usd = exit_px, pos.filled_sz, pos.fees_usd + fees
        await self._book_close(pos, reason, now_ms())
        await self.executor.cancel_all()

    # ------------------------------------------------------------------ run loop
    async def run(self) -> None:
        while True:
            kind, payload = await self.queue.get()
            try:
                await self._dispatch(kind, payload)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — engine must survive handler errors
                log.exception("engine error on %s", kind)
                try:
                    await self.db.log_event("ERROR", f"engine {kind}: {e}")
                    await self.notifier.error(f"engine error on {kind}: {e}")
                except Exception:  # noqa: BLE001
                    pass

    def _primary(self) -> str:
        return getattr(self.feed, "primary", getattr(self.env, "coin", "BTC"))

    def _book(self, coin: str):
        if hasattr(self.feed, "book") and hasattr(self.feed, "books"):
            return self.feed.book(coin)
        return self.feed  # legacy single-coin feed (tests)

    def _mid_of(self, coin: str) -> float:
        if hasattr(self.feed, "mid_of"):
            return self.feed.mid_of(coin)
        return getattr(self.feed, "mid", 0.0)

    async def _dispatch(self, kind: str, payload) -> None:
        if kind == "candle_1m_closed":
            if isinstance(payload, tuple):
                coin, bar = payload
            else:
                coin, bar = self._primary(), payload
            await self._on_bar(coin, bar)
        elif kind == "mid":
            if isinstance(payload, tuple):
                coin, px = payload
            else:
                coin, px = self._primary(), payload
            await self._on_mid(coin, px)
        elif kind == "fill":
            if self.executor is not None and self.executor.name == "live":
                await self.handle_fill(self.executor.to_fill(payload))
        elif kind == "candle_15m_closed":
            if isinstance(payload, tuple):
                coin, bar = payload
                await self._on_bar15(coin, bar)
        elif kind == "timer":
            await self._on_timer(payload)
        elif kind == "cmd":
            await self._on_cmd(payload)

    # ------------------------------------------------------------------ commands
    async def submit(self, name: str, **kw):
        fut = asyncio.get_running_loop().create_future()
        self.queue.put_nowait(("cmd", (name, kw, fut)))
        return await fut

    async def _on_cmd(self, payload) -> None:
        name, kw, fut = payload
        try:
            fn = getattr(self, f"_cmd_{name}")
            result = await fn(**kw)
            if not fut.done():
                fut.set_result(result)
        except Exception as e:  # noqa: BLE001
            if not fut.done():
                fut.set_exception(e)

    async def _cmd_pause(self) -> str:
        st = self.state
        if st.bot_state == Bot.STOPPED:
            return "bot is STOPPED — /resume first"
        st.set_bot_state(Bot.PAUSED)
        await self._notify("⏸ PAUSED — no new entries; open trade still managed")
        return "paused"

    async def _cmd_resume(self) -> str:
        st = self.state
        if st.bot_state == Bot.HALTED_DAILY:
            return "HALTED_DAILY — clears automatically at 00:00 UTC"
        if st.bot_state not in (Bot.PAUSED, Bot.COOLDOWN, Bot.STOPPED):
            return f"nothing to resume (state {st.bot_state.value})"
        st.set_bot_state(Bot.RECONCILE)
        if st.mode == "live":
            await self._reconcile_boot()
        if st.bot_state == Bot.RECONCILE:
            st.cooldown_until = 0
            st.consec_losses = 0
            st.set_bot_state(Bot.RUNNING)
        await self._notify("▶️ RESUMED — RUNNING")
        return "running"

    async def _cmd_stop(self) -> str:
        """Kill switch: cancel everything and flatten NOW."""
        st = self.state
        t0 = time.monotonic()
        await self.executor.cancel_all()
        pos = st.position
        if pos is not None and pos.filled_sz - pos.exit_sz > self._sz_eps:
            pos.pending_exit_reason = "kill"
            st.set_pos_state(Pos.EXITING)
            await self.executor.flatten("kill", coin=pos.coin or self._primary())
        elif pos is not None:
            st.position = None
            st.set_pos_state(Pos.FLAT)
        st.set_bot_state(Bot.STOPPED)
        dt = time.monotonic() - t0
        await self._notify(f"🛑 KILL SWITCH — canceled + flattened in {dt:.1f}s. state STOPPED")
        return f"stopped ({dt:.1f}s)"

    async def _cmd_mode(self, target: str) -> str:
        st = self.state
        if target == st.mode:
            return f"already in {target} mode"
        if st.pos_state != Pos.FLAT:
            return "open position/order — /stop to flatten before switching modes"
        st.set_bot_state(Bot.RECONCILE)
        try:
            if target == "live":
                ex = LiveExecutor(self.env, self.db, self._mid_of)
                await ex.boot()
                eq = await ex.equity()
                if eq <= 0:
                    spot = await ex.spot_usdc()
                    if spot > 0:
                        raise RuntimeError(
                            f"saldo PERPS $0, tapi ada ${spot:,.2f} di akun SPOT — pindahkan dulu"
                            f" lewat tombol 'Perps ⇄ Spot' di app Hyperliquid, lalu /mode live lagi.")
                    raise RuntimeError(
                        f"equity akun live ${eq:,.2f} — alamat wallet utama salah (/setwallet)"
                        f" atau belum ada deposit. Tetap di paper.")
                szi, _, orders = await ex.reconcile_raw()
                if szi != 0 or orders:
                    self.executor = ex
                    st.mode = "live"
                    await ex.cancel_all()
                    await ex.flatten("manual")
                    await self._notify("⚠️ going live: found existing position/orders — flattened for safety")
                else:
                    self.executor = ex
                    st.mode = "live"
                self.feed.enable_user_fills(self.env.trading_address)
            else:
                ex = PaperExecutor(self.env, self.db, self._mid_of, self.handle_fill)
                await ex.boot()
                self.executor = ex
                st.mode = "paper"
            self._sz_eps = 10 ** -self.executor.sz_decimals / 2
            st.day_start_equity = await self.executor.equity()
            st.set_bot_state(Bot.RUNNING)
            st.save_snapshot()
            await self._notify(f"{'🔴 LIVE MODE' if target == 'live' else '🟡 PAPER MODE'} — confirmed,"
                               f" equity {messages.fmt_usd(st.day_start_equity)}")
            return f"mode switched to {target}"
        except Exception as e:
            st.set_bot_state(Bot.RUNNING)
            raise RuntimeError(f"mode switch failed, staying in {st.mode}: {e}") from e

    async def _cmd_why(self) -> str:
        """Live diagnosis: walk the entry conditions against current buffers
        and report exactly which stage is blocking."""
        from core import strategy as stg
        from core import timewin
        st = self.state
        s = self.store.current
        books = getattr(self.feed, "books", None) or {self._primary(): self.feed}
        lines = [f"🔎 KENAPA BELUM ENTRY — kondisi sekarang",
                 f"state {st.bot_state.value} | posisi {st.pos_state.value} | mode {st.mode}"]
        if s.trend_mode:
            lines[0] = "🔎 KENAPA BELUM ENTRY — strategi v3 trend 4 jam"
            for _coin in books:
                lines.append("")
                lines.append(f"— {_coin} —")
                lines.extend(self._why_trend_coin(_coin, s))
            return "\n".join(lines)
        for _coin, _bk in books.items():
            lines.append("")
            lines.append(f"— {_coin} —")
            lines.extend(self._why_coin(_bk, s))
        return "\n".join(lines)

    def _why_coin(self, bk, s) -> list:
        from core import strategy as stg
        c1m = list(bk.candles_1m)
        c15 = list(bk.candles_15m)
        deltas = list(bk.deltas_1m)
        lines = []
        need = max(s.sweep_lookback + 3, 60)
        if len(c1m) < need or len(c15) < 210:
            lines.append(f"❌ data: {len(c1m)} bar 1m (butuh {need}), {len(c15)} bar 15m (butuh 210)")
            return lines
        last, prev = c1m[-1], c1m[-2]

        sess = stg.session_ok(last.ts + 60_000, s)
        lines.append(f"{'✅' if sess else '❌'} sesi entry: {'AKTIF' if sess else 'TUTUP'}"
                     f" ({', '.join(s.session_windows)} UTC)")

        bps = stg.atr(c1m, 14) / last.close * 10_000
        ok_atr = s.atr_floor_bps <= bps <= s.atr_ceil_bps
        note = "" if ok_atr else (" — pasar terlalu SEPI, semua sinyal dilewati"
                                  if bps < s.atr_floor_bps else " — pasar terlalu LIAR")
        lines.append(f"{'✅' if ok_atr else '❌'} volatilitas: ATR {bps:.1f} bps"
                     f" (syarat {s.atr_floor_bps:.0f}–{s.atr_ceil_bps:.0f}){note}")

        bias = stg._bias(c15, s)
        lines.append(f"ℹ️ bias tren 15m: {bias} ({'long & short' if bias == 'both' else bias + ' saja'})")

        window = c1m[-(s.sweep_lookback + 2):-2]
        lo = min(c.low for c in window)
        hi = max(c.high for c in window)
        pen = s.sweep_min_pen_pct / 100
        need_lo, need_hi = lo * (1 - pen), hi * (1 + pen)
        swept = (last.low <= need_lo or prev.low <= need_lo
                 or last.high >= need_hi or prev.high >= need_hi)
        lines.append(f"{'✅' if swept else '⏳'} struktur sweep: "
                     f"{'TERJADI di 2 bar terakhir' if swept else 'belum ada sapuan level'}")
        lines.append(f"   level bawah {lo:,.0f} → butuh low ≤ {need_lo:,.0f}"
                     f" (low terakhir {last.low:,.0f})")
        lines.append(f"   level atas {hi:,.0f} → butuh high ≥ {need_hi:,.0f}"
                     f" (high terakhir {last.high:,.0f})")

        if s.cvd_filter:
            k = s.sweep_lookback + 24
            tail = deltas[-k:] if len(deltas) >= k else deltas
            nones = sum(1 for x in tail if x is None)
            lines.append("✅ filter CVD: siap" if nones == 0 and len(tail) >= k
                         else f"⏳ filter CVD: warm-up, ±{max(nones, k - len(tail))} menit lagi")
        return lines

    async def _cmd_closepos(self) -> str:
        """Manual close from the owner: cancel a pending entry or flatten."""
        st = self.state
        pos = st.position
        if pos is None:
            return "tidak ada posisi/order yang terbuka"
        if st.pos_state == Pos.PENDING_ENTRY:
            await self.executor.cancel_entry(pos.oids.get("entry", ""),
                                             coin=pos.coin or self._primary())
            st.position = None
            st.set_pos_state(Pos.FLAT)
            return "order entry dibatalkan ✅"
        if st.pos_state in (Pos.IN_POSITION, Pos.EXITING):
            pos.pending_exit_reason = "manual"
            st.set_pos_state(Pos.EXITING)
            await self.executor.flatten("manual", coin=pos.coin or self._primary())
            return "posisi ditutup di harga pasar ✅"
        return f"tidak bisa: {st.pos_state.value}"

    async def _cmd_closemaker(self) -> str:
        """Eco close: pull the reduce-only TP limit to the current mid so the
        exit fills as maker. SL stays armed; not guaranteed to fill."""
        st = self.state
        pos = st.position
        if pos is None or st.pos_state != Pos.IN_POSITION:
            return "tidak ada posisi terbuka (atau entry masih pending)"
        mid = self._mid_of(pos.coin or self._primary())
        if not mid:
            return "harga belum tersedia"
        pos.tp_is_manual = True
        pos.tp_px = mid
        await self.executor.move_tp(pos, mid)
        st.save_snapshot()
        return (f"💚 limit penutup dipasang @ {mid:,.0f} (fee maker, murah).\n"
                f"terisi begitu harga melewatinya; SL tetap berjaga.\n"
                f"kalau harga kabur dan Anda ingin pasti keluar → tombol market.")

    async def _cmd_testtrade(self, live_ok: bool = False) -> str:
        """Inject one synthetic trade to exercise the entry→manage→exit
        pipeline. Paper: normal sizing. Live: requires explicit confirmation
        and uses the MINIMUM viable size (~$11 notional) — an execution
        check, not a bet."""
        st = self.state
        if st.bot_state != Bot.RUNNING or st.pos_state != Pos.FLAT:
            return f"tidak bisa sekarang: state {st.bot_state.value}/{st.pos_state.value}"
        if st.mode == "live" and not live_ok:
            return "trade uji di akun REAL butuh konfirmasi — kirim /testtrade lalu tekan tombolnya"
        mid = self._mid_of(self._primary())
        if not mid:
            return "harga belum tersedia — feed masih menyambung"
        s = self.store.current

        if st.mode == "live":
            # passive maker just under mid; minimal size clearing the $10 floor
            coin = self._primary()
            dec = (self.executor.sz_dec(coin) if hasattr(self.executor, "sz_dec")
                   else self.executor.sz_decimals)
            entry = mid * 0.9999
            sz = math.ceil((11.0 / entry) * 10 ** dec) / 10 ** dec
            sl = entry * 0.997
            sig = Signal("long", entry, sl, entry + s.tp_r * (entry - sl),
                         swept_level=round(mid),
                         reason="TEST TRADE REAL — ukuran minimal, bukan sinyal strategi")
            eq = await self.executor.equity()
            override = risk.SizeResult(True, sz, sz * entry, (sz * entry) / max(eq, 1.0), False)
            await self._place_entry(sig, s, now_ms(), size_override=override, coin=coin)
            if st.pos_state == Pos.PENDING_ENTRY:
                return (f"🔴 test trade REAL dipasang: LONG {sz} {coin} (≈${sz * entry:,.2f})"
                        f" @ ~{entry:,.0f}.\n"
                        f"order maker pasif — kalau {s.maker_timeout_s} detik tidak terisi,"
                        f" otomatis batal (kirim ulang saja).\n"
                        f"SL/TP terpasang di exchange; rugi maksimal ±sen + fee.")
            return "entry tidak terpasang — cek /status dan events log"

        entry = mid * 1.0002          # a hair above mid so the first downtick fills it
        sl = entry * 0.997            # 0.3% test stop
        sig = Signal("long", entry, sl, entry + s.tp_r * (entry - sl),
                     swept_level=round(mid), reason="TEST TRADE manual — bukan sinyal strategi")
        await self._place_entry(sig, s, now_ms(), coin=self._primary())
        if st.pos_state == Pos.PENDING_ENTRY:
            return (f"🧪 test trade dipasang: LONG @ ~{entry:,.0f} (paper).\n"
                    f"fill maker biasanya beberapa detik; SL/TP/time-stop berjalan normal —"
                    f" pantau alert & dashboard.")
        return "entry tidak terpasang — cek /status dan events log"

    async def _cmd_report(self) -> str:
        await self._send_daily_report(today=True)
        return "report posted"

    # ------------------------------------------------------------------ market events
    async def _on_bar(self, coin: str, bar) -> None:
        st = self.state
        st.last_tick_ts = self.feed.last_tick_ts
        if self.executor is None:
            return
        if self.executor.name == "paper":
            await self.executor.on_bar_closed(bar, coin)
        # housekeeping rides event time (bar close); in live operation this is
        # wall clock anyway, and it keeps replays/backtests deterministic
        await self._housekeeping(bar.ts + 60_000)
        if st.pos_state != Pos.FLAT or st.bot_state != Bot.RUNNING:
            return
        s = self.store.current
        if s.trend_mode:
            return                      # v3: entries come from 4h closes, not 1m sweeps
        close_dt = datetime.fromtimestamp((bar.ts + 60_000) / 1000, tz=timezone.utc)
        ok, why = risk.can_enter(st, s, close_dt, bar_ts=bar.ts)
        if not ok:
            log.debug("no entry (%s): %s", coin, why)
            return
        bk = self._book(coin)
        funding = getattr(bk, "funding_hourly", None)
        if funding is None:
            funding = getattr(bk, "current_funding_hourly", None)
        res = evaluate_ex(list(bk.candles_1m), list(bk.candles_15m),
                          list(bk.deltas_1m), s, funding)
        if res.skip_gate is not None:
            await self.db.log_event("INFO", f"gate skip {coin} [{res.skip_gate}] {res.detail}")
            return
        sig = res.signal
        if sig is None:
            return
        ok, why = risk.can_enter(st, s, close_dt, bar_ts=bar.ts, sig=sig, coin=coin)
        if not ok:
            await self.db.log_event("INFO", f"entry blocked {coin}: {why}")
            return
        await self._place_entry(sig, s, bar.ts + 60_000, coin=coin)

    async def _place_entry(self, sig: Signal, s: Settings, ts: int,
                           size_override: Optional[risk.SizeResult] = None,
                           coin: Optional[str] = None) -> None:
        st = self.state
        coin = coin or self._primary()
        equity = await self.executor.equity()
        sz_dec = (self.executor.sz_dec(coin) if hasattr(self.executor, "sz_dec")
                  else self.executor.sz_decimals)
        size = size_override or risk.position_size(equity, sig.entry_px, sig.sl_px, s, sz_dec)
        if not size.ok:
            await self.db.log_event("INFO", f"size skip: {size.skip_reason}")
            return
        if size.lev_capped:
            await self.db.log_event("INFO",
                                    f"leverage capped at {s.leverage_cap}x — realized risk below {s.risk_pct}%")
        oid = await self.executor.place_entry(sig, size, coin=coin)
        st.position = Position(side=sig.side, entry_px=sig.entry_px, size_btc=size.size_btc,
                               size_usd=size.size_usd, sl_px=sig.sl_px, tp_px=sig.tp_px,
                               swept_level=sig.swept_level, reason=sig.reason,
                               oids={"entry": oid}, placed_ts=ts, coin=coin)
        st.set_pos_state(Pos.PENDING_ENTRY)
        await self._notify(messages.entry_placed(sig.side, sig.entry_px, st.mode, coin))

    # ------------------------------------------------------------------ v3 trend (4h)
    @staticmethod
    def _agg_4h(c15m) -> list:
        """Closed 15m bars → 4h buckets. The first bucket may be partial
        (bootstrap can start mid-bucket) so it is dropped."""
        out: list[Candle] = []
        cur: Optional[Candle] = None
        for b in c15m:
            t0 = b.ts // FOUR_H_MS * FOUR_H_MS
            if cur is None or t0 > cur.ts:
                if cur is not None:
                    out.append(cur)
                cur = Candle(t0, b.open, b.high, b.low, b.close, b.volume)
            else:
                cur.high = max(cur.high, b.high)
                cur.low = min(cur.low, b.low)
                cur.close = b.close
                cur.volume += b.volume
        if cur is not None:
            out.append(cur)
        return out[1:] if len(out) > 1 else out

    async def _on_bar15(self, coin: str, bar) -> None:
        """v3 driver: act only on 15m closes that complete a 4h boundary."""
        st = self.state
        st.last_tick_ts = self.feed.last_tick_ts
        s = self.store.current
        if not s.trend_mode or self.executor is None:
            return
        if (bar.ts + FIFTEEN_MIN_MS) % FOUR_H_MS != 0:
            return
        c4h = self._agg_4h(self._book(coin).candles_15m)
        if len(c4h) < max(s.trend_lookback + 2, TREND_ATR_PERIOD + 2):
            return
        pos = st.position
        if (pos is not None and pos.trend and st.pos_state == Pos.IN_POSITION
                and (pos.coin or self._primary()) == coin):
            await self._trend_trail(pos, c4h, s)
        elif st.pos_state == Pos.FLAT and st.bot_state == Bot.RUNNING:
            await self._trend_entry(coin, c4h, s)

    async def _trend_trail(self, pos: Position, c4h: list, s) -> None:
        last = c4h[-1]
        pos.hi_close = max(pos.hi_close or pos.entry_px, last.close)
        a = atr(c4h, TREND_ATR_PERIOD)
        if a <= 0:
            return
        new_stop = pos.hi_close - s.trend_atr_k * a
        if new_stop > pos.sl_px:
            old = pos.sl_px
            pos.sl_px = new_stop
            await self.executor.move_sl(pos, new_stop)
            self.state.save_snapshot()
            await self.db.log_event("INFO", f"trail {pos.coin}: sl {old:.2f} → {new_stop:.2f}")
            locked = (new_stop - pos.entry_px) / pos.entry_px * 100
            await self._notify(f"🪜 trailing stop {pos.coin} naik → {messages.fmt_px(new_stop)}"
                               f" ({locked:+.2f}% dari entry)")

    async def _trend_entry(self, coin: str, c4h: list, s) -> None:
        st = self.state
        last = c4h[-1]
        highest = max(c.high for c in c4h[-(s.trend_lookback + 1):-1])
        if last.close <= highest:
            return
        if not st.last_tick_ts or now_ms() - st.last_tick_ts > risk.STALE_FEED_MS:
            await self.db.log_event("INFO", f"trend entry skip {coin}: stale feed")
            return
        a = atr(c4h, TREND_ATR_PERIOD)
        mid = self._mid_of(coin) or last.close
        sl = mid - s.trend_atr_k * a
        if a <= 0 or sl <= 0:
            return
        equity = await self.executor.equity()
        sz_dec = (self.executor.sz_dec(coin) if hasattr(self.executor, "sz_dec")
                  else self.executor.sz_decimals)
        size = risk.position_size(equity, mid, sl, s, sz_dec)
        if not size.ok:
            await self.db.log_event("INFO", f"trend size skip {coin}: {size.skip_reason}")
            return
        sig = Signal("long", mid, sl, 0.0, swept_level=0.0,
                     reason=f"v3 breakout 4h > {highest:,.0f} | trail {s.trend_atr_k}xATR")
        st.position = Position(side="long", entry_px=mid, size_btc=size.size_btc,
                               size_usd=size.size_usd, sl_px=sl, tp_px=0.0,
                               reason=sig.reason, oids={}, placed_ts=now_ms(), coin=coin,
                               trend=True, hi_close=last.close)
        st.position.taker_sent_ts = now_ms()
        st.set_pos_state(Pos.PENDING_ENTRY)
        await self._notify(f"⚡ v3 breakout {coin}: entry market @ ~{messages.fmt_px(mid)}"
                           f" | sl awal {messages.fmt_px(sl)} (trailing)")
        await self.executor.taker_entry(sig, size.size_btc, coin=coin)

    def _why_trend_coin(self, coin: str, s) -> list:
        c4h = self._agg_4h(self._book(coin).candles_15m)
        need = max(s.trend_lookback + 2, TREND_ATR_PERIOD + 2)
        if len(c4h) < need:
            return [f"❌ data: {len(c4h)} bar 4h (butuh {need})"]
        last = c4h[-1]
        highest = max(c.high for c in c4h[-(s.trend_lookback + 1):-1])
        bps = atr(c4h, TREND_ATR_PERIOD) / last.close * 10_000
        return [f"✅ data: {len(c4h)} bar 4h | ATR {bps:.0f} bps",
                f"{'✅ BREAKOUT' if last.close > highest else '⏳ menunggu breakout'}:"
                f" close 4h {last.close:,.0f} vs level {highest:,.0f}"
                f" (butuh > {highest:,.0f})"]

    async def _on_mid(self, coin: str, mid: float) -> None:
        st = self.state
        st.last_tick_ts = self.feed.last_tick_ts
        if self.executor is None:
            return
        if self.executor.name == "paper":
            await self.executor.on_mid(mid, coin)
            return
        pos = st.position
        if (pos is not None and st.pos_state == Pos.IN_POSITION and pos.sl_cross_ts == 0
                and (pos.coin or self._primary()) == coin):
            crossed = mid <= pos.sl_px if pos.side == "long" else mid >= pos.sl_px
            if crossed:
                pos.sl_cross_ts = now_ms()

    # ------------------------------------------------------------------ fills
    async def handle_fill(self, f: Fill) -> None:
        st = self.state
        pos = st.position
        if pos is None:
            await self.db.log_event("WARN", f"orphan fill oid={f.oid} px={f.px} sz={f.sz}")
            return
        if f.coin and pos.coin and f.coin != pos.coin:
            await self.db.log_event("WARN", f"fill for {f.coin} but position is {pos.coin} — ignored")
            return
        entry_side = "B" if pos.side == "long" else "A"
        is_entry = (f.oid == pos.oids.get("entry") or f.kind in ("entry", "taker_entry")
                    or (st.pos_state == Pos.PENDING_ENTRY and f.side == entry_side))
        if is_entry and st.pos_state == Pos.PENDING_ENTRY:
            await self._on_entry_fill(f)
        elif f.side != entry_side and st.pos_state in (Pos.IN_POSITION, Pos.EXITING):
            await self._on_exit_fill(f)
        else:
            await self.db.log_event("WARN", f"unroutable fill oid={f.oid} side={f.side}"
                                            f" pos_state={st.pos_state.value}")

    async def _on_entry_fill(self, f: Fill) -> None:
        st, pos = self.state, self.state.position
        total = pos.filled_sz + f.sz
        pos.entry_px = (pos.entry_px * pos.filled_sz + f.px * f.sz) / total if pos.filled_sz > 0 else f.px
        pos.filled_sz = total
        pos.fees_usd += f.fee_usd
        if f.tx_hash:
            pos.entry_tx = f.tx_hash
            pos.entry_tx_crossed = f.crossed
        if pos.filled_sz + self._sz_eps >= pos.size_btc:
            await self._complete_entry(f.time)

    async def _complete_entry(self, ts: int, partial: bool = False) -> None:
        st, pos = self.state, self.state.position
        s = self.store.current
        pos.size_btc = pos.filled_sz
        pos.size_usd = pos.entry_px * pos.filled_sz
        sl_dist = abs(pos.entry_px - pos.sl_px) / pos.entry_px
        pos.entry_risk_usd = pos.size_usd * sl_dist
        pos.ts_open = ts
        pos.trade_id = await self.db.insert_trade(
            mode=st.mode, side=pos.side, ts_open=ts, entry_px=pos.entry_px,
            size_btc=pos.filled_sz, size_usd=pos.size_usd, sl_px=pos.sl_px,
            tp_px=pos.tp_px, fees_usd=pos.fees_usd, signal_reason=pos.reason,
            coin=pos.coin or self._primary())
        await self.executor.arm_exits(pos)
        st.set_pos_state(Pos.IN_POSITION)
        await self._notify(messages.entry_block(pos, st.mode, s.risk_pct, partial=partial,
                                                coin=pos.coin or self._primary())
                           + messages.tx_proof(pos.entry_tx, pos.entry_tx_crossed,
                                               self.env.trading_address))

    async def _on_exit_fill(self, f: Fill) -> None:
        st, pos = self.state, self.state.position
        total = pos.exit_sz + f.sz
        pos.exit_px = (pos.exit_px * pos.exit_sz + f.px * f.sz) / total if pos.exit_sz > 0 else f.px
        pos.exit_sz = total
        pos.fees_usd += f.fee_usd
        if f.tx_hash:
            pos.exit_tx = f.tx_hash
            pos.exit_tx_crossed = f.crossed
        # explicit fill kind/reason wins; oid matching is the fallback for raw
        # live fills (an oid collision must never relabel a manual close)
        if f.kind == "sl" or (f.kind == "" and f.oid == pos.oids.get("sl")):
            reason = "sl"
        elif f.kind == "tp" or (f.kind == "" and f.oid == pos.oids.get("tp")):
            reason = "manual" if pos.tp_is_manual else "tp"
        elif f.kind == "flatten" and (f.reason or pos.pending_exit_reason):
            reason = f.reason or pos.pending_exit_reason
        else:
            reason = f.reason or pos.pending_exit_reason or "manual"
        if pos.exit_sz + self._sz_eps >= pos.filled_sz:
            await self._book_close(pos, reason, f.time)

    async def _book_close(self, pos: Position, reason: str, ts: int) -> None:
        st = self.state
        s = self.store.current
        direction = 1 if pos.side == "long" else -1
        pnl = direction * (pos.exit_px - pos.entry_px) * pos.filled_sz - pos.fees_usd
        r = pnl / pos.entry_risk_usd if pos.entry_risk_usd > 0 else 0.0
        if pos.trade_id is not None:
            await self.db.close_trade(pos.trade_id, ts_close=ts, exit_px=pos.exit_px,
                                      fees_usd=pos.fees_usd, pnl_usd=pnl, r_multiple=r,
                                      exit_reason=reason)
        await self.executor.apply_realized(pnl)
        # cancel the surviving sibling exit order
        sibling = pos.oids.get("sl") if reason == "tp" else pos.oids.get("tp")
        if self.executor.name == "live" and sibling and reason in ("tp", "sl"):
            await self.executor.cancel_entry(sibling, coin=pos.coin or self._primary())
        elif self.executor.name == "live" and reason not in ("tp", "sl"):
            await self.executor.cancel_all()
        if self.executor.name == "paper":
            self.executor.disarm()

        st.day_realized_pnl += pnl
        st.day_realized_r += r
        st.day_trades += 1
        st.consec_losses = st.consec_losses + 1 if r < 0 else 0
        st.last_exit_bar_ts = ts // 60_000 * 60_000
        st.last_swept_level[f"{pos.coin or self._primary()}:{pos.side}"] = pos.swept_level
        minutes = max(0, (ts - pos.ts_open) // 60_000) if pos.ts_open else 0
        st.position = None
        st.set_pos_state(Pos.FLAT)

        equity = await self.executor.equity()
        await self.db.snapshot_equity(st.mode, equity, ts)
        await self._notify(messages.exit_block(pos.side, r, pnl, pos.entry_px, pos.exit_px,
                                               minutes, reason, st.day_realized_r, equity,
                                               coin=pos.coin or self._primary())
                           + messages.tx_proof(pos.exit_tx, pos.exit_tx_crossed,
                                               self.env.trading_address))
        await self._circuit_breakers(s, equity)

    async def _circuit_breakers(self, s: Settings, equity: float) -> None:
        st = self.state
        if st.bot_state not in (Bot.RUNNING, Bot.PAUSED, Bot.COOLDOWN):
            return
        limit = s.daily_loss_limit_pct / 100 * st.day_start_equity
        if st.day_start_equity > 0 and st.day_realized_pnl <= -limit:
            st.halted_reason = f"daily loss {messages.fmt_usd(st.day_realized_pnl, signed=True)}"
            st.set_bot_state(Bot.HALTED_DAILY)
            await self._notify(f"🛑 HALTED_DAILY — day pnl {messages.fmt_usd(st.day_realized_pnl, signed=True)}"
                               f" breached {s.daily_loss_limit_pct}% of {messages.fmt_usd(st.day_start_equity)}."
                               f" No entries until 00:00 UTC.")
            return
        if st.consec_losses >= s.max_consec_losses and st.bot_state == Bot.RUNNING:
            st.cooldown_until = now_ms() + s.cooldown_min * 60_000
            st.set_bot_state(Bot.COOLDOWN)
            await self._notify(f"❄️ COOLDOWN — {st.consec_losses} consecutive losses;"
                               f" pausing entries for {s.cooldown_min}m")

    # ------------------------------------------------------------------ timers
    async def _on_timer(self, now: int) -> None:
        st = self.state
        st.last_tick_ts = self.feed.last_tick_ts
        if self.executor is None:
            return
        s = self.store.current

        if utc_day(now) != st.day_utc:
            await self._rollover(now)

        if st.bot_state == Bot.COOLDOWN and st.cooldown_until and now >= st.cooldown_until:
            st.cooldown_until = 0
            st.consec_losses = 0
            st.set_bot_state(Bot.RUNNING)
            await self._notify("✅ cooldown over — RUNNING")

        if (st.last_tick_ts and now - st.last_tick_ts > STALE_ALERT_MS
                and now - self._last_stale_alert > 300_000):
            self._last_stale_alert = now
            await self.notifier.error(f"feed stale for {(now - st.last_tick_ts) // 1000}s")

        await self._housekeeping(now)

        if self.executor.name == "live" and now - self._last_reconcile >= RECONCILE_EVERY_MS:
            self._last_reconcile = now
            await self._reconcile_runtime()

    async def _housekeeping(self, now: int) -> None:
        st = self.state
        s = self.store.current
        pos = st.position

        # maker entry timeout / IOC fallback watchdog
        if st.pos_state == Pos.PENDING_ENTRY and pos is not None:
            if pos.taker_sent_ts:
                if now - pos.taker_sent_ts >= TAKER_GIVEUP_MS:
                    if pos.filled_sz > self._sz_eps:
                        await self._complete_entry(now, partial=True)
                    else:
                        st.position = None
                        st.set_pos_state(Pos.FLAT)
                        await self.db.log_event("INFO", "taker entry missed — trade skipped")
            elif pos.placed_ts and now - pos.placed_ts >= s.maker_timeout_s * 1000:
                await self._maker_timeout(pos, s)

        pos = st.position
        # time stop (trend positions ride for days — no clock on them)
        if (st.pos_state == Pos.IN_POSITION and pos is not None and pos.ts_open
                and not pos.trend
                and now - pos.ts_open >= s.time_stop_min * 60_000):
            pos.pending_exit_reason = "time_stop"
            st.set_pos_state(Pos.EXITING)
            await self.executor.flatten("time_stop", coin=pos.coin or self._primary())

        pos = st.position
        # engine-side backup stop (live): trigger crossed ≥2s ago and no fill yet
        if (self.executor.name == "live" and pos is not None and st.pos_state == Pos.IN_POSITION
                and pos.sl_cross_ts and now - pos.sl_cross_ts >= BACKUP_STOP_DELAY_MS):
            await self.db.log_event("WARN", "engine-side backup stop firing (trigger not filled in 2s)")
            pos.pending_exit_reason = "sl"
            st.set_pos_state(Pos.EXITING)
            await self.executor.flatten("sl", coin=pos.coin or self._primary())

    async def _maker_timeout(self, pos: Position, s: Settings) -> None:
        st = self.state
        coin = pos.coin or self._primary()
        await self.executor.cancel_entry(pos.oids.get("entry", ""), coin=coin)
        if pos.filled_sz > self._sz_eps:
            await self._complete_entry(now_ms(), partial=True)
            await self.db.log_event("INFO", f"maker timeout with partial fill {pos.filled_sz} {coin} — kept")
        elif s.allow_taker_entry:
            sig = Signal(pos.side, pos.entry_px, pos.sl_px, pos.tp_px, pos.swept_level, pos.reason)
            pos.taker_sent_ts = now_ms()
            await self.executor.taker_entry(sig, pos.size_btc, coin=coin)
            await self.db.log_event("INFO", f"maker timeout → IOC taker entry sent ({coin})")
        else:
            st.position = None
            st.set_pos_state(Pos.FLAT)
            await self.db.log_event("INFO", "maker entry timeout — trade skipped")

    async def _rollover(self, now: int) -> None:
        st = self.state
        await self._send_daily_report(today=False)
        st.day_utc = utc_day(now)
        st.day_start_ts = utc_midnight_ms(now)
        st.day_start_equity = await self.executor.equity()
        st.day_realized_pnl = 0.0
        st.day_realized_r = 0.0
        st.day_trades = 0
        if st.bot_state == Bot.HALTED_DAILY:
            st.halted_reason = ""
            st.set_bot_state(Bot.RUNNING)
            await self._notify("🌅 new UTC day — daily halt cleared, RUNNING")
        st.save_snapshot()

    async def _send_daily_report(self, today: bool) -> None:
        st = self.state
        summary = await self.db.summary(st.mode, since_ts=st.day_start_ts)
        equity = await self.executor.equity()
        await self._notify(messages.daily_report(st.day_utc, st.mode, summary, equity, st.consec_losses))

    async def _reconcile_runtime(self) -> None:
        """Live drift check every 30s: position, open orders, equity."""
        st = self.state
        for coin in getattr(self.executor, "coins", (self._primary(),)):
            try:
                szi, _, orders = await self.executor.reconcile_raw(coin)
            except Exception as e:  # noqa: BLE001
                log.warning("runtime reconcile failed (%s): %s", coin, e)
                return
            pos = st.position if (st.position
                                  and (st.position.coin or self._primary()) == coin) else None
            if pos is not None and st.pos_state in (Pos.IN_POSITION, Pos.EXITING):
                if szi == 0 and pos.exit_sz < pos.filled_sz:
                    await self.db.log_event("WARN", f"reconcile: exchange flat but internal"
                                                    f" IN_POSITION ({coin}) — booking unseen exit")
                    await self._close_unseen_exit(pos)
            elif pos is None and szi != 0:
                await self.notifier.error(f"reconcile: exchange shows {szi:+.4f} {coin} but bot"
                                          f" has no such position — not touching it")
            elif st.pos_state == Pos.FLAT and orders:
                await self.executor.cancel_all()
                await self.db.log_event("WARN", f"reconcile: canceled {len(orders)} orphan {coin} orders")

    async def _notify(self, text: str) -> None:
        try:
            await self.notifier.channel(text)
        except Exception as e:  # noqa: BLE001 — alerts must never kill trading
            log.warning("notify failed: %s", e)
