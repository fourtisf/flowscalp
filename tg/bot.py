"""Telegram control bot (python-telegram-bot v21+ async, shared event loop).

Every handler passes the owner gate: non-owner input is ignored silently,
one `events` row is written, and no reply is ever sent.
Going live is a two-step: /mode live replies a random 6-char code; the owner
must send /confirm <code> within 60s or the switch is aborted.
"""
from __future__ import annotations

import logging
import secrets
import time
from functools import wraps
from typing import Optional

from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

from core.state import Pos
from tg import messages

log = logging.getLogger("flowscalp.tg")

ERROR_ALERT_EVERY_S = 300
CONFIRM_TTL_S = 60


class ConfirmFlow:
    """Single-use confirmation code for arming live mode."""

    def __init__(self, ttl_s: int = CONFIRM_TTL_S):
        self.ttl_s = ttl_s
        self._code: Optional[str] = None
        self._expires: float = 0.0

    def request(self) -> str:
        self._code = secrets.token_hex(3)  # 6 hex chars
        self._expires = time.monotonic() + self.ttl_s
        return self._code

    def check(self, code: str) -> bool:
        ok = (self._code is not None and time.monotonic() <= self._expires
              and secrets.compare_digest(code.strip().lower(), self._code))
        self._code = None  # single use, success or not
        return ok

    @property
    def pending(self) -> bool:
        return self._code is not None and time.monotonic() <= self._expires


class Notifier:
    """Channel alerts + rate-limited error alerts. Degrades to log-only when
    Telegram is not configured (dev) or temporarily failing."""

    def __init__(self, env):
        self.env = env
        self.app: Optional[Application] = None
        self._last_error_ts = 0.0

    async def channel(self, text: str) -> None:
        log.info("[channel] %s", text.replace("\n", " | "))
        if self.app is None or not self.env.tg_channel_id:
            return
        await self.app.bot.send_message(chat_id=self.env.tg_channel_id, text=text)

    async def error(self, text: str) -> None:
        log.error("[alert] %s", text)
        now = time.monotonic()
        if now - self._last_error_ts < ERROR_ALERT_EVERY_S:
            return
        self._last_error_ts = now
        try:
            await self.channel(f"⚠️ ERROR: {text}")
        except Exception as e:  # noqa: BLE001
            log.warning("error alert failed: %s", e)


def owner_gate(env, db):
    """Silently drop anything not from TG_OWNER_ID, logging one events row."""

    def deco(fn):
        @wraps(fn)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            user = update.effective_user
            if user is None or env.tg_owner_id is None or user.id != env.tg_owner_id:
                uid = user.id if user else "?"
                txt = (update.effective_message.text or "")[:80] if update.effective_message else ""
                await db.log_event("WARN", f"non-owner input ignored: uid={uid} cmd={txt!r}")
                return
            return await fn(update, context)
        return wrapper
    return deco


class TgBot:
    def __init__(self, env, db, store, state, feed, engine, notifier: Notifier):
        self.env = env
        self.db = db
        self.store = store
        self.state = state
        self.feed = feed
        self.engine = engine
        self.notifier = notifier
        self.confirm = ConfirmFlow()
        self.app: Optional[Application] = None

    # -- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        if not self.env.tg_bot_token:
            log.warning("TG_BOT_TOKEN not set — telegram control disabled, channel alerts log-only")
            return
        self.app = ApplicationBuilder().token(self.env.tg_bot_token).build()
        gate = owner_gate(self.env, self.db)
        for name, fn in [
            ("start", self.cmd_start), ("help", self.cmd_start),
            ("status", self.cmd_status), ("pnl", self.cmd_pnl), ("positions", self.cmd_positions),
            ("set", self.cmd_set), ("settings", self.cmd_settings), ("pause", self.cmd_pause),
            ("resume", self.cmd_resume), ("stop", self.cmd_stop), ("mode", self.cmd_mode),
            ("confirm", self.cmd_confirm), ("report", self.cmd_report),
        ]:
            self.app.add_handler(CommandHandler(name, gate(fn)))
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(allowed_updates=["message"])
        self.notifier.app = self.app
        log.info("telegram bot polling")

    async def stop(self) -> None:
        if self.app is None:
            return
        try:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        except Exception as e:  # noqa: BLE001
            log.warning("tg shutdown: %s", e)

    @staticmethod
    async def _reply(update: Update, text: str) -> None:
        await update.effective_message.reply_text(text)

    def _upnl(self) -> Optional[float]:
        pos, mid = self.state.position, self.feed.mid
        if pos is None or not pos.ts_open or not mid:
            return None
        d = 1 if pos.side == "long" else -1
        return d * (mid - pos.entry_px) * pos.filled_sz

    # -- commands ----------------------------------------------------------
    async def cmd_start(self, update, context) -> None:
        await self._reply(update,
                          "FlowScalp ready.\n"
                          "/status – state, equity, day PnL\n"
                          "/pnl day|week|all – realized PnL stats\n"
                          "/positions – open position detail\n"
                          "/set <key> <value> – adjust a setting\n"
                          "/settings – full settings dump\n"
                          "/pause • /resume – gate new entries\n"
                          "/stop – KILL SWITCH (flatten now)\n"
                          "/mode paper|live – live needs /confirm <code>\n"
                          "/report – post summary to channel")

    async def cmd_status(self, update, context) -> None:
        st = self.state
        equity = await self.engine.executor.equity() if self.engine.executor else 0.0
        uptime_min = int((time.time() * 1000 - st.boot_ts) / 60_000)
        await self._reply(update, messages.status_text(st, self.store.current, equity,
                                                       self._upnl(), self.feed.mid, uptime_min))

    async def cmd_pnl(self, update, context) -> None:
        period = (context.args[0].lower() if context.args else "day")
        now = int(time.time() * 1000)
        since = {"day": self.state.day_start_ts, "week": now - 7 * 86_400_000, "all": None}.get(period)
        if period not in ("day", "week", "all"):
            await self._reply(update, "usage: /pnl [day|week|all]")
            return
        s = await self.db.summary(self.state.mode, since_ts=since)
        await self._reply(update, messages.pnl_text(period, self.state.mode, s))

    async def cmd_positions(self, update, context) -> None:
        pos = self.state.position
        minutes = 0
        if pos is not None and pos.ts_open:
            minutes = int((time.time() * 1000 - pos.ts_open) / 60_000)
        await self._reply(update, messages.position_text(pos, self._upnl() or 0.0, minutes))

    async def cmd_set(self, update, context) -> None:
        if len(context.args) < 2:
            await self._reply(update, "usage: /set <key> <value>")
            return
        key, value = context.args[0], " ".join(context.args[1:])
        ok, old, new = await self.store.set(key, value)
        if ok:
            await self.db.log_event("INFO", f"/set {key}: {old} → {new}")
            await self._reply(update, f"{key}: {old} → {new}")
        else:
            await self._reply(update, f"rejected: {new}")

    async def cmd_settings(self, update, context) -> None:
        await self._reply(update, messages.settings_text(self.store.current))

    async def cmd_pause(self, update, context) -> None:
        await self._reply(update, await self.engine.submit("pause"))

    async def cmd_resume(self, update, context) -> None:
        await self._reply(update, await self.engine.submit("resume"))

    async def cmd_stop(self, update, context) -> None:
        await self._reply(update, await self.engine.submit("stop"))

    async def cmd_mode(self, update, context) -> None:
        target = (context.args[0].lower() if context.args else "")
        if target not in ("paper", "live"):
            await self._reply(update, "usage: /mode paper|live")
            return
        if target == "paper":
            await self._reply(update, await self.engine.submit("mode", target="paper"))
            return
        if self.state.pos_state != Pos.FLAT:
            await self._reply(update, "open position/order — /stop before going live")
            return
        code = self.confirm.request()
        await self._reply(update, f"⚠️ going LIVE risks real funds.\n"
                                  f"send /confirm {code} within {CONFIRM_TTL_S}s to proceed")

    async def cmd_confirm(self, update, context) -> None:
        code = context.args[0] if context.args else ""
        if not self.confirm.check(code):
            await self._reply(update, "invalid or expired code — /mode live to retry")
            await self.db.log_event("WARN", "live confirm failed (bad/expired code)")
            return
        await self.db.log_event("INFO", "live mode confirmed by owner")
        await self._reply(update, await self.engine.submit("mode", target="live"))

    async def cmd_report(self, update, context) -> None:
        await self._reply(update, await self.engine.submit("report"))
