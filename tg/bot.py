"""Telegram control bot (python-telegram-bot v21+ async, shared event loop).

Every handler passes the owner gate: non-owner input is ignored silently,
one `events` row is written, and no reply is ever sent.
Going live is a two-step: /mode live replies a random 6-char code; the owner
must send /confirm <code> within 60s or the switch is aborted.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
import signal
import time
from functools import wraps
from typing import Optional

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (Application, ApplicationBuilder, CallbackQueryHandler,
                          CommandHandler, ContextTypes, MessageHandler, filters)

from config import RANGES, update_env_file
from core.state import Pos
from tg import messages

# tap-to-set value suggestions for the /set inline keyboard
PRESETS = {
    "risk_pct": [0.1, 0.25, 0.5, 1.0, 2.0],
    "sl_cap_pct": [0.3, 0.4, 0.5, 0.7, 1.0],
    "sl_buffer_pct": [0.03, 0.05, 0.1, 0.15],
    "tp_r": [1.2, 1.5, 1.8, 2.2, 3.0],
    "time_stop_min": [10, 15, 20, 30, 60],
    "leverage_cap": [2, 3, 5, 10],
    "daily_loss_limit_pct": [2.0, 3.0, 5.0],
    "max_consec_losses": [3, 4, 5, 6],
    "cooldown_min": [60, 120, 240, 480],
    "atr_floor_bps": [4, 8, 12, 20],
    "atr_ceil_bps": [40, 60, 100, 200],
    "sweep_lookback": [20, 30, 60, 120],
    "sweep_min_pen_pct": [0.03, 0.05, 0.1, 0.2],
    "maker_timeout_s": [10, 20, 30, 60],
    "sweep_vol_mult": [1.0, 1.5, 1.7, 2.2, 3.0],
}

_KEY_RE = re.compile(r"^(0x)?[0-9a-fA-F]{64}$")
_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

PENDING_INPUT_TTL_S = 120

_ENV_SPECS = {
    "setkey": {"env_key": "HL_AGENT_PRIVATE_KEY", "label": "API key agent",
               "pattern": _KEY_RE, "secret": True, "allow_empty": False},
    "setwallet": {"env_key": "HL_MAIN_WALLET_ADDRESS", "label": "alamat wallet utama",
                  "pattern": _ADDR_RE, "secret": False, "allow_empty": False},
    "setsub": {"env_key": "HL_SUBACCOUNT_ADDRESS", "label": "alamat subaccount",
               "pattern": _ADDR_RE, "secret": False, "allow_empty": True},
}

BOT_COMMANDS = [
    ("status", "kondisi bot, equity, PnL hari ini"),
    ("profile", "saldo & ringkasan hasil trading"),
    ("positions", "detail posisi terbuka"),
    ("pnl", "statistik PnL: day | week | all"),
    ("settings", "semua pengaturan + penjelasan"),
    ("set", "ubah pengaturan: /set <key> <nilai>"),
    ("setkey", "simpan API key agent Hyperliquid"),
    ("setwallet", "simpan alamat wallet utama"),
    ("setsub", "simpan alamat subaccount (opsional)"),
    ("pause", "tahan entry baru"),
    ("resume", "lanjutkan / bangunkan bot"),
    ("stop", "DARURAT: batalkan order & tutup posisi"),
    ("mode", "ganti mode: paper | live"),
    ("report", "kirim ringkasan hari ini"),
    ("help", "daftar perintah"),
]

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
        self._pending_input: Optional[tuple[str, float]] = None  # (spec name, armed_ts)
        # replaced in tests; in production a SIGTERM lets systemd restart us
        self.restarter = lambda: asyncio.get_running_loop().call_later(
            3, os.kill, os.getpid(), signal.SIGTERM)

    # -- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        if not self.env.tg_bot_token:
            log.warning("TG_BOT_TOKEN not set — telegram control disabled, channel alerts log-only")
            return
        self.app = ApplicationBuilder().token(self.env.tg_bot_token).build()
        gate = owner_gate(self.env, self.db)
        for name, fn in [
            ("start", self.cmd_start), ("help", self.cmd_start),
            ("status", self.cmd_status), ("profile", self.cmd_profile),
            ("pnl", self.cmd_pnl), ("positions", self.cmd_positions),
            ("set", self.cmd_set), ("settings", self.cmd_settings),
            ("setkey", self.cmd_setkey), ("setwallet", self.cmd_setwallet),
            ("setsub", self.cmd_setsub), ("pause", self.cmd_pause),
            ("resume", self.cmd_resume), ("stop", self.cmd_stop), ("mode", self.cmd_mode),
            ("confirm", self.cmd_confirm), ("report", self.cmd_report),
        ]:
            self.app.add_handler(CommandHandler(name, gate(fn)))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, gate(self.on_text)))
        self.app.add_handler(CallbackQueryHandler(gate(self.on_callback)))
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(allowed_updates=["message"])
        try:
            await self.app.bot.set_my_commands([BotCommand(c, d) for c, d in BOT_COMMANDS])
        except Exception as e:  # noqa: BLE001 — menu registration is cosmetic
            log.warning("set_my_commands failed: %s", e)
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

    async def _submit_reply(self, update: Update, name: str, **kw) -> None:
        """Run an engine command and ALWAYS reply — failures included."""
        try:
            await self._reply(update, await self.engine.submit(name, **kw))
        except Exception as e:  # noqa: BLE001 — surface the failure to the owner
            log.exception("command %s failed", name)
            await self.db.log_event("ERROR", f"/{name} gagal: {e}")
            msg = str(e)
            if len(msg) > 300:
                msg = msg[:300] + "… (detail lengkap di log server)"
            await self._reply(update, f"⚠️ gagal: {msg}")

    def _upnl(self) -> Optional[float]:
        pos, mid = self.state.position, self.feed.mid
        if pos is None or not pos.ts_open or not mid:
            return None
        d = 1 if pos.side == "long" else -1
        return d * (mid - pos.entry_px) * pos.filled_sz

    # -- commands ----------------------------------------------------------
    async def cmd_start(self, update, context) -> None:
        await self._reply(update,
                          "FlowScalp siap. Perintah:\n"
                          "/status – kondisi bot, equity, PnL hari ini\n"
                          "/pnl day|week|all – statistik hasil trading\n"
                          "/positions – detail posisi terbuka\n"
                          "/profile – saldo & ringkasan hasil\n"
                          "/set <key> <nilai> – ubah pengaturan\n"
                          "/settings – semua pengaturan + penjelasan\n"
                          "/setkey <key> – simpan API key agent HL (pesan auto-terhapus)\n"
                          "/setwallet <0x…> – simpan alamat wallet utama\n"
                          "/setsub <0x…> – simpan alamat subaccount\n"
                          "/pause • /resume – tahan / lanjutkan entry baru\n"
                          "/stop – TOMBOL DARURAT: tutup semua sekarang\n"
                          "/mode paper|live – ganti mode (live butuh /confirm <kode>)\n"
                          "/report – kirim ringkasan hari ini")

    async def cmd_status(self, update, context) -> None:
        st = self.state
        equity = await self.engine.executor.equity() if self.engine.executor else 0.0
        uptime_min = int((time.time() * 1000 - st.boot_ts) / 60_000)
        await self._reply(update, messages.status_text(st, self.store.current, equity,
                                                       self._upnl(), self.feed.mid, uptime_min))

    async def cmd_profile(self, update, context) -> None:
        st = self.state
        equity = await self.engine.executor.equity() if self.engine.executor else 0.0
        now = int(time.time() * 1000)
        day = await self.db.summary(st.mode, since_ts=st.day_start_ts)
        week = await self.db.summary(st.mode, since_ts=now - 7 * 86_400_000)
        alls = await self.db.summary(st.mode)
        await self._reply(update, messages.profile_text(st, equity, day, week, alls))

    # -- credential setters (.env writers) -----------------------------------
    # A bare command (e.g. tapped from the Telegram menu) arms a 2-minute
    # wait: the owner's NEXT plain message is taken as the value. Messages
    # that may contain a secret are always deleted and never echoed back.
    async def cmd_setkey(self, update, context) -> None:
        await self._credential_cmd(update, context, "setkey")

    async def cmd_setwallet(self, update, context) -> None:
        await self._credential_cmd(update, context, "setwallet")

    async def cmd_setsub(self, update, context) -> None:
        await self._credential_cmd(update, context, "setsub")

    async def _credential_cmd(self, update, context, name: str) -> None:
        spec = _ENV_SPECS[name]
        arg = context.args[0].strip() if context.args else ""
        if not arg:
            self._pending_input = ("env", name, time.monotonic())
            extra = "kirim 'hapus' untuk mengosongkan, atau " if spec["allow_empty"] else ""
            await self._reply(update,
                              f"OK — sekarang kirim {spec['label']} sebagai pesan berikutnya.\n"
                              f"({extra}ketik 'batal' untuk membatalkan)\n"
                              f"pesan Anda akan otomatis dihapus demi keamanan.")
            return
        await self._apply_env_value(update, arg, spec)

    async def on_text(self, update, context) -> None:
        """Owner plain text: consumed as the pending credential/setting value,
        otherwise a gentle hint."""
        text = (update.effective_message.text or "").strip()
        pend, self._pending_input = self._pending_input, None
        if pend and time.monotonic() - pend[2] <= PENDING_INPUT_TTL_S:
            kind, name = pend[0], pend[1]
            if text.lower() in ("batal", "cancel"):
                await self._reply(update, "dibatalkan.")
                return
            if kind == "env":
                await self._apply_env_value(update, text, _ENV_SPECS[name])
                return
            ok, old, new = await self.store.set(name, text)
            if ok:
                await self.db.log_event("INFO", f"/set {name}: {old} → {new}")
                await self._reply(update, f"✅ {name}: {old} → {new}")
            else:
                await self._reply(update, f"⚠️ ditolak: {new}")
            return
        await self._reply(update, "saya hanya merespons perintah — tekan tombol Menu atau ketik /")

    # -- /set inline keyboard --------------------------------------------------
    def _settings_keyboard(self) -> InlineKeyboardMarkup:
        rows, row = [], []
        for k in RANGES:
            row.append(InlineKeyboardButton(k, callback_data=f"set:{k}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        return InlineKeyboardMarkup(rows)

    def _value_keyboard(self, key: str) -> InlineKeyboardMarkup:
        spec = RANGES[key]
        if spec["type"] is bool:
            vals = ["on", "off"]
        elif key in PRESETS:
            vals = [str(v) for v in PRESETS[key]]
        else:
            vals = []
        rows = [[InlineKeyboardButton(v, callback_data=f"setv:{key}:{v}") for v in vals[i:i + 3]]
                for i in range(0, len(vals), 3)]
        rows.append([InlineKeyboardButton("✏️ ketik nilai", callback_data=f"sett:{key}"),
                     InlineKeyboardButton("« kembali", callback_data="set:__list__")])
        return InlineKeyboardMarkup(rows)

    def _key_detail(self, key: str) -> str:
        spec = RANGES[key]
        cur = getattr(self.store.current, key)
        if isinstance(cur, tuple):
            cur = ", ".join(cur) or "(kosong)"
        rng = f"\nrentang: {spec['min']}–{spec['max']}" if "min" in spec else ""
        return f"{key} = {cur}{rng}\n{spec.get('desc', '')}\n\npilih nilai baru:"

    async def on_callback(self, update, context) -> None:
        q = update.callback_query
        try:
            await q.answer()
        except Exception:  # noqa: BLE001
            pass
        data = q.data or ""
        if data == "set:__list__":
            await q.edit_message_text("pilih setting yang mau diubah:",
                                      reply_markup=self._settings_keyboard())
            return
        if data.startswith("setv:"):
            _, key, val = data.split(":", 2)
            ok, old, new = await self.store.set(key, val)
            if ok:
                await self.db.log_event("INFO", f"/set {key}: {old} → {new}")
                await q.edit_message_text(f"✅ {key}: {old} → {new}\n\nubah yang lain?",
                                          reply_markup=self._settings_keyboard())
            else:
                await q.edit_message_text(f"⚠️ ditolak: {new}",
                                          reply_markup=self._value_keyboard(key))
            return
        if data.startswith("sett:"):
            key = data.split(":", 1)[1]
            self._pending_input = ("setting", key, time.monotonic())
            spec = RANGES.get(key, {})
            rng = f" [{spec['min']}–{spec['max']}]" if "min" in spec else ""
            await q.edit_message_text(f"ketik nilai baru untuk {key}{rng} sebagai pesan"
                                      f" berikutnya. ('batal' untuk membatalkan)")
            return
        if data.startswith("set:"):
            key = data.split(":", 1)[1]
            if key in RANGES:
                await q.edit_message_text(self._key_detail(key),
                                          reply_markup=self._value_keyboard(key))

    async def _apply_env_value(self, update, raw: str, spec: dict) -> None:
        chat_id = update.effective_chat.id
        try:
            await update.effective_message.delete()
        except Exception as e:  # noqa: BLE001
            log.warning("could not delete credential message: %s", e)

        async def say(text: str) -> None:
            await self.app.bot.send_message(chat_id=chat_id, text=text)

        if spec["allow_empty"] and raw.lower() in ("hapus", "kosong", "clear", ""):
            update_env_file({spec["env_key"]: ""})
            await self.db.log_event("INFO", f"{spec['env_key']} dikosongkan via telegram")
            await say(f"{spec['label']} dikosongkan. Bot restart otomatis ±5 detik…")
            self.restarter()
            return
        if not spec["pattern"].fullmatch(raw):
            await say(f"format {spec['label']} tidak valid (pesan Anda tetap dihapus demi keamanan).\n"
                      f"kirim ulang: key = 64 digit hex" if spec["secret"] else
                      f"format {spec['label']} tidak valid — harus 0x diikuti 40 digit hex.\n"
                      f"ulangi perintahnya untuk mencoba lagi.")
            return
        extra = ""
        if spec["secret"]:
            try:
                import eth_account
                raw = raw if raw.startswith("0x") else "0x" + raw
                agent_addr = eth_account.Account.from_key(raw).address
                extra = (f"\nalamat agent: {agent_addr}\n"
                         f"pastikan alamat ini yang ter-authorize di Hyperliquid (More → API).")
            except Exception:
                await say("key tidak valid (gagal diverifikasi) — pesan Anda sudah dihapus.")
                return
        update_env_file({spec["env_key"]: raw})
        await self.db.log_event("INFO", f"{spec['env_key']} diset via telegram")
        shown = ("tersimpan (pesan berisi key sudah dihapus demi keamanan)" if spec["secret"]
                 else f"tersimpan: {raw}")
        await say(f"✅ {spec['label']} {shown}.{extra}\n"
                  f"Bot restart otomatis ±5 detik untuk memuat nilai baru — "
                  f"setelah itu cek dengan /status.")
        self.restarter()

    async def cmd_pnl(self, update, context) -> None:
        period = (context.args[0].lower() if context.args else "day")
        now = int(time.time() * 1000)
        since = {"day": self.state.day_start_ts, "week": now - 7 * 86_400_000, "all": None}.get(period)
        if period not in ("day", "week", "all"):
            await self._reply(update, "cara pakai: /pnl day|week|all")
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
        if len(context.args) == 0:
            await update.effective_message.reply_text(
                "pilih setting yang mau diubah:", reply_markup=self._settings_keyboard())
            return
        if len(context.args) == 1:
            key = context.args[0]
            if key in RANGES:
                await update.effective_message.reply_text(
                    self._key_detail(key), reply_markup=self._value_keyboard(key))
            else:
                await self._reply(update, f"key {key!r} tidak dikenal — lihat /settings")
            return
        key, value = context.args[0], " ".join(context.args[1:])
        ok, old, new = await self.store.set(key, value)
        if ok:
            await self.db.log_event("INFO", f"/set {key}: {old} → {new}")
            await self._reply(update, f"{key}: {old} → {new}")
        else:
            await self._reply(update, f"ditolak: {new}")

    async def cmd_settings(self, update, context) -> None:
        await self._reply(update, messages.settings_text(self.store.current))

    async def cmd_pause(self, update, context) -> None:
        await self._submit_reply(update, "pause")

    async def cmd_resume(self, update, context) -> None:
        await self._submit_reply(update, "resume")

    async def cmd_stop(self, update, context) -> None:
        await self._submit_reply(update, "stop")

    async def cmd_mode(self, update, context) -> None:
        target = (context.args[0].lower() if context.args else "")
        if target not in ("paper", "live"):
            await self._reply(update, "cara pakai: /mode paper|live")
            return
        if target == "paper":
            await self._submit_reply(update, "mode", target="paper")
            return
        if self.state.pos_state != Pos.FLAT:
            await self._reply(update, "masih ada posisi/order terbuka — /stop dulu sebelum live")
            return
        code = self.confirm.request()
        await self._reply(update, f"⚠️ mode LIVE memakai dana sungguhan.\n"
                                  f"kirim /confirm {code} dalam {CONFIRM_TTL_S} detik untuk lanjut")

    async def cmd_confirm(self, update, context) -> None:
        code = context.args[0] if context.args else ""
        if not code:
            await self._reply(update, "cara pakai: /confirm <kode> (kode dari /mode live)")
            return
        if not self.confirm.check(code):
            await self._reply(update, "kode salah atau kedaluwarsa — ulangi /mode live")
            await self.db.log_event("WARN", "live confirm failed (bad/expired code)")
            return
        await self.db.log_event("INFO", "live mode confirmed by owner")
        await self._submit_reply(update, "mode", target="live")

    async def cmd_report(self, update, context) -> None:
        await self._submit_reply(update, "report")
