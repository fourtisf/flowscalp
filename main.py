"""FlowScalp entrypoint: one process, asyncio tasks for feed, engine,
telegram bot, web server, timer and equity snapshotter.

Boot mode policy (security rule §2.6): the bot ALWAYS boots in paper mode.
The single exception is crash recovery — if the state snapshot shows the bot
was LIVE with an open position/exposure, it resumes live to keep managing it
(leaving a live position unmanaged is the more dangerous failure mode).
Going live fresh always requires /mode live + /confirm in Telegram.
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import re
import signal
import time

import uvicorn

from config import Env, SettingsStore
from core.engine import Engine
from core.feed import Feed
from core.state import BotState
from db import Database
from tg.bot import Notifier, TgBot
from web.server import create_app

log = logging.getLogger("flowscalp")

KEY_RE = re.compile(r"(0x)?[0-9a-fA-F]{64}")


class RedactFilter(logging.Filter):
    """Scrub anything that looks like a private key from every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if KEY_RE.search(msg):
            record.msg = KEY_RE.sub("[REDACTED_KEY]", msg)
            record.args = ()
        return True


def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    handlers = [
        logging.StreamHandler(),  # journald via systemd
        logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "flowscalp.log"), maxBytes=10_000_000, backupCount=7),
    ]
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(RedactFilter())
        root.addHandler(h)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.WARNING)


def snapshot_path(env: Env) -> str:
    return os.path.join(os.path.dirname(env.db_path) or ".", "state_snapshot.json")


def decide_boot_mode(env: Env, snap: dict | None) -> tuple[str, str]:
    """paper unless the crash snapshot shows live exposure to manage."""
    if snap and snap.get("mode") == "live" and snap.get("position") \
            and env.hl_agent_private_key and env.hl_main_wallet_address:
        return "live", "crash recovery: snapshot shows a LIVE position — resuming live to manage it"
    if env.mode == "live":
        return "paper", "MODE=live in .env is ignored at boot — going live requires /mode live + /confirm"
    return "paper", ""


async def amain() -> None:
    env = Env.load()
    setup_logging(env.log_dir)

    db = Database(env.db_path)
    await db.connect()
    store = SettingsStore(db)
    await store.load()

    snap = BotState.load_snapshot(snapshot_path(env))
    boot_mode, note = decide_boot_mode(env, snap)
    state = BotState(snapshot_path(env), mode=boot_mode)
    if note:
        log.warning(note)
        await db.log_event("WARN", note)

    queue: asyncio.Queue = asyncio.Queue()
    feed = Feed(env, queue, db)
    notifier = Notifier(env)
    engine = Engine(env, db, store, state, feed, queue, notifier)
    bot = TgBot(env, db, store, state, feed, engine, notifier)

    stop_ev = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_ev.set)

    # market data first (retry in background on failure so TG/web stay up)
    async def feed_starter():
        backoff = 5
        while not stop_ev.is_set():
            try:
                await feed.start()
                return
            except Exception as e:  # noqa: BLE001 — boot must survive API outages
                log.error("feed bootstrap failed: %s — retrying in %ds", e, backoff)
                await db.log_event("ERROR", f"feed bootstrap failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    feed_task = asyncio.create_task(feed_starter(), name="feed-starter")
    await engine.boot()
    await bot.start()
    if note:
        await notifier.channel(f"⚠️ {note}")

    web_app = create_app(env, db, state, feed, store, engine)
    server = uvicorn.Server(uvicorn.Config(web_app, host=env.dash_host, port=env.dash_port,
                                           log_level="warning"))

    async def timer_task():
        while not stop_ev.is_set():
            queue.put_nowait(("timer", int(time.time() * 1000)))
            await asyncio.sleep(1)

    async def snapshotter():
        while not stop_ev.is_set():
            await asyncio.sleep(60)
            try:
                if engine.executor is not None:
                    await db.snapshot_equity(state.mode, await engine.executor.equity())
            except Exception as e:  # noqa: BLE001
                log.warning("equity snapshot failed: %s", e)

    tasks = [
        feed_task,
        asyncio.create_task(engine.run(), name="engine"),
        asyncio.create_task(server.serve(), name="web"),
        asyncio.create_task(timer_task(), name="timer"),
        asyncio.create_task(snapshotter(), name="snapshotter"),
    ]
    await db.log_event("INFO", f"flowscalp up: mode={state.mode} state={state.bot_state.value}"
                               f" dash=http://{env.dash_host}:{env.dash_port}")
    log.info("flowscalp running (mode=%s, dashboard http://%s:%d)",
             state.mode, env.dash_host, env.dash_port)

    await stop_ev.wait()
    log.info("shutting down…")
    server.should_exit = True
    await feed.stop()
    await bot.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    state.save_snapshot()
    await db.log_event("INFO", "clean shutdown")
    await db.close()


if __name__ == "__main__":
    asyncio.run(amain())
