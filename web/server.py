"""Read-only dashboard API (FastAPI, in-process).

Every /api/* route requires `Authorization: Bearer DASH_BEARER_TOKEN`.
Uvicorn binds 127.0.0.1 only — public exposure is the owner's Cloudflare
Tunnel (see README). index.html is served openly but renders nothing
without the token.
"""
from __future__ import annotations

import os
import secrets
import time
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse

from core import timewin

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

RANGES_MS = {"1d": 86_400_000, "7d": 7 * 86_400_000, "30d": 30 * 86_400_000}


def now_ms() -> int:
    return int(time.time() * 1000)


def create_app(env, db, state, feed, store, engine) -> FastAPI:
    app = FastAPI(title="FlowScalp", docs_url=None, redoc_url=None, openapi_url=None)

    async def auth(authorization: Optional[str] = Header(None)) -> None:
        if getattr(env, "dash_public", False):
            return  # owner chose an open read-only dashboard
        expected = f"Bearer {env.dash_bearer_token}"
        if (not env.dash_bearer_token or authorization is None
                or not secrets.compare_digest(authorization, expected)):
            raise HTTPException(status_code=401, detail="unauthorized")

    @app.get("/")
    async def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.get("/api/status", dependencies=[Depends(auth)])
    async def status():
        equity = await engine.executor.equity() if engine.executor else 0.0
        pos = state.position
        open_position = None
        if pos is not None and pos.ts_open:
            d = 1 if pos.side == "long" else -1
            upnl = d * (feed.mid - pos.entry_px) * pos.filled_sz if feed.mid else 0.0
            open_position = {
                "coin": getattr(pos, "coin", "") or getattr(env, "coin", "BTC"),
                "side": pos.side, "entry_px": pos.entry_px, "size_btc": pos.filled_sz,
                "sl_px": pos.sl_px, "tp_px": pos.tp_px,
                "upnl_usd": round(upnl, 2),
                "minutes": max(0, (now_ms() - pos.ts_open) // 60_000),
            }
        return {
            "state": state.bot_state.value,
            "mode": state.mode,
            "equity": equity,
            "day_pnl_usd": round(state.day_realized_pnl, 2),
            "day_r": round(state.day_realized_r, 2),
            "open_position": open_position,
            "mid": feed.mid,
            "coin": getattr(env, "coin", "BTC"),
            "mids": dict(getattr(feed, "mids", {}) or {}),
            "streak": state.consec_losses,
            "uptime_min": max(0, (now_ms() - state.boot_ts) // 60_000),
            **(await _strategy_status()),
            "updated_at": now_ms() // 1000,
        }

    async def _strategy_status() -> dict:
        """Live answers to 'why no trade yet': session gate, CVD warm-up,
        and the most recent filtered-out signal."""
        s = store.current
        minute = timewin.minute_of_day_utc(now_ms())
        in_session = not s.session_windows or timewin.in_any_window(minute, s.session_windows)
        in_blackout = bool(s.blackout_windows) and timewin.in_any_window(minute, s.blackout_windows)
        deltas = list(getattr(feed, "deltas_1m", []) or [])
        need = s.sweep_lookback + 24
        cvd_ready = (not s.cvd_filter) or (
            len(deltas) >= need and all(d is not None for d in deltas[-need:]))
        row = await db.fetchone(
            "SELECT ts, msg FROM events WHERE msg LIKE 'gate skip%' OR msg LIKE 'entry blocked%'"
            " ORDER BY ts DESC LIMIT 1")
        return {
            "session_open": in_session and not in_blackout,
            "session_windows": list(s.session_windows),
            "cvd_ready": cvd_ready,
            "last_skip": {"ts": row["ts"], "msg": row["msg"]} if row else None,
        }

    @app.get("/api/summary", dependencies=[Depends(auth)])
    async def summary():
        now = now_ms()
        return {
            "day": await db.summary(state.mode, since_ts=state.day_start_ts),
            "7d": await db.summary(state.mode, since_ts=now - RANGES_MS["7d"]),
            "30d": await db.summary(state.mode, since_ts=now - RANGES_MS["30d"]),
            "all": await db.summary(state.mode),
        }

    @app.get("/api/trades", dependencies=[Depends(auth)])
    async def trades(limit: int = Query(50, ge=1, le=500)):
        return await db.recent_trades(state.mode, limit)

    @app.get("/api/equity", dependencies=[Depends(auth)])
    async def equity_curve(range: str = Query("1d")):
        if range not in RANGES_MS:
            raise HTTPException(status_code=422, detail="range must be 1d|7d|30d")
        return await db.equity_curve(state.mode, now_ms() - RANGES_MS[range])

    return app
