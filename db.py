"""SQLite (WAL) persistence layer. One connection, one-writer pattern.

All timestamps are unix milliseconds (UTC). All helpers are async (aiosqlite).
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Iterable, Optional

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mode TEXT NOT NULL,                -- paper | live
  side TEXT NOT NULL,                -- long | short
  ts_open INTEGER NOT NULL, ts_close INTEGER,
  entry_px REAL NOT NULL, exit_px REAL,
  size_btc REAL NOT NULL, size_usd REAL NOT NULL,
  sl_px REAL NOT NULL, tp_px REAL NOT NULL,
  fees_usd REAL DEFAULT 0, pnl_usd REAL, r_multiple REAL,
  exit_reason TEXT,                  -- tp | sl | time_stop | kill | manual
  signal_reason TEXT
);
CREATE TABLE IF NOT EXISTS equity_snapshots (ts INTEGER, mode TEXT, equity REAL);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER);
CREATE TABLE IF NOT EXISTS events (ts INTEGER, level TEXT, msg TEXT);
CREATE INDEX IF NOT EXISTS idx_trades_close ON trades (mode, ts_close);
CREATE INDEX IF NOT EXISTS idx_equity_ts ON equity_snapshots (mode, ts);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events (ts);
"""


def now_ms() -> int:
    return int(time.time() * 1000)


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._wlock = asyncio.Lock()

    async def connect(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    # -- low level -----------------------------------------------------------
    async def _write(self, sql: str, params: Iterable[Any] = ()) -> aiosqlite.Cursor:
        assert self._conn, "db not connected"
        async with self._wlock:
            cur = await self._conn.execute(sql, tuple(params))
            await self._conn.commit()
            return cur

    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[aiosqlite.Row]:
        assert self._conn, "db not connected"
        cur = await self._conn.execute(sql, tuple(params))
        rows = await cur.fetchall()
        await cur.close()
        return list(rows)

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[aiosqlite.Row]:
        rows = await self.fetchall(sql, params)
        return rows[0] if rows else None

    # -- trades --------------------------------------------------------------
    async def insert_trade(self, *, mode: str, side: str, ts_open: int, entry_px: float,
                           size_btc: float, size_usd: float, sl_px: float, tp_px: float,
                           fees_usd: float = 0.0, signal_reason: str = "") -> int:
        cur = await self._write(
            "INSERT INTO trades (mode, side, ts_open, entry_px, size_btc, size_usd, sl_px, tp_px,"
            " fees_usd, signal_reason) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (mode, side, ts_open, entry_px, size_btc, size_usd, sl_px, tp_px, fees_usd, signal_reason),
        )
        return cur.lastrowid

    async def close_trade(self, trade_id: int, *, ts_close: int, exit_px: float, fees_usd: float,
                          pnl_usd: float, r_multiple: float, exit_reason: str) -> None:
        await self._write(
            "UPDATE trades SET ts_close=?, exit_px=?, fees_usd=?, pnl_usd=?, r_multiple=?,"
            " exit_reason=? WHERE id=?",
            (ts_close, exit_px, fees_usd, pnl_usd, r_multiple, exit_reason, trade_id),
        )

    async def recent_trades(self, mode: str, limit: int = 50) -> list[dict]:
        rows = await self.fetchall(
            "SELECT * FROM trades WHERE mode=? AND ts_close IS NOT NULL ORDER BY ts_close DESC LIMIT ?",
            (mode, limit),
        )
        return [dict(r) for r in rows]

    async def summary(self, mode: str, since_ts: Optional[int] = None) -> dict:
        """Aggregate stats over closed trades (and equity snapshots for max DD)."""
        where = "mode=? AND ts_close IS NOT NULL"
        params: list[Any] = [mode]
        if since_ts is not None:
            where += " AND ts_close>=?"
            params.append(since_ts)
        row = await self.fetchone(
            f"""SELECT COUNT(*) AS trades,
                       SUM(CASE WHEN r_multiple>=0 THEN 1 ELSE 0 END) AS wins,
                       COALESCE(SUM(pnl_usd),0) AS pnl_usd,
                       COALESCE(SUM(r_multiple),0) AS net_r,
                       COALESCE(SUM(fees_usd),0) AS fees_usd,
                       COALESCE(SUM(CASE WHEN pnl_usd>0 THEN pnl_usd ELSE 0 END),0) AS gross_win,
                       COALESCE(SUM(CASE WHEN pnl_usd<0 THEN -pnl_usd ELSE 0 END),0) AS gross_loss
                FROM trades WHERE {where}""",
            params,
        )
        trades = row["trades"] or 0
        wins = row["wins"] or 0
        pf = (row["gross_win"] / row["gross_loss"]) if row["gross_loss"] > 0 else (float("inf") if row["gross_win"] > 0 else 0.0)
        return {
            "trades": trades,
            "wins": wins,
            "losses": trades - wins,
            "win_rate": (wins / trades) if trades else 0.0,
            "net_r": row["net_r"],
            "pnl_usd": row["pnl_usd"],
            "fees_usd": row["fees_usd"],
            "profit_factor": pf if pf != float("inf") else None,
            "max_dd_pct": await self.max_drawdown_pct(mode, since_ts),
        }

    async def max_drawdown_pct(self, mode: str, since_ts: Optional[int] = None) -> float:
        where = "mode=?"
        params: list[Any] = [mode]
        if since_ts is not None:
            where += " AND ts>=?"
            params.append(since_ts)
        rows = await self.fetchall(f"SELECT equity FROM equity_snapshots WHERE {where} ORDER BY ts", params)
        peak, dd = 0.0, 0.0
        for r in rows:
            eq = r["equity"]
            peak = max(peak, eq)
            if peak > 0:
                dd = max(dd, (peak - eq) / peak)
        return dd * 100

    # -- equity / events -----------------------------------------------------
    async def snapshot_equity(self, mode: str, equity: float, ts: Optional[int] = None) -> None:
        await self._write("INSERT INTO equity_snapshots (ts, mode, equity) VALUES (?,?,?)",
                          (ts or now_ms(), mode, equity))

    async def equity_curve(self, mode: str, since_ts: int) -> list[dict]:
        rows = await self.fetchall(
            "SELECT ts, equity FROM equity_snapshots WHERE mode=? AND ts>=? ORDER BY ts",
            (mode, since_ts),
        )
        return [{"ts": r["ts"], "equity": r["equity"]} for r in rows]

    async def log_event(self, level: str, msg: str, ts: Optional[int] = None) -> None:
        await self._write("INSERT INTO events (ts, level, msg) VALUES (?,?,?)", (ts or now_ms(), level, msg))

    # -- settings ------------------------------------------------------------
    async def get_setting(self, key: str) -> Optional[str]:
        row = await self.fetchone("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else None

    async def set_setting(self, key: str, value: str) -> None:
        await self._write(
            "INSERT INTO settings (key, value, updated_at) VALUES (?,?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, now_ms()),
        )

    async def all_settings(self) -> dict[str, str]:
        rows = await self.fetchall("SELECT key, value FROM settings")
        return {r["key"]: r["value"] for r in rows}
