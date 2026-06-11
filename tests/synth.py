"""Synthetic candle fixtures for strategy/executor tests.

Base shape: flat 1m bars around `base` with range `rng` (open=close=base,
high=base+rng/2, low=base-rng/2), so swing_low = base-rng/2 and ATR == rng.
The last bar CLOSES at `close_utc`.
"""
from __future__ import annotations

from datetime import datetime, timezone

from config import Settings
from core.strategy import Candle

MIN_MS = 60_000


def ts_utc(s: str) -> int:
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)


def flat_1m(n: int = 80, base: float = 100_000.0, rng: float = 200.0, vol: float = 100.0,
            close_utc: str = "2026-01-05T12:00") -> list[Candle]:
    end_open = ts_utc(close_utc) - MIN_MS
    return [Candle(end_open - (n - 1 - i) * MIN_MS, base, base + rng / 2, base - rng / 2, base, vol)
            for i in range(n)]


def flat_15m(n: int = 220, base: float = 100_000.0) -> list[Candle]:
    t0 = ts_utc("2026-01-01T00:00")
    return [Candle(t0 + i * 900_000, base, base + 50, base - 50, base, 1000.0) for i in range(n)]


def trend_15m(n: int = 220, start: float = 90_000.0, step: float = 60.0) -> list[Candle]:
    t0 = ts_utc("2026-01-01T00:00")
    return [Candle(t0 + i * 900_000, start + i * step, start + i * step + 50,
                   start + i * step - 50, start + i * step, 1000.0) for i in range(n)]


def settings(**over) -> Settings:
    """Test settings: all confirmation gates off unless overridden."""
    base = dict(session_windows=("00:00-24:00",), blackout_windows=(),
                sweep_vol_mult=1.0, cvd_filter=False, bias_filter=False, funding_filter=False)
    base.update(over)
    return Settings(**base)


def long_sweep_same_bar(vol_sweep: float = 100.0, sweep_low: float = 99_845.0,
                        close_utc: str = "2026-01-05T12:00", unique_dip: bool = False) -> list[Candle]:
    """Same-bar sweep+reclaim of swing_low. Default swing_low = 99,900;
    with unique_dip the swing low is a single bar at 99,880 (index n-11)."""
    c = flat_1m(close_utc=close_utc)
    if unique_dip:
        b = c[-11]
        c[-11] = Candle(b.ts, b.open, b.high, 99_880.0, b.close, b.volume)
    t = c[-1].ts
    c[-1] = Candle(t, 99_950.0, 100_000.0, sweep_low, 99_960.0, vol_sweep)
    return c


def long_sweep_next_bar() -> list[Candle]:
    c = flat_1m()
    c[-2] = Candle(c[-2].ts, 99_950.0, 100_000.0, 99_820.0, 99_880.0, 100.0)
    c[-1] = Candle(c[-1].ts, 99_880.0, 100_000.0, 99_860.0, 99_960.0, 100.0)
    return c


def short_sweep_same_bar(close_utc: str = "2026-01-05T12:00") -> list[Candle]:
    c = flat_1m(close_utc=close_utc)
    t = c[-1].ts
    c[-1] = Candle(t, 100_050.0, 100_155.0, 100_000.0, 100_040.0, 100.0)
    return c
