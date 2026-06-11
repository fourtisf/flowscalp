"""UTC time-window helpers shared by config validation, strategy and risk gates.

Windows are "HH:MM-HH:MM" strings interpreted in UTC as half-open intervals
[start, end). A window whose end is before its start crosses midnight
(e.g. "22:00-02:00" = 22:00 <= t < 24:00 OR 00:00 <= t < 02:00).
"24:00" is accepted as an end-of-day alias. start == end covers the full day.
"""
from __future__ import annotations

import re
from typing import Sequence

_WINDOW_RE = re.compile(
    r"^(?P<h1>[01]?\d|2[0-3]):(?P<m1>[0-5]\d)-(?:(?P<h2>[01]?\d|2[0-3]):(?P<m2>[0-5]\d)|(?P<eod>24:00))$"
)

MINUTES_PER_DAY = 1440


def parse_window(spec: str) -> tuple[int, int]:
    """Parse "HH:MM-HH:MM" into (start_minute, end_minute). Raises ValueError."""
    m = _WINDOW_RE.match(spec.strip())
    if not m:
        raise ValueError(f"invalid window {spec!r}, expected HH:MM-HH:MM (UTC)")
    start = int(m.group("h1")) * 60 + int(m.group("m1"))
    end = MINUTES_PER_DAY if m.group("eod") else int(m.group("h2")) * 60 + int(m.group("m2"))
    return start, end


def parse_windows(specs: Sequence[str]) -> list[tuple[int, int]]:
    return [parse_window(s) for s in specs]


def minute_of_day_utc(ts_ms: int) -> int:
    return (ts_ms // 60_000) % MINUTES_PER_DAY


def in_window(minute: int, window: tuple[int, int]) -> bool:
    start, end = window
    if start == end:
        return True  # degenerate window = full day
    if end == MINUTES_PER_DAY:
        end = 0
        if start == 0:
            return True
    if start < end:
        return start <= minute < end
    if end == 0:  # plain "x-24:00" window
        return minute >= start
    return minute >= start or minute < end  # crosses midnight


def in_any_window(minute: int, specs: Sequence[str]) -> bool:
    return any(in_window(minute, parse_window(s)) for s in specs)
