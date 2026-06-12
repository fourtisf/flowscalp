"""Liquidity-sweep + reclaim strategy for BTC-PERP 1m, 15m bias filter.

Pure functions, no IO, no globals — the backtester imports this unchanged.
`evaluate` must be called ONLY on a closed 1m candle, only when FLAT and
entries are allowed. Confirmation gates (§6.3b) are conjunctive: a Signal is
returned only if every enabled gate passes.

Candle.ts is the bar OPEN time in unix milliseconds (UTC), matching the
Hyperliquid candle field "t"; a 1m bar closes at ts + 60_000.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from core import timewin

# Funding skew gate threshold: 0.005 % per hour, expressed as a decimal rate.
FUNDING_SKEW_LIMIT = 0.00005

ONE_MINUTE_MS = 60_000


@dataclass
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class Signal:
    side: str          # "long" | "short"
    entry_px: float
    sl_px: float
    tp_px: float
    swept_level: float # for anti-overtrade dedupe
    reason: str


@dataclass(frozen=True)
class EvalResult:
    """Outcome of one evaluation. `skip_gate` is set only when the base
    structure matched but a confirmation gate failed — those skips must be
    logged by the caller so filter performance can be audited later."""
    signal: Optional[Signal] = None
    skip_gate: Optional[str] = None
    detail: str = ""


def ema(values: Sequence[float], period: int) -> float:
    # standard EMA seeded with SMA of first `period` values
    sma = sum(values[:period]) / period
    k = 2 / (period + 1)
    e = sma
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def atr(c: Sequence["Candle"], period: int = 14) -> float:
    trs = []
    for i in range(1, len(c)):
        trs.append(max(c[i].high - c[i].low,
                       abs(c[i].high - c[i - 1].close),
                       abs(c[i].low - c[i - 1].close)))
    if len(trs) < period:
        return 0.0
    return sum(trs[-period:]) / period


def _bias(c15m, s) -> str:
    if not s.bias_filter:
        return "both"
    e200 = ema([c.close for c in c15m], 200)
    px = c15m[-1].close
    if px > e200 * 1.001:
        return "long"
    if px < e200 * 0.999:
        return "short"
    return "both"


def session_ok(close_ts_ms: int, s) -> bool:
    """Whitelist session windows AND outside every blackout window (UTC,
    half-open). Empty session_windows = no whitelist restriction."""
    minute = timewin.minute_of_day_utc(close_ts_ms)
    if s.session_windows and not timewin.in_any_window(minute, s.session_windows):
        return False
    if s.blackout_windows and timewin.in_any_window(minute, s.blackout_windows):
        return False
    return True


def _volume_gate(c1m: Sequence[Candle], sweep_idx: int, s) -> tuple[bool, str]:
    """Sweep-bar volume >= sweep_vol_mult x SMA20 of the 20 bars before it.
    sweep_vol_mult == 1.0 disables the gate."""
    if s.sweep_vol_mult <= 1.0:
        return True, ""
    if sweep_idx < 20:
        return False, "vol window short"
    sma = sum(b.volume for b in c1m[sweep_idx - 20:sweep_idx]) / 20
    if sma <= 0:
        return True, "vol sma0"
    mult = c1m[sweep_idx].volume / sma
    return mult >= s.sweep_vol_mult, f"vol {mult:.1f}x"


def _cvd_gate(deltas, swing_idx: int, n: int, side: str) -> tuple[Optional[bool], str]:
    """CVD absorption divergence. With cvd[i] = cumsum(deltas[:i+1]), long
    requires cvd_at_reclaim_close >= cvd_at_bar_of_swing_low, i.e. the net
    delta from the swing bar to now is >= 0 while price made a lower low.
    Short is the mirror. Returns (None, _) when no delta data is supplied at
    all (backtest without trades input — gate skipped); a None inside the
    window means warm-up → fail closed."""
    if deltas is None:
        return None, "cvd n/a"
    if len(deltas) != n:
        return False, "cvd misaligned"
    span = list(deltas[swing_idx + 1:n])
    if any(d is None for d in span):
        return False, "cvd warm-up"
    net = float(sum(span))
    ok = net >= 0 if side == "long" else net <= 0
    return ok, f"cvd {net:+.2f}"


def _last_index_of(values: list[float], target: float, offset: int) -> int:
    """Absolute index (offset + i) of the last occurrence of target."""
    for i in range(len(values) - 1, -1, -1):
        if values[i] == target:
            return offset + i
    return offset + len(values) - 1  # unreachable for well-formed input


def evaluate_ex(c1m: Sequence["Candle"], c15m: Sequence["Candle"], deltas, s,
                funding_hourly: Optional[float] = None) -> EvalResult:
    """Full evaluation with skip diagnostics. Gate order (cheapest first):
    session → ATR band → bias → structure (incl. SL cap) → volume → CVD → funding."""
    n = len(c1m)
    # the bias-frame history (EMA200) is only required when the bias filter is on
    if n < max(s.sweep_lookback + 3, 60) or (s.bias_filter and len(c15m) < 210):
        return EvalResult()
    last, prev = c1m[-1], c1m[-2]

    if not session_ok(last.ts + ONE_MINUTE_MS, s):
        return EvalResult()

    a = atr(c1m, 14)
    atr_bps = a / last.close * 10_000
    if not (s.atr_floor_bps <= atr_bps <= s.atr_ceil_bps):
        return EvalResult()

    bias = _bias(c15m, s)
    window = c1m[-(s.sweep_lookback + 2):-2]   # exclude last 2 bars
    win_offset = n - (s.sweep_lookback + 2)
    swing_low = min(c.low for c in window)
    swing_high = max(c.high for c in window)
    pen = s.sweep_min_pen_pct / 100

    # ---- LONG: sweep below swing_low then reclaim ----
    long_same = last.low <= swing_low * (1 - pen) and last.close > swing_low
    long_next = (prev.low <= swing_low * (1 - pen) and prev.close <= swing_low
                 and last.close > swing_low and last.low > prev.low)
    if bias in ("long", "both") and (long_same or long_next):
        sweep_low = prev.low if long_next else last.low
        entry = last.close
        sl = sweep_low * (1 - s.sl_buffer_pct / 100)
        sl_dist_pct = (entry - sl) / entry * 100
        if 0 < sl_dist_pct <= s.sl_cap_pct:
            base = f"sweep+reclaim {swing_low:.0f} low | atr {atr_bps:.0f}bps | bias {bias}"
            sweep_idx = n - 2 if long_next else n - 1
            swing_idx = _last_index_of([c.low for c in window], swing_low, win_offset)
            return _confirm("long", entry, sl,
                            tp=entry + s.tp_r * (entry - sl),
                            swept_level=round(swing_low), base=base,
                            c1m=c1m, deltas=deltas, s=s, sweep_idx=sweep_idx,
                            swing_idx=swing_idx, funding_hourly=funding_hourly)

    # ---- SHORT: exact mirror on swing_high ----
    short_same = last.high >= swing_high * (1 + pen) and last.close < swing_high
    short_next = (prev.high >= swing_high * (1 + pen) and prev.close >= swing_high
                  and last.close < swing_high and last.high < prev.high)
    if bias in ("short", "both") and (short_same or short_next):
        sweep_high = prev.high if short_next else last.high
        entry = last.close
        sl = sweep_high * (1 + s.sl_buffer_pct / 100)
        sl_dist_pct = (sl - entry) / entry * 100
        if 0 < sl_dist_pct <= s.sl_cap_pct:
            base = f"sweep+reclaim {swing_high:.0f} high | atr {atr_bps:.0f}bps | bias {bias}"
            sweep_idx = n - 2 if short_next else n - 1
            swing_idx = _last_index_of([c.high for c in window], swing_high, win_offset)
            return _confirm("short", entry, sl,
                            tp=entry - s.tp_r * (sl - entry),
                            swept_level=round(swing_high), base=base,
                            c1m=c1m, deltas=deltas, s=s, sweep_idx=sweep_idx,
                            swing_idx=swing_idx, funding_hourly=funding_hourly)
    return EvalResult()


def _confirm(side: str, entry: float, sl: float, tp: float, swept_level: float, base: str,
             *, c1m, deltas, s, sweep_idx: int, swing_idx: int,
             funding_hourly: Optional[float]) -> EvalResult:
    """Confirmation stack v2 — conjunctive gates layered on a matched structure."""
    parts = []

    vol_ok, vol_d = _volume_gate(c1m, sweep_idx, s)
    if not vol_ok:
        return EvalResult(None, "volume", f"{side} {base} | {vol_d} < {s.sweep_vol_mult:.1f}x")
    if vol_d:
        parts.append(vol_d)

    if s.cvd_filter:
        cvd_ok, cvd_d = _cvd_gate(deltas, swing_idx, len(c1m), side)
        if cvd_ok is False:
            gate = "cvd_warmup" if "warm-up" in cvd_d else "cvd"
            return EvalResult(None, gate, f"{side} {base} | {cvd_d}")
        if cvd_ok is True:
            parts.append(f"{cvd_d} ok")
        # cvd_ok None → no delta data supplied (backtest) → gate skipped

    if s.funding_filter and funding_hourly is not None:
        allowed = (funding_hourly <= FUNDING_SKEW_LIMIT if side == "long"
                   else funding_hourly >= -FUNDING_SKEW_LIMIT)
        if not allowed:
            return EvalResult(None, "funding", f"{side} {base} | funding {funding_hourly * 100:.4f}%/h")
        parts.append("fund ok")

    reason = base if not parts else base + " | " + " | ".join(parts)
    return EvalResult(Signal(side, entry, sl, tp, swept_level=swept_level, reason=reason))


def evaluate(c1m: Sequence["Candle"], c15m: Sequence["Candle"], deltas, s,
             funding_hourly: Optional[float] = None) -> Optional[Signal]:
    """Call ONLY on a closed 1m candle, only when FLAT and entries allowed."""
    return evaluate_ex(c1m, c15m, deltas, s, funding_hourly).signal
