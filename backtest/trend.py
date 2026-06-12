"""Strategy v3 backtest: long-flat trend following on 4h bars.

Rules (mechanical, no discretion):
- ENTRY  : a 4h bar CLOSES above the highest high of the previous `lookback`
           bars → buy at the NEXT bar's open (taker + slippage; no lookahead).
- STOP   : initial stop = fill − atr_k × ATR(14); afterwards a chandelier
           trailing stop = max(stop, highest close since entry − atr_k × ATR)
           — it only ever rises. Exit when a bar's low touches it (taker).
- NO take-profit and NO time stop: the trade lives while the trend lives.
- COSTS  : taker fee both sides, 1bp adverse slippage, plus perp FUNDING
           accrued per bar on entry notional (long pays when funding > 0).

Sizing reuses risk.position_size (risk% of equity / stop distance, lev cap).
This is a strategy-level simulation: the live engine's daily-loss halt and
loss-streak cooldown are not applied here (they gate entries, not exits, and
would need a portfolio-level replay — added if the strategy graduates).

CLI:
  python -m backtest.trend --csv data/btc_4h.csv --oos 0.3 \
      --lookback 42 --atr-k 3.0 --tag "BTC tf4h lb42 k3.0"
1m input is auto-resampled to 4h (--frame-ms to override the bar size).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from config import FALLBACK_TAKER_FEE, Settings
from core.risk import position_size
from core.strategy import Candle, atr
from backtest.data import load_candles
from backtest.run import (BTTrade, SimResult, compute_report, plot_curve,
                          print_report, resample_candles)

FOUR_H_MS = 14_400_000
TAKER_SLIP = 0.0001
ATR_PERIOD = 14


def simulate_trend(candles: list[Candle], *, lookback: int = 42, atr_k: float = 3.0,
                   risk_pct: float = 0.5, start_equity: float = 10_000.0,
                   taker_fee: float = FALLBACK_TAKER_FEE, funding_8h_pct: float = 0.01,
                   sz_decimals: int = 5) -> SimResult:
    """Deterministic long-flat replay over closed bars (any fixed bar size)."""
    res = SimResult()
    s = Settings(risk_pct=risk_pct)
    bar_ms = FOUR_H_MS
    if len(candles) > 1:
        bar_ms = min(b.ts - a.ts for a, b in zip(candles[:50], candles[1:51]))
    funding_per_bar = funding_8h_pct / 100 * (bar_ms / 28_800_000)

    equity = start_equity
    pos: Optional[BTTrade] = None
    stop = 0.0
    hi_close = 0.0
    pending = False                    # breakout close seen → buy next open
    window = max(lookback + 2, ATR_PERIOD + 2)

    def close_trade(t: BTTrade, px: float, ts: int, reason: str) -> None:
        nonlocal pos, equity
        t.exit_px = px
        t.ts_close = ts
        t.fees += px * t.size_btc * taker_fee
        t.pnl = (t.exit_px - t.entry_px) * t.size_btc - t.fees
        t.r = t.pnl / t.entry_risk_usd if t.entry_risk_usd > 0 else 0.0
        t.reason = reason
        equity += t.pnl
        res.trades.append(t)
        res.curve.append((ts, equity))
        pos = None

    for i, bar in enumerate(candles):
        close_ts = bar.ts + bar_ms

        # ---- fill a pending breakout entry at THIS bar's open (no lookahead)
        if pending and pos is None:
            pending = False
            hist = candles[max(0, i - ATR_PERIOD - 1):i]
            a = atr(hist, ATR_PERIOD)
            if a > 0:
                fill = bar.open * (1 + TAKER_SLIP)
                sl = fill - atr_k * a
                if sl > 0:
                    size = position_size(equity, fill, sl, s, sz_decimals)
                    if size.ok:
                        t = BTTrade("long", bar.ts, entry_px=fill,
                                    size_btc=size.size_btc, size_usd=size.size_usd)
                        t.fees = size.size_usd * taker_fee
                        t.entry_risk_usd = size.size_usd * (fill - sl) / fill
                        pos = t
                        stop = sl
                        hi_close = fill
                    else:
                        res.skip_counts["size"] = res.skip_counts.get("size", 0) + 1

        # ---- manage the open position on this bar
        if pos is not None:
            pos.fees += pos.size_usd * funding_per_bar          # perp funding drag
            if bar.low <= stop:
                close_trade(pos, min(bar.open, stop), close_ts, "trail")
            else:
                hi_close = max(hi_close, bar.close)
                hist = candles[max(0, i - ATR_PERIOD):i + 1]
                a = atr(hist, ATR_PERIOD)
                if a > 0:
                    stop = max(stop, hi_close - atr_k * a)      # ratchet up only

        # ---- evaluate a fresh breakout on this CLOSED bar
        if pos is None and not pending and i >= window:
            highest = max(c.high for c in candles[i - lookback:i])
            if bar.close > highest:
                pending = True

    if pos is not None and candles:
        last = candles[-1]
        close_trade(pos, last.close * (1 - TAKER_SLIP), last.ts + bar_ms, "eod")
    res.curve.insert(0, (candles[0].ts if candles else 0, start_equity))
    return res


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="FlowScalp v3 trend-following backtest (4h, long-flat)")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--frame-ms", type=int, default=FOUR_H_MS,
                    help="bar size; input with smaller bars is resampled up")
    ap.add_argument("--lookback", type=int, default=42, help="breakout lookback (bars)")
    ap.add_argument("--atr-k", type=float, default=3.0, help="ATR multiple for the trailing stop")
    ap.add_argument("--risk", type=float, default=0.5)
    ap.add_argument("--fee-taker", type=float, default=FALLBACK_TAKER_FEE)
    ap.add_argument("--funding-8h", type=float, default=0.01,
                    help="perp funding per 8h in PERCENT charged while holding (0 = off)")
    ap.add_argument("--start-equity", type=float, default=10_000.0)
    ap.add_argument("--oos", type=float, help="hold out this fraction chronologically")
    ap.add_argument("--tag", default="", help="print a one-line RINGKAS summary with this label")
    ap.add_argument("--out", default="data/trend_report")
    a = ap.parse_args(argv)

    candles = load_candles(a.csv)
    if len(candles) > 1 and candles[1].ts - candles[0].ts < a.frame_ms:
        candles = resample_candles(candles, a.frame_ms)
    days = (candles[-1].ts - candles[0].ts) / 86_400_000 if candles else 0.0
    print(f"loaded {len(candles)} bars ({days:.1f} days) @ {a.frame_ms // 3_600_000}h"
          f" | lookback {a.lookback} | trail {a.atr_k}xATR | funding {a.funding_8h}%/8h")

    def run_one(tag: str, cs: list[Candle], write: bool = False) -> dict:
        res = simulate_trend(cs, lookback=a.lookback, atr_k=a.atr_k, risk_pct=a.risk,
                             start_equity=a.start_equity, taker_fee=a.fee_taker,
                             funding_8h_pct=a.funding_8h)
        rep = compute_report(res, a.start_equity)
        print_report(tag, rep)
        if write:
            os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
            with open(a.out + ".json", "w") as f:
                json.dump(rep, f, indent=2)
            plot_curve(res, a.out + ".png")
        return rep

    def pf(rep: dict) -> str:
        return "—" if rep["profit_factor"] is None else f"{rep['profit_factor']:.2f}"

    if a.oos:
        cut = int(len(candles) * (1 - a.oos))
        rep_is = run_one(f"IN-SAMPLE ({1 - a.oos:.0%})", candles[:cut], write=True)
        rep_o = run_one(f"OUT-OF-SAMPLE ({a.oos:.0%})", candles[cut:])
        if a.tag:
            print(f"RINGKAS | {a.tag} | IS {rep_is['trades']}t PF {pf(rep_is)}"
                  f" {rep_is['net_r']:+.1f}R | OOS {rep_o['trades']}t PF {pf(rep_o)}"
                  f" {rep_o['net_r']:+.1f}R")
    else:
        rep = run_one("FULL PERIOD", candles, write=True)
        if a.tag:
            print(f"RINGKAS | {a.tag} | FULL {rep['trades']}t PF {pf(rep)} {rep['net_r']:+.1f}R")
    return 0


if __name__ == "__main__":
    sys.exit(main())
