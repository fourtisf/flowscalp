"""Backtester: replays 1m candles through the SAME strategy.evaluate +
risk.position_size + PaperExecutor fill rules used live.

Fill model (mirrors PaperExecutor at 1m granularity):
- maker entry at signal close px; fills only when a LATER bar trades through
  (touch ≠ fill); pending lifetime = ceil(maker_timeout_s/60) bars, then
  canceled (or IOC taker fill at that bar's close + 1bp slip if allowed)
- exits intra-bar, SL-first when both SL and TP are inside the bar
- time stop / taker fills at bar close with 1bp adverse slippage
- engine-level anti-overtrade, daily loss halt and loss-streak cooldown are
  simulated identically to the live engine

CLI:
  python -m backtest.run --csv data/btc_1m.csv [--trades data/aggtrades/]
      --risk 0.5 --tp-r 1.8 --sl-cap 0.5 [--oos 0.3] [--grid] [--ablate]
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sys
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from config import FALLBACK_MAKER_FEE, FALLBACK_TAKER_FEE, Settings
from core.risk import position_size
from core.state import utc_day
from core.strategy import Candle, evaluate_ex
from backtest.data import load_agg_trades, load_candles

ONE_MIN_MS = 60_000
FIFTEEN_MIN_MS = 900_000
TAKER_SLIP = 0.0001
POST_EXIT_BAR_SPACING = 3
WARMUP_15M = 210


@dataclass
class BTTrade:
    side: str
    ts_open: int
    ts_close: int = 0
    entry_px: float = 0.0
    exit_px: float = 0.0
    size_btc: float = 0.0
    size_usd: float = 0.0
    entry_risk_usd: float = 0.0
    fees: float = 0.0
    pnl: float = 0.0
    r: float = 0.0
    reason: str = ""
    swept_level: Optional[float] = None


@dataclass
class SimResult:
    trades: list[BTTrade] = field(default_factory=list)
    curve: list[tuple[int, float]] = field(default_factory=list)
    skip_counts: dict = field(default_factory=dict)
    cvd_used: bool = False


def simulate(candles: list[Candle], s: Settings, *, deltas_map: Optional[dict[int, float]] = None,
             start_equity: float = 10_000.0, maker_fee: float = FALLBACK_MAKER_FEE,
             taker_fee: float = FALLBACK_TAKER_FEE, sz_decimals: int = 5) -> SimResult:
    res = SimResult(cvd_used=deltas_map is not None)
    k1 = max(s.sweep_lookback + 24, 60)          # exact: every evaluate component is look-back bounded
    c1m: deque[Candle] = deque(maxlen=k1)
    d1m: deque[Optional[float]] = deque(maxlen=k1)
    c15m: deque[Candle] = deque(maxlen=300)      # full buffer: EMA200 result depends on history length
    cur15: Optional[Candle] = None

    equity = start_equity
    pending: Optional[dict] = None               # side, px, sz, bars_left, sig
    pos: Optional[BTTrade] = None
    sl_px = tp_px = 0.0
    last_exit_bar_ts = 0
    last_swept = {"long": None, "short": None}
    consec_losses = 0
    cooldown_until = 0
    day = ""
    day_pnl = 0.0
    halted = False
    pending_lifetime = max(1, math.ceil(s.maker_timeout_s / 60))

    def close_trade(t: BTTrade, px: float, ts: int, reason: str, fee_rate: float) -> None:
        nonlocal pos, equity, consec_losses, cooldown_until, day_pnl, halted, last_exit_bar_ts
        t.exit_px = px
        t.ts_close = ts
        t.fees += px * t.size_btc * fee_rate
        d = 1 if t.side == "long" else -1
        t.pnl = d * (t.exit_px - t.entry_px) * t.size_btc - t.fees
        t.r = t.pnl / t.entry_risk_usd if t.entry_risk_usd > 0 else 0.0
        t.reason = reason
        equity += t.pnl
        day_pnl += t.pnl
        consec_losses = consec_losses + 1 if t.r < 0 else 0
        last_exit_bar_ts = ts // ONE_MIN_MS * ONE_MIN_MS
        last_swept[t.side] = t.swept_level
        res.trades.append(t)
        res.curve.append((ts, equity))
        pos = None
        if day_pnl <= -(s.daily_loss_limit_pct / 100) * day_start_equity:
            halted = True
        elif consec_losses >= s.max_consec_losses:
            cooldown_until = ts + s.cooldown_min * 60_000

    day_start_equity = equity
    for bar in candles:
        # ---- roll 15m bars from the 1m stream (closed bars only, no lookahead)
        b15 = bar.ts // FIFTEEN_MIN_MS * FIFTEEN_MIN_MS
        if cur15 is None or b15 > cur15.ts:
            if cur15 is not None:
                c15m.append(cur15)
            cur15 = Candle(b15, bar.open, bar.high, bar.low, bar.close, bar.volume)
        else:
            cur15.high = max(cur15.high, bar.high)
            cur15.low = min(cur15.low, bar.low)
            cur15.close = bar.close
            cur15.volume += bar.volume

        close_ts = bar.ts + ONE_MIN_MS

        # ---- UTC day rollover
        d = utc_day(close_ts)
        if d != day:
            day = d
            day_pnl = 0.0
            day_start_equity = equity
            halted = False

        # ---- 1. pending maker entry: fill-through, else expire
        if pending is not None:
            through = (bar.low < pending["px"]) if pending["side"] == "long" else (bar.high > pending["px"])
            if through:
                t = BTTrade(pending["side"], close_ts, entry_px=pending["px"],
                            size_btc=pending["sz"], size_usd=pending["px"] * pending["sz"])
                t.fees = t.size_usd * maker_fee
                t.entry_risk_usd = t.size_usd * abs(t.entry_px - pending["sl"]) / t.entry_px
                t.swept_level = pending["swept"]
                sl_px, tp_px = pending["sl"], pending["tp"]
                pos = t
                pending = None
            else:
                pending["bars_left"] -= 1
                if pending["bars_left"] <= 0:
                    if s.allow_taker_entry:
                        px = bar.close * (1 + TAKER_SLIP) if pending["side"] == "long" \
                            else bar.close * (1 - TAKER_SLIP)
                        t = BTTrade(pending["side"], close_ts, entry_px=px,
                                    size_btc=pending["sz"], size_usd=px * pending["sz"])
                        t.fees = t.size_usd * taker_fee
                        t.entry_risk_usd = t.size_usd * abs(px - pending["sl"]) / px
                        t.swept_level = pending["swept"]
                        sl_px, tp_px = pending["sl"], pending["tp"]
                        pos = t
                    pending = None

        # ---- 2. open position: SL-first intra-bar, then TP, then time stop
        if pos is not None:
            if pos.side == "long":
                sl_hit, tp_hit = bar.low <= sl_px, bar.high >= tp_px
            else:
                sl_hit, tp_hit = bar.high >= sl_px, bar.low <= tp_px
            if sl_hit:
                close_trade(pos, sl_px, close_ts, "sl", taker_fee)
            elif tp_hit:
                close_trade(pos, tp_px, close_ts, "tp", maker_fee)
            elif close_ts - pos.ts_open >= s.time_stop_min * ONE_MIN_MS:
                px = bar.close * (1 - TAKER_SLIP) if pos.side == "long" else bar.close * (1 + TAKER_SLIP)
                close_trade(pos, px, close_ts, "time_stop", taker_fee)

        # ---- 3. append bar and evaluate a fresh entry
        c1m.append(bar)
        d1m.append(deltas_map.get(bar.ts, 0.0) if deltas_map is not None else None)

        if pos is not None or pending is not None or len(c15m) < WARMUP_15M:
            continue
        if halted or close_ts < cooldown_until:
            continue
        if cooldown_until and close_ts >= cooldown_until:
            cooldown_until = 0
            consec_losses = 0
        if consec_losses >= s.max_consec_losses:
            continue
        if last_exit_bar_ts and (bar.ts - last_exit_bar_ts) // ONE_MIN_MS < POST_EXIT_BAR_SPACING:
            continue

        ev = evaluate_ex(list(c1m), list(c15m),
                         list(d1m) if deltas_map is not None else None, s, None)
        if ev.skip_gate is not None:
            res.skip_counts[ev.skip_gate] = res.skip_counts.get(ev.skip_gate, 0) + 1
            continue
        sig = ev.signal
        if sig is None:
            continue
        if last_swept.get(sig.side) == sig.swept_level:
            res.skip_counts["swept_dedupe"] = res.skip_counts.get("swept_dedupe", 0) + 1
            continue
        size = position_size(equity, sig.entry_px, sig.sl_px, s, sz_decimals)
        if not size.ok:
            res.skip_counts["size"] = res.skip_counts.get("size", 0) + 1
            continue
        pending = {"side": sig.side, "px": sig.entry_px, "sz": size.size_btc,
                   "sl": sig.sl_px, "tp": sig.tp_px, "swept": sig.swept_level,
                   "bars_left": pending_lifetime}

    # close any open position at the last bar (mark-out, taker)
    if pos is not None and candles:
        last = candles[-1]
        px = last.close * (1 - TAKER_SLIP) if pos.side == "long" else last.close * (1 + TAKER_SLIP)
        close_trade(pos, px, last.ts + ONE_MIN_MS, "eod", taker_fee)
    res.curve.insert(0, (candles[0].ts if candles else 0, start_equity))
    return res


# ============================================================ report
def compute_report(res: SimResult, start_equity: float) -> dict:
    tr = res.trades
    n = len(tr)
    wins = sum(1 for t in tr if t.r >= 0)
    gross_win = sum(t.pnl for t in tr if t.pnl > 0)
    gross_loss = sum(-t.pnl for t in tr if t.pnl < 0)
    fees = sum(t.fees for t in tr)
    pre_fee_gross_win = sum(max(t.pnl + t.fees, 0.0) for t in tr)
    peak, dd = start_equity, 0.0
    for _, eq in res.curve:
        peak = max(peak, eq)
        dd = max(dd, (peak - eq) / peak if peak > 0 else 0.0)
    streak = longest = 0
    for t in tr:
        streak = streak + 1 if t.r < 0 else 0
        longest = max(longest, streak)
    by_hour: dict[int, dict] = {}
    for t in tr:
        h = (t.ts_open // 3_600_000) % 24
        b = by_hour.setdefault(h, {"trades": 0, "net_r": 0.0})
        b["trades"] += 1
        b["net_r"] += t.r
    return {
        "trades": n,
        "win_rate": wins / n if n else 0.0,
        "avg_r": sum(t.r for t in tr) / n if n else 0.0,
        "net_r": sum(t.r for t in tr),
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else None,
        "net_pnl_after_fees": sum(t.pnl for t in tr),
        "fees_usd": fees,
        "fee_share_of_gross": fees / pre_fee_gross_win if pre_fee_gross_win > 0 else None,
        "max_drawdown_pct": dd * 100,
        "longest_loss_streak": longest,
        "end_equity": res.curve[-1][1] if res.curve else start_equity,
        "pnl_by_utc_hour": {h: {"trades": v["trades"], "net_r": round(v["net_r"], 2)}
                            for h, v in sorted(by_hour.items())},
        "gate_skips": res.skip_counts,
        "exit_reasons": _count(tr),
    }


def _count(trades: list[BTTrade]) -> dict:
    out: dict[str, int] = {}
    for t in trades:
        out[t.reason] = out.get(t.reason, 0) + 1
    return out


def print_report(tag: str, rep: dict) -> None:
    pf = rep["profit_factor"]
    fs = rep["fee_share_of_gross"]
    print(f"\n=== {tag} ===")
    print(f"trades {rep['trades']} | win {rep['win_rate'] * 100:.1f}% | avg {rep['avg_r']:+.2f}R"
          f" | net {rep['net_r']:+.1f}R")
    print(f"PF {'—' if pf is None else f'{pf:.2f}'}"
          f" | net pnl ${rep['net_pnl_after_fees']:+,.2f} | fees ${rep['fees_usd']:,.2f}"
          f" ({'—' if fs is None else f'{fs * 100:.0f}%'} of gross)")
    print(f"max DD {rep['max_drawdown_pct']:.1f}% | longest loss streak {rep['longest_loss_streak']}"
          f" | end equity ${rep['end_equity']:,.2f}")
    if rep["exit_reasons"]:
        print("exits: " + ", ".join(f"{k}={v}" for k, v in sorted(rep["exit_reasons"].items())))
    if rep["gate_skips"]:
        print("gate skips: " + ", ".join(f"{k}={v}" for k, v in sorted(rep["gate_skips"].items())))
    if rep["pnl_by_utc_hour"]:
        print("PnL by UTC hour (h: trades / net R):")
        cells = [f"{h:02d}: {v['trades']:>3}/{v['net_r']:>+6.1f}" for h, v in rep["pnl_by_utc_hour"].items()]
        for i in range(0, len(cells), 6):
            print("  " + "  ".join(cells[i:i + 6]))


def plot_curve(res: SimResult, path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    xs = [t / 86_400_000 for t, _ in res.curve]
    ys = [e for _, e in res.curve]
    plt.figure(figsize=(11, 4), facecolor="#0B0E11")
    ax = plt.gca()
    ax.set_facecolor("#11161C")
    ax.plot(xs, ys, color="#2EE6A6", linewidth=1.2)
    ax.tick_params(colors="#5C6873")
    for sp in ax.spines.values():
        sp.set_color("#1C2530")
    ax.set_title("FlowScalp backtest equity", color="#C9D4DF")
    plt.tight_layout()
    plt.savefig(path, dpi=120)
    plt.close()


# ============================================================ CLI flows
def _settings_from_args(a) -> Settings:
    over = dict(risk_pct=a.risk, tp_r=a.tp_r, sl_cap_pct=a.sl_cap,
                sweep_lookback=a.lookback, sweep_vol_mult=a.vol_mult,
                cvd_filter=not a.no_cvd, bias_filter=not a.no_bias,
                allow_taker_entry=a.taker_entry, time_stop_min=a.time_stop)
    if a.session:
        over["session_windows"] = tuple(w.strip() for w in a.session.split(",") if w.strip())
    if a.blackout is not None:
        over["blackout_windows"] = tuple(w.strip() for w in a.blackout.split(",") if w.strip())
    return Settings(**over)


def _run_one(tag, candles, s, deltas, a, write_files=False):
    res = simulate(candles, s, deltas_map=deltas, start_equity=a.start_equity,
                   maker_fee=a.fee_maker, taker_fee=a.fee_taker)
    rep = compute_report(res, a.start_equity)
    print_report(tag, rep)
    if write_files:
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        with open(a.out + ".json", "w") as f:
            json.dump(rep, f, indent=2)
        plot_curve(res, a.out + ".png")
        print(f"\nreport → {a.out}.json, equity curve → {a.out}.png")
    return res, rep


def _grid(candles, base: Settings, deltas, a) -> None:
    rows = []
    grid = list(itertools.product((1.4, 1.8, 2.2), (0.4, 0.5, 0.7), (20, 30, 60), (1.0, 1.7, 2.2)))
    print(f"\n=== GRID ({len(grid)} combos) ===")
    for i, (tp_r, sl_cap, lb, vm) in enumerate(grid, 1):
        s = base.model_copy(update={"tp_r": tp_r, "sl_cap_pct": sl_cap,
                                    "sweep_lookback": lb, "sweep_vol_mult": vm})
        res = simulate(candles, s, deltas_map=deltas, start_equity=a.start_equity,
                       maker_fee=a.fee_maker, taker_fee=a.fee_taker)
        rep = compute_report(res, a.start_equity)
        rows.append(((tp_r, sl_cap, lb, vm), rep))
        pf_s = "—" if rep["profit_factor"] is None else f"{rep['profit_factor']:.2f}"
        print(f"  [{i}/{len(grid)}] tp_r={tp_r} sl_cap={sl_cap} lb={lb} vol={vm}"
              f" → {rep['trades']}t PF {pf_s}", flush=True)
    eligible = [(p, r) for p, r in rows if r["trades"] >= 100 and r["profit_factor"] is not None]
    eligible.sort(key=lambda x: x[1]["profit_factor"], reverse=True)
    print("\ntop 10 by profit factor (≥100 trades):")
    print(f"{'tp_r':>5} {'sl_cap':>6} {'lb':>4} {'vol':>4} | {'trades':>6} {'win%':>6} {'PF':>5} {'netR':>7}")
    for (tp_r, sl_cap, lb, vm), r in eligible[:10]:
        print(f"{tp_r:>5} {sl_cap:>6} {lb:>4} {vm:>4} | {r['trades']:>6} {r['win_rate'] * 100:>5.1f}%"
              f" {r['profit_factor']:>5.2f} {r['net_r']:>+7.1f}")
    if not eligible:
        print("  (no combo reached 100 trades)")
    print("⚠ in-sample results — validate on the OOS split before trusting any of this.")


def _ablate(candles, base: Settings, deltas, a) -> None:
    configs = [
        ("A base structure", {"sweep_vol_mult": 1.0, "cvd_filter": False, "funding_filter": False}),
        ("B +volume", {"cvd_filter": False, "funding_filter": False}),
        ("C +volume+CVD", {"funding_filter": False}),
        ("D full stack", {}),
    ]
    print("\n=== ABLATION ===")
    if deltas is None:
        print("  (no --trades data: CVD gate is skipped → B≈C expected)")
    rows = []
    for tag, over in configs:
        s = base.model_copy(update=over)
        res = simulate(candles, s, deltas_map=deltas, start_equity=a.start_equity,
                       maker_fee=a.fee_maker, taker_fee=a.fee_taker)
        rep = compute_report(res, a.start_equity)
        rows.append((tag, rep))
    print(f"{'config':<18} {'trades':>6} {'win%':>6} {'PF':>6} {'netR':>8} {'fees$':>9}")
    for tag, r in rows:
        pf = "—" if r["profit_factor"] is None else f"{r['profit_factor']:.2f}"
        print(f"{tag:<18} {r['trades']:>6} {r['win_rate'] * 100:>5.1f}% {pf:>6}"
              f" {r['net_r']:>+8.1f} {r['fees_usd']:>9,.0f}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="FlowScalp backtester")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--trades", help="Binance Vision aggTrades dir/file for CVD deltas")
    ap.add_argument("--risk", type=float, default=0.5)
    ap.add_argument("--tp-r", type=float, default=1.8)
    ap.add_argument("--sl-cap", type=float, default=0.5)
    ap.add_argument("--lookback", type=int, default=30)
    ap.add_argument("--vol-mult", type=float, default=1.7)
    ap.add_argument("--time-stop", type=int, default=15)
    ap.add_argument("--no-cvd", action="store_true")
    ap.add_argument("--no-bias", action="store_true")
    ap.add_argument("--taker-entry", action="store_true")
    ap.add_argument("--session", help='override session windows, e.g. "07:00-20:00"')
    ap.add_argument("--blackout", help='override blackout windows ("" = none)')
    ap.add_argument("--oos", type=float, help="hold out this fraction (e.g. 0.3) chronologically")
    ap.add_argument("--grid", action="store_true")
    ap.add_argument("--ablate", action="store_true")
    ap.add_argument("--start-equity", type=float, default=10_000.0)
    ap.add_argument("--fee-maker", type=float, default=FALLBACK_MAKER_FEE)
    ap.add_argument("--fee-taker", type=float, default=FALLBACK_TAKER_FEE)
    ap.add_argument("--out", default="data/backtest_report")
    a = ap.parse_args(argv)

    candles = load_candles(a.csv)
    print(f"loaded {len(candles)} 1m candles"
          f" ({(candles[-1].ts - candles[0].ts) / 86_400_000:.1f} days)" if candles else "no candles")
    deltas = load_agg_trades(a.trades) if a.trades else None
    s = _settings_from_args(a)
    if deltas is None and s.cvd_filter:
        print("⚠ no --trades input: cvd_filter runs UNVALIDATED (gate skipped in backtest)."
              " Supply Binance aggTrades to validate CVD.")

    if a.oos:
        cut = int(len(candles) * (1 - a.oos))
        is_c, oos_c = candles[:cut], candles[cut:]
        _run_one(f"IN-SAMPLE ({1 - a.oos:.0%})", is_c, s, deltas, a, write_files=True)
        if a.grid:
            _grid(is_c, s, deltas, a)
        if a.ablate:
            _ablate(is_c, s, deltas, a)
        _run_one(f"OUT-OF-SAMPLE ({a.oos:.0%})", oos_c, s, deltas, a)
    else:
        _run_one("FULL PERIOD", candles, s, deltas, a, write_files=True)
        if a.grid:
            _grid(candles, s, deltas, a)
        if a.ablate:
            _ablate(candles, s, deltas, a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
