"""Synthetic BTC 1m candle generator for backtester smoke runs.

Regime-switching random walk with planted liquidity-sweep episodes (range →
stop-run wick below the range low on a volume spike → reclaim), so the
strategy has something structurally honest to chew on. This is NOT a market
model — use real candleSnapshot data for any tuning decision.

  python -m backtest.synth --days 60 --out data/synth_btc_1m.csv [--seed 7]
"""
from __future__ import annotations

import argparse
import math
import random
import sys

from backtest.data import save_candles
from core.strategy import Candle

ONE_MIN_MS = 60_000


def generate_candles(days: int = 60, seed: int = 7, start_px: float = 100_000.0,
                     start_ts: int = 1_760_000_000_000) -> list[Candle]:
    rng = random.Random(seed)
    n = days * 1440
    px = start_px
    out: list[Candle] = []
    vol_regime = 1.0
    sweep_cooldown = 0
    drift = 0.0
    for i in range(n):
        ts = start_ts + i * ONE_MIN_MS
        if i % 240 == 0:  # vol regime shifts every 4h
            vol_regime = rng.choice((0.5, 1.0, 1.0, 1.6, 2.4))
        if i % 360 == 0:
            drift = rng.uniform(-0.3, 0.3)  # slow trend component, $/bar
        # ~7bps base 1m vol → ATR ≈ 5–25bps across regimes, straddling the
        # strategy's default 8–60bps band (dead-tape regimes get skipped)
        sigma = px * 0.0007 * vol_regime

        o = px
        ret = rng.gauss(drift, sigma)
        c = max(1000.0, o + ret)
        hi = max(o, c) + abs(rng.gauss(0, sigma * 0.6))
        lo = min(o, c) - abs(rng.gauss(0, sigma * 0.6))
        v = abs(rng.gauss(8, 3)) * vol_regime

        if sweep_cooldown > 0:
            sweep_cooldown -= 1
        elif i > 120 and rng.random() < 0.006:  # ~8-9 sweep episodes/day
            lookback = out[-45:]
            if rng.random() < 0.5:
                level = min(b.low for b in lookback)
                lo = min(lo, level * (1 - rng.uniform(0.0008, 0.002)))
                c = max(c, level * (1 + rng.uniform(0.0002, 0.001)))
                hi = max(hi, c)
            else:
                level = max(b.high for b in lookback)
                hi = max(hi, level * (1 + rng.uniform(0.0008, 0.002)))
                c = min(c, level * (1 - rng.uniform(0.0002, 0.001)))
                lo = min(lo, c)
            v *= rng.uniform(2.0, 3.5)
            sweep_cooldown = 60
            # ~55% of sweeps actually reverse for a while; the rest keep going
            drift = (math.copysign(0.6, c - o) if rng.random() < 0.55
                     else math.copysign(0.6, o - c)) * rng.uniform(0.5, 1.5)

        out.append(Candle(ts, o, hi, lo, c, v))
        px = c
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="data/synth_btc_1m.csv")
    a = ap.parse_args(argv)
    candles = generate_candles(a.days, a.seed)
    save_candles(candles, a.out)
    print(f"saved {len(candles)} synthetic 1m candles ({a.days}d) → {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
