"""Backtest data: Hyperliquid candleSnapshot downloader + CSV/aggTrades loaders.

CSV formats accepted by load_candles():
- native:  header `ts,open,high,low,close,volume` (ts = bar open, unix ms)
- Binance kline dumps (12 columns, with or without header, ms or µs timestamps)

Delta data for CVD backtests: load_agg_trades() accepts a directory (or single
file) of Binance Vision BTCUSDT aggTrades .csv/.zip files and aggregates
`is_buyer_maker` into a per-1m delta proxy: taker-buy qty − taker-sell qty.

Usage:
  python -m backtest.data --start 2026-03-01 --end 2026-06-01 --out data/btc_1m.csv
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import pandas as pd

from core.strategy import Candle

ONE_MIN_MS = 60_000
PAGE_CANDLES = 5_000  # Hyperliquid candleSnapshot cap per request

HL_INFO_URL = "https://api.hyperliquid.xyz/info"


def download_candles(start_ms: int, end_ms: int, interval: str = "1m",
                     coin: str = "BTC", url: str = HL_INFO_URL) -> list[Candle]:
    """Paginate candleSnapshot (~5000/req) over [start_ms, end_ms]."""
    step = PAGE_CANDLES * ONE_MIN_MS * (15 if interval == "15m" else 1)
    out: list[Candle] = []
    cur = start_ms
    with httpx.Client(timeout=30) as client:
        while cur < end_ms:
            req = {"type": "candleSnapshot",
                   "req": {"coin": coin, "interval": interval,
                           "startTime": cur, "endTime": min(cur + step, end_ms)}}
            r = client.post(url, json=req)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                cur += step
                continue
            for d in batch:
                ts = int(d["t"])
                if out and ts <= out[-1].ts:
                    continue
                out.append(Candle(ts, float(d["o"]), float(d["h"]), float(d["l"]),
                                  float(d["c"]), float(d["v"])))
            cur = out[-1].ts + ONE_MIN_MS
            time.sleep(0.25)  # stay polite to the public endpoint
    return out


def save_candles(candles: list[Candle], path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    pd.DataFrame([c.__dict__ for c in candles])[
        ["ts", "open", "high", "low", "close", "volume"]].to_csv(path, index=False)


def _norm_ts(v: float) -> int:
    v = int(v)
    return v // 1000 if v > 10 ** 15 else v  # Binance µs dumps → ms


def load_candles(path: str) -> list[Candle]:
    df = pd.read_csv(path, header=None if _headerless(path) else 0)
    cols = [str(c).lower() for c in df.columns]
    if "ts" in cols:  # native format
        df.columns = cols
        sel = df[["ts", "open", "high", "low", "close", "volume"]]
    elif "open_time" in cols:  # Binance with header
        df.columns = cols
        sel = df[["open_time", "open", "high", "low", "close", "volume"]]
    else:  # Binance headerless kline dump: first 6 cols
        sel = df.iloc[:, :6]
    sel.columns = ["ts", "open", "high", "low", "close", "volume"]
    out = [Candle(_norm_ts(r.ts), float(r.open), float(r.high), float(r.low),
                  float(r.close), float(r.volume))
           for r in sel.itertuples(index=False)]
    out.sort(key=lambda c: c.ts)
    return out


def _headerless(path: str) -> bool:
    first = pd.read_csv(path, nrows=1, header=None).iloc[0, 0]
    try:
        float(first)
        return True
    except (TypeError, ValueError):
        return False


def load_agg_trades(path: str) -> dict[int, float]:
    """Per-1m delta map from Binance aggTrades file(s):
    delta = Σ qty(taker buy, is_buyer_maker=False) − Σ qty(taker sell)."""
    files = []
    if os.path.isdir(path):
        files = sorted(os.path.join(path, f) for f in os.listdir(path)
                       if f.endswith((".csv", ".zip")))
    else:
        files = [path]
    deltas: dict[int, float] = {}
    for f in files:
        df = pd.read_csv(f, header=None if _agg_headerless(f) else 0)
        if df.shape[1] < 7:
            raise ValueError(f"{f}: expected ≥7 aggTrades columns")
        df = df.iloc[:, :7]
        df.columns = ["agg_id", "price", "qty", "first_id", "last_id", "time", "is_buyer_maker"]
        ts = df["time"].astype("int64").map(_norm_ts)
        bucket = ts // ONE_MIN_MS * ONE_MIN_MS
        maker = df["is_buyer_maker"].astype(str).str.lower().isin(("true", "1"))
        signed = df["qty"].astype(float).where(~maker, -df["qty"].astype(float))
        for b, v in signed.groupby(bucket).sum().items():
            deltas[int(b)] = deltas.get(int(b), 0.0) + float(v)
    return deltas


def _agg_headerless(path: str) -> bool:
    first = pd.read_csv(path, nrows=1, header=None).iloc[0, 0]
    try:
        float(first)
        return True
    except (TypeError, ValueError):
        return False


def _parse_date(s: str) -> int:
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="download Hyperliquid 1m candles to CSV")
    ap.add_argument("--start", required=True, help="ISO date, e.g. 2026-03-01")
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default="data/btc_1m.csv")
    ap.add_argument("--interval", default="1m")
    args = ap.parse_args(argv)
    candles = download_candles(_parse_date(args.start), _parse_date(args.end), args.interval)
    save_candles(candles, args.out)
    print(f"saved {len(candles)} candles → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
