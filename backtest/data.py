"""Backtest data: candle downloaders (Hyperliquid + Binance archive) + loaders.

Sources:
- Hyperliquid candleSnapshot (`--source hl`): ONLY keeps ≈3.5 days of 1m
  candles (5000 bars) — fine for smoke runs, useless for tuning.
- Binance Vision public archive (`--source binance`): months of UM-futures
  1m klines via monthly zips (daily zips fill the current month). Prices are
  not Hyperliquid's, but structure/volatility match closely enough for
  filter/session decisions.

CSV formats accepted by load_candles():
- native:  header `ts,open,high,low,close,volume` (ts = bar open, unix ms)
- Binance kline dumps (12 columns, with or without header, ms or µs timestamps)

Delta data for CVD backtests: load_agg_trades() accepts a directory (or single
file) of Binance Vision BTCUSDT aggTrades .csv/.zip files and aggregates
`is_buyer_maker` into a per-1m delta proxy: taker-buy qty − taker-sell qty.

Usage:
  python -m backtest.data --source binance --coin SOL \
      --start 2026-03-01 --end 2026-06-11 --out data/sol_1m.csv
"""
from __future__ import annotations

import argparse
import calendar
import io
import os
import sys
import time
from datetime import date, datetime, timezone
from typing import Callable, Optional

import httpx
import pandas as pd

from core.strategy import Candle

ONE_MIN_MS = 60_000
PAGE_CANDLES = 5_000  # Hyperliquid candleSnapshot cap per request

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
BINANCE_UM_URL = "https://data.binance.vision/data/futures/um"


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


def _parse_kline_zip(content: bytes) -> list[Candle]:
    """Binance Vision kline zip → Candles. Header rows (if any) are dropped;
    µs timestamps (2025+ dumps) are normalized to ms by _norm_ts."""
    df = pd.read_csv(io.BytesIO(content), compression="zip", header=None)
    df[0] = pd.to_numeric(df[0], errors="coerce")
    df = df[df[0].notna()]
    return [Candle(_norm_ts(r[0]), float(r[1]), float(r[2]), float(r[3]),
                   float(r[4]), float(r[5]))
            for r in df.itertuples(index=False)]


def download_binance_klines(start_ms: int, end_ms: int, coin: str = "BTC",
                            url: str = BINANCE_UM_URL, interval: str = "1m",
                            fetch: Optional[Callable[[str], Optional[bytes]]] = None) -> list[Candle]:
    """UM-futures klines from the Binance Vision archive. Whole months come
    as one monthly zip; months without a monthly file yet (the current one)
    fall back to daily zips. Missing files (404) are skipped."""
    symbol = f"{coin.upper()}USDT"
    if fetch is None:
        client = httpx.Client(timeout=60, follow_redirects=True)

        def fetch(u: str) -> Optional[bytes]:
            r = client.get(u)
            return r.content if r.status_code == 200 else None

    s_date = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date()
    e_date = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).date()
    out: dict[int, Candle] = {}
    cur = s_date.replace(day=1)
    while cur <= e_date:
        content = fetch(f"{url}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{cur:%Y-%m}.zip")
        if content is not None:
            for c in _parse_kline_zip(content):
                out[c.ts] = c
            print(f"  {symbol} {interval} {cur:%Y-%m} (zip bulanan) ✓", flush=True)
        else:
            got = 0
            for day in range(1, calendar.monthrange(cur.year, cur.month)[1] + 1):
                d = cur.replace(day=day)
                if d > e_date:
                    break
                if d < s_date:
                    continue
                dc = fetch(f"{url}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{d:%Y-%m-%d}.zip")
                if dc is None:
                    continue
                for c in _parse_kline_zip(dc):
                    out[c.ts] = c
                got += 1
            print(f"  {symbol} {interval} {cur:%Y-%m} ({got} zip harian) ✓", flush=True)
        cur = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
    return [out[k] for k in sorted(out) if start_ms <= k < end_ms]


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
    ap = argparse.ArgumentParser(description="download 1m candles to CSV")
    ap.add_argument("--start", required=True, help="ISO date, e.g. 2026-03-01")
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default="data/btc_1m.csv")
    ap.add_argument("--interval", default="1m")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--source", choices=("hl", "binance"), default="hl",
                    help="hl: Hyperliquid candleSnapshot (hanya ±3.5 hari terakhir);"
                         " binance: arsip publik Binance (berbulan-bulan)")
    args = ap.parse_args(argv)
    if args.source == "binance":
        candles = download_binance_klines(_parse_date(args.start), _parse_date(args.end),
                                          coin=args.coin.upper(), interval=args.interval)
    else:
        candles = download_candles(_parse_date(args.start), _parse_date(args.end), args.interval,
                                   coin=args.coin.upper())
    save_candles(candles, args.out)
    days = (candles[-1].ts - candles[0].ts) / 86_400_000 if candles else 0.0
    print(f"saved {len(candles)} candles ({days:.1f} days) → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
