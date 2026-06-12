"""Backtester: loaders round-trip, sim mechanics, determinism, report shape."""
import pandas as pd
import pytest

from backtest.data import load_agg_trades, load_candles, save_candles
from backtest.run import compute_report, simulate
from backtest.synth import generate_candles
from config import Settings
from core.strategy import Candle

S_OPEN = Settings(session_windows=("00:00-24:00",), blackout_windows=(),
                  sweep_vol_mult=1.7, cvd_filter=True, bias_filter=True)


def test_csv_roundtrip_native(tmp_path):
    candles = generate_candles(days=1, seed=1)
    p = str(tmp_path / "c.csv")
    save_candles(candles, p)
    back = load_candles(p)
    assert len(back) == len(candles)
    for a, b in ((back[0], candles[0]), (back[-1], candles[-1])):
        assert a.ts == b.ts
        for f in ("open", "high", "low", "close", "volume"):
            assert getattr(a, f) == pytest.approx(getattr(b, f), rel=1e-12)


def test_binance_kline_format_headerless_and_microseconds(tmp_path):
    rows = [[1760000000000000 + i * 60_000_000, 100, 101, 99, 100.5, 5.0,
             0, 0, 10, 2.5, 0, 0] for i in range(3)]
    p = str(tmp_path / "b.csv")
    pd.DataFrame(rows).to_csv(p, index=False, header=False)
    c = load_candles(p)
    assert len(c) == 3
    assert c[0].ts == 1760000000000  # µs → ms
    assert c[0].close == 100.5 and c[1].ts - c[0].ts == 60_000


def _kline_zip(start_ts_ms: int, n: int, *, micros: bool = False, header: bool = False) -> bytes:
    import io
    import zipfile
    mult = 1000 if micros else 1
    lines = []
    if header:
        lines.append("open_time,open,high,low,close,volume,close_time,qv,n,tb,tq,ig")
    for i in range(n):
        ts = (start_ts_ms + i * 60_000) * mult
        lines.append(f"{ts},100,101,99,100.5,5.0,0,0,10,2.5,0,0")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("k.csv", "\n".join(lines))
    return buf.getvalue()


def test_binance_downloader_monthly_plus_daily_fallback():
    """May = monthly zip (with header, µs). June = no monthly file yet →
    daily zips for the requested days only; 404 days are skipped. Output is
    sorted, deduped and trimmed to [start, end)."""
    from backtest.data import download_binance_klines

    may_1 = 1_777_593_600_000        # 2026-05-01 00:00 UTC
    jun_1 = 1_780_272_000_000        # 2026-06-01 00:00 UTC
    start = jun_1 - 2 * 86_400_000   # 2026-05-30
    end = jun_1 + 2 * 86_400_000     # exclusive → 2026-06-03

    urls = []

    def fetch(u: str):
        urls.append(u)
        if "monthly" in u and "2026-05" in u:
            return _kline_zip(may_1, 44_640, micros=True, header=True)  # full May
        if "daily" in u and "2026-06-01" in u:
            return _kline_zip(jun_1, 1440)
        if "daily" in u and "2026-06-02" in u:
            return _kline_zip(jun_1 + 86_400_000, 1440)
        return None                   # June monthly + other days → 404

    c = download_binance_klines(start, end, coin="SOL", fetch=fetch)
    assert any("SOLUSDT-1m-2026-05.zip" in u for u in urls)
    assert not any("daily" in u and "2026-05" in u for u in urls)   # May covered monthly
    assert c[0].ts == start                                          # trimmed to start
    assert c[-1].ts == end - 60_000                                  # exclusive end
    assert len(c) == 4 * 1440                                        # 4 full days
    assert all(b.ts - a.ts == 60_000 for a, b in zip(c, c[1:]))      # gapless, sorted
    assert c[0].close == 100.5


def test_aggtrades_delta(tmp_path):
    t0 = 1_760_000_000_000 // 60_000 * 60_000
    rows = [
        [1, 100000.0, 1.5, 1, 1, t0 + 1_000, False],   # taker buy  +1.5
        [2, 100000.0, 0.5, 2, 2, t0 + 2_000, True],    # taker sell −0.5
        [3, 100000.0, 2.0, 3, 3, t0 + 61_000, True],   # next minute −2.0
    ]
    p = str(tmp_path / "agg.csv")
    pd.DataFrame(rows).to_csv(p, index=False, header=False)
    d = load_agg_trades(p)
    assert d[t0] == pytest.approx(1.0)
    assert d[t0 + 60_000] == pytest.approx(-2.0)


def test_simulate_produces_trades_and_consistent_math():
    candles = generate_candles(days=6, seed=7)
    res = simulate(candles, S_OPEN, start_equity=10_000.0)
    assert len(res.trades) >= 3, "synthetic sweeps should produce trades"
    for t in res.trades:
        assert t.ts_close > t.ts_open
        assert t.reason in ("tp", "sl", "time_stop", "eod")
        d = 1 if t.side == "long" else -1
        assert t.pnl == pytest.approx(d * (t.exit_px - t.entry_px) * t.size_btc - t.fees, rel=1e-9)
        if t.entry_risk_usd > 0:
            assert t.r == pytest.approx(t.pnl / t.entry_risk_usd, rel=1e-9)
        if t.reason == "sl":
            assert -1.3 < t.r < 0  # ≈ −1R plus fees
        if t.reason == "tp":
            assert 1.4 < t.r < 1.9  # ≈ +1.8R minus fees
    # equity curve consistent with summed pnl
    assert res.curve[-1][1] == pytest.approx(10_000.0 + sum(t.pnl for t in res.trades), rel=1e-9)


def test_simulate_deterministic():
    candles = generate_candles(days=3, seed=11)
    r1 = simulate(candles, S_OPEN)
    r2 = simulate(candles, S_OPEN)
    assert [(t.ts_open, t.r) for t in r1.trades] == [(t.ts_open, t.r) for t in r2.trades]


def test_every_sim_entry_was_traded_through():
    """With allow_taker_entry off (default), every fill must come from a bar
    that traded strictly THROUGH the maker price — a touch is not a fill."""
    candles = generate_candles(days=6, seed=7)
    res = simulate(candles, S_OPEN)
    assert res.trades
    by_ts = {c.ts: c for c in candles}
    for t in res.trades:
        fill_bar = by_ts[t.ts_open - 60_000]
        if t.side == "long":
            assert fill_bar.low < t.entry_px
        else:
            assert fill_bar.high > t.entry_px


def test_report_shape_and_oos_split():
    candles = generate_candles(days=6, seed=7)
    res = simulate(candles, S_OPEN)
    rep = compute_report(res, 10_000.0)
    for key in ("trades", "win_rate", "avg_r", "net_r", "profit_factor",
                "net_pnl_after_fees", "fees_usd", "fee_share_of_gross",
                "max_drawdown_pct", "longest_loss_streak", "pnl_by_utc_hour",
                "gate_skips", "exit_reasons"):
        assert key in rep
    assert 0 <= rep["win_rate"] <= 1
    assert rep["max_drawdown_pct"] >= 0
    cut = int(len(candles) * 0.7)
    r_is = simulate(candles[:cut], S_OPEN)
    r_oos = simulate(candles[cut:], S_OPEN)
    assert compute_report(r_is, 10_000.0)["trades"] + compute_report(r_oos, 10_000.0)["trades"] > 0


def test_cvd_filter_with_deltas_changes_behavior():
    candles = generate_candles(days=6, seed=7)
    s = S_OPEN
    # all-negative deltas → every long fails CVD absorption; shorts all pass
    neg = {c.ts: -1.0 for c in candles}
    res_neg = simulate(candles, s, deltas_map=neg)
    assert all(t.side == "short" for t in res_neg.trades)
    pos = {c.ts: +1.0 for c in candles}
    res_pos = simulate(candles, s, deltas_map=pos)
    assert all(t.side == "long" for t in res_pos.trades)
    assert res_neg.trades or res_pos.trades  # the gate filtered, not silenced, the strategy
