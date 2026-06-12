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


def test_resample_15m_ohlcv_buckets():
    from backtest.run import resample_candles
    base = 1_800_000_000_000 // 900_000 * 900_000
    c = [Candle(base + i * 60_000, float(i), i + 10.0, i - 10.0, i + 1.0, 2.0)
         for i in range(31)]
    r = resample_candles(c, 900_000)
    assert len(r) == 3                                   # 15 + 15 + 1 bars
    assert r[0].ts == base and r[1].ts == base + 900_000
    assert r[0].open == 0.0 and r[0].close == 15.0       # first open, last close
    assert r[0].high == 24.0 and r[0].low == -10.0
    assert r[0].volume == pytest.approx(30.0)
    assert r[2].volume == pytest.approx(2.0)             # trailing partial bucket kept


def test_tf15m_main_runs_and_prints_ringkas(tmp_path, capsys):
    from backtest.data import save_candles as save
    from backtest.run import main as bt_main
    p = str(tmp_path / "c.csv")
    save(generate_candles(days=40, seed=5), p)
    rc = bt_main(["--csv", p, "--tf", "15m", "--oos", "0.3", "--vol-mult", "1.0",
                  "--no-cvd", "--time-stop", "240", "--sl-cap", "1.2",
                  "--sl-buffer", "0.1", "--atr-floor", "5", "--atr-ceil", "400",
                  "--session", "00:00-24:00", "--blackout", "",
                  "--tag", "T15", "--out", str(tmp_path / "r")])
    assert rc == 0
    out = capsys.readouterr().out
    assert "bar 15 menit" in out
    assert "RINGKAS | T15 | IS" in out and "OOS" in out


def _bar4h(i: int, o: float, h: float, lo: float, c: float, base_ts: int = 1_800_000_000_000) -> Candle:
    t0 = base_ts // 14_400_000 * 14_400_000
    return Candle(t0 + i * 14_400_000, o, h, lo, c, 100.0)


def test_trend_breakout_entry_next_open_and_chandelier_exit():
    """Flat range → breakout close → entry at the NEXT bar's open (no
    lookahead), trailing stop ratchets with highest close, exits on touch."""
    from backtest.trend import simulate_trend
    bars = [_bar4h(i, 100.0, 101.0, 99.0, 100.0) for i in range(60)]
    bars.append(_bar4h(60, 100.0, 106.0, 100.0, 105.0))     # breakout close > 101
    bars.append(_bar4h(61, 105.0, 112.0, 104.0, 111.0))     # fill at open 105(+slip)
    bars.append(_bar4h(62, 111.0, 118.0, 110.0, 117.0))     # trend runs, stop ratchets
    bars.append(_bar4h(63, 117.0, 117.5, 90.0, 92.0))       # collapse through the stop
    res = simulate_trend(bars, lookback=42, atr_k=3.0, funding_8h_pct=0.0)
    assert len(res.trades) == 1
    t = res.trades[0]
    assert t.entry_px == pytest.approx(105.0 * 1.0001)      # next bar open + slip
    assert t.ts_open == bars[61].ts
    assert t.reason == "trail"
    # the chandelier ratcheted ABOVE entry while the trend ran, so the collapse
    # exits at the locked-in stop — in profit, well below the peak close
    assert t.entry_px < t.exit_px < 117.0
    assert t.r > 0
    assert t.pnl == pytest.approx((t.exit_px - t.entry_px) * t.size_btc - t.fees, rel=1e-9)


def test_trend_no_trades_on_flat_tape_and_deterministic():
    from backtest.trend import simulate_trend
    flat = [_bar4h(i, 100.0, 101.0, 99.0, 100.0) for i in range(200)]
    assert simulate_trend(flat).trades == []
    up = [_bar4h(i, 100.0 + i, 101.5 + i, 99.5 + i, 101.0 + i) for i in range(120)]
    r1, r2 = simulate_trend(up), simulate_trend(up)
    assert [(t.ts_open, t.r) for t in r1.trades] == [(t.ts_open, t.r) for t in r2.trades]
    assert r1.trades, "a clean uptrend must produce at least one trade"
    assert all(t.reason in ("trail", "eod") for t in r1.trades)


def test_trend_funding_costs_reduce_pnl():
    from backtest.trend import simulate_trend
    up = [_bar4h(i, 100.0 + i, 101.5 + i, 99.5 + i, 101.0 + i) for i in range(120)]
    no_funding = simulate_trend(up, funding_8h_pct=0.0)
    with_funding = simulate_trend(up, funding_8h_pct=0.05)
    assert sum(t.pnl for t in with_funding.trades) < sum(t.pnl for t in no_funding.trades)


def test_trend_main_smoke_and_ringkas(tmp_path, capsys):
    from backtest.data import save_candles as save
    from backtest.trend import main as trend_main
    save(generate_candles(days=100, seed=7), str(tmp_path / "c.csv"))   # 1m → auto-resample
    rc = trend_main(["--csv", str(tmp_path / "c.csv"), "--oos", "0.3",
                     "--lookback", "28", "--atr-k", "2.5", "--tag", "T4H",
                     "--out", str(tmp_path / "r")])
    assert rc == 0
    out = capsys.readouterr().out
    assert "@ 4h" in out and "RINGKAS | T4H | IS" in out


def test_binance_downloader_interval_in_urls():
    from backtest.data import download_binance_klines
    urls = []

    def fetch(u: str):
        urls.append(u)
        return None
    download_binance_klines(1_777_593_600_000, 1_777_593_600_000 + 86_400_000,
                            coin="ETH", interval="4h", fetch=fetch)
    assert urls and all("/4h/" in u and "ETHUSDT-4h-" in u for u in urls)


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
