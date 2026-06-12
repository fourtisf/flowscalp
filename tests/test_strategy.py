import pytest

from core.strategy import Candle, evaluate, evaluate_ex
from tests.synth import (flat_1m, flat_15m, trend_15m, settings,
                         long_sweep_same_bar, long_sweep_next_bar, short_sweep_same_bar)

C15 = flat_15m()


def test_long_fires_same_bar_sweep_reclaim():
    c = long_sweep_same_bar()
    sig = evaluate(c, C15, None, settings())
    assert sig is not None and sig.side == "long"
    assert sig.entry_px == 99_960.0
    assert sig.sl_px == pytest.approx(99_845.0 * (1 - 0.0005))
    assert sig.tp_px == pytest.approx(sig.entry_px + 1.8 * (sig.entry_px - sig.sl_px))
    assert sig.swept_level == 99_900
    assert "sweep+reclaim" in sig.reason


def test_long_fires_next_bar_reclaim():
    c = long_sweep_next_bar()
    sig = evaluate(c, C15, None, settings())
    assert sig is not None and sig.side == "long"
    # SL hangs off the sweep bar's wick (prev.low), not the reclaim bar
    assert sig.sl_px == pytest.approx(99_820.0 * (1 - 0.0005))


def test_short_mirror_fires():
    c = short_sweep_same_bar()
    sig = evaluate(c, C15, None, settings())
    assert sig is not None and sig.side == "short"
    assert sig.sl_px == pytest.approx(100_155.0 * (1 + 0.0005))
    assert sig.tp_px == pytest.approx(sig.entry_px - 1.8 * (sig.sl_px - sig.entry_px))
    assert sig.swept_level == 100_100


def test_no_signal_on_flat_tape():
    assert evaluate(flat_1m(), C15, None, settings()) is None


def test_bias_off_skips_bias_frame_warmup():
    """With bias_filter off, a short bias-frame history must NOT block
    evaluation; with it on, the 210-bar EMA200 warm-up applies (fail-closed)."""
    c = long_sweep_same_bar(vol_sweep=200.0)
    short15 = flat_15m(n=5)
    assert evaluate_ex(c, short15, None, settings(), None).signal is not None
    assert evaluate_ex(c, short15, None, settings(bias_filter=True), None).signal is None


def test_skip_when_sl_distance_exceeds_cap():
    c = long_sweep_same_bar(sweep_low=99_300.0)  # 0.7% structural SL > 0.5% cap
    assert evaluate(c, C15, None, settings()) is None
    assert evaluate(c, C15, None, settings(sl_cap_pct=1.0)) is not None


def test_skip_atr_below_floor():
    c = flat_1m(rng=4.0)
    t = c[-1].ts
    c[-1] = Candle(t, 99_999.0, 100_001.0, 99_945.0, 100_000.0, 100.0)  # sweep of 99,998
    assert evaluate(c, C15, None, settings()) is None
    # identical structure fires once the floor is out of the way (ATR ~0.8bps)
    assert evaluate(c, C15, None, settings(atr_floor_bps=0.5)) is not None


def test_skip_atr_above_ceiling():
    c = flat_1m(rng=700.0)  # ATR 70bps > 60 ceiling
    t = c[-1].ts
    c[-1] = Candle(t, 99_700.0, 99_750.0, 99_595.0, 99_700.0, 100.0)  # sweep of 99,650
    assert evaluate(c, C15, None, settings()) is None
    assert evaluate(c, C15, None, settings(atr_ceil_bps=100.0)) is not None


def test_bias_blocks_short_in_uptrend_and_neutral_allows_both():
    up = trend_15m()  # px > EMA200 * 1.001 → bias long
    assert evaluate(short_sweep_same_bar(), up, None, settings(bias_filter=True)) is None
    assert evaluate(long_sweep_same_bar(), up, None, settings(bias_filter=True)) is not None
    # flat 15m → inside neutral band → "both": short fires
    assert evaluate(short_sweep_same_bar(), C15, None, settings(bias_filter=True)) is not None


def test_volume_gate():
    s_on = settings(sweep_vol_mult=1.7)
    assert evaluate(long_sweep_same_bar(vol_sweep=200.0), C15, None, s_on) is not None  # 2.0x SMA20
    res = evaluate_ex(long_sweep_same_bar(vol_sweep=120.0), C15, None, s_on)            # 1.2x SMA20
    assert res.signal is None and res.skip_gate == "volume"
    s_off = settings(sweep_vol_mult=1.0)
    assert evaluate(long_sweep_same_bar(vol_sweep=120.0), C15, None, s_off) is not None


def test_volume_gate_uses_sweep_bar_in_next_bar_case():
    s_on = settings(sweep_vol_mult=1.7)
    c = long_sweep_next_bar()
    c[-2] = Candle(c[-2].ts, c[-2].open, c[-2].high, c[-2].low, c[-2].close, 200.0)  # sweep bar 2x
    assert evaluate(c, C15, None, s_on) is not None
    c[-2] = Candle(c[-2].ts, c[-2].open, c[-2].high, c[-2].low, c[-2].close, 120.0)
    assert evaluate_ex(c, C15, None, s_on).skip_gate == "volume"


def _cvd_fixture():
    c = long_sweep_same_bar(sweep_low=99_825.0, unique_dip=True)  # unique swing low at idx n-11
    n = len(c)
    return c, n, n - 11  # swing bar index


def test_cvd_divergence_pass_and_fail():
    s_on = settings(cvd_filter=True)
    c, n, swing_idx = _cvd_fixture()
    deltas = [0.0] * n
    for i in range(swing_idx + 1, n):
        deltas[i] = +5.0   # price lower low, CVD higher → absorption → pass
    assert evaluate(c, C15, deltas, s_on) is not None
    for i in range(swing_idx + 1, n):
        deltas[i] = -5.0   # CVD followed price down → no absorption → skip
    res = evaluate_ex(c, C15, deltas, s_on)
    assert res.signal is None and res.skip_gate == "cvd"


def test_cvd_warmup_fails_closed_but_missing_data_skips_gate():
    s_on = settings(cvd_filter=True)
    c, n, swing_idx = _cvd_fixture()
    deltas = [5.0] * n
    deltas[-3] = None      # warm-up bar inside the window → fail closed
    res = evaluate_ex(c, C15, deltas, s_on)
    assert res.signal is None and res.skip_gate == "cvd_warmup"
    # deltas=None entirely (backtest without trades input) → gate skipped
    assert evaluate(c, C15, None, s_on) is not None


def _session_settings(**over):
    return settings(session_windows=("07:00-20:00",),
                    blackout_windows=("13:15-13:45", "18:45-19:15"), **over)


def test_session_gate_open_close_blackout():
    s = _session_settings()
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T06:59"), C15, None, s) is None
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T07:01"), C15, None, s) is not None
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T13:20"), C15, None, s) is None
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T19:59"), C15, None, s) is not None
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T20:00"), C15, None, s) is None


def test_session_window_crossing_midnight():
    s = settings(session_windows=("22:00-02:00",))
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T23:30"), C15, None, s) is not None
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-06T01:30"), C15, None, s) is not None
    assert evaluate(long_sweep_same_bar(close_utc="2026-01-05T12:00"), C15, None, s) is None


def test_funding_skew_gate():
    s_on = settings(funding_filter=True)
    c = long_sweep_same_bar()
    res = evaluate_ex(c, C15, None, s_on, funding_hourly=0.0002)  # +0.02%/h > +0.005% → no longs
    assert res.signal is None and res.skip_gate == "funding"
    assert evaluate(c, C15, None, s_on, funding_hourly=0.00003) is not None
    assert evaluate(c, C15, None, s_on, funding_hourly=None) is not None  # unavailable → fail-open
    sh = short_sweep_same_bar()
    assert evaluate_ex(sh, C15, None, s_on, funding_hourly=-0.0002).skip_gate == "funding"
    assert evaluate(sh, C15, None, s_on, funding_hourly=0.0002) is not None


def test_insufficient_history_returns_none():
    assert evaluate(long_sweep_same_bar()[-40:], C15, None, settings()) is None
    # short bias-frame history only blocks when the bias filter needs it
    assert evaluate(long_sweep_same_bar(), C15[:100], None, settings(bias_filter=True)) is None
