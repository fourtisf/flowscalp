import pytest

from config import Settings, SettingsStore, validate_setting
from db import Database


def test_validate_in_range():
    ok, v = validate_setting("risk_pct", "0.75")
    assert ok and v == 0.75
    ok, v = validate_setting("tp_r", "2.0")
    assert ok and v == 2.0
    ok, v = validate_setting("sweep_lookback", "60")
    assert ok and v == 60 and isinstance(v, int)


def test_trend_v3_settings_validate():
    ok, v = validate_setting("trend_mode", "true")
    assert ok and v is True
    ok, v = validate_setting("trend_lookback", "42")
    assert ok and v == 42
    ok, _ = validate_setting("trend_lookback", "10")          # below min 20
    assert not ok
    ok, v = validate_setting("trend_atr_k", "3.5")
    assert ok and v == 3.5
    ok, _ = validate_setting("trend_atr_k", "9")              # above max 5
    assert not ok


def test_validate_out_of_range():
    ok, err = validate_setting("risk_pct", "5")
    assert not ok and "out of range" in err
    ok, err = validate_setting("tp_r", "0.5")
    assert not ok
    ok, err = validate_setting("cooldown_min", "10000")
    assert not ok


def test_validate_unknown_key():
    ok, err = validate_setting("martingale", "on")
    assert not ok and "unknown key" in err


def test_validate_bool_and_int_strictness():
    ok, v = validate_setting("cvd_filter", "off")
    assert ok and v is False
    ok, v = validate_setting("allow_taker_entry", "TRUE")
    assert ok and v is True
    ok, _ = validate_setting("cvd_filter", "maybe")
    assert not ok
    ok, _ = validate_setting("time_stop_min", "12.5")
    assert not ok  # int key rejects fractional


def test_validate_windows():
    ok, v = validate_setting("session_windows", "07:00-20:00,22:00-02:00")
    assert ok and v == ("07:00-20:00", "22:00-02:00")
    ok, v = validate_setting("blackout_windows", '["13:15-13:45"]')
    assert ok and v == ("13:15-13:45",)
    ok, err = validate_setting("session_windows", "25:00-26:00")
    assert not ok
    ok, v = validate_setting("blackout_windows", "")
    assert ok and v == ()


@pytest.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "t.db"))
    await d.connect()
    yield d
    await d.close()


async def test_store_bootstrap_and_roundtrip(db):
    store = SettingsStore(db)
    s = await store.load()
    assert s == Settings()  # defaults bootstrapped
    assert (await db.get_setting("risk_pct")) is not None

    ok, old, new = await store.set("tp_r", "2.0")
    assert ok and old == 1.8 and new == 2.0
    assert (await store.get()).tp_r == 2.0

    ok, old, err = await store.set("risk_pct", "9")
    assert not ok and (await store.get()).risk_pct == 0.5

    # persisted: a fresh store sees the committed value
    store2 = SettingsStore(db)
    s2 = await store2.load()
    assert s2.tp_r == 2.0 and s2.risk_pct == 0.5


async def test_store_atr_band_consistency(db):
    store = SettingsStore(db)
    await store.load()
    ok, _, err = await store.set("atr_floor_bps", "50")
    assert ok  # 50 < default ceil 60
    ok, _, err = await store.set("atr_ceil_bps", "45")
    assert not ok and "must stay below" in str(err)
