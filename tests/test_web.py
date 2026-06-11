"""Dashboard API: bearer auth, payload shapes, DB consistency."""
import asyncio
from collections import deque
from types import SimpleNamespace

import httpx
import pytest

from config import SettingsStore
from core.state import BotState, Pos, Position
from db import Database, now_ms
from web.server import create_app


class _Env:
    dash_bearer_token = "sekret123"


@pytest.fixture
async def client(tmp_path):
    db = Database(str(tmp_path / "w.db"))
    await db.connect()
    store = SettingsStore(db)
    await store.load()
    state = BotState(str(tmp_path / "s.json"), mode="paper")
    state.day_realized_pnl = 21.4
    state.day_realized_r = 2.3
    feed = SimpleNamespace(mid=100_100.0, candles_1m=deque(), candles_15m=deque())

    async def eq():
        return 10_214.2
    engine = SimpleNamespace(executor=SimpleNamespace(equity=eq, name="paper"))
    app = create_app(_Env(), db, state, feed, store, engine)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        c.db, c.state = db, state
        yield c
    await db.close()


AUTH = {"Authorization": "Bearer sekret123"}


async def test_auth_required_on_all_api_routes(client):
    for path in ("/api/status", "/api/summary", "/api/trades", "/api/equity"):
        r = await client.get(path)
        assert r.status_code == 401, path
        r = await client.get(path, headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401, path
        r = await client.get(path, headers=AUTH)
        assert r.status_code == 200, path


async def test_index_served_openly(client):
    r = await client.get("/")
    assert r.status_code == 200 and "flowscalp" in r.text.lower()
    # owner-requested persistent login: token remembered in the owner's
    # browser localStorage and accepted via #fragment (never sent to server)
    assert "localStorage" in r.text and "location.hash" in r.text


async def test_status_shape_flat_and_open(client):
    r = await client.get("/api/status", headers=AUTH)
    d = r.json()
    assert d["state"] == "BOOT" and d["mode"] == "paper"
    assert d["equity"] == pytest.approx(10_214.2)
    assert d["day_pnl_usd"] == 21.4 and d["day_r"] == 2.3
    assert d["open_position"] is None and "updated_at" in d

    client.state.position = Position(side="long", entry_px=100_000.0, size_btc=0.0142,
                                     size_usd=1_420.0, sl_px=99_700.0, tp_px=100_500.0,
                                     ts_open=now_ms() - 7 * 60_000, filled_sz=0.0142)
    client.state.pos_state = Pos.IN_POSITION
    d = (await client.get("/api/status", headers=AUTH)).json()
    p = d["open_position"]
    assert p["side"] == "long" and p["minutes"] == 7
    assert p["upnl_usd"] == pytest.approx((100_100.0 - 100_000.0) * 0.0142, abs=0.01)


async def test_summary_trades_equity_match_db(client):
    db = client.db
    t1 = await db.insert_trade(mode="paper", side="long", ts_open=now_ms() - 60_000,
                               entry_px=100_000, size_btc=0.1, size_usd=10_000,
                               sl_px=99_800, tp_px=100_360, signal_reason="t")
    await db.close_trade(t1, ts_close=now_ms(), exit_px=100_360, fees_usd=4.5,
                         pnl_usd=31.5, r_multiple=1.58, exit_reason="tp")
    await db.snapshot_equity("paper", 10_031.5)

    s = (await client.get("/api/summary", headers=AUTH)).json()
    assert s["all"]["trades"] == 1 and s["all"]["wins"] == 1
    assert s["all"]["pnl_usd"] == pytest.approx(31.5)
    assert s["day"]["net_r"] == pytest.approx(1.58)

    tr = (await client.get("/api/trades?limit=5", headers=AUTH)).json()
    assert len(tr) == 1 and tr[0]["exit_reason"] == "tp"

    eq = (await client.get("/api/equity?range=1d", headers=AUTH)).json()
    assert eq and eq[-1]["equity"] == pytest.approx(10_031.5)
    r = await client.get("/api/equity?range=2y", headers=AUTH)
    assert r.status_code == 422


async def test_public_mode_skips_auth(tmp_path):
    """DASH_PUBLIC=true → owner-opted open read-only dashboard."""
    db = Database(str(tmp_path / "pub.db"))
    await db.connect()
    try:
        store = SettingsStore(db)
        await store.load()
        state = BotState(str(tmp_path / "s2.json"), mode="paper")
        feed = SimpleNamespace(mid=1.0, deltas_1m=[])

        async def eq():
            return 1000.0
        engine = SimpleNamespace(executor=SimpleNamespace(equity=eq, name="paper"))

        class _PubEnv:
            dash_bearer_token = "sekret123"
            dash_public = True
        app = create_app(_PubEnv(), db, state, feed, store, engine)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.get("/api/status")          # no Authorization header
            assert r.status_code == 200
            assert r.json()["equity"] == 1000.0
    finally:
        await db.close()
