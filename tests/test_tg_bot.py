"""Owner gate, /mode live confirm flow and /set round-trip — no telegram network."""
import time
from types import SimpleNamespace

import pytest

from config import SettingsStore
from core.state import BotState
from db import Database
from tg.bot import ConfirmFlow, Notifier, TgBot, owner_gate

OWNER = 123456789


class _Env:
    tg_owner_id = OWNER
    tg_bot_token = None
    tg_channel_id = None
    paper_start_equity = 10_000.0


class StubEngine:
    def __init__(self):
        self.calls = []

        async def eq():
            return 10_000.0
        self.executor = SimpleNamespace(equity=eq, name="paper")

    async def submit(self, name, **kw):
        self.calls.append((name, kw))
        return f"ok:{name}:{kw.get('target', '')}"


def mk_update(uid, text=""):
    replies = []

    async def reply_text(t, parse_mode=None):
        replies.append(t)

    msg = SimpleNamespace(text=text, reply_text=reply_text)
    return SimpleNamespace(effective_user=SimpleNamespace(id=uid) if uid else None,
                           effective_message=msg), replies


def mk_ctx(*args):
    return SimpleNamespace(args=list(args))


@pytest.fixture
async def bot(tmp_path):
    db = Database(str(tmp_path / "t.db"))
    await db.connect()
    store = SettingsStore(db)
    await store.load()
    state = BotState(str(tmp_path / "s.json"))
    feed = SimpleNamespace(mid=100_000.0)
    engine = StubEngine()
    b = TgBot(_Env(), db, store, state, feed, engine, Notifier(_Env()))
    b.engine_stub = engine
    yield b
    await db.close()


async def test_owner_gate_silently_drops_and_logs(bot):
    gated = owner_gate(bot.env, bot.db)(bot.cmd_stop)
    upd, replies = mk_update(999, "/stop")
    await gated(upd, mk_ctx())
    assert replies == []                      # never reply
    assert bot.engine_stub.calls == []        # never act
    rows = await bot.db.fetchall("SELECT msg FROM events WHERE msg LIKE '%non-owner%'")
    assert len(rows) == 1 and "999" in rows[0]["msg"]
    # owner passes through
    upd, replies = mk_update(OWNER, "/stop")
    await gated(upd, mk_ctx())
    assert bot.engine_stub.calls == [("stop", {})] and replies


async def test_mode_live_requires_confirm_code(bot):
    upd, replies = mk_update(OWNER)
    await bot.cmd_mode(upd, mk_ctx("live"))
    assert bot.engine_stub.calls == []        # not switched yet
    assert "confirm" in replies[0]
    code = replies[0].split("/confirm ")[1].split()[0]

    # wrong code → rejected, code burned
    upd2, replies2 = mk_update(OWNER)
    await bot.cmd_confirm(upd2, mk_ctx("nope42"))
    assert "salah" in replies2[0] and bot.engine_stub.calls == []
    upd3, replies3 = mk_update(OWNER)
    await bot.cmd_confirm(upd3, mk_ctx(code))  # old code no longer valid (single try)
    assert "salah" in replies3[0] and bot.engine_stub.calls == []

    # proper flow
    upd4, replies4 = mk_update(OWNER)
    await bot.cmd_mode(upd4, mk_ctx("live"))
    code2 = replies4[0].split("/confirm ")[1].split()[0]
    upd5, replies5 = mk_update(OWNER)
    await bot.cmd_confirm(upd5, mk_ctx(code2))
    assert bot.engine_stub.calls == [("mode", {"target": "live"})]
    assert "ok:mode:live" in replies5[0]


async def test_confirm_expiry(bot):
    bot.confirm = ConfirmFlow(ttl_s=0)
    upd, replies = mk_update(OWNER)
    await bot.cmd_mode(upd, mk_ctx("live"))
    code = replies[0].split("/confirm ")[1].split()[0]
    time.sleep(0.01)
    upd2, replies2 = mk_update(OWNER)
    await bot.cmd_confirm(upd2, mk_ctx(code))
    assert "kedaluwarsa" in replies2[0]
    assert bot.engine_stub.calls == []


async def test_mode_paper_needs_no_confirm(bot):
    upd, replies = mk_update(OWNER)
    await bot.cmd_mode(upd, mk_ctx("paper"))
    assert bot.engine_stub.calls == [("mode", {"target": "paper"})]


async def test_set_roundtrip_and_rejection(bot):
    upd, replies = mk_update(OWNER)
    await bot.cmd_set(upd, mk_ctx("tp_r", "2.0"))
    assert "tp_r: 1.8 → 2.0" in replies[0]
    assert bot.store.current.tp_r == 2.0
    upd2, replies2 = mk_update(OWNER)
    await bot.cmd_set(upd2, mk_ctx("risk_pct", "5"))
    assert "ditolak" in replies2[0] and "out of range" in replies2[0]
    assert bot.store.current.risk_pct == 0.5
    upd3, replies3 = mk_update(OWNER)
    await bot.cmd_set(upd3, mk_ctx("martingale", "on"))
    assert "ditolak" in replies3[0] and "unknown key" in replies3[0]


async def test_status_and_pnl_render(bot):
    upd, replies = mk_update(OWNER)
    await bot.cmd_status(upd, mk_ctx())
    assert "state BOOT" in replies[0] and "equity $10,000.00" in replies[0]
    upd2, replies2 = mk_update(OWNER)
    await bot.cmd_pnl(upd2, mk_ctx("all"))
    assert "PnL [all]" in replies2[0]
    upd3, replies3 = mk_update(OWNER)
    await bot.cmd_pnl(upd3, mk_ctx("fortnight"))
    assert "cara pakai" in replies3[0]
