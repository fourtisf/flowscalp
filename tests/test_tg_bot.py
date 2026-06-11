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

    async def reply_text(t, parse_mode=None, **kw):
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


def mk_update_chat(uid, text=""):
    """Update stub with effective_chat + deletable, replyable message."""
    state = {"deleted": False, "replies": []}

    async def delete():
        state["deleted"] = True

    async def reply_text(t, parse_mode=None, **kw):
        state["replies"].append(t)
        state.setdefault("markups", []).append(kw.get("reply_markup"))

    msg = SimpleNamespace(text=text, delete=delete, reply_text=reply_text)
    upd = SimpleNamespace(effective_user=SimpleNamespace(id=uid),
                          effective_message=msg,
                          effective_chat=SimpleNamespace(id=uid))
    return upd, state


class _FakeBotApp:
    def __init__(self, sink):
        self.bot = SimpleNamespace(send_message=self._send)
        self._sink = sink

    async def _send(self, chat_id=None, text=None):
        self._sink.append(text)


async def test_setkey_deletes_message_writes_env_and_restarts(bot, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sent = []
    bot.app = _FakeBotApp(sent)
    restarts = []
    bot.restarter = lambda: restarts.append(1)

    key = "ab" * 32  # valid 64-hex (no 0x prefix → bot normalizes)
    upd, st = mk_update_chat(OWNER, f"/setkey {key}")
    await bot.cmd_setkey(upd, mk_ctx(key))
    assert st["deleted"], "message containing the key must be deleted"
    assert restarts == [1]
    env = (tmp_path / ".env").read_text()
    assert f"HL_AGENT_PRIVATE_KEY=0x{key}" in env
    assert key not in " ".join(sent)          # never echo the key back
    assert any("alamat agent: 0x" in t for t in sent)
    import os as _os
    assert (_os.stat(tmp_path / ".env").st_mode & 0o777) == 0o600


async def test_setkey_rejects_garbage_but_still_deletes(bot, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sent = []
    bot.app = _FakeBotApp(sent)
    bot.restarter = lambda: (_ for _ in ()).throw(AssertionError("no restart on reject"))
    upd, st = mk_update_chat(OWNER, "/setkey bukan-key")
    await bot.cmd_setkey(upd, mk_ctx("bukan-key"))
    assert st["deleted"]
    assert any("tidak valid" in t for t in sent)
    assert not (tmp_path / ".env").exists()


async def test_setwallet_direct_and_setsub_clear_via_followup(bot, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sent = []
    bot.app = _FakeBotApp(sent)
    restarts = []
    bot.restarter = lambda: restarts.append(1)
    addr = "0x" + "a1" * 20
    upd, _ = mk_update_chat(OWNER)
    await bot.cmd_setwallet(upd, mk_ctx(addr))
    # bare /setsub arms a wait, then 'hapus' clears the value
    upd2, st2 = mk_update_chat(OWNER)
    await bot.cmd_setsub(upd2, mk_ctx())
    assert any("kirim" in r for r in st2["replies"])
    upd3, _ = mk_update_chat(OWNER, "hapus")
    await bot.on_text(upd3, mk_ctx())
    env = (tmp_path / ".env").read_text()
    assert f"HL_MAIN_WALLET_ADDRESS={addr}" in env
    assert "HL_SUBACCOUNT_ADDRESS=\n" in env or env.endswith("HL_SUBACCOUNT_ADDRESS=")
    assert len(restarts) == 2


async def test_bare_command_then_plain_message_flow(bot, tmp_path, monkeypatch):
    """The menu-tap scenario: /setwallet with no arg, then the address as a
    plain follow-up message — must be accepted, deleted and stored."""
    monkeypatch.chdir(tmp_path)
    sent = []
    bot.app = _FakeBotApp(sent)
    restarts = []
    bot.restarter = lambda: restarts.append(1)
    addr = "0xeef06bdfbc1c29a80d8049f640ad3b0682fa2744"

    upd, st = mk_update_chat(OWNER)
    await bot.cmd_setwallet(upd, mk_ctx())            # bare, like a menu tap
    assert any("kirim" in r for r in st["replies"])
    upd2, st2 = mk_update_chat(OWNER, addr)
    await bot.on_text(upd2, mk_ctx())                 # plain follow-up message
    assert st2["deleted"]
    assert restarts == [1]
    assert f"HL_MAIN_WALLET_ADDRESS={addr}" in (tmp_path / ".env").read_text()

    # without a pending request, plain text gets a gentle hint
    upd3, st3 = mk_update_chat(OWNER, "halo bot")
    await bot.on_text(upd3, mk_ctx())
    assert any("perintah" in r for r in st3["replies"])

    # 'batal' cancels a pending request
    upd4, _ = mk_update_chat(OWNER)
    await bot.cmd_setkey(upd4, mk_ctx())
    upd5, st5 = mk_update_chat(OWNER, "batal")
    await bot.on_text(upd5, mk_ctx())
    assert any("dibatalkan" in r for r in st5["replies"])
    assert len(restarts) == 1


async def test_profile_renders(bot):
    upd, replies = mk_update(OWNER)
    await bot.cmd_profile(upd, mk_ctx())
    assert "PROFIL" in replies[0] and "saldo: $10,000.00" in replies[0]
    assert "hari ini" in replies[0] and "total" in replies[0]


def mk_callback(uid, data):
    state = {"edits": [], "markups": []}

    async def answer():
        pass

    async def edit_message_text(t, **kw):
        state["edits"].append(t)
        state["markups"].append(kw.get("reply_markup"))

    q = SimpleNamespace(data=data, answer=answer, edit_message_text=edit_message_text)
    upd = SimpleNamespace(effective_user=SimpleNamespace(id=uid),
                          effective_message=SimpleNamespace(text=""),
                          callback_query=q)
    return upd, state


async def test_bare_set_shows_button_keyboard(bot):
    upd, st = mk_update_chat(OWNER)
    await bot.cmd_set(upd, mk_ctx())
    assert st["markups"] and st["markups"][0] is not None  # inline keyboard attached
    assert "pilih setting" in st["replies"][0]


async def test_callback_value_tap_applies_setting(bot):
    upd, st = mk_callback(OWNER, "set:risk_pct")
    await bot.on_callback(upd, mk_ctx())
    assert "risk_pct = 0.5" in st["edits"][0] and st["markups"][0] is not None
    upd2, st2 = mk_callback(OWNER, "setv:risk_pct:0.25")
    await bot.on_callback(upd2, mk_ctx())
    assert bot.store.current.risk_pct == 0.25
    assert "0.5 → 0.25" in st2["edits"][0]
    # out-of-range via callback is rejected too
    upd3, st3 = mk_callback(OWNER, "setv:risk_pct:9")
    await bot.on_callback(upd3, mk_ctx())
    assert "ditolak" in st3["edits"][0] and bot.store.current.risk_pct == 0.25


async def test_callback_typed_value_flow(bot):
    upd, st = mk_callback(OWNER, "sett:tp_r")
    await bot.on_callback(upd, mk_ctx())
    assert "ketik nilai baru untuk tp_r" in st["edits"][0]
    upd2, replies2 = mk_update(OWNER, "2.2")
    upd2.effective_message.text = "2.2"
    await bot.on_text(upd2, mk_ctx())
    assert bot.store.current.tp_r == 2.2
    assert any("tp_r: 1.8 → 2.2" in r for r in replies2)
