"""Pure text builders for Telegram alerts and command replies (monospace blocks).

No IO and no telegram imports — the engine and tests use these directly.
"""
from __future__ import annotations

from typing import Optional


def fmt_px(x: float) -> str:
    return f"{x:,.0f}"


def fmt_usd(x: float, signed: bool = False) -> str:
    s = f"{abs(x):,.2f}"
    if signed:
        return f"+${s}" if x >= 0 else f"-${s}"
    return f"${s}"


def entry_placed(side: str, px: float, mode: str) -> str:
    return f"⏳ placing {side.upper()} entry {fmt_px(px)} ({mode})"


def entry_block(pos, mode: str, risk_pct: float, partial: bool = False) -> str:
    e = "🟢" if pos.side == "long" else "🔴"
    sl_pct = (pos.sl_px - pos.entry_px) / pos.entry_px * 100
    risk = abs(pos.entry_px - pos.sl_px)
    tp_r = abs(pos.tp_px - pos.entry_px) / risk if risk > 0 else 0.0
    tag = f"{mode}, partial fill" if partial else mode
    return (f"{e} {pos.side.upper()} BTC  ({tag})\n"
            f"entry {fmt_px(pos.entry_px)} | sl {fmt_px(pos.sl_px)} ({sl_pct:+.2f}%)"
            f" | tp {fmt_px(pos.tp_px)} ({tp_r:.1f}R)\n"
            f"size {pos.size_btc:.4f} BTC ({fmt_usd(pos.size_usd)}) | risk {risk_pct:.2f}% eq\n"
            f"reason: {pos.reason}")


_EXIT_EMOJI = {"time_stop": "⏱", "kill": "🛑"}


def exit_block(side: str, r: float, pnl: float, entry_px: float, exit_px: float,
               minutes: int, reason: str, day_r: float, equity: float) -> str:
    e = _EXIT_EMOJI.get(reason, "✅" if r >= 0 else "❌")
    return (f"{e} CLOSED {side.upper()} BTC  {r:+.2f}R  ({fmt_usd(pnl, signed=True)})\n"
            f"entry {fmt_px(entry_px)} → exit {fmt_px(exit_px)} | {minutes}m | reason: {reason.upper()}\n"
            f"day: {day_r:+.1f}R | equity {fmt_usd(equity)}")


def daily_report(day: str, mode: str, s: dict, equity: float, streak: int) -> str:
    pf = s.get("profit_factor")
    pf_s = "—" if pf is None else f"{pf:.2f}"
    return (f"📊 DAILY REPORT {day} ({mode})\n"
            f"trades {s['trades']} | W/L {s['wins']}/{s['losses']}"
            f" | win {s['win_rate'] * 100:.0f}%\n"
            f"net {s['net_r']:+.2f}R | pnl {fmt_usd(s['pnl_usd'], signed=True)}"
            f" | fees {fmt_usd(s['fees_usd'])}\n"
            f"PF {pf_s} | max DD {s['max_dd_pct']:.1f}% | equity {fmt_usd(equity)}\n"
            f"streak: {streak} losses")


def status_text(st, settings, equity: float, upnl: Optional[float], mid: float,
                uptime_min: int) -> str:
    lines = [
        f"state {st.bot_state.value} | mode {st.mode} | uptime {uptime_min}m",
        f"equity {fmt_usd(equity)} | day {fmt_usd(st.day_realized_pnl, signed=True)}"
        f" ({st.day_realized_r:+.1f}R, {st.day_trades} trades)",
    ]
    pos = st.position
    if pos is not None and pos.ts_open:
        lines.append(f"open {pos.side.upper()} {pos.filled_sz:.4f} BTC @ {fmt_px(pos.entry_px)}"
                     f" | sl {fmt_px(pos.sl_px)} tp {fmt_px(pos.tp_px)}"
                     f" | uPnL {fmt_usd(upnl or 0, signed=True)}")
    elif st.pos_state.value == "PENDING_ENTRY":
        lines.append(f"pending {pos.side.upper()} entry @ {fmt_px(pos.entry_px)}")
    else:
        lines.append("position: flat")
    lines.append(f"mid {fmt_px(mid)} | streak {st.consec_losses}L"
                 f" | risk {settings.risk_pct}% | tp {settings.tp_r}R"
                 f" | sl cap {settings.sl_cap_pct}% | lev≤{settings.leverage_cap}x")
    if st.bot_state.value == "COOLDOWN":
        lines.append(f"cooldown until {st.cooldown_until}")
    return "\n".join(lines)


def pnl_text(period: str, mode: str, s: dict) -> str:
    pf = s.get("profit_factor")
    return (f"PnL [{period}] ({mode})\n"
            f"trades {s['trades']} | win {s['win_rate'] * 100:.0f}%"
            f" | net {s['net_r']:+.2f}R\n"
            f"pnl {fmt_usd(s['pnl_usd'], signed=True)} | fees {fmt_usd(s['fees_usd'])}"
            f" | PF {'—' if pf is None else f'{pf:.2f}'}")


def position_text(pos, upnl: float, minutes: int) -> str:
    if pos is None or not pos.ts_open:
        return "no open position"
    return (f"{pos.side.upper()} {pos.filled_sz:.4f} BTC @ {fmt_px(pos.entry_px)}\n"
            f"sl {fmt_px(pos.sl_px)} | tp {fmt_px(pos.tp_px)}"
            f" | uPnL {fmt_usd(upnl, signed=True)} | {minutes}m in trade")


def settings_text(settings) -> str:
    from config import RANGES
    rows = []
    for k, v in settings.model_dump().items():
        spec = RANGES.get(k, {})
        if isinstance(v, tuple):
            v = ", ".join(v) or "(none)"
        rng = f" [{spec['min']}–{spec['max']}]" if "min" in spec else ""
        desc = spec.get("desc", "")
        rows.append(f"• {k} = {v}{rng}\n   {desc}")
    return "\n".join(rows)
