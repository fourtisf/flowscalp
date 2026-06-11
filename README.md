# FlowScalp v1

Automated BTC-PERP scalper for **Hyperliquid** — one symbol, max one position
at a time. Liquidity-sweep + reclaim entries on 1m candles with a 15m EMA200
bias filter and a conjunctive confirmation stack (volume, CVD absorption,
session windows, funding skew). Telegram control + channel reports, live PnL
web dashboard, paper mode and a backtester that share the exact same strategy
and fill rules.

> The deliverable is **correctness and safety**, not a profit guarantee.
> Default parameters are a framework to tune via backtest → paper → live.

```
feed.py ──(asyncio.Queue events)──▶ engine.py ──▶ executor (live | paper)
   │                                   │ ▲                │
   │ candles/mids/fills                ▼ │ commands       ▼ fills/orders
   └────────────▶ state.py ◀── tg/bot.py            Hyperliquid / simulator
                     │
              db.py (SQLite WAL) ◀── web/server.py (read-only)
```

---

## 1. Security model (read first)

- The key in `.env` is a **Hyperliquid agent (API) wallet key**. It can sign
  orders but **cannot withdraw**. Your main wallet key must never exist on
  this machine — the bot will never ask for it.
- **Create the agent wallet:** Hyperliquid app → **More → API** → generate an
  API/agent wallet → **approve it from your main wallet** → copy the agent's
  private key into `HL_AGENT_PRIVATE_KEY`, and your main wallet's *address*
  (not key) into `HL_MAIN_WALLET_ADDRESS`.
- **Use a subaccount:** Hyperliquid app → Sub-Accounts → create one (e.g.
  "flowscalp") → transfer a small test amount → put its address in
  `HL_SUBACCOUNT_ADDRESS`. The bot then trades and reads equity from that
  subaccount only. **Fund it with test capital only.**
- `.env` must be `chmod 600`, owned by the `flowscalp` user (installer does this).
- Every log handler scrubs anything matching a 64-hex private key to
  `[REDACTED_KEY]`.
- The dashboard binds `127.0.0.1` only and every `/api/*` call requires
  `Authorization: Bearer DASH_BEARER_TOKEN`. Public exposure is your job via
  a [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)
  (`cloudflared tunnel --url http://127.0.0.1:8080`).
- The bot **always boots in paper mode**. Going live requires the
  `/mode live` + `/confirm <code>` two-step in Telegram. The single exception:
  if the process crashed while LIVE **with an open position**, it resumes live
  on restart to keep managing that position (exits re-armed, no duplicates).

## 2. Telegram setup

1. **Bot:** talk to [@BotFather](https://t.me/BotFather) → `/newbot` → copy
   the token into `TG_BOT_TOKEN`.
2. **Owner ID:** message [@userinfobot](https://t.me/userinfobot) → put your
   numeric id into `TG_OWNER_ID`. Only this user can issue commands; everyone
   else is silently ignored (and logged).
3. **Channel:** create a private channel for alerts, add your bot as admin
   (post permission), forward any channel message to @userinfobot to get the
   `-100…` id → `TG_CHANNEL_ID`.

## 3. Install (Ubuntu 22.04/24.04 VPS)

```bash
git clone <this repo> && cd flowscalp
sudo deploy/install.sh          # user, /opt/flowscalp, venv, systemd, backup cron
sudo nano /opt/flowscalp/.env   # fill in keys/ids; see .env.example
sudo systemctl start flowscalp
journalctl -u flowscalp -f
```

Nightly cron backs the SQLite DB up to `/opt/flowscalp/backups/` (14 kept).
**Restore:** stop the service, copy a backup over `data/flowscalp.db`, start.

## 4. Operating it

| command | effect |
|---|---|
| `/status` | state, mode, equity, day PnL ($, R), open position, settings digest |
| `/pnl [day\|week\|all]` | realized PnL, trades, win rate, net R, fees |
| `/positions` | open position detail with live uPnL |
| `/set <key> <value>` | validated against RANGES, e.g. `/set risk_pct 0.75` |
| `/settings` | full settings dump |
| `/pause` / `/resume` | pause new entries / resume (clears PAUSED, COOLDOWN, STOPPED) |
| `/stop` | **kill switch**: cancel all + flatten now, state STOPPED |
| `/mode paper\|live` | live replies a 6-char code → `/confirm <code>` within 60s |
| `/report` | post the daily-style summary to the channel immediately |

State machine: `BOOT → RECONCILE → RUNNING ⇄ PAUSED`; loss streak →
`COOLDOWN(t)` (auto-resumes and resets the streak); daily loss limit →
`HALTED_DAILY` (clears at 00:00 UTC); `/stop` → `STOPPED` → `/resume`.

### The ladder: paper → testnet → live

1. **Paper** (default): run for days; check fills/alerts/dashboard sanity.
2. **Testnet:** set `HL_NETWORK=testnet` with a testnet agent key
   (faucet at app.hyperliquid-testnet.xyz), `/mode live` + `/confirm`,
   verify entry/TP/SL/kill behavior and a restart mid-position.
3. **Live:** `HL_NETWORK=mainnet`, subaccount funded small, and **start with**
   `/set risk_pct 0.25`. Raise only after the live fill quality matches paper.

Notes:
- Paper equity persists in the DB (`paper_equity` settings row). Reset:
  stop the service and `sqlite3 data/flowscalp.db "DELETE FROM settings WHERE key='paper_equity'"`.
- After any restart the bot is in **paper** (unless live crash-recovery, above).
- `MODE=live` in `.env` is intentionally ignored at boot.

## 5. Backtesting

```bash
# 1) data (run on the VPS, ~5k candles/request, paginated)
venv/bin/python -m backtest.data --start 2026-03-01 --end 2026-06-01 --out data/btc_1m.csv

# 2) run — same strategy + sizing + fill rules as live
venv/bin/python -m backtest.run --csv data/btc_1m.csv --risk 0.5 --tp-r 1.8 --sl-cap 0.5 --oos 0.3

# which filters earn their keep / parameter sweep
venv/bin/python -m backtest.run --csv data/btc_1m.csv --ablate --grid --oos 0.3

# validate the CVD gate with real order-flow deltas (Binance Vision aggTrades)
#   https://data.binance.vision/?prefix=data/futures/um/daily/aggTrades/BTCUSDT/
venv/bin/python -m backtest.run --csv data/btc_1m.csv --trades data/aggtrades/
```

**How to judge a report** (in this order):
1. **net R after fees** over ≥60 days — positive and stable across IS/OOS;
2. **profit factor > 1.3** on the OOS split, not just in-sample;
3. **fee share of gross < 35%** — above that the edge is being eaten by fees;
4. **max drawdown** you could actually sit through at your risk_pct;
5. the **PnL-by-UTC-hour table** → tighten `session_windows`/`blackout_windows`.

Without `--trades`, the CVD gate is skipped and the run prints a warning —
treat such results as *CVD-unvalidated*. `--grid` output is in-sample by
construction; only trust combos that hold up OOS.

## 6. Dashboard

Open `http://127.0.0.1:8080` (or your tunnel URL), paste the bearer token once
(kept in page memory only). Red top border = LIVE money, amber = paper.
`DASH_PORT`/`DASH_HOST` env vars override the default `127.0.0.1:8080`.

## 7. Troubleshooting

- `journalctl -u flowscalp -f` and `logs/flowscalp.log` (rotated, scrubbed).
- `sqlite3 data/flowscalp.db "SELECT datetime(ts/1000,'unixepoch'),level,msg FROM events ORDER BY ts DESC LIMIT 30"`
  — every gate skip, reconcile discrepancy and error lands here.
- Feed stale >30s blocks entries automatically; >60s raises a channel alert.
- Tests: `venv/bin/python -m pytest` (71 tests, no network needed).
