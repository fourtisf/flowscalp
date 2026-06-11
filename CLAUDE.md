# FlowScalp — Context for Claude Code sessions

Automated liquidity-sweep scalper on Hyperliquid perps (BTC/SOL/ETH, ONE open
position total) with Telegram control + per-trade reports, a public real-time
PnL dashboard, paper mode and a backtester sharing the live code paths.
Built and deployed June 2026; owner is Indonesian — **reply in Bahasa
Indonesia**, user-facing bot/dashboard text is Indonesian too.

## Deployment (owner's VPS — you have NO direct access)
- Hostinger VPS `srv1728105` (Ubuntu 24.04, Python 3.12), bot at
  `/opt/flowscalp`, systemd service `flowscalp`, user `flowscalp`.
- The owner pastes commands into their VS Code SSH terminal. ALWAYS give
  paste-safe one-liners chained with `&&` ending in `&& echo SELESAI`,
  and never mix inspection + `rm` in one block.
- Standard update flow (owner runs it):
  `cd ~/flowscalp && git pull && bash deploy/install.sh && systemctl restart flowscalp && echo SELESAI`
- Telegram bot @scalpaiflowbot ("scalpflow"), owner id 7176469093; alerts go
  to the owner chat directly (TG_CHANNEL_ID = owner id).
- Dashboard: `http://<vps-ip>` via nginx → 127.0.0.1:8080, DASH_PUBLIC=true
  (owner chose open read-only access), `/dashboard` in TG returns the URL.
- `.env` (never in git) holds the HL **agent** key (`/setkey` flow), main
  wallet address, AUTO_RELIVE=true, COINS=BTC,SOL,ETH, PAPER_START_EQUITY=1000.
- Live account ≈ $1,000 USDC in Hyperliquid **Perps** (Account Type must stay
  "Manual"; "Unified" hides funds from clearinghouseState). Agent API keys
  expire ~180 days (authorized ~June 11 2026 → renew ~Dec 2026 via More→API
  + `/setkey`).

## Architecture (spec in DEVIATIONS.md + ACCEPTANCE.md history)
```
core/feed.py    multi-coin CoinBooks; WS supervisor (SDK manager has no
                reconnect — we rebuild w/ backoff); events: ("candle_1m_closed",
                (coin, Candle)), ("mid", (coin, px)), ("fill", raw)
core/strategy.py pure sweep+reclaim evaluate_ex (session/ATR/bias/structure/
                volume/CVD/funding gates) — backtester imports it unchanged
core/risk.py    position_size (risk% / SL distance, lev cap) + can_enter gates
core/engine.py  single consumer of the queue; one position total; circuit
                breakers (daily −3% halt, 4-loss cooldown); maker timeout;
                backup stop; /why diagnosis; testtrade; eco/market close
core/executor.py PaperExecutor (trade-through fills, SL-first) +
                LiveExecutor (Alo entry, reduce-only TP + SL trigger, IOC
                flatten, per-coin szDecimals) — verified vs hyperliquid SDK 0.24
tg/bot.py       owner-gated commands, inline keyboards (/set buttons, close
                buttons, live-testtrade confirm), /setkey-style env writers
                (auto-delete message + self-restart via SIGTERM)
web/            FastAPI read-only API + single-file premium dashboard
backtest/       candleSnapshot downloader (--coin), replay sim, --oos/--grid/
                --ablate, synthetic generator
```
Key behaviors: boot is ALWAYS paper unless AUTO_RELIVE or crash-recovery with
an open live position; settings live in DB (`/set`, instant, restart-proof);
every restart resets the per-coin CVD warm-up (~lookback+24 min, fail-closed);
exit-reason routing prefers fill kind over oid (oid collision bug history);
fill tx hashes: maker fills carry the COUNTERPARTY's hash (alerts annotate).

## Workflow rules for sessions
- Tests: `./venv/bin/python -m pytest -q` (97+, offline). Use
  `set -o pipefail` when piping pytest. Gate every change on green.
- This sandbox CANNOT reach api.hyperliquid.xyz (host allowlist) — never try
  live API calls here; the owner's VPS can.
- Push to `main` (the deploy branch the VPS pulls). Never commit secrets;
  log scrubber redacts 64-hex keys — keep it that way.
- The owner is a trading beginner: explain WHY (fees, R math, stop-losses)
  patiently, prefer buttons/automation over typed commands, and protect them
  from risk-increasing requests (never remove SL; risk_pct guidance 0.25–1.0
  on a $1k account). Their instinct is to tinker — remind them that every
  restart costs CVD warm-up and that judgment belongs to weekly /report +
  backtests, not single trades.

## Pending / next steps
1. Real-data backtests per coin on the VPS (`python -m backtest.data --coin
   SOL …` then `backtest.run --oos 0.3 --ablate`) → tune session_windows and
   verdict on the CVD/volume filters. Owner has NOT run this yet.
2. Domain + certbot HTTPS for the dashboard (currently plain HTTP on the IP).
3. Watch first live signal trades; review /report after ~2 weeks before any
   risk increase.
4. Agent key renewal ~Dec 2026.
