# DEVIATIONS

Everything that diverged from the build spec, and why. Items marked **[env]**
are consequences of the build environment (a sandbox whose network policy
blocks `api.hyperliquid.xyz` and has no Telegram/testnet credentials) — the
code paths exist and are unit/integration tested; the listed follow-up should
be run once on the owner's VPS.

## Environment-driven

1. **[env] Build step 3 gate ("log 5 closed 1m candles live").** The exchange
   API is unreachable from the build sandbox (HTTP 403, host allowlist).
   Replaced with `tests/test_feed_logic.py`, which drives the real handler
   code with recorded-shape messages (rollover close detection, dead-tape
   finalization, delta bucketing, snapshot-skip on userFills). *Follow-up:*
   start the service on the VPS and confirm five `candle_1m_closed` lines.

2. **[env] Build step 4 gate ("paper against real feed until one full trade
   cycle").** Replaced with `tests/test_integration_cycle.py`: a deterministic
   replay drives the real Engine + PaperExecutor + SQLite through
   signal → maker fill → TP/SL/time-stop/kill exits and asserts DB rows, R
   math, alerts and circuit breakers. Strictly more repeatable than a live
   soak; the live soak should still be done on the VPS.

3. **[env] Build step 7 (testnet LiveExecutor pass).** No network/keys here.
   LiveExecutor is written strictly against the installed
   `hyperliquid-python-sdk` 0.24 source (`Info`, `Exchange.order` shapes,
   `Alo`/`Ioc` tifs, trigger orders, `vault_address` for subaccounts,
   `account_address` for agent keys, price rounding incl. the integer-price
   rule for BTC ≥ 100k). *Follow-up:* run the §4 testnet ladder in README
   (entry/exits/kill, kill -9 mid-position restart) before mainnet.

4. **[env] Acceptance 8 ("backtest on ≥60 days").** Real candles can't be
   downloaded here. The backtester was run end-to-end on 60 days of
   *synthetic* data (86,400 bars, sweeps planted; `backtest/synth.py`),
   including `--oos` and `--ablate`. *Follow-up:* `python -m backtest.data`
   + a real 60-day run on the VPS. Expect the synthetic run to lose money —
   random-walk sweeps carry no information; it validates mechanics, not edge.

## Judgment calls (spec ambiguous or silent)

5. **Crash recovery vs "boot default is paper".** §2.6 mandates paper on
   boot; acceptance 3 requires a kill -9 mid-position restart to reconcile
   and re-arm exits. Resolution: boot is always paper **except** when the
   state snapshot shows mode=live with an open position and credentials are
   present — then the bot resumes live (RECONCILE → adopt → re-arm exits via
   cancel_all-then-arm so duplicates are impossible). Going live fresh always
   requires `/mode live` + `/confirm`. `MODE=live` in `.env` is ignored at
   boot with a logged + channel warning (acceptance 10).

6. **Cooldown exit resets the loss streak.** The spec doesn't say. Without a
   reset, `can_enter` (consec_losses ≥ max) would block forever after the
   cooldown expired. COOLDOWN → RUNNING (timer or `/resume`) sets
   `consec_losses = 0`.

7. **`evaluate()` signature.** §6.3b's funding gate needs the current funding
   rate, which isn't in `evaluate(c1m, c15m, deltas, s)`. Added an optional
   trailing parameter `funding_hourly=None` (None → fail-open per spec).
   Strategy stays pure; the backtester passes None (funding history is not
   replayed — `funding_filter` defaults off anyway, noted in the report).

8. **Maker-fill granularity.** Spec: maker entries fill when a *subsequent 1m
   bar* trades through, but `maker_timeout_s` defaults to 20s — shorter than
   one bar, so a bar-close-only simulator would cancel every entry before it
   could fill. PaperExecutor therefore fills from the **mid stream**
   (strictly-through, touch ≠ fill — spec §6.5 says "fills simulated from the
   candle/mid stream") with the bar trade-through rule as fallback; the
   backtester (no mids) gives pending entries a lifetime of
   `ceil(maker_timeout_s / 60)` bars (1 bar at defaults), then cancels or
   IOC-fills per `allow_taker_entry`.

9. **Entry alert at placement.** §6.6 alerts on PENDING_ENTRY and on fill,
   but §7 only defines the filled-entry block. Implemented: compact
   `⏳ placing LONG entry …` at placement + the full spec block on fill.

10. **Runtime reconcile of an unknown exchange position** (bot FLAT, exchange
    not): alert-only, no auto-flatten — auto-flatten could fight a human
    trading the same (sub)account. At *boot*, unknown positions are flattened
    per spec ("adopt or flatten"). README tells the owner to dedicate the
    subaccount to the bot.

11. **SL trigger guard price.** Exchange-side stop-market orders need a limit
    bound; spec doesn't give one. Used trigger ± 5% (it's a backstop — the
    engine-side backup stop at mid ± 0.2% after 2s is the precise path).

12. **Dashboard port** isn't specified: `DASH_PORT` env, default 8080,
    bound to `127.0.0.1` per the security rules.

13. **Trades-stream side field.** Per SDK types and HL docs, `side: "B"`
    = taker buy, `"A"` = taker sell; delta = Σbuy − Σsell. The first live
    session should sanity-check CVD signs via the warm-up events log
    (`gate skip [cvd…]` entries include the running net delta).

14. **Post-exit spacing semantics.** "No new entry within 3 closed 1m bars
    after any exit" implemented as: entries allowed again once
    `(bar_ts − exit_bar_ts) ≥ 3 minutes`, i.e. on the close of the third full
    bar after the exit bar.

15. **`/resume` does not clear HALTED_DAILY** (spec lists only
    PAUSED/COOLDOWN/STOPPED) — the daily halt clears at 00:00 UTC only.

16. **Telegram message format.** §7 specified monospace blocks; the owner
    requested plain-text messages after first use, so replies and alerts are
    sent as normal Telegram text.

17. **Backtest circuit breakers.** §9 only mandates strategy + sizing + fill
    rules; the daily-loss halt, loss-streak cooldown and anti-overtrade
    spacing/dedupe are also simulated so backtest trade counts match what the
    live engine would actually take.
