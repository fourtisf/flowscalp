# Acceptance Checklist (§13)

Build-time results. **[VPS]** items need the owner's VPS (network/testnet
credentials are unavailable in the build environment — see DEVIATIONS.md);
the code paths behind them are covered by the referenced offline tests.

| # | check | result | evidence |
|---|---|---|---|
| 1 | Fresh boot (paper): full simulated trade cycle, correct R math in DB + TG alerts | **PASS** | `tests/test_integration_cycle.py::test_full_cycle_signal_fill_tp_exit` — signal → maker fill → TP; DB row pnl/r asserted to 1e-9; entry/exit alert blocks asserted |
| 2 | `/stop` during a position: cancel + flat < 2s, channel alert | **PASS** (paper) / **[VPS]** testnet | `test_kill_switch_flattens_and_stops` — flatten + STOPPED + 🛑 alert, duration reported; LiveExecutor kill path implemented (cancel_all → IOC ±0.2%, retry on partial) |
| 3 | `kill -9` mid-position, restart: reconciled, exits re-armed, no duplicate orders | **PASS** (paper) / **[VPS]** testnet | `tests/test_main_boot.py::test_paper_position_survives_restart` — same trade id closes after restart; live path: snapshot adopt → cancel_all → arm_exits once |
| 4 | Daily loss limit → HALTED_DAILY, no entries until next UTC day | **PASS** | `test_sl_exit_daily_loss_halt_and_rollover` — halt fires, signal bar ignored, rollover clears + daily report |
| 5 | `/set risk_pct 5` rejected; `/set tp_r 2.0` applied & visible | **PASS** | `tests/test_tg_bot.py::test_set_roundtrip_and_rejection` + `test_config.py` store round-trip (next trade reads `store.current`) |
| 6 | Dashboard == `/pnl day` == DB | **PASS** | both call the same `db.summary(mode, since_ts=state.day_start_ts)`; `tests/test_web.py::test_summary_trades_equity_match_db` |
| 7 | Non-owner `/stop` → ignored + logged | **PASS** | `tests/test_tg_bot.py::test_owner_gate_silently_drops_and_logs` — no reply, no action, one events row |
| 8 | Backtest on ≥60 days runs clean; `--oos` works | **PASS** (synthetic) / **[VPS]** real data | 86,400-bar synthetic run with `--oos 0.3 --ablate` in ~10s, JSON+PNG written; `tests/test_backtest.py` |
| 9 | `grep -rE "0x?[0-9a-fA-F]{64}"` over logs + events → nothing | **PASS** | grep over `logs/` + events table clean; `tests/test_main_boot.py::test_redact_filter_scrubs_key_material` |
| 10 | MODE live switch impossible without `/confirm` | **PASS** | `test_boot_mode_policy` (env MODE=live ignored at boot) + `test_mode_live_requires_confirm_code` (single-use, 60s TTL, wrong code burns it) |

Full suite: **71 passed**, no network required.
