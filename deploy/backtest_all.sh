#!/usr/bin/env bash
# FlowScalp — backtest lengkap satu perintah (BTC, SOL, ETH).
# Pakai di VPS:   bash deploy/backtest_all.sh
# Hasil lengkap:  /opt/flowscalp/data/backtest_summary.txt
#
# Yang diuji per coin (semua divalidasi out-of-sample 30%):
#   [1] setelan sekarang + tabel ABLATION (struktur saja vs +volume vs +CVD)
#   [2] tanpa filter bias tren 15m   (--no-bias)
#   [3] jam trading dibuka 24 jam    (--session 00:00-24:00)
# Baseline [1] juga mencetak "PnL by UTC hour" — bahan menyetel session_windows.
# Data: arsip publik Binance (UM futures 1m) — Hyperliquid hanya menyimpan
# ±3.5 hari candle 1m, terlalu pendek untuk keputusan apa pun. File CSV lama
# yang kekecilan (< MIN_ROWS baris) otomatis diunduh ulang.
# Catatan: tanpa data aggTrades Binance, gate CVD dilewati di backtest,
# jadi baris B dan C di tabel ablation akan identik (CVD belum tervalidasi).
set -euo pipefail

APP=${APP:-/opt/flowscalp}
PY=${PY:-$APP/venv/bin/python}
START=${START:-$(date -u -d "95 days ago" +%F)}
END=${END:-$(date -u -d "yesterday" +%F)}
MIN_ROWS=${MIN_ROWS:-20000}
LOG=$APP/data/backtest_summary.txt

command -v "$PY" >/dev/null 2>&1 || { echo "ERROR: $PY tidak ditemukan — jalankan di VPS, atau set APP=/path/repo PY=python3"; exit 1; }
cd "$APP"
mkdir -p data
: > "$LOG"

say() { printf '%s\n' "$*" | tee -a "$LOG"; }
run() { say ""; say "$1"; shift; "$@" 2>&1 | tee -a "$LOG"; }

say "FlowScalp backtest_all — data $START → $END (UTC), validasi oos 0.3"
for C in BTC SOL ETH; do
  c=$(printf '%s' "$C" | tr '[:upper:]' '[:lower:]')
  csv="data/${c}_1m.csv"
  if [ ! -s "$csv" ] || [ "$(wc -l < "$csv")" -lt "$MIN_ROWS" ]; then
    run "⏬ download data $C $START → $END dari arsip Binance (sekali saja)…" \
      "$PY" -m backtest.data --source binance --coin "$C" --start "$START" --end "$END" --out "$csv"
  fi
  say ""
  say "════════════════════════ $C ════════════════════════"
  run "--- [1] $C: SETELAN SEKARANG + UJI FILTER (ablation) ---" \
    "$PY" -m backtest.run --csv "$csv" --oos 0.3 --ablate --out "data/report_${c}"
  run "--- [2] $C: TANPA BIAS TREN 15m ---" \
    "$PY" -m backtest.run --csv "$csv" --oos 0.3 --no-bias --out "data/report_${c}_nobias"
  run "--- [3] $C: SESI 24 JAM ---" \
    "$PY" -m backtest.run --csv "$csv" --oos 0.3 --session "00:00-24:00" --out "data/report_${c}_24h"
done
say ""
say "✅ SEMUA SELESAI — hasil lengkap tersimpan di: $LOG"
say "   Kirim screenshot terminal (atau isi file itu) ke Claude untuk dibacakan."
