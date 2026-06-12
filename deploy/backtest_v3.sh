#!/usr/bin/env bash
# FlowScalp — uji STRATEGI V3: trend-following bar 4 JAM, long-flat (BTC, SOL, ETH).
# Aturan: breakout tertinggi N bar → beli di open bar berikutnya; keluar lewat
# trailing stop chandelier (k × ATR14) yang hanya bisa naik. Tanpa TP, tanpa
# time-stop. Biaya jujur: taker 2 sisi + slippage + funding 0.01%/8 jam.
# Pakai di VPS:  bash deploy/backtest_v3.sh
# Hasil lengkap: data/backtest_v3.txt; ringkasan RINGKAS dicetak di akhir.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
APP=${APP:-$REPO}
PY=${PY:-/opt/flowscalp/venv/bin/python}
command -v "$PY" >/dev/null 2>&1 || PY=python3
LOG=$APP/data/backtest_v3.txt
DAYS=${DAYS:-540}                 # ±18 bulan sejarah
NEED_ROWS=${NEED_ROWS:-3000}      # ±540 hari bar 4h; kurang → download ulang

cd "$APP"
mkdir -p data
: > "$LOG"

for c in btc sol eth; do
  csv="data/${c}_4h.csv"
  if [ ! -s "$csv" ] || [ "$(wc -l < "$csv")" -lt "$NEED_ROWS" ]; then
    echo "⏬ download data 4h $c (${DAYS} hari, file kecil — sebentar)…" | tee -a "$LOG"
    "$PY" -m backtest.data --source binance --interval 4h --coin "$(echo "$c" | tr a-z A-Z)" \
      --start "$(date -u -d "${DAYS} days ago" +%F)" --end "$(date -u -d 'yesterday' +%F)" \
      --out "$csv" 2>&1 | tee -a "$LOG"
  fi
  for lb in 28 42; do
    for k in 2.5 3.5; do
      tag="${c^^} 4h lb${lb} k${k}"
      echo "── $tag ──" | tee -a "$LOG"
      "$PY" -m backtest.trend --csv "$csv" --oos 0.3 \
        --lookback "$lb" --atr-k "$k" \
        --tag "$tag" --out "data/v3_${c}_lb${lb}_k${k}" 2>&1 | tee -a "$LOG"
    done
  done
done

echo ""
echo "================ RINGKASAN V3 (kirim screenshot bagian ini saja) ================"
grep "^RINGKAS" "$LOG"
echo "SELESAI-V3"
