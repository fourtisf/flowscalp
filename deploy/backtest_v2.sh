#!/usr/bin/env bash
# FlowScalp — uji STRATEGI V2: sweep+reclaim di bar 15 MENIT (BTC, SOL, ETH).
# Hipotesis (hasil otopsi v1): SL/TP lebih lebar → fee per R turun dari ~15-20%
# ke ~3-5%, dan time-stop 4 jam memberi ruang follow-through.
# Pakai di VPS:  bash deploy/backtest_v2.sh
#   (data 1m harus sudah ada — jalankan deploy/backtest_all.sh sekali sebelum ini)
# Hasil lengkap: data/backtest_v2.txt; ringkasan dicetak di akhir (RINGKASAN V2).
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
APP=${APP:-$REPO}
PY=${PY:-/opt/flowscalp/venv/bin/python}
command -v "$PY" >/dev/null 2>&1 || PY=python3
LOG=$APP/data/backtest_v2.txt
NEED_ROWS=${NEED_ROWS:-250000}    # ±180 hari 1m; kurang dari ini → download 6 bulan

cd "$APP"
mkdir -p data
: > "$LOG"

for c in btc sol eth; do
  csv="data/${c}_1m.csv"
  if [ ! -s "$csv" ] || [ "$(wc -l < "$csv")" -lt "$NEED_ROWS" ]; then
    echo "⏬ data $c kurang dari ±6 bulan → download arsip Binance (sekali saja)…" | tee -a "$LOG"
    "$PY" -m backtest.data --source binance --coin "$(echo "$c" | tr a-z A-Z)" \
      --start "$(date -u -d '185 days ago' +%F)" --end "$(date -u -d 'yesterday' +%F)" \
      --out "$csv" 2>&1 | tee -a "$LOG"
  fi
  for tp in 1.8 2.5; do
    for sl in 0.8 1.2; do
      for lb in 20 30; do
        tag="${c^^} 15m tp${tp} sl${sl} lb${lb}"
        echo "── $tag ──" | tee -a "$LOG"
        "$PY" -m backtest.run --csv "$csv" --tf 15m --oos 0.3 \
          --tp-r "$tp" --sl-cap "$sl" --lookback "$lb" --vol-mult 1.0 --no-cvd \
          --time-stop 240 --atr-floor 10 --atr-ceil 250 --sl-buffer 0.10 \
          --session "00:00-24:00" --blackout "" \
          --tag "$tag" --out "data/v2_${c}_tp${tp}_sl${sl}_lb${lb}" 2>&1 | tee -a "$LOG"
      done
    done
  done
done

echo ""
echo "================ RINGKASAN V2 (kirim screenshot bagian ini saja) ================"
grep "^RINGKAS" "$LOG"
echo "SELESAI-V2"
