#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../" && pwd)}"
OUT_DIR="${OUT_DIR:-$REPO_DIR/runs/opensea-heartbeat}"
MAX_LISTINGS="${MAX_LISTINGS:-200}"
MIN_NOVA_TOKENS="${MIN_NOVA_TOKENS:-1}"
WORKERS="${WORKERS:-8}"

mkdir -p "$OUT_DIR"

ts="$(date -u +"%Y%m%dT%H%M%SZ")"
json_out="$OUT_DIR/scan-$ts.json"
csv_out="$OUT_DIR/scan-$ts.csv"
log_out="$OUT_DIR/scan-$ts.log"

{
  echo "[heartbeat] started_utc=$ts"
  echo "[heartbeat] repo=$REPO_DIR"
  echo "[heartbeat] out_json=$json_out"
  echo "[heartbeat] out_csv=$csv_out"
  echo "[heartbeat] max_listings=$MAX_LISTINGS min_nova_tokens=$MIN_NOVA_TOKENS workers=$WORKERS"

  cd "$REPO_DIR"
  python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py \
    --max-listings "$MAX_LISTINGS" \
    --min-nova-tokens "$MIN_NOVA_TOKENS" \
    --workers "$WORKERS" \
    --strict-validate \
    --out-json "$json_out" \
    --out-csv "$csv_out"

  ln -sfn "$json_out" "$OUT_DIR/latest.json"
  ln -sfn "$csv_out" "$OUT_DIR/latest.csv"
  echo "[heartbeat] completed_utc=$(date -u +"%Y%m%dT%H%M%SZ")"
} | tee "$log_out"
