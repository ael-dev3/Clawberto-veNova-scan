#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../" && pwd)}"
OUT_DIR="${OUT_DIR:-$REPO_DIR/runs/opensea-heartbeat}"
MAX_LISTINGS="${MAX_LISTINGS:-200}"
MIN_NOVA_TOKENS="${MIN_NOVA_TOKENS:-1}"
WORKERS="${WORKERS:-8}"
COLLECTION_SLUG="${COLLECTION_SLUG:-venova-652488835}"
VE_EXCHANGE_CHAIN_ID="${VE_EXCHANGE_CHAIN_ID:-1}"
VE_EXCHANGE_VE_ADDRESS="${VE_EXCHANGE_VE_ADDRESS:-0x4C3e7640B3e3A39a2e5d030A0C1412d80FEE1D44}"
VE_EXCHANGE_API_BASE="${VE_EXCHANGE_API_BASE:-https://api.ve.exchange}"
SKIP_VE_EXCHANGE="${SKIP_VE_EXCHANGE:-0}"

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
  echo "[heartbeat] collection_slug=$COLLECTION_SLUG"
  echo "[heartbeat] max_listings=$MAX_LISTINGS min_nova_tokens=$MIN_NOVA_TOKENS workers=$WORKERS"
  echo "[heartbeat] ve_exchange_chain_id=$VE_EXCHANGE_CHAIN_ID ve_exchange_ve_address=$VE_EXCHANGE_VE_ADDRESS"
  echo "[heartbeat] ve_exchange_api_base=$VE_EXCHANGE_API_BASE skip_ve_exchange=$SKIP_VE_EXCHANGE"

  cd "$REPO_DIR"
  cmd=(python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py \
    --collection-slug "$COLLECTION_SLUG" \
    --sort-by latest \
    --max-listings "$MAX_LISTINGS" \
    --min-nova-tokens "$MIN_NOVA_TOKENS" \
    --workers "$WORKERS" \
    --ve-exchange-chain-id "$VE_EXCHANGE_CHAIN_ID" \
    --ve-exchange-ve-address "$VE_EXCHANGE_VE_ADDRESS" \
    --ve-exchange-api-base "$VE_EXCHANGE_API_BASE" \
    --strict-validate \
    --out-json "$json_out" \
    --out-csv "$csv_out")
  if [[ "$SKIP_VE_EXCHANGE" == "1" ]]; then
    cmd+=(--skip-ve-exchange)
  fi
  "${cmd[@]}"

  ln -sfn "$json_out" "$OUT_DIR/latest.json"
  ln -sfn "$csv_out" "$OUT_DIR/latest.csv"
  echo "[heartbeat] completed_utc=$(date -u +"%Y%m%dT%H%M%SZ")"
} | tee "$log_out"
