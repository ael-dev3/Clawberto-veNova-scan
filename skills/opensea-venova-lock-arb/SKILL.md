---
name: opensea-venova-lock-arb
description: OpenClaw skill for veNOVA lock valuation. Retrieve OpenSea (GraphQL) and ve.exchange listing data, read lock internals on-chain, fetch NOVA and ETH market prices, and compute lock-level premium/discount versus underlying NOVA spot value with strict arithmetic validation. Use for lock arbitrage scans, freshness-ranked listing reports, and scheduled heartbeat monitoring.
---

# veNOVA Lock Arb

## Overview

Run one deterministic pipeline:
1. Pull OpenSea listings from paginated GraphQL (`LISTING_CREATED_DATE DESC`) for true freshest ordering.
2. Pull ve.exchange listings from `api.ve.exchange/tracking/subgraphs/listings/{chainId}/{veAddress}`.
3. Parse OpenSea activity from embedded `urql_transport` payloads (optional).
4. For each listing row, read lock state on-chain via `venova_lock_report.py`.
5. Fetch NOVA spot from DexScreener and ETH spot from CoinGecko (with listing-implied fallback).
6. Rank listings explicitly (default: `latest`) and compute per-lock premium/discount versus underlying NOVA.
7. Export JSON/CSV.

Ranking rails:
- `latest` uses `listing_start_time` when present.
- If timestamp is missing (common on ve.exchange rows), fallback is source-native listing id.
- Output includes `listing_timestamp_quality` (`exact`, `id_fallback`, `missing`).

Safety rails:
- HTTPS-only requests.
- Host allowlist for all external HTTP calls.
- Response size caps.
- Input validation for slug/address/worker ranges.

## Workflow

1. Run scan script from repo root:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py
```

2. For latest listings answers (freshest first):
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --sort-by latest
```

3. For production-quality rails, run strict mode with tiny-lock filter:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --min-nova-tokens 1 --strict-validate
```

4. For fast iteration:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --max-listings 30 --skip-activity
```

5. If ve.exchange needs to be excluded:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --skip-ve-exchange
```

6. Keep default output files unless user asks otherwise:
- `venova_opensea_discount_report.json`
- `venova_opensea_discount_report.csv`

7. Inspect required output fields:
- Source + identity: `listing_source`, `lock_id`, `listing_id`, `item_url`
- Timing quality: `listing_start_time`, `listing_timestamp_quality`, `rank_latest`
- Pricing: `listing_price_quote_unit`, `listing_quote_symbol`, `listing_price_usd_effective`
- On-chain value: `nova_inside_tokens`, `nova_inside_value_usd`
- Relative valuation: `premium_pct_vs_spot`, `discount_pct_vs_spot`
- Validation rails: `validation.*`, `arithmetic_validation`, `listing_errors`

## Calculation Rails

Use these exact formulas:
1. `nova_inside_tokens = nova_inside_wei / 1e18`
2. `nova_inside_value_usd = nova_inside_tokens * nova_spot.price_usd`
3. `listing_price_usd_effective`:
- use marketplace-provided USD if present
- else if quote is `ETH/WETH` and ETH spot exists: `listing_price_quote_unit * eth_spot_used.price_usd`
4. `premium_pct_vs_spot = ((listing_price_usd_effective / nova_inside_value_usd) - 1) * 100`
5. `discount_pct_vs_spot = -premium_pct_vs_spot`

Lock valuation basis:
- `smnft_principal`: use converted principal from `calculate_original_sm_nft_amount`
- `locked_amount`: use raw locked amount

## Guardrails

1. OpenSea listings must come from GraphQL with explicit newest-first sort.
2. OpenSea activity can use embedded page payload only; avoid key-gated REST paths.
3. ve.exchange listings should use API endpoint, not DOM scraping.
4. Use `venova_lock_report.query_lock()` for lock reads to keep lock math consistent.
5. Treat rows with missing listing USD or zero underlying NOVA as non-actionable.
6. Keep outputs deterministic and machine-readable (JSON + CSV every run).
7. In autonomous mode, require `--strict-validate`; any arithmetic or listing error should fail.

## Key Script

`scripts/venova_opensea_discount_scan.py`

Main flags:
- `--collection-slug` (default: `venova-652488835`)
- `--rpc-url`
- `--ve-address`
- `--nova-token` (default: `0x00Da8466B296E382E5Da2Bf20962D0cB87200c78`)
- `--nova-price-only` (spot/pricing metadata only)
- `--sort-by` (`latest` default, or `discount`, `price-low`, `price-high`)
- `--eth-price-usd` (manual ETH/USD override)
- `--max-listings`
- `--workers` (1..64)
- `--min-nova-tokens`
- `--skip-activity`
- `--skip-ve-exchange`
- `--ve-exchange-chain-id` (default: `1`)
- `--ve-exchange-ve-address` (default: veNOVA collection address)
- `--ve-exchange-api-base` (default: `https://api.ve.exchange`)
- `--strict-validate`
- `--out-json`
- `--out-csv`

## Output Use

When reporting opportunities, rank by `discount_pct_vs_spot` descending and include:
- Listing source
- Lock id
- List price (quote + USD)
- Underlying NOVA amount + USD value
- Discount/premium %
- Listing URL
- Timestamp + timestamp quality
- Validation warnings
