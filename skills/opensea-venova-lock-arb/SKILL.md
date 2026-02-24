---
name: opensea-venova-lock-arb
description: OpenClaw skill for veNOVA lock valuation. Retrieve OpenSea listing/activity data (without OpenSea API keys), read lock internals on-chain, fetch NOVA and ETH market prices, and compute lock-level premium/discount versus underlying NOVA spot value with strict arithmetic validation. Use for lock arbitrage scans, pricing reports, and scheduled heartbeat monitoring.
---

# OpenSea veNOVA Lock Arb

## Overview

Run one deterministic pipeline:
1. Parse OpenSea embedded `urql_transport` payloads for `collectionItems` and `collectionActivity`.
2. For each listed lock, read lock state on-chain via `venova_lock_report.py` helpers.
3. Fetch NOVA spot price from DexScreener and ETH spot from CoinGecko (with OpenSea-implied ETH fallback).
4. Compute per-lock premium/discount versus underlying NOVA value and export JSON/CSV.

## Workflow

1. Run scan script from repo root:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py
```

2. For quick iteration (faster RPC pass), cap lock count:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --max-listings 20
```

3. For production-quality rails, run strict mode with tiny-lock filter:
```bash
python3 skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --min-nova-tokens 1 --strict-validate
```

4. Keep default output files unless user asks otherwise:
- `venova_opensea_discount_report.json`
- `venova_opensea_discount_report.csv`

5. Inspect required fields in output:
- Listing identity: `lock_id`, `item_url`, `listing_marketplace`
- Listing valuation: `listing_price_quote_unit`, `listing_quote_symbol`, `listing_price_usd`, `listing_price_usd_effective`
- On-chain lock value: `nova_inside_tokens`, `nova_inside_value_usd`
- Relative valuation: `premium_pct_vs_spot`, `discount_pct_vs_spot`
- Validation flags: `validation.*`
- Validation list: `arithmetic_validation`

## Calculation Rails

Use these exact formulas:
1. `nova_inside_tokens = nova_inside_wei / 1e18`
2. `nova_inside_value_usd = nova_inside_tokens * nova_spot.price_usd`
3. `listing_price_usd_effective`:
- use OpenSea `listing_price_usd` if present
- else if quote is `ETH/WETH` and ETH spot exists: `listing_price_quote_unit * eth_spot_used.price_usd`
4. `premium_pct_vs_spot = ((listing_price_usd_effective / nova_inside_value_usd) - 1) * 100`
5. `discount_pct_vs_spot = -premium_pct_vs_spot`

Lock valuation basis:
- `smnft_principal`: use converted principal from `calculate_original_sm_nft_amount` path
- `locked_amount`: use raw locked amount

## Guardrails

1. Use OpenSea embedded `urql_transport` payloads, not brittle DOM scraping.
2. Do not rely on OpenSea public REST events endpoint without key; it is key-gated.
3. Use `venova_lock_report.query_lock()` for lock reads to keep lock math consistent.
4. For `is_smnft` locks, use converted principal path from the lock helper; otherwise use `amount`.
5. Treat rows with missing listing USD price or zero underlying NOVA as non-actionable.
6. Keep outputs deterministic and machine-readable (JSON + CSV every run).
7. In autonomous mode, require `--strict-validate`; if validation errors are non-zero, treat run as failed.

## Key Script

`scripts/venova_opensea_discount_scan.py`

Main flags:
- `--collection-slug` (default: `venova-652488835`)
- `--rpc-url`
- `--ve-address`
- `--nova-token` (default: `0x00Da8466B296E382E5Da2Bf20962D0cB87200c78`)
- `--nova-price-only` (fetch spot only)
- `--eth-price-usd` (manual ETH/USD override)
- `--max-listings`
- `--workers`
- `--min-nova-tokens` (skip valuation for tiny-underlying locks)
- `--strict-validate` (hard-fail on arithmetic drift)
- `--skip-activity`
- `--out-json`
- `--out-csv`

## Output Use

When reporting opportunities, rank by `discount_pct_vs_spot` descending (largest discount first), then include:
- Lock id
- List price (ETH + USD)
- Underlying NOVA amount + USD value
- Discount/premium %
- Listing URL
- Any validation warnings
