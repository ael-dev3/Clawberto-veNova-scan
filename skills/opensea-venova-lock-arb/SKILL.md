---
name: opensea-venova-lock-arb
description: Fetch veNOVA lock listing and activity data from OpenSea without API keys, join each lock with on-chain NOVA amount from VotingEscrow, fetch live NOVA market price, and compute lock-level premium/discount versus spot. Use when asked to scan veNOVA locks, compare listing value to underlying NOVA, find discounted/premium locks, or produce OpenSea+on-chain arbitrage reports.
---

# OpenSea veNOVA Lock Arb

## Overview

Run one deterministic pipeline:
1. Parse OpenSea embedded `urql_transport` payloads for `collectionItems` and `collectionActivity`.
2. For each listed lock, read lock state on-chain via `venova_lock_report.py` helpers.
3. Fetch NOVA spot price from DexScreener.
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

3. Keep default output files unless user asks otherwise:
- `venova_opensea_discount_report.json`
- `venova_opensea_discount_report.csv`

4. Inspect required fields in output:
- Listing identity: `lock_id`, `item_url`, `listing_marketplace`
- Listing valuation: `listing_price_quote_unit`, `listing_quote_symbol`, `listing_price_usd`
- On-chain lock value: `nova_inside_tokens`, `nova_inside_value_usd`
- Relative valuation: `premium_pct_vs_spot`, `discount_pct_vs_spot`
- Validation flags: `validation.*`

## Guardrails

1. Use OpenSea embedded `urql_transport` payloads, not brittle DOM scraping.
2. Do not rely on OpenSea public REST events endpoint without key; it is key-gated.
3. Use `venova_lock_report.query_lock()` for lock reads to keep lock math consistent.
4. For `is_smnft` locks, use converted principal path from the lock helper; otherwise use `amount`.
5. Treat rows with missing listing USD price or zero underlying NOVA as non-actionable.
6. Keep outputs deterministic and machine-readable (JSON + CSV every run).

## Key Script

`scripts/venova_opensea_discount_scan.py`

Main flags:
- `--collection-slug` (default: `venova-652488835`)
- `--rpc-url`
- `--ve-address`
- `--nova-token` (default: `0x00Da8466B296E382E5Da2Bf20962D0cB87200c78`)
- `--nova-price-only` (fetch spot only)
- `--max-listings`
- `--workers`
- `--min-nova-tokens` (skip valuation for tiny-underlying locks)
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
