# veNOVA Listings Heartbeat

## Purpose

Run an automated veNOVA listing scan every 30 minutes with strict validation.

The heartbeat run:
- retrieves OpenSea listings (GraphQL, newest-first)
- retrieves ve.exchange listings (subgraph API)
- keeps ranking deterministic with timestamp + source-id fallback rails
- reads lock internals on-chain
- fetches NOVA and ETH market prices
- computes premium/discount with strict arithmetic checks
- writes timestamped JSON/CSV artifacts

## One-Time Setup

1. Make scripts executable:
```bash
chmod +x skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py
chmod +x skills/opensea-venova-lock-arb/scripts/heartbeat_opensea_scan.sh
```

2. Dry-run once:
```bash
./skills/opensea-venova-lock-arb/scripts/heartbeat_opensea_scan.sh
```

3. Confirm outputs:
- `runs/opensea-heartbeat/latest.json`
- `runs/opensea-heartbeat/latest.csv`
- `runs/opensea-heartbeat/scan-<timestamp>.log`

## 30-Minute Cron

Add this cron entry:
```cron
*/30 * * * * cd /Users/marko/Clawberto-veNova-scan && /bin/bash /Users/marko/Clawberto-veNova-scan/skills/opensea-venova-lock-arb/scripts/heartbeat_opensea_scan.sh >> /Users/marko/Clawberto-veNova-scan/runs/opensea-heartbeat/cron.log 2>&1
```

Install:
```bash
mkdir -p /Users/marko/Clawberto-veNova-scan/runs/opensea-heartbeat
crontab -e
```

## Runtime Controls

Set optional env vars in cron if needed:
- `COLLECTION_SLUG` (default `venova-652488835`)
- `MAX_LISTINGS` (default `200`)
- `MIN_NOVA_TOKENS` (default `1`)
- `WORKERS` (default `8`)
- `VE_EXCHANGE_CHAIN_ID` (default `1`)
- `VE_EXCHANGE_VE_ADDRESS` (default `0x4C3e7640B3e3A39a2e5d030A0C1412d80FEE1D44`)
- `VE_EXCHANGE_API_BASE` (default `https://api.ve.exchange`)
- `SKIP_VE_EXCHANGE` (`0` default, set `1` to disable ve.exchange source)
- `OUT_DIR` (default `runs/opensea-heartbeat`)
- `REPO_DIR` (auto-detected)

Example:
```cron
*/30 * * * * REPO_DIR=/Users/marko/Clawberto-veNova-scan OUT_DIR=/Users/marko/Clawberto-veNova-scan/runs/opensea-heartbeat COLLECTION_SLUG=venova-652488835 MAX_LISTINGS=300 MIN_NOVA_TOKENS=1 WORKERS=10 VE_EXCHANGE_CHAIN_ID=1 VE_EXCHANGE_VE_ADDRESS=0x4C3e7640B3e3A39a2e5d030A0C1412d80FEE1D44 VE_EXCHANGE_API_BASE=https://api.ve.exchange /bin/bash /Users/marko/Clawberto-veNova-scan/skills/opensea-venova-lock-arb/scripts/heartbeat_opensea_scan.sh >> /Users/marko/Clawberto-veNova-scan/runs/opensea-heartbeat/cron.log 2>&1
```

## Failure Policy

- Heartbeat script uses `--strict-validate`.
- Any arithmetic drift or listing/lock enrichment error causes non-zero exit.
- Non-zero exit appears in `cron.log` and the timestamped run log.
- `latest.json`/`latest.csv` are updated only on successful runs.
