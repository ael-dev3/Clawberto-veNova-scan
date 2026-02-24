# veNOVA OpenSea Heartbeat

## Purpose

Run an automated OpenSea lock scan every 30 minutes with strict validation.

The heartbeat run:
- retrieves OpenSea listing/activity payload data
- ranks listings by most recent listing time
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
- `MAX_LISTINGS` (default `200`)
- `MIN_NOVA_TOKENS` (default `1`)
- `WORKERS` (default `8`)
- `OUT_DIR` (default `runs/opensea-heartbeat`)
- `REPO_DIR` (auto-detected)

Example:
```cron
*/30 * * * * REPO_DIR=/Users/marko/Clawberto-veNova-scan OUT_DIR=/Users/marko/Clawberto-veNova-scan/runs/opensea-heartbeat MAX_LISTINGS=300 MIN_NOVA_TOKENS=1 WORKERS=10 /bin/bash /Users/marko/Clawberto-veNova-scan/skills/opensea-venova-lock-arb/scripts/heartbeat_opensea_scan.sh >> /Users/marko/Clawberto-veNova-scan/runs/opensea-heartbeat/cron.log 2>&1
```

## Failure Policy

- Heartbeat script uses `--strict-validate`.
- Any arithmetic drift causes non-zero exit.
- Non-zero exit appears in `cron.log` and the timestamped run log.
- `latest.json`/`latest.csv` are updated only on successful runs.
