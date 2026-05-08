# Clawberto veNOVA Scan

Read-only veNOVA listing and lock analysis for OpenSea and ve.exchange.

## What It Does

- Reads veNOVA lock internals and listing data.
- Computes listing premium or discount against the underlying NOVA value.
- Writes JSON/CSV report artifacts for manual review or heartbeat jobs.
- Keeps arithmetic validation strict so bad enrichment fails loudly.

## Entrypoints

```bash
python venova_lock_report.py --help
python skills/opensea-venova-lock-arb/scripts/venova_opensea_discount_scan.py --help
```

The 30-minute operational heartbeat is documented in `HEARTBEAT.md`.

## Validation

```bash
python -m unittest discover -s tests
```

## Generated Files

Runtime outputs belong under `runs/` and are ignored by git. The tracked
`venova_*_report.*` files are snapshots that can be refreshed deliberately when
the report format or reference data changes.
