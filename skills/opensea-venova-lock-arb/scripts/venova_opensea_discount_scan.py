#!/usr/bin/env python3
"""
Scan veNOVA lock listings from OpenSea and compute premium/discount versus NOVA spot price.

Data sources:
- OpenSea collection page (embedded urql payload: collectionItems)
- OpenSea activity page (embedded urql payload: collectionActivity)
- On-chain VotingEscrow lock data via venova_lock_report.query_lock()
- NOVA spot from DexScreener token endpoint
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from venova_lock_report import (  # type: ignore[import]
        DECIMALS,
        DEFAULT_RPC_URL,
        DEFAULT_VE_ADDRESS,
        query_lock,
    )
except Exception as exc:
    raise RuntimeError(
        "Failed to import venova_lock_report.py. Run this script from inside the repository."
    ) from exc


DEFAULT_COLLECTION_SLUG = "venova-652488835"
DEFAULT_NOVA_TOKEN = "0x00Da8466B296E382E5Da2Bf20962D0cB87200c78"
DEFAULT_TIMEOUT_SEC = 30
USER_AGENT = "Mozilla/5.0 (compatible; OpenClaw-veNOVA-scan/1.0)"
ETH_QUOTES = {"ETH", "WETH"}

URQL_PUSH_RE = re.compile(
    r'\(window\[Symbol\.for\("urql_transport"\)\] \?\?= \[\]\)\.push\((\{.*?\})\)</script>',
    re.S,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch OpenSea veNOVA lock listings, enrich with on-chain lock NOVA amounts, "
            "and compute premium/discount versus NOVA spot."
        )
    )
    parser.add_argument(
        "--collection-slug",
        default=DEFAULT_COLLECTION_SLUG,
        help=f"OpenSea collection slug (default: {DEFAULT_COLLECTION_SLUG})",
    )
    parser.add_argument(
        "--rpc-url",
        default=DEFAULT_RPC_URL,
        help=f"Ethereum JSON-RPC URL (default: {DEFAULT_RPC_URL})",
    )
    parser.add_argument(
        "--ve-address",
        default=DEFAULT_VE_ADDRESS,
        help=f"VotingEscrow contract address (default: {DEFAULT_VE_ADDRESS})",
    )
    parser.add_argument(
        "--nova-token",
        default=DEFAULT_NOVA_TOKEN,
        help=f"NOVA token address for spot price (default: {DEFAULT_NOVA_TOKEN})",
    )
    parser.add_argument(
        "--max-listings",
        type=int,
        default=0,
        help="Optional max number of listing rows to analyze (0 = all parsed listings).",
    )
    parser.add_argument(
        "--sort-by",
        default="latest",
        choices=["latest", "discount", "price-low", "price-high"],
        help=(
            "Ordering of output listings array: "
            "latest (most recently listed first), discount (largest discount first), "
            "price-low, price-high. Default: latest."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel RPC worker count for lock reads (default: 8).",
    )
    parser.add_argument(
        "--skip-activity",
        action="store_true",
        help="Skip activity-page fetch/parsing.",
    )
    parser.add_argument(
        "--nova-price-only",
        action="store_true",
        help="Only fetch and print NOVA spot pricing metadata (skip OpenSea and lock analysis).",
    )
    parser.add_argument(
        "--min-nova-tokens",
        type=float,
        default=0.0,
        help="If >0, skip premium/discount calc for rows below this underlying NOVA amount.",
    )
    parser.add_argument(
        "--eth-price-usd",
        type=float,
        default=None,
        help="Optional manual ETH/USD override used when listing USD must be derived from ETH quote.",
    )
    parser.add_argument(
        "--strict-validate",
        action="store_true",
        help="Fail the run if arithmetic consistency checks detect calculation drift.",
    )
    parser.add_argument(
        "--out-json",
        default="venova_opensea_discount_report.json",
        help="Output JSON report path (default: venova_opensea_discount_report.json).",
    )
    parser.add_argument(
        "--out-csv",
        default="venova_opensea_discount_report.csv",
        help="Output CSV report path (default: venova_opensea_discount_report.csv).",
    )
    return parser.parse_args()


def must_url(path: str) -> str:
    if not path.startswith("http://") and not path.startswith("https://"):
        raise ValueError(f"Expected URL, got: {path}")
    return path


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num):
        return None
    return num


def parse_iso8601_utc(value: Any) -> Optional[dt.datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def fetch_text(url: str, timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> str:
    req = urllib.request.Request(
        must_url(url),
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as response:
            raw = response.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"HTTP request failed for {url}: {exc}") from exc
    return raw.decode("utf-8", errors="replace")


def fetch_json(url: str, timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> Dict[str, Any]:
    raw = fetch_text(url, timeout_sec=timeout_sec)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from {url}: {exc}") from exc


def extract_urql_payloads(html: str) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    for blob in URQL_PUSH_RE.findall(html):
        try:
            payload = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def iter_rehydrate_entries(payloads: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for payload in payloads:
        rehydrate = payload.get("rehydrate")
        if not isinstance(rehydrate, dict):
            continue
        for value in rehydrate.values():
            if isinstance(value, dict):
                yield value


def should_replace_listing(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
    old_usd = to_float(old.get("listing_price_usd"))
    new_usd = to_float(new.get("listing_price_usd"))
    if old_usd is not None and new_usd is not None:
        return new_usd < old_usd
    if old_usd is None and new_usd is not None:
        return True
    old_q = to_float(old.get("listing_price_quote_unit"))
    new_q = to_float(new.get("listing_price_quote_unit"))
    if old_q is not None and new_q is not None:
        return new_q < old_q
    return False


def parse_collection_listings(
    payloads: Iterable[Dict[str, Any]],
    *,
    collection_slug: str,
) -> List[Dict[str, Any]]:
    by_lock: Dict[int, Dict[str, Any]] = {}

    for entry in iter_rehydrate_entries(payloads):
        data = entry.get("data")
        if not isinstance(data, dict):
            continue
        collection_items = data.get("collectionItems")
        if not isinstance(collection_items, dict):
            continue

        items = collection_items.get("items")
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            token_id_raw = str(item.get("tokenId") or "").strip()
            if not token_id_raw.isdigit():
                continue
            lock_id = int(token_id_raw)
            best_listing = item.get("bestListing")
            if not isinstance(best_listing, dict):
                continue

            price_per_item = best_listing.get("pricePerItem")
            if not isinstance(price_per_item, dict):
                continue

            token_price = price_per_item.get("token")
            token_price = token_price if isinstance(token_price, dict) else {}

            listing_symbol = str(token_price.get("symbol") or "").upper() or None
            listing_quote = to_float(token_price.get("unit"))
            listing_usd = to_float(price_per_item.get("usd"))
            market = best_listing.get("marketplace")
            market = market if isinstance(market, dict) else {}

            item_name = str(item.get("name") or "")
            lock_in_name = None
            m = re.search(r"lock\s*#\s*(\d+)", item_name, flags=re.IGNORECASE)
            if m:
                lock_in_name = int(m.group(1))

            row = {
                "lock_id": lock_id,
                "item_name": item_name,
                "item_contract": str(item.get("contractAddress") or ""),
                "item_token_id": token_id_raw,
                "item_url": f"https://opensea.io/item/ethereum/{item.get('contractAddress')}/{token_id_raw}",
                "listing_id": best_listing.get("id"),
                "listing_price_quote_unit": listing_quote,
                "listing_quote_symbol": listing_symbol,
                "listing_price_usd": listing_usd,
                "listing_marketplace": market.get("identifier"),
                "listing_start_time": best_listing.get("startTime"),
                "listing_end_time": best_listing.get("endTime"),
                "listing_quantity_remaining": best_listing.get("quantityRemaining"),
                "listing_maker": (
                    (best_listing.get("maker") or {}).get("address")
                    if isinstance(best_listing.get("maker"), dict)
                    else None
                ),
                "collection_slug": collection_slug,
                "name_lock_id_match": (lock_in_name == lock_id) if lock_in_name is not None else None,
            }

            prev = by_lock.get(lock_id)
            if prev is None or should_replace_listing(prev, row):
                by_lock[lock_id] = row

    rows = list(by_lock.values())

    def collection_latest_key(row: Dict[str, Any]):
        start_dt = parse_iso8601_utc(row.get("listing_start_time"))
        if start_dt is None:
            return (1, 0.0, int(row.get("lock_id") or 0))
        return (0, -start_dt.timestamp(), int(row.get("lock_id") or 0))

    rows.sort(key=collection_latest_key)
    return rows


def parse_collection_activity(payloads: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for entry in iter_rehydrate_entries(payloads):
        data = entry.get("data")
        if not isinstance(data, dict):
            continue
        activity = data.get("collectionActivity")
        if not isinstance(activity, dict):
            continue
        items = activity.get("items")
        if not isinstance(items, list):
            continue

        for event in items:
            if not isinstance(event, dict):
                continue
            item = event.get("item")
            item = item if isinstance(item, dict) else {}
            token_id_raw = str(item.get("tokenId") or "").strip()
            lock_id = int(token_id_raw) if token_id_raw.isdigit() else None

            price = event.get("price")
            price = price if isinstance(price, dict) else {}
            price_token = price.get("token")
            price_token = price_token if isinstance(price_token, dict) else {}

            out.append(
                {
                    "event_id": event.get("id"),
                    "event_time": event.get("eventTime"),
                    "event_type": event.get("__typename") or event.get("type"),
                    "lock_id": lock_id,
                    "transaction_hash": event.get("transactionHash"),
                    "price_quote_unit": to_float(price_token.get("unit")),
                    "price_quote_symbol": (str(price_token.get("symbol") or "").upper() or None),
                    "price_usd": to_float(price.get("usd")),
                    "from": ((event.get("from") or {}).get("address") if isinstance(event.get("from"), dict) else None),
                    "to": ((event.get("to") or {}).get("address") if isinstance(event.get("to"), dict) else None),
                    "marketplace": event.get("mintMarketplace"),
                }
            )
    out.sort(key=lambda x: str(x.get("event_time") or ""), reverse=True)
    return out


def fetch_nova_spot(token_address: str) -> Dict[str, Any]:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{urllib.parse.quote(token_address)}"
    payload = fetch_json(url)
    pairs = payload.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        raise RuntimeError("DexScreener returned no pairs for NOVA token.")

    token_address_lc = token_address.lower()
    candidates = []
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        base = pair.get("baseToken")
        base = base if isinstance(base, dict) else {}
        base_addr = str(base.get("address") or "").lower()
        if base_addr == token_address_lc:
            candidates.append(pair)

    if not candidates:
        candidates = [p for p in pairs if isinstance(p, dict)]
    if not candidates:
        raise RuntimeError("DexScreener candidate pair set is empty.")

    def liq_usd(p: Dict[str, Any]) -> float:
        liq = p.get("liquidity")
        liq = liq if isinstance(liq, dict) else {}
        return to_float(liq.get("usd")) or -1.0

    best = max(candidates, key=liq_usd)
    price_usd = to_float(best.get("priceUsd"))
    if price_usd is None or price_usd <= 0:
        raise RuntimeError("DexScreener best pair is missing valid priceUsd.")

    quote = best.get("quoteToken")
    quote = quote if isinstance(quote, dict) else {}
    base = best.get("baseToken")
    base = base if isinstance(base, dict) else {}
    liquidity = best.get("liquidity")
    liquidity = liquidity if isinstance(liquidity, dict) else {}

    return {
        "source": "dexscreener",
        "token_address": token_address,
        "pair_address": best.get("pairAddress"),
        "dex_id": best.get("dexId"),
        "pair_url": best.get("url"),
        "base_symbol": base.get("symbol"),
        "quote_symbol": quote.get("symbol"),
        "price_usd": price_usd,
        "price_native": to_float(best.get("priceNative")),
        "liquidity_usd": to_float(liquidity.get("usd")),
        "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def fetch_eth_spot_usd() -> Dict[str, Any]:
    payload = fetch_json("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd")
    eth = payload.get("ethereum")
    eth = eth if isinstance(eth, dict) else {}
    price_usd = to_float(eth.get("usd"))
    if price_usd is None or price_usd <= 0:
        raise RuntimeError("CoinGecko ETH price missing or invalid.")
    return {
        "source": "coingecko",
        "price_usd": price_usd,
        "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def derive_eth_usd_from_listings(rows: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    implied = []
    for row in rows:
        quote_symbol = str(row.get("listing_quote_symbol") or "").upper()
        if quote_symbol not in ETH_QUOTES:
            continue
        quote_unit = to_float(row.get("listing_price_quote_unit"))
        usd = to_float(row.get("listing_price_usd"))
        if quote_unit is None or quote_unit <= 0 or usd is None or usd <= 0:
            continue
        implied.append(usd / quote_unit)
    if not implied:
        return None
    return {
        "source": "opensea_implied",
        "price_usd": statistics.median(implied),
        "sample_count": len(implied),
        "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def relative_diff_pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return abs(a - b) / b * 100.0


def compute_nova_inside_wei(lock: Dict[str, Any]) -> int:
    amount_wei = int(lock.get("amount_wei") or 0)
    principal_wei = int(lock.get("principal_wei") or 0)
    is_smnft = bool(lock.get("is_smnft"))

    # For smNFT locks, use converted principal from calculate_original_sm_nft_amount().
    if is_smnft:
        return max(principal_wei, 0)
    return max(amount_wei, 0)


def enrich_listing(
    listing_row: Dict[str, Any],
    *,
    rpc_url: str,
    ve_address: str,
    nova_spot_usd: float,
    eth_spot_usd: Optional[float],
    min_nova_tokens_for_valuation: float,
) -> Dict[str, Any]:
    lock_id = int(listing_row["lock_id"])
    lock = query_lock(rpc_url, ve_address, lock_id)
    nova_inside_wei = compute_nova_inside_wei(lock)
    nova_inside_tokens = nova_inside_wei / DECIMALS
    nova_inside_value_usd = nova_inside_tokens * nova_spot_usd if nova_inside_tokens > 0 else None

    listing_quote_symbol = str(listing_row.get("listing_quote_symbol") or "").upper()
    listing_quote_unit = to_float(listing_row.get("listing_price_quote_unit"))
    listing_usd = to_float(listing_row.get("listing_price_usd"))
    listing_usd_effective = listing_usd
    premium_pct = None
    discount_pct = None
    implied_nova_unit_usd = None
    valuation_warnings: List[str] = []
    if nova_inside_tokens <= 0:
        valuation_warnings.append("NO_UNDERLYING_NOVA")
    elif min_nova_tokens_for_valuation > 0 and nova_inside_tokens < min_nova_tokens_for_valuation:
        valuation_warnings.append("UNDERLYING_BELOW_MIN_THRESHOLD")

    if (
        (listing_usd_effective is None or listing_usd_effective <= 0)
        and listing_quote_symbol in ETH_QUOTES
        and listing_quote_unit is not None
        and listing_quote_unit > 0
        and eth_spot_usd is not None
        and eth_spot_usd > 0
    ):
        listing_usd_effective = listing_quote_unit * eth_spot_usd
        valuation_warnings.append("LISTING_USD_DERIVED_FROM_ETH_SPOT")

    has_blocking_warning = any(
        w in {"NO_UNDERLYING_NOVA", "UNDERLYING_BELOW_MIN_THRESHOLD", "MISSING_LISTING_USD"}
        for w in valuation_warnings
    )

    if (
        listing_usd_effective is not None
        and listing_usd_effective > 0
        and nova_inside_value_usd is not None
        and nova_inside_value_usd > 0
        and not has_blocking_warning
    ):
        premium_pct = ((listing_usd_effective / nova_inside_value_usd) - 1.0) * 100.0
        discount_pct = -premium_pct
        implied_nova_unit_usd = listing_usd_effective / nova_inside_tokens
    elif listing_usd_effective is None or listing_usd_effective <= 0:
        valuation_warnings.append("MISSING_LISTING_USD")

    if (
        listing_quote_symbol in ETH_QUOTES
        and listing_quote_unit is not None
        and nova_inside_tokens > 0
    ):
        implied_nova_unit_quote = listing_quote_unit / nova_inside_tokens
    else:
        implied_nova_unit_quote = None

    out = dict(listing_row)
    out.update(
        {
            "lock_amount_wei": lock.get("amount_wei"),
            "lock_amount_veNOVA": lock.get("amount_tokens"),
            "lock_principal_wei": lock.get("principal_wei"),
            "lock_principal_veNOVA": lock.get("principal_tokens"),
            "lock_end": lock.get("end"),
            "lock_is_permanent": lock.get("is_permanent"),
            "lock_is_smnft": lock.get("is_smnft"),
            "valuation_basis": "smnft_principal" if lock.get("is_smnft") else "locked_amount",
            "listing_price_usd_effective": listing_usd_effective,
            "nova_inside_wei": nova_inside_wei,
            "nova_inside_tokens": nova_inside_tokens,
            "nova_inside_value_usd": nova_inside_value_usd,
            "implied_nova_unit_usd": implied_nova_unit_usd,
            "implied_nova_unit_quote": implied_nova_unit_quote,
            "premium_pct_vs_spot": premium_pct,
            "discount_pct_vs_spot": discount_pct,
            "valuation_warnings": valuation_warnings,
            "validation": {
                "has_listing_price_usd": listing_usd_effective is not None and listing_usd_effective > 0,
                "listing_price_usd_was_derived": (
                    "LISTING_USD_DERIVED_FROM_ETH_SPOT" in valuation_warnings
                ),
                "has_nova_inside": nova_inside_tokens > 0,
                "item_name_matches_lock_id": listing_row.get("name_lock_id_match"),
            },
        }
    )
    return out


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    headers = [
        "lock_id",
        "listing_start_time",
        "rank_latest",
        "rank_discount",
        "rank_price_low",
        "rank_price_high",
        "listing_price_quote_unit",
        "listing_quote_symbol",
        "listing_price_usd",
        "listing_price_usd_effective",
        "nova_inside_tokens",
        "nova_inside_value_usd",
        "implied_nova_unit_quote",
        "implied_nova_unit_usd",
        "premium_pct_vs_spot",
        "discount_pct_vs_spot",
        "valuation_basis",
        "valuation_warnings",
        "listing_marketplace",
        "listing_maker",
        "listing_end_time",
        "item_url",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in headers})


def validate_arithmetic(rows: Iterable[Dict[str, Any]]) -> List[str]:
    errors: List[str] = []
    for row in rows:
        lock_id = row.get("lock_id")
        premium = to_float(row.get("premium_pct_vs_spot"))
        discount = to_float(row.get("discount_pct_vs_spot"))
        if premium is not None and discount is not None:
            if abs((premium + discount)) > 1e-9:
                errors.append(
                    f"lock {lock_id}: premium/discount mismatch ({premium} + {discount} != 0)"
                )

        listing_usd = to_float(row.get("listing_price_usd_effective"))
        underlying_usd = to_float(row.get("nova_inside_value_usd"))
        if premium is not None and listing_usd is not None and underlying_usd is not None and underlying_usd > 0:
            reconstructed = (listing_usd / underlying_usd - 1.0) * 100.0
            if abs(reconstructed - premium) > 1e-7:
                errors.append(
                    f"lock {lock_id}: premium reconstruction drift ({premium} vs {reconstructed})"
                )
    return errors


def build_rankings(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    def key_latest(row: Dict[str, Any]):
        start_dt = parse_iso8601_utc(row.get("listing_start_time"))
        if start_dt is None:
            return (1, 0.0, int(row.get("lock_id") or 0))
        return (0, -start_dt.timestamp(), int(row.get("lock_id") or 0))

    def key_discount(row: Dict[str, Any]):
        discount = to_float(row.get("discount_pct_vs_spot"))
        start_dt = parse_iso8601_utc(row.get("listing_start_time"))
        start_key = -start_dt.timestamp() if start_dt is not None else 0.0
        if discount is None:
            return (1, 0.0, 1, start_key, int(row.get("lock_id") or 0))
        return (0, -discount, 0, start_key, int(row.get("lock_id") or 0))

    def key_price_low(row: Dict[str, Any]):
        price = to_float(row.get("listing_price_usd_effective"))
        start_dt = parse_iso8601_utc(row.get("listing_start_time"))
        start_key = -start_dt.timestamp() if start_dt is not None else 0.0
        if price is None:
            return (1, float("inf"), start_key, int(row.get("lock_id") or 0))
        return (0, price, start_key, int(row.get("lock_id") or 0))

    def key_price_high(row: Dict[str, Any]):
        price = to_float(row.get("listing_price_usd_effective"))
        start_dt = parse_iso8601_utc(row.get("listing_start_time"))
        start_key = -start_dt.timestamp() if start_dt is not None else 0.0
        if price is None:
            return (1, 0.0, start_key, int(row.get("lock_id") or 0))
        return (0, -price, start_key, int(row.get("lock_id") or 0))

    latest = sorted(rows, key=key_latest)
    discount = sorted(rows, key=key_discount)
    price_low = sorted(rows, key=key_price_low)
    price_high = sorted(rows, key=key_price_high)

    for idx, row in enumerate(latest, start=1):
        row["rank_latest"] = idx
    for idx, row in enumerate(discount, start=1):
        row["rank_discount"] = idx
    for idx, row in enumerate(price_low, start=1):
        row["rank_price_low"] = idx
    for idx, row in enumerate(price_high, start=1):
        row["rank_price_high"] = idx

    return {
        "latest": latest,
        "discount": discount,
        "price-low": price_low,
        "price-high": price_high,
    }


def main() -> int:
    args = parse_args()
    collection_url = f"https://opensea.io/collection/{args.collection_slug}"
    activity_url = f"{collection_url}/activity"

    try:
        nova_spot = fetch_nova_spot(args.nova_token)
        nova_price_usd = float(nova_spot["price_usd"])

        collection_html = fetch_text(collection_url)
        collection_payloads = extract_urql_payloads(collection_html)
        listings = parse_collection_listings(collection_payloads, collection_slug=args.collection_slug)
        if not listings:
            raise RuntimeError("No listings parsed from OpenSea collection payload.")
        if args.max_listings > 0:
            listings = listings[: args.max_listings]

        implied_eth = derive_eth_usd_from_listings(listings)
        coingecko_eth = None
        coingecko_eth_error = None
        try:
            coingecko_eth = fetch_eth_spot_usd()
        except Exception as exc:
            coingecko_eth_error = str(exc)

        eth_override = to_float(args.eth_price_usd)
        if eth_override is not None and eth_override <= 0:
            raise RuntimeError(f"Invalid --eth-price-usd value: {args.eth_price_usd}")

        eth_spot_usd = None
        eth_price_source = "unavailable"
        if eth_override is not None:
            eth_spot_usd = eth_override
            eth_price_source = "manual_override"
        elif coingecko_eth is not None:
            eth_spot_usd = to_float(coingecko_eth.get("price_usd"))
            eth_price_source = "coingecko"
        elif implied_eth is not None:
            eth_spot_usd = to_float(implied_eth.get("price_usd"))
            eth_price_source = "opensea_implied"

        eth_price_diff_pct = relative_diff_pct(
            to_float(coingecko_eth.get("price_usd")) if coingecko_eth else None,
            to_float(implied_eth.get("price_usd")) if implied_eth else None,
        )

        if args.nova_price_only:
            print(
                json.dumps(
                    {
                        "nova_spot": nova_spot,
                        "eth_spot_used": {
                            "source": eth_price_source,
                            "price_usd": eth_spot_usd,
                            "coingecko_error": coingecko_eth_error,
                            "coingecko": coingecko_eth,
                            "opensea_implied": implied_eth,
                            "coingecko_vs_opensea_diff_pct": eth_price_diff_pct,
                        },
                    },
                    indent=2,
                )
            )
            return 0

        activity_rows: List[Dict[str, Any]] = []
        if not args.skip_activity:
            activity_html = fetch_text(activity_url)
            activity_payloads = extract_urql_payloads(activity_html)
            activity_rows = parse_collection_activity(activity_payloads)

        workers = max(1, int(args.workers))
        enriched: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=min(workers, len(listings))) as pool:
            futures = {
                pool.submit(
                    enrich_listing,
                    row,
                    rpc_url=args.rpc_url,
                    ve_address=args.ve_address,
                    nova_spot_usd=nova_price_usd,
                    eth_spot_usd=eth_spot_usd,
                    min_nova_tokens_for_valuation=max(0.0, float(args.min_nova_tokens)),
                ): row["lock_id"]
                for row in listings
            }
            for fut in as_completed(futures):
                lock_id = futures[fut]
                try:
                    enriched.append(fut.result())
                except Exception as exc:
                    errors.append({"lock_id": lock_id, "error": str(exc)})

        rankings = build_rankings(enriched)
        output_listings = rankings[args.sort_by]
        valid_discount_rows = [
            r for r in rankings["discount"] if to_float(r.get("discount_pct_vs_spot")) is not None
        ]
        positive_discount_rows = [
            r for r in valid_discount_rows if (to_float(r.get("discount_pct_vs_spot")) or 0) > 0
        ]
        avg_premium = None
        if valid_discount_rows:
            avg_premium = sum(to_float(r["premium_pct_vs_spot"]) or 0 for r in valid_discount_rows) / len(
                valid_discount_rows
            )
        warning_rows = [r for r in output_listings if r.get("valuation_warnings")]
        listing_usd_derived_rows = [
            r
            for r in output_listings
            if isinstance(r.get("validation"), dict) and r["validation"].get("listing_price_usd_was_derived")
        ]
        validation_errors = validate_arithmetic(output_listings)
        latest_head = rankings["latest"][0] if rankings["latest"] else None

        summary = {
            "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "collection_slug": args.collection_slug,
            "collection_url": collection_url,
            "activity_url": activity_url,
            "rpc_url": args.rpc_url,
            "ve_address": args.ve_address,
            "nova_token": args.nova_token,
            "nova_spot": nova_spot,
            "eth_spot_used": {
                "source": eth_price_source,
                "price_usd": eth_spot_usd,
                "coingecko": coingecko_eth,
                "opensea_implied": implied_eth,
                "coingecko_error": coingecko_eth_error,
                "coingecko_vs_opensea_diff_pct": eth_price_diff_pct,
            },
            "listing_rows_parsed": len(listings),
            "listing_rows_enriched": len(output_listings),
            "listing_rows_failed": len(errors),
            "listings_sorted_by": args.sort_by,
            "premium_rows": len(valid_discount_rows),
            "discount_rows_positive": len(positive_discount_rows),
            "rows_with_valuation_warnings": len(warning_rows),
            "rows_with_listing_usd_derived": len(listing_usd_derived_rows),
            "avg_premium_pct": avg_premium,
            "arithmetic_validation_errors": len(validation_errors),
            "latest_listing": {
                "lock_id": latest_head.get("lock_id") if latest_head else None,
                "listing_start_time": latest_head.get("listing_start_time") if latest_head else None,
                "listing_price_quote_unit": latest_head.get("listing_price_quote_unit") if latest_head else None,
                "listing_quote_symbol": latest_head.get("listing_quote_symbol") if latest_head else None,
            },
            "activity_rows": len(activity_rows),
            "activity_type_counts": {},
        }

        if activity_rows:
            counts: Dict[str, int] = {}
            for row in activity_rows:
                t = str(row.get("event_type") or "UNKNOWN")
                counts[t] = counts.get(t, 0) + 1
            summary["activity_type_counts"] = counts

        report = {
            "summary": summary,
            "listings": output_listings,
            "rankings": {
                "latest_lock_ids": [r.get("lock_id") for r in rankings["latest"]],
                "discount_lock_ids": [r.get("lock_id") for r in rankings["discount"]],
                "price_low_lock_ids": [r.get("lock_id") for r in rankings["price-low"]],
                "price_high_lock_ids": [r.get("lock_id") for r in rankings["price-high"]],
            },
            "listing_errors": errors,
            "activity": activity_rows,
            "arithmetic_validation": validation_errors,
        }

        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
            f.write("\n")
        write_csv(enriched, args.out_csv)

        print("veNOVA OpenSea discount scan complete")
        print(f"- collection: {collection_url}")
        print(
            f"- listings parsed: {len(listings)} | enriched: {len(output_listings)} | "
            f"failed: {len(errors)} | sorted_by: {args.sort_by}"
        )
        if latest_head is not None:
            print(
                f"- latest listing: lock #{latest_head.get('lock_id')} "
                f"(start={latest_head.get('listing_start_time')}, "
                f"price={latest_head.get('listing_price_quote_unit')} {latest_head.get('listing_quote_symbol')})"
            )
        print(
            f"- NOVA spot: ${nova_price_usd:.8f} "
            f"(dex={nova_spot.get('dex_id')}, pair={nova_spot.get('pair_address')})"
        )
        print(
            f"- ETH spot used: {('n/a' if eth_spot_usd is None else f'${eth_spot_usd:.8f}')} "
            f"(source={eth_price_source})"
        )
        if eth_price_diff_pct is not None:
            print(f"- ETH price cross-check (CoinGecko vs OpenSea implied): {eth_price_diff_pct:.4f}%")
        if coingecko_eth_error:
            print(f"- ETH external source warning: {coingecko_eth_error}")
        if positive_discount_rows:
            best = positive_discount_rows[0]
            print(
                f"- best discount row: lock #{best.get('lock_id')} "
                f"{best.get('discount_pct_vs_spot'):.2f}% "
                f"(listing ${best.get('listing_price_usd_effective'):.6f}, underlying ${best.get('nova_inside_value_usd'):.6f})"
            )
        elif valid_discount_rows:
            best = valid_discount_rows[0]
            print(
                f"- best available row (no positive discounts): lock #{best.get('lock_id')} "
                f"{best.get('discount_pct_vs_spot'):.2f}%"
            )
        print(f"- rows using derived listing USD: {len(listing_usd_derived_rows)}")
        print(f"- arithmetic validation errors: {len(validation_errors)}")
        if args.strict_validate and validation_errors:
            raise RuntimeError(
                f"Strict validation failed with {len(validation_errors)} arithmetic errors. "
                f"See arithmetic_validation in {args.out_json}."
            )
        print(f"- json: {args.out_json}")
        print(f"- csv: {args.out_csv}")
        return 0
    except Exception as exc:
        print(f"Failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
