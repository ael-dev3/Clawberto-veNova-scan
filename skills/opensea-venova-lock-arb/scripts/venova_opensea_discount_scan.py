#!/usr/bin/env python3
"""
Scan veNOVA lock listings and compute premium/discount versus NOVA spot price.

Data sources:
- OpenSea GraphQL collection listings (paginated, newest-first)
- OpenSea activity page (embedded urql payload: collectionActivity)
- ve.exchange listings endpoint
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
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


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
DEFAULT_VE_EXCHANGE_API_BASE = "https://api.ve.exchange"
DEFAULT_TIMEOUT_SEC = 30
USER_AGENT = "Mozilla/5.0 (compatible; OpenClaw-veNOVA-scan/1.0)"
ETH_QUOTES = {"ETH", "WETH"}
OPENSEA_GQL_URL = "https://gql.opensea.io/graphql"
OPENSEA_GQL_PAGE_LIMIT = 50
MAX_HTTP_RESPONSE_BYTES = 12 * 1024 * 1024

ALLOWED_HTTP_HOSTS = {
    "opensea.io",
    "gql.opensea.io",
    "api.dexscreener.com",
    "api.coingecko.com",
    "api.ve.exchange",
    "ve.exchange",
}

SLUG_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,127}$")
ETH_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

URQL_PUSH_RE = re.compile(
    r'\(window\[Symbol\.for\("urql_transport"\)\] \?\?= \[\]\)\.push\((\{.*?\})\)</script>',
    re.S,
)

OPENSEA_COLLECTION_ITEMS_QUERY = """
query CollectionItemsListQuery(
  $collectionSlug: String!,
  $cursor: String,
  $limit: Int!,
  $sort: CollectionItemsSort!,
  $filter: CollectionItemsFilter
) {
  collectionItems(
    collectionSlug: $collectionSlug,
    cursor: $cursor,
    limit: $limit,
    sort: $sort,
    filter: $filter
  ) {
    items {
      tokenId
      name
      contractAddress
      bestListing {
        id
        startTime
        endTime
        quantityRemaining
        marketplace {
          identifier
        }
        maker {
          address
        }
        pricePerItem {
          usd
          token {
            unit
            symbol
          }
        }
      }
    }
    nextPageCursor
  }
}
""".strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch veNOVA lock listings (OpenSea + optional ve.exchange), "
            "enrich with on-chain lock NOVA amounts, and compute premium/discount versus NOVA spot."
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
        help="Optional max listing rows to analyze after latest-ordering (0 = all parsed listings).",
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
        "--skip-ve-exchange",
        action="store_true",
        help="Skip ve.exchange listing fetch.",
    )
    parser.add_argument(
        "--ve-exchange-chain-id",
        type=int,
        default=1,
        help="Chain ID for ve.exchange listing endpoint (default: 1).",
    )
    parser.add_argument(
        "--ve-exchange-ve-address",
        default=DEFAULT_VE_ADDRESS,
        help=f"veNFT collection address for ve.exchange endpoint (default: {DEFAULT_VE_ADDRESS}).",
    )
    parser.add_argument(
        "--ve-exchange-api-base",
        default=DEFAULT_VE_EXCHANGE_API_BASE,
        help=f"ve.exchange API base URL (default: {DEFAULT_VE_EXCHANGE_API_BASE}).",
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


def must_https_url(
    url: str,
    *,
    allowed_hosts: Optional[set[str]] = None,
) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"Only https URLs are allowed, got: {url}")
    host = (parsed.hostname or "").lower()
    if not host:
        raise ValueError(f"Missing hostname in URL: {url}")
    if allowed_hosts is not None and host not in allowed_hosts:
        raise ValueError(f"Blocked host '{host}' for URL: {url}")
    return url


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


def fetch_text(
    url: str,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    *,
    allowed_hosts: Optional[set[str]] = None,
) -> str:
    req = urllib.request.Request(
        must_https_url(url, allowed_hosts=allowed_hosts),
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as response:
            raw = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
    except urllib.error.URLError as exc:
        raise RuntimeError(f"HTTP request failed for {url}: {exc}") from exc
    if len(raw) > MAX_HTTP_RESPONSE_BYTES:
        raise RuntimeError(f"HTTP response too large from {url} (> {MAX_HTTP_RESPONSE_BYTES} bytes)")
    return raw.decode("utf-8", errors="replace")


def fetch_json(
    url: str,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    *,
    allowed_hosts: Optional[set[str]] = None,
) -> Dict[str, Any]:
    raw = fetch_text(url, timeout_sec=timeout_sec, allowed_hosts=allowed_hosts)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from {url}: {exc}") from exc


def post_json(
    url: str,
    payload: Dict[str, Any],
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    *,
    allowed_hosts: Optional[set[str]] = None,
) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        must_https_url(url, allowed_hosts=allowed_hosts),
        data=body,
        method="POST",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as response:
            raw = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
    except urllib.error.URLError as exc:
        raise RuntimeError(f"HTTP POST failed for {url}: {exc}") from exc
    if len(raw) > MAX_HTTP_RESPONSE_BYTES:
        raise RuntimeError(f"HTTP response too large from {url} (> {MAX_HTTP_RESPONSE_BYTES} bytes)")
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from {url}: {exc}") from exc


def parse_epoch_seconds_to_iso8601_utc(value: Any) -> Optional[str]:
    num = to_float(value)
    if num is None or num <= 0:
        return None
    try:
        dt_obj = dt.datetime.fromtimestamp(num, tz=dt.timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return dt_obj.isoformat().replace("+00:00", "Z")


def parse_numeric_string(value: Any) -> Optional[int]:
    s = str(value or "").strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    return None


def listing_latest_sort_key(row: Dict[str, Any]) -> Tuple[int, float, int, float, str, int]:
    start_dt = parse_iso8601_utc(row.get("listing_start_time"))
    ts_sort = -start_dt.timestamp() if start_dt is not None else 0.0
    has_start = 0 if start_dt is not None else 1

    source_hint = to_float(row.get("listing_source_order_hint"))
    has_hint = 0 if source_hint is not None else 1
    hint_sort = -(source_hint or 0.0)

    return (
        has_start,
        ts_sort,
        has_hint,
        hint_sort,
        str(row.get("listing_source") or ""),
        int(row.get("lock_id") or 0),
    )


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


def parse_opensea_collection_items(
    items: Iterable[Dict[str, Any]],
    *,
    collection_slug: str,
) -> List[Dict[str, Any]]:
    by_key: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        token_id_raw = str(item.get("tokenId") or "").strip()
        lock_id = parse_numeric_string(token_id_raw)
        if lock_id is None:
            continue
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
        listing_id = str(best_listing.get("id") or "") or None

        item_name = str(item.get("name") or "")
        lock_in_name = None
        m = re.search(r"lock\s*#\s*(\d+)", item_name, flags=re.IGNORECASE)
        if m:
            lock_in_name = int(m.group(1))

        contract_address = str(item.get("contractAddress") or "")
        row = {
            "listing_source": "opensea",
            "lock_id": lock_id,
            "item_name": item_name,
            "item_contract": contract_address,
            "item_token_id": token_id_raw,
            "item_url": f"https://opensea.io/item/ethereum/{contract_address}/{token_id_raw}",
            "listing_id": listing_id,
            "listing_native_id": listing_id,
            "listing_source_order_hint": None,
            "listing_price_quote_unit": listing_quote,
            "listing_quote_symbol": listing_symbol,
            "listing_price_usd": listing_usd,
            "listing_marketplace": market.get("identifier"),
            "listing_start_time": best_listing.get("startTime"),
            "listing_end_time": best_listing.get("endTime"),
            "listing_timestamp_quality": ("exact" if best_listing.get("startTime") else "missing"),
            "listing_quantity_remaining": best_listing.get("quantityRemaining"),
            "listing_maker": (
                (best_listing.get("maker") or {}).get("address")
                if isinstance(best_listing.get("maker"), dict)
                else None
            ),
            "collection_slug": collection_slug,
            "name_lock_id_match": (lock_in_name == lock_id) if lock_in_name is not None else None,
        }
        key = ("opensea", lock_id)
        prev = by_key.get(key)
        if prev is None or should_replace_listing(prev, row):
            by_key[key] = row

    rows = list(by_key.values())
    rows.sort(key=listing_latest_sort_key)
    return rows


def fetch_opensea_collection_listings(
    *,
    collection_slug: str,
    page_limit: int = OPENSEA_GQL_PAGE_LIMIT,
    max_rows_hint: int = 0,
) -> List[Dict[str, Any]]:
    if page_limit <= 0 or page_limit > 100:
        raise ValueError(f"Invalid OpenSea page limit: {page_limit}")

    all_items: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page_count = 0
    while True:
        payload = {
            "operationName": "CollectionItemsListQuery",
            "query": OPENSEA_COLLECTION_ITEMS_QUERY,
            "variables": {
                "collectionSlug": collection_slug,
                "cursor": cursor,
                "limit": page_limit,
                "sort": {"by": "LISTING_CREATED_DATE", "direction": "DESC"},
                "filter": {"isListed": True},
            },
        }
        response = post_json(
            OPENSEA_GQL_URL,
            payload,
            allowed_hosts=ALLOWED_HTTP_HOSTS,
        )
        errors = response.get("errors")
        if isinstance(errors, list) and errors:
            raise RuntimeError(f"OpenSea GraphQL returned errors: {errors[0]}")

        data = response.get("data")
        data = data if isinstance(data, dict) else {}
        collection_items = data.get("collectionItems")
        collection_items = collection_items if isinstance(collection_items, dict) else {}
        items = collection_items.get("items")
        if not isinstance(items, list):
            break

        all_items.extend(item for item in items if isinstance(item, dict))
        if max_rows_hint > 0 and len(all_items) >= max_rows_hint:
            break
        page_count += 1
        next_cursor = collection_items.get("nextPageCursor")
        next_cursor = str(next_cursor) if next_cursor else None
        if not next_cursor:
            break
        if next_cursor == cursor:
            break
        cursor = next_cursor
        if page_count >= 60:
            # Safety stop to avoid infinite loops if pagination regresses upstream.
            break

    return parse_opensea_collection_items(all_items, collection_slug=collection_slug)


def parse_ve_exchange_listings(
    rows: Iterable[Dict[str, Any]],
    *,
    ve_collection_address: str,
) -> List[Dict[str, Any]]:
    by_key: Dict[Tuple[str, int], Dict[str, Any]] = {}
    normalized_address = ve_collection_address.lower()

    for item in rows:
        if not isinstance(item, dict):
            continue
        lock_id = parse_numeric_string(item.get("tokenId"))
        if lock_id is None:
            continue
        listing_id = str(item.get("listingID") or item.get("listingId") or "").strip() or None
        listing_hint = parse_numeric_string(listing_id)
        start_time = parse_epoch_seconds_to_iso8601_utc(item.get("startDate"))
        end_time = parse_epoch_seconds_to_iso8601_utc(item.get("endDate"))
        quote_symbol = str(item.get("fundingTokenSymbol") or "").upper() or None
        row = {
            "listing_source": "ve.exchange",
            "lock_id": lock_id,
            "item_name": f"lock #{lock_id}",
            "item_contract": str(item.get("nft") or normalized_address),
            "item_token_id": str(lock_id),
            "item_url": f"https://ve.exchange/collection/{normalized_address}/{lock_id}",
            "listing_id": listing_id,
            "listing_native_id": listing_id,
            "listing_source_order_hint": listing_hint,
            "listing_price_quote_unit": to_float(item.get("priceDecimalised")),
            "listing_quote_symbol": quote_symbol,
            "listing_price_usd": to_float(item.get("priceUsd")),
            "listing_marketplace": "ve.exchange",
            "listing_start_time": start_time,
            "listing_end_time": end_time,
            "listing_timestamp_quality": ("exact" if start_time else ("id_fallback" if listing_hint is not None else "missing")),
            "listing_quantity_remaining": 1,
            "listing_maker": item.get("seller"),
            "collection_slug": None,
            "name_lock_id_match": True,
            "ve_exchange_is_latest_listing": bool(item.get("isLatestListing")),
            "ve_exchange_listing_valid": bool(item.get("isValid")),
            "ve_exchange_listing_withdrawn": bool(item.get("isWithdrawn")),
            "ve_exchange_last_validation_update": parse_epoch_seconds_to_iso8601_utc(
                item.get("lastValidationUpdate")
            ),
        }
        key = ("ve.exchange", lock_id)
        prev = by_key.get(key)
        if prev is None or should_replace_listing(prev, row):
            by_key[key] = row

    out = list(by_key.values())
    out.sort(key=listing_latest_sort_key)
    return out


def fetch_ve_exchange_listings(
    *,
    chain_id: int,
    ve_collection_address: str,
    api_base: str,
) -> List[Dict[str, Any]]:
    base = api_base.rstrip("/")
    must_https_url(base, allowed_hosts=ALLOWED_HTTP_HOSTS)
    url = (
        f"{base}/tracking/subgraphs/listings/{urllib.parse.quote(str(chain_id))}/"
        f"{urllib.parse.quote(ve_collection_address.lower())}?ts={int(time.time())}"
    )
    payload = fetch_json(url, allowed_hosts=ALLOWED_HTTP_HOSTS)
    data = payload.get("data")
    data = data if isinstance(data, dict) else {}
    rows = data.get("nftListeds")
    if not isinstance(rows, list):
        rows = []
    return parse_ve_exchange_listings(rows, ve_collection_address=ve_collection_address)


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
    payload = fetch_json(url, allowed_hosts=ALLOWED_HTTP_HOSTS)
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
    payload = fetch_json(
        "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
        allowed_hosts=ALLOWED_HTTP_HOSTS,
    )
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
        "source": "listing_implied",
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
    lock: Dict[str, Any],
    nova_spot_usd: float,
    eth_spot_usd: Optional[float],
    min_nova_tokens_for_valuation: float,
) -> Dict[str, Any]:
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


def fetch_locks_by_id(
    lock_ids: Iterable[int],
    *,
    rpc_url: str,
    ve_address: str,
    workers: int,
) -> Tuple[Dict[int, Dict[str, Any]], List[Dict[str, Any]]]:
    unique_ids = sorted({int(x) for x in lock_ids if int(x) >= 0})
    if not unique_ids:
        return {}, []
    lock_map: Dict[int, Dict[str, Any]] = {}
    errors: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(max(1, workers), len(unique_ids))) as pool:
        futures = {pool.submit(query_lock, rpc_url, ve_address, lock_id): lock_id for lock_id in unique_ids}
        for fut in as_completed(futures):
            lock_id = futures[fut]
            try:
                lock_map[lock_id] = fut.result()
            except Exception as exc:
                errors.append({"lock_id": lock_id, "error": str(exc)})
    return lock_map, errors


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    headers = [
        "listing_source",
        "lock_id",
        "listing_id",
        "listing_native_id",
        "listing_start_time",
        "listing_timestamp_quality",
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
        return listing_latest_sort_key(row)

    def key_discount(row: Dict[str, Any]):
        discount = to_float(row.get("discount_pct_vs_spot"))
        latest_key = listing_latest_sort_key(row)
        if discount is None:
            return (1, 0.0, latest_key)
        return (0, -discount, latest_key)

    def key_price_low(row: Dict[str, Any]):
        price = to_float(row.get("listing_price_usd_effective"))
        latest_key = listing_latest_sort_key(row)
        if price is None:
            return (1, float("inf"), latest_key)
        return (0, price, latest_key)

    def key_price_high(row: Dict[str, Any]):
        price = to_float(row.get("listing_price_usd_effective"))
        latest_key = listing_latest_sort_key(row)
        if price is None:
            return (1, 0.0, latest_key)
        return (0, -price, latest_key)

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


def ensure_output_parent(path: Path) -> None:
    parent = path.parent
    if str(parent) and str(parent) != ".":
        parent.mkdir(parents=True, exist_ok=True)


def ranking_compact(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "listing_source": row.get("listing_source"),
            "lock_id": row.get("lock_id"),
            "listing_id": row.get("listing_id"),
            "listing_start_time": row.get("listing_start_time"),
            "listing_timestamp_quality": row.get("listing_timestamp_quality"),
            "discount_pct_vs_spot": row.get("discount_pct_vs_spot"),
        }
        for row in rows
    ]


def main() -> int:
    args = parse_args()
    collection_url = f"https://opensea.io/collection/{args.collection_slug}"
    activity_url = f"{collection_url}/activity"

    try:
        if not SLUG_RE.fullmatch(args.collection_slug):
            raise RuntimeError(f"Invalid --collection-slug: {args.collection_slug}")
        if not ETH_ADDRESS_RE.fullmatch(args.ve_address):
            raise RuntimeError(f"Invalid --ve-address: {args.ve_address}")
        if not ETH_ADDRESS_RE.fullmatch(args.ve_exchange_ve_address):
            raise RuntimeError(f"Invalid --ve-exchange-ve-address: {args.ve_exchange_ve_address}")
        if args.max_listings < 0:
            raise RuntimeError(f"Invalid --max-listings: {args.max_listings}")
        if args.workers < 1 or args.workers > 64:
            raise RuntimeError(f"Invalid --workers (expected 1..64): {args.workers}")
        if args.ve_exchange_chain_id <= 0:
            raise RuntimeError(f"Invalid --ve-exchange-chain-id: {args.ve_exchange_chain_id}")

        nova_spot = fetch_nova_spot(args.nova_token)
        nova_price_usd = float(nova_spot["price_usd"])

        coingecko_eth = None
        coingecko_eth_error = None
        try:
            coingecko_eth = fetch_eth_spot_usd()
        except Exception as exc:
            coingecko_eth_error = str(exc)

        eth_override = to_float(args.eth_price_usd)
        if eth_override is not None and eth_override <= 0:
            raise RuntimeError(f"Invalid --eth-price-usd value: {args.eth_price_usd}")

        if args.nova_price_only:
            eth_spot_usd = None
            eth_price_source = "unavailable"
            if eth_override is not None:
                eth_spot_usd = eth_override
                eth_price_source = "manual_override"
            elif coingecko_eth is not None:
                eth_spot_usd = to_float(coingecko_eth.get("price_usd"))
                eth_price_source = "coingecko"
            print(
                json.dumps(
                    {
                        "nova_spot": nova_spot,
                        "listing_sources": {
                            "opensea_rows": None,
                            "ve_exchange_rows": None,
                        },
                        "eth_spot_used": {
                            "source": eth_price_source,
                            "price_usd": eth_spot_usd,
                            "coingecko_error": coingecko_eth_error,
                            "coingecko": coingecko_eth,
                            "listing_implied": None,
                            "coingecko_vs_opensea_diff_pct": None,
                        },
                    },
                    indent=2,
                )
            )
            return 0

        opensea_rows = fetch_opensea_collection_listings(
            collection_slug=args.collection_slug,
            max_rows_hint=(args.max_listings * 4 if args.max_listings > 0 else 0),
        )
        if not opensea_rows:
            raise RuntimeError("No listings parsed from OpenSea GraphQL.")
        ve_exchange_rows: List[Dict[str, Any]] = []
        if not args.skip_ve_exchange:
            ve_exchange_rows = fetch_ve_exchange_listings(
                chain_id=args.ve_exchange_chain_id,
                ve_collection_address=args.ve_exchange_ve_address,
                api_base=args.ve_exchange_api_base,
            )

        listings = list(opensea_rows) + list(ve_exchange_rows)
        listings.sort(key=listing_latest_sort_key)
        if not listings:
            raise RuntimeError("No listings parsed from any source.")
        latest_by_source_parsed: Dict[str, Dict[str, Any]] = {}
        for row in listings:
            source = str(row.get("listing_source") or "unknown")
            if source in latest_by_source_parsed:
                continue
            latest_by_source_parsed[source] = {
                "lock_id": row.get("lock_id"),
                "listing_id": row.get("listing_id"),
                "listing_start_time": row.get("listing_start_time"),
                "listing_timestamp_quality": row.get("listing_timestamp_quality"),
                "listing_price_quote_unit": row.get("listing_price_quote_unit"),
                "listing_quote_symbol": row.get("listing_quote_symbol"),
            }
        if args.max_listings > 0:
            listings = listings[: args.max_listings]

        implied_eth = derive_eth_usd_from_listings(listings)

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
            eth_price_source = "listing_implied"

        eth_price_diff_pct = relative_diff_pct(
            to_float(coingecko_eth.get("price_usd")) if coingecko_eth else None,
            to_float(implied_eth.get("price_usd")) if implied_eth else None,
        )

        activity_rows: List[Dict[str, Any]] = []
        if not args.skip_activity:
            activity_html = fetch_text(activity_url, allowed_hosts=ALLOWED_HTTP_HOSTS)
            activity_payloads = extract_urql_payloads(activity_html)
            activity_rows = parse_collection_activity(activity_payloads)

        workers = max(1, int(args.workers))
        enriched: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        lock_map, lock_errors = fetch_locks_by_id(
            [int(row["lock_id"]) for row in listings if parse_numeric_string(row.get("lock_id")) is not None],
            rpc_url=args.rpc_url,
            ve_address=args.ve_address,
            workers=workers,
        )
        errors.extend(lock_errors)

        for row in listings:
            lock_id = int(row["lock_id"])
            lock = lock_map.get(lock_id)
            if lock is None:
                errors.append({"lock_id": lock_id, "error": "lock_fetch_failed"})
                continue
            try:
                enriched.append(
                    enrich_listing(
                        row,
                        lock=lock,
                        nova_spot_usd=nova_price_usd,
                        eth_spot_usd=eth_spot_usd,
                        min_nova_tokens_for_valuation=max(0.0, float(args.min_nova_tokens)),
                    )
                )
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
        source_counts: Dict[str, int] = {}
        for row in output_listings:
            source = str(row.get("listing_source") or "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1

        latest_by_source: Dict[str, Dict[str, Any]] = {}
        for row in rankings["latest"]:
            source = str(row.get("listing_source") or "unknown")
            if source in latest_by_source:
                continue
            latest_by_source[source] = {
                "lock_id": row.get("lock_id"),
                "listing_id": row.get("listing_id"),
                "listing_start_time": row.get("listing_start_time"),
                "listing_timestamp_quality": row.get("listing_timestamp_quality"),
                "listing_price_quote_unit": row.get("listing_price_quote_unit"),
                "listing_quote_symbol": row.get("listing_quote_symbol"),
            }

        json_path = Path(args.out_json).expanduser()
        if not json_path.is_absolute():
            json_path = Path.cwd() / json_path
        csv_path = Path(args.out_csv).expanduser()
        if not csv_path.is_absolute():
            csv_path = Path.cwd() / csv_path
        ensure_output_parent(json_path)
        ensure_output_parent(csv_path)

        summary = {
            "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "collection_slug": args.collection_slug,
            "collection_url": collection_url,
            "activity_url": activity_url,
            "rpc_url": args.rpc_url,
            "ve_address": args.ve_address,
            "ve_exchange": {
                "enabled": not args.skip_ve_exchange,
                "api_base": args.ve_exchange_api_base,
                "chain_id": args.ve_exchange_chain_id,
                "ve_address": args.ve_exchange_ve_address,
            },
            "nova_token": args.nova_token,
            "nova_spot": nova_spot,
            "eth_spot_used": {
                "source": eth_price_source,
                "price_usd": eth_spot_usd,
                "coingecko": coingecko_eth,
                "listing_implied": implied_eth,
                "coingecko_error": coingecko_eth_error,
                "coingecko_vs_opensea_diff_pct": eth_price_diff_pct,
            },
            "listing_rows_source": {
                "opensea": len(opensea_rows),
                "ve.exchange": len(ve_exchange_rows),
            },
            "listing_rows_parsed": len(opensea_rows) + len(ve_exchange_rows),
            "listing_rows_considered": len(listings),
            "listing_rows_enriched": len(output_listings),
            "listing_rows_failed": len(errors),
            "listing_source_counts_output": source_counts,
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
                "listing_source": latest_head.get("listing_source") if latest_head else None,
            },
            "latest_listing_by_source": latest_by_source,
            "latest_listing_by_source_parsed": latest_by_source_parsed,
            "activity_rows": len(activity_rows),
            "activity_type_counts": {},
            "output_paths": {
                "json": str(json_path),
                "csv": str(csv_path),
            },
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
                "latest": ranking_compact(rankings["latest"]),
                "discount": ranking_compact(rankings["discount"]),
                "price_low": ranking_compact(rankings["price-low"]),
                "price_high": ranking_compact(rankings["price-high"]),
            },
            "listing_errors": errors,
            "activity": activity_rows,
            "arithmetic_validation": validation_errors,
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
            f.write("\n")
        write_csv(output_listings, str(csv_path))

        print("veNOVA listing discount scan complete")
        print(f"- collection: {collection_url}")
        print(
            f"- listings parsed total: {len(opensea_rows) + len(ve_exchange_rows)} "
            f"(opensea={len(opensea_rows)}, ve.exchange={len(ve_exchange_rows)})"
        )
        print(
            f"- listings considered: {len(listings)} | enriched: {len(output_listings)} | "
            f"failed: {len(errors)} | sorted_by: {args.sort_by}"
        )
        if latest_head is not None:
            print(
                f"- latest listing: lock #{latest_head.get('lock_id')} "
                f"(start={latest_head.get('listing_start_time')}, "
                f"price={latest_head.get('listing_price_quote_unit')} {latest_head.get('listing_quote_symbol')}, "
                f"source={latest_head.get('listing_source')})"
            )
        if latest_by_source_parsed:
            print("- latest by source (parsed):")
            for source, payload in sorted(latest_by_source_parsed.items()):
                print(
                    f"  - {source}: lock #{payload.get('lock_id')} "
                    f"(start={payload.get('listing_start_time')}, quality={payload.get('listing_timestamp_quality')})"
                )
        if latest_by_source and latest_by_source != latest_by_source_parsed:
            print("- latest by source (output window):")
            for source, payload in sorted(latest_by_source.items()):
                print(
                    f"  - {source}: lock #{payload.get('lock_id')} "
                    f"(start={payload.get('listing_start_time')}, quality={payload.get('listing_timestamp_quality')})"
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
                f"- best discount row: {best.get('listing_source')} lock #{best.get('lock_id')} "
                f"{best.get('discount_pct_vs_spot'):.2f}% "
                f"(listing ${best.get('listing_price_usd_effective'):.6f}, underlying ${best.get('nova_inside_value_usd'):.6f})"
            )
        elif valid_discount_rows:
            best = valid_discount_rows[0]
            print(
                f"- best available row (no positive discounts): "
                f"{best.get('listing_source')} lock #{best.get('lock_id')} {best.get('discount_pct_vs_spot'):.2f}%"
            )
        print(f"- rows using derived listing USD: {len(listing_usd_derived_rows)}")
        print(f"- arithmetic validation errors: {len(validation_errors)}")
        if args.strict_validate and (validation_errors or errors):
            raise RuntimeError(
                f"Strict validation failed (arithmetic_errors={len(validation_errors)}, "
                f"listing_errors={len(errors)}). See report: {json_path}"
            )
        print(f"- json: {json_path}")
        print(f"- csv: {csv_path}")
        return 0
    except Exception as exc:
        print(f"Failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
