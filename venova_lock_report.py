#!/usr/bin/env python3
"""
Generate a text report of veNOVA amounts for a list of lock IDs.

Usage examples:
  python venova_lock_report.py 590 1319 190
  python venova_lock_report.py 590,1319,190 --out locks.txt
  python venova_lock_report.py --locks-file lock_ids.txt
  python venova_lock_report.py 73 907 --rpc-url https://ethereum-rpc.publicnode.com
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import urllib.error
import urllib.request
from typing import Iterable, List


DEFAULT_RPC_URL = "https://ethereum-rpc.publicnode.com"
DEFAULT_VE_ADDRESS = "0x4c3e7640b3e3a39a2e5d030a0c1412d80fee1d44"
DECIMALS = 10**18

# Precomputed function selectors:
# locked(uint256) -> 0xb45a3c0e
# calculate_original_sm_nft_amount(uint256) -> 0x09b2b405
SEL_LOCKED = "b45a3c0e"
SEL_CALC_ORIGINAL_SM = "09b2b405"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query veNOVA lock amounts from Ethereum mainnet.")
    parser.add_argument(
        "locks",
        nargs="*",
        help="Lock IDs (supports space-separated and/or comma-separated values).",
    )
    parser.add_argument(
        "--locks-file",
        help="Path to a text file containing lock IDs (comma, space, or newline separated).",
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
        "--out",
        default="venova_locks_report.txt",
        help="Output .txt file path (default: venova_locks_report.txt).",
    )
    return parser.parse_args()


def parse_lock_ids(raw_values: Iterable[str]) -> List[int]:
    ids: List[int] = []
    for raw in raw_values:
        for match in re.findall(r"\d+", raw):
            value = int(match)
            if value < 0:
                raise ValueError(f"Lock id must be non-negative: {value}")
            ids.append(value)
    return ids


def u256_hex(value: int) -> str:
    return hex(value)[2:].rjust(64, "0")


def wei_to_token_str(amount_wei: int) -> str:
    sign = "-" if amount_wei < 0 else ""
    value = abs(amount_wei)
    whole, frac = divmod(value, DECIMALS)
    if frac == 0:
        return f"{sign}{whole}"
    frac_str = str(frac).rjust(18, "0").rstrip("0")
    return f"{sign}{whole}.{frac_str}"


def rpc_call(rpc_url: str, method: str, params: list) -> dict:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "veNOVA-lock-report/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
    except urllib.error.URLError as exc:
        raise RuntimeError(f"RPC request failed: {exc}") from exc

    if "error" in data:
        raise RuntimeError(f"RPC error for {method}: {data['error']}")
    return data


def eth_call(rpc_url: str, to: str, data_hex: str, block: str = "latest") -> str:
    result = rpc_call(rpc_url, "eth_call", [{"to": to, "data": data_hex}, block])["result"]
    if not isinstance(result, str) or not result.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_call result: {result}")
    return result


def get_block_context(rpc_url: str) -> tuple[int, int]:
    block_hex = rpc_call(rpc_url, "eth_blockNumber", [])["result"]
    block_num = int(block_hex, 16)
    block = rpc_call(rpc_url, "eth_getBlockByNumber", [block_hex, False])["result"]
    timestamp = int(block["timestamp"], 16)
    return block_num, timestamp


def decode_signed_256(word_hex: str) -> int:
    value = int(word_hex, 16)
    if value >= (1 << 255):
        return value - (1 << 256)
    return value


def query_lock(rpc_url: str, ve_address: str, lock_id: int) -> dict:
    data = "0x" + SEL_LOCKED + u256_hex(lock_id)
    raw = eth_call(rpc_url, ve_address, data)
    words = [raw[2 + i : 2 + i + 64] for i in range(0, 64 * 4, 64)]
    if len(words) < 4:
        raise RuntimeError(f"Malformed locked() response for lock {lock_id}: {raw}")

    amount = decode_signed_256(words[0])
    end = int(words[1], 16)
    is_permanent = int(words[2], 16) != 0
    is_smnft = int(words[3], 16) != 0

    principal = 0
    if is_smnft and amount >= 0:
        calc_data = "0x" + SEL_CALC_ORIGINAL_SM + u256_hex(amount)
        principal = int(eth_call(rpc_url, ve_address, calc_data), 16)
    elif is_permanent:
        principal = amount

    return {
        "lock_id": lock_id,
        "amount_wei": amount,
        "amount_tokens": wei_to_token_str(amount),
        "end": end,
        "is_permanent": is_permanent,
        "is_smnft": is_smnft,
        "principal_wei": principal,
        "principal_tokens": wei_to_token_str(principal),
    }


def render_report(
    ve_address: str,
    rpc_url: str,
    block_number: int,
    block_ts: int,
    locks: List[dict],
) -> str:
    now_utc = dt.datetime.now(dt.timezone.utc).isoformat()
    block_utc = dt.datetime.fromtimestamp(block_ts, dt.timezone.utc).isoformat()

    lines = [
        "veNOVA Lock Report",
        "=================",
        f"Generated UTC: {now_utc}",
        f"RPC URL: {rpc_url}",
        f"VotingEscrow: {ve_address}",
        f"Block: {block_number}",
        f"Block Timestamp UTC: {block_utc}",
        "",
        "Columns:",
        "lock_id | lock_amount_veNOVA | permanent_principal_veNOVA | is_permanent | is_smnft | end",
        "",
    ]

    total_principal = 0
    for item in locks:
        total_principal += item["principal_wei"]
        lines.append(
            f"{item['lock_id']} | "
            f"{item['amount_tokens']} | "
            f"{item['principal_tokens']} | "
            f"{str(item['is_permanent']).lower()} | "
            f"{str(item['is_smnft']).lower()} | "
            f"{item['end']}"
        )

    lines.extend(
        [
            "",
            f"Total permanent principal (input locks): {wei_to_token_str(total_principal)} veNOVA",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    raw_inputs = list(args.locks)

    if args.locks_file:
        try:
            with open(args.locks_file, "r", encoding="utf-8") as f:
                raw_inputs.append(f.read())
        except OSError as exc:
            print(f"Failed to read --locks-file: {exc}", file=sys.stderr)
            return 1

    try:
        lock_ids = parse_lock_ids(raw_inputs)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if not lock_ids:
        print("No lock IDs provided.", file=sys.stderr)
        return 1

    try:
        block_number, block_ts = get_block_context(args.rpc_url)
        results = [query_lock(args.rpc_url, args.ve_address, lock_id) for lock_id in lock_ids]
        report = render_report(args.ve_address, args.rpc_url, block_number, block_ts, results)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report)
    except Exception as exc:
        print(f"Failed: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote report for {len(lock_ids)} locks to: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
