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
from dataclasses import dataclass
from typing import Any, Iterable, Sequence


DEFAULT_RPC_URL = "https://ethereum-rpc.publicnode.com"
DEFAULT_VE_ADDRESS = "0x4c3e7640b3e3a39a2e5d030a0c1412d80fee1d44"
DEFAULT_BATCH_SIZE = 100
DECIMALS = 10**18
HEX_ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}$")
LOCK_TOKEN_RE = re.compile(r"-?\d+$")

# Precomputed function selectors:
# locked(uint256) -> 0xb45a3c0e
# calculate_original_sm_nft_amount(uint256) -> 0x09b2b405
SEL_LOCKED = "b45a3c0e"
SEL_CALC_ORIGINAL_SM = "09b2b405"


@dataclass(frozen=True)
class BlockContext:
    number: int
    hex_number: str
    timestamp: int


@dataclass
class LockReportRow:
    lock_id: int
    amount_wei: int
    end: int
    is_permanent: bool
    is_smnft: bool
    principal_wei: int = 0

    @property
    def amount_tokens(self) -> str:
        return wei_to_token_str(self.amount_wei)

    @property
    def principal_tokens(self) -> str:
        return wei_to_token_str(self.principal_wei)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return parsed


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
        "--batch-size",
        type=positive_int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Max eth_call requests per JSON-RPC batch (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--out",
        default="venova_locks_report.txt",
        help="Output .txt file path (default: venova_locks_report.txt).",
    )
    return parser.parse_args()


def parse_lock_ids(raw_values: Iterable[str]) -> list[int]:
    ids: list[int] = []
    for raw in raw_values:
        tokens = [token for token in re.split(r"[\s,]+", raw.strip()) if token]
        for token in tokens:
            if not LOCK_TOKEN_RE.fullmatch(token):
                raise ValueError(f"Invalid lock id token: {token}")
            value = int(token)
            if value < 0:
                raise ValueError(f"Lock id must be non-negative: {value}")
            ids.append(value)
    return ids


def normalize_address(address: str) -> str:
    if not HEX_ADDRESS_RE.fullmatch(address):
        raise ValueError(f"Invalid Ethereum address: {address}")
    return address.lower()


def u256_hex(value: int) -> str:
    if value < 0:
        raise ValueError(f"uint256 cannot encode negative value: {value}")
    return hex(value)[2:].rjust(64, "0")


def wei_to_token_str(amount_wei: int) -> str:
    sign = "-" if amount_wei < 0 else ""
    value = abs(amount_wei)
    whole, frac = divmod(value, DECIMALS)
    if frac == 0:
        return f"{sign}{whole}"
    frac_str = str(frac).rjust(18, "0").rstrip("0")
    return f"{sign}{whole}.{frac_str}"


def rpc_request(rpc_url: str, payload: Any) -> Any:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        rpc_url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "veNOVA-lock-report/2.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as exc:
        raise RuntimeError(f"RPC request failed: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"RPC returned invalid JSON: {exc}") from exc


def rpc_call(rpc_url: str, method: str, params: list[Any]) -> dict[str, Any]:
    data = rpc_request(rpc_url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected RPC response for {method}: {data!r}")
    if "error" in data:
        raise RuntimeError(f"RPC error for {method}: {data['error']}")
    if "result" not in data:
        raise RuntimeError(f"RPC response missing result for {method}: {data!r}")
    return data


def rpc_batch_call(rpc_url: str, payloads: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    if not payloads:
        return []

    data = rpc_request(rpc_url, list(payloads))
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected batch RPC response: {data!r}")

    responses_by_id: dict[int, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            raise RuntimeError(f"Unexpected batch RPC item: {item!r}")
        request_id = item.get("id")
        if not isinstance(request_id, int):
            raise RuntimeError(f"Batch RPC item missing numeric id: {item!r}")
        if request_id in responses_by_id:
            raise RuntimeError(f"Duplicate batch RPC response id: {request_id}")
        responses_by_id[request_id] = item

    if len(responses_by_id) != len(payloads):
        raise RuntimeError(
            f"Batch RPC response count mismatch: expected {len(payloads)}, got {len(responses_by_id)}"
        )

    ordered: list[dict[str, Any]] = []
    for payload in payloads:
        request_id = payload["id"]
        response = responses_by_id.get(request_id)
        if response is None:
            raise RuntimeError(f"Missing batch RPC response for id: {request_id}")
        if "error" in response:
            raise RuntimeError(f"RPC error for {payload['method']}: {response['error']}")
        if "result" not in response:
            raise RuntimeError(f"Batch RPC response missing result for id {request_id}: {response!r}")
        ordered.append(response)
    return ordered


def eth_call(rpc_url: str, to: str, data_hex: str, block_tag: str) -> str:
    result = rpc_call(rpc_url, "eth_call", [{"to": to, "data": data_hex}, block_tag])["result"]
    if not isinstance(result, str) or not result.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_call result: {result!r}")
    return result


def parse_result_words(result: str, *, expected_words: int | None = None) -> list[str]:
    if not isinstance(result, str) or not result.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_call result: {result!r}")
    payload = result[2:]
    if len(payload) % 64 != 0:
        raise RuntimeError(f"Malformed eth_call result length: {result}")
    if expected_words is not None and len(payload) != expected_words * 64:
        raise RuntimeError(f"Malformed eth_call result word count: expected {expected_words}, got {len(payload) // 64}")
    return [payload[index : index + 64] for index in range(0, len(payload), 64)]


def get_block_context(rpc_url: str) -> BlockContext:
    block_hex = rpc_call(rpc_url, "eth_blockNumber", [])["result"]
    if not isinstance(block_hex, str) or not block_hex.startswith("0x"):
        raise RuntimeError(f"Unexpected eth_blockNumber result: {block_hex!r}")

    block = rpc_call(rpc_url, "eth_getBlockByNumber", [block_hex, False])["result"]
    if not isinstance(block, dict):
        raise RuntimeError(f"Unexpected eth_getBlockByNumber result: {block!r}")

    timestamp_hex = block.get("timestamp")
    if not isinstance(timestamp_hex, str) or not timestamp_hex.startswith("0x"):
        raise RuntimeError(f"Unexpected block timestamp: {timestamp_hex!r}")

    return BlockContext(number=int(block_hex, 16), hex_number=block_hex, timestamp=int(timestamp_hex, 16))


def decode_signed_256(word_hex: str) -> int:
    value = int(word_hex, 16)
    if value >= (1 << 255):
        return value - (1 << 256)
    return value


def decode_locked_response(lock_id: int, raw: str) -> LockReportRow:
    words = parse_result_words(raw, expected_words=4)
    amount = decode_signed_256(words[0])
    end = int(words[1], 16)
    is_permanent = int(words[2], 16) != 0
    is_smnft = int(words[3], 16) != 0
    principal = amount if is_permanent and not is_smnft else 0

    return LockReportRow(
        lock_id=lock_id,
        amount_wei=amount,
        end=end,
        is_permanent=is_permanent,
        is_smnft=is_smnft,
        principal_wei=principal,
    )


def decode_u256_result(raw: str) -> int:
    words = parse_result_words(raw, expected_words=1)
    return int(words[0], 16)


def chunked(values: Sequence[tuple[int, str]], size: int) -> Iterable[Sequence[tuple[int, str]]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def eth_call_many(
    rpc_url: str,
    to: str,
    calls: Sequence[tuple[int, str]],
    block_tag: str,
    batch_size: int,
) -> list[tuple[int, str]]:
    ordered_results: list[tuple[int, str]] = []
    next_request_id = 1

    for call_chunk in chunked(calls, batch_size):
        request_key_by_id: dict[int, int] = {}
        payloads: list[dict[str, Any]] = []
        for request_key, data_hex in call_chunk:
            payloads.append(
                {
                    "jsonrpc": "2.0",
                    "id": next_request_id,
                    "method": "eth_call",
                    "params": [{"to": to, "data": data_hex}, block_tag],
                }
            )
            request_key_by_id[next_request_id] = request_key
            next_request_id += 1

        try:
            responses = rpc_batch_call(rpc_url, payloads)
            for response in responses:
                result = response["result"]
                if not isinstance(result, str) or not result.startswith("0x"):
                    raise RuntimeError(f"Unexpected eth_call result: {result!r}")
                ordered_results.append((request_key_by_id[response["id"]], result))
        except RuntimeError:
            for request_key, data_hex in call_chunk:
                ordered_results.append((request_key, eth_call(rpc_url, to, data_hex, block_tag)))

    return ordered_results


def query_locks(
    rpc_url: str,
    ve_address: str,
    lock_ids: Sequence[int],
    block_tag: str,
    batch_size: int,
) -> list[LockReportRow]:
    rows: list[LockReportRow | None] = [None] * len(lock_ids)
    locked_calls = [
        (index, "0x" + SEL_LOCKED + u256_hex(lock_id))
        for index, lock_id in enumerate(lock_ids)
    ]

    smnft_calls: list[tuple[int, str]] = []
    for index, raw in eth_call_many(rpc_url, ve_address, locked_calls, block_tag, batch_size):
        row = decode_locked_response(lock_ids[index], raw)
        rows[index] = row
        if row.is_smnft and row.amount_wei >= 0:
            smnft_calls.append((index, "0x" + SEL_CALC_ORIGINAL_SM + u256_hex(row.amount_wei)))

    for index, raw in eth_call_many(rpc_url, ve_address, smnft_calls, block_tag, batch_size):
        row = rows[index]
        if row is None:
            raise RuntimeError(f"Missing base row for lock index: {index}")
        row.principal_wei = decode_u256_result(raw)

    finalized_rows = [row for row in rows if row is not None]
    if len(finalized_rows) != len(lock_ids):
        raise RuntimeError("Failed to populate all lock rows.")
    return finalized_rows


def render_report(
    ve_address: str,
    rpc_url: str,
    block_number: int,
    block_ts: int,
    locks: Sequence[LockReportRow],
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
        total_principal += item.principal_wei
        lines.append(
            f"{item.lock_id} | "
            f"{item.amount_tokens} | "
            f"{item.principal_tokens} | "
            f"{str(item.is_permanent).lower()} | "
            f"{str(item.is_smnft).lower()} | "
            f"{item.end}"
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
            with open(args.locks_file, "r", encoding="utf-8") as file_handle:
                raw_inputs.append(file_handle.read())
        except OSError as exc:
            print(f"Failed to read --locks-file: {exc}", file=sys.stderr)
            return 1

    try:
        lock_ids = parse_lock_ids(raw_inputs)
        ve_address = normalize_address(args.ve_address)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if not lock_ids:
        print("No lock IDs provided.", file=sys.stderr)
        return 1

    try:
        block_context = get_block_context(args.rpc_url)
        results = query_locks(
            args.rpc_url,
            ve_address,
            lock_ids,
            block_context.hex_number,
            args.batch_size,
        )
        report = render_report(
            ve_address,
            args.rpc_url,
            block_context.number,
            block_context.timestamp,
            results,
        )
        with open(args.out, "w", encoding="utf-8") as file_handle:
            file_handle.write(report)
    except Exception as exc:
        print(f"Failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Wrote report for {len(lock_ids)} locks to: {args.out} "
        f"(snapshot block {block_context.number})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
