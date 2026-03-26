import unittest
from unittest.mock import patch

import venova_lock_report as report


def encode_u256(value: int) -> str:
    return f"{value & ((1 << 256) - 1):064x}"


def encode_i256(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return encode_u256(value)


def locked_result(amount: int, end: int, is_permanent: bool, is_smnft: bool) -> str:
    return (
        "0x"
        + encode_i256(amount)
        + encode_u256(end)
        + encode_u256(int(is_permanent))
        + encode_u256(int(is_smnft))
    )


def scalar_result(value: int) -> str:
    return "0x" + encode_u256(value)


class ParseLockIdsTests(unittest.TestCase):
    def test_accepts_mixed_delimiters(self) -> None:
        self.assertEqual(report.parse_lock_ids(["1, 2\n3\t4"]), [1, 2, 3, 4])

    def test_rejects_negative_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-negative"):
            report.parse_lock_ids(["-7"])

    def test_rejects_non_numeric_tokens(self) -> None:
        with self.assertRaisesRegex(ValueError, "Invalid lock id token"):
            report.parse_lock_ids(["abc123"])


class ValidationTests(unittest.TestCase):
    def test_rejects_invalid_address(self) -> None:
        with self.assertRaisesRegex(ValueError, "Invalid Ethereum address"):
            report.normalize_address("not-an-address")


class QueryLockTests(unittest.TestCase):
    def test_query_lock_uses_explicit_block_tag_and_normalizes_address(self) -> None:
        fake_row = report.LockReportRow(
            lock_id=7,
            amount_wei=2 * report.DECIMALS,
            principal_wei=report.DECIMALS,
            is_permanent=True,
            is_smnft=False,
            end=123,
        )

        with patch.object(report, "query_locks", return_value=[fake_row]) as mock_query_locks:
            row = report.query_lock(
                "https://example-rpc.invalid",
                "0x4C3E7640B3E3A39A2E5D030A0C1412D80FEE1D44",
                7,
                block_tag="0xabc",
            )

        mock_query_locks.assert_called_once_with(
            "https://example-rpc.invalid",
            report.DEFAULT_VE_ADDRESS,
            [7],
            "0xabc",
            report.DEFAULT_BATCH_SIZE,
        )
        self.assertEqual(
            row,
            {
                "lock_id": 7,
                "amount_wei": 2 * report.DECIMALS,
                "amount_tokens": "2",
                "end": 123,
                "is_permanent": True,
                "is_smnft": False,
                "principal_wei": report.DECIMALS,
                "principal_tokens": "1",
            },
        )

    def test_query_lock_uses_latest_snapshot_when_block_tag_is_missing(self) -> None:
        fake_row = report.LockReportRow(
            lock_id=9,
            amount_wei=report.DECIMALS,
            principal_wei=0,
            is_permanent=False,
            is_smnft=False,
            end=999,
        )

        with patch.object(
            report,
            "get_block_context",
            return_value=report.BlockContext(number=123, hex_number="0xdeadbeef", timestamp=1_700_000_000),
        ), patch.object(report, "query_locks", return_value=[fake_row]) as mock_query_locks:
            row = report.query_lock(
                "https://example-rpc.invalid",
                report.DEFAULT_VE_ADDRESS,
                9,
            )

        self.assertEqual(row["lock_id"], 9)
        self.assertEqual(mock_query_locks.call_args.args[3], "0xdeadbeef")


class QueryLocksTests(unittest.TestCase):
    def test_pins_every_eth_call_to_the_snapshot_block(self) -> None:
        seen_payloads: list[list[dict[str, object]]] = []

        def fake_batch_call(_rpc_url: str, payloads: list[dict[str, object]]) -> list[dict[str, object]]:
            seen_payloads.append(payloads)
            responses: list[dict[str, object]] = []
            for payload in payloads:
                data_hex = payload["params"][0]["data"]
                if data_hex.startswith("0x" + report.SEL_LOCKED):
                    result = locked_result(2 * report.DECIMALS, 123, False, True)
                else:
                    result = scalar_result(report.DECIMALS)
                responses.append({"jsonrpc": "2.0", "id": payload["id"], "result": result})
            return responses

        with patch.object(report, "rpc_batch_call", side_effect=fake_batch_call):
            rows = report.query_locks(
                "https://example-rpc.invalid",
                report.DEFAULT_VE_ADDRESS,
                [42],
                "0xabc",
                batch_size=100,
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].principal_wei, report.DECIMALS)
        for payloads in seen_payloads:
            for payload in payloads:
                self.assertEqual(payload["params"][1], "0xabc")

    def test_preserves_duplicate_lock_ids(self) -> None:
        def fake_batch_call(_rpc_url: str, payloads: list[dict[str, object]]) -> list[dict[str, object]]:
            return [
                {
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": locked_result(report.DECIMALS, 999, True, False),
                }
                for payload in payloads
            ]

        with patch.object(report, "rpc_batch_call", side_effect=fake_batch_call):
            rows = report.query_locks(
                "https://example-rpc.invalid",
                report.DEFAULT_VE_ADDRESS,
                [7, 7],
                "0xabc",
                batch_size=100,
            )

        self.assertEqual([row.lock_id for row in rows], [7, 7])
        self.assertEqual([row.principal_wei for row in rows], [report.DECIMALS, report.DECIMALS])

    def test_falls_back_to_sequential_calls_when_batching_is_unsupported(self) -> None:
        def fake_rpc_call(
            _rpc_url: str,
            method: str,
            params: list[object],
        ) -> dict[str, object]:
            self.assertEqual(method, "eth_call")
            data_hex = params[0]["data"]
            if data_hex.startswith("0x" + report.SEL_LOCKED):
                result = locked_result(report.DECIMALS, 999, True, False)
            else:
                result = scalar_result(report.DECIMALS)
            return {"jsonrpc": "2.0", "id": 1, "result": result}

        with patch.object(report, "rpc_batch_call", side_effect=RuntimeError("batch unsupported")), patch.object(
            report,
            "rpc_call",
            side_effect=fake_rpc_call,
        ):
            rows = report.query_locks(
                "https://example-rpc.invalid",
                report.DEFAULT_VE_ADDRESS,
                [7],
                "0xabc",
                batch_size=100,
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].principal_wei, report.DECIMALS)


class RenderReportTests(unittest.TestCase):
    def test_renders_total_principal(self) -> None:
        output = report.render_report(
            report.DEFAULT_VE_ADDRESS,
            report.DEFAULT_RPC_URL,
            123,
            1_700_000_000,
            [
                report.LockReportRow(
                    lock_id=1,
                    amount_wei=2 * report.DECIMALS,
                    principal_wei=report.DECIMALS,
                    is_permanent=True,
                    is_smnft=False,
                    end=0,
                )
            ],
        )

        self.assertIn("Block: 123", output)
        self.assertIn("1 | 2 | 1 | true | false | 0", output)
        self.assertIn("Total permanent principal (input locks): 1 veNOVA", output)


if __name__ == "__main__":
    unittest.main()
