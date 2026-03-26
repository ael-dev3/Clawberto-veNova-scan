import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "skills" / "opensea-venova-lock-arb" / "scripts" / "venova_opensea_discount_scan.py"

SPEC = importlib.util.spec_from_file_location("venova_opensea_discount_scan", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Failed to load scan module from {MODULE_PATH}")
scan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scan)


class FetchLocksByIdTests(unittest.TestCase):
    def test_pins_every_lock_read_to_one_snapshot_block(self) -> None:
        snapshot = scan.BlockContext(number=123, hex_number="0xabc", timestamp=1_700_000_000)
        seen_calls: list[tuple[int, str | None]] = []

        def fake_query_lock(
            _rpc_url: str,
            _ve_address: str,
            lock_id: int,
            *,
            block_tag: str | None = None,
            batch_size: int = 100,
        ) -> dict[str, object]:
            self.assertEqual(batch_size, 100)
            seen_calls.append((lock_id, block_tag))
            return {"lock_id": lock_id, "amount_wei": lock_id}

        with patch.object(scan, "get_block_context", return_value=snapshot), patch.object(
            scan,
            "query_lock",
            side_effect=fake_query_lock,
        ):
            lock_map, errors, returned_snapshot = scan.fetch_locks_by_id(
                [7, 9, 7],
                rpc_url="https://example-rpc.invalid",
                ve_address=scan.DEFAULT_VE_ADDRESS,
                workers=8,
            )

        self.assertEqual(errors, [])
        self.assertEqual(returned_snapshot, snapshot)
        self.assertEqual(lock_map, {7: {"lock_id": 7, "amount_wei": 7}, 9: {"lock_id": 9, "amount_wei": 9}})
        self.assertCountEqual(seen_calls, [(7, "0xabc"), (9, "0xabc")])


if __name__ == "__main__":
    unittest.main()
