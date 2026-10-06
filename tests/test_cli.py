"""CLI 冒烟与关键流程测试（子进程真实执行）。"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run_cli(*args: str) -> tuple[int, dict]:
    result = subprocess.run(
        [sys.executable, "run_cli.py", *args],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    payload = json.loads(result.stdout) if result.stdout.strip() else {}
    return result.returncode, payload


class CliTests(unittest.TestCase):
    def test_smoke_default(self) -> None:
        code, payload = run_cli()
        self.assertEqual(code, 0)
        self.assertIn("fingerprint", payload)

    def test_demo_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "demo.db")
            code, payload = run_cli("--db", db, "demo")
            self.assertEqual(code, 0)
            self.assertTrue(payload["lock"]["idempotent_replay"])
            self.assertTrue(payload["quote"]["verify"])
            self.assertEqual(payload["degrade_migration"]["segments"], 2)
            self.assertTrue(payload["settlement_idempotent"])
            self.assertGreaterEqual(payload["trail_summary"]["bills"], 1)

    def test_lock_settle_trail_via_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "flow.db")
            base = ["--db", db, "--now", "2026-10-06T00:00:00Z"]
            code, node = run_cli(*base, "node-register", "--name", "n1",
                                 "--fault-domain", "room-a", "--energy-level", "medium")
            self.assertEqual(code, 0)
            code, batch = run_cli(*base, "batch-publish", "--node", node["node_code"],
                                  "--resource-type", "gpu", "--units", "4",
                                  "--from", "2026-10-05T00:00:00Z",
                                  "--until", "2026-10-10T00:00:00Z")
            self.assertEqual(code, 0)
            code, locked = run_cli(*base, "lock", "--idempotency-key", "cli-1",
                                   "--tenant", "tenant-cli", "--batch", batch["batch_code"],
                                   "--units", "2", "--end", "2026-10-06T08:00:00Z")
            self.assertEqual(code, 0)
            # 幂等重放
            code, replay = run_cli(*base, "lock", "--idempotency-key", "cli-1",
                                   "--tenant", "tenant-cli", "--batch", batch["batch_code"],
                                   "--units", "2", "--end", "2026-10-06T08:00:00Z")
            self.assertTrue(replay["idempotent_replay"])
            # 推进时间后结算
            code, settlement = run_cli("--db", db, "--now", "2026-10-06T05:00:00Z",
                                       "settle", "--tenant", "tenant-cli")
            self.assertEqual(code, 0)
            self.assertEqual(len(settlement["new_bills"]), 1)
            # 账单金额 = 2 单元 × 12 元 × 5 小时 = 120 元
            code, bills = run_cli(*base, "bills", "--tenant", "tenant-cli")
            self.assertEqual(bills[0]["amount_cents"], 12000)
            code, trail = run_cli(*base, "trail", "--tenant", "tenant-cli")
            self.assertEqual(len(trail["reservations"]), 1)
            self.assertEqual(len(trail["bills"]), 1)
            # 超卖被拒：错误码稳定、退出码非零
            code, err = run_cli(*base, "lock", "--idempotency-key", "cli-2",
                                "--tenant", "tenant-cli", "--batch", batch["batch_code"],
                                "--units", "3", "--end", "2026-10-06T08:00:00Z")
            self.assertEqual(code, 2)
            self.assertEqual(err["error"]["code"], "capacity_insufficient")


if __name__ == "__main__":
    unittest.main()
