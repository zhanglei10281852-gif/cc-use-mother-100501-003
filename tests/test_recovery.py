"""进程重启恢复测试：未决迁移续跑、到期清扫补跑、审计保留。"""

import tempfile
import unittest
from pathlib import Path

from helpers import T0, make_backend, publish, quote_and_reserve


class RecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "compute.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_pending_migration_resumes_after_restart(self) -> None:
        backend, clock = make_backend(self.db)
        publish(backend, clock, node="node-a", fault_domain="fd-1")
        publish(backend, clock, node="node-b", fault_domain="fd-2")
        reservation = quote_and_reserve(backend, clock, cards=2, idem_key="r1", node_id="node-a")
        migration = backend.lifecycle.plan_migration(
            reservation_id=reservation["reservation_id"], reason="DEGRADATION",
            horizon_seconds=7200, require_domain_change=True)
        backend.close()  # 模拟进程在 PLANNED 与执行之间崩溃

        backend2, _ = make_backend(self.db)
        result = backend2.recover()
        self.assertEqual(result["pending_migrations_resumed"], [migration["migration_id"]])
        row = backend2.store.one("SELECT state, to_node FROM migrations WHERE migration_id = ?",
                                 (migration["migration_id"],))
        self.assertEqual(row["state"], "COMPLETED")
        moved = backend2.store.one("SELECT node_id, state FROM reservations WHERE reservation_id = ?",
                                   (reservation["reservation_id"],))
        self.assertEqual((moved["node_id"], moved["state"]), ("node-b", "ACTIVE"))
        open_count = backend2.store.one(
            "SELECT COUNT(*) AS n FROM billing_segments WHERE reservation_id = ? AND end_at IS NULL",
            (reservation["reservation_id"],))
        self.assertEqual(open_count["n"], 1)
        # 恢复幂等：再次执行不产生变化。
        again = backend2.recover()
        self.assertEqual(again["pending_migrations_resumed"], [])
        backend2.close()

    def test_expiry_reclaim_continues_after_restart(self) -> None:
        backend, clock = make_backend(self.db)
        publish(backend, clock, node="node-a", cards=4, hours=48)
        reservation = quote_and_reserve(backend, clock, cards=2, duration=1800, idem_key="r1",
                                        auto_renew=False, migratable=False)
        backend.tasks.submit_group(tenant_id="tenant-1", reservation_id=reservation["reservation_id"],
                                   tasks=[{"name": "job", "cards": 1, "depends_on": []}])
        backend.close()

        # 重启时时钟已越过到期点，recover 应补跑到期回收。
        backend2, _ = make_backend(self.db, start=T0 + 1900)
        result = backend2.recover()
        self.assertIn(reservation["reservation_id"], result["sweep"]["expired"])
        row = backend2.store.one("SELECT state FROM reservations WHERE reservation_id = ?",
                                 (reservation["reservation_id"],))
        self.assertEqual(row["state"], "EXPIRED")
        ledger = backend2.store.one("SELECT allocated_cards FROM capacity_ledger")
        self.assertEqual(ledger["allocated_cards"], 0)
        # 审计轨迹在重启后仍然完整可查。
        trail = backend2.trail_service.trail(tenant_id="tenant-1")
        self.assertGreater(len(trail["timeline"]), 0)
        backend2.close()

    def test_idempotency_survives_restart(self) -> None:
        backend, clock = make_backend(self.db)
        publish(backend, clock, node="node-a", cards=4)
        quote = backend.quotes.request_quote(
            tenant_id="tenant-1", cards=2, start_at=clock.now(), duration_seconds=600)
        first = backend.reservations.reserve(
            tenant_id="tenant-1", quote_id=quote["quote_id"], idem_key="persist-1")
        backend.close()

        backend2, _ = make_backend(self.db)
        replay = backend2.reservations.reserve(
            tenant_id="tenant-1", quote_id=quote["quote_id"], idem_key="persist-1")
        self.assertEqual(replay["reservation_id"], first["reservation_id"])
        self.assertTrue(replay["replayed"])
        backend2.close()


if __name__ == "__main__":
    unittest.main()
