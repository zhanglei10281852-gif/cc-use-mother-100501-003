"""履约轨迹核对测试。"""

import unittest

from helpers import T0, make_backend, publish, quote_and_reserve


class TrailTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        publish(self.backend, self.clock, node="node-a", fault_domain="fd-1", price=10.0)
        publish(self.backend, self.clock, node="node-b", fault_domain="fd-2", price=10.0)

    def tearDown(self) -> None:
        self.backend.close()

    def test_trail_covers_quote_to_settlement(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=3600, idem_key="r1")
        rid = reservation["reservation_id"]
        group = self.backend.tasks.submit_group(
            tenant_id="tenant-1", reservation_id=rid,
            tasks=[{"name": "job", "cards": 2, "depends_on": []}])
        self.clock.advance(600)
        migration = self.backend.lifecycle.plan_migration(
            reservation_id=rid, reason="REBALANCE", horizon_seconds=3600,
            require_domain_change=True)
        self.backend.lifecycle.execute_migration(migration["migration_id"])
        self.backend.billing.ingest_events(events=[{
            "event_id": "e1", "reservation_id": rid, "node_id": "node-a",
            "cards": 2, "usage_start": T0, "usage_end": T0 + 600}])
        self.clock.advance(3000)
        self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid,
            period_start=T0, period_end=T0 + 3600, finalize=True)

        trail = self.backend.trail_service.trail(tenant_id="tenant-1", reservation_id=rid)
        self.assertEqual(len(trail["quotes"]), 1)
        self.assertEqual(len(trail["reservations"]), 1)
        self.assertEqual(len(trail["billing_segments"]), 2)  # 迁移产生两段
        self.assertEqual(len(trail["task_groups"]), 1)
        self.assertEqual(len(trail["migrations"]), 1)
        self.assertEqual(len(trail["bills"]), 1)
        self.assertEqual(len(trail["consumption_events"]), 1)

        event_types = [entry["type"] for entry in trail["timeline"]]
        seqs = [entry["seq"] for entry in trail["timeline"]]
        self.assertEqual(seqs, sorted(seqs))  # 时间线按发生顺序排列
        for expected in ("quote.created", "reservation.created", "segment.opened",
                         "task_group.submitted", "migration.completed", "usage.ingested",
                         "bill.generated"):
            self.assertIn(expected, event_types)
        # 轨迹中的报价指纹可独立复核。
        verification = self.backend.quotes.verify_quote(trail["quotes"][0]["quote_id"])
        self.assertTrue(verification["fingerprint_matches"])

    def test_trail_scoped_to_tenant(self) -> None:
        quote_and_reserve(self.backend, self.clock, tenant="tenant-1", cards=1, idem_key="a")
        quote_and_reserve(self.backend, self.clock, tenant="tenant-2", cards=1, idem_key="b")
        trail = self.backend.trail_service.trail(tenant_id="tenant-1")
        self.assertEqual(len(trail["reservations"]), 1)
        self.assertEqual(trail["reservations"][0]["tenant_id"], "tenant-1")


if __name__ == "__main__":
    unittest.main()
