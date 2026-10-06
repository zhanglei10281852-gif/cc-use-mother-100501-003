"""迁移不双重计费测试。"""

import unittest

from helpers import T0, all_segments, make_backend, open_segments, publish, quote_and_reserve


class MigrationBillingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch_a = publish(self.backend, self.clock, node="node-a", fault_domain="fd-1", price=10.0)
        self.batch_b = publish(self.backend, self.clock, node="node-b", fault_domain="fd-2", price=10.0)
        self.reservation = quote_and_reserve(
            self.backend, self.clock, cards=2, duration=7200, idem_key="r1", node_id="node-a")

    def tearDown(self) -> None:
        self.backend.close()

    def migrate_now(self) -> dict:
        migration = self.backend.lifecycle.plan_migration(
            reservation_id=self.reservation["reservation_id"], reason="REBALANCE",
            horizon_seconds=7200, require_domain_change=True)
        return self.backend.lifecycle.execute_migration(migration["migration_id"])

    def test_segments_never_overlap_and_single_open(self) -> None:
        self.clock.advance(1800)
        self.migrate_now()
        self.clock.advance(1800)
        segments = all_segments(self.backend, self.reservation["reservation_id"])
        self.assertEqual(len(segments), 2)
        # 半开区间首尾相接：旧段终点即新段起点，不存在重叠计费窗口。
        self.assertEqual(segments[0]["end_at"], segments[1]["start_at"])
        self.assertEqual(segments[0]["node_id"], "node-a")
        self.assertEqual(segments[1]["node_id"], "node-b")
        self.assertEqual(len(open_segments(self.backend, self.reservation["reservation_id"])), 1)

    def test_bill_around_cutover_has_no_double_charge(self) -> None:
        rid = self.reservation["reservation_id"]
        self.backend.billing.ingest_events(events=[
            {"event_id": "e1", "reservation_id": rid, "node_id": "node-a",
             "cards": 2, "usage_start": T0, "usage_end": T0 + 1800},
        ])
        self.clock.advance(1800)
        self.migrate_now()
        self.backend.billing.ingest_events(events=[
            {"event_id": "e2", "reservation_id": rid, "node_id": "node-b",
             "cards": 2, "usage_start": T0 + 1800, "usage_end": T0 + 3600},
        ])
        self.clock.advance(1800)
        bill = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 3600)
        occupancy = [l for l in bill["lines"] if l["kind"] == "occupancy"]
        self.assertEqual(len(occupancy), 2)
        # 每段 2 卡 × 0.5 小时 × 10 = 10，合计 20；跨段事件各归各段，无重复计费。
        self.assertEqual(bill["total"], 20.0)
        self.assertEqual(bill["usage"]["card_seconds"], 7200.0)
        self.assertEqual(bill["warnings"], [])

    def test_late_event_for_old_node_stays_on_old_segment(self) -> None:
        rid = self.reservation["reservation_id"]
        self.clock.advance(1800)
        self.migrate_now()
        # 切换完成后才到达的旧节点事件，按时间窗仍归属旧段，不会记到新节点头上。
        self.backend.billing.ingest_events(events=[
            {"event_id": "late", "reservation_id": rid, "node_id": "node-a",
             "cards": 2, "usage_start": T0 + 900, "usage_end": T0 + 1800},
        ])
        self.clock.advance(600)
        bill = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 2400)
        self.assertEqual(bill["usage"]["card_seconds"], 1800.0)
        self.assertEqual(bill["warnings"], [])

    def test_execute_migration_is_idempotent(self) -> None:
        self.clock.advance(600)
        migration = self.backend.lifecycle.plan_migration(
            reservation_id=self.reservation["reservation_id"], reason="REBALANCE",
            horizon_seconds=7200, require_domain_change=True)
        first = self.backend.lifecycle.execute_migration(migration["migration_id"])
        second = self.backend.lifecycle.execute_migration(migration["migration_id"])
        self.assertEqual(first["cutover_at"], second["cutover_at"])
        self.assertEqual(len(all_segments(self.backend, self.reservation["reservation_id"])), 2)


if __name__ == "__main__":
    unittest.main()
