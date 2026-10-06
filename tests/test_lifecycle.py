"""生命周期测试：到期续约/迁移/释放、节点降级、批次退役。"""

import unittest

from helpers import allocated, all_segments, make_backend, open_segments, publish, quote_and_reserve

from compute_reservation import errors


def running_task(backend, reservation, cards=2):
    group = backend.tasks.submit_group(
        tenant_id=reservation["tenant_id"], reservation_id=reservation["reservation_id"],
        tasks=[{"name": "job", "cards": cards, "depends_on": []}])
    return group["tasks"][0]


class ExpiryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch = publish(self.backend, self.clock, cards=8, hours=48)

    def tearDown(self) -> None:
        self.backend.close()

    def test_expiry_with_auto_renew_renews(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=3600,
                                        idem_key="r1", auto_renew=True, max_renewals=1)
        running_task(self.backend, reservation)
        old_end = reservation["end_at"]
        self.clock.set(old_end + 1)
        summary = self.backend.lifecycle.sweep()
        self.assertIn(reservation["reservation_id"], summary["renewed"])

        updated = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.assertEqual(updated["state"], "ACTIVE")
        self.assertEqual(updated["renewals_used"], 1)
        self.assertAlmostEqual(updated["end_at"], self.clock.now() + 3600, places=3)
        segments = all_segments(self.backend, reservation["reservation_id"])
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["close_reason"], "RENEWAL")
        self.assertIsNone(segments[1]["end_at"])

    def test_second_expiry_without_renewals_releases(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=3600,
                                        idem_key="r1", auto_renew=True, max_renewals=1,
                                        migratable=False)
        running_task(self.backend, reservation)
        self.clock.set(reservation["end_at"] + 1)
        self.backend.lifecycle.sweep()
        updated = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.clock.set(updated["end_at"] + 1)
        self.backend.lifecycle.sweep()
        final = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.assertEqual(final["state"], "EXPIRED")
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 0)

    def test_expiry_migrates_when_renew_not_allowed(self) -> None:
        publish(self.backend, self.clock, node="node-b", fault_domain="fd-2", hours=48)
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=3600,
                                        idem_key="r1", auto_renew=False, migratable=True,
                                        extension_seconds=1800, node_id="node-a")
        running_task(self.backend, reservation)
        self.clock.set(reservation["end_at"] + 1)
        summary = self.backend.lifecycle.sweep()
        self.assertIn(reservation["reservation_id"], summary["migrated"])

        updated = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.assertEqual(updated["state"], "ACTIVE")
        self.assertEqual(updated["node_id"], "node-b")
        self.assertAlmostEqual(updated["end_at"], self.clock.now() + 1800, places=3)
        segments = all_segments(self.backend, reservation["reservation_id"])
        self.assertEqual([s["close_reason"] for s in segments[:-1]], ["MIGRATION"])
        self.assertEqual(len(open_segments(self.backend, reservation["reservation_id"])), 1)

    def test_expiry_releases_when_no_option(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=3600,
                                        idem_key="r1", auto_renew=False, migratable=False)
        task = running_task(self.backend, reservation)
        self.clock.set(reservation["end_at"] + 1)
        summary = self.backend.lifecycle.sweep()
        self.assertIn(reservation["reservation_id"], summary["expired"])

        updated = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.assertEqual(updated["state"], "EXPIRED")
        task_row = self.backend.store.one("SELECT state FROM tasks WHERE task_id = ?", (task["task_id"],))
        self.assertEqual(task_row["state"], "INTERRUPTED")
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 0)

    def test_expiry_without_tasks_releases_directly(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=3600,
                                        idem_key="r1", auto_renew=True, max_renewals=3)
        self.clock.set(reservation["end_at"] + 1)
        summary = self.backend.lifecycle.sweep()
        self.assertIn(reservation["reservation_id"], summary["expired"])

    def test_sweep_expires_quotes_and_retires_batches(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=1, start_at=self.clock.now(), duration_seconds=600, quote_ttl=10)
        short = publish(self.backend, self.clock, node="node-short", cards=2, hours=1)
        self.clock.advance(3700)
        summary = self.backend.lifecycle.sweep()
        self.assertEqual(summary["quotes_expired"], 1)
        self.assertGreaterEqual(summary["batches_retired"], 1)
        row = self.backend.store.one("SELECT state FROM quotes WHERE quote_id = ?", (quote["quote_id"],))
        self.assertEqual(row["state"], "EXPIRED")
        row = self.backend.store.one("SELECT state FROM batches WHERE batch_id = ?", (short["batch_id"],))
        self.assertEqual(row["state"], "RETIRED")


class DegradationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch_a = publish(self.backend, self.clock, node="node-a", fault_domain="fd-1", cards=8)
        self.batch_b = publish(self.backend, self.clock, node="node-b", fault_domain="fd-2", cards=8)

    def tearDown(self) -> None:
        self.backend.close()

    def test_degrade_migrates_to_different_fault_domain(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, idem_key="r1",
                                        migratable=True, node_id="node-a")
        running_task(self.backend, reservation)
        result = self.backend.lifecycle.degrade_node(node_id="node-a", reason="温度告警")

        self.assertEqual(result["migrated"], [reservation["reservation_id"]])
        updated = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.assertEqual(updated["node_id"], "node-b")
        new_batch = self.backend.store.one("SELECT fault_domain, state FROM batches WHERE batch_id = ?",
                                           (updated["batch_id"],))
        self.assertEqual(new_batch["fault_domain"], "fd-2")
        old_batch = self.backend.store.one("SELECT state FROM batches WHERE batch_id = ?",
                                           (self.batch_a["batch_id"],))
        self.assertEqual(old_batch["state"], "DEGRADED")
        self.assertEqual(allocated(self.backend, self.batch_a["batch_id"]), 0)
        self.assertEqual(allocated(self.backend, self.batch_b["batch_id"]), 2)

    def test_degrade_preempts_non_migratable_with_compensation(self) -> None:
        reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=7200,
                                        idem_key="r1", migratable=False, node_id="node-a")
        self.clock.advance(1800)
        result = self.backend.lifecycle.degrade_node(node_id="node-a", reason="电源故障")

        self.assertEqual(result["preempted"], [reservation["reservation_id"]])
        record = result["preemption"]
        self.assertIsNotNone(record)
        item = record["items"][0]
        self.assertEqual(item["tenant_id"], "tenant-1")
        self.assertEqual(item["recovery_rank"], 1)
        # 剩余 5400 秒 × 2 卡 × 10/卡时 × 1.5 = 45
        self.assertEqual(item["compensation"], "45.000000")
        updated = self.backend.reservations.get("tenant-1", reservation["reservation_id"])
        self.assertEqual(updated["state"], "PREEMPTED")

    def test_open_quotes_invalidated_on_degrade(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=1, start_at=self.clock.now(), duration_seconds=600,
            node_id="node-a")
        self.backend.lifecycle.degrade_node(node_id="node-a", reason="网络分区")
        row = self.backend.store.one("SELECT state FROM quotes WHERE quote_id = ?", (quote["quote_id"],))
        self.assertEqual(row["state"], "INVALIDATED")

    def test_batch_retirement_evacuates_reservations(self) -> None:
        short = publish(self.backend, self.clock, node="node-c", fault_domain="fd-3", cards=4, hours=1)
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=1800,
            node_id="node-c")
        reservation = self.backend.reservations.reserve(
            tenant_id="t-1", quote_id=quote["quote_id"], idem_key="r1", migratable=True)
        running_task(self.backend, reservation)
        self.clock.advance(3601)
        self.backend.lifecycle.sweep()
        updated = self.backend.reservations.get("t-1", reservation["reservation_id"])
        self.assertEqual(updated["state"], "ACTIVE")
        self.assertNotEqual(updated["batch_id"], short["batch_id"])


if __name__ == "__main__":
    unittest.main()
