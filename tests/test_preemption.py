"""抢占记录、补偿与恢复次序测试。"""

import unittest

from helpers import make_backend, publish, quote_and_reserve


class PreemptionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        publish(self.backend, self.clock, node="node-a", fault_domain="fd-1", cards=8, price=10.0)
        publish(self.backend, self.clock, node="node-b", fault_domain="fd-2", cards=8, price=10.0)

    def tearDown(self) -> None:
        self.backend.close()

    def _reserve(self, tenant, priority, idem_key, node="node-a"):
        return quote_and_reserve(self.backend, self.clock, tenant=tenant, cards=2,
                                 duration=7200, idem_key=idem_key, migratable=False,
                                 tenant_priority=priority, node_id=node)

    def test_record_keeps_tenants_compensation_and_recovery_order(self) -> None:
        low = self._reserve("tenant-low", priority=200, idem_key="k-low")
        high = self._reserve("tenant-high", priority=10, idem_key="k-high")
        record = self.backend.lifecycle.preempt(
            reservation_ids=[low["reservation_id"], high["reservation_id"]],
            reason="保障关键业务", created_by="ops")

        ranks = {i["tenant_id"]: i["recovery_rank"] for i in record["items"]}
        self.assertEqual(ranks["tenant-high"], 1)  # 优先级数值小者先恢复
        self.assertEqual(ranks["tenant-low"], 2)
        for item in record["items"]:
            self.assertEqual(item["compensation"], "60.000000")  # 2h × 2卡 × 10 × 1.5
            self.assertEqual(item["cards"], 2)
        self.assertEqual(record["reason"], "保障关键业务")

    def test_restore_follows_recorded_order(self) -> None:
        low = self._reserve("tenant-low", priority=200, idem_key="k-low")
        high = self._reserve("tenant-high", priority=10, idem_key="k-high")
        record = self.backend.lifecycle.preempt(
            reservation_ids=[low["reservation_id"], high["reservation_id"]],
            reason="测试", created_by="ops")
        result = self.backend.lifecycle.restore_preemption(preemption_id=record["preemption_id"])

        self.assertEqual(result["state"], "CLOSED")
        self.assertEqual(result["restored"],
                         [high["reservation_id"], low["reservation_id"]])
        for rid in (high["reservation_id"], low["reservation_id"]):
            row = self.backend.store.one("SELECT state FROM reservations WHERE reservation_id = ?", (rid,))
            self.assertEqual(row["state"], "ACTIVE")
        restored = self.backend.store.query(
            "SELECT payload FROM audit_log WHERE type = 'preemption.restored' ORDER BY seq")
        self.assertEqual(len(restored), 2)
        import json
        order = [json.loads(r["payload"])["reservation_id"] for r in restored]
        self.assertEqual(order, [high["reservation_id"], low["reservation_id"]])

    def test_restore_waits_when_no_capacity(self) -> None:
        reservation = self._reserve("tenant-x", priority=50, idem_key="k-x")
        record = self.backend.lifecycle.preempt(
            reservation_ids=[reservation["reservation_id"]], reason="测试", created_by="ops")
        # 两个节点都降级，没有可恢复容量。
        self.backend.lifecycle.degrade_node(node_id="node-a", reason="故障")
        self.backend.lifecycle.degrade_node(node_id="node-b", reason="故障")
        result = self.backend.lifecycle.restore_preemption(preemption_id=record["preemption_id"])
        self.assertEqual(result["state"], "RESTORING")
        self.assertEqual(result["pending"], [reservation["reservation_id"]])
        # 节点恢复后按原次序重新接纳。
        self.backend.lifecycle.restore_node(node_id="node-b")
        result = self.backend.lifecycle.restore_preemption(preemption_id=record["preemption_id"])
        self.assertEqual(result["state"], "CLOSED")
        self.assertEqual(result["restored"], [reservation["reservation_id"]])


if __name__ == "__main__":
    unittest.main()
