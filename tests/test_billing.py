"""消费事件收敛与唯一账单测试。"""

import unittest

from helpers import T0, make_backend, publish, quote_and_reserve


def event(event_id, rid, node="node-a", cards=2, start=T0, end=T0 + 1800, energy=1.0):
    return {"event_id": event_id, "reservation_id": rid, "node_id": node,
            "cards": cards, "usage_start": start, "usage_end": end, "energy_kwh": energy}


class IngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        publish(self.backend, self.clock, cards=8)
        self.reservation = quote_and_reserve(self.backend, self.clock, cards=2, duration=7200, idem_key="r1")
        self.rid = self.reservation["reservation_id"]

    def tearDown(self) -> None:
        self.backend.close()

    def test_duplicate_event_ignored(self) -> None:
        first = self.backend.billing.ingest_events(events=[event("e1", self.rid)])
        second = self.backend.billing.ingest_events(events=[event("e1", self.rid)])
        self.assertEqual(first["results"][0]["status"], "accepted")
        self.assertEqual(second["results"][0]["status"], "duplicate")
        count = self.backend.store.one("SELECT COUNT(*) AS n FROM consumption_events")
        self.assertEqual(count["n"], 1)

    def test_same_id_different_payload_conflicts(self) -> None:
        self.backend.billing.ingest_events(events=[event("e1", self.rid)])
        result = self.backend.billing.ingest_events(events=[event("e1", self.rid, cards=3)])
        self.assertEqual(result["results"][0]["status"], "conflict")

    def test_invalid_events_rejected(self) -> None:
        result = self.backend.billing.ingest_events(events=[
            {"event_id": "", "reservation_id": self.rid},
            event("e2", self.rid, start=T0, end=T0 - 1),
            event("e3", "res_ghost"),
        ])
        statuses = [r["status"] for r in result["results"]]
        self.assertEqual(statuses, ["rejected", "rejected", "rejected"])


class BillConvergenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        publish(self.backend, self.clock, cards=8, price=10.0)
        publish(self.backend, self.clock, node="node-a2", fault_domain="fd-1", cards=8, price=10.0)

    def tearDown(self) -> None:
        self.backend.close()

    def _reserve(self, idem_key, node):
        return quote_and_reserve(self.backend, self.clock, cards=2, duration=3600,
                                 idem_key=idem_key, node_id=node)

    def test_out_of_order_and_duplicate_events_converge(self) -> None:
        first = self._reserve("r1", "node-a")
        second = self._reserve("r2", "node-a2")
        events = [event("e1", first["reservation_id"]), event("e2", first["reservation_id"], start=T0 + 1800, end=T0 + 3600)]
        # 第一个预留：乱序 + 重复到达。
        self.backend.billing.ingest_events(events=[events[1], events[0], events[1]])
        # 第二个预留：正常顺序，事件内容镜像（event_id 全局唯一，故换用 m 前缀）。
        mirror = [event("m1", second["reservation_id"], node="node-a2"),
                  event("m2", second["reservation_id"], node="node-a2", start=T0 + 1800, end=T0 + 3600)]
        self.backend.billing.ingest_events(events=mirror)
        self.clock.advance(3601)

        bill_first = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=first["reservation_id"],
            period_start=T0, period_end=T0 + 3600)
        bill_second = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=second["reservation_id"],
            period_start=T0, period_end=T0 + 3600)
        self.assertEqual(bill_first["usage"], bill_second["usage"])
        self.assertEqual(bill_first["total"], bill_second["total"])
        self.assertEqual(bill_first["usage"]["event_count"], 2)  # 重复事件只计一次

    def test_regeneration_is_idempotent_single_bill(self) -> None:
        reservation = self._reserve("r1", "node-a")
        self.backend.billing.ingest_events(events=[event("e1", reservation["reservation_id"])])
        self.clock.advance(3601)
        one = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=reservation["reservation_id"],
            period_start=T0, period_end=T0 + 3600)
        two = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=reservation["reservation_id"],
            period_start=T0, period_end=T0 + 3600)
        self.assertEqual(one["bill_id"], two["bill_id"])
        self.assertEqual(one["fingerprint"], two["fingerprint"])
        count = self.backend.store.one("SELECT COUNT(*) AS n FROM bills")
        self.assertEqual(count["n"], 1)

    def test_late_event_updates_draft_then_adjusts_finalized(self) -> None:
        reservation = self._reserve("r1", "node-a")
        rid = reservation["reservation_id"]
        self.backend.billing.ingest_events(events=[event("e1", rid)])
        self.clock.advance(3601)
        draft = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 3600)
        # 草稿阶段迟到事件 -> 同一账单版本递增。
        self.backend.billing.ingest_events(events=[event("e2", rid, start=T0 + 1800, end=T0 + 3600)])
        updated = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 3600)
        self.assertEqual(updated["bill_id"], draft["bill_id"])
        self.assertEqual(updated["version"], 2)
        # 终审后又有迟到事件 -> 生成调整单，原账单保持不变。
        finalized = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 3600,
            finalize=True)
        self.assertEqual(finalized["state"], "FINALIZED")
        self.backend.billing.ingest_events(events=[event("e3", rid, cards=4, start=T0 + 100, end=T0 + 200)])
        adjustment = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 3600)
        self.assertEqual(adjustment["kind"], "ADJUSTMENT")
        self.assertEqual(adjustment["supersedes"], finalized["bill_id"])
        original = self.backend.billing.get_bill("tenant-1", finalized["bill_id"])
        self.assertEqual(original["fingerprint"], finalized["fingerprint"])

    def test_overage_billed_when_meter_exceeds_entitlement(self) -> None:
        reservation = self._reserve("r1", "node-a")
        rid = reservation["reservation_id"]
        # 预留 2 卡，事件按 4 卡跑满整小时 -> 超出配额的 2 卡时计入 overage。
        self.backend.billing.ingest_events(events=[event("e1", rid, cards=4, start=T0, end=T0 + 3600)])
        self.clock.advance(3601)
        bill = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 3600)
        kinds = [l["kind"] for l in bill["lines"]]
        self.assertIn("overage", kinds)
        occupancy = next(l for l in bill["lines"] if l["kind"] == "occupancy")
        overage = next(l for l in bill["lines"] if l["kind"] == "overage")
        self.assertEqual(occupancy["amount"], "20.000000")  # 2卡 × 1h × 10
        self.assertEqual(overage["amount"], "20.000000")    # 超出 2卡 × 1h × 10
        self.assertEqual(bill["total"], 40.0)

    def test_preemption_credit_appears_in_bill(self) -> None:
        reservation = self._reserve("r1", "node-a")
        rid = reservation["reservation_id"]
        self.clock.advance(1800)
        self.backend.lifecycle.preempt(reservation_ids=[rid], reason="测试", created_by="ops")
        self.clock.advance(600)
        bill = self.backend.billing.generate_bill(
            tenant_id="tenant-1", reservation_id=rid, period_start=T0, period_end=T0 + 2400)
        credits = [l for l in bill["lines"] if l["kind"] == "credit"]
        self.assertEqual(len(credits), 1)
        # 剩余 1800 秒 × 2 卡 × 10 × 1.5 = 15，账单总额 = 占用 10 - 补偿 15 = -5。
        self.assertEqual(credits[0]["amount"], "-15.000000")
        self.assertEqual(bill["total"], -5.0)


if __name__ == "__main__":
    unittest.main()
