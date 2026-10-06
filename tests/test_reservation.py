"""预留幂等与防超卖测试。"""

import unittest
from concurrent.futures import ThreadPoolExecutor

from helpers import allocated, make_backend, publish, quote_and_reserve

from compute_reservation import errors


class IdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch = publish(self.backend, self.clock, cards=4)

    def tearDown(self) -> None:
        self.backend.close()

    def test_same_key_same_payload_replays(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=600)
        first = self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-1")
        # 首次成功后报价已消费，若幂等重放走缓存响应，不应报 quote_consumed。
        second = self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-1")
        self.assertEqual(first["reservation_id"], second["reservation_id"])
        self.assertTrue(second["replayed"])
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 2)

    def test_same_key_different_payload_conflicts(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=600)
        self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-1")
        with self.assertRaises(errors.DomainError) as ctx:
            self.backend.reservations.reserve(
                tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-1", tenant_priority=5)
        self.assertEqual(ctx.exception.code, "idempotency_conflict")

    def test_consumed_quote_cannot_be_reused_with_new_key(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=600)
        self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-1")
        with self.assertRaises(errors.DomainError) as ctx:
            self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-2")
        self.assertEqual(ctx.exception.code, "quote_consumed")

    def test_expired_quote_rejected(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=600, quote_ttl=30)
        self.clock.advance(60)
        with self.assertRaises(errors.DomainError) as ctx:
            self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote["quote_id"], idem_key="k-1")
        self.assertEqual(ctx.exception.code, "quote_expired")

    def test_other_tenant_cannot_use_quote(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=600)
        with self.assertRaises(errors.DomainError) as ctx:
            self.backend.reservations.reserve(tenant_id="t-2", quote_id=quote["quote_id"], idem_key="k-1")
        self.assertEqual(ctx.exception.code, "forbidden")


class OversellTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch = publish(self.backend, self.clock, cards=4)

    def tearDown(self) -> None:
        self.backend.close()

    def test_sequential_oversell_rejected(self) -> None:
        # 两笔报价都在容量充足时开出；第二笔锁定时容量已被第一笔占掉。
        quote_a = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=3, start_at=self.clock.now(), duration_seconds=600)
        quote_b = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=600)
        self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote_a["quote_id"], idem_key="a")
        with self.assertRaises(errors.DomainError) as ctx:
            self.backend.reservations.reserve(tenant_id="t-1", quote_id=quote_b["quote_id"], idem_key="b")
        self.assertEqual(ctx.exception.code, "capacity_exceeded")
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 3)

    def test_concurrent_reservations_do_not_oversell(self) -> None:
        quotes = [
            self.backend.quotes.request_quote(
                tenant_id="t-1", cards=4, start_at=self.clock.now(), duration_seconds=600)
            for _ in range(8)
        ]

        def attempt(index: int) -> str:
            try:
                self.backend.reservations.reserve(
                    tenant_id="t-1", quote_id=quotes[index]["quote_id"], idem_key=f"race-{index}")
                return "ok"
            except errors.DomainError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(attempt, range(8)))
        self.assertEqual(results.count("ok"), 1)
        self.assertEqual(results.count("capacity_exceeded"), 7)
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 4)

    def test_same_inventory_cannot_be_published_twice(self) -> None:
        publish(self.backend, self.clock, node="node-x", inventory_ref="inv-001")
        with self.assertRaises(errors.DomainError) as ctx:
            publish(self.backend, self.clock, node="node-y", inventory_ref="inv-001")
        self.assertEqual(ctx.exception.code, "inventory_conflicts")


if __name__ == "__main__":
    unittest.main()
