"""定价规则与报价复核测试。"""

import unittest
from decimal import Decimal

from helpers import make_backend, publish

from compute_reservation import errors, pricing


class PricingTests(unittest.TestCase):
    def test_price_is_deterministic(self) -> None:
        left = pricing.price_quote(
            unit_price=10.0, energy_tier="P2", batch_capabilities=["training", "fp8"],
            requested_capabilities=["training"], cards=4, duration_seconds=3600)
        right = pricing.price_quote(
            unit_price=10.0, energy_tier="P2", batch_capabilities=["fp8", "training"],
            requested_capabilities=["training"], cards=4, duration_seconds=3600)
        self.assertEqual(left, right)
        self.assertEqual(left.total, Decimal("42.000000"))  # 10 * 1.0 * 1.05 * 4 * 1h

    def test_energy_tier_adjusts_price(self) -> None:
        base = dict(unit_price=10.0, batch_capabilities=[], requested_capabilities=[],
                    cards=1, duration_seconds=3600)
        p1 = pricing.price_quote(energy_tier="P1", **base)
        p3 = pricing.price_quote(energy_tier="P3", **base)
        self.assertEqual(p1.total, Decimal("9.000000"))
        self.assertEqual(p3.total, Decimal("12.500000"))

    def test_unknown_energy_tier_rejected(self) -> None:
        with self.assertRaises(errors.DomainError):
            pricing.price_quote(unit_price=1, energy_tier="P9", batch_capabilities=[],
                                requested_capabilities=[], cards=1, duration_seconds=60)


class QuoteVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch = publish(self.backend, self.clock, price=12.0, capabilities=["training"])

    def tearDown(self) -> None:
        self.backend.close()

    def test_quote_verifies_against_stored_inputs(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=2, start_at=self.clock.now(), duration_seconds=1800,
            required_capabilities=["training"])
        result = self.backend.quotes.verify_quote(quote["quote_id"])
        self.assertTrue(result["inputs_intact"])
        self.assertTrue(result["price_matches"])
        self.assertTrue(result["fingerprint_matches"])
        self.assertFalse(result["expired"])

    def test_quote_selects_cheapest_batch(self) -> None:
        publish(self.backend, self.clock, node="node-b", price=8.0, fault_domain="fd-2")
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=1, start_at=self.clock.now(), duration_seconds=600)
        cheap = self.backend.store.one("SELECT batch_id, price_per_card_hour FROM batches WHERE price_per_card_hour = 8.0")
        self.assertEqual(quote["batch_id"], cheap["batch_id"])

    def test_quote_expires(self) -> None:
        quote = self.backend.quotes.request_quote(
            tenant_id="t-1", cards=1, start_at=self.clock.now(), duration_seconds=600, quote_ttl=60)
        self.clock.advance(120)
        result = self.backend.quotes.verify_quote(quote["quote_id"])
        self.assertTrue(result["expired"])

    def test_no_capacity_means_no_quote(self) -> None:
        with self.assertRaises(errors.DomainError) as ctx:
            self.backend.quotes.request_quote(
                tenant_id="t-1", cards=999, start_at=self.clock.now(), duration_seconds=600)
        self.assertEqual(ctx.exception.code, "capacity_exceeded")


if __name__ == "__main__":
    unittest.main()
