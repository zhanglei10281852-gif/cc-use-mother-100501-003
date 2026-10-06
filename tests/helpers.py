"""测试共享辅助。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from compute_reservation import Backend, ManualClock  # noqa: E402

T0 = 1_700_000_000.0


def make_backend(db_path: str = ":memory:", start: float = T0) -> tuple[Backend, ManualClock]:
    clock = ManualClock(start)
    return Backend(db_path, clock=clock), clock


def publish(backend: Backend, clock: ManualClock, *, node: str = "node-a", cards: int = 8,
            fault_domain: str = "fd-1", energy_tier: str = "P2", price: float = 10.0,
            capabilities: list[str] | None = None, hours: float = 24,
            inventory_ref: str | None = None) -> dict:
    return backend.catalog.publish_batch(
        node_id=node, total_cards=cards, valid_from=clock.now() - 1, valid_until=clock.now() + hours * 3600,
        fault_domain=fault_domain, energy_tier=energy_tier, capabilities=capabilities or [],
        price_per_card_hour=price, inventory_ref=inventory_ref)


def quote_and_reserve(backend: Backend, clock: ManualClock, *, tenant: str = "tenant-1",
                      cards: int = 4, duration: float = 3600.0, idem_key: str = "k-1",
                      capabilities: list[str] | None = None, node_id: str | None = None,
                      **contract) -> dict:
    quote = backend.quotes.request_quote(
        tenant_id=tenant, cards=cards, start_at=clock.now(), duration_seconds=duration,
        required_capabilities=capabilities or [], node_id=node_id)
    return backend.reservations.reserve(
        tenant_id=tenant, quote_id=quote["quote_id"], idem_key=idem_key, **contract)


def allocated(backend: Backend, batch_id: str) -> int:
    row = backend.store.one("SELECT allocated_cards FROM capacity_ledger WHERE batch_id = ?", (batch_id,))
    return row["allocated_cards"]


def open_segments(backend: Backend, reservation_id: str) -> list[dict]:
    return backend.store.query(
        "SELECT * FROM billing_segments WHERE reservation_id = ? AND end_at IS NULL", (reservation_id,))


def all_segments(backend: Backend, reservation_id: str) -> list[dict]:
    return backend.store.query(
        "SELECT * FROM billing_segments WHERE reservation_id = ? ORDER BY start_at, rowid", (reservation_id,))
