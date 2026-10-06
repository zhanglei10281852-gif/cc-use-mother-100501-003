"""跨节点算力预留与履约命令行冒烟入口。

默认输出基础契约示例；加 --demo 运行一次端到端内存演示
（发布批次 -> 报价 -> 幂等预留 -> 任务组 -> 用量 -> 账单 -> 履约轨迹）。
"""

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from compute_reservation import Backend, ComputeReservation, ManualClock


def smoke() -> None:
    item = ComputeReservation(reservation_code='reservation-code-001', tenant_code='tenant-code-001', capacity_batch='capacity-batch-001', state='state-001')
    print(json.dumps({"item": asdict(item), "fingerprint": item.fingerprint()}, ensure_ascii=False, sort_keys=True))


def demo() -> None:
    clock = ManualClock(1_700_000_000.0)
    backend = Backend(":memory:", clock=clock)
    batch = backend.catalog.publish_batch(
        node_id="node-a", total_cards=8, valid_from=clock.now(), valid_until=clock.now() + 86400,
        fault_domain="fd-1", energy_tier="P2", capabilities=["training"], price_per_card_hour=12.0,
        inventory_ref="inv-a-001")
    quote = backend.quotes.request_quote(
        tenant_id="tenant-1", cards=4, start_at=clock.now(), duration_seconds=7200,
        required_capabilities=["training"])
    reservation = backend.reservations.reserve(
        tenant_id="tenant-1", quote_id=quote["quote_id"], idem_key="demo-reserve-1",
        auto_renew=True, max_renewals=1)
    group = backend.tasks.submit_group(
        tenant_id="tenant-1", reservation_id=reservation["reservation_id"],
        tasks=[{"name": "prepare", "cards": 2, "depends_on": []},
               {"name": "train", "cards": 4, "depends_on": ["prepare"]}])
    clock.advance(1800)
    backend.tasks.complete_task(tenant_id="tenant-1", task_id=group["tasks"][0]["task_id"])
    clock.advance(1800)
    backend.billing.ingest_events(events=[{
        "event_id": "evt-1", "reservation_id": reservation["reservation_id"],
        "task_id": group["tasks"][0]["task_id"], "node_id": batch["node_id"],
        "cards": 2, "usage_start": 1_700_000_000.0, "usage_end": 1_700_001_800.0, "energy_kwh": 3.2}])
    bill = backend.billing.generate_bill(
        tenant_id="tenant-1", reservation_id=reservation["reservation_id"],
        period_start=1_700_000_000.0, period_end=clock.now(), finalize=True)
    trail = backend.trail_service.trail(tenant_id="tenant-1")
    print(json.dumps({
        "batch_id": batch["batch_id"],
        "quote_fingerprint": quote["fingerprint"],
        "reservation_id": reservation["reservation_id"],
        "group_state": group["state"],
        "bill_total": bill["total"],
        "trail_sections": sorted(k for k, v in trail.items() if isinstance(v, list) and v),
        "timeline_events": len(trail["timeline"]),
    }, ensure_ascii=False, sort_keys=True, indent=2))


def main() -> None:
    if "--demo" in sys.argv[1:]:
        demo()
    else:
        smoke()


if __name__ == "__main__":
    main()
