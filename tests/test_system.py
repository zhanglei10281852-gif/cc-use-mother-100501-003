"""跨节点算力预留与履约核心行为测试。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from compute_reservation import DomainError, Store, System
from compute_reservation.models import parse_time

BASE = "2026-10-06T00:00:00Z"
WINDOW_FROM = "2026-10-05T00:00:00Z"
WINDOW_UNTIL = "2026-10-10T00:00:00Z"
GPU_RESERVED_MEDIUM = 12.0  # 元/(单元·小时)，默认费率卡


class Clock:
    def __init__(self, start: str = BASE) -> None:
        self.moment = parse_time(start)

    def __call__(self):
        return self.moment

    def advance(self, hours: float) -> None:
        self.moment += timedelta(hours=hours)

    def iso(self) -> str:
        from compute_reservation.models import format_time

        return format_time(self.moment)


def end_after(hours: float) -> str:
    from compute_reservation.models import add_hours

    return add_hours(BASE, hours)


class SystemCase(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.store = Store(":memory:")
        self.sys = System(self.store, now_fn=self.clock, actor="test")

    def tearDown(self) -> None:
        self.store.close()

    # -- 测试辅助 ---------------------------------------------------------

    def make_node(self, name="node-a", fault_domain="room-a", energy="medium", caps=None):
        return self.sys.register_node(name, fault_domain, energy, caps or ["rdma"])

    def make_batch(self, node_code, units=4, resource="gpu", until=WINDOW_UNTIL, caps=None):
        return self.sys.publish_batch(node_code, resource, units, WINDOW_FROM, until, caps)

    def lock(self, tenant, batch, units, end=None, key=None, contract=None, mode="reserved"):
        return self.sys.lock_reservation(
            idempotency_key=key or f"{tenant}-{batch['batch_code']}-{units}",
            tenant_code=tenant,
            batch_code=batch["batch_code"],
            units=units,
            end_at=end or end_after(24),
            billing_mode=mode,
            contract=contract,
        )

    def assertDomainError(self, code, fn, *args, **kwargs):
        with self.assertRaises(DomainError) as ctx:
            fn(*args, **kwargs)
        self.assertEqual(ctx.exception.code, code)
        return ctx.exception


class QuoteAndLockTests(SystemCase):
    def test_quote_is_reviewable_and_verifiable(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=8)
        quote = self.sys.create_quote("tenant-a", batch["batch_code"], 2, BASE, end_after(10))
        self.assertEqual(quote["snapshot"]["batch"]["available_units"], 8)
        self.assertEqual(quote["amount_cents"], round(2 * GPU_RESERVED_MEDIUM * 10 * 100))
        verification = self.sys.verify_quote(quote["quote_code"])
        self.assertTrue(verification["match"])
        self.assertEqual(verification["snapshot_hash"], quote["snapshot_hash"])

    def test_lock_is_idempotent_and_conflict_detected(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"])
        first = self.lock("tenant-a", batch, 2, key="k-1")
        replay = self.lock("tenant-a", batch, 2, key="k-1")
        self.assertEqual(first["reservation_code"], replay["reservation_code"])
        self.assertTrue(replay["idempotent_replay"])
        # 同键不同内容 → 拒绝，防止重试改单
        self.assertDomainError("idempotency_conflict", self.lock, "tenant-a", batch, 3, key="k-1")
        # 重放不重复占用容量
        self.assertEqual(self.sys.get_batch(batch["batch_code"])["allocated_units"], 2)

    def test_oversell_is_rejected_atomically(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=4)
        self.lock("tenant-a", batch, 3)
        self.assertDomainError("capacity_insufficient", self.lock, "tenant-b", batch, 2)
        view = self.sys.get_batch(batch["batch_code"])
        self.assertEqual(view["allocated_units"], 3)
        self.assertEqual(view["available_units"], 1)

    def test_quote_has_single_winner(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"])
        quote = self.sys.create_quote("tenant-a", batch["batch_code"], 2, BASE, end_after(10))
        winner = self.sys.lock_reservation(idempotency_key="w-1", quote_code=quote["quote_code"])
        self.assertEqual(winner["state"], "ACTIVE")
        self.assertDomainError(
            "quote_consumed",
            self.sys.lock_reservation,
            idempotency_key="w-2",
            quote_code=quote["quote_code"],
        )

    def test_batch_window_overlap_rejected(self):
        node = self.make_node()
        self.make_batch(node["node_code"])
        self.assertDomainError(
            "batch_window_overlap",
            self.sys.publish_batch,
            node["node_code"],
            "gpu",
            2,
            "2026-10-07T00:00:00Z",
            "2026-10-12T00:00:00Z",
        )
        # 不同资源类型不冲突
        other = self.sys.publish_batch(node["node_code"], "cpu", 64, WINDOW_FROM, WINDOW_UNTIL)
        self.assertEqual(other["state"], "OPEN")

    def test_renew_rules_and_optimistic_concurrency(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], until="2026-10-10T00:00:00Z")
        rsv = self.lock("tenant-a", batch, 2, end=end_after(24))
        renewed = self.sys.renew_reservation(rsv["reservation_code"], end_after(48))
        self.assertEqual(renewed["end_at"], end_after(48))
        # 超出批次有效期 → 拒绝
        self.assertDomainError(
            "window_outside_batch",
            self.sys.renew_reservation,
            rsv["reservation_code"],
            "2026-10-11T00:00:00Z",
        )
        # 乐观并发：版本不符 → 冲突
        stale = renewed["version"]
        self.sys.renew_reservation(rsv["reservation_code"], end_after(60))
        self.assertDomainError(
            "version_conflict",
            self.sys.renew_reservation,
            rsv["reservation_code"],
            end_after(70),
            expected_version=stale,
        )


class ExpiryTests(SystemCase):
    def test_expiry_release_reclaims_capacity_and_bills(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=4)
        rsv = self.lock("tenant-a", batch, 2, end=end_after(5))  # on_expiry 默认 release
        self.clock.advance(6)
        summary = self.sys.sweep()
        self.assertIn(rsv["reservation_code"], summary["expired"])
        view = self.sys.get_reservation(rsv["reservation_code"])
        self.assertEqual(view["state"], "EXPIRED")
        self.assertEqual(self.sys.get_batch(batch["batch_code"])["allocated_units"], 0)
        # 账单按实际占用 5 小时结算
        bills = self.sys.list_bills("tenant-a")
        self.assertEqual(len(bills), 1)
        self.assertEqual(bills[0]["amount_cents"], round(2 * GPU_RESERVED_MEDIUM * 5 * 100))

    def test_expiry_auto_renew_within_batch_validity(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], until="2026-10-08T00:00:00Z")
        rsv = self.lock(
            "tenant-a", batch, 2, end=end_after(5),
            contract={"on_expiry": "renew", "renew_extension_hours": 6},
        )
        self.clock.advance(6)
        summary = self.sys.sweep()
        self.assertIn(rsv["reservation_code"], summary["renewed"])
        self.assertEqual(self.sys.get_reservation(rsv["reservation_code"])["end_at"], end_after(11))

    def test_expiry_migrates_when_contract_says_migrate(self):
        node_a = self.make_node("node-a", "room-a")
        node_b = self.make_node("node-b", "room-b")
        batch_a = self.sys.publish_batch(node_a["node_code"], "gpu", 4, WINDOW_FROM, end_after(5))
        batch_b = self.make_batch(node_b["node_code"], until="2026-10-09T00:00:00Z")
        rsv = self.lock(
            "tenant-a", batch_a, 2, end=end_after(5),
            contract={"on_expiry": "migrate", "renew_extension_hours": 6},
        )
        self.clock.advance(6)
        summary = self.sys.sweep()
        self.assertIn(rsv["reservation_code"], summary["migrated"])
        view = self.sys.get_reservation(rsv["reservation_code"])
        self.assertEqual(view["state"], "ACTIVE")
        self.assertEqual(view["batch_code"], batch_b["batch_code"])
        self.assertEqual(view["end_at"], end_after(12))
        # 源批次配额已释放
        self.assertEqual(self.sys.get_batch(batch_a["batch_code"])["allocated_units"], 0)

    def test_forced_expiry_compensates_when_contract_cannot_be_honored(self):
        node = self.make_node()
        batch = self.sys.publish_batch(node["node_code"], "gpu", 4, WINDOW_FROM, end_after(5))
        rsv = self.lock(
            "tenant-a", batch, 2, end=end_after(5),
            contract={"on_expiry": "renew", "renew_extension_hours": 6},
        )
        self.clock.advance(6)
        self.sys.sweep()  # 批次已到期无法续约，也无迁移目标 → 强制到期 + 补偿
        view = self.sys.get_reservation(rsv["reservation_code"])
        self.assertEqual(view["state"], "EXPIRED")
        credits = [
            e for e in self.sys.list_ledger(rsv["reservation_code"])
            if e["entry_type"] == "credit" and e["reason"] == "forced_expiry"
        ]
        self.assertEqual(len(credits), 1)
        self.assertLess(credits[0]["amount_cents"], 0)


class DegradeAndPreemptionTests(SystemCase):
    def test_node_degrade_triggers_interruptible_migration_with_compensation(self):
        node_a = self.make_node("node-a", "room-a")
        node_b = self.make_node("node-b", "room-b")
        batch_a = self.make_batch(node_a["node_code"])
        batch_b = self.make_batch(node_b["node_code"])
        rsv = self.lock("tenant-a", batch_a, 2, contract={"on_degrade": "migrate"})
        self.sys.set_node_state(node_a["node_code"], "DEGRADED")
        view = self.sys.get_reservation(rsv["reservation_code"])
        self.assertEqual(view["state"], "ACTIVE")
        self.assertEqual(view["batch_code"], batch_b["batch_code"])
        self.assertEqual(len(view["segments"]), 2)
        credits = [
            e for e in self.sys.list_ledger(rsv["reservation_code"])
            if e["entry_type"] == "credit" and e["reason"] == "forced_migration"
        ]
        self.assertEqual(len(credits), 1)
        migrations = self.sys.list_migrations()
        self.assertEqual(migrations[0]["reason"], "node_degraded")
        self.assertEqual(migrations[0]["state"], "COMMITTED")

    def test_node_offline_preempts_and_recovery_restores_in_order(self):
        node_a = self.make_node("node-a", "room-a")
        node_b = self.make_node("node-b", "room-b")
        batch_a = self.make_batch(node_a["node_code"], units=4)
        batch_b = self.make_batch(node_b["node_code"], units=4)
        low = self.lock("tenant-low", batch_a, 2, key="k-low", contract={"priority": 10})
        high = self.lock("tenant-high", batch_a, 2, key="k-high", contract={"priority": 200})
        self.sys.set_node_state(node_a["node_code"], "OFFLINE")
        for rsv, tenant in ((low, "tenant-low"), (high, "tenant-high")):
            view = self.sys.get_reservation(rsv["reservation_code"])
            self.assertEqual(view["state"], "PREEMPTED", tenant)
        preemptions = self.sys.list_preemptions()
        self.assertEqual(len(preemptions), 1)
        plan = preemptions[0]["plan"]
        # 恢复次序：优先级高者先恢复
        self.assertEqual(plan["recovery_order"], [high["reservation_code"], low["reservation_code"]])
        self.assertEqual(
            {v["tenant_code"] for v in plan["victims"]}, {"tenant-low", "tenant-high"}
        )
        self.assertTrue(all(v["compensation_cents"] < 0 for v in plan["victims"]))
        # 容量恢复后按序回迁
        result = self.sys.run_recovery()
        self.assertEqual(len(result["recovered"]), 2)
        for rsv in (low, high):
            view = self.sys.get_reservation(rsv["reservation_code"])
            self.assertEqual(view["state"], "ACTIVE")
            self.assertEqual(view["batch_code"], batch_b["batch_code"])

    def test_operator_preemption_preserves_victims_compensation_and_order(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=4)
        low = self.lock(
            "tenant-low", batch, 2, key="k-low",
            contract={"priority": 10, "on_preempt": "release"},
        )
        high = self.lock(
            "tenant-high", batch, 2, key="k-high",
            contract={"priority": 200, "on_preempt": "release"},
        )
        record = self.sys.preempt(batch["batch_code"], 4, reason="urgent_job")
        plan = record["plan"]
        # 低优先级先被抢占
        self.assertEqual(
            [v["reservation_code"] for v in plan["victims"]],
            [low["reservation_code"], high["reservation_code"]],
        )
        # 恢复次序高优先级在前
        self.assertEqual(
            plan["recovery_order"], [high["reservation_code"], low["reservation_code"]]
        )
        for victim in plan["victims"]:
            self.assertEqual(victim["action"], "preempted")
            self.assertLess(victim["compensation_cents"], 0)
        self.assertEqual(self.sys.get_batch(batch["batch_code"])["available_units"], 4)
        # 被抢占租户的账本上已有 capacity_loss 贷记
        for rsv in (low, high):
            credits = [
                e for e in self.sys.list_ledger(rsv["reservation_code"])
                if e["entry_type"] == "credit" and e["reason"] == "capacity_loss"
            ]
            self.assertEqual(len(credits), 1)


class MigrationBillingTests(SystemCase):
    def test_migration_never_bills_same_period_twice(self):
        node_a = self.make_node("node-a", "room-a")
        node_b = self.make_node("node-b", "room-b")
        batch_a = self.make_batch(node_a["node_code"])
        batch_b = self.make_batch(node_b["node_code"])
        rsv = self.lock("tenant-a", batch_a, 2)
        self.clock.advance(4)
        migration = self.sys.plan_migration(rsv["reservation_code"], auto_commit=True)
        self.assertEqual(migration["state"], "COMMITTED")
        self.clock.advance(6)
        self.sys.settle(reservation_code=rsv["reservation_code"])
        view = self.sys.get_reservation(rsv["reservation_code"])
        seg1, seg2 = view["segments"]
        # 区间连续且不重叠
        self.assertEqual(seg1["end_at"], seg2["start_at"])
        self.assertEqual(seg1["batch_code"], batch_a["batch_code"])
        self.assertEqual(seg2["batch_code"], batch_b["batch_code"])
        # 总账单 == 连续 10 小时 × 2 单元 × 单价，不多收一秒
        bills = self.sys.list_bills("tenant-a")
        total = sum(b["amount_cents"] for b in bills)
        self.assertEqual(total, round(2 * GPU_RESERVED_MEDIUM * 10 * 100))
        # 切换事件进入消费流，可核对
        events = self.sys.list_events(rsv["reservation_code"])
        self.assertTrue(any(
            (e["payload"] or {}).get("kind") == "migration_cutover" for e in events
        ))

    def test_abort_only_allowed_for_interruptible_migration(self):
        node_a = self.make_node("node-a", "room-a")
        node_b = self.make_node("node-b", "room-b")
        batch_a = self.make_batch(node_a["node_code"])
        self.make_batch(node_b["node_code"])
        rsv = self.lock("tenant-a", batch_a, 2, contract={"interruptible": False})
        migration = self.sys.plan_migration(rsv["reservation_code"])
        self.assertDomainError("not_interruptible", self.sys.abort_migration, migration["migration_code"])
        committed = self.sys.commit_migration(migration["migration_code"])
        self.assertEqual(committed["state"], "COMMITTED")


class EventConvergenceTests(SystemCase):
    def test_out_of_order_and_duplicate_events_converge_to_single_bill(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"])
        rsv = self.lock("tenant-a", batch, 2, mode="on_demand")
        code = rsv["reservation_code"]
        on_demand_price = round(GPU_RESERVED_MEDIUM * 1.3, 4)
        events = [
            {"event_id": "e-2", "reservation_code": code, "event_type": "usage",
             "occurred_at": end_after(2), "unit_hours": 2.0},
            {"event_id": "e-1", "reservation_code": code, "event_type": "usage",
             "occurred_at": end_after(1), "unit_hours": 3.0},  # 乱序到达
            {"event_id": "e-2", "reservation_code": code, "event_type": "usage",
             "occurred_at": end_after(2), "unit_hours": 2.0},  # 重复到达
        ]
        result = self.sys.ingest_events(events)
        statuses = [r["status"] for r in result["results"]]
        self.assertEqual(statuses, ["accepted", "accepted", "duplicate"])
        self.clock.advance(5)
        first = self.sys.settle(reservation_code=code)
        self.assertEqual(len(first["new_bills"]), 1)
        self.assertEqual(first["new_bills"][0]["amount_cents"], round(5.0 * on_demand_price * 100))
        # 重复结算收敛：不再产生新账单
        second = self.sys.settle(reservation_code=code)
        self.assertEqual(second["new_bills"], [])
        self.assertEqual(len(self.sys.list_bills("tenant-a")), 1)
        # 迟到事件（发生在已结算时段）入账后仍收敛为唯一增量账单
        self.sys.ingest_events([
            {"event_id": "e-0", "reservation_code": code, "event_type": "usage",
             "occurred_at": end_after(0.5), "unit_hours": 1.0},
        ])
        self.clock.advance(1)
        third = self.sys.settle(reservation_code=code)
        self.assertEqual(len(third["new_bills"]), 1)
        self.assertEqual(third["new_bills"][0]["amount_cents"], round(1.0 * on_demand_price * 100))
        total = sum(b["amount_cents"] for b in self.sys.list_bills("tenant-a"))
        self.assertEqual(total, round(6.0 * on_demand_price * 100))


class TaskGroupTests(SystemCase):
    def test_dag_unblocks_and_partial_completion_shrinks_then_releases(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=4)
        rsv = self.lock(
            "tenant-a", batch, 4,
            contract={"on_partial": "shrink", "on_complete": "release"},
        )
        code = rsv["reservation_code"]
        group = self.sys.submit_task_group("tenant-a", "train", [
            {"name": "prepare", "reservation_code": code, "required_units": 1},
            {"name": "train", "reservation_code": code, "required_units": 4, "depends_on": ["prepare"]},
            {"name": "evaluate", "reservation_code": code, "required_units": 2, "depends_on": ["train"]},
        ])
        states = {t["name"]: t["state"] for t in group["tasks"]}
        self.assertEqual(states, {"prepare": "READY", "train": "PENDING", "evaluate": "PENDING"})
        tasks = {t["name"]: t["task_code"] for t in group["tasks"]}

        def finish(name, result="SUCCEEDED"):
            self.clock.advance(1)
            self.sys.ingest_events([
                {"event_id": f"finish-{name}", "reservation_code": code,
                 "task_code": tasks[name], "event_type": "task_finished",
                 "occurred_at": self.clock.iso(), "payload": {"result": result}},
            ])

        finish("prepare")
        group = self.sys.get_task_group(group["group_code"])
        states = {t["name"]: t["state"] for t in group["tasks"]}
        self.assertEqual(states["train"], "READY")
        # 剩余任务最大需求 4 → 不收缩
        self.assertEqual(self.sys.get_reservation(code)["units"], 4)

        finish("train")
        # 仅剩 evaluate(2) → 按合同收缩到 2，释放 2 单元回批次
        view = self.sys.get_reservation(code)
        self.assertEqual(view["units"], 2)
        self.assertEqual(self.sys.get_batch(batch["batch_code"])["allocated_units"], 2)

        finish("evaluate")
        # 全部完成 → 按合同自动释放并出账
        view = self.sys.get_reservation(code)
        self.assertEqual(view["state"], "RELEASED")
        self.assertEqual(view["close_reason"], "tasks_completed")
        self.assertEqual(self.sys.get_batch(batch["batch_code"])["allocated_units"], 0)
        self.assertTrue(self.sys.list_bills("tenant-a"))

    def test_dependency_cycle_rejected(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"])
        rsv = self.lock("tenant-a", batch, 2)
        self.assertDomainError(
            "bad_request",
            self.sys.submit_task_group,
            "tenant-a",
            "cyclic",
            [
                {"name": "a", "reservation_code": rsv["reservation_code"], "required_units": 1, "depends_on": ["b"]},
                {"name": "b", "reservation_code": rsv["reservation_code"], "required_units": 1, "depends_on": ["a"]},
            ],
        )

    def test_failed_dependency_blocks_descendants(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"])
        rsv = self.lock("tenant-a", batch, 2, contract={"on_complete": "keep"})
        code = rsv["reservation_code"]
        group = self.sys.submit_task_group("tenant-a", "dag", [
            {"name": "a", "reservation_code": code, "required_units": 1},
            {"name": "b", "reservation_code": code, "required_units": 1, "depends_on": ["a"]},
        ])
        tasks = {t["name"]: t["task_code"] for t in group["tasks"]}
        self.sys.ingest_events([
            {"event_id": "fail-a", "reservation_code": code, "task_code": tasks["a"],
             "event_type": "task_finished", "occurred_at": self.clock.iso(),
             "payload": {"result": "FAILED"}},
        ])
        group = self.sys.get_task_group(group["group_code"])
        states = {t["name"]: t["state"] for t in group["tasks"]}
        self.assertEqual(states, {"a": "FAILED", "b": "BLOCKED"})


class ConcurrencyTests(SystemCase):
    def test_concurrent_locks_never_oversell(self):
        """事故回归：多请求同时确认同一批容量，成功数不得超过总量。"""
        import threading

        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=4)
        outcomes: list[str] = []
        barrier = threading.Barrier(8)

        def attempt(idx: int) -> None:
            barrier.wait()
            try:
                self.lock(f"tenant-{idx}", batch, 1, key=f"race-{idx}")
                outcomes.append("ok")
            except DomainError as exc:
                outcomes.append(exc.code)

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("ok"), 4)
        self.assertEqual(outcomes.count("capacity_insufficient"), 4)
        self.assertEqual(self.sys.get_batch(batch["batch_code"])["allocated_units"], 4)

    def test_concurrent_quote_consumption_single_winner(self):
        import threading

        node = self.make_node()
        batch = self.make_batch(node["node_code"], units=4)
        quote = self.sys.create_quote("tenant-a", batch["batch_code"], 2, BASE, end_after(10))
        outcomes: list[str] = []
        barrier = threading.Barrier(4)

        def attempt(idx: int) -> None:
            barrier.wait()
            try:
                self.sys.lock_reservation(
                    idempotency_key=f"qw-{idx}", quote_code=quote["quote_code"]
                )
                outcomes.append("ok")
            except DomainError as exc:
                outcomes.append(exc.code)

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("quote_consumed"), 3)


class RestartRecoveryTests(unittest.TestCase):
    def test_restart_resumes_expiry_reclaim_and_pending_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "state.db")
            clock = Clock()
            store = Store(db)
            sys1 = System(store, now_fn=clock, actor="test")
            node_a = sys1.register_node("node-a", "room-a", "medium", ["rdma"])
            node_b = sys1.register_node("node-b", "room-b", "medium", ["rdma"])
            batch_a = sys1.publish_batch(node_a["node_code"], "gpu", 4, WINDOW_FROM, WINDOW_UNTIL)
            sys1.publish_batch(node_b["node_code"], "gpu", 4, WINDOW_FROM, WINDOW_UNTIL)
            rsv = sys1.lock_reservation(
                idempotency_key="restart-1", tenant_code="tenant-a",
                batch_code=batch_a["batch_code"], units=2, end_at=end_after(3),
            )
            migration = sys1.plan_migration(rsv["reservation_code"])
            self.assertEqual(migration["state"], "PLANNED")
            store.close()  # 模拟进程退出：未决迁移与到期回收悬而未决

            # 重启：新进程基于同一库继续
            store2 = Store(db)
            clock.advance(4)  # 已过预留截止时间
            sys2 = System(store2, now_fn=clock, actor="test")
            summary = sys2.sweep()
            self.assertIn(migration["migration_code"], summary["migrations_resumed"])
            self.assertIn(rsv["reservation_code"], summary["expired"])
            view = sys2.get_reservation(rsv["reservation_code"])
            self.assertEqual(view["state"], "EXPIRED")
            self.assertEqual(len(view["segments"]), 2)  # 迁移已切换后再到期
            self.assertTrue(sys2.list_bills("tenant-a"))
            store2.close()


class TrailTests(SystemCase):
    def test_trail_covers_quote_to_settlement(self):
        node = self.make_node()
        batch = self.make_batch(node["node_code"])
        quote = self.sys.create_quote("tenant-a", batch["batch_code"], 2, BASE, end_after(10))
        rsv = self.sys.lock_reservation(idempotency_key="trail-1", quote_code=quote["quote_code"])
        self.clock.advance(3)
        self.sys.settle(tenant_code="tenant-a")
        trail = self.sys.trail("tenant-a")
        self.assertEqual(len(trail["quotes"]), 1)
        self.assertEqual(len(trail["reservations"]), 1)
        self.assertEqual(trail["reservations"][0]["reservation_code"], rsv["reservation_code"])
        self.assertEqual(len(trail["bills"]), 1)
        actions = [a["action"] for a in trail["audit"]]
        self.assertIn("quote_created", actions)
        self.assertIn("reservation_locked", actions)
        self.assertIn("bill_issued", actions)


if __name__ == "__main__":
    unittest.main()
