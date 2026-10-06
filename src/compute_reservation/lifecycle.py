"""生命周期引擎：到期处置、节点降级、可中断迁移、抢占与恢复。

合同规则决策树（预留到期且仍有未完成任务时）：
1. 允许自动续约且批次仍可用 -> 续约（轮换计费段、按当前批次价重算费率）；
2. 否则允许迁移 -> 选择健康批次做可中断迁移，延长 end_at；
3. 否则 -> 到期释放，未完成任务中断。
节点降级时：可迁移 -> 迁移到不同故障域；不可迁移 -> 抢占并记录补偿与恢复次序。
"""

from __future__ import annotations

import json
import sqlite3

from . import errors, pricing
from .models import (
    BatchState,
    GroupState,
    MigrationReason,
    MigrationState,
    PreemptionState,
    QuoteState,
    ReservationState,
    SegmentCloseReason,
    TaskState,
    canonical_json,
    new_id,
)
from .services import (
    RESERVATION_JSON_COLUMNS,
    _release_in_tx,
    allocate,
    close_open_segment,
    get_reservation,
    open_segment,
    release_allocation,
)
from .store import Store, decode_json_columns


class LifecycleEngine:
    def __init__(self, store: Store) -> None:
        self.store = store

    # ------------------------------------------------------------------ 到期清扫

    def sweep(self, now: float | None = None) -> dict:
        """到期清扫：报价过期、批次退役、预留按合同规则续约/迁移/释放。"""
        now = self.store.clock.now() if now is None else now
        summary = {"quotes_expired": 0, "batches_retired": 0, "renewed": [], "migrated": [], "expired": []}

        with self.store.tx() as conn:
            rows = conn.execute(
                "SELECT quote_id FROM quotes WHERE state = ? AND expires_at <= ?",
                (QuoteState.OPEN, now),
            ).fetchall()
            for row in rows:
                conn.execute("UPDATE quotes SET state = ? WHERE quote_id = ?", (QuoteState.EXPIRED, row["quote_id"]))
                self.store.audit(conn, actor="system:lifecycle", type_="quote.expired", entity_id=row["quote_id"])
            summary["quotes_expired"] = len(rows)

        with self.store.tx() as conn:
            rows = conn.execute(
                "SELECT batch_id FROM batches WHERE state = ? AND valid_until <= ?",
                (BatchState.ACTIVE, now),
            ).fetchall()
            batch_ids = [r["batch_id"] for r in rows]
            for batch_id in batch_ids:
                conn.execute("UPDATE batches SET state = ? WHERE batch_id = ?", (BatchState.RETIRED, batch_id))
                self.store.audit(conn, actor="system:lifecycle", type_="batch.retired", entity_id=batch_id)
            summary["batches_retired"] = len(batch_ids)
        for batch_id in batch_ids:
            self._evacuate_batch(batch_id, reason=MigrationReason.BATCH_RETIRED, require_domain_change=False)

        active = self.store.query(
            "SELECT * FROM reservations WHERE state = ? AND end_at <= ? ORDER BY end_at, reservation_id",
            (ReservationState.ACTIVE, now),
        )
        for row in active:
            reservation = decode_json_columns(row, RESERVATION_JSON_COLUMNS)
            outcome = self._expire_reservation(reservation, now)
            summary[outcome].append(reservation["reservation_id"])
        return summary

    def _has_unfinished_tasks(self, reservation_id: str) -> bool:
        row = self.store.one(
            """
            SELECT COUNT(*) AS pending FROM tasks t
            JOIN task_groups g ON g.group_id = t.group_id
            WHERE g.reservation_id = ? AND t.state IN (?, ?, ?)
            """,
            (reservation_id, TaskState.WAITING, TaskState.READY, TaskState.RUNNING),
        )
        return bool(row and row["pending"] > 0)

    def _expire_reservation(self, reservation: dict, now: float) -> str:
        """按合同规则处理到期预留，返回 renewed / migrated / expired。"""
        if not self._has_unfinished_tasks(reservation["reservation_id"]):
            with self.store.tx() as conn:
                fresh = conn.execute(
                    "SELECT * FROM reservations WHERE reservation_id = ?", (reservation["reservation_id"],)
                ).fetchone()
                if fresh["state"] != ReservationState.ACTIVE:
                    return "expired"
                _release_in_tx(self.store, conn, dict(fresh), now=now, close_reason=SegmentCloseReason.EXPIRE,
                               final_state=ReservationState.EXPIRED, actor="system:lifecycle",
                               detail={"reason": "合同到期且无未完成任务"})
            return "expired"

        if reservation["auto_renew"] and reservation["renewals_used"] < reservation["max_renewals"]:
            try:
                self.renew(tenant_id=reservation["tenant_id"], reservation_id=reservation["reservation_id"],
                           actor="system:lifecycle")
                return "renewed"
            except errors.DomainError:
                pass  # 续约失败则继续尝试迁移

        if reservation["migratable"]:
            try:
                migration = self.plan_migration(
                    reservation_id=reservation["reservation_id"],
                    reason=MigrationReason.EXPIRY,
                    horizon_seconds=reservation["extension_seconds"],
                    require_domain_change=False,
                )
                self.execute_migration(migration["migration_id"])
                with self.store.tx() as conn:
                    conn.execute(
                        "UPDATE reservations SET end_at = ?, version = version + 1, updated_at = ? WHERE reservation_id = ?",
                        (now + reservation["extension_seconds"], now, reservation["reservation_id"]),
                    )
                return "migrated"
            except errors.DomainError:
                pass  # 迁移无果则释放

        with self.store.tx() as conn:
            fresh = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation["reservation_id"],)
            ).fetchone()
            if fresh["state"] in (ReservationState.ACTIVE, ReservationState.MIGRATING):
                _release_in_tx(self.store, conn, dict(fresh), now=now, close_reason=SegmentCloseReason.EXPIRE,
                               final_state=ReservationState.EXPIRED, actor="system:lifecycle",
                               detail={"reason": "合同到期，续约与迁移均不可用"})
        return "expired"

    def renew(self, *, tenant_id: str, reservation_id: str, actor: str | None = None) -> dict:
        """续约：按当前批次价格重算费率并轮换计费段。"""
        actor = actor or f"tenant:{tenant_id}"
        now = self.store.clock.now()
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if row is None:
                raise errors.not_found(f"预留 {reservation_id} 不存在")
            reservation = dict(row)
            if reservation["tenant_id"] != tenant_id:
                raise errors.forbidden("预留不属于该租户")
            if reservation["state"] != ReservationState.ACTIVE:
                raise errors.conflict(f"预留状态为 {reservation['state']}，不能续约", "invalid_state")
            if reservation["renewals_used"] >= reservation["max_renewals"]:
                raise errors.conflict("续约次数已用尽", "renewal_exhausted")
            batch = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (reservation["batch_id"],)).fetchone()
            if batch is None or batch["state"] != BatchState.ACTIVE:
                raise errors.conflict("原批次已不可用，无法续约", "batch_unavailable")
            new_end = max(now, reservation["end_at"]) + reservation["base_duration"]
            if batch["valid_until"] < new_end:
                raise errors.conflict("批次有效期无法覆盖续约区间", "batch_unavailable")

            required = json.loads(reservation["required_caps"])
            breakdown = pricing.price_quote(
                unit_price=batch["price_per_card_hour"],
                energy_tier=batch["energy_tier"],
                batch_capabilities=json.loads(batch["capabilities"]),
                requested_capabilities=required,
                cards=reservation["cards"],
                duration_seconds=reservation["base_duration"],
            )
            new_rate = float(breakdown.to_dict()["effective_unit_price"])
            close_open_segment(self.store, conn, reservation_id, end_at=now,
                               close_reason=SegmentCloseReason.RENEWAL, tenant_id=tenant_id)
            open_segment(
                self.store, conn,
                reservation_id=reservation_id,
                node_id=reservation["node_id"],
                batch_id=reservation["batch_id"],
                cards=reservation["cards"],
                unit_price=new_rate,
                start_at=now,
                open_reason="RENEWAL",
                tenant_id=tenant_id,
            )
            conn.execute(
                """
                UPDATE reservations SET end_at = ?, locked_unit_price = ?, renewals_used = renewals_used + 1,
                                        version = version + 1, updated_at = ?
                WHERE reservation_id = ?
                """,
                (new_end, new_rate, now, reservation_id),
            )
            self.store.audit(
                conn, actor=actor, type_="reservation.renewed", entity_id=reservation_id,
                tenant_id=tenant_id,
                payload={"new_end_at": new_end, "new_unit_price": new_rate,
                         "renewals_used": reservation["renewals_used"] + 1},
            )
        return get_reservation(self.store, reservation_id)

    # ------------------------------------------------------------------ 迁移

    def _select_target_batch(
        self,
        *,
        cards: int,
        required_caps: list[str],
        horizon_seconds: float,
        exclude_node: str | None,
        exclude_fault_domain: str | None,
        now: float,
    ) -> dict | None:
        rows = self.store.query(
            """
            SELECT b.*, l.allocated_cards FROM batches b
            JOIN capacity_ledger l ON l.batch_id = b.batch_id
            WHERE b.state = ? AND b.valid_from <= ? AND b.valid_until >= ?
              AND b.total_cards - l.allocated_cards >= ?
            ORDER BY b.price_per_card_hour, b.created_at, b.batch_id
            """,
            (BatchState.ACTIVE, now, now + horizon_seconds, cards),
        )
        for row in rows:
            caps = json.loads(row["capabilities"])
            if not set(required_caps).issubset(set(caps)):
                continue
            if exclude_node and row["node_id"] == exclude_node:
                continue
            if exclude_fault_domain and row["fault_domain"] == exclude_fault_domain:
                continue
            return row
        return None

    def plan_migration(
        self,
        *,
        reservation_id: str,
        reason: str,
        horizon_seconds: float,
        require_domain_change: bool,
    ) -> dict:
        """规划迁移：先落库 PLANNED，崩溃后由恢复流程继续执行。"""
        now = self.store.clock.now()
        with self.store.tx() as conn:
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if row is None:
                raise errors.not_found(f"预留 {reservation_id} 不存在")
            reservation = dict(row)
            if reservation["state"] != ReservationState.ACTIVE:
                raise errors.conflict(f"预留状态为 {reservation['state']}，不能迁移", "invalid_state")
            source_batch = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (reservation["batch_id"],)
            ).fetchone()
            target = self._select_target_batch(
                cards=reservation["cards"],
                required_caps=json.loads(reservation["required_caps"]),
                horizon_seconds=horizon_seconds,
                exclude_node=reservation["node_id"],
                exclude_fault_domain=source_batch["fault_domain"] if require_domain_change else None,
                now=now,
            )
            if target is None:
                raise errors.conflict("没有可用的迁移目标批次", "capacity_exceeded")
            migration_id = new_id("mig")
            mode = "interruptible" if reservation["interruptible"] else "live"
            conn.execute(
                """
                INSERT INTO migrations(migration_id, reservation_id, from_node, to_node, from_batch, to_batch,
                                       reason, mode, state, planned_at, cutover_at, detail)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    migration_id, reservation_id, reservation["node_id"], target["node_id"],
                    reservation["batch_id"], target["batch_id"], reason, mode,
                    MigrationState.PLANNED, now, None, None,
                ),
            )
            conn.execute(
                "UPDATE reservations SET state = ?, version = version + 1, updated_at = ? WHERE reservation_id = ?",
                (ReservationState.MIGRATING, now, reservation_id),
            )
            self.store.audit(
                conn, actor="system:lifecycle", type_="migration.planned", entity_id=migration_id,
                tenant_id=reservation["tenant_id"],
                payload={"reservation_id": reservation_id, "from_node": reservation["node_id"],
                         "to_node": target["node_id"], "reason": reason, "mode": mode},
            )
        return self.get_migration(migration_id)

    def execute_migration(self, migration_id: str) -> dict:
        """执行切换：同一事务内关旧段、开新段、搬台账，保证任一瞬间只有一个计费点。"""
        now = self.store.clock.now()
        with self.store.tx() as conn:
            migration = conn.execute(
                "SELECT * FROM migrations WHERE migration_id = ?", (migration_id,)
            ).fetchone()
            if migration is None:
                raise errors.not_found(f"迁移 {migration_id} 不存在")
            if migration["state"] == MigrationState.COMPLETED:
                return dict(migration)  # 幂等：已完成的迁移直接返回
            if migration["state"] != MigrationState.PLANNED:
                raise errors.conflict(f"迁移状态为 {migration['state']}，不能执行", "invalid_state")
            reservation = dict(conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (migration["reservation_id"],)
            ).fetchone())
            try:
                allocate(conn, migration["to_batch"], reservation["cards"])
            except errors.DomainError:
                conn.execute(
                    "UPDATE migrations SET state = ?, detail = ? WHERE migration_id = ?",
                    (MigrationState.FAILED, "目标批次容量不足", migration_id),
                )
                conn.execute(
                    "UPDATE reservations SET state = ?, version = version + 1, updated_at = ? WHERE reservation_id = ?",
                    (ReservationState.ACTIVE, now, reservation["reservation_id"]),
                )
                self.store.audit(
                    conn, actor="system:lifecycle", type_="migration.failed", entity_id=migration_id,
                    tenant_id=reservation["tenant_id"], payload={"reason": "目标批次容量不足"},
                )
                raise errors.conflict("迁移目标批次容量不足", "capacity_exceeded")

            close_open_segment(self.store, conn, reservation["reservation_id"],
                               end_at=now, close_reason=SegmentCloseReason.MIGRATION,
                               tenant_id=reservation["tenant_id"])
            open_segment(
                self.store, conn,
                reservation_id=reservation["reservation_id"],
                node_id=migration["to_node"],
                batch_id=migration["to_batch"],
                cards=reservation["cards"],
                unit_price=reservation["locked_unit_price"],
                start_at=now,
                open_reason="MIGRATION",
                tenant_id=reservation["tenant_id"],
            )
            release_allocation(conn, migration["from_batch"], reservation["cards"])
            conn.execute(
                """
                UPDATE reservations SET node_id = ?, batch_id = ?, state = ?,
                                        version = version + 1, updated_at = ?
                WHERE reservation_id = ?
                """,
                (migration["to_node"], migration["to_batch"], ReservationState.ACTIVE, now,
                 reservation["reservation_id"]),
            )
            conn.execute(
                "UPDATE migrations SET state = ?, cutover_at = ? WHERE migration_id = ?",
                (MigrationState.COMPLETED, now, migration_id),
            )
            # 运行中的任务随预留切换到新节点。
            conn.execute(
                """
                UPDATE tasks SET node_id = ?
                WHERE state = ? AND group_id IN (SELECT group_id FROM task_groups WHERE reservation_id = ?)
                """,
                (migration["to_node"], TaskState.RUNNING, reservation["reservation_id"]),
            )
            self.store.audit(
                conn, actor="system:lifecycle", type_="migration.completed", entity_id=migration_id,
                tenant_id=reservation["tenant_id"],
                payload={"reservation_id": reservation["reservation_id"], "cutover_at": now,
                         "from_node": migration["from_node"], "to_node": migration["to_node"]},
            )
        return self.get_migration(migration_id)

    def get_migration(self, migration_id: str) -> dict:
        row = self.store.one("SELECT * FROM migrations WHERE migration_id = ?", (migration_id,))
        if row is None:
            raise errors.not_found(f"迁移 {migration_id} 不存在")
        return row

    def recover_pending_migrations(self) -> list[str]:
        """重启恢复：把仍处 PLANNED 的迁移继续执行完毕。"""
        pending = self.store.query(
            "SELECT migration_id FROM migrations WHERE state = ? ORDER BY planned_at, migration_id",
            (MigrationState.PLANNED,),
        )
        resumed = []
        for row in pending:
            try:
                self.execute_migration(row["migration_id"])
                resumed.append(row["migration_id"])
            except errors.DomainError:
                continue
        return resumed

    # ------------------------------------------------------------------ 降级与抢占

    def degrade_node(self, *, node_id: str, reason: str, actor: str = "system:ops") -> dict:
        """节点降级：批次置为 DEGRADED，可迁移预留迁往其他故障域，否则抢占。"""
        now = self.store.clock.now()
        with self.store.tx() as conn:
            batches = conn.execute(
                "SELECT batch_id FROM batches WHERE node_id = ? AND state = ?", (node_id, BatchState.ACTIVE)
            ).fetchall()
            if not batches:
                raise errors.not_found(f"节点 {node_id} 没有可用批次")
            for batch in batches:
                conn.execute("UPDATE batches SET state = ? WHERE batch_id = ?", (BatchState.DEGRADED, batch["batch_id"]))
                self.store.audit(conn, actor=actor, type_="batch.degraded", entity_id=batch["batch_id"],
                                 payload={"node_id": node_id, "reason": reason})
            conn.execute(
                """
                UPDATE quotes SET state = ?
                WHERE state = ? AND batch_id IN (SELECT batch_id FROM batches WHERE node_id = ?)
                """,
                (QuoteState.INVALIDATED, QuoteState.OPEN, node_id),
            )

        migrated, preempted = [], []
        reservations = self.store.query(
            """
            SELECT r.* FROM reservations r
            JOIN batches b ON b.batch_id = r.batch_id
            WHERE b.node_id = ? AND r.state = ?
            ORDER BY r.tenant_priority, r.created_at, r.reservation_id
            """,
            (node_id, ReservationState.ACTIVE),
        )
        to_preempt = []
        for row in reservations:
            reservation = decode_json_columns(row, RESERVATION_JSON_COLUMNS)
            if reservation["migratable"]:
                try:
                    migration = self.plan_migration(
                        reservation_id=reservation["reservation_id"],
                        reason=MigrationReason.DEGRADATION,
                        horizon_seconds=max(reservation["end_at"] - now, reservation["extension_seconds"]),
                        require_domain_change=True,
                    )
                    self.execute_migration(migration["migration_id"])
                    migrated.append(reservation["reservation_id"])
                    continue
                except errors.DomainError:
                    pass
            to_preempt.append(reservation["reservation_id"])
        preemption = None
        if to_preempt:
            preemption = self.preempt(reservation_ids=to_preempt,
                                      reason=f"节点降级：{reason}", created_by=actor)
            preempted = to_preempt
        return {"node_id": node_id, "migrated": migrated, "preempted": preempted,
                "preemption": preemption}

    def restore_node(self, *, node_id: str, actor: str = "system:ops") -> dict:
        with self.store.tx() as conn:
            cursor = conn.execute(
                "UPDATE batches SET state = ? WHERE node_id = ? AND state = ?",
                (BatchState.ACTIVE, node_id, BatchState.DEGRADED),
            )
            self.store.audit(conn, actor=actor, type_="node.restored", entity_id=node_id,
                             payload={"batches_reactivated": cursor.rowcount})
        return {"node_id": node_id, "batches_reactivated": cursor.rowcount}

    def preempt(self, *, reservation_ids: list[str], reason: str, created_by: str) -> dict:
        """抢占：记录受影响租户、补偿金额与恢复次序。"""
        now = self.store.clock.now()
        with self.store.tx() as conn:
            items = []
            for reservation_id in reservation_ids:
                row = conn.execute(
                    "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
                ).fetchone()
                if row is None:
                    raise errors.not_found(f"预留 {reservation_id} 不存在")
                if row["state"] != ReservationState.ACTIVE:
                    continue
                items.append(dict(row))
            if not items:
                raise errors.conflict("没有可抢占的活动预留", "invalid_state")
            # 恢复次序：优先级数值小者优先恢复，同级按创建时间先后。
            items.sort(key=lambda r: (r["tenant_priority"], r["created_at"], r["reservation_id"]))
            record_items = []
            for rank, reservation in enumerate(items, start=1):
                remaining = max(0.0, reservation["end_at"] - now)
                compensation = pricing.compensation_amount(
                    cards=reservation["cards"],
                    remaining_seconds=remaining,
                    effective_unit_price=reservation["locked_unit_price"],
                )
                record_items.append({
                    "reservation_id": reservation["reservation_id"],
                    "tenant_id": reservation["tenant_id"],
                    "node_id": reservation["node_id"],
                    "cards": reservation["cards"],
                    "remaining_seconds": remaining,
                    "compensation": str(compensation),
                    "recovery_rank": rank,
                    "restored": False,
                })
            preemption_id = new_id("pre")
            conn.execute(
                "INSERT INTO preemptions(preemption_id, reason, created_by, created_at, state, items) VALUES(?,?,?,?,?,?)",
                (preemption_id, reason, created_by, now, PreemptionState.ISSUED, canonical_json(record_items)),
            )
            for reservation in items:
                _release_in_tx(self.store, conn, reservation, now=now,
                               close_reason=SegmentCloseReason.PREEMPT,
                               final_state=ReservationState.PREEMPTED, actor=created_by,
                               detail={"preemption_id": preemption_id})
            self.store.audit(
                conn, actor=created_by, type_="preemption.issued", entity_id=preemption_id,
                payload={"reason": reason, "items": record_items},
            )
        return self.get_preemption(preemption_id)

    def get_preemption(self, preemption_id: str) -> dict:
        row = self.store.one("SELECT * FROM preemptions WHERE preemption_id = ?", (preemption_id,))
        if row is None:
            raise errors.not_found(f"抢占记录 {preemption_id} 不存在")
        return decode_json_columns(row, ("items",))

    def restore_preemption(self, *, preemption_id: str, actor: str = "system:ops") -> dict:
        """按记录的恢复次序尝试重新接纳被抢占的预留。"""
        record = self.get_preemption(preemption_id)
        now = self.store.clock.now()
        restored, still_pending = [], []
        items = sorted(record["items"], key=lambda i: i["recovery_rank"])
        for item in items:
            if item["restored"]:
                continue
            reservation = get_reservation(self.store, item["reservation_id"])
            if reservation["state"] != ReservationState.PREEMPTED:
                item["restored"] = True
                continue
            target = self._select_target_batch(
                cards=reservation["cards"],
                required_caps=reservation["required_caps"],
                horizon_seconds=max(item["remaining_seconds"], 60.0),
                exclude_node=None,
                exclude_fault_domain=None,
                now=now,
            )
            if target is None:
                still_pending.append(item["reservation_id"])
                continue
            with self.store.tx() as conn:
                allocate(conn, target["batch_id"], reservation["cards"])
                open_segment(
                    self.store, conn,
                    reservation_id=reservation["reservation_id"],
                    node_id=target["node_id"],
                    batch_id=target["batch_id"],
                    cards=reservation["cards"],
                    unit_price=reservation["locked_unit_price"],
                    start_at=now,
                    open_reason="RESTORE",
                    tenant_id=reservation["tenant_id"],
                )
                conn.execute(
                    """
                    UPDATE reservations SET state = ?, node_id = ?, batch_id = ?, end_at = ?,
                                            version = version + 1, updated_at = ?
                    WHERE reservation_id = ?
                    """,
                    (ReservationState.ACTIVE, target["node_id"], target["batch_id"],
                     now + item["remaining_seconds"], now, reservation["reservation_id"]),
                )
                self.store.audit(
                    conn, actor=actor, type_="preemption.restored", entity_id=preemption_id,
                    tenant_id=reservation["tenant_id"],
                    payload={"reservation_id": reservation["reservation_id"],
                             "recovery_rank": item["recovery_rank"], "node_id": target["node_id"]},
                )
            item["restored"] = True
            restored.append(reservation["reservation_id"])

        new_state = PreemptionState.CLOSED if not still_pending else PreemptionState.RESTORING
        with self.store.tx() as conn:
            conn.execute(
                "UPDATE preemptions SET state = ?, items = ? WHERE preemption_id = ?",
                (new_state, canonical_json(items), preemption_id),
            )
        result = self.get_preemption(preemption_id)
        result["restored"] = restored
        result["pending"] = still_pending
        return result

    def _evacuate_batch(self, batch_id: str, *, reason: str, require_domain_change: bool) -> None:
        """批次不可用时疏散其上的活动预留。"""
        now = self.store.clock.now()
        reservations = self.store.query(
            "SELECT * FROM reservations WHERE batch_id = ? AND state = ? ORDER BY tenant_priority, created_at",
            (batch_id, ReservationState.ACTIVE),
        )
        to_preempt = []
        for row in reservations:
            reservation = decode_json_columns(row, RESERVATION_JSON_COLUMNS)
            if reservation["migratable"]:
                try:
                    migration = self.plan_migration(
                        reservation_id=reservation["reservation_id"],
                        reason=reason,
                        horizon_seconds=max(reservation["end_at"] - now, reservation["extension_seconds"]),
                        require_domain_change=require_domain_change,
                    )
                    self.execute_migration(migration["migration_id"])
                    continue
                except errors.DomainError:
                    pass
            to_preempt.append(reservation["reservation_id"])
        if to_preempt:
            self.preempt(reservation_ids=to_preempt, reason=f"批次退役：{batch_id}",
                         created_by="system:lifecycle")
