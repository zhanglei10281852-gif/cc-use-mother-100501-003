"""准入路径核心服务：容量目录、可复核报价、幂等预留、任务组调度。"""

from __future__ import annotations

import json
import sqlite3

from . import errors, pricing
from .models import (
    BatchState,
    GroupState,
    QuoteState,
    ReservationState,
    SegmentCloseReason,
    TaskState,
    canonical_json,
    new_id,
    stable_hash,
)
from .store import Store, decode_json_columns

QUOTE_TTL_SECONDS = 900.0
DEFAULT_EXTENSION_SECONDS = 3600.0

BATCH_JSON_COLUMNS = ("capabilities",)
QUOTE_JSON_COLUMNS = ("required_caps", "breakdown")
RESERVATION_JSON_COLUMNS = ("required_caps",)


def get_batch(store: Store, batch_id: str) -> dict:
    row = store.one("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
    if row is None:
        raise errors.not_found(f"容量批次 {batch_id} 不存在")
    return decode_json_columns(row, BATCH_JSON_COLUMNS)


def get_reservation(store: Store, reservation_id: str) -> dict:
    row = store.one("SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,))
    if row is None:
        raise errors.not_found(f"预留 {reservation_id} 不存在")
    return decode_json_columns(row, RESERVATION_JSON_COLUMNS)


def allocate(conn: sqlite3.Connection, batch_id: str, cards: int) -> None:
    """事务内条件分配：容量不足时影响行数为零，拒绝超卖。"""
    try:
        cursor = conn.execute(
            """
            UPDATE capacity_ledger SET allocated_cards = allocated_cards + ?
            WHERE batch_id = ?
              AND allocated_cards + ? <= (SELECT total_cards FROM batches WHERE batch_id = ?)
            """,
            (cards, batch_id, cards, batch_id),
        )
    except sqlite3.IntegrityError as exc:  # 触发器兜底
        raise errors.conflict(f"批次 {batch_id} 剩余容量不足", "capacity_exceeded") from exc
    if cursor.rowcount != 1:
        raise errors.conflict(f"批次 {batch_id} 剩余容量不足，无法分配 {cards} 张卡", "capacity_exceeded")


def release_allocation(conn: sqlite3.Connection, batch_id: str, cards: int) -> None:
    cursor = conn.execute(
        "UPDATE capacity_ledger SET allocated_cards = allocated_cards - ? WHERE batch_id = ? AND allocated_cards - ? >= 0",
        (cards, batch_id, cards),
    )
    if cursor.rowcount != 1:
        raise errors.conflict(f"批次 {batch_id} 台账释放越界", "ledger_underflow")


def open_segment(
    store: Store,
    conn: sqlite3.Connection,
    *,
    reservation_id: str,
    node_id: str,
    batch_id: str,
    cards: int,
    unit_price: float,
    start_at: float,
    open_reason: str,
    tenant_id: str | None = None,
) -> dict:
    segment = {
        "segment_id": new_id("seg"),
        "reservation_id": reservation_id,
        "node_id": node_id,
        "batch_id": batch_id,
        "cards": cards,
        "unit_price": unit_price,
        "start_at": start_at,
        "end_at": None,
        "close_reason": None,
        "open_reason": open_reason,
    }
    conn.execute(
        """
        INSERT INTO billing_segments(segment_id, reservation_id, node_id, batch_id, cards,
                                     unit_price, start_at, end_at, close_reason, open_reason)
        VALUES(:segment_id, :reservation_id, :node_id, :batch_id, :cards,
               :unit_price, :start_at, :end_at, :close_reason, :open_reason)
        """,
        segment,
    )
    store.audit(
        conn,
        actor="system:billing",
        type_="segment.opened",
        entity_id=segment["segment_id"],
        tenant_id=tenant_id,
        payload={"reservation_id": reservation_id, "node_id": node_id, "cards": cards, "reason": open_reason},
    )
    return segment


def close_open_segment(
    store: Store,
    conn: sqlite3.Connection,
    reservation_id: str,
    *,
    end_at: float,
    close_reason: str,
    tenant_id: str | None = None,
) -> dict | None:
    segment = store.one(
        "SELECT * FROM billing_segments WHERE reservation_id = ? AND end_at IS NULL",
        (reservation_id,),
    )
    if segment is None:
        return None
    conn.execute(
        "UPDATE billing_segments SET end_at = ?, close_reason = ? WHERE segment_id = ?",
        (end_at, close_reason, segment["segment_id"]),
    )
    store.audit(
        conn,
        actor="system:billing",
        type_="segment.closed",
        entity_id=segment["segment_id"],
        tenant_id=tenant_id,
        payload={"reservation_id": reservation_id, "end_at": end_at, "reason": close_reason},
    )
    segment["end_at"] = end_at
    segment["close_reason"] = close_reason
    return segment


class CatalogService:
    """节点容量批次的发布、查询与状态变更。"""

    def __init__(self, store: Store) -> None:
        self.store = store

    def publish_batch(
        self,
        *,
        node_id: str,
        total_cards: int,
        valid_from: float,
        valid_until: float,
        fault_domain: str,
        energy_tier: str,
        capabilities: list[str],
        price_per_card_hour: float,
        inventory_ref: str | None = None,
    ) -> dict:
        if not node_id.strip():
            raise errors.validation("节点标识不能为空")
        if total_cards < 1:
            raise errors.validation("批次卡数必须大于零")
        if valid_until <= valid_from:
            raise errors.validation("批次有效期必须为正区间")
        if energy_tier not in pricing.ENERGY_FACTORS:
            raise errors.validation(f"未知能耗等级 {energy_tier}")
        if price_per_card_hour <= 0:
            raise errors.validation("单价必须大于零")
        caps = sorted({c.strip() for c in capabilities if c.strip()})

        batch_id = new_id("bat")
        now = self.store.clock.now()
        try:
            with self.store.tx() as conn:
                conn.execute(
                    """
                    INSERT INTO batches(batch_id, node_id, inventory_ref, total_cards, valid_from, valid_until,
                                        fault_domain, energy_tier, capabilities, price_per_card_hour, state, created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        batch_id,
                        node_id,
                        inventory_ref,
                        total_cards,
                        valid_from,
                        valid_until,
                        fault_domain,
                        energy_tier,
                        canonical_json(caps),
                        price_per_card_hour,
                        BatchState.ACTIVE,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO capacity_ledger(batch_id, allocated_cards) VALUES(?, 0)",
                    (batch_id,),
                )
                self.store.audit(
                    conn,
                    actor=f"node:{node_id}",
                    type_="batch.published",
                    entity_id=batch_id,
                    payload={
                        "node_id": node_id,
                        "total_cards": total_cards,
                        "fault_domain": fault_domain,
                        "energy_tier": energy_tier,
                        "capabilities": caps,
                        "inventory_ref": inventory_ref,
                    },
                )
        except sqlite3.IntegrityError as exc:
            if "inventory_ref" in str(exc):
                raise errors.conflict(
                    f"库存引用 {inventory_ref} 已被发布，同一批加速卡不能重复上架",
                    "inventory_conflicts",
                ) from exc
            raise
        return get_batch(self.store, batch_id)

    def list_batches(self, *, state: str | None = None, node_id: str | None = None) -> list[dict]:
        sql = """
            SELECT b.*, l.allocated_cards FROM batches b
            JOIN capacity_ledger l ON l.batch_id = b.batch_id
        """
        clauses, params = [], []
        if state:
            clauses.append("b.state = ?")
            params.append(state)
        if node_id:
            clauses.append("b.node_id = ?")
            params.append(node_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY b.created_at, b.batch_id"
        rows = self.store.query(sql, tuple(params))
        for row in rows:
            decode_json_columns(row, BATCH_JSON_COLUMNS)
            row["free_cards"] = row["total_cards"] - row["allocated_cards"]
        return rows


class QuoteService:
    """可复核报价：价格完全由输入决定，指纹可重算验证。"""

    def __init__(self, store: Store) -> None:
        self.store = store

    def request_quote(
        self,
        *,
        tenant_id: str,
        cards: int,
        start_at: float,
        duration_seconds: float,
        required_capabilities: list[str] | None = None,
        fault_domain: str | None = None,
        node_id: str | None = None,
        quote_ttl: float = QUOTE_TTL_SECONDS,
    ) -> dict:
        if not tenant_id.strip():
            raise errors.validation("租户标识不能为空")
        if cards < 1:
            raise errors.validation("卡数必须大于零")
        if duration_seconds <= 0:
            raise errors.validation("时长必须大于零")
        end_at = start_at + duration_seconds
        required = sorted({c.strip() for c in (required_capabilities or []) if c.strip()})

        candidates = []
        for batch in self.catalog_view():
            if batch["state"] != BatchState.ACTIVE:
                continue
            if batch["valid_from"] > start_at or batch["valid_until"] < end_at:
                continue
            if batch["free_cards"] < cards:
                continue
            if fault_domain and batch["fault_domain"] != fault_domain:
                continue
            if node_id and batch["node_id"] != node_id:
                continue
            if not set(required).issubset(set(batch["capabilities"])):
                continue
            candidates.append(batch)
        if not candidates:
            raise errors.conflict("没有满足时间窗、能力与容量要求的可用批次", "capacity_exceeded")

        # 选价最低者，价格相同则取先发布者，保证选择确定性。
        candidates.sort(key=lambda b: (b["price_per_card_hour"], b["created_at"], b["batch_id"]))
        chosen = candidates[0]
        breakdown = pricing.price_quote(
            unit_price=chosen["price_per_card_hour"],
            energy_tier=chosen["energy_tier"],
            batch_capabilities=chosen["capabilities"],
            requested_capabilities=required,
            cards=cards,
            duration_seconds=duration_seconds,
        )
        inputs = {
            "batch_id": chosen["batch_id"],
            "cards": cards,
            "start_at": start_at,
            "duration_seconds": duration_seconds,
            "required_capabilities": required,
            "batch_snapshot": {
                "unit_price": chosen["price_per_card_hour"],
                "energy_tier": chosen["energy_tier"],
                "capabilities": chosen["capabilities"],
                "fault_domain": chosen["fault_domain"],
                "node_id": chosen["node_id"],
            },
        }
        inputs_hash = stable_hash(inputs)
        fingerprint = stable_hash({"inputs_hash": inputs_hash, "breakdown": breakdown.to_dict()})

        quote_id = new_id("quo")
        now = self.store.clock.now()
        with self.store.tx() as conn:
            conn.execute(
                """
                INSERT INTO quotes(quote_id, tenant_id, batch_id, cards, start_at, duration_seconds,
                                   required_caps, breakdown, inputs_hash, fingerprint, expires_at, state, created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    quote_id,
                    tenant_id,
                    chosen["batch_id"],
                    cards,
                    start_at,
                    duration_seconds,
                    canonical_json(required),
                    canonical_json({"inputs": inputs, "price": breakdown.to_dict()}),
                    inputs_hash,
                    fingerprint,
                    now + quote_ttl,
                    QuoteState.OPEN,
                    now,
                ),
            )
            self.store.audit(
                conn,
                actor=f"tenant:{tenant_id}",
                type_="quote.created",
                entity_id=quote_id,
                tenant_id=tenant_id,
                payload={"batch_id": chosen["batch_id"], "cards": cards, "total": breakdown.to_dict()["total"]},
            )
        return self.get_quote(quote_id)

    def catalog_view(self) -> list[dict]:
        rows = self.store.query(
            """
            SELECT b.*, l.allocated_cards FROM batches b
            JOIN capacity_ledger l ON l.batch_id = b.batch_id
            """
        )
        for row in rows:
            decode_json_columns(row, BATCH_JSON_COLUMNS)
            row["free_cards"] = row["total_cards"] - row["allocated_cards"]
        return rows

    def get_quote(self, quote_id: str) -> dict:
        row = self.store.one("SELECT * FROM quotes WHERE quote_id = ?", (quote_id,))
        if row is None:
            raise errors.not_found(f"报价 {quote_id} 不存在")
        return decode_json_columns(row, QUOTE_JSON_COLUMNS)

    def verify_quote(self, quote_id: str) -> dict:
        """用存储的输入重算价格与指纹，供租户复核报价未被篡改。"""
        quote = self.get_quote(quote_id)
        inputs = quote["breakdown"]["inputs"]
        snapshot = inputs["batch_snapshot"]
        recomputed = pricing.price_quote(
            unit_price=snapshot["unit_price"],
            energy_tier=snapshot["energy_tier"],
            batch_capabilities=snapshot["capabilities"],
            requested_capabilities=inputs["required_capabilities"],
            cards=inputs["cards"],
            duration_seconds=inputs["duration_seconds"],
        )
        recomputed_inputs_hash = stable_hash(inputs)
        recomputed_fingerprint = stable_hash(
            {"inputs_hash": recomputed_inputs_hash, "breakdown": recomputed.to_dict()}
        )
        now = self.store.clock.now()
        return {
            "quote_id": quote_id,
            "inputs_intact": recomputed_inputs_hash == quote["inputs_hash"],
            "price_matches": recomputed.to_dict() == quote["breakdown"]["price"],
            "fingerprint_matches": recomputed_fingerprint == quote["fingerprint"],
            "recomputed_fingerprint": recomputed_fingerprint,
            "stored_fingerprint": quote["fingerprint"],
            "state": quote["state"],
            "expired": quote["expires_at"] <= now,
        }


class ReservationService:
    """幂等预留：凭有效报价在事务内锁定配额。"""

    def __init__(self, store: Store) -> None:
        self.store = store

    def reserve(
        self,
        *,
        tenant_id: str,
        quote_id: str,
        idem_key: str,
        auto_renew: bool = False,
        max_renewals: int = 0,
        migratable: bool = True,
        interruptible: bool = True,
        shrink_on_partial: bool = True,
        extension_seconds: float = DEFAULT_EXTENSION_SECONDS,
        tenant_priority: int = 100,
    ) -> dict:
        if not idem_key or not idem_key.strip():
            raise errors.validation("幂等键不能为空")
        request_hash = stable_hash(
            {
                "op": "reserve",
                "tenant_id": tenant_id,
                "quote_id": quote_id,
                "auto_renew": auto_renew,
                "max_renewals": max_renewals,
                "migratable": migratable,
                "interruptible": interruptible,
                "shrink_on_partial": shrink_on_partial,
                "extension_seconds": extension_seconds,
                "tenant_priority": tenant_priority,
            }
        )
        now = self.store.clock.now()
        with self.store.tx() as conn:
            existing = conn.execute(
                "SELECT * FROM idempotency_keys WHERE idem_key = ? AND tenant_id = ?",
                (idem_key, tenant_id),
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise errors.conflict(
                        f"幂等键 {idem_key} 已绑定不同请求内容", "idempotency_conflict"
                    )
                return {**json.loads(existing["response"]), "replayed": True}

            quote = conn.execute("SELECT * FROM quotes WHERE quote_id = ?", (quote_id,)).fetchone()
            if quote is None:
                raise errors.not_found(f"报价 {quote_id} 不存在")
            if quote["tenant_id"] != tenant_id:
                raise errors.forbidden("报价不属于该租户")
            if quote["state"] == QuoteState.CONSUMED:
                raise errors.conflict(f"报价 {quote_id} 已被使用", "quote_consumed")
            if quote["state"] != QuoteState.OPEN:
                raise errors.conflict(f"报价 {quote_id} 状态为 {quote['state']}，不可用", "invalid_state")
            if quote["expires_at"] <= now:
                conn.execute("UPDATE quotes SET state = ? WHERE quote_id = ?", (QuoteState.EXPIRED, quote_id))
                raise errors.conflict(f"报价 {quote_id} 已过期", "quote_expired")

            batch = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (quote["batch_id"],)).fetchone()
            if batch is None or batch["state"] != BatchState.ACTIVE:
                raise errors.conflict("报价对应批次已不可用", "batch_unavailable")
            if batch["valid_from"] > quote["start_at"] or batch["valid_until"] < quote["start_at"] + quote["duration_seconds"]:
                raise errors.conflict("批次有效期已无法覆盖报价区间", "batch_unavailable")

            allocate(conn, quote["batch_id"], quote["cards"])
            conn.execute("UPDATE quotes SET state = ? WHERE quote_id = ?", (QuoteState.CONSUMED, quote_id))

            breakdown = json.loads(quote["breakdown"])
            unit_price = float(breakdown["price"]["effective_unit_price"])
            reservation_id = new_id("res")
            start_at = max(quote["start_at"], now)
            end_at = start_at + quote["duration_seconds"]
            conn.execute(
                """
                INSERT INTO reservations(reservation_id, tenant_id, quote_id, batch_id, node_id, cards,
                                         start_at, end_at, base_duration, extension_seconds, locked_unit_price,
                                         required_caps, state, auto_renew, max_renewals, renewals_used,
                                         migratable, interruptible, shrink_on_partial, tenant_priority,
                                         idem_key, restored_from, version, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    reservation_id,
                    tenant_id,
                    quote_id,
                    quote["batch_id"],
                    batch["node_id"],
                    quote["cards"],
                    start_at,
                    end_at,
                    quote["duration_seconds"],
                    extension_seconds,
                    unit_price,
                    quote["required_caps"],
                    ReservationState.ACTIVE,
                    int(auto_renew),
                    max_renewals,
                    0,
                    int(migratable),
                    int(interruptible),
                    int(shrink_on_partial),
                    tenant_priority,
                    idem_key,
                    None,
                    1,
                    now,
                    now,
                ),
            )
            open_segment(
                self.store,
                conn,
                reservation_id=reservation_id,
                node_id=batch["node_id"],
                batch_id=quote["batch_id"],
                cards=quote["cards"],
                unit_price=unit_price,
                start_at=start_at,
                open_reason="RESERVE",
                tenant_id=tenant_id,
            )
            self.store.audit(
                conn,
                actor=f"tenant:{tenant_id}",
                type_="reservation.created",
                entity_id=reservation_id,
                tenant_id=tenant_id,
                payload={
                    "quote_id": quote_id,
                    "batch_id": quote["batch_id"],
                    "node_id": batch["node_id"],
                    "cards": quote["cards"],
                    "end_at": end_at,
                },
            )
            reservation = decode_json_columns(
                dict(conn.execute("SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)).fetchone()),
                RESERVATION_JSON_COLUMNS,
            )
            conn.execute(
                "INSERT INTO idempotency_keys(idem_key, tenant_id, request_hash, response, created_at) VALUES(?,?,?,?,?)",
                (idem_key, tenant_id, request_hash, canonical_json(reservation), now),
            )
        return reservation

    def get(self, tenant_id: str, reservation_id: str) -> dict:
        reservation = get_reservation(self.store, reservation_id)
        if reservation["tenant_id"] != tenant_id:
            raise errors.forbidden("预留不属于该租户")
        reservation["segments"] = self.store.query(
            "SELECT * FROM billing_segments WHERE reservation_id = ? ORDER BY start_at, rowid",
            (reservation_id,),
        )
        reservation["tasks"] = self.store.query(
            "SELECT * FROM tasks WHERE tenant_id = ? AND group_id IN (SELECT group_id FROM task_groups WHERE reservation_id = ?) ORDER BY rowid",
            (tenant_id, reservation_id),
        )
        return reservation

    def list_for_tenant(self, tenant_id: str) -> list[dict]:
        rows = self.store.query(
            "SELECT * FROM reservations WHERE tenant_id = ? ORDER BY created_at, reservation_id",
            (tenant_id,),
        )
        return [decode_json_columns(row, RESERVATION_JSON_COLUMNS) for row in rows]

    def release(self, *, tenant_id: str, reservation_id: str, reason: str = "tenant_release") -> dict:
        now = self.store.clock.now()
        with self.store.tx() as conn:
            reservation = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if reservation is None:
                raise errors.not_found(f"预留 {reservation_id} 不存在")
            if reservation["tenant_id"] != tenant_id:
                raise errors.forbidden("预留不属于该租户")
            if reservation["state"] != ReservationState.ACTIVE:
                raise errors.conflict(f"预留状态为 {reservation['state']}，不能释放", "invalid_state")
            _release_in_tx(self.store, conn, dict(reservation), now=now, close_reason=SegmentCloseReason.RELEASE,
                           final_state=ReservationState.RELEASED, actor=f"tenant:{tenant_id}",
                           detail={"reason": reason})
        return get_reservation(self.store, reservation_id)


def _release_in_tx(
    store: Store,
    conn: sqlite3.Connection,
    reservation: dict,
    *,
    now: float,
    close_reason: str,
    final_state: str,
    actor: str,
    detail: dict | None = None,
) -> None:
    """在调用方事务内释放预留：关闭计费段、回收台账、中断未完成任务。"""
    close_open_segment(store, conn, reservation["reservation_id"], end_at=now,
                       close_reason=close_reason, tenant_id=reservation["tenant_id"])
    release_allocation(conn, reservation["batch_id"], reservation["cards"])
    conn.execute(
        "UPDATE reservations SET state = ?, version = version + 1, updated_at = ? WHERE reservation_id = ?",
        (final_state, now, reservation["reservation_id"]),
    )
    interrupted = conn.execute(
        """
        SELECT task_id FROM tasks
        WHERE group_id IN (SELECT group_id FROM task_groups WHERE reservation_id = ?)
          AND state IN (?, ?, ?)
        """,
        (reservation["reservation_id"], TaskState.WAITING, TaskState.READY, TaskState.RUNNING),
    ).fetchall()
    for row in interrupted:
        conn.execute("UPDATE tasks SET state = ? WHERE task_id = ?", (TaskState.INTERRUPTED, row["task_id"]))
        store.audit(
            conn,
            actor=actor,
            type_="task.interrupted",
            entity_id=row["task_id"],
            tenant_id=reservation["tenant_id"],
            payload={"reservation_id": reservation["reservation_id"], "reason": close_reason},
        )
    conn.execute(
        """
        UPDATE task_groups SET state = ?, detail = ?
        WHERE reservation_id = ? AND state IN (?, ?, ?)
        """,
        (
            GroupState.FAILED,
            canonical_json({"reason": close_reason, **(detail or {})}),
            reservation["reservation_id"],
            GroupState.ADMITTED,
            GroupState.RUNNING,
            GroupState.PARTIAL,
        ),
    )
    store.audit(
        conn,
        actor=actor,
        type_=f"reservation.{final_state.lower()}",
        entity_id=reservation["reservation_id"],
        tenant_id=reservation["tenant_id"],
        payload={"close_reason": close_reason, **(detail or {})},
    )


class TaskService:
    """任务组提交、依赖推进与部分完成收缩。"""

    def __init__(self, store: Store) -> None:
        self.store = store

    def submit_group(
        self,
        *,
        tenant_id: str,
        reservation_id: str,
        tasks: list[dict],
        idem_key: str | None = None,
    ) -> dict:
        if not tasks:
            raise errors.validation("任务组不能为空")
        names = [t.get("name", "") for t in tasks]
        if any(not n.strip() for n in names) or len(set(names)) != len(names):
            raise errors.validation("任务名称必须非空且在组内唯一")
        for spec in tasks:
            if int(spec.get("cards", 0)) < 1:
                raise errors.validation(f"任务 {spec.get('name')} 卡数必须大于零")
            unknown = set(spec.get("depends_on", [])) - set(names)
            if unknown:
                raise errors.validation(f"任务 {spec.get('name')} 依赖了不存在的任务 {sorted(unknown)}")
        self._assert_acyclic(tasks)

        request_hash = stable_hash({"op": "submit_group", "tenant_id": tenant_id,
                                    "reservation_id": reservation_id, "tasks": tasks})
        now = self.store.clock.now()
        with self.store.tx() as conn:
            if idem_key:
                existing = conn.execute(
                    "SELECT * FROM idempotency_keys WHERE idem_key = ? AND tenant_id = ?",
                    (idem_key, tenant_id),
                ).fetchone()
                if existing is not None:
                    if existing["request_hash"] != request_hash:
                        raise errors.conflict(f"幂等键 {idem_key} 已绑定不同请求内容", "idempotency_conflict")
                    return {**json.loads(existing["response"]), "replayed": True}

            reservation = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if reservation is None:
                raise errors.not_found(f"预留 {reservation_id} 不存在")
            if reservation["tenant_id"] != tenant_id:
                raise errors.forbidden("预留不属于该租户")
            if reservation["state"] != ReservationState.ACTIVE:
                raise errors.conflict(f"预留状态为 {reservation['state']}，不能提交任务组", "invalid_state")
            for spec in tasks:
                if int(spec["cards"]) > reservation["cards"]:
                    raise errors.validation(
                        f"任务 {spec['name']} 需要 {spec['cards']} 张卡，超出预留配额 {reservation['cards']}"
                    )

            group_id = new_id("grp")
            conn.execute(
                "INSERT INTO task_groups(group_id, tenant_id, reservation_id, state, detail, created_at) VALUES(?,?,?,?,?,?)",
                (group_id, tenant_id, reservation_id, GroupState.ADMITTED, None, now),
            )
            for spec in tasks:
                conn.execute(
                    """
                    INSERT INTO tasks(task_id, group_id, tenant_id, name, cards, state, depends_on, node_id, started_at, completed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        new_id("tsk"),
                        group_id,
                        tenant_id,
                        spec["name"],
                        int(spec["cards"]),
                        TaskState.WAITING,
                        canonical_json(sorted(spec.get("depends_on", []))),
                        None,
                        None,
                        None,
                    ),
                )
            self.store.audit(
                conn,
                actor=f"tenant:{tenant_id}",
                type_="task_group.submitted",
                entity_id=group_id,
                tenant_id=tenant_id,
                payload={"reservation_id": reservation_id, "tasks": tasks},
            )
            self._promote_locked(store=self.store, conn=conn, group_id=group_id, now=now)
            group = self._group_view(conn, group_id)
            if idem_key:
                conn.execute(
                    "INSERT INTO idempotency_keys(idem_key, tenant_id, request_hash, response, created_at) VALUES(?,?,?,?,?)",
                    (idem_key, tenant_id, request_hash, canonical_json(group), now),
                )
        return group

    @staticmethod
    def _assert_acyclic(tasks: list[dict]) -> None:
        graph = {t["name"]: list(t.get("depends_on", [])) for t in tasks}
        visiting, done = set(), set()

        def visit(node: str, path: list[str]) -> None:
            if node in done:
                return
            if node in visiting:
                raise errors.validation(f"任务依赖存在环：{' -> '.join(path + [node])}")
            visiting.add(node)
            for dep in graph[node]:
                visit(dep, path + [node])
            visiting.discard(node)
            done.add(node)

        for name in graph:
            visit(name, [])

    @staticmethod
    def _promote_locked(*, store: Store, conn: sqlite3.Connection, group_id: str, now: float) -> None:
        """把依赖已满足的任务置为 READY，并在预留配额内启动 READY 任务。"""
        rows = conn.execute(
            "SELECT * FROM tasks WHERE group_id = ? ORDER BY rowid", (group_id,)
        ).fetchall()
        by_name = {r["name"]: dict(r) for r in rows}
        for task in rows:
            if task["state"] != TaskState.WAITING:
                continue
            deps = json.loads(task["depends_on"])
            if all(by_name[d]["state"] == TaskState.COMPLETED for d in deps):
                conn.execute("UPDATE tasks SET state = ? WHERE task_id = ?", (TaskState.READY, task["task_id"]))
                by_name[task["name"]]["state"] = TaskState.READY

        group = conn.execute("SELECT * FROM task_groups WHERE group_id = ?", (group_id,)).fetchone()
        reservation = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = ?", (group["reservation_id"],)
        ).fetchone()
        running_cards = conn.execute(
            """
            SELECT COALESCE(SUM(cards), 0) AS used FROM tasks
            WHERE group_id IN (SELECT group_id FROM task_groups WHERE reservation_id = ?) AND state = ?
            """,
            (reservation["reservation_id"], TaskState.RUNNING),
        ).fetchone()["used"]
        for task in rows:
            fresh = by_name[task["name"]]
            if fresh["state"] != TaskState.READY:
                continue
            if running_cards + fresh["cards"] > reservation["cards"]:
                continue
            conn.execute(
                "UPDATE tasks SET state = ?, node_id = ?, started_at = ? WHERE task_id = ?",
                (TaskState.RUNNING, reservation["node_id"], now, fresh["task_id"]),
            )
            running_cards += fresh["cards"]
            store.audit(
                conn,
                actor="system:scheduler",
                type_="task.started",
                entity_id=fresh["task_id"],
                tenant_id=group["tenant_id"],
                payload={"group_id": group_id, "node_id": reservation["node_id"], "cards": fresh["cards"]},
            )
        started = conn.execute(
            "SELECT COUNT(*) AS n FROM tasks WHERE group_id = ? AND state = ?",
            (group_id, TaskState.RUNNING),
        ).fetchone()["n"]
        if started:
            conn.execute(
                "UPDATE task_groups SET state = ? WHERE group_id = ? AND state = ?",
                (GroupState.RUNNING, group_id, GroupState.ADMITTED),
            )

    def complete_task(self, *, tenant_id: str, task_id: str) -> dict:
        now = self.store.clock.now()
        with self.store.tx() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if task is None:
                raise errors.not_found(f"任务 {task_id} 不存在")
            if task["tenant_id"] != tenant_id:
                raise errors.forbidden("任务不属于该租户")
            if task["state"] == TaskState.COMPLETED:
                return self._task_view(conn, task_id)
            if task["state"] != TaskState.RUNNING:
                raise errors.conflict(f"任务状态为 {task['state']}，不能完成", "invalid_state")
            conn.execute(
                "UPDATE tasks SET state = ?, completed_at = ? WHERE task_id = ?",
                (TaskState.COMPLETED, now, task_id),
            )
            self.store.audit(
                conn,
                actor=f"tenant:{tenant_id}",
                type_="task.completed",
                entity_id=task_id,
                tenant_id=tenant_id,
                payload={"group_id": task["group_id"]},
            )
            self._promote_locked(store=self.store, conn=conn, group_id=task["group_id"], now=now)
            self._refresh_group_state(conn, task["group_id"])
            group = conn.execute("SELECT * FROM task_groups WHERE group_id = ?", (task["group_id"],)).fetchone()
            self._maybe_shrink(conn=conn, reservation_id=group["reservation_id"], now=now)
            return self._task_view(conn, task_id)

    def _refresh_group_state(self, conn: sqlite3.Connection, group_id: str) -> None:
        states = [
            r["state"]
            for r in conn.execute("SELECT state FROM tasks WHERE group_id = ?", (group_id,)).fetchall()
        ]
        if not states:
            return
        if all(s == TaskState.COMPLETED for s in states):
            new_state = GroupState.COMPLETED
        elif any(s == TaskState.COMPLETED for s in states):
            new_state = GroupState.PARTIAL
        else:
            return
        conn.execute("UPDATE task_groups SET state = ? WHERE group_id = ?", (new_state, group_id))

    def _maybe_shrink(self, *, conn: sqlite3.Connection, reservation_id: str, now: float) -> None:
        """部分完成时按合同收缩配额：关闭旧段、开启新段，保证占用与计费一致。"""
        reservation = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
        ).fetchone()
        if reservation is None or reservation["state"] != ReservationState.ACTIVE:
            return
        if not reservation["shrink_on_partial"]:
            return
        running_cards = conn.execute(
            """
            SELECT COALESCE(SUM(t.cards), 0) AS used FROM tasks t
            JOIN task_groups g ON g.group_id = t.group_id
            WHERE g.reservation_id = ? AND t.state = ?
            """,
            (reservation_id, TaskState.RUNNING),
        ).fetchone()["used"]
        pending_max = conn.execute(
            """
            SELECT COALESCE(MAX(t.cards), 0) AS need FROM tasks t
            JOIN task_groups g ON g.group_id = t.group_id
            WHERE g.reservation_id = ? AND t.state IN (?, ?)
            """,
            (reservation_id, TaskState.WAITING, TaskState.READY),
        ).fetchone()["need"]
        target = max(int(running_cards), int(pending_max))
        if target < 1 or target >= reservation["cards"]:
            return
        freed = reservation["cards"] - target
        close_open_segment(self.store, conn, reservation_id, end_at=now,
                           close_reason=SegmentCloseReason.RESIZE,
                           tenant_id=reservation["tenant_id"])
        open_segment(
            self.store,
            conn,
            reservation_id=reservation_id,
            node_id=reservation["node_id"],
            batch_id=reservation["batch_id"],
            cards=target,
            unit_price=reservation["locked_unit_price"],
            start_at=now,
            open_reason="RESIZE",
            tenant_id=reservation["tenant_id"],
        )
        release_allocation(conn, reservation["batch_id"], freed)
        conn.execute(
            "UPDATE reservations SET cards = ?, version = version + 1, updated_at = ? WHERE reservation_id = ?",
            (target, now, reservation_id),
        )
        self.store.audit(
            conn,
            actor="system:lifecycle",
            type_="reservation.resized",
            entity_id=reservation_id,
            tenant_id=reservation["tenant_id"],
            payload={"from_cards": reservation["cards"], "to_cards": target, "freed": freed},
        )

    def _group_view(self, conn: sqlite3.Connection, group_id: str) -> dict:
        group = dict(conn.execute("SELECT * FROM task_groups WHERE group_id = ?", (group_id,)).fetchone())
        group["tasks"] = [
            self._task_view(conn, r["task_id"])
            for r in conn.execute("SELECT task_id FROM tasks WHERE group_id = ? ORDER BY rowid", (group_id,)).fetchall()
        ]
        return group

    @staticmethod
    def _task_view(conn: sqlite3.Connection, task_id: str) -> dict:
        task = dict(conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone())
        task["depends_on"] = json.loads(task["depends_on"])
        return task

    def get_group(self, tenant_id: str, group_id: str) -> dict:
        row = self.store.one("SELECT group_id FROM task_groups WHERE group_id = ? AND tenant_id = ?", (group_id, tenant_id))
        if row is None:
            raise errors.not_found(f"任务组 {group_id} 不存在")
        with self.store.tx() as conn:
            return self._group_view(conn, group_id)
