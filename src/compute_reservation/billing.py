"""计费与履约轨迹。

收敛保证：
- 消费事件按 event_id 去重，同 ID 不同载荷视为冲突，乱序到达不影响结果；
- 账单由（预留、账期）唯一确定，重算输入相同则指纹相同，不产生重复账单；
- 账单已终审后又收到迟到事件时，生成 ADJUSTMENT 调整单而不是篡改历史；
- 计费段半开区间 [start, end) 首尾相接，迁移/续约/收缩都不会产生重叠计费。
"""

from __future__ import annotations

import json

from . import errors, pricing
from .models import (
    BillState,
    PreemptionState,
    ReservationState,
    canonical_json,
    new_id,
    stable_hash,
)
from .services import get_reservation
from .store import Store, decode_json_columns


class BillingService:
    def __init__(self, store: Store) -> None:
        self.store = store

    # ------------------------------------------------------------------ 事件摄入

    def ingest_events(self, *, events: list[dict], actor: str = "system:ingest") -> dict:
        """批量摄入消费事件：重复事件幂等忽略，同 ID 异载荷报冲突。"""
        now = self.store.clock.now()
        results = []
        accepted = 0
        for event in events:
            results.append(self._ingest_one(event, now=now, actor=actor))
            if results[-1]["status"] == "accepted":
                accepted += 1
        return {"accepted": accepted, "total": len(events), "results": results}

    def _ingest_one(self, event: dict, *, now: float, actor: str) -> dict:
        event_id = str(event.get("event_id", "")).strip()
        if not event_id:
            return {"event_id": event_id, "status": "rejected", "reason": "event_id 不能为空"}
        payload = {
            "event_id": event_id,
            "reservation_id": event.get("reservation_id"),
            "task_id": event.get("task_id"),
            "node_id": event.get("node_id"),
            "cards": event.get("cards"),
            "usage_start": event.get("usage_start"),
            "usage_end": event.get("usage_end"),
            "energy_kwh": event.get("energy_kwh", 0),
        }
        try:
            if not payload["reservation_id"] or not payload["node_id"]:
                raise errors.validation("事件缺少 reservation_id 或 node_id")
            cards = int(payload["cards"])
            if cards < 1:
                raise errors.validation("事件卡数必须大于零")
            usage_start = float(payload["usage_start"])
            usage_end = float(payload["usage_end"])
            if usage_end <= usage_start:
                raise errors.validation("事件用量区间必须为正")
        except (TypeError, ValueError) as exc:
            return {"event_id": event_id, "status": "rejected", "reason": f"字段非法：{exc}"}
        except errors.DomainError as exc:
            return {"event_id": event_id, "status": "rejected", "reason": exc.message}

        payload_hash = stable_hash(payload)
        with self.store.tx() as conn:
            existing = conn.execute(
                "SELECT payload_hash FROM consumption_events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    return {"event_id": event_id, "status": "conflict",
                            "reason": "相同 event_id 的事件载荷不一致"}
                return {"event_id": event_id, "status": "duplicate"}
            known = conn.execute(
                "SELECT reservation_id, tenant_id FROM reservations WHERE reservation_id = ?",
                (payload["reservation_id"],),
            ).fetchone()
            if known is None:
                return {"event_id": event_id, "status": "rejected", "reason": "未知预留"}
            conn.execute(
                """
                INSERT INTO consumption_events(event_id, reservation_id, task_id, node_id, cards,
                                               usage_start, usage_end, energy_kwh, payload_hash, received_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id, payload["reservation_id"], payload.get("task_id"), payload["node_id"],
                    cards, usage_start, usage_end, float(payload.get("energy_kwh") or 0),
                    payload_hash, now,
                ),
            )
            self.store.audit(
                conn, actor=actor, type_="usage.ingested", entity_id=event_id,
                tenant_id=known["tenant_id"],
                payload={"reservation_id": payload["reservation_id"], "cards": cards,
                         "usage_start": usage_start, "usage_end": usage_end},
            )
        return {"event_id": event_id, "status": "accepted"}

    # ------------------------------------------------------------------ 账单生成

    def _segments_for(self, reservation_id: str, period_start: float, period_end: float) -> list[dict]:
        return self.store.query(
            """
            SELECT * FROM billing_segments
            WHERE reservation_id = ? AND start_at < ? AND (end_at IS NULL OR end_at > ?)
            ORDER BY start_at, rowid
            """,
            (reservation_id, period_end, period_start),
        )

    def _events_for(self, reservation_id: str, period_start: float, period_end: float) -> list[dict]:
        return self.store.query(
            """
            SELECT * FROM consumption_events
            WHERE reservation_id = ? AND usage_start < ? AND usage_end > ?
            ORDER BY event_id
            """,
            (reservation_id, period_end, period_start),
        )

    def _credits_for(self, reservation_id: str, period_start: float, period_end: float) -> list[dict]:
        """账期内发生的抢占补偿，作为负金额行进入账单。"""
        credits = []
        for row in self.store.query("SELECT * FROM preemptions ORDER BY created_at, preemption_id"):
            if not (period_start <= row["created_at"] < period_end):
                continue
            for item in json.loads(row["items"]):
                if item["reservation_id"] == reservation_id:
                    credits.append({
                        "kind": "credit",
                        "preemption_id": row["preemption_id"],
                        "recovery_rank": item["recovery_rank"],
                        "amount": str(-pricing.to_decimal(item["compensation"])),
                        "note": f"抢占补偿（{row['reason']}）",
                    })
        return credits

    def _compute_bill(self, *, reservation: dict, period_start: float, period_end: float) -> dict:
        segments = self._segments_for(reservation["reservation_id"], period_start, period_end)
        events = self._events_for(reservation["reservation_id"], period_start, period_end)
        lines: list[dict] = []
        usage = {"card_seconds": 0.0, "energy_kwh": 0.0, "event_count": 0}
        attributed: set[str] = set()

        for segment in segments:
            seg_start = max(segment["start_at"], period_start)
            seg_end = min(segment["end_at"] if segment["end_at"] is not None else period_end, period_end)
            if seg_end <= seg_start:
                continue
            window = seg_end - seg_start
            amount = pricing.occupancy_amount(
                cards=segment["cards"], seconds=window, effective_unit_price=segment["unit_price"]
            )
            lines.append({
                "kind": "occupancy",
                "segment_id": segment["segment_id"],
                "node_id": segment["node_id"],
                "cards": segment["cards"],
                "from": seg_start,
                "to": seg_end,
                "hours": str(pricing.quantize(pricing.to_decimal(window) / pricing.to_decimal(3600))),
                "unit_price": str(pricing.to_decimal(segment["unit_price"])),
                "amount": str(amount),
            })
            metered = 0.0
            for event in events:
                if event["node_id"] != segment["node_id"]:
                    continue
                overlap = min(event["usage_end"], seg_end) - max(event["usage_start"], seg_start)
                if overlap <= 0:
                    continue
                attributed.add(event["event_id"])
                card_seconds = event["cards"] * overlap
                metered += card_seconds
                usage["card_seconds"] += card_seconds
                usage["energy_kwh"] += event["energy_kwh"] * (overlap / (event["usage_end"] - event["usage_start"]))
                usage["event_count"] += 1
            entitled = segment["cards"] * window
            if metered > entitled:
                overage = pricing.occupancy_amount(
                    cards=1, seconds=metered - entitled, effective_unit_price=segment["unit_price"]
                )
                lines.append({
                    "kind": "overage",
                    "segment_id": segment["segment_id"],
                    "node_id": segment["node_id"],
                    "extra_card_seconds": str(pricing.quantize(pricing.to_decimal(metered - entitled))),
                    "amount": str(overage),
                })

        warnings = sorted(
            f"事件 {e['event_id']} 的用量窗口不落在任何计费段内，未计入"
            for e in events if e["event_id"] not in attributed
        )
        lines.extend(self._credits_for(reservation["reservation_id"], period_start, period_end))

        total = sum(pricing.to_decimal(line["amount"]) for line in lines)
        usage["card_seconds"] = float(pricing.quantize(pricing.to_decimal(usage["card_seconds"])))
        usage["energy_kwh"] = float(pricing.quantize(pricing.to_decimal(usage["energy_kwh"])))
        return {
            "lines": lines,
            "usage": usage,
            "warnings": warnings,
            "total": str(pricing.quantize(total)),
        }

    def generate_bill(
        self,
        *,
        tenant_id: str,
        reservation_id: str,
        period_start: float,
        period_end: float,
        finalize: bool = False,
    ) -> dict:
        """生成或收敛账单：同账期重复生成幂等，终审后迟到事件走调整单。"""
        if period_end <= period_start:
            raise errors.validation("账期必须为正区间")
        now = self.store.clock.now()
        if period_end > now:
            raise errors.validation("账期不能延伸到未来")
        reservation = get_reservation(self.store, reservation_id)
        if reservation["tenant_id"] != tenant_id:
            raise errors.forbidden("预留不属于该租户")

        computed = self._compute_bill(reservation=reservation, period_start=period_start, period_end=period_end)
        # 指纹基于归一化明细（剔除随机生成的段 ID），保证同样输入永远得到同样指纹。
        normalized_lines = [
            {k: v for k, v in line.items() if k != "segment_id"} for line in computed["lines"]
        ]
        fingerprint = stable_hash({
            "reservation_id": reservation_id,
            "period_start": period_start,
            "period_end": period_end,
            "lines": normalized_lines,
            "usage": computed["usage"],
        })
        with self.store.tx() as conn:
            standard = conn.execute(
                """
                SELECT * FROM bills WHERE reservation_id = ? AND period_start = ? AND period_end = ? AND kind = 'STANDARD'
                """,
                (reservation_id, period_start, period_end),
            ).fetchone()

            if standard is None:
                bill_id = new_id("bil")
                state = BillState.FINALIZED if finalize else BillState.DRAFT
                conn.execute(
                    """
                    INSERT INTO bills(bill_id, tenant_id, reservation_id, period_start, period_end, kind, version,
                                      state, lines, usage, warnings, total, currency, fingerprint, supersedes, created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        bill_id, tenant_id, reservation_id, period_start, period_end, "STANDARD", 1, state,
                        canonical_json(computed["lines"]), canonical_json(computed["usage"]),
                        canonical_json(computed["warnings"]), float(computed["total"]),
                        pricing.CURRENCY, fingerprint, None, now,
                    ),
                )
                self.store.audit(
                    conn, actor=f"tenant:{tenant_id}", type_="bill.generated", entity_id=bill_id,
                    tenant_id=tenant_id,
                    payload={"reservation_id": reservation_id, "total": computed["total"], "state": state},
                )
                return self.get_bill(tenant_id, bill_id)

            if standard["fingerprint"] == fingerprint:
                if finalize and standard["state"] == BillState.DRAFT:
                    conn.execute("UPDATE bills SET state = ? WHERE bill_id = ?", (BillState.FINALIZED, standard["bill_id"]))
                    self.store.audit(conn, actor=f"tenant:{tenant_id}", type_="bill.finalized",
                                     entity_id=standard["bill_id"], tenant_id=tenant_id)
                return self.get_bill(tenant_id, standard["bill_id"])

            if standard["state"] == BillState.DRAFT:
                new_state = BillState.FINALIZED if finalize else BillState.DRAFT
                conn.execute(
                    """
                    UPDATE bills SET version = version + 1, state = ?, lines = ?, usage = ?, warnings = ?,
                                     total = ?, fingerprint = ? WHERE bill_id = ?
                    """,
                    (
                        new_state, canonical_json(computed["lines"]), canonical_json(computed["usage"]),
                        canonical_json(computed["warnings"]), float(computed["total"]), fingerprint,
                        standard["bill_id"],
                    ),
                )
                self.store.audit(
                    conn, actor=f"tenant:{tenant_id}", type_="bill.recomputed", entity_id=standard["bill_id"],
                    tenant_id=tenant_id, payload={"total": computed["total"]},
                )
                return self.get_bill(tenant_id, standard["bill_id"])

            # 已终审但输入变化（迟到事件）：生成调整单，保留原账单不动。
            adjustment = conn.execute(
                """
                SELECT * FROM bills WHERE reservation_id = ? AND period_start = ? AND period_end = ? AND kind = 'ADJUSTMENT'
                """,
                (reservation_id, period_start, period_end),
            ).fetchone()
            delta = pricing.to_decimal(computed["total"]) - pricing.to_decimal(standard["total"])
            adj_fingerprint = stable_hash({"supersedes": standard["bill_id"], "fingerprint": fingerprint})
            if adjustment is None:
                bill_id = new_id("bil")
                conn.execute(
                    """
                    INSERT INTO bills(bill_id, tenant_id, reservation_id, period_start, period_end, kind, version,
                                      state, lines, usage, warnings, total, currency, fingerprint, supersedes, created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        bill_id, tenant_id, reservation_id, period_start, period_end, "ADJUSTMENT", 1,
                        BillState.FINALIZED, canonical_json(computed["lines"]), canonical_json(computed["usage"]),
                        canonical_json(computed["warnings"]), float(pricing.quantize(delta)),
                        pricing.CURRENCY, adj_fingerprint, standard["bill_id"], now,
                    ),
                )
                self.store.audit(
                    conn, actor=f"tenant:{tenant_id}", type_="bill.adjusted", entity_id=bill_id,
                    tenant_id=tenant_id,
                    payload={"supersedes": standard["bill_id"], "delta": str(pricing.quantize(delta))},
                )
                return self.get_bill(tenant_id, bill_id)
            if adjustment["fingerprint"] != adj_fingerprint:
                conn.execute(
                    """
                    UPDATE bills SET version = version + 1, lines = ?, usage = ?, warnings = ?,
                                     total = ?, fingerprint = ? WHERE bill_id = ?
                    """,
                    (
                        canonical_json(computed["lines"]), canonical_json(computed["usage"]),
                        canonical_json(computed["warnings"]), float(pricing.quantize(delta)),
                        adj_fingerprint, adjustment["bill_id"],
                    ),
                )
            return self.get_bill(tenant_id, adjustment["bill_id"])

    def get_bill(self, tenant_id: str, bill_id: str) -> dict:
        row = self.store.one("SELECT * FROM bills WHERE bill_id = ?", (bill_id,))
        if row is None:
            raise errors.not_found(f"账单 {bill_id} 不存在")
        if row["tenant_id"] != tenant_id:
            raise errors.forbidden("账单不属于该租户")
        return decode_json_columns(row, ("lines", "usage", "warnings"))

    def list_bills(self, tenant_id: str, reservation_id: str | None = None) -> list[dict]:
        if reservation_id:
            rows = self.store.query(
                "SELECT * FROM bills WHERE tenant_id = ? AND reservation_id = ? ORDER BY created_at, bill_id",
                (tenant_id, reservation_id),
            )
        else:
            rows = self.store.query(
                "SELECT * FROM bills WHERE tenant_id = ? ORDER BY created_at, bill_id", (tenant_id,)
            )
        return [decode_json_columns(row, ("lines", "usage", "warnings")) for row in rows]


class TrailService:
    """履约轨迹：从报价、占用到结算的完整可核对视图。"""

    def __init__(self, store: Store) -> None:
        self.store = store

    def trail(self, *, tenant_id: str, reservation_id: str | None = None) -> dict:
        params: list[object] = [tenant_id]
        rfilter = ""
        if reservation_id:
            rfilter = " AND reservation_id = ?"
            params.append(reservation_id)

        quotes = self.store.query(
            f"SELECT * FROM quotes WHERE tenant_id = ? ORDER BY created_at, quote_id", (tenant_id,)
        )
        if reservation_id:
            quote_ids = {
                r["quote_id"] for r in self.store.query(
                    "SELECT quote_id FROM reservations WHERE reservation_id = ?", (reservation_id,)
                )
            }
            quotes = [q for q in quotes if q["quote_id"] in quote_ids]
        for quote in quotes:
            decode_json_columns(quote, ("required_caps", "breakdown"))

        reservations = self.store.query(
            f"SELECT * FROM reservations WHERE tenant_id = ?{rfilter} ORDER BY created_at, reservation_id",
            tuple(params),
        )
        reservation_ids = [r["reservation_id"] for r in reservations]
        for reservation in reservations:
            decode_json_columns(reservation, ("required_caps",))

        segments, groups, tasks, migrations, bills, events = [], [], [], [], [], []
        preemptions = []
        for rid in reservation_ids:
            segments += self.store.query(
                "SELECT * FROM billing_segments WHERE reservation_id = ? ORDER BY start_at, rowid", (rid,))
            groups += self.store.query(
                "SELECT * FROM task_groups WHERE reservation_id = ? ORDER BY created_at, group_id", (rid,))
            migrations += self.store.query(
                "SELECT * FROM migrations WHERE reservation_id = ? ORDER BY planned_at, migration_id", (rid,))
            bills += self.store.query(
                "SELECT * FROM bills WHERE reservation_id = ? ORDER BY created_at, bill_id", (rid,))
            events += self.store.query(
                "SELECT * FROM consumption_events WHERE reservation_id = ? ORDER BY usage_start, event_id", (rid,))
            for row in self.store.query("SELECT * FROM preemptions ORDER BY created_at, preemption_id"):
                items = json.loads(row["items"])
                if any(i["reservation_id"] == rid for i in items):
                    row["items"] = items
                    preemptions.append(row)
        for group in groups:
            tasks += self.store.query(
                "SELECT * FROM tasks WHERE group_id = ? ORDER BY rowid", (group["group_id"],))
        for task in tasks:
            task["depends_on"] = json.loads(task["depends_on"])
        for bill in bills:
            decode_json_columns(bill, ("lines", "usage", "warnings"))

        timeline = self.store.query(
            "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY seq", (tenant_id,)
        )
        for entry in timeline:
            entry["payload"] = json.loads(entry["payload"])

        return {
            "tenant_id": tenant_id,
            "generated_at": self.store.clock.now(),
            "quotes": quotes,
            "reservations": reservations,
            "billing_segments": segments,
            "task_groups": groups,
            "tasks": tasks,
            "migrations": migrations,
            "preemptions": preemptions,
            "consumption_events": events,
            "bills": bills,
            "timeline": timeline,
        }
