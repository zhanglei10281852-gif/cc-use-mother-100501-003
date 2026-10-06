"""跨节点算力预留与履约的核心服务层。

所有业务不变量在此强制：
- 准入防超卖：``allocated_units + ? <= total_units`` 在写事务内原子生效；
- 幂等锁定：``idempotency_key`` 唯一 + 请求指纹比对，重放返回原结果；
- 报价单赢家：``UPDATE ... WHERE state='OPEN'`` 保证同一报价只被消费一次；
- 迁移不重复计费：预留由连续不重叠的 segment 组成，切换点同时关闭/开启；
- 账单收敛：消费事件按 event_id 去重，结算按"应收总额 - 已入账"差额入账，
  乱序、重复、迟到的事件最终收敛为同一本账。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Callable, Iterable

from .models import (
    BILLING_MODES,
    COMPENSATION_DEFAULTS,
    COMPENSATION_HORIZON_HOURS,
    DEFAULT_CONTRACT,
    DEFAULT_RATES,
    ENERGY_LEVELS,
    ON_COMPLETE_POLICIES,
    ON_DEGRADE_POLICIES,
    ON_DEMAND_MARKUP,
    ON_EXPIRY_POLICIES,
    ON_PARTIAL_POLICIES,
    ON_PREEMPT_POLICIES,
    RATE_EFFECTIVE_FROM,
    RESOURCE_TYPES,
    TERMINAL_TASK_STATES,
    DomainError,
    add_hours,
    bad_request,
    conflict,
    format_time,
    hours_between,
    not_found,
    now_utc,
    parse_time,
    stable_hash,
    to_cents,
    yuan,
)
from .store import Store

QUOTE_TTL_HOURS = 0.5


def _json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _loads(text: str) -> Any:
    return json.loads(text)


class System:
    """领域服务门面：API 与 CLI 共用同一入口，保证行为一致。"""

    def __init__(
        self,
        store: Store,
        now_fn: Callable[[], Any] = now_utc,
        actor: str = "system",
    ) -> None:
        self.store = store
        self._now_fn = now_fn
        self.actor = actor
        self._bg_stop = threading.Event()
        self._bg_thread: threading.Thread | None = None
        with self.store.tx() as conn:
            self._seed_rate_cards(conn)

    def with_actor(self, actor: str) -> "System":
        """共享存储与时钟、仅替换操作者身份的轻量视图（审计归属用）。"""
        clone = object.__new__(System)
        clone.store = self.store
        clone._now_fn = self._now_fn
        clone.actor = actor
        clone._bg_stop = self._bg_stop
        clone._bg_thread = self._bg_thread
        return clone

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> str:
        return format_time(self._now_fn())

    @staticmethod
    def _require(value: Any, field: str) -> Any:
        if value is None or (isinstance(value, str) and not value.strip()):
            raise bad_request(f"缺少必填字段: {field}")
        return value

    def _seed_rate_cards(self, conn: sqlite3.Connection) -> None:
        for resource_type, by_energy in DEFAULT_RATES.items():
            for energy_level, reserved_price in by_energy.items():
                for billing_mode, price in (
                    ("reserved", reserved_price),
                    ("on_demand", round(reserved_price * ON_DEMAND_MARKUP, 4)),
                ):
                    exists = Store.one(
                        conn,
                        "SELECT rate_code FROM rate_cards WHERE resource_type=? AND energy_level=? "
                        "AND billing_mode=? AND effective_from=?",
                        (resource_type, energy_level, billing_mode, RATE_EFFECTIVE_FROM),
                    )
                    if exists is None:
                        conn.execute(
                            "INSERT INTO rate_cards(rate_code, resource_type, energy_level, billing_mode, "
                            "unit_price, effective_from) VALUES(?,?,?,?,?,?)",
                            (
                                self.store.next_code(conn, "rate_cards"),
                                resource_type,
                                energy_level,
                                billing_mode,
                                price,
                                RATE_EFFECTIVE_FROM,
                            ),
                        )

    def _audit(
        self,
        conn: sqlite3.Connection,
        action: str,
        resource_type: str,
        resource_code: str,
        detail: dict[str, Any],
        tenant_code: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO audit_log(audit_code, actor, action, resource_type, resource_code, "
            "tenant_code, detail, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                self.store.next_code(conn, "audit_log"),
                self.actor,
                action,
                resource_type,
                resource_code,
                tenant_code,
                _json(detail),
                self._now(),
            ),
        )

    @staticmethod
    def _merge_contract(custom: dict[str, Any] | None) -> dict[str, Any]:
        contract = _loads(_json(DEFAULT_CONTRACT))
        for key, value in (custom or {}).items():
            if key not in contract:
                raise bad_request(f"合同字段不支持: {key}")
            contract[key] = value
        checks = (
            ("on_expiry", ON_EXPIRY_POLICIES),
            ("on_degrade", ON_DEGRADE_POLICIES),
            ("on_partial", ON_PARTIAL_POLICIES),
            ("on_complete", ON_COMPLETE_POLICIES),
            ("on_preempt", ON_PREEMPT_POLICIES),
        )
        for key, allowed in checks:
            if contract[key] not in allowed:
                raise bad_request(f"合同字段 {key} 只能取 {allowed}")
        if not isinstance(contract["priority"], int):
            raise bad_request("合同字段 priority 必须为整数")
        if not isinstance(contract["renew_extension_hours"], (int, float)) or contract["renew_extension_hours"] <= 0:
            raise bad_request("合同字段 renew_extension_hours 必须为正数")
        if not isinstance(contract["required_capabilities"], list):
            raise bad_request("合同字段 required_capabilities 必须为数组")
        merged = dict(COMPENSATION_DEFAULTS)
        for key, value in (contract.get("compensation") or {}).items():
            if key not in merged:
                raise bad_request(f"补偿规则不支持: {key}")
            if not isinstance(value, (int, float)) or not 0 <= value <= 1:
                raise bad_request(f"补偿比例 {key} 必须在 [0,1]")
            merged[key] = float(value)
        contract["compensation"] = merged
        return contract

    def _rate_for(
        self,
        conn: sqlite3.Connection,
        resource_type: str,
        energy_level: str,
        billing_mode: str,
        at: str,
    ) -> dict[str, Any]:
        row = Store.one(
            conn,
            "SELECT * FROM rate_cards WHERE resource_type=? AND energy_level=? AND billing_mode=? "
            "AND effective_from<=? ORDER BY effective_from DESC LIMIT 1",
            (resource_type, energy_level, billing_mode, at),
        )
        if row is None:
            raise conflict(
                "rate_missing",
                f"缺少费率卡: {resource_type}/{energy_level}/{billing_mode}",
            )
        return row

    # ------------------------------------------------------------------
    # 容量：节点与批次
    # ------------------------------------------------------------------

    def register_node(
        self,
        name: str,
        fault_domain: str,
        energy_level: str,
        capabilities: list[str] | None = None,
        node_code: str | None = None,
    ) -> dict[str, Any]:
        self._require(name, "name")
        self._require(fault_domain, "fault_domain")
        if energy_level not in ENERGY_LEVELS:
            raise bad_request(f"energy_level 只能取 {ENERGY_LEVELS}")
        with self.store.tx() as conn:
            now = self._now()
            code = node_code or self.store.next_code(conn, "nodes")
            if Store.get(conn, "nodes", code) is not None:
                raise conflict("duplicate_node", f"节点 {code} 已存在")
            conn.execute(
                "INSERT INTO nodes(node_code, name, fault_domain, energy_level, state, capabilities, "
                "created_at, updated_at, version) VALUES(?,?,?,?,?,?,?,?,1)",
                (code, name, fault_domain, energy_level, "ACTIVE", _json(capabilities or []), now, now),
            )
            self._audit(conn, "node_registered", "node", code, {"name": name, "fault_domain": fault_domain})
            return self.get_node(code)

    def get_node(self, node_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = Store.get(conn, "nodes", node_code)
            if row is None:
                raise not_found("节点", node_code)
            row["capabilities"] = _loads(row["capabilities"])
            return row

    def list_nodes(self) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = Store.query(conn, "SELECT * FROM nodes ORDER BY node_code")
            for row in rows:
                row["capabilities"] = _loads(row["capabilities"])
            return rows

    def publish_batch(
        self,
        node_code: str,
        resource_type: str,
        total_units: int,
        available_from: str,
        available_until: str,
        capabilities: list[str] | None = None,
    ) -> dict[str, Any]:
        if resource_type not in RESOURCE_TYPES:
            raise bad_request(f"resource_type 只能取 {RESOURCE_TYPES}")
        if not isinstance(total_units, int) or total_units < 1:
            raise bad_request("total_units 必须为正整数")
        start = parse_time(available_from)
        end = parse_time(available_until)
        if not start < end:
            raise bad_request("available_from 必须早于 available_until")
        with self.store.tx() as conn:
            node = Store.get(conn, "nodes", node_code)
            if node is None:
                raise not_found("节点", node_code)
            if node["state"] == "OFFLINE":
                raise conflict("node_offline", f"节点 {node_code} 已离线，不能发布容量")
            # 同一节点同一资源类型的 OPEN 批次时间窗不得重叠：
            # 避免两个批次对同一批物理加速卡重复准入（事故根因之一）。
            overlap = Store.one(
                conn,
                "SELECT batch_code FROM capacity_batches WHERE node_code=? AND resource_type=? "
                "AND state='OPEN' AND available_from < ? AND ? < available_until",
                (node_code, resource_type, format_time(end), format_time(start)),
            )
            if overlap is not None:
                raise conflict(
                    "batch_window_overlap",
                    f"与已开放批次 {overlap['batch_code']} 的时间窗重叠",
                )
            now = self._now()
            code = self.store.next_code(conn, "capacity_batches")
            batch_capabilities = capabilities if capabilities is not None else _loads(node["capabilities"])
            conn.execute(
                "INSERT INTO capacity_batches(batch_code, node_code, resource_type, total_units, "
                "allocated_units, available_from, available_until, fault_domain, energy_level, "
                "capabilities, state, created_at, updated_at, version) "
                "VALUES(?,?,?,?,0,?,?,?,?,?,'OPEN',?,?,1)",
                (
                    code,
                    node_code,
                    resource_type,
                    total_units,
                    format_time(start),
                    format_time(end),
                    node["fault_domain"],
                    node["energy_level"],
                    _json(batch_capabilities),
                    now,
                    now,
                ),
            )
            self._audit(
                conn,
                "batch_published",
                "capacity_batch",
                code,
                {"node_code": node_code, "resource_type": resource_type, "total_units": total_units},
            )
            return self.get_batch(code)

    def close_batch(self, batch_code: str) -> dict[str, Any]:
        with self.store.tx() as conn:
            batch = Store.get(conn, "capacity_batches", batch_code)
            if batch is None:
                raise not_found("容量批次", batch_code)
            if batch["state"] != "OPEN":
                raise conflict("batch_not_open", f"批次 {batch_code} 当前状态 {batch['state']}")
            if batch["allocated_units"] > 0:
                raise conflict("batch_in_use", f"批次 {batch_code} 仍有占用，不能关闭")
            conn.execute(
                "UPDATE capacity_batches SET state='CLOSED', updated_at=?, version=version+1 WHERE batch_code=?",
                (self._now(), batch_code),
            )
            self._audit(conn, "batch_closed", "capacity_batch", batch_code, {})
            return self.get_batch(batch_code)

    def get_batch(self, batch_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = Store.get(conn, "capacity_batches", batch_code)
            if row is None:
                raise not_found("容量批次", batch_code)
            return self._view_batch(row)

    def list_batches(self, node_code: str | None = None, state: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM capacity_batches"
        clauses, params = [], []
        if node_code:
            clauses.append("node_code=?")
            params.append(node_code)
        if state:
            clauses.append("state=?")
            params.append(state)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY batch_code"
        with self.store.read() as conn:
            return [self._view_batch(row) for row in Store.query(conn, sql, tuple(params))]

    @staticmethod
    def _view_batch(row: dict[str, Any]) -> dict[str, Any]:
        row = dict(row)
        row["capabilities"] = _loads(row["capabilities"])
        row["available_units"] = row["total_units"] - row["allocated_units"]
        return row

    # ------------------------------------------------------------------
    # 报价
    # ------------------------------------------------------------------

    def create_quote(
        self,
        tenant_code: str,
        batch_code: str,
        units: int,
        start_at: str,
        end_at: str,
        billing_mode: str = "reserved",
        required_capabilities: list[str] | None = None,
    ) -> dict[str, Any]:
        self._require(tenant_code, "tenant_code")
        if billing_mode not in BILLING_MODES:
            raise bad_request(f"billing_mode 只能取 {BILLING_MODES}")
        if not isinstance(units, int) or units < 1:
            raise bad_request("units 必须为正整数")
        start, end = parse_time(start_at), parse_time(end_at)
        if not start < end:
            raise bad_request("start_at 必须早于 end_at")
        with self.store.tx() as conn:
            batch = Store.get(conn, "capacity_batches", batch_code)
            if batch is None:
                raise not_found("容量批次", batch_code)
            if batch["state"] != "OPEN":
                raise conflict("batch_not_open", f"批次 {batch_code} 当前状态 {batch['state']}")
            node = Store.get(conn, "nodes", batch["node_code"])
            if node["state"] != "ACTIVE":
                raise conflict("node_unavailable", f"节点 {batch['node_code']} 当前状态 {node['state']}")
            if format_time(start) < batch["available_from"] or format_time(end) > batch["available_until"]:
                raise conflict(
                    "window_outside_batch",
                    f"请求窗口超出批次有效期 [{batch['available_from']}, {batch['available_until']}]",
                )
            available = batch["total_units"] - batch["allocated_units"]
            if units > available:
                raise conflict("capacity_insufficient", f"批次 {batch_code} 可用 {available}，请求 {units}")
            missing = set(required_capabilities or []) - set(_loads(batch["capabilities"]))
            if missing:
                raise conflict("capability_mismatch", f"批次缺少服务能力: {sorted(missing)}")
            now = self._now()
            if format_time(end) <= now:
                raise bad_request("end_at 必须晚于当前时间")
            rate = self._rate_for(conn, batch["resource_type"], batch["energy_level"], billing_mode, now)
            hours = hours_between(format_time(start), format_time(end))
            amount_cents = to_cents(units * rate["unit_price"] * hours)
            snapshot = {
                "request": {
                    "tenant_code": tenant_code,
                    "units": units,
                    "start_at": format_time(start),
                    "end_at": format_time(end),
                    "billing_mode": billing_mode,
                    "required_capabilities": required_capabilities or [],
                },
                "batch": {
                    "batch_code": batch["batch_code"],
                    "node_code": batch["node_code"],
                    "resource_type": batch["resource_type"],
                    "fault_domain": batch["fault_domain"],
                    "energy_level": batch["energy_level"],
                    "available_units": available,
                    "batch_version": batch["version"],
                    "available_from": batch["available_from"],
                    "available_until": batch["available_until"],
                },
                "rate_card": {
                    "rate_code": rate["rate_code"],
                    "unit_price": rate["unit_price"],
                    "effective_from": rate["effective_from"],
                },
                "priced_hours": round(hours, 6),
                "amount_cents": amount_cents,
            }
            code = self.store.next_code(conn, "quotes")
            expires_at = min(add_hours(now, QUOTE_TTL_HOURS), format_time(end))
            conn.execute(
                "INSERT INTO quotes(quote_code, tenant_code, batch_code, units, start_at, end_at, "
                "billing_mode, unit_price, amount_cents, snapshot, snapshot_hash, state, expires_at, "
                "created_at, version) VALUES(?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,1)",
                (
                    code,
                    tenant_code,
                    batch_code,
                    units,
                    format_time(start),
                    format_time(end),
                    billing_mode,
                    rate["unit_price"],
                    amount_cents,
                    _json(snapshot),
                    stable_hash(snapshot),
                    expires_at,
                    now,
                ),
            )
            self._audit(
                conn,
                "quote_created",
                "quote",
                code,
                {"batch_code": batch_code, "units": units, "amount_cents": amount_cents},
                tenant_code,
            )
            return self.get_quote(code)

    def get_quote(self, quote_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = Store.get(conn, "quotes", quote_code)
            if row is None:
                raise not_found("报价", quote_code)
            return self._view_quote(row)

    @staticmethod
    def _view_quote(row: dict[str, Any]) -> dict[str, Any]:
        row = dict(row)
        row["snapshot"] = _loads(row["snapshot"])
        row["amount"] = yuan(row["amount_cents"])
        return row

    def verify_quote(self, quote_code: str) -> dict[str, Any]:
        """复核报价：按快照中的输入与费率重算金额，与冻结值比对。"""
        quote = self.get_quote(quote_code)
        snapshot = quote["snapshot"]
        recomputed = to_cents(
            snapshot["request"]["units"] * snapshot["rate_card"]["unit_price"] * snapshot["priced_hours"]
        )
        return {
            "quote_code": quote_code,
            "snapshot_hash": quote["snapshot_hash"],
            "stored_amount_cents": quote["amount_cents"],
            "recomputed_amount_cents": recomputed,
            "match": recomputed == quote["amount_cents"],
        }

    # ------------------------------------------------------------------
    # 预留：幂等锁定、续约、释放
    # ------------------------------------------------------------------

    def lock_reservation(
        self,
        idempotency_key: str,
        quote_code: str | None = None,
        tenant_code: str | None = None,
        batch_code: str | None = None,
        units: int | None = None,
        end_at: str | None = None,
        billing_mode: str | None = None,
        contract: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require(idempotency_key, "idempotency_key")
        fingerprint = stable_hash(
            {
                "quote_code": quote_code,
                "tenant_code": tenant_code,
                "batch_code": batch_code,
                "units": units,
                "end_at": end_at,
                "billing_mode": billing_mode,
                "contract": contract,
            }
        )
        with self.store.tx() as conn:
            existing = Store.one(conn, "SELECT * FROM reservations WHERE idempotency_key=?", (idempotency_key,))
            if existing is not None:
                if existing["request_fingerprint"] != fingerprint:
                    raise conflict(
                        "idempotency_conflict",
                        f"幂等键 {idempotency_key} 已绑定不同的请求内容",
                    )
                view = self._view_reservation(conn, existing["reservation_code"])
                view["idempotent_replay"] = True
                return view

            now = self._now()
            unit_price: float
            if quote_code is not None:
                quote = Store.get(conn, "quotes", quote_code)
                if quote is None:
                    raise not_found("报价", quote_code)
                if quote["state"] == "EXPIRED" or quote["expires_at"] <= now:
                    raise conflict("quote_expired", f"报价 {quote_code} 已过期")
                # 单赢家：只有仍处 OPEN 的报价能被消费，并发锁定只有一个成功
                consumed = conn.execute(
                    "UPDATE quotes SET state='CONSUMED', version=version+1 "
                    "WHERE quote_code=? AND state='OPEN'",
                    (quote_code,),
                )
                if consumed.rowcount != 1:
                    raise conflict("quote_consumed", f"报价 {quote_code} 已被消费")
                tenant_code = quote["tenant_code"]
                batch_code = quote["batch_code"]
                units = quote["units"]
                end_at = quote["end_at"]
                billing_mode = quote["billing_mode"]
                unit_price = quote["unit_price"]
                required_capabilities = _loads(quote["snapshot"])["request"]["required_capabilities"]
            else:
                for field, value in (
                    ("tenant_code", tenant_code),
                    ("batch_code", batch_code),
                    ("units", units),
                    ("end_at", end_at),
                ):
                    self._require(value, field)
                billing_mode = billing_mode or "reserved"
                if billing_mode not in BILLING_MODES:
                    raise bad_request(f"billing_mode 只能取 {BILLING_MODES}")
                required_capabilities = (contract or {}).get("required_capabilities", [])
                batch_row = Store.get(conn, "capacity_batches", batch_code)
                if batch_row is None:
                    raise not_found("容量批次", batch_code)
                rate = self._rate_for(
                    conn, batch_row["resource_type"], batch_row["energy_level"], billing_mode, now
                )
                unit_price = rate["unit_price"]

            merged_contract = self._merge_contract(contract)
            if required_capabilities and not merged_contract["required_capabilities"]:
                merged_contract["required_capabilities"] = list(required_capabilities)

            end_iso = format_time(parse_time(end_at))
            if end_iso <= now:
                raise bad_request("end_at 必须晚于当前时间")
            batch = Store.get(conn, "capacity_batches", batch_code)
            if batch is None:
                raise not_found("容量批次", batch_code)
            if batch["state"] != "OPEN":
                raise conflict("batch_not_open", f"批次 {batch_code} 当前状态 {batch['state']}")
            node = Store.get(conn, "nodes", batch["node_code"])
            if node["state"] != "ACTIVE":
                raise conflict("node_unavailable", f"节点 {batch['node_code']} 当前状态 {node['state']}")
            if end_iso > batch["available_until"]:
                raise conflict(
                    "window_outside_batch",
                    f"预留截止 {end_iso} 超出批次有效期 {batch['available_until']}",
                )
            missing = set(merged_contract["required_capabilities"]) - set(_loads(batch["capabilities"]))
            if missing:
                raise conflict("capability_mismatch", f"批次缺少服务能力: {sorted(missing)}")

            # 准入防超卖：原子地占用配额，容量不足则整单拒绝
            admitted = conn.execute(
                "UPDATE capacity_batches SET allocated_units = allocated_units + ?, "
                "updated_at=?, version=version+1 "
                "WHERE batch_code=? AND state='OPEN' AND allocated_units + ? <= total_units",
                (units, now, batch_code, units),
            )
            if admitted.rowcount != 1:
                available = batch["total_units"] - batch["allocated_units"]
                raise conflict(
                    "capacity_insufficient",
                    f"批次 {batch_code} 可用 {available}，请求 {units}，拒绝超卖",
                )

            code = self.store.next_code(conn, "reservations")
            conn.execute(
                "INSERT INTO reservations(reservation_code, idempotency_key, request_fingerprint, "
                "quote_code, tenant_code, batch_code, node_code, resource_type, units, start_at, end_at, "
                "billing_mode, unit_price, state, contract, settled_upto, activated_at, closed_at, "
                "close_reason, preempted_at, recovery_rank, created_at, updated_at, version) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,NULL,?,NULL,NULL,NULL,NULL,?,?,1)",
                (
                    code,
                    idempotency_key,
                    fingerprint,
                    quote_code,
                    tenant_code,
                    batch_code,
                    batch["node_code"],
                    batch["resource_type"],
                    units,
                    now,
                    end_iso,
                    billing_mode,
                    unit_price,
                    _json(merged_contract),
                    now,
                    now,
                    now,
                ),
            )
            self._open_segment(conn, code, batch["batch_code"], batch["node_code"], units, now)
            self._audit(
                conn,
                "reservation_locked",
                "reservation",
                code,
                {
                    "batch_code": batch_code,
                    "units": units,
                    "end_at": end_iso,
                    "quote_code": quote_code,
                    "idempotency_key": idempotency_key,
                },
                tenant_code,
            )
            return self._view_reservation(conn, code)

    def renew_reservation(
        self,
        reservation_code: str,
        new_end_at: str,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        new_end_iso = format_time(parse_time(new_end_at))
        with self.store.tx() as conn:
            rsv = self._must_reservation(conn, reservation_code)
            self._check_version(rsv, expected_version)
            if rsv["state"] not in ("ACTIVE", "MIGRATING"):
                raise conflict("invalid_state", f"预留 {reservation_code} 状态 {rsv['state']}，不能续约")
            if new_end_iso <= rsv["end_at"]:
                raise bad_request(f"new_end_at 必须晚于当前截止 {rsv['end_at']}")
            batch = Store.get(conn, "capacity_batches", rsv["batch_code"])
            if batch["state"] != "OPEN" or new_end_iso > batch["available_until"]:
                raise conflict(
                    "window_outside_batch",
                    f"续约截止 {new_end_iso} 超出批次 {rsv['batch_code']} 有效期 {batch['available_until']}",
                )
            conn.execute(
                "UPDATE reservations SET end_at=?, updated_at=?, version=version+1 WHERE reservation_code=?",
                (new_end_iso, self._now(), reservation_code),
            )
            self._audit(
                conn,
                "reservation_renewed",
                "reservation",
                reservation_code,
                {"old_end_at": rsv["end_at"], "new_end_at": new_end_iso},
                rsv["tenant_code"],
            )
            return self._view_reservation(conn, reservation_code)

    def release_reservation(self, reservation_code: str, reason: str = "tenant_release") -> dict[str, Any]:
        with self.store.tx() as conn:
            rsv = self._must_reservation(conn, reservation_code)
            if rsv["state"] == "MIGRATING":
                raise conflict("invalid_state", "迁移中的预留请先中止迁移再释放")
            if rsv["state"] in ("RELEASED", "EXPIRED"):
                raise conflict("invalid_state", f"预留 {reservation_code} 已关闭")
            self._close_reservation(conn, rsv, "RELEASED", reason, self._now())
            return self._view_reservation(conn, reservation_code)

    # ------------------------------------------------------------------
    # 任务组
    # ------------------------------------------------------------------

    def submit_task_group(
        self,
        tenant_code: str,
        name: str,
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        self._require(tenant_code, "tenant_code")
        self._require(name, "name")
        if not tasks:
            raise bad_request("任务组至少包含一个任务")
        names = [t.get("name") for t in tasks]
        if len(set(names)) != len(names) or any(not n for n in names):
            raise bad_request("任务名称在组内必须非空且唯一")
        with self.store.tx() as conn:
            for spec in tasks:
                rsv = Store.get(conn, "reservations", spec.get("reservation_code", ""))
                if rsv is None:
                    raise not_found("预留", str(spec.get("reservation_code")))
                if rsv["tenant_code"] != tenant_code:
                    raise conflict("tenant_mismatch", f"预留 {rsv['reservation_code']} 不属于租户 {tenant_code}")
                if rsv["state"] != "ACTIVE":
                    raise conflict("invalid_state", f"预留 {rsv['reservation_code']} 状态 {rsv['state']}，不能挂载任务")
                required = spec.get("required_units")
                if not isinstance(required, int) or required < 1:
                    raise bad_request(f"任务 {spec.get('name')} 的 required_units 必须为正整数")
                if required > rsv["units"]:
                    raise conflict(
                        "capacity_insufficient",
                        f"任务 {spec['name']} 需要 {required}，超出预留 {rsv['reservation_code']} 的 {rsv['units']}",
                    )
            # 依赖必须构成 DAG（按组内任务名引用）
            deps_by_name: dict[str, list[str]] = {}
            for spec in tasks:
                deps = spec.get("depends_on") or []
                unknown = set(deps) - set(names)
                if unknown:
                    raise bad_request(f"任务 {spec['name']} 依赖未知任务: {sorted(unknown)}")
                deps_by_name[spec["name"]] = list(deps)
            self._assert_acyclic(deps_by_name)

            now = self._now()
            group_code = self.store.next_code(conn, "task_groups")
            conn.execute(
                "INSERT INTO task_groups(group_code, tenant_code, name, created_at) VALUES(?,?,?,?)",
                (group_code, tenant_code, name, now),
            )
            code_by_name = {spec["name"]: self.store.next_code(conn, "tasks") for spec in tasks}
            for spec in tasks:
                dep_codes = sorted(code_by_name[d] for d in deps_by_name[spec["name"]])
                state = "READY" if not dep_codes else "PENDING"
                conn.execute(
                    "INSERT INTO tasks(task_code, group_code, tenant_code, reservation_code, name, "
                    "depends_on, required_units, state, submitted_at, started_at, finished_at, progress, "
                    "updated_at, version) VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL,0,?,1)",
                    (
                        code_by_name[spec["name"]],
                        group_code,
                        tenant_code,
                        spec["reservation_code"],
                        spec["name"],
                        _json(dep_codes),
                        spec["required_units"],
                        state,
                        now,
                        now,
                    ),
                )
            self._audit(
                conn,
                "task_group_submitted",
                "task_group",
                group_code,
                {"name": name, "task_count": len(tasks)},
                tenant_code,
            )
            return self.get_task_group(group_code)

    @staticmethod
    def _assert_acyclic(deps_by_name: dict[str, list[str]]) -> None:
        indegree = {name: 0 for name in deps_by_name}
        for name, deps in deps_by_name.items():
            for dep in deps:
                indegree[name] += 1
        queue = [name for name, degree in indegree.items() if degree == 0]
        seen = 0
        while queue:
            node = queue.pop()
            seen += 1
            for name, deps in deps_by_name.items():
                if node in deps:
                    indegree[name] -= 1
                    if indegree[name] == 0:
                        queue.append(name)
        if seen != len(deps_by_name):
            raise bad_request("任务依赖存在环，无法调度")

    def get_task_group(self, group_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            group = Store.get(conn, "task_groups", group_code)
            if group is None:
                raise not_found("任务组", group_code)
            tasks = Store.query(
                conn, "SELECT * FROM tasks WHERE group_code=? ORDER BY task_code", (group_code,)
            )
            for task in tasks:
                task["depends_on"] = _loads(task["depends_on"])
            group["tasks"] = tasks
            group["state"] = self._group_state(tasks)
            return group

    @staticmethod
    def _group_state(tasks: list[dict[str, Any]]) -> str:
        if not tasks:
            return "EMPTY"
        states = {t["state"] for t in tasks}
        if states == {"SUCCEEDED"}:
            return "SUCCEEDED"
        if states <= set(TERMINAL_TASK_STATES):
            return "FAILED" if states & {"FAILED", "BLOCKED"} else "SUCCEEDED"
        if states & {"RUNNING", "READY"}:
            return "RUNNING"
        return "PENDING"

    # ------------------------------------------------------------------
    # 消费事件：去重、乱序收敛、驱动任务状态机
    # ------------------------------------------------------------------

    def ingest_events(self, events: Iterable[dict[str, Any]]) -> dict[str, Any]:
        results = []
        with self.store.tx() as conn:
            for event in events:
                results.append(self._ingest_one(conn, event))
        return {"results": results}

    def _ingest_one(self, conn: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
        event_id = event.get("event_id")
        self._require(event_id, "event_id")
        duplicate = Store.one(
            conn, "SELECT event_code FROM consumption_events WHERE event_id=?", (event_id,)
        )
        if duplicate is not None:
            return {"event_id": event_id, "status": "duplicate", "event_code": duplicate["event_code"]}

        reservation_code = event.get("reservation_code")
        rsv = Store.get(conn, "reservations", reservation_code or "")
        if rsv is None:
            raise not_found("预留", str(reservation_code))
        event_type = event.get("event_type")
        occurred_at = format_time(parse_time(self._require(event.get("occurred_at"), "occurred_at")))
        unit_hours = event.get("unit_hours")
        if event_type == "usage":
            if not isinstance(unit_hours, (int, float)) or unit_hours <= 0:
                raise bad_request("usage 事件必须携带正的 unit_hours")
        elif event_type in ("task_started", "task_finished"):
            self._require(event.get("task_code"), "task_code")
        elif event_type != "note":
            raise bad_request(f"不支持的事件类型: {event_type}")

        now = self._now()
        seq_row = Store.one(conn, "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM consumption_events")
        code = self.store.next_code(conn, "consumption_events")
        conn.execute(
            "INSERT INTO consumption_events(event_code, event_id, reservation_code, task_code, event_type, "
            "occurred_at, unit_hours, payload, received_at, seq) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                code,
                event_id,
                rsv["reservation_code"],
                event.get("task_code"),
                event_type,
                occurred_at,
                unit_hours,
                _json(event.get("payload") or {}),
                now,
                seq_row["next_seq"],
            ),
        )

        if event_type in ("task_started", "task_finished"):
            task = Store.get(conn, "tasks", event["task_code"])
            if task is None:
                raise not_found("任务", str(event.get("task_code")))
            if task["reservation_code"] != rsv["reservation_code"]:
                raise conflict("tenant_mismatch", "任务不属于该预留")
            if event_type == "task_started" and task["state"] in ("READY", "PENDING", "RUNNING"):
                started_at = task["started_at"] or occurred_at
                if occurred_at < started_at:
                    started_at = occurred_at
                conn.execute(
                    "UPDATE tasks SET state='RUNNING', started_at=?, updated_at=?, version=version+1 "
                    "WHERE task_code=?",
                    (started_at, now, task["task_code"]),
                )
            elif event_type == "task_finished" and task["state"] not in TERMINAL_TASK_STATES:
                payload = event.get("payload") or {}
                result = payload.get("result", "SUCCEEDED")
                if result not in ("SUCCEEDED", "FAILED"):
                    raise bad_request("task_finished 的 result 只能取 SUCCEEDED/FAILED")
                conn.execute(
                    "UPDATE tasks SET state=?, finished_at=?, progress=?, updated_at=?, version=version+1 "
                    "WHERE task_code=?",
                    (result, occurred_at, float(payload.get("progress", 1.0)), now, task["task_code"]),
                )
                self._refresh_group(conn, task["group_code"], now)
                self._apply_partial_completion(conn, rsv, now)

        return {"event_id": event_id, "status": "accepted", "event_code": code}

    def _refresh_group(self, conn: sqlite3.Connection, group_code: str, now: str) -> None:
        """按依赖收敛任务状态：全部依赖成功→READY；任一依赖失败→BLOCKED。"""
        changed = True
        while changed:
            changed = False
            tasks = Store.query(conn, "SELECT * FROM tasks WHERE group_code=?", (group_code,))
            state_by_code = {t["task_code"]: t["state"] for t in tasks}
            for task in tasks:
                if task["state"] != "PENDING":
                    continue
                dep_states = [state_by_code[d] for d in _loads(task["depends_on"])]
                if all(s == "SUCCEEDED" for s in dep_states):
                    new_state = "READY"
                elif any(s in ("FAILED", "BLOCKED") for s in dep_states):
                    new_state = "BLOCKED"
                else:
                    continue
                conn.execute(
                    "UPDATE tasks SET state=?, updated_at=?, version=version+1 WHERE task_code=?",
                    (new_state, now, task["task_code"]),
                )
                changed = True

    def _apply_partial_completion(self, conn: sqlite3.Connection, rsv: dict[str, Any], now: str) -> None:
        """任务部分完成时按合同收缩或释放预留。"""
        if rsv["state"] != "ACTIVE":
            return
        contract = _loads(rsv["contract"])
        tasks = Store.query(
            conn, "SELECT * FROM tasks WHERE reservation_code=?", (rsv["reservation_code"],)
        )
        if not tasks:
            return
        remaining = [t for t in tasks if t["state"] not in TERMINAL_TASK_STATES]
        if remaining and contract["on_partial"] == "shrink":
            needed = max(t["required_units"] for t in remaining)
            if needed < rsv["units"]:
                freed = rsv["units"] - needed
                conn.execute(
                    "UPDATE capacity_batches SET allocated_units = allocated_units - ?, "
                    "updated_at=?, version=version+1 WHERE batch_code=?",
                    (freed, now, rsv["batch_code"]),
                )
                conn.execute(
                    "UPDATE reservations SET units=?, updated_at=?, version=version+1 WHERE reservation_code=?",
                    (needed, now, rsv["reservation_code"]),
                )
                # 单元数变化切分新的占用区间，保证账单按段精确
                self._close_open_segment(conn, rsv["reservation_code"], now)
                self._open_segment(
                    conn, rsv["reservation_code"], rsv["batch_code"], rsv["node_code"], needed, now
                )
                self._audit(
                    conn,
                    "reservation_shrunk",
                    "reservation",
                    rsv["reservation_code"],
                    {"freed_units": freed, "remaining_units": needed},
                    rsv["tenant_code"],
                )
                rsv = self._must_reservation(conn, rsv["reservation_code"])
        if not remaining and contract["on_complete"] == "release":
            fresh = self._must_reservation(conn, rsv["reservation_code"])
            if fresh["state"] == "ACTIVE":
                self._close_reservation(conn, fresh, "RELEASED", "tasks_completed", now)

    # ------------------------------------------------------------------
    # 迁移：计划、提交（原子切换占用区间）、中止
    # ------------------------------------------------------------------

    def plan_migration(
        self,
        reservation_code: str,
        reason: str = "operator",
        target_batch_code: str | None = None,
        interruptible: bool | None = None,
        auto_commit: bool = False,
    ) -> dict[str, Any]:
        with self.store.tx() as conn:
            rsv = self._must_reservation(conn, reservation_code)
            migration = self._plan_migration(conn, rsv, reason, target_batch_code, interruptible)
            if auto_commit:
                migration = self._commit_migration(conn, migration["migration_code"])
            return migration

    def _plan_migration(
        self,
        conn: sqlite3.Connection,
        rsv: dict[str, Any],
        reason: str,
        target_batch_code: str | None,
        interruptible: bool | None,
        exclude_fault_domain: str | None = None,
    ) -> dict[str, Any]:
        if rsv["state"] not in ("ACTIVE", "PREEMPTED"):
            raise conflict("invalid_state", f"预留 {rsv['reservation_code']} 状态 {rsv['state']}，不能迁移")
        pending = Store.one(
            conn,
            "SELECT migration_code FROM migrations WHERE reservation_code=? AND state='PLANNED'",
            (rsv["reservation_code"],),
        )
        if pending is not None:
            raise conflict("migration_pending", f"预留 {rsv['reservation_code']} 已有未决迁移")
        now = self._now()
        if target_batch_code is None:
            target = self._find_target_batch(conn, rsv, now, exclude_fault_domain)
            if target is None:
                raise conflict("no_migration_target", f"预留 {rsv['reservation_code']} 没有可用迁移目标")
            target_batch_code = target["batch_code"]
        else:
            target = Store.get(conn, "capacity_batches", target_batch_code)
            if target is None:
                raise not_found("容量批次", target_batch_code)
            self._assert_target_suitable(conn, rsv, target, now)
        contract = _loads(rsv["contract"])
        if interruptible is None:
            interruptible = bool(contract["interruptible"])
        code = self.store.next_code(conn, "migrations")
        conn.execute(
            "INSERT INTO migrations(migration_code, reservation_code, from_batch, to_batch, units, reason, "
            "interruptible, state, planned_at, updated_at, version) VALUES(?,?,?,?,?,?,?,'PLANNED',?,?,1)",
            (
                code,
                rsv["reservation_code"],
                rsv["batch_code"],
                target_batch_code,
                rsv["units"],
                reason,
                1 if interruptible else 0,
                now,
                now,
            ),
        )
        if rsv["state"] == "ACTIVE":
            conn.execute(
                "UPDATE reservations SET state='MIGRATING', updated_at=?, version=version+1 "
                "WHERE reservation_code=?",
                (now, rsv["reservation_code"]),
            )
        self._audit(
            conn,
            "migration_planned",
            "migration",
            code,
            {
                "reservation_code": rsv["reservation_code"],
                "from_batch": rsv["batch_code"],
                "to_batch": target_batch_code,
                "reason": reason,
            },
            rsv["tenant_code"],
        )
        return Store.get(conn, "migrations", code)

    def _assert_target_suitable(
        self,
        conn: sqlite3.Connection,
        rsv: dict[str, Any],
        target: dict[str, Any],
        now: str,
    ) -> None:
        if target["batch_code"] == rsv["batch_code"]:
            raise conflict("invalid_target", "迁移目标不能是当前批次")
        if target["state"] != "OPEN":
            raise conflict("batch_not_open", f"目标批次 {target['batch_code']} 当前状态 {target['state']}")
        node = Store.get(conn, "nodes", target["node_code"])
        if node["state"] != "ACTIVE":
            raise conflict("node_unavailable", f"目标节点 {target['node_code']} 当前状态 {node['state']}")
        if target["resource_type"] != rsv["resource_type"]:
            raise conflict("resource_mismatch", "目标批次资源类型不匹配")
        if target["available_until"] < rsv["end_at"]:
            raise conflict("window_outside_batch", "目标批次有效期覆盖不了预留截止")
        contract = _loads(rsv["contract"])
        missing = set(contract["required_capabilities"]) - set(_loads(target["capabilities"]))
        if missing:
            raise conflict("capability_mismatch", f"目标批次缺少服务能力: {sorted(missing)}")

    def _find_target_batch(
        self,
        conn: sqlite3.Connection,
        rsv: dict[str, Any],
        now: str,
        exclude_fault_domain: str | None = None,
    ) -> dict[str, Any] | None:
        candidates = Store.query(
            conn,
            "SELECT b.* FROM capacity_batches b JOIN nodes n ON n.node_code = b.node_code "
            "WHERE b.state='OPEN' AND n.state='ACTIVE' AND b.resource_type=? "
            "AND b.batch_code != ? AND b.available_from <= ? AND b.available_until >= ? "
            "AND b.allocated_units + ? <= b.total_units",
            (rsv["resource_type"], rsv["batch_code"], now, rsv["end_at"], rsv["units"]),
        )
        contract = _loads(rsv["contract"])
        required = set(contract["required_capabilities"])
        source = Store.get(conn, "capacity_batches", rsv["batch_code"])
        suitable = []
        for batch in candidates:
            if exclude_fault_domain and batch["fault_domain"] == exclude_fault_domain:
                continue
            if not required <= set(_loads(batch["capabilities"])):
                continue
            suitable.append(batch)
        if not suitable:
            return None

        def rank(batch: dict[str, Any]) -> tuple:
            return (
                0 if batch["fault_domain"] != source["fault_domain"] else 1,
                0 if batch["energy_level"] == source["energy_level"] else 1,
                -(batch["total_units"] - batch["allocated_units"]),
                batch["batch_code"],
            )

        return sorted(suitable, key=rank)[0]

    def commit_migration(self, migration_code: str) -> dict[str, Any]:
        with self.store.tx() as conn:
            return self._commit_migration(conn, migration_code)

    def _commit_migration(self, conn: sqlite3.Connection, migration_code: str) -> dict[str, Any]:
        migration = Store.get(conn, "migrations", migration_code)
        if migration is None:
            raise not_found("迁移", migration_code)
        if migration["state"] != "PLANNED":
            raise conflict("invalid_state", f"迁移 {migration_code} 状态 {migration['state']}，不能提交")
        rsv = self._must_reservation(conn, migration["reservation_code"])
        if rsv["state"] not in ("MIGRATING", "PREEMPTED"):
            raise conflict("invalid_state", f"预留 {rsv['reservation_code']} 状态 {rsv['state']}，不能切换")
        target = Store.get(conn, "capacity_batches", migration["to_batch"])
        self._assert_target_suitable(conn, rsv, target, self._now())

        now = self._now()
        # 原子切换：目标批次占用 + 源批次释放 + 关闭旧区间开新区间，同一事务完成
        admitted = conn.execute(
            "UPDATE capacity_batches SET allocated_units = allocated_units + ?, updated_at=?, "
            "version=version+1 WHERE batch_code=? AND state='OPEN' "
            "AND allocated_units + ? <= total_units",
            (rsv["units"], now, target["batch_code"], rsv["units"]),
        )
        if admitted.rowcount != 1:
            raise conflict("capacity_insufficient", f"目标批次 {target['batch_code']} 容量不足，迁移失败")
        if rsv["state"] == "MIGRATING":
            self._free_capacity(conn, rsv, now)
        self._open_segment(
            conn, rsv["reservation_code"], target["batch_code"], target["node_code"], rsv["units"], now
        )
        conn.execute(
            "UPDATE reservations SET batch_code=?, node_code=?, state='ACTIVE', updated_at=?, "
            "version=version+1 WHERE reservation_code=?",
            (target["batch_code"], target["node_code"], now, rsv["reservation_code"]),
        )
        conn.execute(
            "UPDATE migrations SET state='COMMITTED', updated_at=?, version=version+1 WHERE migration_code=?",
            (now, migration_code),
        )
        # 切换事件进入消费流，供账单与轨迹核对（event_id 确定性生成，重复提交自动去重）
        self._ingest_one(
            conn,
            {
                "event_id": f"cutover:{migration_code}",
                "reservation_code": rsv["reservation_code"],
                "event_type": "note",
                "occurred_at": now,
                "payload": {
                    "kind": "migration_cutover",
                    "migration_code": migration_code,
                    "from_batch": migration["from_batch"],
                    "to_batch": migration["to_batch"],
                },
            },
        )
        self._audit(
            conn,
            "migration_committed",
            "migration",
            migration_code,
            {
                "reservation_code": rsv["reservation_code"],
                "from_batch": migration["from_batch"],
                "to_batch": migration["to_batch"],
            },
            rsv["tenant_code"],
        )
        return Store.get(conn, "migrations", migration_code)

    def abort_migration(self, migration_code: str, reason: str = "operator_abort") -> dict[str, Any]:
        with self.store.tx() as conn:
            migration = Store.get(conn, "migrations", migration_code)
            if migration is None:
                raise not_found("迁移", migration_code)
            if migration["state"] != "PLANNED":
                raise conflict("invalid_state", f"迁移 {migration_code} 状态 {migration['state']}，不能中止")
            if not migration["interruptible"]:
                raise conflict("not_interruptible", f"迁移 {migration_code} 为不可中断，不能中止")
            rsv = self._must_reservation(conn, migration["reservation_code"])
            now = self._now()
            if rsv["state"] == "MIGRATING":
                conn.execute(
                    "UPDATE reservations SET state='ACTIVE', updated_at=?, version=version+1 "
                    "WHERE reservation_code=?",
                    (now, rsv["reservation_code"]),
                )
            conn.execute(
                "UPDATE migrations SET state='ABORTED', updated_at=?, version=version+1 WHERE migration_code=?",
                (now, migration_code),
            )
            self._audit(
                conn,
                "migration_aborted",
                "migration",
                migration_code,
                {"reservation_code": rsv["reservation_code"], "reason": reason},
                rsv["tenant_code"],
            )
            return Store.get(conn, "migrations", migration_code)

    def get_migration(self, migration_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            row = Store.get(conn, "migrations", migration_code)
            if row is None:
                raise not_found("迁移", migration_code)
            return row

    def list_migrations(self, state: str | None = None) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            if state:
                return Store.query(
                    conn, "SELECT * FROM migrations WHERE state=? ORDER BY migration_code", (state,)
                )
            return Store.query(conn, "SELECT * FROM migrations ORDER BY migration_code")

    # ------------------------------------------------------------------
    # 抢占与恢复
    # ------------------------------------------------------------------

    def preempt(self, batch_code: str, needed_units: int, reason: str = "operator_preempt") -> dict[str, Any]:
        if not isinstance(needed_units, int) or needed_units < 1:
            raise bad_request("needed_units 必须为正整数")
        with self.store.tx() as conn:
            batch = Store.get(conn, "capacity_batches", batch_code)
            if batch is None:
                raise not_found("容量批次", batch_code)
            now = self._now()
            shortfall = needed_units - (batch["total_units"] - batch["allocated_units"])
            victims: list[dict[str, Any]] = []
            if shortfall > 0:
                candidates = Store.query(
                    conn,
                    "SELECT * FROM reservations WHERE batch_code=? AND state IN ('ACTIVE','MIGRATING') "
                    "ORDER BY json_extract(contract, '$.priority') ASC, end_at ASC, reservation_code ASC",
                    (batch_code,),
                )
                for candidate in candidates:
                    if shortfall <= 0:
                        break
                    victims.append(candidate)
                    shortfall -= candidate["units"]
                if shortfall > 0:
                    raise conflict(
                        "capacity_insufficient",
                        f"批次 {batch_code} 即使全部抢占也缺 {shortfall} 单元",
                    )
            actions = []
            for victim in victims:
                actions.append(self._displace(conn, victim, now, reason))
            preempted = [a for a in actions if a["action"] == "preempted"]
            recovery_order = [
                a["reservation_code"]
                for a in sorted(
                    preempted,
                    key=lambda a: (-a["priority"], a["end_at"], a["reservation_code"]),
                )
            ]
            for rank, action in enumerate(
                sorted(preempted, key=lambda a: (-a["priority"], a["end_at"], a["reservation_code"])),
                start=1,
            ):
                conn.execute(
                    "UPDATE reservations SET recovery_rank=? WHERE reservation_code=?",
                    (rank, action["reservation_code"]),
                )
            plan = {
                "reason": reason,
                "trigger_batch": batch_code,
                "needed_units": needed_units,
                "victims": actions,
                "recovery_order": recovery_order,
            }
            code = self.store.next_code(conn, "preemptions")
            conn.execute(
                "INSERT INTO preemptions(preemption_code, reason, trigger_batch, state, plan, created_at) "
                "VALUES(?,?,?,'EXECUTED',?,?)",
                (code, reason, batch_code, _json(plan), now),
            )
            self._audit(
                conn,
                "preemption_executed",
                "preemption",
                code,
                {"batch_code": batch_code, "victim_count": len(victims)},
            )
            result = Store.get(conn, "preemptions", code)
            result["plan"] = _loads(result["plan"])
            return result

    def _displace(
        self, conn: sqlite3.Connection, victim: dict[str, Any], now: str, reason: str
    ) -> dict[str, Any]:
        """驱离一个预留：合同允许则迁移，否则抢占并补偿。"""
        contract = _loads(victim["contract"])
        action = {
            "reservation_code": victim["reservation_code"],
            "tenant_code": victim["tenant_code"],
            "units": victim["units"],
            "priority": contract["priority"],
            "end_at": victim["end_at"],
        }
        if contract["on_preempt"] == "migrate":
            target = self._find_target_batch(conn, victim, now)
            if target is not None:
                migration = self._plan_migration(conn, victim, f"{reason}:displace", None, None)
                self._commit_migration(conn, migration["migration_code"])
                credit = self._compensation_credit(conn, victim, "forced_migration", now)
                action.update(
                    action="migrated",
                    to_batch=target["batch_code"],
                    migration_code=migration["migration_code"],
                    compensation_cents=credit,
                )
                return action
        if victim["state"] == "MIGRATING":
            pending = Store.one(
                conn,
                "SELECT * FROM migrations WHERE reservation_code=? AND state='PLANNED'",
                (victim["reservation_code"],),
            )
            if pending is not None:
                conn.execute(
                    "UPDATE migrations SET state='ABORTED', updated_at=?, version=version+1 "
                    "WHERE migration_code=?",
                    (now, pending["migration_code"]),
                )
        self._free_capacity(conn, victim, now)
        conn.execute(
            "UPDATE reservations SET state='PREEMPTED', preempted_at=?, updated_at=?, version=version+1 "
            "WHERE reservation_code=?",
            (now, now, victim["reservation_code"]),
        )
        credit = self._compensation_credit(conn, victim, "capacity_loss", now)
        action.update(action="preempted", compensation_cents=credit)
        self._audit(
            conn,
            "reservation_preempted",
            "reservation",
            victim["reservation_code"],
            {"reason": reason, "compensation_cents": credit},
            victim["tenant_code"],
        )
        return action

    def run_recovery(self) -> dict[str, Any]:
        """容量恢复后按抢占时记录的次序回迁 PREEMPTED 预留。"""
        with self.store.tx() as conn:
            now = self._now()
            waiting = Store.query(
                conn,
                "SELECT * FROM reservations WHERE state='PREEMPTED' "
                "ORDER BY preempted_at ASC, recovery_rank ASC, reservation_code ASC",
            )
            recovered, expired, pending = [], [], []
            for rsv in waiting:
                if rsv["end_at"] <= now:
                    self._close_reservation(conn, rsv, "EXPIRED", "expired_while_preempted", rsv["end_at"])
                    expired.append(rsv["reservation_code"])
                    continue
                target = self._find_target_batch(conn, rsv, now)
                if target is None:
                    pending.append(rsv["reservation_code"])
                    continue
                migration = self._plan_migration(conn, rsv, "recovery", target["batch_code"], None)
                self._commit_migration(conn, migration["migration_code"])
                recovered.append(
                    {"reservation_code": rsv["reservation_code"], "to_batch": target["batch_code"]}
                )
            return {"recovered": recovered, "expired": expired, "pending": pending}

    # ------------------------------------------------------------------
    # 节点状态与降级/离线处理
    # ------------------------------------------------------------------

    def set_node_state(self, node_code: str, state: str) -> dict[str, Any]:
        if state not in ("ACTIVE", "DEGRADED", "OFFLINE"):
            raise bad_request("state 只能取 ACTIVE/DEGRADED/OFFLINE")
        with self.store.tx() as conn:
            node = Store.get(conn, "nodes", node_code)
            if node is None:
                raise not_found("节点", node_code)
            now = self._now()
            conn.execute(
                "UPDATE nodes SET state=?, updated_at=?, version=version+1 WHERE node_code=?",
                (state, now, node_code),
            )
            self._audit(
                conn, "node_state_changed", "node", node_code, {"from": node["state"], "to": state}
            )
            node = Store.get(conn, "nodes", node_code)
            if state == "DEGRADED":
                self._handle_degraded(conn, node, now)
            elif state == "OFFLINE":
                self._handle_offline(conn, node, now)
            return self.get_node(node_code)

    def _handle_degraded(self, conn: sqlite3.Connection, node: dict[str, Any], now: str) -> None:
        reservations = Store.query(
            conn,
            "SELECT r.* FROM reservations r JOIN capacity_batches b ON b.batch_code = r.batch_code "
            "WHERE b.node_code=? AND r.state='ACTIVE'",
            (node["node_code"],),
        )
        for rsv in reservations:
            contract = _loads(rsv["contract"])
            if contract["on_degrade"] != "migrate":
                continue
            target = self._find_target_batch(conn, rsv, now, exclude_fault_domain=node["fault_domain"])
            if target is None:
                self._audit(
                    conn,
                    "migration_pending",
                    "reservation",
                    rsv["reservation_code"],
                    {"reason": "node_degraded", "node_code": node["node_code"]},
                    rsv["tenant_code"],
                )
                continue
            migration = self._plan_migration(
                conn, rsv, "node_degraded", target["batch_code"], None,
                exclude_fault_domain=node["fault_domain"],
            )
            self._commit_migration(conn, migration["migration_code"])
            credit = self._compensation_credit(conn, rsv, "forced_migration", now)
            self._audit(
                conn,
                "degrade_migration",
                "reservation",
                rsv["reservation_code"],
                {"to_batch": target["batch_code"], "compensation_cents": credit},
                rsv["tenant_code"],
            )

    def _handle_offline(self, conn: sqlite3.Connection, node: dict[str, Any], now: str) -> None:
        batches = Store.query(
            conn, "SELECT * FROM capacity_batches WHERE node_code=? AND state='OPEN'", (node["node_code"],)
        )
        for batch in batches:
            conn.execute(
                "UPDATE capacity_batches SET state='CLOSED', updated_at=?, version=version+1 "
                "WHERE batch_code=?",
                (now, batch["batch_code"]),
            )
        reservations = Store.query(
            conn,
            "SELECT r.* FROM reservations r JOIN capacity_batches b ON b.batch_code = r.batch_code "
            "WHERE b.node_code=? AND r.state IN ('ACTIVE','MIGRATING')",
            (node["node_code"],),
        )
        actions = []
        for rsv in reservations:
            pending = Store.one(
                conn,
                "SELECT * FROM migrations WHERE reservation_code=? AND state='PLANNED'",
                (rsv["reservation_code"],),
            )
            if pending is not None:
                conn.execute(
                    "UPDATE migrations SET state='ABORTED', updated_at=?, version=version+1 "
                    "WHERE migration_code=?",
                    (now, pending["migration_code"]),
                )
            self._free_capacity(conn, rsv, now)
            conn.execute(
                "UPDATE reservations SET state='PREEMPTED', preempted_at=?, updated_at=?, "
                "version=version+1 WHERE reservation_code=?",
                (now, now, rsv["reservation_code"]),
            )
            credit = self._compensation_credit(conn, rsv, "capacity_loss", now)
            contract = _loads(rsv["contract"])
            actions.append(
                {
                    "reservation_code": rsv["reservation_code"],
                    "tenant_code": rsv["tenant_code"],
                    "units": rsv["units"],
                    "priority": contract["priority"],
                    "end_at": rsv["end_at"],
                    "action": "preempted",
                    "compensation_cents": credit,
                }
            )
        recovery_order = [
            a["reservation_code"]
            for a in sorted(actions, key=lambda a: (-a["priority"], a["end_at"], a["reservation_code"]))
        ]
        for rank, action in enumerate(
            sorted(actions, key=lambda a: (-a["priority"], a["end_at"], a["reservation_code"])), start=1
        ):
            conn.execute(
                "UPDATE reservations SET recovery_rank=? WHERE reservation_code=?",
                (rank, action["reservation_code"]),
            )
        if actions:
            code = self.store.next_code(conn, "preemptions")
            conn.execute(
                "INSERT INTO preemptions(preemption_code, reason, trigger_batch, state, plan, created_at) "
                "VALUES(?,?,NULL,'EXECUTED',?,?)",
                (
                    code,
                    f"node_offline:{node['node_code']}",
                    _json(
                        {
                            "reason": "node_offline",
                            "node_code": node["node_code"],
                            "victims": actions,
                            "recovery_order": recovery_order,
                        }
                    ),
                    now,
                ),
            )
            self._audit(
                conn,
                "node_offline_preemption",
                "preemption",
                code,
                {"node_code": node["node_code"], "victim_count": len(actions)},
            )

    # ------------------------------------------------------------------
    # 过期回收与后台维护（进程重启后调用同一入口即可续跑）
    # ------------------------------------------------------------------

    def sweep(self) -> dict[str, Any]:
        """一轮维护：续跑未决迁移 → 到期回收 → 批次/报价过期 → 降级重试 → 抢占恢复。"""
        with self.store.tx() as conn:
            now = self._now()
            summary: dict[str, Any] = {
                "migrations_resumed": [],
                "expired": [],
                "renewed": [],
                "migrated": [],
                "batches_expired": [],
                "quotes_expired": [],
                "degrade_retries": [],
                "recovery": {},
            }

            for migration in Store.query(
                conn, "SELECT * FROM migrations WHERE state='PLANNED' ORDER BY migration_code"
            ):
                rsv = self._must_reservation(conn, migration["reservation_code"])
                if rsv["state"] not in ("MIGRATING", "PREEMPTED"):
                    continue
                try:
                    self._commit_migration(conn, migration["migration_code"])
                    summary["migrations_resumed"].append(migration["migration_code"])
                except DomainError:
                    conn.execute(
                        "UPDATE migrations SET state='ABORTED', updated_at=?, version=version+1 "
                        "WHERE migration_code=?",
                        (now, migration["migration_code"]),
                    )
                    if rsv["state"] == "MIGRATING":
                        conn.execute(
                            "UPDATE reservations SET state='ACTIVE', updated_at=?, version=version+1 "
                            "WHERE reservation_code=?",
                            (now, rsv["reservation_code"]),
                        )

            for rsv in Store.query(
                conn,
                "SELECT * FROM reservations WHERE state IN ('ACTIVE','MIGRATING') AND end_at<=? "
                "ORDER BY reservation_code",
                (now,),
            ):
                outcome = self._handle_expiry(conn, rsv, now)
                summary[outcome].append(rsv["reservation_code"])

            for batch in Store.query(
                conn,
                "SELECT * FROM capacity_batches WHERE state='OPEN' AND available_until<=?",
                (now,),
            ):
                conn.execute(
                    "UPDATE capacity_batches SET state='EXPIRED', updated_at=?, version=version+1 "
                    "WHERE batch_code=?",
                    (now, batch["batch_code"]),
                )
                summary["batches_expired"].append(batch["batch_code"])

            for quote in Store.query(
                conn, "SELECT * FROM quotes WHERE state='OPEN' AND expires_at<=?", (now,)
            ):
                conn.execute(
                    "UPDATE quotes SET state='EXPIRED', version=version+1 WHERE quote_code=?",
                    (quote["quote_code"],),
                )
                summary["quotes_expired"].append(quote["quote_code"])

            degraded = Store.query(
                conn,
                "SELECT r.* FROM reservations r JOIN capacity_batches b ON b.batch_code=r.batch_code "
                "JOIN nodes n ON n.node_code=b.node_code "
                "WHERE n.state='DEGRADED' AND r.state='ACTIVE'",
            )
            for rsv in degraded:
                contract = _loads(rsv["contract"])
                if contract["on_degrade"] != "migrate":
                    continue
                node = Store.get(conn, "nodes", rsv["node_code"])
                target = self._find_target_batch(conn, rsv, now, exclude_fault_domain=node["fault_domain"])
                if target is None:
                    continue
                migration = self._plan_migration(
                    conn, rsv, "node_degraded", target["batch_code"], None,
                    exclude_fault_domain=node["fault_domain"],
                )
                self._commit_migration(conn, migration["migration_code"])
                self._compensation_credit(conn, rsv, "forced_migration", now)
                summary["degrade_retries"].append(rsv["reservation_code"])

            summary["recovery"] = self._run_recovery(conn, now)
            return summary

    def _handle_expiry(self, conn: sqlite3.Connection, rsv: dict[str, Any], now: str) -> str:
        """预留到期：按合同续约 → 可中断迁移 → 释放；非自愿到期给予补偿。"""
        contract = _loads(rsv["contract"])
        policy = contract["on_expiry"]
        if policy == "renew":
            new_end = add_hours(rsv["end_at"], float(contract["renew_extension_hours"]))
            batch = Store.get(conn, "capacity_batches", rsv["batch_code"])
            if batch["state"] == "OPEN" and new_end <= batch["available_until"]:
                conn.execute(
                    "UPDATE reservations SET end_at=?, updated_at=?, version=version+1 "
                    "WHERE reservation_code=?",
                    (new_end, now, rsv["reservation_code"]),
                )
                self._audit(
                    conn,
                    "reservation_auto_renewed",
                    "reservation",
                    rsv["reservation_code"],
                    {"new_end_at": new_end},
                    rsv["tenant_code"],
                )
                return "renewed"
            policy = "migrate"  # 续约不成，退而迁移
        if policy == "migrate":
            fresh = self._must_reservation(conn, rsv["reservation_code"])
            if fresh["end_at"] > now:
                # 刚续约成功或状态已变化，无需迁移
                return "renewed"
            target = self._find_target_batch(conn, fresh, now)
            if target is not None and fresh["state"] == "ACTIVE":
                try:
                    migration = self._plan_migration(conn, fresh, "expiry_migration", None, None)
                    self._commit_migration(conn, migration["migration_code"])
                except DomainError:
                    pass  # 迁移不可行时回落为到期关闭
                else:
                    fresh = self._must_reservation(conn, rsv["reservation_code"])
                    new_end = add_hours(now, float(contract["renew_extension_hours"]))
                    batch = Store.get(conn, "capacity_batches", fresh["batch_code"])
                    new_end = min(new_end, batch["available_until"])
                    conn.execute(
                        "UPDATE reservations SET end_at=?, updated_at=?, version=version+1 "
                        "WHERE reservation_code=?",
                        (new_end, now, rsv["reservation_code"]),
                    )
                    self._audit(
                        conn,
                        "expiry_migration",
                        "reservation",
                        rsv["reservation_code"],
                        {"to_batch": fresh["batch_code"], "new_end_at": new_end},
                        rsv["tenant_code"],
                    )
                    return "migrated"
        fresh = self._must_reservation(conn, rsv["reservation_code"])
        self._close_reservation(conn, fresh, "EXPIRED", "expired", fresh["end_at"])
        if contract["on_expiry"] != "release":
            horizon = min(float(contract["renew_extension_hours"]), COMPENSATION_HORIZON_HOURS)
            self._compensation_credit(conn, fresh, "forced_expiry", fresh["end_at"], horizon)
        return "expired"

    def _run_recovery(self, conn: sqlite3.Connection, now: str) -> dict[str, Any]:
        waiting = Store.query(
            conn,
            "SELECT * FROM reservations WHERE state='PREEMPTED' "
            "ORDER BY preempted_at ASC, recovery_rank ASC, reservation_code ASC",
        )
        recovered, expired, pending = [], [], []
        for rsv in waiting:
            if rsv["end_at"] <= now:
                self._close_reservation(conn, rsv, "EXPIRED", "expired_while_preempted", rsv["end_at"])
                expired.append(rsv["reservation_code"])
                continue
            target = self._find_target_batch(conn, rsv, now)
            if target is None:
                pending.append(rsv["reservation_code"])
                continue
            migration = self._plan_migration(conn, rsv, "recovery", target["batch_code"], None)
            self._commit_migration(conn, migration["migration_code"])
            recovered.append({"reservation_code": rsv["reservation_code"], "to_batch": target["batch_code"]})
        return {"recovered": recovered, "expired": expired, "pending": pending}

    def start_background_sweep(self, interval_seconds: float = 30.0) -> None:
        """后台循环执行 sweep；进程重启后重新调用即可继续。"""
        if self._bg_thread is not None:
            return

        def loop() -> None:
            while not self._bg_stop.wait(interval_seconds):
                try:
                    self.sweep()
                except Exception:  # 后台任务不让异常杀死线程
                    pass

        self._bg_thread = threading.Thread(target=loop, name="sweep", daemon=True)
        self._bg_thread.start()

    def stop_background_sweep(self) -> None:
        self._bg_stop.set()
        if self._bg_thread is not None:
            self._bg_thread.join(timeout=5)
            self._bg_thread = None

    # ------------------------------------------------------------------
    # 结算：差额入账 + 账单签发
    # ------------------------------------------------------------------

    def settle(
        self,
        tenant_code: str | None = None,
        reservation_code: str | None = None,
        upto: str | None = None,
    ) -> dict[str, Any]:
        upto_iso = format_time(parse_time(upto)) if upto else self._now()
        with self.store.tx() as conn:
            if reservation_code:
                targets = [self._must_reservation(conn, reservation_code)]
            elif tenant_code:
                targets = Store.query(
                    conn,
                    "SELECT * FROM reservations WHERE tenant_code=? ORDER BY reservation_code",
                    (tenant_code,),
                )
            else:
                targets = Store.query(conn, "SELECT * FROM reservations ORDER BY reservation_code")
            new_bills = []
            for rsv in targets:
                bill = self._settle_reservation(conn, rsv, upto_iso)
                if bill is not None:
                    new_bills.append(bill)
            return {"upto": upto_iso, "new_bills": new_bills, "settled": [t["reservation_code"] for t in targets]}

    def _settle_reservation(
        self, conn: sqlite3.Connection, rsv: dict[str, Any], upto: str
    ) -> dict[str, Any] | None:
        """按"应收总额 - 已入账"差额入账，并把未出账分录签发为唯一账单。"""
        segments = Store.query(
            conn,
            "SELECT * FROM reservation_segments WHERE reservation_code=? ORDER BY seq",
            (rsv["reservation_code"],),
        )
        if not segments:
            return None
        expected_cents = 0
        breakdown = []
        for seg in segments:
            seg_end = seg["end_at"] or upto
            effective_end = min(seg_end, upto)
            if effective_end <= seg["start_at"]:
                continue
            if rsv["billing_mode"] == "reserved":
                hours = hours_between(seg["start_at"], effective_end)
                cents = to_cents(seg["units"] * rsv["unit_price"] * hours)
                detail = {"hours": round(hours, 6), "units": seg["units"]}
            else:
                usage = Store.one(
                    conn,
                    "SELECT COALESCE(SUM(unit_hours), 0) AS uh, COUNT(*) AS n FROM consumption_events "
                    "WHERE reservation_code=? AND event_type='usage' AND occurred_at>=? AND occurred_at<?",
                    (rsv["reservation_code"], seg["start_at"], effective_end),
                )
                cents = to_cents(usage["uh"] * rsv["unit_price"])
                detail = {"unit_hours": usage["uh"], "event_count": usage["n"]}
            expected_cents += cents
            breakdown.append(
                {
                    "segment_code": seg["segment_code"],
                    "batch_code": seg["batch_code"],
                    "from": seg["start_at"],
                    "to": effective_end,
                    "amount_cents": cents,
                    **detail,
                }
            )
        charged_row = Store.one(
            conn,
            "SELECT COALESCE(SUM(amount_cents), 0) AS total FROM ledger_entries "
            "WHERE reservation_code=? AND entry_type='charge'",
            (rsv["reservation_code"],),
        )
        delta = expected_cents - charged_row["total"]
        now = self._now()
        if delta != 0:
            period_start = rsv["settled_upto"] or segments[0]["start_at"]
            conn.execute(
                "INSERT INTO ledger_entries(entry_code, bill_code, reservation_code, task_code, entry_type, "
                "reason, amount_cents, period_start, period_end, source, created_at) "
                "VALUES(?,NULL,?,NULL,'charge','settlement',?,?,?,?,?)",
                (
                    self.store.next_code(conn, "ledger_entries"),
                    rsv["reservation_code"],
                    delta,
                    period_start,
                    upto,
                    _json({"segments": breakdown}),
                    now,
                ),
            )
            conn.execute(
                "UPDATE reservations SET settled_upto=?, updated_at=? WHERE reservation_code=?",
                (upto, now, rsv["reservation_code"]),
            )
        unbilled = Store.query(
            conn,
            "SELECT * FROM ledger_entries WHERE reservation_code=? AND bill_code IS NULL "
            "ORDER BY entry_code",
            (rsv["reservation_code"],),
        )
        if not unbilled:
            return None
        bill_code = self.store.next_code(conn, "bills")
        period_start = min(e["period_start"] for e in unbilled)
        period_end = max(e["period_end"] for e in unbilled)
        amount = sum(e["amount_cents"] for e in unbilled)
        conn.execute(
            "INSERT INTO bills(bill_code, tenant_code, reservation_code, period_start, period_end, "
            "amount_cents, entry_count, state, issued_at) VALUES(?,?,?,?,?,?,?,'ISSUED',?)",
            (
                bill_code,
                rsv["tenant_code"],
                rsv["reservation_code"],
                period_start,
                period_end,
                amount,
                len(unbilled),
                now,
            ),
        )
        for entry in unbilled:
            conn.execute(
                "UPDATE ledger_entries SET bill_code=? WHERE entry_code=?",
                (bill_code, entry["entry_code"]),
            )
        self._audit(
            conn,
            "bill_issued",
            "bill",
            bill_code,
            {
                "reservation_code": rsv["reservation_code"],
                "amount_cents": amount,
                "entry_count": len(unbilled),
            },
            rsv["tenant_code"],
        )
        return self.get_bill(bill_code)

    def get_bill(self, bill_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            bill = Store.get(conn, "bills", bill_code)
            if bill is None:
                raise not_found("账单", bill_code)
            entries = Store.query(
                conn,
                "SELECT * FROM ledger_entries WHERE bill_code=? ORDER BY entry_code",
                (bill_code,),
            )
            bill["amount"] = yuan(bill["amount_cents"])
            bill["entries"] = [self._view_entry(e) for e in entries]
            return bill

    def list_bills(self, tenant_code: str | None = None) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            if tenant_code:
                rows = Store.query(
                    conn, "SELECT * FROM bills WHERE tenant_code=? ORDER BY bill_code", (tenant_code,)
                )
            else:
                rows = Store.query(conn, "SELECT * FROM bills ORDER BY bill_code")
            for row in rows:
                row["amount"] = yuan(row["amount_cents"])
            return rows

    def list_ledger(self, reservation_code: str) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = Store.query(
                conn,
                "SELECT * FROM ledger_entries WHERE reservation_code=? ORDER BY entry_code",
                (reservation_code,),
            )
            return [self._view_entry(e) for e in rows]

    @staticmethod
    def _view_entry(entry: dict[str, Any]) -> dict[str, Any]:
        entry = dict(entry)
        entry["amount"] = yuan(entry["amount_cents"])
        entry["source"] = _loads(entry["source"])
        return entry

    def _compensation_credit(
        self,
        conn: sqlite3.Connection,
        rsv: dict[str, Any],
        rule: str,
        at: str,
        horizon_hours: float | None = None,
    ) -> int:
        """按合同补偿规则写入贷记分录（负数金额），返回贷记分。"""
        contract = _loads(rsv["contract"])
        rate = float(contract["compensation"][rule])
        if horizon_hours is None:
            horizon_hours = min(
                max(hours_between(at, rsv["end_at"]), 0.0), COMPENSATION_HORIZON_HOURS
            )
        base_cents = to_cents(rsv["units"] * rsv["unit_price"] * horizon_hours)
        credit = -int(round(base_cents * rate))
        if credit == 0:
            return 0
        conn.execute(
            "INSERT INTO ledger_entries(entry_code, bill_code, reservation_code, task_code, entry_type, "
            "reason, amount_cents, period_start, period_end, source, created_at) "
            "VALUES(?,NULL,?,NULL,'credit',?,?,?,?,?,?)",
            (
                self.store.next_code(conn, "ledger_entries"),
                rsv["reservation_code"],
                rule,
                credit,
                at,
                rsv["end_at"],
                _json({"rule": rule, "rate": rate, "base_cents": base_cents}),
                self._now(),
            ),
        )
        return credit

    # ------------------------------------------------------------------
    # 占用区间与容量释放
    # ------------------------------------------------------------------

    def _open_segment(
        self,
        conn: sqlite3.Connection,
        reservation_code: str,
        batch_code: str,
        node_code: str,
        units: int,
        start_at: str,
    ) -> None:
        row = Store.one(
            conn,
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM reservation_segments WHERE reservation_code=?",
            (reservation_code,),
        )
        conn.execute(
            "INSERT INTO reservation_segments(segment_code, reservation_code, batch_code, node_code, seq, "
            "units, start_at, end_at) VALUES(?,?,?,?,?,?,?,NULL)",
            (
                self.store.next_code(conn, "reservation_segments"),
                reservation_code,
                batch_code,
                node_code,
                row["next_seq"],
                units,
                start_at,
            ),
        )

    def _close_open_segment(self, conn: sqlite3.Connection, reservation_code: str, at: str) -> None:
        conn.execute(
            "UPDATE reservation_segments SET end_at=? WHERE reservation_code=? AND end_at IS NULL",
            (at, reservation_code),
        )

    def _free_capacity(self, conn: sqlite3.Connection, rsv: dict[str, Any], at: str) -> None:
        """关闭当前占用区间并把配额还给批次（迁移源侧/抢占/关闭时调用）。"""
        self._close_open_segment(conn, rsv["reservation_code"], at)
        conn.execute(
            "UPDATE capacity_batches SET allocated_units = allocated_units - ?, updated_at=?, "
            "version=version+1 WHERE batch_code=? AND allocated_units >= ?",
            (rsv["units"], at, rsv["batch_code"], rsv["units"]),
        )

    def _close_reservation(
        self,
        conn: sqlite3.Connection,
        rsv: dict[str, Any],
        state: str,
        reason: str,
        effective_at: str,
    ) -> None:
        if rsv["state"] in ("ACTIVE", "MIGRATING"):
            self._free_capacity(conn, rsv, effective_at)
        now = self._now()
        conn.execute(
            "UPDATE reservations SET state=?, closed_at=?, close_reason=?, updated_at=?, "
            "version=version+1 WHERE reservation_code=?",
            (state, now, reason, now, rsv["reservation_code"]),
        )
        self._audit(
            conn,
            f"reservation_{state.lower()}",
            "reservation",
            rsv["reservation_code"],
            {"reason": reason, "effective_at": effective_at},
            rsv["tenant_code"],
        )
        fresh = self._must_reservation(conn, rsv["reservation_code"])
        self._settle_reservation(conn, fresh, effective_at)

    # ------------------------------------------------------------------
    # 查询：预留、事件、审计、履约轨迹
    # ------------------------------------------------------------------

    def _must_reservation(self, conn: sqlite3.Connection, reservation_code: str) -> dict[str, Any]:
        rsv = Store.get(conn, "reservations", reservation_code)
        if rsv is None:
            raise not_found("预留", reservation_code)
        return rsv

    @staticmethod
    def _check_version(rsv: dict[str, Any], expected_version: int | None) -> None:
        if expected_version is not None and expected_version != rsv["version"]:
            raise conflict(
                "version_conflict",
                f"预留版本冲突: 期望 {expected_version}，实际 {rsv['version']}",
            )

    def get_reservation(self, reservation_code: str) -> dict[str, Any]:
        with self.store.read() as conn:
            return self._view_reservation(conn, reservation_code)

    def _view_reservation(self, conn: sqlite3.Connection, reservation_code: str) -> dict[str, Any]:
        rsv = self._must_reservation(conn, reservation_code)
        rsv["contract"] = _loads(rsv["contract"])
        rsv["segments"] = Store.query(
            conn,
            "SELECT * FROM reservation_segments WHERE reservation_code=? ORDER BY seq",
            (reservation_code,),
        )
        return rsv

    def list_reservations(
        self, tenant_code: str | None = None, state: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM reservations"
        clauses, params = [], []
        if tenant_code:
            clauses.append("tenant_code=?")
            params.append(tenant_code)
        if state:
            clauses.append("state=?")
            params.append(state)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY reservation_code"
        with self.store.read() as conn:
            rows = Store.query(conn, sql, tuple(params))
            for row in rows:
                row["contract"] = _loads(row["contract"])
            return rows

    def list_events(self, reservation_code: str) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = Store.query(
                conn,
                "SELECT * FROM consumption_events WHERE reservation_code=? ORDER BY occurred_at, seq",
                (reservation_code,),
            )
            for row in rows:
                row["payload"] = _loads(row["payload"])
            return rows

    def list_preemptions(self) -> list[dict[str, Any]]:
        with self.store.read() as conn:
            rows = Store.query(conn, "SELECT * FROM preemptions ORDER BY preemption_code")
            for row in rows:
                row["plan"] = _loads(row["plan"])
            return rows

    def list_audit(
        self,
        resource_type: str | None = None,
        resource_code: str | None = None,
        tenant_code: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM audit_log"
        clauses, params = [], []
        if resource_type:
            clauses.append("resource_type=?")
            params.append(resource_type)
        if resource_code:
            clauses.append("resource_code=?")
            params.append(resource_code)
        if tenant_code:
            clauses.append("tenant_code=?")
            params.append(tenant_code)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY audit_code"
        with self.store.read() as conn:
            rows = Store.query(conn, sql, tuple(params))
            for row in rows:
                row["detail"] = _loads(row["detail"])
            return rows

    def trail(self, tenant_code: str) -> dict[str, Any]:
        """租户履约轨迹：报价 → 预留/占用区间 → 任务 → 事件 → 迁移/抢占 → 账本分录 → 账单。"""
        with self.store.read() as conn:
            quotes = [
                self._view_quote(q)
                for q in Store.query(
                    conn, "SELECT * FROM quotes WHERE tenant_code=? ORDER BY quote_code", (tenant_code,)
                )
            ]
            reservations = []
            for rsv in Store.query(
                conn,
                "SELECT * FROM reservations WHERE tenant_code=? ORDER BY reservation_code",
                (tenant_code,),
            ):
                view = self._view_reservation(conn, rsv["reservation_code"])
                view["ledger"] = [
                    self._view_entry(e)
                    for e in Store.query(
                        conn,
                        "SELECT * FROM ledger_entries WHERE reservation_code=? ORDER BY entry_code",
                        (rsv["reservation_code"],),
                    )
                ]
                view["events"] = self.list_events(rsv["reservation_code"])
                reservations.append(view)
            groups = []
            for group in Store.query(
                conn, "SELECT * FROM task_groups WHERE tenant_code=? ORDER BY group_code", (tenant_code,)
            ):
                groups.append(self.get_task_group(group["group_code"]))
            bills = []
            for bill in Store.query(
                conn, "SELECT * FROM bills WHERE tenant_code=? ORDER BY bill_code", (tenant_code,)
            ):
                bills.append(self.get_bill(bill["bill_code"]))
            rsv_codes = {r["reservation_code"] for r in reservations}
            migrations = [
                m
                for m in Store.query(conn, "SELECT * FROM migrations ORDER BY migration_code")
                if m["reservation_code"] in rsv_codes
            ]
            preemptions = []
            for pre in Store.query(conn, "SELECT * FROM preemptions ORDER BY preemption_code"):
                plan = _loads(pre["plan"])
                pre["plan"] = plan
                if any(v.get("tenant_code") == tenant_code for v in plan.get("victims", [])):
                    preemptions.append(pre)
            audit = self.list_audit(tenant_code=tenant_code)
            return {
                "tenant_code": tenant_code,
                "generated_at": self._now(),
                "quotes": quotes,
                "reservations": reservations,
                "task_groups": groups,
                "migrations": migrations,
                "preemptions": preemptions,
                "bills": bills,
                "audit": audit,
            }
