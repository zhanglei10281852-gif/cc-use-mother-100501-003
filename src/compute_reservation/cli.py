"""命令行入口：租户与运营方仅凭 CLI 即可完成报价、锁定、结算与轨迹核对。

用法示例：
    python -m compute_reservation.cli --db data.db node-register --name 华东A --fault-domain room-a --energy-level low
    python -m compute_reservation.cli --db data.db demo
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .models import DomainError, format_time, now_utc, parse_time
from .store import Store
from .system import System

DEFAULT_DB = os.environ.get("COMPUTE_RESERVATION_DB", "compute_reservation.db")


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _json_arg(value: str | None, file_value: str | None) -> Any:
    if file_value:
        return json.loads(Path(file_value).read_text(encoding="utf-8"))
    if value:
        return json.loads(value)
    return None


def _csv(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="compute-reservation", description="跨节点算力预留与履约")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite 数据库路径（默认 %(default)s）")
    parser.add_argument("--actor", default="cli", help="操作者标识，写入审计日志")
    parser.add_argument("--now", default=None, help="固定当前时间（ISO-8601），用于可复现演练")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("smoke", help="基础契约冒烟")

    p = sub.add_parser("node-register", help="注册算力节点")
    p.add_argument("--name", required=True)
    p.add_argument("--fault-domain", required=True)
    p.add_argument("--energy-level", required=True, choices=["low", "medium", "high"])
    p.add_argument("--capabilities", default=None, help="逗号分隔的服务能力")
    p.add_argument("--node-code", default=None)

    p = sub.add_parser("node-state", help="变更节点状态（触发降级迁移/离线抢占）")
    p.add_argument("node_code")
    p.add_argument("--state", required=True, choices=["ACTIVE", "DEGRADED", "OFFLINE"])

    sub.add_parser("nodes", help="列出节点")

    p = sub.add_parser("batch-publish", help="发布容量批次")
    p.add_argument("--node", required=True)
    p.add_argument("--resource-type", required=True, choices=["gpu", "npu", "cpu", "memory"])
    p.add_argument("--units", required=True, type=int)
    p.add_argument("--from", dest="available_from", required=True)
    p.add_argument("--until", dest="available_until", required=True)
    p.add_argument("--capabilities", default=None)

    p = sub.add_parser("batch-close", help="关闭容量批次")
    p.add_argument("batch_code")

    p = sub.add_parser("batches", help="列出容量批次")
    p.add_argument("--node", default=None)
    p.add_argument("--state", default=None)

    p = sub.add_parser("quote", help="创建可复核报价")
    p.add_argument("--tenant", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--units", required=True, type=int)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--billing-mode", default="reserved", choices=["reserved", "on_demand"])
    p.add_argument("--capabilities", default=None)

    p = sub.add_parser("quote-show", help="查看报价")
    p.add_argument("quote_code")

    p = sub.add_parser("quote-verify", help="复核报价金额")
    p.add_argument("quote_code")

    p = sub.add_parser("lock", help="幂等锁定配额（可凭报价或直接指定）")
    p.add_argument("--idempotency-key", required=True)
    p.add_argument("--quote", default=None)
    p.add_argument("--tenant", default=None)
    p.add_argument("--batch", default=None)
    p.add_argument("--units", type=int, default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--billing-mode", default=None, choices=["reserved", "on_demand"])
    p.add_argument("--contract", default=None, help="合同规则 JSON")
    p.add_argument("--contract-file", default=None)

    p = sub.add_parser("reservation", help="查看预留（含占用区间）")
    p.add_argument("reservation_code")

    p = sub.add_parser("reservations", help="列出预留")
    p.add_argument("--tenant", default=None)
    p.add_argument("--state", default=None)

    p = sub.add_parser("renew", help="续约预留")
    p.add_argument("reservation_code")
    p.add_argument("--new-end", required=True)
    p.add_argument("--expected-version", type=int, default=None)

    p = sub.add_parser("release", help="释放预留")
    p.add_argument("reservation_code")
    p.add_argument("--reason", default="tenant_release")

    p = sub.add_parser("task-group-submit", help="提交具有依赖关系的任务组")
    p.add_argument("--tenant", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--spec", required=True, help="任务定义 JSON 文件路径")

    p = sub.add_parser("task-group", help="查看任务组")
    p.add_argument("group_code")

    p = sub.add_parser("events", help="上报消费事件（JSON 数组，重复 event_id 自动去重）")
    p.add_argument("--file", required=True, help="事件 JSON 文件路径")

    p = sub.add_parser("migrate-plan", help="计划迁移")
    p.add_argument("--reservation", required=True)
    p.add_argument("--target-batch", default=None)
    p.add_argument("--reason", default="operator")
    p.add_argument("--auto-commit", action="store_true")

    p = sub.add_parser("migrate-commit", help="提交迁移（原子切换占用区间）")
    p.add_argument("migration_code")

    p = sub.add_parser("migrate-abort", help="中止迁移（仅可中断迁移）")
    p.add_argument("migration_code")

    p = sub.add_parser("migrations", help="列出迁移")
    p.add_argument("--state", default=None)

    p = sub.add_parser("preempt", help="抢占容量（保留受影响租户、补偿与恢复次序）")
    p.add_argument("--batch", required=True)
    p.add_argument("--units", required=True, type=int)
    p.add_argument("--reason", default="operator_preempt")

    sub.add_parser("preemptions", help="列出抢占记录")
    sub.add_parser("recovery", help="按恢复次序回迁被抢占预留")
    sub.add_parser("sweep", help="执行一轮过期回收与未决迁移续跑")

    p = sub.add_parser("settle", help="结算并签发账单（幂等收敛）")
    p.add_argument("--tenant", default=None)
    p.add_argument("--reservation", default=None)
    p.add_argument("--upto", default=None)

    p = sub.add_parser("bills", help="列出账单")
    p.add_argument("--tenant", default=None)

    p = sub.add_parser("bill", help="查看账单及分录")
    p.add_argument("bill_code")

    p = sub.add_parser("ledger", help="查看预留的账本分录")
    p.add_argument("--reservation", required=True)

    p = sub.add_parser("trail", help="租户完整履约轨迹（报价→占用→结算）")
    p.add_argument("--tenant", required=True)

    p = sub.add_parser("audit", help="查看审计日志")
    p.add_argument("--tenant", default=None)
    p.add_argument("--resource-type", default=None)
    p.add_argument("--resource-code", default=None)

    p = sub.add_parser("serve", help="启动 HTTP API 服务")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--sweep-interval", type=float, default=30.0)

    sub.add_parser("demo", help="端到端演练：报价→锁定→任务→降级迁移→结算→轨迹")
    return parser


def make_system(args: argparse.Namespace) -> System:
    store = Store(args.db)
    if args.now:
        fixed = parse_time(args.now)
        return System(store, now_fn=lambda: fixed, actor=args.actor)
    return System(store, now_fn=now_utc, actor=args.actor)


def run(args: argparse.Namespace) -> Any:  # noqa: C901 - 命令分发表
    cmd = args.command
    if cmd == "smoke":
        from dataclasses import asdict

        from .contracts import ComputeReservation

        item = ComputeReservation(
            reservation_code="reservation-code-001",
            tenant_code="tenant-code-001",
            capacity_batch="capacity-batch-001",
            state="state-001",
        )
        return {"item": asdict(item), "fingerprint": item.fingerprint()}
    if cmd == "serve":
        from .api import serve

        system = make_system(args)
        print(f"API 服务启动: http://{args.host}:{args.port} (db={args.db})", file=sys.stderr)
        serve(system, args.host, args.port, args.sweep_interval)
        return None
    if cmd == "demo":
        return demo(args)

    system = make_system(args)
    try:
        if cmd == "node-register":
            return system.register_node(args.name, args.fault_domain, args.energy_level, _csv(args.capabilities), args.node_code)
        if cmd == "node-state":
            return system.set_node_state(args.node_code, args.state)
        if cmd == "nodes":
            return system.list_nodes()
        if cmd == "batch-publish":
            return system.publish_batch(args.node, args.resource_type, args.units, args.available_from, args.available_until, _csv(args.capabilities))
        if cmd == "batch-close":
            return system.close_batch(args.batch_code)
        if cmd == "batches":
            return system.list_batches(node_code=args.node, state=args.state)
        if cmd == "quote":
            return system.create_quote(args.tenant, args.batch, args.units, args.start, args.end, args.billing_mode, _csv(args.capabilities))
        if cmd == "quote-show":
            return system.get_quote(args.quote_code)
        if cmd == "quote-verify":
            return system.verify_quote(args.quote_code)
        if cmd == "lock":
            return system.lock_reservation(
                idempotency_key=args.idempotency_key,
                quote_code=args.quote,
                tenant_code=args.tenant,
                batch_code=args.batch,
                units=args.units,
                end_at=args.end,
                billing_mode=args.billing_mode,
                contract=_json_arg(args.contract, args.contract_file),
            )
        if cmd == "reservation":
            return system.get_reservation(args.reservation_code)
        if cmd == "reservations":
            return system.list_reservations(tenant_code=args.tenant, state=args.state)
        if cmd == "renew":
            return system.renew_reservation(args.reservation_code, args.new_end, args.expected_version)
        if cmd == "release":
            return system.release_reservation(args.reservation_code, args.reason)
        if cmd == "task-group-submit":
            spec = json.loads(Path(args.spec).read_text(encoding="utf-8"))
            return system.submit_task_group(args.tenant, args.name, spec["tasks"] if isinstance(spec, dict) else spec)
        if cmd == "task-group":
            return system.get_task_group(args.group_code)
        if cmd == "events":
            events = json.loads(Path(args.file).read_text(encoding="utf-8"))
            return system.ingest_events(events if isinstance(events, list) else [events])
        if cmd == "migrate-plan":
            return system.plan_migration(args.reservation, args.reason, args.target_batch, auto_commit=args.auto_commit)
        if cmd == "migrate-commit":
            return system.commit_migration(args.migration_code)
        if cmd == "migrate-abort":
            return system.abort_migration(args.migration_code)
        if cmd == "migrations":
            return system.list_migrations(state=args.state)
        if cmd == "preempt":
            return system.preempt(args.batch, args.units, args.reason)
        if cmd == "preemptions":
            return system.list_preemptions()
        if cmd == "recovery":
            return system.run_recovery()
        if cmd == "sweep":
            return system.sweep()
        if cmd == "settle":
            return system.settle(tenant_code=args.tenant, reservation_code=args.reservation, upto=args.upto)
        if cmd == "bills":
            return system.list_bills(tenant_code=args.tenant)
        if cmd == "bill":
            return system.get_bill(args.bill_code)
        if cmd == "ledger":
            return system.list_ledger(args.reservation)
        if cmd == "trail":
            return system.trail(args.tenant)
        if cmd == "audit":
            return system.list_audit(args.resource_type, args.resource_code, args.tenant)
        raise DomainError("bad_request", f"未知命令 {cmd}")
    finally:
        system.store.close()


def demo(args: argparse.Namespace) -> dict[str, Any]:
    """端到端演练：夜间训练任务经历报价、锁定、降级迁移、抢占恢复与结算。"""
    clock = [parse_time(args.now) if args.now else parse_time("2026-10-06T18:00:00Z")]
    store = Store(args.db)
    system = System(store, now_fn=lambda: clock[0], actor="demo")
    steps: dict[str, Any] = {}

    def advance(hours: float) -> None:
        from datetime import timedelta

        clock[0] = clock[0] + timedelta(hours=hours)

    try:
        node_a = system.register_node("华东-A", "room-a/pdu-1", "medium", ["rdma", "nvlink"])
        node_b = system.register_node("华北-B", "room-b/pdu-3", "low", ["rdma", "nvlink"])
        batch_a = system.publish_batch(node_a["node_code"], "gpu", 8, "2026-10-06T00:00:00Z", "2026-10-09T00:00:00Z")
        batch_b = system.publish_batch(node_b["node_code"], "gpu", 8, "2026-10-06T00:00:00Z", "2026-10-10T00:00:00Z")
        steps["batches"] = [batch_a["batch_code"], batch_b["batch_code"]]

        quote = system.create_quote("tenant-lab", batch_a["batch_code"], 4, "2026-10-06T18:00:00Z", "2026-10-08T00:00:00Z")
        steps["quote"] = {"code": quote["quote_code"], "amount": quote["amount"], "verify": system.verify_quote(quote["quote_code"])["match"]}

        contract = {"on_expiry": "migrate", "on_degrade": "migrate", "on_partial": "shrink", "priority": 100}
        locked = system.lock_reservation(
            idempotency_key="lab-night-train-001",
            quote_code=quote["quote_code"],
            contract=contract,
        )
        rsv = locked["reservation_code"]
        replay = system.lock_reservation(
            idempotency_key="lab-night-train-001",
            quote_code=quote["quote_code"],
            contract=contract,
        )
        steps["lock"] = {"reservation": rsv, "idempotent_replay": replay.get("idempotent_replay", False)}

        group = system.submit_task_group(
            "tenant-lab",
            "night-train",
            [
                {"name": "prepare", "reservation_code": rsv, "required_units": 1},
                {"name": "train", "reservation_code": rsv, "required_units": 4, "depends_on": ["prepare"]},
                {"name": "evaluate", "reservation_code": rsv, "required_units": 2, "depends_on": ["train"]},
            ],
        )
        steps["task_group"] = group["group_code"]

        advance(2)
        system.ingest_events([
            {"event_id": "e1", "reservation_code": rsv, "task_code": group["tasks"][0]["task_code"], "event_type": "task_started", "occurred_at": format_time(clock[0])},
            {"event_id": "e2", "reservation_code": rsv, "task_code": group["tasks"][0]["task_code"], "event_type": "task_finished", "occurred_at": format_time(clock[0]), "payload": {"result": "SUCCEEDED"}},
        ])
        advance(6)
        # 节点降级：合同 on_degrade=migrate → 自动迁往其他故障域并补偿
        system.set_node_state(node_a["node_code"], "DEGRADED")
        moved = system.get_reservation(rsv)
        steps["degrade_migration"] = {
            "from_batch": batch_a["batch_code"],
            "to_batch": moved["batch_code"],
            "segments": len(moved["segments"]),
        }

        advance(8)
        system.ingest_events([
            {"event_id": "e3", "reservation_code": rsv, "task_code": group["tasks"][1]["task_code"], "event_type": "task_started", "occurred_at": format_time(clock[0])},
            {"event_id": "e4", "reservation_code": rsv, "task_code": group["tasks"][1]["task_code"], "event_type": "task_finished", "occurred_at": format_time(clock[0]), "payload": {"result": "SUCCEEDED"}},
        ])
        settlement = system.settle(tenant_code="tenant-lab")
        steps["settlement"] = [b["bill_code"] for b in settlement["new_bills"]]
        # 重复结算应收敛：不再产生新账单
        steps["settlement_idempotent"] = len(system.settle(tenant_code="tenant-lab")["new_bills"]) == 0

        trail = system.trail("tenant-lab")
        steps["trail_summary"] = {
            "quotes": len(trail["quotes"]),
            "reservations": len(trail["reservations"]),
            "bills": len(trail["bills"]),
            "audit_events": len(trail["audit"]),
        }
        return steps
    finally:
        store.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run(args)
    except DomainError as exc:
        _print(exc.to_dict())
        return 2
    if result is not None:
        _print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
