"""命令行入口：租户与运营仅凭 CLI 即可完成全流程核对。

用法示例：
    python -m compute_reservation.cli --db compute.db publish-batch --node node-a ...
    python -m compute_reservation.cli --db compute.db trail --tenant tenant-1
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from . import errors
from .backend import Backend


def parse_time(value: str) -> float:
    """接受 Unix 秒或 ISO-8601（如 2026-01-01T08:00:00Z）。"""
    try:
        return float(value)
    except ValueError:
        pass
    text = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError as exc:
        raise errors.validation(f"无法解析时间 {value!r}，请使用 Unix 秒或 ISO-8601") from exc


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="compute-reservation", description="跨节点算力预留与履约命令行")
    parser.add_argument("--db", default="compute.db", help="SQLite 数据库路径（默认 compute.db）")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("serve", help="启动 HTTP API 服务")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)

    p = sub.add_parser("publish-batch", help="节点发布容量批次")
    p.add_argument("--node", required=True)
    p.add_argument("--cards", type=int, required=True)
    p.add_argument("--valid-from", required=True)
    p.add_argument("--valid-until", required=True)
    p.add_argument("--fault-domain", required=True)
    p.add_argument("--energy-tier", default="P2", choices=["P1", "P2", "P3"])
    p.add_argument("--capabilities", default="", help="逗号分隔，如 training,inference")
    p.add_argument("--price", type=float, required=True, help="每卡时单价")
    p.add_argument("--inventory-ref", default=None, help="物理库存引用，平台内唯一")

    sub.add_parser("list-batches", help="列出容量批次")

    p = sub.add_parser("quote", help="申请可复核报价")
    p.add_argument("--tenant", required=True)
    p.add_argument("--cards", type=int, required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--duration", type=float, required=True, help="秒")
    p.add_argument("--capabilities", default="")

    p = sub.add_parser("verify-quote", help="复核报价指纹")
    p.add_argument("--quote", required=True)

    p = sub.add_parser("reserve", help="幂等锁定配额")
    p.add_argument("--tenant", required=True)
    p.add_argument("--quote", required=True)
    p.add_argument("--idem-key", required=True)
    p.add_argument("--auto-renew", action="store_true")
    p.add_argument("--max-renewals", type=int, default=0)
    p.add_argument("--no-migratable", action="store_true")
    p.add_argument("--no-interruptible", action="store_true")
    p.add_argument("--no-shrink", action="store_true")
    p.add_argument("--extension-seconds", type=float, default=3600.0)
    p.add_argument("--priority", type=int, default=100)

    p = sub.add_parser("release", help="释放预留")
    p.add_argument("--tenant", required=True)
    p.add_argument("--reservation", required=True)

    p = sub.add_parser("renew", help="手动续约")
    p.add_argument("--tenant", required=True)
    p.add_argument("--reservation", required=True)

    p = sub.add_parser("submit-group", help="提交带依赖的任务组（JSON 文件）")
    p.add_argument("--tenant", required=True)
    p.add_argument("--reservation", required=True)
    p.add_argument("--file", required=True, help='[{"name":"a","cards":2,"depends_on":[]}]')
    p.add_argument("--idem-key", default=None)

    p = sub.add_parser("complete-task", help="标记任务完成")
    p.add_argument("--tenant", required=True)
    p.add_argument("--task", required=True)

    p = sub.add_parser("degrade-node", help="节点降级")
    p.add_argument("--node", required=True)
    p.add_argument("--reason", default="手动降级")

    p = sub.add_parser("restore-node", help="节点恢复")
    p.add_argument("--node", required=True)

    p = sub.add_parser("preempt", help="抢占预留")
    p.add_argument("--reservations", required=True, help="逗号分隔的预留 ID")
    p.add_argument("--reason", default="运营抢占")

    p = sub.add_parser("restore-preemption", help="按恢复次序重新接纳被抢占预留")
    p.add_argument("--preemption", required=True)

    p = sub.add_parser("ingest-usage", help="摄入消费事件（JSON 文件）")
    p.add_argument("--file", required=True)

    p = sub.add_parser("bill", help="生成/收敛账单")
    p.add_argument("--tenant", required=True)
    p.add_argument("--reservation", required=True)
    p.add_argument("--from", dest="period_start", required=True)
    p.add_argument("--to", dest="period_end", required=True)
    p.add_argument("--finalize", action="store_true")

    p = sub.add_parser("trail", help="核对完整履约轨迹")
    p.add_argument("--tenant", required=True)
    p.add_argument("--reservation", default=None)

    sub.add_parser("sweep", help="执行到期清扫")
    sub.add_parser("recover", help="执行重启恢复（未决迁移 + 到期清扫）")
    return parser


def run(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        from .api import serve
        serve(args.db, args.host, args.port)
        return 0

    backend = Backend(args.db)
    try:
        result = _dispatch(backend, args)
        if result is not None:
            emit(result)
        return 0
    except errors.DomainError as exc:
        emit(exc.to_dict())
        return 1
    finally:
        backend.close()


def _dispatch(backend: Backend, args: argparse.Namespace) -> object:
    cmd = args.command
    if cmd == "publish-batch":
        return backend.catalog.publish_batch(
            node_id=args.node, total_cards=args.cards,
            valid_from=parse_time(args.valid_from), valid_until=parse_time(args.valid_until),
            fault_domain=args.fault_domain, energy_tier=args.energy_tier,
            capabilities=[c for c in args.capabilities.split(",") if c.strip()],
            price_per_card_hour=args.price, inventory_ref=args.inventory_ref)
    if cmd == "list-batches":
        return {"batches": backend.catalog.list_batches()}
    if cmd == "quote":
        return backend.quotes.request_quote(
            tenant_id=args.tenant, cards=args.cards, start_at=parse_time(args.start),
            duration_seconds=args.duration,
            required_capabilities=[c for c in args.capabilities.split(",") if c.strip()])
    if cmd == "verify-quote":
        return backend.quotes.verify_quote(args.quote)
    if cmd == "reserve":
        return backend.reservations.reserve(
            tenant_id=args.tenant, quote_id=args.quote, idem_key=args.idem_key,
            auto_renew=args.auto_renew, max_renewals=args.max_renewals,
            migratable=not args.no_migratable, interruptible=not args.no_interruptible,
            shrink_on_partial=not args.no_shrink, extension_seconds=args.extension_seconds,
            tenant_priority=args.priority)
    if cmd == "release":
        return backend.reservations.release(tenant_id=args.tenant, reservation_id=args.reservation)
    if cmd == "renew":
        return backend.lifecycle.renew(tenant_id=args.tenant, reservation_id=args.reservation)
    if cmd == "submit-group":
        with open(args.file, encoding="utf-8") as fh:
            tasks = json.load(fh)
        return backend.tasks.submit_group(tenant_id=args.tenant, reservation_id=args.reservation,
                                          tasks=tasks, idem_key=args.idem_key)
    if cmd == "complete-task":
        return backend.tasks.complete_task(tenant_id=args.tenant, task_id=args.task)
    if cmd == "degrade-node":
        return backend.lifecycle.degrade_node(node_id=args.node, reason=args.reason)
    if cmd == "restore-node":
        return backend.lifecycle.restore_node(node_id=args.node)
    if cmd == "preempt":
        return backend.lifecycle.preempt(reservation_ids=[r for r in args.reservations.split(",") if r.strip()],
                                         reason=args.reason, created_by="cli:ops")
    if cmd == "restore-preemption":
        return backend.lifecycle.restore_preemption(preemption_id=args.preemption)
    if cmd == "ingest-usage":
        with open(args.file, encoding="utf-8") as fh:
            events = json.load(fh)
        return backend.billing.ingest_events(events=events)
    if cmd == "bill":
        return backend.billing.generate_bill(
            tenant_id=args.tenant, reservation_id=args.reservation,
            period_start=parse_time(args.period_start), period_end=parse_time(args.period_end),
            finalize=args.finalize)
    if cmd == "trail":
        return backend.trail_service.trail(tenant_id=args.tenant, reservation_id=args.reservation)
    if cmd == "sweep":
        return backend.lifecycle.sweep()
    if cmd == "recover":
        return backend.recover()
    raise errors.validation(f"未知命令 {cmd}")


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
