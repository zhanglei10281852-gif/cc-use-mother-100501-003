"""HTTP API（仅标准库实现）。

约定：
- 请求与响应均为 JSON；租户身份经 X-Tenant-Id 头或请求体 tenant_id 提供；
- 锁定配额等写操作通过 Idempotency-Key 头保证幂等；
- 错误统一为 {"error": {"code", "message"}}。
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import errors
from .backend import Backend


def _tenant_of(headers, body: dict) -> str:
    tenant = headers.get("X-Tenant-Id") or body.get("tenant_id") or ""
    if not tenant:
        raise errors.validation("缺少租户标识（X-Tenant-Id 头或 tenant_id 字段）")
    if body.get("tenant_id") and headers.get("X-Tenant-Id") and body["tenant_id"] != headers["X-Tenant-Id"]:
        raise errors.forbidden("请求体与请求头的租户标识不一致")
    return tenant


def _f(body: dict, key: str, default: float | None = None) -> float:
    value = body.get(key, default)
    if value is None:
        raise errors.validation(f"缺少必填字段 {key}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise errors.validation(f"字段 {key} 必须是数字") from exc


def _i(body: dict, key: str, default: int | None = None) -> int:
    value = body.get(key, default)
    if value is None:
        raise errors.validation(f"缺少必填字段 {key}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise errors.validation(f"字段 {key} 必须是整数") from exc


def _b(body: dict, key: str, default: bool) -> bool:
    return bool(body.get(key, default))


# ---------------------------------------------------------------------- 处理器

def publish_batch(backend: Backend, match, body, headers, query):
    node_id = match.group(1)
    return 201, backend.catalog.publish_batch(
        node_id=node_id,
        total_cards=_i(body, "total_cards"),
        valid_from=_f(body, "valid_from"),
        valid_until=_f(body, "valid_until"),
        fault_domain=str(body.get("fault_domain", "")),
        energy_tier=str(body.get("energy_tier", "P2")),
        capabilities=list(body.get("capabilities", [])),
        price_per_card_hour=_f(body, "price_per_card_hour"),
        inventory_ref=body.get("inventory_ref"),
    )


def list_batches(backend: Backend, match, body, headers, query):
    return 200, {"batches": backend.catalog.list_batches(
        state=query.get("state", [None])[0], node_id=query.get("node_id", [None])[0])}


def request_quote(backend: Backend, match, body, headers, query):
    tenant = _tenant_of(headers, body)
    return 201, backend.quotes.request_quote(
        tenant_id=tenant,
        cards=_i(body, "cards"),
        start_at=_f(body, "start_at"),
        duration_seconds=_f(body, "duration_seconds"),
        required_capabilities=list(body.get("capabilities", [])),
        fault_domain=body.get("fault_domain"),
        node_id=body.get("node_id"),
    )


def get_quote(backend: Backend, match, body, headers, query):
    return 200, backend.quotes.get_quote(match.group(1))


def verify_quote(backend: Backend, match, body, headers, query):
    return 200, backend.quotes.verify_quote(match.group(1))


def reserve(backend: Backend, match, body, headers, query):
    tenant = _tenant_of(headers, body)
    idem_key = headers.get("Idempotency-Key") or body.get("idempotency_key") or ""
    if not idem_key:
        raise errors.validation("锁定配额必须提供 Idempotency-Key")
    return 201, backend.reservations.reserve(
        tenant_id=tenant,
        quote_id=str(body.get("quote_id", "")),
        idem_key=idem_key,
        auto_renew=_b(body, "auto_renew", False),
        max_renewals=_i(body, "max_renewals", 0),
        migratable=_b(body, "migratable", True),
        interruptible=_b(body, "interruptible", True),
        shrink_on_partial=_b(body, "shrink_on_partial", True),
        extension_seconds=_f(body, "extension_seconds", 3600.0),
        tenant_priority=_i(body, "tenant_priority", 100),
    )


def get_reservation(backend: Backend, match, body, headers, query):
    return 200, backend.reservations.get(_tenant_of(headers, body), match.group(1))


def list_reservations(backend: Backend, match, body, headers, query):
    return 200, {"reservations": backend.reservations.list_for_tenant(_tenant_of(headers, body))}


def release_reservation(backend: Backend, match, body, headers, query):
    return 200, backend.reservations.release(
        tenant_id=_tenant_of(headers, body), reservation_id=match.group(1),
        reason=str(body.get("reason", "tenant_release")))


def renew_reservation(backend: Backend, match, body, headers, query):
    return 200, backend.lifecycle.renew(
        tenant_id=_tenant_of(headers, body), reservation_id=match.group(1))


def submit_group(backend: Backend, match, body, headers, query):
    tenant = _tenant_of(headers, body)
    return 201, backend.tasks.submit_group(
        tenant_id=tenant,
        reservation_id=str(body.get("reservation_id", "")),
        tasks=list(body.get("tasks", [])),
        idem_key=headers.get("Idempotency-Key") or body.get("idempotency_key"),
    )


def get_group(backend: Backend, match, body, headers, query):
    return 200, backend.tasks.get_group(_tenant_of(headers, body), match.group(1))


def complete_task(backend: Backend, match, body, headers, query):
    return 200, backend.tasks.complete_task(tenant_id=_tenant_of(headers, body), task_id=match.group(1))


def degrade_node(backend: Backend, match, body, headers, query):
    return 200, backend.lifecycle.degrade_node(
        node_id=match.group(1), reason=str(body.get("reason", "手动降级")),
        actor=headers.get("X-Actor", "system:ops"))


def restore_node(backend: Backend, match, body, headers, query):
    return 200, backend.lifecycle.restore_node(
        node_id=match.group(1), actor=headers.get("X-Actor", "system:ops"))


def preempt(backend: Backend, match, body, headers, query):
    ids = list(body.get("reservation_ids", []))
    if not ids and body.get("node_id"):
        rows = backend.store.query(
            """
            SELECT r.reservation_id FROM reservations r
            JOIN batches b ON b.batch_id = r.batch_id
            WHERE b.node_id = ? AND r.state = 'ACTIVE'
            """,
            (body["node_id"],),
        )
        ids = [r["reservation_id"] for r in rows]
    if not ids:
        raise errors.validation("必须提供 reservation_ids 或 node_id")
    return 201, backend.lifecycle.preempt(
        reservation_ids=ids, reason=str(body.get("reason", "运营抢占")),
        created_by=headers.get("X-Actor", "system:ops"))


def restore_preemption(backend: Backend, match, body, headers, query):
    return 200, backend.lifecycle.restore_preemption(
        preemption_id=match.group(1), actor=headers.get("X-Actor", "system:ops"))


def get_preemption(backend: Backend, match, body, headers, query):
    return 200, backend.lifecycle.get_preemption(match.group(1))


def ingest_usage(backend: Backend, match, body, headers, query):
    return 200, backend.billing.ingest_events(
        events=list(body.get("events", [])), actor=headers.get("X-Actor", "system:ingest"))


def generate_bill(backend: Backend, match, body, headers, query):
    tenant = _tenant_of(headers, body)
    return 201, backend.billing.generate_bill(
        tenant_id=tenant,
        reservation_id=str(body.get("reservation_id", "")),
        period_start=_f(body, "period_start"),
        period_end=_f(body, "period_end"),
        finalize=_b(body, "finalize", False),
    )


def get_bill(backend: Backend, match, body, headers, query):
    return 200, backend.billing.get_bill(_tenant_of(headers, body), match.group(1))


def list_bills(backend: Backend, match, body, headers, query):
    tenant = _tenant_of(headers, body)
    return 200, {"bills": backend.billing.list_bills(tenant, query.get("reservation_id", [None])[0])}


def trail(backend: Backend, match, body, headers, query):
    tenant = match.group(1)
    header_tenant = headers.get("X-Tenant-Id")
    if header_tenant and header_tenant != tenant:
        raise errors.forbidden("只能核对本租户的履约轨迹")
    return 200, backend.trail_service.trail(
        tenant_id=tenant, reservation_id=query.get("reservation_id", [None])[0])


def admin_sweep(backend: Backend, match, body, headers, query):
    return 200, backend.lifecycle.sweep()


def admin_recover(backend: Backend, match, body, headers, query):
    return 200, backend.recover()


def health(backend: Backend, match, body, headers, query):
    return 200, {"status": "ok", "now": backend.clock.now()}


ROUTES = [
    ("POST", re.compile(r"^/v1/nodes/([^/]+)/batches$"), publish_batch),
    ("GET", re.compile(r"^/v1/batches$"), list_batches),
    ("POST", re.compile(r"^/v1/quotes$"), request_quote),
    ("GET", re.compile(r"^/v1/quotes/([^/]+)$"), get_quote),
    ("GET", re.compile(r"^/v1/quotes/([^/]+)/verify$"), verify_quote),
    ("POST", re.compile(r"^/v1/reservations$"), reserve),
    ("GET", re.compile(r"^/v1/reservations$"), list_reservations),
    ("GET", re.compile(r"^/v1/reservations/([^/]+)$"), get_reservation),
    ("POST", re.compile(r"^/v1/reservations/([^/]+)/release$"), release_reservation),
    ("POST", re.compile(r"^/v1/reservations/([^/]+)/renew$"), renew_reservation),
    ("POST", re.compile(r"^/v1/task-groups$"), submit_group),
    ("GET", re.compile(r"^/v1/task-groups/([^/]+)$"), get_group),
    ("POST", re.compile(r"^/v1/tasks/([^/]+)/complete$"), complete_task),
    ("POST", re.compile(r"^/v1/nodes/([^/]+)/degrade$"), degrade_node),
    ("POST", re.compile(r"^/v1/nodes/([^/]+)/restore$"), restore_node),
    ("POST", re.compile(r"^/v1/preemptions$"), preempt),
    ("GET", re.compile(r"^/v1/preemptions/([^/]+)$"), get_preemption),
    ("POST", re.compile(r"^/v1/preemptions/([^/]+)/restore$"), restore_preemption),
    ("POST", re.compile(r"^/v1/usage-events$"), ingest_usage),
    ("POST", re.compile(r"^/v1/bills$"), generate_bill),
    ("GET", re.compile(r"^/v1/bills$"), list_bills),
    ("GET", re.compile(r"^/v1/bills/([^/]+)$"), get_bill),
    ("GET", re.compile(r"^/v1/tenants/([^/]+)/trail$"), trail),
    ("POST", re.compile(r"^/v1/admin/sweep$"), admin_sweep),
    ("POST", re.compile(r"^/v1/admin/recover$"), admin_recover),
    ("GET", re.compile(r"^/v1/health$"), health),
]


def make_handler(backend: Backend):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # 静默访问日志
            pass

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            body: dict = {}
            if method in ("POST", "PUT", "PATCH"):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                if raw:
                    try:
                        body = json.loads(raw.decode("utf-8"))
                    except json.JSONDecodeError:
                        return self._send(errors.validation("请求体不是合法 JSON"))
            headers = {k: v for k, v in self.headers.items()}
            for route_method, pattern, handler in ROUTES:
                if route_method != method:
                    continue
                match = pattern.match(parsed.path)
                if match:
                    try:
                        status, payload = handler(backend, match, body, headers, query)
                    except errors.DomainError as exc:
                        return self._send(exc)
                    except Exception as exc:  # noqa: BLE001 - 兜底为 500
                        return self._send(errors.DomainError("internal_error", f"内部错误：{exc}", 500))
                    return self._send_raw(status, payload)
            return self._send(errors.not_found(f"路由不存在：{method} {parsed.path}"))

        def _send(self, exc: errors.DomainError) -> None:
            self._send_raw(exc.status, exc.to_dict())

        def _send_raw(self, status: int, payload: object) -> None:
            data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = lambda self: self._dispatch("GET")  # noqa: E731
        do_POST = lambda self: self._dispatch("POST")  # noqa: E731

    return Handler


def create_server(backend: Backend, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    """创建 HTTP 服务；启动前执行恢复流程。"""
    backend.recover()
    return ThreadingHTTPServer((host, port), make_handler(backend))


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> None:
    backend = Backend(db_path)
    server = create_server(backend, host, port)
    try:
        server.serve_forever()
    finally:
        backend.close()
