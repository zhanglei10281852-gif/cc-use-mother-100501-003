"""HTTP JSON API：纯标准库实现，与 CLI 共用同一个 System 门面。"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .models import DomainError
from .system import System

Handler = Callable[[System, re.Match, dict, dict], Any]

# (方法, 路径模板, 处理函数) —— 路径模板中 {name} 捕获一段路径
ROUTES: list[tuple[str, str, Handler]] = []


def route(method: str, template: str) -> Callable[[Handler], Handler]:
    def decorator(fn: Handler) -> Handler:
        ROUTES.append((method, template, fn))
        return fn

    return decorator


def _compile(template: str) -> re.Pattern:
    pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", template)
    return re.compile(f"^{pattern}$")


# ---------------------------------------------------------------------------
# 节点与容量
# ---------------------------------------------------------------------------


@route("POST", "/v1/nodes")
def create_node(system, match, body, query):
    return 201, system.register_node(
        name=body.get("name"),
        fault_domain=body.get("fault_domain"),
        energy_level=body.get("energy_level"),
        capabilities=body.get("capabilities"),
        node_code=body.get("node_code"),
    )


@route("GET", "/v1/nodes")
def list_nodes(system, match, body, query):
    return 200, system.list_nodes()


@route("GET", "/v1/nodes/{code}")
def get_node(system, match, body, query):
    return 200, system.get_node(match.group("code"))


@route("POST", "/v1/nodes/{code}/state")
def set_node_state(system, match, body, query):
    return 200, system.set_node_state(match.group("code"), body.get("state"))


@route("POST", "/v1/batches")
def publish_batch(system, match, body, query):
    return 201, system.publish_batch(
        node_code=body.get("node_code"),
        resource_type=body.get("resource_type"),
        total_units=body.get("total_units"),
        available_from=body.get("available_from"),
        available_until=body.get("available_until"),
        capabilities=body.get("capabilities"),
    )


@route("GET", "/v1/batches")
def list_batches(system, match, body, query):
    return 200, system.list_batches(
        node_code=_first(query, "node_code"), state=_first(query, "state")
    )


@route("GET", "/v1/batches/{code}")
def get_batch(system, match, body, query):
    return 200, system.get_batch(match.group("code"))


@route("POST", "/v1/batches/{code}/close")
def close_batch(system, match, body, query):
    return 200, system.close_batch(match.group("code"))


# ---------------------------------------------------------------------------
# 报价与预留
# ---------------------------------------------------------------------------


@route("POST", "/v1/quotes")
def create_quote(system, match, body, query):
    return 201, system.create_quote(
        tenant_code=body.get("tenant_code"),
        batch_code=body.get("batch_code"),
        units=body.get("units"),
        start_at=body.get("start_at"),
        end_at=body.get("end_at"),
        billing_mode=body.get("billing_mode", "reserved"),
        required_capabilities=body.get("required_capabilities"),
    )


@route("GET", "/v1/quotes/{code}")
def get_quote(system, match, body, query):
    return 200, system.get_quote(match.group("code"))


@route("POST", "/v1/quotes/{code}/verify")
def verify_quote(system, match, body, query):
    return 200, system.verify_quote(match.group("code"))


@route("POST", "/v1/reservations")
def lock_reservation(system, match, body, query):
    return 201, system.lock_reservation(
        idempotency_key=body.get("idempotency_key"),
        quote_code=body.get("quote_code"),
        tenant_code=body.get("tenant_code"),
        batch_code=body.get("batch_code"),
        units=body.get("units"),
        end_at=body.get("end_at"),
        billing_mode=body.get("billing_mode"),
        contract=body.get("contract"),
    )


@route("GET", "/v1/reservations")
def list_reservations(system, match, body, query):
    return 200, system.list_reservations(
        tenant_code=_first(query, "tenant_code"), state=_first(query, "state")
    )


@route("GET", "/v1/reservations/{code}")
def get_reservation(system, match, body, query):
    return 200, system.get_reservation(match.group("code"))


@route("POST", "/v1/reservations/{code}/renew")
def renew_reservation(system, match, body, query):
    return 200, system.renew_reservation(
        match.group("code"),
        new_end_at=body.get("new_end_at"),
        expected_version=body.get("expected_version"),
    )


@route("POST", "/v1/reservations/{code}/release")
def release_reservation(system, match, body, query):
    return 200, system.release_reservation(match.group("code"), reason=body.get("reason", "tenant_release"))


@route("GET", "/v1/reservations/{code}/events")
def list_events(system, match, body, query):
    return 200, system.list_events(match.group("code"))


@route("GET", "/v1/reservations/{code}/ledger")
def list_ledger(system, match, body, query):
    return 200, system.list_ledger(match.group("code"))


# ---------------------------------------------------------------------------
# 任务组与事件
# ---------------------------------------------------------------------------


@route("POST", "/v1/task-groups")
def submit_task_group(system, match, body, query):
    return 201, system.submit_task_group(
        tenant_code=body.get("tenant_code"),
        name=body.get("name"),
        tasks=body.get("tasks") or [],
    )


@route("GET", "/v1/task-groups/{code}")
def get_task_group(system, match, body, query):
    return 200, system.get_task_group(match.group("code"))


@route("POST", "/v1/events")
def ingest_events(system, match, body, query):
    events = body if isinstance(body, list) else body.get("events") or [body]
    return 200, system.ingest_events(events)


# ---------------------------------------------------------------------------
# 迁移、抢占与维护
# ---------------------------------------------------------------------------


@route("POST", "/v1/migrations")
def plan_migration(system, match, body, query):
    return 201, system.plan_migration(
        reservation_code=body.get("reservation_code"),
        reason=body.get("reason", "operator"),
        target_batch_code=body.get("target_batch_code"),
        interruptible=body.get("interruptible"),
        auto_commit=bool(body.get("auto_commit", False)),
    )


@route("GET", "/v1/migrations")
def list_migrations(system, match, body, query):
    return 200, system.list_migrations(state=_first(query, "state"))


@route("POST", "/v1/migrations/{code}/commit")
def commit_migration(system, match, body, query):
    return 200, system.commit_migration(match.group("code"))


@route("POST", "/v1/migrations/{code}/abort")
def abort_migration(system, match, body, query):
    return 200, system.abort_migration(match.group("code"), reason=body.get("reason", "operator_abort"))


@route("POST", "/v1/preemptions")
def preempt(system, match, body, query):
    return 201, system.preempt(
        batch_code=body.get("batch_code"),
        needed_units=body.get("needed_units"),
        reason=body.get("reason", "operator_preempt"),
    )


@route("GET", "/v1/preemptions")
def list_preemptions(system, match, body, query):
    return 200, system.list_preemptions()


@route("POST", "/v1/recovery")
def run_recovery(system, match, body, query):
    return 200, system.run_recovery()


@route("POST", "/v1/sweep")
def sweep(system, match, body, query):
    return 200, system.sweep()


# ---------------------------------------------------------------------------
# 结算与轨迹
# ---------------------------------------------------------------------------


@route("POST", "/v1/settlements")
def settle(system, match, body, query):
    return 200, system.settle(
        tenant_code=body.get("tenant_code"),
        reservation_code=body.get("reservation_code"),
        upto=body.get("upto"),
    )


@route("GET", "/v1/bills")
def list_bills(system, match, body, query):
    return 200, system.list_bills(tenant_code=_first(query, "tenant_code"))


@route("GET", "/v1/bills/{code}")
def get_bill(system, match, body, query):
    return 200, system.get_bill(match.group("code"))


@route("GET", "/v1/tenants/{code}/trail")
def trail(system, match, body, query):
    return 200, system.trail(match.group("code"))


@route("GET", "/v1/audit")
def list_audit(system, match, body, query):
    return 200, system.list_audit(
        resource_type=_first(query, "resource_type"),
        resource_code=_first(query, "resource_code"),
        tenant_code=_first(query, "tenant_code"),
    )


@route("GET", "/v1/health")
def health(system, match, body, query):
    return 200, {"status": "ok"}


def _first(query: dict, key: str) -> Any:
    values = query.get(key)
    return values[0] if values else None


# ---------------------------------------------------------------------------
# 服务器装配
# ---------------------------------------------------------------------------


def make_handler(system: System) -> type[BaseHTTPRequestHandler]:
    compiled = [(method, _compile(template), fn) for method, template, fn in ROUTES]

    class ApiHandler(BaseHTTPRequestHandler):
        server_version = "ComputeReservation/0.1"

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            body: Any = {}
            if method in ("POST", "PUT", "PATCH"):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                if raw:
                    try:
                        body = json.loads(raw.decode("utf-8"))
                    except json.JSONDecodeError:
                        self._respond(400, {"error": {"code": "bad_json", "message": "请求体不是合法 JSON"}})
                        return
            actor = self.headers.get("X-Actor")
            scoped = system.with_actor(actor) if actor else system
            for route_method, pattern, fn in compiled:
                if route_method != method:
                    continue
                match = pattern.match(parsed.path)
                if match is None:
                    continue
                try:
                    status, payload = fn(scoped, match, body, query)
                except DomainError as exc:
                    self._respond(exc.http_status, exc.to_dict())
                except Exception as exc:  # noqa: BLE001 - 兜底返回 500，不泄露堆栈
                    self._respond(500, {"error": {"code": "internal", "message": str(exc)}})
                else:
                    self._respond(status, payload)
                return
            self._respond(404, {"error": {"code": "not_found", "message": f"{method} {parsed.path} 不存在"}})

        def _respond(self, status: int, payload: Any) -> None:
            data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def log_message(self, format: str, *args: Any) -> None:  # 静默访问日志
            return

    return ApiHandler


def create_server(system: System, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(system))


def serve(system: System, host: str = "127.0.0.1", port: int = 8080, sweep_interval: float = 30.0) -> None:
    """启动 API 服务，并后台执行过期回收/未决迁移续跑。"""
    system.start_background_sweep(sweep_interval)
    server = create_server(system, host, port)
    try:
        server.serve_forever()
    finally:
        system.stop_background_sweep()
        server.server_close()
