"""HTTP API 端到端测试：真实 socket 往返。"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from compute_reservation import Store, System
from compute_reservation.api import create_server
from compute_reservation.models import parse_time

BASE = "2026-10-06T00:00:00Z"


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.clock_moment = parse_time(BASE)
        cls.store = Store(":memory:")
        cls.system = System(cls.store, now_fn=lambda: cls.clock_moment, actor="api-test")
        cls.server = create_server(cls.system, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.store.close()

    def call(self, method: str, path: str, body=None, actor: str | None = None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if actor:
            request.add_header("X-Actor", actor)
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_flow_over_http(self) -> None:
        status, node = self.call("POST", "/v1/nodes", {
            "name": "华东-A", "fault_domain": "room-a", "energy_level": "medium",
            "capabilities": ["rdma"],
        }, actor="ops")
        self.assertEqual(status, 201)
        status, batch = self.call("POST", "/v1/batches", {
            "node_code": node["node_code"], "resource_type": "gpu", "total_units": 4,
            "available_from": "2026-10-05T00:00:00Z", "available_until": "2026-10-10T00:00:00Z",
        })
        self.assertEqual(status, 201)

        status, quote = self.call("POST", "/v1/quotes", {
            "tenant_code": "tenant-api", "batch_code": batch["batch_code"], "units": 2,
            "start_at": BASE, "end_at": "2026-10-06T10:00:00Z",
        })
        self.assertEqual(status, 201)
        status, verification = self.call("POST", f"/v1/quotes/{quote['quote_code']}/verify")
        self.assertTrue(verification["match"])

        lock_body = {"idempotency_key": "api-lock-1", "quote_code": quote["quote_code"]}
        status, locked = self.call("POST", "/v1/reservations", lock_body, actor="tenant-api")
        self.assertEqual(status, 201)
        # 网络重试：同键同体重放返回原预留
        status, replay = self.call("POST", "/v1/reservations", lock_body, actor="tenant-api")
        self.assertEqual(replay["reservation_code"], locked["reservation_code"])
        self.assertTrue(replay["idempotent_replay"])
        # 同键不同体 → 409
        status, err = self.call("POST", "/v1/reservations", {
            "idempotency_key": "api-lock-1", "tenant_code": "tenant-api",
            "batch_code": batch["batch_code"], "units": 3, "end_at": "2026-10-06T10:00:00Z",
        })
        self.assertEqual(status, 409)
        self.assertEqual(err["error"]["code"], "idempotency_conflict")

        rsv = locked["reservation_code"]
        status, _ = self.call("POST", "/v1/events", [
            {"event_id": "api-e1", "reservation_code": rsv, "event_type": "usage",
             "occurred_at": "2026-10-06T01:00:00Z", "unit_hours": 2.0},
        ])
        self.assertEqual(status, 200)
        # 推进时钟 3 小时后再结算
        from datetime import timedelta

        type(self).clock_moment = self.clock_moment + timedelta(hours=3)
        status, settlement = self.call("POST", "/v1/settlements", {"tenant_code": "tenant-api"})
        self.assertEqual(status, 200)
        self.assertEqual(len(settlement["new_bills"]), 1)

        status, trail = self.call("GET", "/v1/tenants/tenant-api/trail")
        self.assertEqual(status, 200)
        self.assertEqual(len(trail["quotes"]), 1)
        self.assertEqual(len(trail["reservations"]), 1)
        self.assertEqual(len(trail["bills"]), 1)
        # 审计归属请求头操作者
        actors = {a["actor"] for a in trail["audit"]}
        self.assertIn("tenant-api", actors)

    def test_unknown_route_and_domain_error_shape(self) -> None:
        status, err = self.call("GET", "/v1/nope")
        self.assertEqual(status, 404)
        self.assertEqual(err["error"]["code"], "not_found")
        status, err = self.call("GET", "/v1/reservations/RSV-999999")
        self.assertEqual(status, 404)
        status, err = self.call("POST", "/v1/nodes", {"name": "x"})
        self.assertEqual(status, 400)
        self.assertEqual(err["error"]["code"], "bad_request")


if __name__ == "__main__":
    unittest.main()
