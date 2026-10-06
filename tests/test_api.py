"""HTTP API 端到端测试。"""

import http.client
import json
import threading
import unittest

from helpers import T0, make_backend

from compute_reservation.api import create_server


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.backend, cls.clock = make_backend()
        cls.server = create_server(cls.backend, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.backend.close()

    def call(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        all_headers = {"Content-Type": "application/json"} if body is not None else {}
        all_headers.update(headers or {})
        conn.request(method, path, body=payload, headers=all_headers)
        response = conn.getresponse()
        data = json.loads(response.read().decode("utf-8"))
        conn.close()
        return response.status, data

    def test_full_fulfillment_flow_over_http(self) -> None:
        now = self.clock.now()
        status, batch = self.call("POST", "/v1/nodes/node-a/batches", {
            "total_cards": 8, "valid_from": now, "valid_until": now + 86400,
            "fault_domain": "fd-1", "energy_tier": "P2", "capabilities": ["training"],
            "price_per_card_hour": 10.0, "inventory_ref": "inv-api-1"})
        self.assertEqual(status, 201)

        status, quote = self.call("POST", "/v1/quotes", {
            "tenant_id": "tenant-1", "cards": 2, "start_at": now,
            "duration_seconds": 3600, "capabilities": ["training"]})
        self.assertEqual(status, 201)
        self.assertEqual(quote["state"], "OPEN")

        status, verification = self.call("GET", f"/v1/quotes/{quote['quote_id']}/verify")
        self.assertEqual(status, 200)
        self.assertTrue(verification["fingerprint_matches"])

        headers = {"X-Tenant-Id": "tenant-1", "Idempotency-Key": "api-reserve-1"}
        status, reservation = self.call("POST", "/v1/reservations",
                                        {"quote_id": quote["quote_id"]}, headers)
        self.assertEqual(status, 201)
        rid = reservation["reservation_id"]
        # 幂等重放返回同一预留。
        status, replay = self.call("POST", "/v1/reservations",
                                   {"quote_id": quote["quote_id"]}, headers)
        self.assertEqual(status, 201)
        self.assertEqual(replay["reservation_id"], rid)
        # 同键不同载荷被拒绝。
        status, conflict = self.call("POST", "/v1/reservations",
                                     {"quote_id": quote["quote_id"], "tenant_priority": 1}, headers)
        self.assertEqual(status, 409)
        self.assertEqual(conflict["error"]["code"], "idempotency_conflict")

        status, group = self.call("POST", "/v1/task-groups", {
            "tenant_id": "tenant-1", "reservation_id": rid,
            "tasks": [{"name": "job", "cards": 2, "depends_on": []}]})
        self.assertEqual(status, 201)
        task_id = group["tasks"][0]["task_id"]
        status, _ = self.call("POST", f"/v1/tasks/{task_id}/complete",
                              {"tenant_id": "tenant-1"})
        self.assertEqual(status, 200)

        status, ingest = self.call("POST", "/v1/usage-events", {"events": [{
            "event_id": "api-e1", "reservation_id": rid, "node_id": "node-a",
            "cards": 2, "usage_start": now, "usage_end": now + 1800}]})
        self.assertEqual(status, 200)
        self.assertEqual(ingest["accepted"], 1)

        self.clock.advance(3601)
        status, bill = self.call("POST", "/v1/bills", {
            "tenant_id": "tenant-1", "reservation_id": rid,
            "period_start": now, "period_end": now + 3600, "finalize": True})
        self.assertEqual(status, 201)
        self.assertEqual(bill["state"], "FINALIZED")
        self.assertEqual(bill["total"], 21.0)  # 2卡 × 1h × 10 × 1.05（training 能力溢价）

        status, trail = self.call("GET", "/v1/tenants/tenant-1/trail",
                                  headers={"X-Tenant-Id": "tenant-1"})
        self.assertEqual(status, 200)
        self.assertEqual(len(trail["reservations"]), 1)
        self.assertEqual(len(trail["bills"]), 1)
        self.assertGreater(len(trail["timeline"]), 5)

    def test_tenant_isolation(self) -> None:
        now = self.clock.now()
        status, quote = self.call("POST", "/v1/quotes", {
            "tenant_id": "tenant-iso", "cards": 1, "start_at": now, "duration_seconds": 600})
        self.assertEqual(status, 201)
        status, reservation = self.call("POST", "/v1/reservations",
                                        {"quote_id": quote["quote_id"]},
                                        {"X-Tenant-Id": "tenant-iso", "Idempotency-Key": "iso-1"})
        self.assertEqual(status, 201)
        status, denied = self.call("GET", f"/v1/reservations/{reservation['reservation_id']}",
                                   headers={"X-Tenant-Id": "tenant-other"})
        self.assertEqual(status, 403)
        status, denied_trail = self.call("GET", "/v1/tenants/tenant-iso/trail",
                                         headers={"X-Tenant-Id": "tenant-other"})
        self.assertEqual(status, 403)

    def test_missing_idempotency_key_rejected(self) -> None:
        status, body = self.call("POST", "/v1/reservations",
                                 {"tenant_id": "t", "quote_id": "quo_x"})
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "validation")

    def test_unknown_route_404(self) -> None:
        status, body = self.call("GET", "/v1/nope")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
