"""任务组依赖调度与部分完成收缩测试。"""

import unittest

from helpers import allocated, all_segments, make_backend, publish, quote_and_reserve

from compute_reservation import errors


class TaskGroupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend, self.clock = make_backend()
        self.batch = publish(self.backend, self.clock, cards=8)
        self.reservation = quote_and_reserve(self.backend, self.clock, cards=4, idem_key="r-1")

    def tearDown(self) -> None:
        self.backend.close()

    def submit(self, tasks, **kw):
        return self.backend.tasks.submit_group(
            tenant_id="tenant-1", reservation_id=self.reservation["reservation_id"], tasks=tasks, **kw)

    def test_dependency_cycle_rejected(self) -> None:
        with self.assertRaises(errors.DomainError) as ctx:
            self.submit([{"name": "a", "cards": 1, "depends_on": ["b"]},
                         {"name": "b", "cards": 1, "depends_on": ["a"]}])
        self.assertEqual(ctx.exception.code, "validation")

    def test_unknown_dependency_rejected(self) -> None:
        with self.assertRaises(errors.DomainError):
            self.submit([{"name": "a", "cards": 1, "depends_on": ["ghost"]}])

    def test_task_exceeding_quota_rejected(self) -> None:
        with self.assertRaises(errors.DomainError):
            self.submit([{"name": "big", "cards": 5, "depends_on": []}])

    def test_dependency_gating_and_promotion(self) -> None:
        group = self.submit([
            {"name": "prepare", "cards": 2, "depends_on": []},
            {"name": "train", "cards": 4, "depends_on": ["prepare"]},
        ])
        by_name = {t["name"]: t for t in group["tasks"]}
        self.assertEqual(by_name["prepare"]["state"], "RUNNING")
        self.assertEqual(by_name["train"]["state"], "WAITING")

        done = self.backend.tasks.complete_task(tenant_id="tenant-1", task_id=by_name["prepare"]["task_id"])
        self.assertEqual(done["state"], "COMPLETED")
        view = self.backend.tasks.get_group("tenant-1", group["group_id"])
        train = next(t for t in view["tasks"] if t["name"] == "train")
        self.assertEqual(train["state"], "RUNNING")
        self.assertEqual(view["state"], "PARTIAL")

        self.backend.tasks.complete_task(tenant_id="tenant-1", task_id=train["task_id"])
        view = self.backend.tasks.get_group("tenant-1", group["group_id"])
        self.assertEqual(view["state"], "COMPLETED")

    def test_capacity_gates_parallel_start(self) -> None:
        group = self.submit([
            {"name": "a", "cards": 3, "depends_on": []},
            {"name": "b", "cards": 3, "depends_on": []},
        ])
        states = {t["name"]: t["state"] for t in group["tasks"]}
        self.assertEqual(states, {"a": "RUNNING", "b": "READY"})  # 3+3 超出 4 卡配额
        first = next(t for t in group["tasks"] if t["name"] == "a")
        self.backend.tasks.complete_task(tenant_id="tenant-1", task_id=first["task_id"])
        view = self.backend.tasks.get_group("tenant-1", group["group_id"])
        b = next(t for t in view["tasks"] if t["name"] == "b")
        self.assertEqual(b["state"], "RUNNING")

    def test_partial_completion_shrinks_reservation(self) -> None:
        group = self.submit([
            {"name": "a", "cards": 2, "depends_on": []},
            {"name": "b", "cards": 2, "depends_on": []},
        ])
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 4)
        task_a = next(t for t in group["tasks"] if t["name"] == "a")
        self.backend.tasks.complete_task(tenant_id="tenant-1", task_id=task_a["task_id"])

        updated = self.backend.reservations.get("tenant-1", self.reservation["reservation_id"])
        self.assertEqual(updated["cards"], 2)
        self.assertEqual(allocated(self.backend, self.batch["batch_id"]), 2)
        segments = all_segments(self.backend, self.reservation["reservation_id"])
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["close_reason"], "RESIZE")
        self.assertEqual(segments[1]["cards"], 2)
        self.assertIsNone(segments[1]["end_at"])

    def test_group_submission_is_idempotent(self) -> None:
        tasks = [{"name": "a", "cards": 1, "depends_on": []}]
        first = self.submit(tasks, idem_key="g-1")
        second = self.submit(tasks, idem_key="g-1")
        self.assertEqual(first["group_id"], second["group_id"])
        self.assertTrue(second["replayed"])
        with self.assertRaises(errors.DomainError) as ctx:
            self.submit([{"name": "a", "cards": 2, "depends_on": []}], idem_key="g-1")
        self.assertEqual(ctx.exception.code, "idempotency_conflict")


if __name__ == "__main__":
    unittest.main()
