"""Backend 门面：把各子服务装配成一个整体，供 API 与 CLI 共用。"""

from __future__ import annotations

from pathlib import Path

from .billing import BillingService, TrailService
from .clock import Clock
from .lifecycle import LifecycleEngine
from .recovery import recover as _recover
from .services import CatalogService, QuoteService, ReservationService, TaskService
from .store import Store


class Backend:
    """跨节点算力预留与履约后端。"""

    def __init__(self, db_path: str | Path = ":memory:", clock: Clock | None = None) -> None:
        self.store = Store(db_path, clock=clock)
        self.clock = self.store.clock
        self.catalog = CatalogService(self.store)
        self.quotes = QuoteService(self.store)
        self.reservations = ReservationService(self.store)
        self.tasks = TaskService(self.store)
        self.lifecycle = LifecycleEngine(self.store)
        self.billing = BillingService(self.store)
        self.trail_service = TrailService(self.store)

    def recover(self) -> dict:
        """启动恢复：续跑未决迁移 + 到期清扫。"""
        return _recover(self)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> "Backend":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
