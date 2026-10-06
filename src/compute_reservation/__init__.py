"""跨节点算力预留与履约领域包。"""

from .backend import Backend
from .clock import Clock, ManualClock, SystemClock
from .contracts import ComputeReservation, unique_by_identity
from .errors import DomainError

__all__ = [
    "Backend",
    "Clock",
    "ComputeReservation",
    "DomainError",
    "ManualClock",
    "SystemClock",
    "unique_by_identity",
]
