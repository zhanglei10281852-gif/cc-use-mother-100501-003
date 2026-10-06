"""领域状态常量与公共辅助函数。"""

from __future__ import annotations

import json
from hashlib import sha256
from uuid import uuid4


class BatchState:
    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    RETIRED = "RETIRED"


class QuoteState:
    OPEN = "OPEN"
    CONSUMED = "CONSUMED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"


class ReservationState:
    ACTIVE = "ACTIVE"
    MIGRATING = "MIGRATING"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"
    PREEMPTED = "PREEMPTED"


class GroupState:
    ADMITTED = "ADMITTED"
    RUNNING = "RUNNING"
    PARTIAL = "PARTIAL"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskState:
    WAITING = "WAITING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"


class SegmentCloseReason:
    MIGRATION = "MIGRATION"
    RENEWAL = "RENEWAL"
    RESIZE = "RESIZE"
    PREEMPT = "PREEMPT"
    RELEASE = "RELEASE"
    EXPIRE = "EXPIRE"


class MigrationState:
    PLANNED = "PLANNED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class MigrationReason:
    EXPIRY = "EXPIRY"
    DEGRADATION = "DEGRADATION"
    BATCH_RETIRED = "BATCH_RETIRED"
    REBALANCE = "REBALANCE"


class BillState:
    DRAFT = "DRAFT"
    FINALIZED = "FINALIZED"


class PreemptionState:
    ISSUED = "ISSUED"
    RESTORING = "RESTORING"
    CLOSED = "CLOSED"


def new_id(prefix: str) -> str:
    """生成带前缀的业务标识。"""
    return f"{prefix}_{uuid4().hex[:16]}"


def canonical_json(payload: object) -> str:
    """生成键序稳定的 JSON 文本，供摘要与存储使用。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(payload: object) -> str:
    """对任意可 JSON 序列化对象生成稳定摘要。"""
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()
