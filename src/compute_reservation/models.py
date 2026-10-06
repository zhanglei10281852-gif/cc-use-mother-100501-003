"""跨节点算力预留与履约的领域常量、时间工具与错误类型。"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any


# ---------------------------------------------------------------------------
# 错误
# ---------------------------------------------------------------------------


class DomainError(Exception):
    """业务规则冲突，携带稳定错误码，API/CLI 统一映射。"""

    def __init__(self, code: str, message: str, http_status: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {"error": {"code": self.code, "message": self.message}}


def not_found(resource: str, code: str) -> DomainError:
    return DomainError("not_found", f"{resource} {code} 不存在", 404)


def conflict(code: str, message: str) -> DomainError:
    return DomainError(code, message, 409)


def bad_request(message: str) -> DomainError:
    return DomainError("bad_request", message, 400)


# ---------------------------------------------------------------------------
# 状态机常量
# ---------------------------------------------------------------------------

NODE_STATES = ("ACTIVE", "DEGRADED", "OFFLINE")
BATCH_STATES = ("OPEN", "CLOSED", "EXPIRED")
QUOTE_STATES = ("OPEN", "CONSUMED", "EXPIRED", "CANCELLED")
RESERVATION_STATES = ("ACTIVE", "MIGRATING", "RELEASED", "EXPIRED", "PREEMPTED")
TERMINAL_RESERVATION_STATES = ("RELEASED", "EXPIRED")
TASK_STATES = ("PENDING", "READY", "RUNNING", "SUCCEEDED", "FAILED", "BLOCKED")
TERMINAL_TASK_STATES = ("SUCCEEDED", "FAILED", "BLOCKED")
MIGRATION_STATES = ("PLANNED", "COMMITTED", "ABORTED")
BILLING_MODES = ("reserved", "on_demand")
ENERGY_LEVELS = ("low", "medium", "high")
RESOURCE_TYPES = ("gpu", "npu", "cpu", "memory")

# 合同规则可选值
ON_EXPIRY_POLICIES = ("renew", "migrate", "release")
ON_DEGRADE_POLICIES = ("migrate", "keep")
ON_PARTIAL_POLICIES = ("shrink", "keep")
ON_COMPLETE_POLICIES = ("release", "keep")
ON_PREEMPT_POLICIES = ("migrate", "release")

# 补偿规则：贷记比例（相对受扰动时段的应收费用）
COMPENSATION_DEFAULTS = {
    "capacity_loss": 0.5,      # 节点离线/被抢占导致容量损失
    "forced_migration": 0.2,   # 节点降级被迫迁移
    "forced_expiry": 0.1,      # 合同要求续约/迁移但容量不足被强制到期
}

DEFAULT_CONTRACT: dict[str, Any] = {
    "on_expiry": "release",
    "renew_extension_hours": 12,
    "on_degrade": "migrate",
    "on_partial": "keep",
    "on_complete": "release",
    "on_preempt": "migrate",
    "interruptible": True,
    "priority": 100,
    "required_capabilities": [],
    "compensation": dict(COMPENSATION_DEFAULTS),
}

# 默认费率卡：元 / (单元 × 小时)，按 (资源类型, 能耗等级, 计费模式)
DEFAULT_RATES: dict[str, dict[str, float]] = {
    "gpu": {"low": 14.0, "medium": 12.0, "high": 10.0},
    "npu": {"low": 11.0, "medium": 9.5, "high": 8.0},
    "cpu": {"low": 1.8, "medium": 1.5, "high": 1.2},
    "memory": {"low": 0.5, "medium": 0.4, "high": 0.32},
}
ON_DEMAND_MARKUP = 1.3
RATE_EFFECTIVE_FROM = "2025-01-01T00:00:00.000Z"

# 补偿基准的受扰动时长上限（小时）
COMPENSATION_HORIZON_HOURS = 24.0


# ---------------------------------------------------------------------------
# 时间工具：统一为 UTC ISO 字符串，字典序即时间序
# ---------------------------------------------------------------------------

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def parse_time(value: str) -> datetime:
    """解析 ISO-8601（允许 Z 或时区偏移），归一化为 UTC。"""
    if not isinstance(value, str) or not value.strip():
        raise bad_request(f"时间格式非法: {value!r}")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise bad_request(f"时间格式非法: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(ISO_FORMAT)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return format_time(now_utc())


def hours_between(start_iso: str, end_iso: str) -> float:
    delta = parse_time(end_iso) - parse_time(start_iso)
    return delta.total_seconds() / 3600.0


def add_hours(iso: str, hours: float) -> str:
    from datetime import timedelta

    return format_time(parse_time(iso) + timedelta(hours=hours))


# ---------------------------------------------------------------------------
# 稳定摘要与金额
# ---------------------------------------------------------------------------


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(payload: Any) -> str:
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def to_cents(amount_yuan: float) -> int:
    """金额以分（整数）存储，避免浮点误差进入账本。"""
    return int(round(amount_yuan * 100))


def yuan(cents: int) -> float:
    return round(cents / 100.0, 2)
