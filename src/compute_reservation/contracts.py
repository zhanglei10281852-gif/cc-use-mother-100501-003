"""跨节点算力预留与履约的基础领域契约。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from hashlib import sha256
import json
from typing import Iterable


@dataclass(frozen=True, slots=True)
class ComputeReservation:
    """保存最小且可校验的业务对象。"""

    reservation_code: str
    tenant_code: str
    capacity_batch: str
    state: str

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if isinstance(value, str) and not value.strip():
                raise ValueError(f"{key} 不能为空")
            if isinstance(value, int) and value < 1:
                raise ValueError(f"{key} 必须大于零")

    def evolve(self, **changes: object) -> "ComputeReservation":
        """返回新版本，避免就地改写历史对象。"""
        return replace(self, **changes)

    def fingerprint(self) -> str:
        """生成稳定摘要，供幂等和审计使用。"""
        payload = json.dumps(asdict(self), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return sha256(payload.encode("utf-8")).hexdigest()


def unique_by_identity(items: Iterable[ComputeReservation]) -> list[ComputeReservation]:
    """按业务标识去重，并拒绝同标识不同内容。"""
    found: dict[str, ComputeReservation] = {}
    for item in items:
        key = str(getattr(item, "reservation_code"))
        previous = found.get(key)
        if previous is not None and previous.fingerprint() != item.fingerprint():
            raise ValueError(f"业务标识 {key} 对应的内容发生冲突")
        found[key] = item
    return [found[key] for key in sorted(found)]
