"""可注入时钟，便于确定性测试与恢复重放。"""

from __future__ import annotations

import time


class Clock:
    """时钟接口，返回 Unix  epoch 秒。"""

    def now(self) -> float:
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> float:
        return time.time()


class ManualClock(Clock):
    """测试用手动时钟。"""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def set(self, value: float) -> None:
        self._now = float(value)

    def advance(self, seconds: float) -> float:
        self._now += float(seconds)
        return self._now
