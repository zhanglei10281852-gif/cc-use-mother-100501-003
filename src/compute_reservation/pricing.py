"""确定性定价规则。

报价可复核的关键在于：价格完全由报价输入（批次快照、卡数、时长、
能耗等级、服务能力匹配）决定，任何一方都能用同样输入重算出同样结果。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from . import errors

MONEY_QUANT = Decimal("0.000001")
CURRENCY = "CNY"

# 能耗等级价格因子：等级越低（越省电）价格越优惠。
ENERGY_FACTORS = {
    "P1": Decimal("0.90"),
    "P2": Decimal("1.00"),
    "P3": Decimal("1.25"),
}

# 每匹配一项请求的服务能力，在基准价上附加的比例。
CAPABILITY_PREMIUM_RATE = Decimal("0.05")

# 抢占补偿倍率：按被抢占预留剩余时长价值的 1.5 倍赔付。
COMPENSATION_RATE = Decimal("1.5")


def quantize(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def to_decimal(value: object) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True)
class PriceBreakdown:
    """一次报价的完整计价明细。"""

    unit_price: Decimal
    energy_factor: Decimal
    capability_factor: Decimal
    effective_unit_price: Decimal
    cards: int
    hours: Decimal
    matched_capabilities: tuple[str, ...]
    total: Decimal
    currency: str = CURRENCY

    def to_dict(self) -> dict:
        return {
            "unit_price": str(self.unit_price),
            "energy_factor": str(self.energy_factor),
            "capability_factor": str(self.capability_factor),
            "effective_unit_price": str(self.effective_unit_price),
            "cards": self.cards,
            "hours": str(self.hours),
            "matched_capabilities": list(self.matched_capabilities),
            "total": str(self.total),
            "currency": self.currency,
        }


def price_quote(
    *,
    unit_price: object,
    energy_tier: str,
    batch_capabilities: list[str],
    requested_capabilities: list[str],
    cards: int,
    duration_seconds: float,
) -> PriceBreakdown:
    """按固定规则计算报价明细，同样的输入永远得到同样的输出。"""
    if energy_tier not in ENERGY_FACTORS:
        raise errors.validation(f"未知能耗等级 {energy_tier}")
    if cards < 1:
        raise errors.validation("卡数必须大于零")
    if duration_seconds <= 0:
        raise errors.validation("时长必须大于零")

    base = to_decimal(unit_price)
    energy_factor = ENERGY_FACTORS[energy_tier]
    matched = tuple(sorted(set(batch_capabilities) & set(requested_capabilities)))
    capability_factor = Decimal(1) + CAPABILITY_PREMIUM_RATE * len(matched)
    effective = quantize(base * energy_factor * capability_factor)
    hours = to_decimal(duration_seconds) / Decimal(3600)
    total = quantize(effective * cards * hours)
    return PriceBreakdown(
        unit_price=quantize(base),
        energy_factor=energy_factor,
        capability_factor=capability_factor,
        effective_unit_price=effective,
        cards=cards,
        hours=quantize(hours),
        matched_capabilities=matched,
        total=total,
    )


def occupancy_amount(*, cards: int, seconds: float, effective_unit_price: object) -> Decimal:
    """占用计费：卡数 × 时长 × 单价。"""
    hours = to_decimal(seconds) / Decimal(3600)
    return quantize(to_decimal(effective_unit_price) * cards * hours)


def compensation_amount(*, cards: int, remaining_seconds: float, effective_unit_price: object) -> Decimal:
    """抢占补偿：剩余时长价值 × 补偿倍率。"""
    base = occupancy_amount(cards=cards, seconds=remaining_seconds, effective_unit_price=effective_unit_price)
    return quantize(base * COMPENSATION_RATE)
