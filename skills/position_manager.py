"""Risk-first position sizing and exit-plan calculations.

All percentage inputs use percentage points: ``0.5`` means 0.5%, not 50%.
The module deliberately does not fetch market data or choose a security. It
turns caller-supplied prices and risk limits into an auditable trade plan.
"""

from __future__ import annotations

import math
from typing import Any


RISK_PRESETS = {
    "conservative": {
        "risk_per_trade_pct": 0.5,
        "max_portfolio_risk_pct": 2.0,
        "max_position_pct": 10.0,
    },
    "balanced": {
        "risk_per_trade_pct": 0.75,
        "max_portfolio_risk_pct": 3.0,
        "max_position_pct": 15.0,
    },
    "aggressive": {
        "risk_per_trade_pct": 1.0,
        "max_portfolio_risk_pct": 4.0,
        "max_position_pct": 20.0,
    },
}

DEFAULT_TARGET_R_MULTIPLES = [1.0, 2.0, 3.0]
DEFAULT_TARGET_ALLOCATIONS_PCT = [30.0, 40.0, 30.0]
DEFAULT_GRID_ALLOCATIONS_PCT = [40.0, 30.0, 30.0]


def _number(value: Any, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    if minimum is not None and result < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return result


def _positive(value: Any, name: str) -> float:
    result = _number(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be > 0")
    return result


def _round_down_to_lot(quantity: float, lot_size: float) -> float:
    lots = math.floor((quantity / lot_size) + 1e-12)
    result = lots * lot_size
    decimals = max(0, min(12, -math.floor(math.log10(lot_size)))) if lot_size < 1 else 8
    return round(result, decimals)


def _money(value: float) -> float:
    return round(value, 2)


def _price(value: float) -> float:
    return round(value, 6)


def _resolve_stop(
    *,
    entry_price: float,
    side: str,
    stop_method: str,
    stop_price: Any,
    stop_percent: Any,
    atr: Any,
    atr_multiplier: Any,
) -> tuple[float, dict[str, Any]]:
    direction = 1 if side == "long" else -1

    if stop_method == "price":
        if stop_price is None:
            raise ValueError("stop_price is required when stop_method is 'price'")
        resolved = _positive(stop_price, "stop_price")
        basis = {"method": "price", "input_stop_price": resolved}
    elif stop_method == "percent":
        pct = _positive(8.0 if stop_percent is None else stop_percent, "stop_percent")
        if pct >= 100:
            raise ValueError("stop_percent must be < 100")
        resolved = entry_price * (1 - direction * pct / 100)
        basis = {"method": "percent", "stop_percent": pct}
    elif stop_method == "atr":
        if atr is None:
            raise ValueError("atr is required when stop_method is 'atr'")
        atr_value = _positive(atr, "atr")
        multiplier = _positive(
            2.0 if atr_multiplier is None else atr_multiplier,
            "atr_multiplier",
        )
        resolved = entry_price - direction * atr_value * multiplier
        basis = {
            "method": "atr",
            "atr": atr_value,
            "atr_multiplier": multiplier,
        }
    else:
        raise ValueError("stop_method must be one of: price, percent, atr")

    if resolved <= 0:
        raise ValueError("calculated stop price must be > 0")
    if side == "long" and resolved >= entry_price:
        raise ValueError("long stop price must be below entry_price")
    if side == "short" and resolved <= entry_price:
        raise ValueError("short stop price must be above entry_price")
    return resolved, basis


def _resolve_targets(
    target_r_multiples: Any,
    target_allocations_pct: Any,
) -> tuple[list[float], list[float]]:
    multiples = (
        DEFAULT_TARGET_R_MULTIPLES
        if target_r_multiples is None
        else target_r_multiples
    )
    allocations = (
        DEFAULT_TARGET_ALLOCATIONS_PCT
        if target_allocations_pct is None
        else target_allocations_pct
    )
    if not isinstance(multiples, list) or not multiples:
        raise ValueError("target_r_multiples must be a non-empty array")
    if not isinstance(allocations, list) or not allocations:
        raise ValueError("target_allocations_pct must be a non-empty array")
    if len(multiples) != len(allocations):
        raise ValueError(
            "target_r_multiples and target_allocations_pct must have equal length"
        )
    clean_multiples = [
        _positive(value, f"target_r_multiples[{index}]")
        for index, value in enumerate(multiples)
    ]
    clean_allocations = [
        _positive(value, f"target_allocations_pct[{index}]")
        for index, value in enumerate(allocations)
    ]
    if not math.isclose(sum(clean_allocations), 100.0, abs_tol=1e-6):
        raise ValueError("target_allocations_pct must sum to 100")
    return clean_multiples, clean_allocations


def _resolve_grid_allocations(
    values: Any,
    grid_levels: int,
    name: str,
) -> list[float]:
    if values is None:
        allocations = (
            DEFAULT_GRID_ALLOCATIONS_PCT
            if grid_levels == 3
            else [100.0 / grid_levels] * grid_levels
        )
    else:
        allocations = values
    if not isinstance(allocations, list) or len(allocations) != grid_levels:
        raise ValueError(f"{name} must contain exactly grid_levels items")
    clean = [
        _number(value, f"{name}[{index}]", minimum=0)
        for index, value in enumerate(allocations)
    ]
    if not math.isclose(sum(clean), 100.0, abs_tol=1e-6):
        raise ValueError(f"{name} must sum to 100")
    return clean


def _allocate_lots(
    total_quantity: float,
    allocations: list[float],
    lot_size: float,
) -> list[float]:
    total_lots = math.floor((total_quantity / lot_size) + 1e-12)
    raw_lots = [
        total_lots * allocation / 100
        for allocation in allocations
    ]
    allocated_lots = [math.floor(value) for value in raw_lots]
    remaining_lots = total_lots - sum(allocated_lots)
    priority = sorted(
        range(len(allocations)),
        key=lambda index: (
            raw_lots[index] - allocated_lots[index],
            allocations[index],
            -index,
        ),
        reverse=True,
    )
    for index in priority[:remaining_lots]:
        allocated_lots[index] += 1
    return [
        _round_down_to_lot(lots * lot_size, lot_size)
        for lots in allocated_lots
    ]


def calculate_position_plan(
    *,
    account_equity: Any,
    entry_price: Any,
    side: str = "long",
    risk_profile: str = "balanced",
    stop_method: str = "percent",
    stop_price: Any = None,
    stop_percent: Any = None,
    atr: Any = None,
    atr_multiplier: Any = None,
    risk_per_trade_pct: Any = None,
    max_portfolio_risk_pct: Any = None,
    current_open_risk: Any = 0,
    max_position_pct: Any = None,
    capital_available: Any = None,
    lot_size: Any = 1,
    round_trip_cost_bps: Any = 20,
    target_r_multiples: Any = None,
    target_allocations_pct: Any = None,
) -> dict[str, Any]:
    """Calculate a position size, stop, and staged profit-taking plan."""

    if side not in {"long", "short"}:
        raise ValueError("side must be 'long' or 'short'")
    if risk_profile not in RISK_PRESETS:
        raise ValueError(
            "risk_profile must be one of: conservative, balanced, aggressive"
        )

    equity = _positive(account_equity, "account_equity")
    entry = _positive(entry_price, "entry_price")
    available = (
        equity
        if capital_available is None
        else _number(capital_available, "capital_available", minimum=0)
    )
    open_risk = _number(current_open_risk, "current_open_risk", minimum=0)
    lot = _positive(lot_size, "lot_size")
    costs_bps = _number(round_trip_cost_bps, "round_trip_cost_bps", minimum=0)
    if costs_bps >= 10_000:
        raise ValueError("round_trip_cost_bps must be < 10000")

    preset = RISK_PRESETS[risk_profile]
    trade_risk_pct = _positive(
        preset["risk_per_trade_pct"]
        if risk_per_trade_pct is None
        else risk_per_trade_pct,
        "risk_per_trade_pct",
    )
    portfolio_risk_pct = _positive(
        preset["max_portfolio_risk_pct"]
        if max_portfolio_risk_pct is None
        else max_portfolio_risk_pct,
        "max_portfolio_risk_pct",
    )
    position_pct = _positive(
        preset["max_position_pct"]
        if max_position_pct is None
        else max_position_pct,
        "max_position_pct",
    )
    if trade_risk_pct > portfolio_risk_pct:
        raise ValueError(
            "risk_per_trade_pct cannot exceed max_portfolio_risk_pct"
        )
    if position_pct > 100:
        raise ValueError("max_position_pct must be <= 100")

    resolved_stop, stop_basis = _resolve_stop(
        entry_price=entry,
        side=side,
        stop_method=stop_method,
        stop_price=stop_price,
        stop_percent=stop_percent,
        atr=atr,
        atr_multiplier=atr_multiplier,
    )
    multiples, allocations = _resolve_targets(
        target_r_multiples,
        target_allocations_pct,
    )

    price_risk_per_unit = abs(entry - resolved_stop)
    estimated_cost_per_unit = entry * costs_bps / 10_000
    effective_risk_per_unit = price_risk_per_unit + estimated_cost_per_unit
    max_total_open_risk = equity * portfolio_risk_pct / 100
    remaining_portfolio_risk = max(0.0, max_total_open_risk - open_risk)
    requested_trade_risk = equity * trade_risk_pct / 100
    usable_risk_budget = min(requested_trade_risk, remaining_portfolio_risk)

    max_position_value = min(available, equity * position_pct / 100)
    quantity_by_risk = _round_down_to_lot(
        usable_risk_budget / effective_risk_per_unit,
        lot,
    )
    quantity_by_capital = _round_down_to_lot(max_position_value / entry, lot)
    quantity = min(quantity_by_risk, quantity_by_capital)

    position_value = quantity * entry
    planned_risk = quantity * effective_risk_per_unit
    direction = 1 if side == "long" else -1

    target_quantities: list[float] = []
    allocated = 0.0
    for index, allocation in enumerate(allocations):
        if index == len(allocations) - 1:
            target_quantity = round(quantity - allocated, 12)
        else:
            target_quantity = _round_down_to_lot(
                quantity * allocation / 100,
                lot,
            )
            allocated += target_quantity
        target_quantities.append(target_quantity)

    targets = []
    for multiple, allocation, target_quantity in zip(
        multiples,
        allocations,
        target_quantities,
    ):
        target_price = entry + direction * price_risk_per_unit * multiple
        if target_price <= 0:
            raise ValueError("a calculated target price is not positive")
        targets.append(
            {
                "r_multiple": multiple,
                "price": _price(target_price),
                "allocation_pct": allocation,
                "quantity": target_quantity,
            }
        )

    constraints = []
    if quantity == quantity_by_risk:
        constraints.append("risk_budget")
    if quantity == quantity_by_capital:
        constraints.append("position_or_capital_cap")
    if remaining_portfolio_risk < requested_trade_risk:
        constraints.append("remaining_portfolio_risk")

    warnings = [
        (
            "止损触发价不保证成交价；跳空、停牌或剧烈波动可能使实际亏损"
            "高于计划值。"
        ),
        "止损位应对应交易逻辑失效点；百分比或 ATR 结果仅是风险边界参考。",
    ]
    stop_distance_pct = price_risk_per_unit / entry * 100
    blended_reward_r = sum(
        multiple * allocation / 100
        for multiple, allocation in zip(multiples, allocations)
    )
    net_blended_reward_r = (
        price_risk_per_unit * blended_reward_r - estimated_cost_per_unit
    ) / effective_risk_per_unit
    if stop_distance_pct < 1:
        warnings.append("止损距离小于入场价的 1%，可能容易被正常波动触发。")
    if trade_risk_pct > 1:
        warnings.append("单笔风险高于账户权益的 1%，连续亏损时回撤会放大。")
    if blended_reward_r < 1.5:
        warnings.append("分批止盈后的加权回报低于 1.5R，请复核盈亏比。")
    if quantity == 0:
        warnings.append("在当前限制下无法建立至少一个交易单位的仓位。")

    return {
        "status": "ok",
        "policy": {
            "risk_profile": risk_profile,
            "risk_per_trade_pct": trade_risk_pct,
            "max_portfolio_risk_pct": portfolio_risk_pct,
            "max_position_pct": position_pct,
            "round_trip_cost_bps": costs_bps,
        },
        "stop_plan": {
            **stop_basis,
            "side": side,
            "entry_price": _price(entry),
            "stop_price": _price(resolved_stop),
            "stop_distance": _price(price_risk_per_unit),
            "stop_distance_pct": round(stop_distance_pct, 4),
            "estimated_cost_per_unit": _price(estimated_cost_per_unit),
            "effective_risk_per_unit": _price(effective_risk_per_unit),
        },
        "position_plan": {
            "quantity": quantity,
            "lot_size": lot,
            "position_value": _money(position_value),
            "position_pct_of_equity": round(position_value / equity * 100, 4),
            "requested_trade_risk": _money(requested_trade_risk),
            "usable_risk_budget": _money(usable_risk_budget),
            "planned_risk_at_stop": _money(planned_risk),
            "planned_risk_pct_of_equity": round(planned_risk / equity * 100, 4),
            "quantity_by_risk": quantity_by_risk,
            "quantity_by_capital": quantity_by_capital,
            "binding_constraints": constraints,
        },
        "portfolio_risk": {
            "current_open_risk": _money(open_risk),
            "max_total_open_risk": _money(max_total_open_risk),
            "remaining_before_trade": _money(remaining_portfolio_risk),
            "remaining_after_trade": _money(
                max(0.0, remaining_portfolio_risk - planned_risk)
            ),
        },
        "take_profit_plan": {
            "targets": targets,
            "blended_reward_r": round(blended_reward_r, 4),
            "estimated_net_blended_reward_r": round(net_blended_reward_r, 4),
            "management_rule": (
                "首个止盈成交后，可将剩余仓位止损上移至成本附近；"
                "是否执行应结合跳空风险和标的波动。"
            ),
        },
        "warnings": warnings,
        "disclaimer": "仅用于风险计划与计算，不构成投资建议或收益保证。",
    }


def calculate_core_tactical_plan(
    *,
    account_equity: Any,
    reference_price: Any,
    symbol: str = "",
    max_symbol_pct: Any = 8,
    core_pct_of_symbol: Any = 60,
    initial_tactical_deployment_pct: Any = 50,
    grid_step_pct: Any = 5,
    grid_levels: Any = 3,
    lot_size: Any = 1,
    tactical_stop_pct: Any = 20,
    sell_allocations_pct: Any = None,
    buy_allocations_pct: Any = None,
    current_quantity: Any = None,
) -> dict[str, Any]:
    """Build a core-holding plus tactical high-sell/low-buy allocation plan."""

    equity = _positive(account_equity, "account_equity")
    reference = _positive(reference_price, "reference_price")
    symbol_cap_pct = _positive(max_symbol_pct, "max_symbol_pct")
    if symbol_cap_pct > 100:
        raise ValueError("max_symbol_pct must be <= 100")
    core_ratio = _positive(core_pct_of_symbol, "core_pct_of_symbol")
    if core_ratio >= 100:
        raise ValueError("core_pct_of_symbol must be < 100")
    deployment_pct = _number(
        initial_tactical_deployment_pct,
        "initial_tactical_deployment_pct",
        minimum=0,
    )
    if deployment_pct > 100:
        raise ValueError("initial_tactical_deployment_pct must be <= 100")
    step_pct = _positive(grid_step_pct, "grid_step_pct")
    if step_pct >= 100:
        raise ValueError("grid_step_pct must be < 100")
    if isinstance(grid_levels, bool) or not isinstance(grid_levels, int):
        raise ValueError("grid_levels must be an integer")
    if grid_levels < 1 or grid_levels > 10:
        raise ValueError("grid_levels must be between 1 and 10")
    if step_pct * grid_levels >= 100:
        raise ValueError("grid_step_pct × grid_levels must be < 100")
    lot = _positive(lot_size, "lot_size")
    stop_pct = _positive(tactical_stop_pct, "tactical_stop_pct")
    if stop_pct >= 100:
        raise ValueError("tactical_stop_pct must be < 100")

    sell_allocations = _resolve_grid_allocations(
        sell_allocations_pct,
        grid_levels,
        "sell_allocations_pct",
    )
    buy_allocations = _resolve_grid_allocations(
        buy_allocations_pct,
        grid_levels,
        "buy_allocations_pct",
    )

    max_symbol_value = equity * symbol_cap_pct / 100
    core_budget = max_symbol_value * core_ratio / 100
    tactical_budget = max_symbol_value - core_budget
    core_quantity = _round_down_to_lot(core_budget / reference, lot)
    tactical_max_quantity = _round_down_to_lot(tactical_budget / reference, lot)
    tactical_initial_quantity = _round_down_to_lot(
        tactical_max_quantity * deployment_pct / 100,
        lot,
    )
    tactical_buy_capacity = round(
        tactical_max_quantity - tactical_initial_quantity,
        12,
    )
    max_total_quantity = core_quantity + tactical_max_quantity
    initial_target_quantity = core_quantity + tactical_initial_quantity

    active_tactical_quantity = tactical_initial_quantity
    active_buy_capacity = tactical_buy_capacity
    current_position = None
    if current_quantity is not None:
        current = _number(current_quantity, "current_quantity", minimum=0)
        protected_core = min(current, core_quantity)
        tactical = min(
            max(0.0, current - protected_core),
            tactical_max_quantity,
        )
        above_cap = max(0.0, current - max_total_quantity)
        active_tactical_quantity = tactical
        active_buy_capacity = round(
            tactical_max_quantity - tactical,
            12,
        )
        current_position = {
            "current_quantity": current,
            "classified_core_quantity": protected_core,
            "classified_tactical_quantity": tactical,
            "quantity_above_plan_cap": above_cap,
            "difference_from_initial_target": round(
                current - initial_target_quantity,
                12,
            ),
        }

    sell_quantities = _allocate_lots(
        active_tactical_quantity,
        sell_allocations,
        lot,
    )
    buy_quantities = _allocate_lots(
        active_buy_capacity,
        buy_allocations,
        lot,
    )
    sell_orders = []
    buy_orders = []
    for level in range(1, grid_levels + 1):
        sell_orders.append(
            {
                "level": level,
                "price": _price(reference * (1 + step_pct * level / 100)),
                "allocation_pct": sell_allocations[level - 1],
                "quantity": sell_quantities[level - 1],
            }
        )
        buy_orders.append(
            {
                "level": level,
                "price": _price(reference * (1 - step_pct * level / 100)),
                "allocation_pct": buy_allocations[level - 1],
                "quantity": buy_quantities[level - 1],
            }
        )

    tactical_stop_price = reference * (1 - stop_pct / 100)

    warnings = [
        "底仓不参与日常网格，但基本面逻辑失效时仍需重新评估，不能理解为永久不卖。",
        "网格价格基于参考价的机械距离，不代表支撑阻力或成交保证。",
        "高抛后若价格继续上涨，机动仓可能无法按原价买回；低吸后若继续下跌，仍会产生亏损。",
    ]
    if any(quantity == 0 for quantity in sell_quantities):
        warnings.append("受最小交易单位限制，至少一档高抛数量为零。")
    if any(quantity == 0 for quantity in buy_quantities):
        warnings.append("受最小交易单位限制，至少一档低吸数量为零。")
    if stop_pct <= step_pct * grid_levels:
        warnings.append("机动仓退出线未低于最深一档低吸价，请复核止损距离。")

    return {
        "status": "ok",
        "symbol": symbol or None,
        "reference_price": _price(reference),
        "allocation_plan": {
            "max_symbol_pct": symbol_cap_pct,
            "max_symbol_value": _money(max_symbol_value),
            "core_pct_of_symbol": core_ratio,
            "core_target_quantity": core_quantity,
            "core_target_value": _money(core_quantity * reference),
            "core_actual_pct_of_equity": round(
                core_quantity * reference / equity * 100,
                4,
            ),
            "tactical_max_quantity": tactical_max_quantity,
            "tactical_max_value_at_reference": _money(
                tactical_max_quantity * reference
            ),
            "tactical_initial_quantity": tactical_initial_quantity,
            "tactical_initial_value": _money(
                tactical_initial_quantity * reference
            ),
            "tactical_buy_capacity_quantity": tactical_buy_capacity,
            "tactical_cash_reserve_budget": _money(
                tactical_budget - tactical_initial_quantity * reference
            ),
            "initial_target_quantity": initial_target_quantity,
            "initial_target_value": _money(initial_target_quantity * reference),
            "max_total_quantity": max_total_quantity,
            "max_total_value_at_reference": _money(
                max_total_quantity * reference
            ),
            "lot_size": lot,
        },
        "tactical_grid": {
            "grid_step_pct": step_pct,
            "sell_high_orders": sell_orders,
            "buy_low_orders": buy_orders,
            "tactical_stop_price": _price(tactical_stop_price),
            "tactical_stop_pct": stop_pct,
        },
        "current_position": current_position,
        "rules": {
            "core": "日常高抛低吸不得卖出底仓；仅在基本面逻辑失效或组合上限变化时重评。",
            "tactical": "只使用机动仓挂单；卖出所得回到机动现金池，低吸不得突破机动仓最大数量。",
            "recalculate": "任一档成交后传入最新 current_quantity 重新计算，后续挂单按当前机动持股和剩余容量生成。",
            "anchor_reset": "财报、重大事件或价格趋势显著变化后重设参考价，避免长期沿用失效网格。",
        },
        "warnings": warnings,
        "disclaimer": "仅用于仓位规划与计算，不构成针对该证券的投资建议。",
    }
