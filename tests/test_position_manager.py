import asyncio
import unittest

from mega_finance_bridge import call_tool, list_tools
from skills.position_manager import (
    calculate_core_tactical_plan,
    calculate_position_plan,
)


class PositionManagerTests(unittest.TestCase):
    def test_balanced_percent_stop_uses_risk_and_capital_constraints(self):
        result = calculate_position_plan(
            account_equity=100_000,
            entry_price=50,
            stop_method="percent",
            stop_percent=8,
            round_trip_cost_bps=20,
        )

        self.assertEqual(result["stop_plan"]["stop_price"], 46)
        self.assertEqual(result["position_plan"]["quantity_by_risk"], 182)
        self.assertEqual(result["position_plan"]["quantity_by_capital"], 300)
        self.assertEqual(result["position_plan"]["quantity"], 182)
        self.assertAlmostEqual(
            result["position_plan"]["planned_risk_pct_of_equity"],
            0.7462,
        )
        self.assertEqual(
            [target["price"] for target in result["take_profit_plan"]["targets"]],
            [54, 58, 62],
        )
        self.assertLess(
            result["take_profit_plan"]["estimated_net_blended_reward_r"],
            result["take_profit_plan"]["blended_reward_r"],
        )

    def test_atr_short_calculates_stop_and_targets_in_correct_direction(self):
        result = calculate_position_plan(
            account_equity=50_000,
            entry_price=100,
            side="short",
            stop_method="atr",
            atr=3,
            atr_multiplier=2,
            lot_size=10,
            round_trip_cost_bps=0,
        )

        self.assertEqual(result["stop_plan"]["stop_price"], 106)
        self.assertEqual(result["position_plan"]["quantity"], 60)
        self.assertEqual(
            [target["price"] for target in result["take_profit_plan"]["targets"]],
            [94, 88, 82],
        )
        self.assertEqual(
            sum(
                target["quantity"]
                for target in result["take_profit_plan"]["targets"]
            ),
            result["position_plan"]["quantity"],
        )

    def test_remaining_portfolio_risk_can_reduce_trade_size_to_zero(self):
        result = calculate_position_plan(
            account_equity=10_000,
            entry_price=100,
            stop_method="price",
            stop_price=90,
            current_open_risk=300,
            max_portfolio_risk_pct=3,
        )

        self.assertEqual(result["position_plan"]["quantity"], 0)
        self.assertIn(
            "remaining_portfolio_risk",
            result["position_plan"]["binding_constraints"],
        )

    def test_position_cap_can_bind_before_risk_budget(self):
        result = calculate_position_plan(
            account_equity=100_000,
            entry_price=100,
            stop_method="price",
            stop_price=99,
            risk_per_trade_pct=1,
            max_position_pct=5,
            round_trip_cost_bps=0,
        )

        self.assertEqual(result["position_plan"]["quantity_by_risk"], 1000)
        self.assertEqual(result["position_plan"]["quantity_by_capital"], 50)
        self.assertEqual(result["position_plan"]["quantity"], 50)
        self.assertIn(
            "position_or_capital_cap",
            result["position_plan"]["binding_constraints"],
        )

    def test_invalid_stop_and_target_allocations_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "below entry_price"):
            calculate_position_plan(
                account_equity=10_000,
                entry_price=100,
                stop_method="price",
                stop_price=101,
            )
        with self.assertRaisesRegex(ValueError, "sum to 100"):
            calculate_position_plan(
                account_equity=10_000,
                entry_price=100,
                target_allocations_pct=[50, 40, 20],
            )

    def test_mcp_surface_exposes_and_dispatches_position_tool(self):
        names = {tool["name"] for tool in list_tools()["tools"]}
        self.assertIn("calculate_position_plan", names)

        result = asyncio.run(
            call_tool(
                "calculate_position_plan",
                {
                    "account_equity": 10_000,
                    "entry_price": 100,
                    "stop_method": "price",
                    "stop_price": 95,
                },
            )
        )
        self.assertEqual(result["status"], "ok")

        error = asyncio.run(
            call_tool(
                "calculate_position_plan",
                {
                    "account_equity": 10_000,
                    "entry_price": 100,
                    "stop_method": "price",
                },
            )
        )
        self.assertEqual(error["status"], "error")

    def test_core_tactical_plan_never_uses_core_in_sell_grid(self):
        result = calculate_core_tactical_plan(
            account_equity=10_000_000,
            reference_price=432.6,
            symbol="0700.HK",
            max_symbol_pct=8,
            core_pct_of_symbol=60,
            lot_size=100,
        )

        allocation = result["allocation_plan"]
        sell_quantity = sum(
            order["quantity"]
            for order in result["tactical_grid"]["sell_high_orders"]
        )
        buy_quantity = sum(
            order["quantity"]
            for order in result["tactical_grid"]["buy_low_orders"]
        )
        self.assertEqual(sell_quantity, allocation["tactical_initial_quantity"])
        self.assertEqual(
            buy_quantity,
            allocation["tactical_buy_capacity_quantity"],
        )
        self.assertLess(
            allocation["max_total_value_at_reference"],
            10_000_000 * 0.08,
        )
        self.assertEqual(
            [order["price"] for order in result["tactical_grid"]["sell_high_orders"]],
            [454.23, 475.86, 497.49],
        )
        self.assertEqual(
            [order["price"] for order in result["tactical_grid"]["buy_low_orders"]],
            [410.97, 389.34, 367.71],
        )
        self.assertEqual(
            [order["quantity"] for order in result["tactical_grid"]["sell_high_orders"]],
            [100, 100, 100],
        )

    def test_core_tactical_plan_classifies_existing_position_and_lot_limits(self):
        result = calculate_core_tactical_plan(
            account_equity=1_000_000,
            reference_price=432.6,
            symbol="0700.HK",
            lot_size=100,
            current_quantity=300,
        )

        self.assertGreater(
            result["current_position"]["quantity_above_plan_cap"],
            0,
        )
        self.assertTrue(
            any(
                "高抛数量为零" in warning
                for warning in result["warnings"]
            )
        )

    def test_existing_position_drives_active_sell_and_buy_capacity(self):
        result = calculate_core_tactical_plan(
            account_equity=10_000_000,
            reference_price=432.6,
            lot_size=100,
            current_quantity=1_800,
        )

        self.assertEqual(
            sum(
                order["quantity"]
                for order in result["tactical_grid"]["sell_high_orders"]
            ),
            700,
        )
        self.assertEqual(
            sum(
                order["quantity"]
                for order in result["tactical_grid"]["buy_low_orders"]
            ),
            0,
        )

    def test_one_million_tencent_plan_cycles_one_tactical_lot(self):
        held = calculate_core_tactical_plan(
            account_equity=1_000_000,
            reference_price=432.6,
            symbol="0700.HK",
            max_symbol_pct=10,
            core_pct_of_symbol=50,
            initial_tactical_deployment_pct=100,
            grid_step_pct=5,
            grid_levels=1,
            lot_size=100,
            current_quantity=200,
        )
        after_sell = calculate_core_tactical_plan(
            account_equity=1_000_000,
            reference_price=432.6,
            symbol="0700.HK",
            max_symbol_pct=10,
            core_pct_of_symbol=50,
            initial_tactical_deployment_pct=100,
            grid_step_pct=5,
            grid_levels=1,
            lot_size=100,
            current_quantity=100,
        )

        self.assertEqual(held["allocation_plan"]["core_target_quantity"], 100)
        self.assertEqual(held["allocation_plan"]["tactical_max_quantity"], 100)
        self.assertEqual(
            held["tactical_grid"]["sell_high_orders"][0],
            {
                "level": 1,
                "price": 454.23,
                "allocation_pct": 100.0,
                "quantity": 100,
            },
        )
        self.assertEqual(
            after_sell["tactical_grid"]["buy_low_orders"][0],
            {
                "level": 1,
                "price": 410.97,
                "allocation_pct": 100.0,
                "quantity": 100,
            },
        )

    def test_mcp_surface_dispatches_core_tactical_tool(self):
        names = {tool["name"] for tool in list_tools()["tools"]}
        self.assertIn("calculate_core_tactical_plan", names)
        result = asyncio.run(
            call_tool(
                "calculate_core_tactical_plan",
                {
                    "account_equity": 1_000_000,
                    "reference_price": 432.6,
                    "symbol": "0700.HK",
                },
            )
        )
        self.assertEqual(result["status"], "ok")


if __name__ == "__main__":
    unittest.main()
