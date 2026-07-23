import asyncio
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import finance_mcp_tools
from finance_mcp_tools import (
    _expected_scenario_probabilities,
    _market_from_symbol,
    handle_validate_report,
)
from mega_finance_bridge import list_tools
from skills.buffett_scoring import (
    calculate_graham_value,
    calculate_intrinsic_value,
    calculate_owner_earnings,
    score_owner_earnings,
)


def _render_complete_v2_report():
    text = Path("report_template.md").read_text(encoding="utf-8")
    text = re.sub(r"\[[^\n\]]+\]", "N/A", text)
    while "| |" in text:
        text = text.replace("| |", "| N/A |")

    replacements = {
        "**N/A]** — N/A": "**[[Rating / 评级: 强烈买入]]** — 测试结论。",
        "> - 数据状态: N/A | score_coverage: N/A%": "> - 数据状态: 正常 | score_coverage: 100%",
        "> - Step 1 硬性排除: N/A": "> - Step 1 硬性排除: 未触发",
        "> - 主估值模型: N/A | 安全边际: N/A%": "> - 主估值模型: DCF | 安全边际: 40%",
        "> - 综合评分: **N/A/100** (估值N/A/30 + 品质N/A/25 + 回报N/A/20 + 宏观N/A/15 + 趋势N/A/10)": "> - 综合评分: **100/100** (估值30/30 + 品质25/25 + 回报20/20 + 宏观15/15 + 趋势10/10)",
        "> - 技术面约束: N/A": "> - 技术面约束: none",
        "> - 基础评级 → 最终评级: N/A → N/A": "> - 基础评级 → 最终评级: 强烈买入 → 强烈买入",
        "*   **宏观总分**: **N/A/15**": "*   **宏观总分**: **15/15**",
        "### **6.3 量化评分表 (商业品质基础分 N/A/27 = N/A% | 扩展诊断 N/A/8)": "### **6.3 量化评分表 (商业品质基础分 27/27 = 100% | 扩展诊断 8/8)",
        "primary_trend: N/A": "primary_trend: bullish",
        "ADX: N/A": "ADX: 30",
        "trend_strength: N/A": "trend_strength: strong trend",
        "rating_constraint: N/A": "rating_constraint: none",
        "采用路径: N/A": "采用路径: A",
        "主模型价值: N/A N/A": "主模型价值: USD 140",
        "主模型安全边际: (N/A - N/A) / N/A = N/A%": "主模型安全边际: (140 - 100) / 100 = 40%",
        "总预期年化回报:   N/A%": "总预期年化回报:   20.0%",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    quality_scores = {
        "基本面状况": 7,
        "盈利一致性": 3,
        "竞争护城河": 5,
        "管理层品质": 2,
        "账面价值增长": 5,
        "通胀/成本转嫁力": 5,
    }
    quarter_rows = ["2026Q1", "N/A", "N/A", "N/A"]
    kpi_rows = ["2022", "2023", "2024", "2025", "2026", "**2026Q1**", "**指引**"]
    in_quarters = False
    in_kpi = False
    quarter_index = 0
    kpi_index = 0
    lines = []
    for line in text.splitlines():
        if line.startswith("#### **6.4.1"):
            in_quarters = True
        elif line.startswith("### **6.5"):
            in_quarters = False
            in_kpi = True
        elif line.startswith("### **6.6"):
            in_kpi = False

        if in_quarters and line.startswith("| N/A |"):
            label = quarter_rows[quarter_index]
            line = (
                f"| {label} | 100（YoY +10%） | 10（YoY +10%） | 12（YoY +8%） | 改善 |"
                if quarter_index == 0
                else f"| {label} | N/A | N/A | N/A | 尚未发布 |"
            )
            quarter_index += 1
        if in_kpi and (line.startswith("| N/A |") or line.startswith("| **N/A** |")):
            line = f"| {kpi_rows[kpi_index]} | 100 | +10% | 有来源 |"
            kpi_index += 1

        for label, score in quality_scores.items():
            if line.startswith("| ") and label in line:
                cells = line.split("|")
                cells[3] = f" {score} "
                line = "|".join(cells)

        if "Bull Case" in line:
            line = "| **🟢 乐观 (Bull Case)** | 60.0% | 盈利超预期 | USD 150 | `2027E EPS 10 × 15x PE`；倍数依据为历史中枢 |"
        elif "Base Case" in line:
            line = "| **🟡 中性 (Base Case)** | 30.0% | 维持现状 | USD 120 | `2027E EPS 10 × 12x PE`；倍数依据为同行中枢 |"
        elif "Bear Case" in line:
            line = "| **🔴 悲观 (Bear Case)** | 10.0% | 需求走弱 | USD 80 | `2027E EPS 10 × 8x PE`；倍数依据为历史低位 |"
        elif line.startswith("**MCP 策略共识**"):
            line = "**MCP 策略共识**: **HOLD** | **趋势分类**: bullish | **Z-Score**: 0.2"
        lines.append(line)

    text = "\n".join(lines)
    text = text.replace(
        "- 近 6 个月重大事件预检：N/A",
        "- 近 6 个月重大事件预检：检索范围 2026-01-22 至 2026-07-22，访问日期 2026-07-22，未发现重大事件。",
    )
    return text.replace(
        "- 缺失数据与评分覆盖率：N/A",
        "- 缺失数据与评分覆盖率：无缺失，score_coverage 100%。",
    )


class RulesContractTests(unittest.TestCase):
    def test_every_mcp_tool_exposes_an_input_schema(self):
        for tool in list_tools()["tools"]:
            self.assertIn("inputSchema", tool)
            self.assertEqual(tool["inputSchema"]["type"], "object")

    def test_symbol_market_mapping(self):
        self.assertEqual(_market_from_symbol("AAPL"), "USA")
        self.assertEqual(_market_from_symbol("0700.HK"), "HKG")
        self.assertEqual(_market_from_symbol("600039.SS"), "CHN")
        self.assertEqual(_market_from_symbol("2330.TW"), "TWN")

    def test_verified_maintenance_capex_override_is_used_exactly(self):
        result = calculate_owner_earnings(
            net_income=100,
            depreciation=20,
            capex=80,
            maintenance_capex_override=30,
        )
        self.assertEqual(result["components"]["estimated_maintenance_capex"], 30)
        self.assertEqual(result["owner_earnings"], 90)
        self.assertEqual(result["components"]["maintenance_capex_value_type"], "verified_override")

    def test_heuristic_maintenance_capex_never_exceeds_total_capex(self):
        result = calculate_owner_earnings(100, 100, 40)
        self.assertEqual(result["components"]["estimated_maintenance_capex"], 40)
        score = score_owner_earnings(
            result["owner_earnings"],
            1_000,
            result["components"]["maintenance_capex_value_type"],
        )
        self.assertEqual(score["score_status"], "estimate_only")
        self.assertIsNone(score["score"])

    def test_missing_growth_does_not_receive_a_default(self):
        dcf = calculate_intrinsic_value(100, 10, None)
        graham = calculate_graham_value(5, None, 0.04)
        self.assertEqual(dcf["score_status"], "incomplete")
        self.assertEqual(graham["status"], "incomplete")

    def test_scenario_formula_is_bounded_and_sums_to_one_hundred(self):
        for inputs in [(0, 0, 0), (15, 27, 10), (11, 21, 7)]:
            probabilities = _expected_scenario_probabilities(*inputs)
            self.assertAlmostEqual(sum(probabilities), 100)
            self.assertGreaterEqual(min(probabilities), 0)
            self.assertGreaterEqual(probabilities[2], 10)

    def test_unfilled_template_cannot_pass_validation(self):
        result = asyncio.run(handle_validate_report("report_template.md"))
        self.assertEqual(result["status"], "fail")
        issue_types = {issue["type"] for issue in result["issues"]}
        self.assertIn("unresolved_placeholders", issue_types)
        self.assertIn("scenario_probability_unverified", issue_types)

    def test_template_requires_sotp_multiple_and_cash_flow_validation(self):
        template = Path("report_template.md").read_text(encoding="utf-8")
        rules = Path("rules.md").read_text(encoding="utf-8")

        self.assertIn("SOTP 分部估值 → 隐含 EV/EBIT 验证 → 正常化 FCF/DCF 验证", template)
        self.assertIn("近端增长率", template)
        self.assertIn("稳定期增长率", template)
        self.assertIn("SOTP 隐含 FCF Yield", template)
        self.assertIn("不得把三种结果简单平均", template)
        self.assertIn("SOTP → 隐含 EV/EBIT → 正常化 FCF/DCF", rules)

    def test_complete_v2_report_can_pass_validation(self):
        report_path = Path.cwd() / "reports" / "TEST_analysis_20260722.md"
        report_text = _render_complete_v2_report()
        with patch.object(
            finance_mcp_tools,
            "_read_report",
            return_value=(report_path, report_text, None),
        ):
            result = asyncio.run(handle_validate_report(str(report_path)))
        self.assertEqual(result["status"], "pass", result["issues"])


if __name__ == "__main__":
    unittest.main()
