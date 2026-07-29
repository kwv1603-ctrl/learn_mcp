# learn_mcp

HyperFinanceBridge 是用于股票财务、估值、技术面和报告校验的本地 MCP 服务。

## 仓位管理

`calculate_position_plan` 按“先定义最大亏损，再反推仓位”的方式生成交易计划：

- 风险档位：保守、均衡、积极；所有上限均可覆盖。
- 止损方式：指定失效价、固定百分比、ATR 倍数。
- 仓位约束：单笔风险、组合剩余风险、单票市值上限、可用资金和最小交易单位。
- 止盈方式：按 R 倍数分批退出，默认 `1R / 2R / 3R` 对应 `30% / 40% / 30%`。
- 成本缓冲：将预估手续费与滑点纳入每单位风险。

示例参数：

```json
{
  "account_equity": 100000,
  "entry_price": 50,
  "side": "long",
  "risk_profile": "balanced",
  "stop_method": "atr",
  "atr": 1.5,
  "atr_multiplier": 2,
  "current_open_risk": 800,
  "lot_size": 1
}
```

百分比参数使用“百分点”：`0.5` 表示 `0.5%`。`current_open_risk` 是现有持仓触及各自止损时的预计亏损金额，不是持仓市值。止损单可能因跳空或流动性不足产生超出计划的实际亏损。

完整策略和公式见 [`position_management_policy.md`](position_management_policy.md)。

`calculate_core_tactical_plan` 用于同一标的的“底仓 + 机动仓”管理：

- 底仓不参与日常网格交易。
- 机动仓设置最大容量，初始只投入一部分，其余保留为低吸现金。
- 围绕参考价生成多档高抛和低吸计划。
- 最小交易单位过大、无法拆成多档时会明确提示。

## 报告规则入口

- `rules.md`：SOP、数据纪律和模型适用性。
- `rating_criteria.md`：评分覆盖率、综合评分和最终评级映射。
- `report_template.md`：SOP V2.0 报告结构。

三份文件必须配套使用。工作流以 `rules.md` 为准，评级以 `rating_criteria.md` 为准，结构以 `report_template.md` 为准。

## 启动

```bash
python mega_finance_bridge.py
```

服务通过标准输入/输出处理 MCP JSON-RPC 请求；`tools/list` 会返回每个工具的完整 `inputSchema`。
