# learn_mcp

HyperFinanceBridge 是用于股票财务、估值、技术面和报告校验的本地 MCP 服务。

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
