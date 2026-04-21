# AntiGravity 自动驾驶准则 - Finance Learn MCP 项目

## 1. 报告存储规范 (CRITICAL)
- **禁止使用隐藏目录**：严禁将分析结果、报告、图片保存至 `.gemini/` 或其他系统隐藏文件夹。
- **默认报告路径**：所有生成的报告必须统一保存至 `file:///Users/dap/Documents/work/project/python/finance/learn_mcp/reports/` 目录下。
- **文件名规范**：使用 `symbol_analysis_YYYYMMDD.md` 格式。

## 2. 技能与工具使用规范
- **核心入口**：本项目的所有金融数据分析必须通过 `/Users/dap/Documents/work/project/python/finance/learn_mcp/mega_finance_bridge.py` 进行。
- **禁止冗余**：不要为每次分析创建新的 runner 脚本，直接通过 Python 调用 bridge 里的 handler 函数。
- **本地 Skill 优先**：在处理估值、技术面和巴菲特评分时，必须优先使用工作区 `skills/` 目录下的 Python 实现，而不是系统内置的默认逻辑。

## 3. 分析流程
1. 调用 `mega_finance_bridge.py` 获取量化结果。
2. 结合量化结果与巴菲特框架资料进行综合推理。
3. 直接在 `reports/` 下更新或创建 Markdown 报告。

## 4. 输出规范
- **严禁垃圾文件 (HARD RULE)**：严禁生成任何中间临时脚本、代码文件、测试文件或数据文件（包括 scratch 目录和 .py 文件）。
- **执行方式**：所有数据获取、处理和逻辑执行必须直接通过命令行单行指令（One-liners）完成。
- **直给报告**：针对分析请求，直接输出最终的 Markdown 报告。

## 5. 报告结构与深度规范 (STANDARD)
- **核心指令 (CRITICAL)**：在生成任何分析报告前，**必须首先完整阅读并遵循** 根目录下的 [report_rules.md](file:///Users/dap/Documents/work/project/python/finance/learn_mcp/report_rules.md) 中的 Markdown 模版。
- **一致性要求**：每份分析报告必须严格按照 `report_rules.md` 定义的 10 个模块进行组织，严禁简化标题或删除核心量化指标。
- **模版溯源**：该模版基于 Caterpillar (CAT) 的深度分析模式，代表了本项目最高的生成标准。
