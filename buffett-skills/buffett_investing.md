# Warren Buffett Value Investing Skill

## Description

Use the HyperFinanceBridge MCP tools to evaluate operating quality, Owner Earnings and industry-appropriate intrinsic value without treating missing data as zero or as an invented estimate.

## Required workflow

1. Call `run_report_readiness_check` first. Supply the ticker and, when available, at least two explicit peers.
2. Use `get_stock_valuation` for current market metrics.
3. Call `get_financial_statements` twice: once with `quarterly=false` and once with `quarterly=true`.
4. Use `run_buffett_analysis` for the pure operating-quality `/27` score and separate Owner Earnings/DCF diagnostics. If verified maintenance CapEx is available, pass `override_maintenance_capex`; do not pass maintenance CapEx as `override_capex`.
5. Use `get_peer_comparison`, `run_finagent_strategy_scan`, `run_finagent_reflection` and `get_macro_context` for the corresponding report sections.
6. Obtain current official filings, the same-currency risk-free rate, core KPI evidence and the six-month catalyst scan from external authoritative sources.
7. Write the report using `report_template.md`, determine the rating only through `rating_criteria.md`, then call `validate_report` until it passes.

## Data and valuation rules

- Keep disclosed values, reproducible derived values, estimates and missing values distinct.
- Missing values remain `N/A`; they do not receive points.
- Commercial quality uses only the `/27` operating score. Owner Earnings and intrinsic value are separate diagnostics and must not be double-counted.
- Graham is optional and must not be the primary model for banks, insurers, resource-cycle companies, negative-EPS companies or major restructurings unless a specific applicability case is documented.
- A target price needs a forecast period, per-share input, valuation multiple and evidence for that multiple.
- The technical rating ceiling is controlled only by the structured `rating_constraint` returned by `run_finagent_reflection`.

## Output

Produce only the final report at `reports/symbol_analysis_YYYYMMDD.md`. The report must declare SOP V2.0, score coverage, primary valuation model, base-to-final rating transition and source dates.
