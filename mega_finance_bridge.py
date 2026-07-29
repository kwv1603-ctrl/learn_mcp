"""
HyperFinanceBridge v2.5 — thin MCP JSON-RPC entrypoint.

This file keeps the MCP surface visible: tool names, descriptions, and argument
dispatch live here. Heavy financial-data and report-validation implementations
live in finance_mcp_tools.py.
"""
import asyncio
import json
import math
import sys

from finance_mcp_tools import (
    handle_get_financial_statements,
    handle_get_macro_context,
    handle_get_peer_comparison,
    handle_get_stock_price_history,
    handle_get_stock_valuation,
    handle_run_buffett_analysis,
    handle_run_finagent_reflection,
    handle_run_finagent_strategy_scan,
    handle_run_report_readiness_check,
    handle_validate_report,
)
from skills.position_manager import (
    calculate_core_tactical_plan,
    calculate_position_plan,
)

SERVER_INFO = {"name": "HyperFinanceBridge", "version": "2.6.0"}
PROTOCOL_VERSION = "2024-11-05"

TOOLS = [
    {"name": "get_stock_valuation", "description": "Valuation metrics", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "get_financial_statements", "description": "Annual or quarterly financial reports", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "quarterly": {"type": "boolean", "default": False}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "get_stock_price_history", "description": "Price history", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "period": {"type": "string", "default": "6mo"}, "interval": {"type": "string", "default": "1d"}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "run_buffett_analysis", "description": "Buffett operating-quality score plus separate Owner Earnings and DCF diagnostics", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "risk_free_rate": {"type": "number", "description": "Decimal rate, e.g. 0.044 for 4.4%"}, "override_capex": {"type": "number", "description": "Verified total CapEx override"}, "override_maintenance_capex": {"type": "number", "description": "Verified maintenance CapEx used exactly without another heuristic"}, "override_growth": {"type": "number", "description": "Documented long-cycle growth rate in decimal form"}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "run_finagent_strategy_scan", "description": "Technical strategies", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "period": {"type": "string", "default": "6mo"}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "run_finagent_reflection", "description": "Multi-timeframe trend reflection and structured rating constraint", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "period": {"type": "string", "default": "3mo"}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "get_peer_comparison", "description": "Peer valuation and profitability comparison", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "peers": {"type": "array", "items": {"type": "string"}, "minItems": 1}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "get_macro_context", "description": "Macro context and same-market risk-free-rate data requirements", "inputSchema": {"type": "object", "properties": {"market": {"type": "string", "enum": ["USA", "CHN", "HKG", "TWN"]}}, "required": ["market"], "additionalProperties": False}},
    {"name": "run_report_readiness_check", "description": "Pre-report annual, quarterly, valuation, score and evidence availability check", "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "report_path": {"type": "string"}, "peers": {"type": "array", "items": {"type": "string"}}}, "required": ["symbol"], "additionalProperties": False}},
    {"name": "validate_report", "description": "Validate generated report structure, numeric logic and hard-rule compliance", "inputSchema": {"type": "object", "properties": {"report_path": {"type": "string"}}, "required": ["report_path"], "additionalProperties": False}},
    {
        "name": "calculate_position_plan",
        "description": "Risk-first position size, stop-loss and staged take-profit plan",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account_equity": {"type": "number", "exclusiveMinimum": 0, "description": "Total account equity"},
                "entry_price": {"type": "number", "exclusiveMinimum": 0},
                "side": {"type": "string", "enum": ["long", "short"], "default": "long"},
                "risk_profile": {"type": "string", "enum": ["conservative", "balanced", "aggressive"], "default": "balanced"},
                "stop_method": {"type": "string", "enum": ["price", "percent", "atr"], "default": "percent"},
                "stop_price": {"type": "number", "exclusiveMinimum": 0, "description": "Required for stop_method=price"},
                "stop_percent": {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 100, "description": "Percentage points; 8 means 8%. Defaults to 8 for stop_method=percent"},
                "atr": {"type": "number", "exclusiveMinimum": 0, "description": "Required for stop_method=atr"},
                "atr_multiplier": {"type": "number", "exclusiveMinimum": 0, "default": 2},
                "risk_per_trade_pct": {"type": "number", "exclusiveMinimum": 0, "description": "Optional preset override in percentage points; 0.5 means 0.5%"},
                "max_portfolio_risk_pct": {"type": "number", "exclusiveMinimum": 0, "description": "Optional preset override; total open risk as percentage points of equity"},
                "current_open_risk": {"type": "number", "minimum": 0, "default": 0, "description": "Current open risk in account currency, not position market value"},
                "max_position_pct": {"type": "number", "exclusiveMinimum": 0, "maximum": 100, "description": "Optional preset override; maximum position value as percentage points of equity"},
                "capital_available": {"type": "number", "minimum": 0, "description": "Cash or notional capacity available; defaults to account equity"},
                "lot_size": {"type": "number", "exclusiveMinimum": 0, "default": 1},
                "round_trip_cost_bps": {"type": "number", "minimum": 0, "exclusiveMaximum": 10000, "default": 20, "description": "Estimated fees plus slippage for entry and exit, in basis points"},
                "target_r_multiples": {"type": "array", "items": {"type": "number", "exclusiveMinimum": 0}, "minItems": 1, "default": [1, 2, 3]},
                "target_allocations_pct": {"type": "array", "items": {"type": "number", "exclusiveMinimum": 0}, "minItems": 1, "default": [30, 40, 30], "description": "Percentage points allocated to each target; must sum to 100"}
            },
            "required": ["account_equity", "entry_price"],
            "additionalProperties": False
        }
    },
    {
        "name": "calculate_core_tactical_plan",
        "description": "Split one security into a protected core holding and a capped tactical high-sell/low-buy sleeve",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account_equity": {"type": "number", "exclusiveMinimum": 0},
                "reference_price": {"type": "number", "exclusiveMinimum": 0},
                "symbol": {"type": "string"},
                "max_symbol_pct": {"type": "number", "exclusiveMinimum": 0, "maximum": 100, "default": 8, "description": "Maximum security exposure as percentage points of account equity"},
                "core_pct_of_symbol": {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 100, "default": 60, "description": "Core sleeve as percentage points of maximum security exposure"},
                "initial_tactical_deployment_pct": {"type": "number", "minimum": 0, "maximum": 100, "default": 50, "description": "Share of tactical capacity initially held as shares; the rest remains a buy-low cash reserve"},
                "grid_step_pct": {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 100, "default": 5},
                "grid_levels": {"type": "integer", "minimum": 1, "maximum": 10, "default": 3},
                "lot_size": {"type": "number", "exclusiveMinimum": 0, "default": 1},
                "tactical_stop_pct": {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 100, "default": 20},
                "sell_allocations_pct": {"type": "array", "items": {"type": "number", "minimum": 0}, "minItems": 1, "description": "One item per grid level; must sum to 100"},
                "buy_allocations_pct": {"type": "array", "items": {"type": "number", "minimum": 0}, "minItems": 1, "description": "One item per grid level; must sum to 100"},
                "current_quantity": {"type": "number", "minimum": 0}
            },
            "required": ["account_equity", "reference_price"],
            "additionalProperties": False
        }
    },
]


def list_tools():
    return {"tools": TOOLS}


async def call_tool(tool_name, args):
    args = args or {}
    tool_definition = next((tool for tool in TOOLS if tool["name"] == tool_name), None)
    if tool_definition is None:
        return {"error": f"Unknown tool: {tool_name}"}
    missing_required = [
        key for key in tool_definition["inputSchema"].get("required", [])
        if args.get(key) in (None, "")
    ]
    if missing_required:
        return {"error": f"Missing required arguments: {', '.join(missing_required)}"}
    if tool_name == "get_stock_valuation":
        return await handle_get_stock_valuation(args.get("symbol"))
    if tool_name == "get_financial_statements":
        return await handle_get_financial_statements(args.get("symbol"), args.get("quarterly", False))
    if tool_name == "get_stock_price_history":
        return await handle_get_stock_price_history(args.get("symbol"), args.get("period", "6mo"), args.get("interval", "1d"))
    if tool_name == "run_buffett_analysis":
        return await handle_run_buffett_analysis(
            args.get("symbol"),
            risk_free_rate=args.get("risk_free_rate"),
            override_capex=args.get("override_capex"),
            override_growth=args.get("override_growth"),
            override_maintenance_capex=args.get("override_maintenance_capex"),
        )
    if tool_name == "run_finagent_strategy_scan":
        return await handle_run_finagent_strategy_scan(args.get("symbol"), args.get("period", "6mo"))
    if tool_name == "run_finagent_reflection":
        return await handle_run_finagent_reflection(args.get("symbol"), args.get("period", "3mo"))
    if tool_name == "get_peer_comparison":
        return await handle_get_peer_comparison(args.get("symbol"), args.get("peers"))
    if tool_name == "get_macro_context":
        return await handle_get_macro_context(args.get("market"))
    if tool_name == "run_report_readiness_check":
        return await handle_run_report_readiness_check(args.get("symbol"), args.get("report_path"), args.get("peers"))
    if tool_name == "validate_report":
        return await handle_validate_report(args.get("report_path"))
    if tool_name == "calculate_position_plan":
        try:
            return calculate_position_plan(**args)
        except ValueError as exc:
            return {"status": "error", "error": str(exc)}
    if tool_name == "calculate_core_tactical_plan":
        try:
            return calculate_core_tactical_plan(**args)
        except ValueError as exc:
            return {"status": "error", "error": str(exc)}
    return {"error": f"Unknown tool: {tool_name}"}



def sanitize_for_json(obj):
    """Recursively convert non-serializable objects (like Timestamp keys) to JSON-safe types."""
    if isinstance(obj, dict):
        return {str(k): sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


async def process_request(request):
    req_id = request.get("id")
    method = request.get("method")
    params = request.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": SERVER_INFO,
            },
        }
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": list_tools()}
    if method == "tools/call":
        res = await call_tool(params.get("name"), params.get("arguments", {}))
        safe_res = sanitize_for_json(res)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"content": [{"type": "text", "text": json.dumps(safe_res, default=str)}]},
        }
    return None


async def main():
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        try:
            request = json.loads(line)
            response = await process_request(request)
            if response:
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
        except Exception as exc:
            error = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(exc)}}
            sys.stdout.write(json.dumps(error) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    asyncio.run(main())
