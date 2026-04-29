"""
HyperFinanceBridge v2.3 — thin MCP JSON-RPC entrypoint.

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

SERVER_INFO = {"name": "HyperFinanceBridge", "version": "2.3.0"}
PROTOCOL_VERSION = "2024-11-05"

TOOLS = [
    {"name": "get_stock_valuation", "description": "Valuation metrics", "args": ["symbol"]},
    {"name": "get_financial_statements", "description": "Financial reports", "args": ["symbol", "quarterly?"]},
    {"name": "get_stock_price_history", "description": "Price history", "args": ["symbol", "period?", "interval?"]},
    {"name": "run_buffett_analysis", "description": "Buffett scoring", "args": ["symbol", "risk_free_rate?", "override_capex?", "override_growth?"]},
    {"name": "run_finagent_strategy_scan", "description": "Technical strategies", "args": ["symbol", "period?"]},
    {"name": "run_finagent_reflection", "description": "Price reflection", "args": ["symbol", "period?"]},
    {"name": "get_peer_comparison", "description": "Peer valuation and profitability comparison", "args": ["symbol", "peers?"]},
    {"name": "get_macro_context", "description": "Macro context and risk-free-rate data requirements", "args": ["market"]},
    {"name": "run_report_readiness_check", "description": "Pre-report data availability check", "args": ["symbol", "report_path?"]},
    {"name": "validate_report", "description": "Validate generated report structure and hard-rule compliance", "args": ["report_path"]},
]


def list_tools():
    # MCP clients only require name/description; args stay as lightweight local documentation.
    return {"tools": [{"name": t["name"], "description": t["description"]} for t in TOOLS]}


async def call_tool(tool_name, args):
    args = args or {}
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
            override_growth=args.get("override_growth")
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
        return await handle_run_report_readiness_check(args.get("symbol"), args.get("report_path"))
    if tool_name == "validate_report":
        return await handle_validate_report(args.get("report_path"))
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
