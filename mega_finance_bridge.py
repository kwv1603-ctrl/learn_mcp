"""
HyperFinanceBridge v2.2 — MCP Financial Data & Analysis Platform
=================================================================
Supports yfinance for financial data and analysis.
"""
import sys
import os
import json
import math
import asyncio
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# Add path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from skills.buffett_scoring import run_full_buffett_analysis
from skills.finagent_strategies import run_all_strategies
from skills.finagent_reflection import compute_multi_timeframe_reflection

async def get_unified_ticker(symbol):
    """Returns a yfinance ticker object."""
    return yf.Ticker(symbol)

# ═══════════════════════════════════════════════
#  Tool Handlers (Async)
# ═══════════════════════════════════════════════

async def handle_get_stock_valuation(symbol):
    ticker = await get_unified_ticker(symbol)
    info = ticker.info
    if not info: return {"error": f"No data found for {symbol}"}
    
    # Same logic as v2.0
    val = {
        "symbol": symbol,
        "name": info.get("longName", info.get("shortName")),
        "price": info.get("currentPrice"),
        "pe": info.get("trailingPE"),
        "pb": info.get("priceToBook"),
        "dividendYield": info.get("dividendYield"),
        "marketCap": info.get("marketCap")
    }
    return val

async def handle_get_financial_statements(symbol, quarterly=False):
    ticker = await get_unified_ticker(symbol)
    if quarterly:
        return {
            "income": ticker.quarterly_income_stmt.to_dict(),
            "balance": ticker.quarterly_balance_sheet.to_dict(),
            "cashflow": ticker.quarterly_cashflow.to_dict()
        }
    return {
        "income": ticker.income_stmt.to_dict(),
        "balance": ticker.balance_sheet.to_dict(),
        "cashflow": ticker.cashflow.to_dict()
    }

async def handle_get_stock_price_history(symbol, period="6mo", interval="1d"):
    ticker = await get_unified_ticker(symbol)
    if hasattr(ticker, 'history') and asyncio.iscoroutinefunction(ticker.history):
        hist = await ticker.history(period=period, interval=interval)
    else:
        hist = ticker.history(period=period, interval=interval)
    
    if hist.empty: return {"error": "No data"}
    return hist.tail(30).to_dict()

async def handle_run_buffett_analysis(symbol):
    ticker = await get_unified_ticker(symbol)
    return run_full_buffett_analysis(ticker)

async def handle_run_finagent_strategy_scan(symbol, period="6mo"):
    ticker = await get_unified_ticker(symbol)
    if hasattr(ticker, 'history') and asyncio.iscoroutinefunction(ticker.history):
        hist = await ticker.history(period=period, interval="1d")
    else:
        hist = ticker.history(period=period, interval="1d")
    return run_all_strategies(hist)

async def handle_run_finagent_reflection(symbol, period="3mo"):
    ticker = await get_unified_ticker(symbol)
    if hasattr(ticker, 'history') and asyncio.iscoroutinefunction(ticker.history):
        hist = await ticker.history(period=period, interval="1d")
    else:
        hist = ticker.history(period=period, interval="1d")
    return compute_multi_timeframe_reflection(hist)

# ═══════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════

def list_tools():
    return {
        "tools": [
            {"name": "get_stock_valuation", "description": "Valuation metrics"},
            {"name": "get_financial_statements", "description": "Financial reports"},
            {"name": "get_stock_price_history", "description": "Price history"},
            {"name": "run_buffett_analysis", "description": "Buffett scoring"},
            {"name": "run_finagent_strategy_scan", "description": "Technical strategies"},
            {"name": "run_finagent_reflection", "description": "Price reflection"}
        ]
    }

async def process_request(request):
    req_id = request.get("id")
    method = request.get("method")
    params = request.get("params", {})

    if method == "initialize":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "HyperFinanceBridge", "version": "2.1.0"}}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": list_tools()}
    if method == "tools/call":
        tool_name = params.get("name")
        args = params.get("arguments", {})
        res = {}
        if tool_name == "get_stock_valuation": res = await handle_get_stock_valuation(args.get("symbol"))
        elif tool_name == "get_financial_statements": res = await handle_get_financial_statements(args.get("symbol"), args.get("quarterly", False))
        elif tool_name == "get_stock_price_history": res = await handle_get_stock_price_history(args.get("symbol"), args.get("period", "6mo"))
        elif tool_name == "run_buffett_analysis": res = await handle_run_buffett_analysis(args.get("symbol"))
        elif tool_name == "run_finagent_strategy_scan": res = await handle_run_finagent_strategy_scan(args.get("symbol"), args.get("period", "6mo"))
        elif tool_name == "run_finagent_reflection": res = await handle_run_finagent_reflection(args.get("symbol"), args.get("period", "3mo"))
        return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "text", "text": json.dumps(res, default=str)}]}}
    return None

async def main():
    while True:
        line = sys.stdin.readline()
        if not line: break
        try:
            request = json.loads(line)
            response = await process_request(request)
            if response:
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
        except: pass

if __name__ == "__main__":
    asyncio.run(main())
