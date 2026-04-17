"""
HyperFinanceBridge v2.0 — MCP Financial Data & Analysis Platform
=================================================================

7 pure tools, 0 external API keys required.
Data Source: yfinance (free)
Quantitative Analysis: Extracted skills from ai-hedge-fund + FinAgent

Architecture:
  Data Layer (yfinance) → Quantitative Scoring (pure Python) → LLM Reasoning (Antigravity)
"""

import sys
import os
import json
import math
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# Add skills directory to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from skills.buffett_scoring import run_full_buffett_analysis
from skills.finagent_strategies import run_all_strategies
from skills.finagent_reflection import compute_multi_timeframe_reflection


# ═══════════════════════════════════════════════
#  MCP Tool Registry
# ═══════════════════════════════════════════════
def list_tools():
    return {
        "tools": [
            {
                "name": "get_stock_valuation",
                "description": "Quick quantitative valuation: PE, PB, EV/EBITDA, Graham Intrinsic Value, Dividend Yield",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol (e.g., TSM, AAPL, 2330.TW)"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "get_financial_statements",
                "description": "Get income statement, balance sheet, and cash flow statement (annual + quarterly)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol"},
                        "quarterly": {"type": "boolean", "description": "If true, return quarterly data. Default: false (annual)"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "get_stock_price_history",
                "description": "Get historical OHLCV price data with optional period/interval",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol"},
                        "period": {"type": "string", "description": "Data period: 1mo, 3mo, 6mo, 1y, 2y, 5y, max. Default: 6mo"},
                        "interval": {"type": "string", "description": "Data interval: 1d, 1wk, 1mo. Default: 1d"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "get_stock_news_and_analysts",
                "description": "Get latest news, analyst recommendations, and institutional holders",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "run_buffett_analysis",
                "description": "Run 8-dimension Buffett quantitative scoring: fundamentals, moat, management quality, owner earnings, DCF intrinsic value, margin of safety, book value growth, pricing power. (Extracted from ai-hedge-fund 54k⭐)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "run_finagent_strategy_scan",
                "description": "Run 5 technical strategy scanners (MACD, KDJ+RSI, Stochastic+Bollinger, Mean Reversion, ATR Volatility) and return consensus BUY/SELL/HOLD signal. (Extracted from FinAgent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol"},
                        "period": {"type": "string", "description": "Historical data period for analysis. Default: 6mo"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "run_finagent_reflection",
                "description": "Multi-timeframe price movement reflection (1-day/7-day/14-day), trend classification, and annualized volatility. (Extracted from FinAgent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "Stock symbol"},
                        "period": {"type": "string", "description": "Historical data period. Default: 3mo"}
                    },
                    "required": ["symbol"]
                }
            }
        ]
    }


# ═══════════════════════════════════════════════
#  Tool Handlers
# ═══════════════════════════════════════════════

def handle_get_stock_valuation(symbol):
    """Quick valuation metrics from yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        eps = info.get("trailingEps", 0)
        growth = info.get("earningsGrowth", 0.05)
        growth_pct = (growth if growth else 0.05) * 100
        current_price = info.get("currentPrice", 0)

        # Graham Formula: V = EPS × (8.5 + 2g)
        graham_val = round(eps * (8.5 + 2 * min(growth_pct, 15)), 2) if eps and eps > 0 else None

        return {
            "symbol": symbol,
            "name": info.get("shortName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "price": current_price,
            "currency": info.get("currency"),
            "pe_trailing": info.get("trailingPE"),
            "pe_forward": info.get("forwardPE"),
            "pb": info.get("priceToBook"),
            "ps": info.get("priceToSalesTrailing12Months"),
            "ev_ebitda": info.get("enterpriseToEbitda"),
            "dividend_yield": f"{info.get('dividendYield', 0) * 100:.2f}%" if info.get("dividendYield") else "N/A",
            "peg_ratio": info.get("pegRatio"),
            "graham_intrinsic_value": graham_val,
            "margin_of_safety": f"{(graham_val - current_price) / current_price:.1%}" if graham_val and current_price else "N/A",
            "market_cap": info.get("marketCap"),
            "enterprise_value": info.get("enterpriseValue"),
        }
    except Exception as e:
        return {"error": str(e)}


def handle_get_financial_statements(symbol, quarterly=False):
    """Get income statement, balance sheet, cash flow."""
    try:
        ticker = yf.Ticker(symbol)

        if quarterly:
            income = ticker.quarterly_income_stmt
            balance = ticker.quarterly_balance_sheet
            cashflow = ticker.quarterly_cashflow
            period_type = "quarterly"
        else:
            income = ticker.income_stmt
            balance = ticker.balance_sheet
            cashflow = ticker.cashflow
            period_type = "annual"

        def df_to_dict(df):
            if df is None or df.empty:
                return {}
            result = {}
            for col in df.columns:
                period_key = col.strftime("%Y-%m-%d") if hasattr(col, 'strftime') else str(col)
                result[period_key] = {}
                for idx in df.index:
                    val = df.loc[idx, col]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        result[period_key][idx] = float(val) if isinstance(val, (int, float)) else str(val)
            return result

        return {
            "symbol": symbol,
            "period_type": period_type,
            "income_statement": df_to_dict(income),
            "balance_sheet": df_to_dict(balance),
            "cash_flow": df_to_dict(cashflow),
        }
    except Exception as e:
        return {"error": str(e)}


def handle_get_stock_price_history(symbol, period="6mo", interval="1d"):
    """Get OHLCV price history."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)

        if hist.empty:
            return {"error": f"No price data for {symbol} with period={period}"}

        # Convert to list of dicts (last 60 data points to keep response manageable)
        records = []
        for date, row in hist.tail(60).iterrows():
            records.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(float(row.get("Open", 0)), 2),
                "high": round(float(row.get("High", 0)), 2),
                "low": round(float(row.get("Low", 0)), 2),
                "close": round(float(row.get("Close", 0)), 2),
                "volume": int(row.get("Volume", 0)),
            })

        return {
            "symbol": symbol,
            "period": period,
            "interval": interval,
            "data_points": len(records),
            "latest_close": records[-1]["close"] if records else None,
            "prices": records,
        }
    except Exception as e:
        return {"error": str(e)}


def handle_get_stock_news_and_analysts(symbol):
    """Get news, analyst ratings, and institutional holders."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        # News
        news_items = []
        try:
            news = ticker.news
            if news:
                for item in news[:10]:
                    news_items.append({
                        "title": item.get("title", ""),
                        "publisher": item.get("publisher", ""),
                        "link": item.get("link", ""),
                        "published": item.get("providerPublishTime", ""),
                    })
        except Exception:
            pass

        # Analyst recommendations
        recommendations = []
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                for date, row in recs.tail(10).iterrows():
                    rec_dict = {"date": str(date)}
                    for col in row.index:
                        rec_dict[col] = str(row[col]) if row[col] is not None else None
                    recommendations.append(rec_dict)
        except Exception:
            pass

        # Analyst price targets
        target_data = {}
        for key in ["targetHighPrice", "targetLowPrice", "targetMeanPrice", "targetMedianPrice",
                     "recommendationKey", "recommendationMean", "numberOfAnalystOpinions"]:
            if info.get(key) is not None:
                target_data[key] = info[key]

        # Major holders
        holders_info = []
        try:
            holders = ticker.institutional_holders
            if holders is not None and not holders.empty:
                for _, row in holders.head(10).iterrows():
                    holders_info.append({
                        "holder": str(row.get("Holder", "")),
                        "shares": int(row.get("Shares", 0)) if row.get("Shares") else 0,
                        "pct_out": f"{row.get('pctHeld', 0) * 100:.2f}%" if row.get("pctHeld") else "N/A",
                    })
        except Exception:
            pass

        return {
            "symbol": symbol,
            "company": info.get("shortName"),
            "news": news_items,
            "analyst_targets": target_data,
            "recommendations": recommendations,
            "institutional_holders": holders_info,
        }
    except Exception as e:
        return {"error": str(e)}


def handle_run_buffett_analysis(symbol):
    """Run full 8-dimension Buffett quantitative analysis."""
    try:
        ticker = yf.Ticker(symbol)
        result = run_full_buffett_analysis(ticker)
        return result
    except Exception as e:
        return {"error": f"Buffett analysis failed: {str(e)}"}


def handle_run_finagent_strategy_scan(symbol, period="6mo"):
    """Run 5 FinAgent trading strategies and return consensus."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval="1d")

        if hist.empty or len(hist) < 30:
            return {"error": f"Insufficient price data for {symbol} (need ≥30 days)"}

        result = run_all_strategies(hist)
        result["symbol"] = symbol
        result["period"] = period
        result["data_points"] = len(hist)
        return result
    except Exception as e:
        return {"error": f"Strategy scan failed: {str(e)}"}


def handle_run_finagent_reflection(symbol, period="3mo"):
    """Run multi-timeframe price reflection analysis."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval="1d")

        if hist.empty:
            return {"error": f"No price data for {symbol}"}

        result = compute_multi_timeframe_reflection(hist)
        result["symbol"] = symbol
        result["period"] = period
        return result
    except Exception as e:
        return {"error": f"Reflection analysis failed: {str(e)}"}


# ═══════════════════════════════════════════════
#  MCP Protocol Handler
# ═══════════════════════════════════════════════
def main():
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            request = json.loads(line)
            req_id = request.get("id")
            method = request.get("method")
            params = request.get("params", {})

            if method == "initialize":
                response = {
                    "jsonrpc": "2.0", "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "HyperFinanceBridge", "version": "2.0.0"}
                    }
                }
            elif method == "tools/list":
                response = {"jsonrpc": "2.0", "id": req_id, "result": list_tools()}
            elif method == "tools/call":
                tool_name = params.get("name")
                args = params.get("arguments", {})

                if tool_name == "get_stock_valuation":
                    res = handle_get_stock_valuation(args.get("symbol"))
                elif tool_name == "get_financial_statements":
                    res = handle_get_financial_statements(args.get("symbol"), args.get("quarterly", False))
                elif tool_name == "get_stock_price_history":
                    res = handle_get_stock_price_history(args.get("symbol"), args.get("period", "6mo"), args.get("interval", "1d"))
                elif tool_name == "get_stock_news_and_analysts":
                    res = handle_get_stock_news_and_analysts(args.get("symbol"))
                elif tool_name == "run_buffett_analysis":
                    res = handle_run_buffett_analysis(args.get("symbol"))
                elif tool_name == "run_finagent_strategy_scan":
                    res = handle_run_finagent_strategy_scan(args.get("symbol"), args.get("period", "6mo"))
                elif tool_name == "run_finagent_reflection":
                    res = handle_run_finagent_reflection(args.get("symbol"), args.get("period", "3mo"))
                else:
                    res = {"error": f"Tool '{tool_name}' not found"}

                response = {
                    "jsonrpc": "2.0", "id": req_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(res, ensure_ascii=False, indent=2, default=str)}]}
                }
            elif method == "notifications/initialized":
                # Client acknowledgment — no response needed
                continue
            else:
                response = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method '{method}' not found"}}

            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
        except json.JSONDecodeError:
            continue
        except Exception as e:
            sys.stderr.write(f"Error: {str(e)}\n")
            sys.stderr.flush()


if __name__ == "__main__":
    main()
