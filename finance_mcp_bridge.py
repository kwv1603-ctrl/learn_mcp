import sys
import json
import yfinance as yf
import pandas as pd

def list_tools():
    return {
        "tools": [
            {
                "name": "get_stock_metrics",
                "description": "获取股票的关键量化数据（如市盈率、市净率、市值等）",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "股票代码 (e.g., AAPL)"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "get_financial_statements",
                "description": "获取公司的财务三表（资产负债表、利润表、现金流量表）",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "股票代码"},
                        "type": {"type": "string", "enum": ["income", "balance", "cashflow"], "description": "表单类型"}
                    },
                    "required": ["symbol", "type"]
                }
            },
            {
                "name": "calculate_valuation",
                "description": "基于格雷厄姆或巴菲特模型进行简易估值计算",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "股票代码"}
                    },
                    "required": ["symbol"]
                }
            },
            {
                "name": "get_stock_news",
                "description": "获取股票的相关新闻标题用于情绪分析",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string", "description": "股票代码"}
                    },
                    "required": ["symbol"]
                }
            }
        ]
    }

def handle_get_stock_metrics(symbol):
    ticker = yf.Ticker(symbol)
    info = ticker.info
    return {
        "symbol": symbol,
        "name": info.get("shortName"),
        "pe_ratio": info.get("trailingPE"),
        "forward_pe": info.get("forwardPE"),
        "market_cap": info.get("marketCap"),
        "pb_ratio": info.get("priceToBook"),
        "dividend_yield": info.get("dividendYield"),
        "sector": info.get("sector"),
        "currency": info.get("currency")
    }

def handle_get_financial_statements(symbol, table_type):
    ticker = yf.Ticker(symbol)
    if table_type == "income":
        df = ticker.financials
    elif table_type == "balance":
        df = ticker.balance_sheet
    else:
        df = ticker.cashflow
    
    # Return last 3 years
    return df.iloc[:, :3].to_json()

def handle_calculate_valuation(symbol):
    ticker = yf.Ticker(symbol)
    info = ticker.info
    
    # Simple Graham Formula: V = EPS * (8.5 + 2g)
    eps = info.get("trailingEps", 0)
    growth = info.get("earningsGrowth", 0.05) * 100 # percentage
    
    if eps <= 0:
        return {"error": "Negative or zero EPS, cannot calculate simple Graham valuation."}
    
    valuation = eps * (8.5 + 2 * min(growth, 15)) # Cap growth at 15% for conservative estimate
    
    return {
        "symbol": symbol,
        "current_price": info.get("currentPrice"),
        "graham_intrinsic_value": round(valuation, 2),
        "eps": eps,
        "assumed_growth": round(growth, 2),
        "status": "undervalued" if valuation > info.get("currentPrice", 0) else "overvalued"
    }

def handle_get_stock_news(symbol):
    ticker = yf.Ticker(symbol)
    news = ticker.news
    return [{"title": n.get("title"), "publisher": n.get("publisher"), "link": n.get("link")} for n in news[:5]]

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
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "FinanceBridge", "version": "1.0.0"}
                    }
                }
            elif method == "tools/list":
                response = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": list_tools()
                }
            elif method == "tools/call":
                tool_name = params.get("name")
                tool_args = params.get("arguments", {})
                
                try:
                    if tool_name == "get_stock_metrics":
                        result = handle_get_stock_metrics(tool_args.get("symbol"))
                    elif tool_name == "get_financial_statements":
                        result = handle_get_financial_statements(tool_args.get("symbol"), tool_args.get("type"))
                    elif tool_name == "calculate_valuation":
                        result = handle_calculate_valuation(tool_args.get("symbol"))
                    elif tool_name == "get_stock_news":
                        result = handle_get_stock_news(tool_args.get("symbol"))
                    else:
                        result = {"error": f"Unknown tool: {tool_name}"}
                    
                    response = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "result": {
                            "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False, indent=2)}]
                        }
                    }
                except Exception as e:
                    response = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32000, "message": str(e)}
                    }
            else:
                response = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": "Method not found"}
                }

            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()

        except Exception as e:
            # Silence errors in loop to prevent crashing, but log to stderr
            sys.stderr.write(f"Error: {str(e)}\n")
            sys.stderr.flush()

if __name__ == "__main__":
    main()
