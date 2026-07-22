"""Tool implementations for HyperFinanceBridge MCP server.

Keep JSON-RPC protocol code in mega_finance_bridge.py; put data, scoring,
readiness and report validation logic here so the MCP entrypoint stays small.
"""
import math
import asyncio
import re
import pandas as pd
import yfinance as yf
import sqlite3
import os
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = str(Path(__file__).resolve().parent)

from skills.buffett_scoring import run_full_buffett_analysis
from skills.finagent_strategies import run_all_strategies
from skills.finagent_reflection import compute_multi_timeframe_reflection

async def get_unified_ticker(symbol):
    """Returns a yfinance ticker object, wrapped in a local data adapter if available."""
    ticker = yf.Ticker(symbol)
    
    # Try to load local data
    local_annual = _get_local_financials(symbol, quarterly=False)
    local_quarterly = _get_local_financials(symbol, quarterly=True)
    
    if local_annual or local_quarterly:
        return LocalTickerAdapter(ticker, local_annual, local_quarterly)
    
    return ticker

# ═══════════════════════════════════════════════
#  Local Data Adapter & Priority Logic
# ═══════════════════════════════════════════════

AV_TO_YF_MAP = {
    "totalRevenue": "Total Revenue",
    "grossProfit": "Gross Profit",
    "operatingIncome": "Operating Income",
    "ebit": "EBIT",
    "netIncome": "Net Income",
    "netIncomeCommonStockholders": "Net Income Common Stockholders",
    "totalShareholderEquity": "Stockholders Equity",
    "depreciationAndAmortization": "Depreciation And Amortization",
    "capitalExpenditures": "Capital Expenditure",
    "dividendPayout": "Cash Dividends Paid",
    "proceedsFromRepurchaseOfEquity": "Repurchase Of Capital Stock",
    "commonStockRepurchased": "Common Stock Repurchased",
}

def _get_local_financials(symbol, quarterly=False):
    """Attempt to fetch financial statements from local AlphaVantage SQLite databases."""
    db_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "learn_st_list", "data", "alphavantage"))
    tables = "quarterly_reports" if quarterly else "annual_reports"
    
    res = {}
    statement_configs = [
        ("income", "income_statement.db"),
        ("balance", "balance_sheet.db"),
        ("cashflow", "cash_flow.db")
    ]
    
    for stmt_key, db_file in statement_configs:
        db_path = os.path.join(db_dir, db_file)
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tables,))
            if not cursor.fetchone():
                conn.close()
                return None
            
            df = pd.read_sql(f"SELECT * FROM {tables} WHERE symbol=?", conn, params=(symbol,))
            conn.close()
            
            if df.empty:
                return None
                
            df = df.set_index("fiscalDateEnding")
            if "symbol" in df.columns:
                df = df.drop(columns=["symbol"])
            
            raw_dict = df.to_dict(orient="index")
            processed_dict = {}
            for date, metrics in raw_dict.items():
                processed_metrics = {}
                for k, v in metrics.items():
                    yf_key = AV_TO_YF_MAP.get(k, k)
                    if v == "None":
                        processed_metrics[yf_key] = None
                    else:
                        try:
                            processed_metrics[yf_key] = float(v)
                        except (ValueError, TypeError):
                            processed_metrics[yf_key] = v
                processed_dict[str(date)] = processed_metrics
            res[stmt_key] = processed_dict
        except Exception:
            return None
            
    return res if len(res) == 3 else None

class LocalTickerAdapter:
    """Wraps yfinance Ticker to prioritize local AlphaVantage data."""
    def __init__(self, yf_ticker, local_annual, local_quarterly):
        self._yf = yf_ticker
        self.info = yf_ticker.info
        
        def dict_to_df(data_dict, key):
            if not data_dict or key not in data_dict: return None
            # pd.DataFrame({date: {metric: val}}) gives metrics as index, dates as columns
            return pd.DataFrame(data_dict[key])

        self.income_stmt = dict_to_df(local_annual, "income")
        if self.income_stmt is None: self.income_stmt = yf_ticker.income_stmt
        
        self.balance_sheet = dict_to_df(local_annual, "balance")
        if self.balance_sheet is None: self.balance_sheet = yf_ticker.balance_sheet
        
        self.cashflow = dict_to_df(local_annual, "cashflow")
        if self.cashflow is None: self.cashflow = yf_ticker.cashflow
        
        self.quarterly_income_stmt = dict_to_df(local_quarterly, "income")
        if self.quarterly_income_stmt is None: self.quarterly_income_stmt = yf_ticker.quarterly_income_stmt
        
        self.quarterly_balance_sheet = dict_to_df(local_quarterly, "balance")
        if self.quarterly_balance_sheet is None: self.quarterly_balance_sheet = yf_ticker.quarterly_balance_sheet
        
        self.quarterly_cashflow = dict_to_df(local_quarterly, "cashflow")
        if self.quarterly_cashflow is None: self.quarterly_cashflow = yf_ticker.quarterly_cashflow
        
        self._is_local = True

    def __getattr__(self, name):
        return getattr(self._yf, name)

    def history(self, *args, **kwargs):
        return self._yf.history(*args, **kwargs)

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

def _financial_df_to_dict(df):
    """Return yfinance financial frames with JSON-safe labels."""
    if df is None or df.empty:
        return {}
    out = df.copy()
    out.columns = [col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col) for col in out.columns]
    out.index = [str(idx) for idx in out.index]
    return out.to_dict()

def _safe_float(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        if math.isnan(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None

def _ticker_metric_summary(symbol):
    ticker = yf.Ticker(symbol)
    info = ticker.info or {}
    return {
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "roe": _safe_float(info.get("returnOnEquity")),
        "net_margin": _safe_float(info.get("profitMargins")),
        "revenue_growth": _safe_float(info.get("revenueGrowth")),
        "pe_ttm": _safe_float(info.get("trailingPE")),
        "pb": _safe_float(info.get("priceToBook")),
        "dividend_yield": _safe_float(info.get("dividendYield")),
        "market_cap": info.get("marketCap"),
        "currency": info.get("currency"),
        "data_status": "ok" if info else "missing",
    }

async def handle_get_financial_statements(symbol, quarterly=False):
    ticker = await get_unified_ticker(symbol)
    
    # Check if we are using local data (adapter)
    is_local = getattr(ticker, "_is_local", False)
    
    if quarterly:
        res = {
            "income": _financial_df_to_dict(ticker.quarterly_income_stmt),
            "balance": _financial_df_to_dict(ticker.quarterly_balance_sheet),
            "cashflow": _financial_df_to_dict(ticker.quarterly_cashflow)
        }
    else:
        res = {
            "income": _financial_df_to_dict(ticker.income_stmt),
            "balance": _financial_df_to_dict(ticker.balance_sheet),
            "cashflow": _financial_df_to_dict(ticker.cashflow)
        }
        
    if is_local:
        res["_source"] = "local_alphavantage"
        
    if symbol.upper().endswith((".SS", ".SH", ".SZ")):
        res["_WARNING_ASHARE_CAPEX"] = "A-Share detected. Automated CapEx (Capital Expenditures) data from API may erroneously merge 'Purchases of Intangible Assets' with 'Purchases of Property, Plant and Equipment'. You MUST manually search the company's annual report footnotes to verify the true Maintenance CapEx for Owner Earnings calculations."
        
    return res

async def handle_get_stock_price_history(symbol, period="6mo", interval="1d"):
    ticker = await get_unified_ticker(symbol)
    if hasattr(ticker, 'history') and asyncio.iscoroutinefunction(ticker.history):
        hist = await ticker.history(period=period, interval=interval)
    else:
        hist = ticker.history(period=period, interval=interval)
    
    if hist.empty: return {"error": "No data"}
    return hist.tail(30).to_dict()

async def handle_run_buffett_analysis(
    symbol,
    risk_free_rate=None,
    override_capex=None,
    override_growth=None,
    override_maintenance_capex=None,
):
    ticker = await get_unified_ticker(symbol)
    return run_full_buffett_analysis(
        ticker,
        risk_free_rate=risk_free_rate,
        override_capex=override_capex,
        override_growth=override_growth,
        override_maintenance_capex=override_maintenance_capex,
    )

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

async def handle_get_peer_comparison(symbol, peers=None):
    """Return valuation and profitability metrics for the target and selected peers."""
    if isinstance(peers, str):
        peers = [p.strip() for p in peers.split(",") if p.strip()]
    peers = peers or []
    symbols = [symbol] + [p for p in peers if p and p != symbol]
    rows = []
    errors = []
    for sym in symbols:
        try:
            rows.append(_ticker_metric_summary(sym))
        except Exception as exc:
            errors.append({"symbol": sym, "error": str(exc)})

    target = rows[0] if rows else {}
    return {
        "symbol": symbol,
        "peers_requested": peers,
        "peer_selection_status": "user_supplied" if peers else "missing_peers",
        "peer_selection_note": (
            "No peers supplied; pass peers=[...] for strict report table values."
            if not peers else
            "Peer list supplied by caller."
        ),
        "target_sector": target.get("sector"),
        "target_industry": target.get("industry"),
        "metrics": rows,
        "errors": errors,
    }

def _normalize_market(market):
    market = (market or "").upper()
    aliases = {
        "US": "USA",
        "USA": "USA",
        "NYSE": "USA",
        "NASDAQ": "USA",
        "CN": "CHN",
        "CHINA": "CHN",
        "CHN": "CHN",
        "A": "CHN",
        "HK": "HKG",
        "HKG": "HKG",
        "HONGKONG": "HKG",
        "TAIWAN": "TWN",
        "TW": "TWN",
        "TWN": "TWN",
    }
    return aliases.get(market, market or "UNKNOWN")


def _market_from_symbol(symbol):
    symbol = (symbol or "").upper()
    if symbol.endswith((".SS", ".SH", ".SZ")):
        return "CHN"
    if symbol.endswith(".HK"):
        return "HKG"
    if symbol.endswith((".TW", ".TWO")):
        return "TWN"
    return "USA"

async def handle_get_macro_context(market):
    """Return macro context placeholders without fabricating missing rates."""
    normalized = _normalize_market(market)
    rate_sources = {
        "USA": "US 10Y Treasury yield",
        "CHN": "China 10Y Government Bond yield",
        "HKG": "Hong Kong 10Y Government Bond yield",
        "TWN": "Taiwan 10Y Government Bond yield",
    }
    return {
        "market": normalized,
        "risk_free_rate": None,
        "risk_free_rate_status": "missing",
        "source_required": rate_sources.get(normalized, "Relevant local 10Y sovereign yield"),
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "note": "Bridge does not fetch live macro rates; caller must supply/search an official source before Graham rate adjustment.",
    }

def _statement_payload_status(result):
    if not isinstance(result, dict):
        return False, ["invalid_payload"]
    missing = [name for name in ("income", "balance", "cashflow") if not result.get(name)]
    return not missing, missing


async def handle_run_report_readiness_check(symbol, report_path=None, peers=None):
    """Summarize data availability before writing a report."""
    checks = {}
    missing = []
    calls = [
        ("valuation", handle_get_stock_valuation(symbol)),
        ("annual_financial_statements", handle_get_financial_statements(symbol, quarterly=False)),
        ("quarterly_financial_statements", handle_get_financial_statements(symbol, quarterly=True)),
        ("buffett_analysis", handle_run_buffett_analysis(symbol)),
        ("strategy_scan", handle_run_finagent_strategy_scan(symbol)),
        ("reflection", handle_run_finagent_reflection(symbol)),
        ("peer_comparison", handle_get_peer_comparison(symbol, peers)),
    ]
    results = await asyncio.gather(*(coro for _, coro in calls), return_exceptions=True)

    for (name, _), result in zip(calls, results):
        if isinstance(result, Exception):
            checks[name] = {"status": "error", "summary": str(result)}
            missing.append(name)
            continue
        if isinstance(result, dict) and result.get("error"):
            checks[name] = {"status": "missing", "summary": result.get("error")}
            missing.append(name)
            continue

        if name in {"annual_financial_statements", "quarterly_financial_statements"}:
            ok, missing_statements = _statement_payload_status(result)
            checks[name] = {
                "status": "ok" if ok else "missing",
                "missing_statements": missing_statements,
                "source": result.get("_source", "automatic_api") if isinstance(result, dict) else None,
            }
            if not ok:
                missing.append(name)
        elif name == "valuation":
            required = [field for field in ("price", "marketCap") if result.get(field) is None]
            checks[name] = {"status": "ok" if not required else "missing", "missing_fields": required}
            if required:
                missing.append(name)
        elif name == "buffett_analysis":
            machine_status = result.get("machine_score", {}).get("status")
            extended_status = result.get("extended_diagnostics", {}).get("status")
            checks[name] = {
                "status": "ok" if machine_status == "complete" else "missing",
                "machine_score_status": machine_status,
                "extended_diagnostics_status": extended_status,
                "growth_source": result.get("growth_assumption", {}).get("source"),
            }
            if machine_status != "complete":
                missing.append(name)
        elif name == "peer_comparison":
            supplied = result.get("peer_selection_status") == "user_supplied"
            checks[name] = {
                "status": "ok" if supplied and result.get("metrics") else "needs_manual_evidence",
                "peer_selection_status": result.get("peer_selection_status"),
                "metrics_count": len(result.get("metrics", [])),
            }
        else:
            checks[name] = {"status": "ok"}

    market = _market_from_symbol(symbol)
    checks["macro_context"] = {"status": "needs_external_rate", "summary": await handle_get_macro_context(market)}
    checks["external_evidence"] = {
        "status": "required",
        "required_items": [
            "official latest-quarter evidence and all current-fiscal-year quarters",
            "same-currency risk-free rate or documented proxy basis",
            "core KPI trend and guidance",
            "peer/analyst evidence",
            "six-month catalyst and report-comparability scan",
        ],
    }
    if report_path:
        checks["report_validation"] = await handle_validate_report(report_path)

    return {
        "symbol": symbol,
        "market": market,
        "status": "ready_with_manual_evidence" if not missing else "incomplete",
        "missing_bridge_data": missing,
        "checks": checks,
    }

def _read_report(report_path):
    path = Path(report_path)
    if not path.is_absolute():
        path = Path(BASE_DIR) / path
    if not path.exists():
        return path, None, f"Report not found: {path}"
    return path, path.read_text(encoding="utf-8", errors="ignore"), None

def _extract_percentages_near(text, marker):
    idx = text.find(marker)
    if idx == -1:
        return []
    end_candidates = [pos for pos in [text.find("\n---", idx), text.find("\n## ", idx + 1), text.find("\n### ", idx + 1)] if pos != -1]
    end = min(end_candidates) if end_candidates else idx + 2000
    window = text[idx:end]
    values = []
    for line in window.splitlines():
        if "|" not in line:
            continue
        if not re.search(r"Bull Case|Base Case|Bear Case|乐观|中性|悲观", line, re.IGNORECASE):
            continue
        cells = [c.strip() for c in line.split("|")]
        if len(cells) < 3:
            continue
        match = re.search(r"(\d+(?:\.\d+)?)\s*%", cells[2])
        if match:
            values.append(float(match.group(1)))
    return values


def _expected_scenario_probabilities(macro_score, quality_score, trend_score):
    """Return bounded Bull/Base/Bear probabilities from SOP V2 normalized inputs."""
    m = max(0.0, min(float(macro_score) / 15.0, 1.0))
    q = max(0.0, min(float(quality_score) / 27.0, 1.0))
    e = max(0.0, min(float(trend_score) / 10.0, 1.0))
    bull = 20 + 20 * m + 10 * q + 10 * e
    bear = 10 + 20 * (1 - m) + 20 * (1 - q) + 10 * (1 - e)
    base = 100 - bull - bear
    return [bull, base, bear]

def _section_between(text, start_marker, end_marker=None):
    start = text.find(start_marker)
    if start == -1:
        return ""
    if end_marker is None:
        return text[start:]
    end = text.find(end_marker, start + len(start_marker))
    return text[start:] if end == -1 else text[start:end]

def _table_lines(section):
    return [
        line.strip()
        for line in section.splitlines()
        if line.strip().startswith("|") and not re.match(r"^\|\s*:?-{3,}", line.strip())
    ]


def _blank_table_rows(section):
    blank = []
    for line in _table_lines(section):
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if cells and any(cell == "" for cell in cells):
            blank.append(line)
    return blank

def _line_has_signal(line):
    return bool(re.search(r"\|\s*(BUY|HOLD|SELL|ERROR|N/A|NA|不适用)\s*(?:\||$)", line, re.IGNORECASE))

def _find_missing_terms(section, terms):
    missing = []
    for label, patterns in terms:
        if not any(re.search(pattern, section, re.IGNORECASE) for pattern in patterns):
            missing.append(label)
    return missing

def _validate_required_table_terms(section, terms, issue_type, min_rows=None):
    table_rows = _table_lines(section)
    data_rows = [line for line in table_rows if not re.search(r"指标|触发标准|策略|情景|年份|维度", line)]
    missing = _find_missing_terms(section, terms)
    issues = []
    if missing:
        issues.append({"type": issue_type, "details": {"missing": missing}})
    if min_rows is not None and len(data_rows) < min_rows:
        issues.append({"type": f"{issue_type}_row_count", "details": f"Expected at least {min_rows} data rows, found {len(data_rows)}."})
    return issues

def _validate_technical_strategy_rows(section_10):
    expected = [
        ("MACD Crossover", [r"MACD"]),
        ("KDJ + RSI", [r"KDJ.*RSI", r"RSI.*KDJ"]),
        ("Stochastic + BB", [r"Stochastic", r"Bollinger", r"\bBB\b"]),
        ("Mean Reversion", [r"Mean Reversion", r"均值回归"]),
        ("ATR Volatility", [r"ATR", r"Volatility", r"波动率"]),
    ]
    rows = _table_lines(section_10)
    strategy_rows = [
        row for row in rows
        if any(re.search(pattern, row, re.IGNORECASE) for _, patterns in expected for pattern in patterns)
    ]
    missing = []
    missing_signal = []
    for label, patterns in expected:
        matching = [row for row in strategy_rows if any(re.search(pattern, row, re.IGNORECASE) for pattern in patterns)]
        if not matching:
            missing.append(label)
        elif not any(_line_has_signal(row) for row in matching):
            missing_signal.append(label)

    issues = []
    if missing or len(strategy_rows) < 5:
        issues.append({
            "type": "technical_strategy_rows_missing",
            "details": {
                "expected": 5,
                "found": len(strategy_rows),
                "missing": missing,
            },
        })
    if missing_signal:
        issues.append({
            "type": "technical_strategy_signal_missing",
            "details": {"missing_signal": missing_signal},
        })
    return issues

async def handle_validate_report(report_path):
    """Validate SOP V2 report structure, numeric logic and hard-rule signals."""
    path, text, error = _read_report(report_path)
    if error:
        return {"status": "error", "path": str(path), "issues": [error]}

    issues = []
    warnings = []

    if path.parent.name != "reports":
        issues.append({"type": "report_directory", "details": "Final reports must be stored directly in ./reports/."})
    if not re.fullmatch(r"[A-Za-z0-9.^-]+_analysis_\d{8}\.md", path.name):
        issues.append({"type": "report_filename", "details": "Expected symbol_analysis_YYYYMMDD.md using a normalized ticker."})

    if not re.search(r"SOP\s*版本\*{0,2}:\s*V2\.0", text, re.IGNORECASE):
        issues.append({"type": "sop_version_missing", "details": "Report must declare SOP version V2.0."})

    placeholder_pattern = re.compile(
        r"\[(?:XX?|X(?:\.X+)?|金额|价格|市值|数值(?:/N/A)?|百分比(?:/N/A)?|"
        r"分数(?:/N/A)?|货币|货币代码|公司名称|股票代码|"
        r"YYYY(?:-MM-DD(?: HH:MM 时区)?|Q[1-4](?:/N/A)?)?|评级|模型|建议|判定|现价|"
        r"算出的目标价|加权值|主模型价值|压力值|本公司|同行[A-Z]|信号|说明|趋势|"
        r"核心优势\d+)\]"
        r"|\[[^\]]*(?:填写|列出|简述|为何|逐项|触发条件|维持现状|宏观恶化|"
        r"建议详情|要求持股|事实证据|逻辑简述|评分影响|给出结论)[^\]]*\]",
        re.IGNORECASE,
    )
    placeholders = sorted(set(placeholder_pattern.findall(text)))
    if placeholders:
        issues.append({"type": "unresolved_placeholders", "details": placeholders[:30]})

    required_sections = [f"## **{i}." for i in range(1, 14)]
    missing_sections = [s for s in required_sections if s not in text]
    if missing_sections:
        issues.append({"type": "missing_sections", "details": missing_sections})

    required_markers = [
        "### **5.1 护城河",
        "### **5.2 管理层",
        "### **5.3 同行对比",
        "### **6.1 主人盈余",
        "### **6.2 内在价值",
        "### **6.3 量化评分表",
        "### **6.4 核心财务指标",
        "#### **6.4.1 当前年度季度序列",
        "### **6.5 核心业务 KPI",
        "### **6.6 利润纯度",
        "### **7.1 增长驱动力",
        "### **7.2 第二曲线",
        "### **8.1 股价驱动",
        "### **8.2 上行与下行",
        "### **9.1 折价体系",
        "### **9.2 估值逻辑",
        "### **9.3 多维度",
        "### **9.4 行业特定",
        "### **9.5 绝对与相对",
        "### **9.6 全收益投影",
        "### **12.1 核心风险",
        "### **12.2 核心机会",
        "### **分析师共识",
    ]
    missing_markers = [m for m in required_markers if m not in text]
    if missing_markers:
        issues.append({"type": "missing_required_subsections", "details": missing_markers})

    if "/27" not in text or "扩展诊断" not in text:
        issues.append({"type": "buffett_score_basis", "details": "Expected commercial-quality /27 plus separately labeled extended diagnostics."})

    headline_unrated = bool(re.search(r"\[\[Rating\s*/\s*评级:\s*暂不评级\]\]", text, re.IGNORECASE))
    scenario_percents = _extract_percentages_near(text, "Scenario Analysis")
    if not scenario_percents:
        scenario_percents = _extract_percentages_near(text, "情景")
    if len(scenario_percents) == 3:
        total = sum(scenario_percents[:3])
        if abs(total - 100) > 0.5:
            issues.append({"type": "scenario_probability_sum", "details": f"First three scenario probabilities sum to {total:.1f}%, expected 100%."})
        if min(scenario_percents) < 0 or scenario_percents[2] < 10:
            issues.append({"type": "scenario_probability_bounds", "details": "Probabilities must be non-negative and Bear Case must be at least 10%."})
    elif not headline_unrated:
        issues.append({"type": "scenario_probability_unverified", "details": "Exactly three numeric scenario probabilities are required."})

    section_10 = text[text.find("## **10."):text.find("## **11.")] if "## **10." in text and "## **11." in text else ""
    section_1 = text[text.find("## **1."):text.find("## **2.")] if "## **1." in text and "## **2." in text else ""
    section_5_3 = _section_between(text, "### **5.3", "## **6.")
    section_6_1 = _section_between(text, "### **6.1", "### **6.2")
    section_6_2 = _section_between(text, "### **6.2", "### **6.3")
    section_6_3 = _section_between(text, "### **6.3", "### **6.4")
    section_6_4 = _section_between(text, "### **6.4", "### **6.5")
    section_6_5 = _section_between(text, "### **6.5", "### **6.6")
    section_6_6 = _section_between(text, "### **6.6", "## **7.")
    section_9_6 = _section_between(text, "### **9.6", "## **10.")
    section_11 = _section_between(text, "## **11.", "## **12.")
    section_sources = _section_between(text, "## **资料来源与二次校验")
    no_current_quarter = re.search(r"当前年度尚无季报|尚未发布.*季报|尚无.*季度", section_6_4)

    table_sections = {
        "5.1 moat": _section_between(text, "### **5.1", "### **5.2"),
        "5.2 management": _section_between(text, "### **5.2", "### **5.3"),
        "5.3 peers": section_5_3,
        "6.3 score": section_6_3,
        "6.4 financials": section_6_4,
        "6.5 KPI": section_6_5,
        "7.1 growth": _section_between(text, "### **7.1", "### **7.2"),
        "8.1 drivers": _section_between(text, "### **8.1", "### **8.2"),
        "8.2 scenarios": _section_between(text, "### **8.2", "## **9."),
        "9.2 valuation history": _section_between(text, "### **9.2", "### **9.3"),
        "9.3 valuation methods": _section_between(text, "### **9.3", "### **9.4"),
        "9.4 sector references": _section_between(text, "### **9.4", "### **9.5"),
        "9.6 total return": section_9_6,
        "10 technical": section_10,
        "11 sell standards": section_11,
    }
    blank_tables = {label: rows[:3] for label, section in table_sections.items() if (rows := _blank_table_rows(section))}
    if blank_tables:
        issues.append({"type": "blank_required_table_cells", "details": blank_tables})

    issues.extend(_validate_technical_strategy_rows(section_10))
    issues.extend(_validate_required_table_terms(
        section_5_3,
        [
            ("ROE", [r"\bROE\b"]),
            ("净利率", [r"净利率", r"Net Margin"]),
            ("营收增速", [r"营收增速", r"Revenue Growth"]),
            ("PE (TTM)", [r"\bPE\b", r"P/E"]),
            ("PB", [r"\bPB\b", r"P/B"]),
            ("股息率", [r"股息率", r"Dividend Yield"]),
        ],
        "peer_comparison_rows_missing",
        min_rows=6,
    ))
    issues.extend(_validate_required_table_terms(
        section_6_4,
        [
            ("营收", [r"营收", r"Revenue"]),
            ("净利润", [r"净利润", r"Net Income"]),
            ("经营现金流", [r"经营现金流", r"Operating Cash"]),
            ("自由现金流", [r"自由现金流", r"Free Cash"]),
            ("净利率", [r"净利率", r"Net Margin"]),
            ("ROE", [r"\bROE\b"]),
            ("D/E", [r"\bD/E\b", r"Debt"]),
            ("BVPS", [r"\bBVPS\b"]),
            ("营业利润率", [r"营业利润率", r"Operating Margin"]),
            ("毛利率", [r"毛利率", r"Gross Margin"]),
            ("股息+回购", [r"股息\+回购", r"Dividend.*Buyback"]),
            ("流通股", [r"流通股", r"Shares"]),
        ],
        "financial_snapshot_rows_missing",
        min_rows=12,
    ))

    issues.extend(_validate_required_table_terms(
        section_6_5,
        [
            ("年-4", [r"\[年-4\]", r"20\d\d"]),
            ("历史年份", [r"\[年-3\]", r"\[年-2\]", r"\[年-1\]", r"20\d\d"]),
            ("最新年", [r"\[最新年\]", r"20\d\d"]),
            ("最新季", [r"\[最新季\]", r"20\d\dQ\d"]),
            ("指引", [r"\[指引\]", r"指引", r"Guidance"]),
        ],
        "kpi_trend_rows_missing",
        min_rows=5,
    ))

    if not all(str(i) in section_6_6 for i in ["1", "2", "3"]):
        issues.append({"type": "profit_purity_missing_answers", "details": "Section 6.6 must list at least 3 numbered points answering purity questions."})

    if not re.search(r"采用路径:\s*[AB]", section_9_6) and not (headline_unrated and re.search(r"采用路径:\s*N/A", section_9_6, re.IGNORECASE)):
        issues.append({"type": "total_return_path_missing", "details": "Section 9.6 must explicitly select path A or B."})
    if not re.search(r"总预期回报.*=.*%", section_9_6) and not re.search(r"总预期(?:年化)?回报.*%.*", section_9_6):
        issues.append({"type": "total_return_formula_missing", "details": "Section 9.6 is missing the explicit 3-factor expected return formula computation."})
    issues.extend(_validate_required_table_terms(
        section_11,
        [
            ("宏观逻辑崩塌", [r"宏观逻辑崩塌"]),
            ("价格远超价值", [r"价格远超", r"价值"]),
            ("护城河被侵蚀", [r"护城河被侵蚀", r"护城河"]),
            ("管理层失信", [r"管理层失信", r"资本错配"]),
            ("增长引擎熄火", [r"增长引擎熄火", r"增长引擎"]),
            ("发现更好的机会成本", [r"发现更好的机会成本", r"机会成本"]),
        ],
        "sell_standard_rows_missing",
        min_rows=6,
    ))

    if "评级依据" not in section_1 and "Rating Basis" not in section_1:
        issues.append({"type": "rating_basis_missing", "details": "Section 1 must include a Rating Basis block showing Step 1 status, coverage and composite score breakdown per rating_criteria.md."})
    else:
        rating_match = re.search(r"\[\[Rating\s*/\s*评级:\s*([^\]]+)\]\]", section_1, re.IGNORECASE)
        status_match = re.search(r"数据状态:\s*(正常|低置信度|暂不评级)", section_1)
        score_match = re.search(r"综合评分:\s*\*{0,2}(\d{1,3}|N/A)/100", section_1, re.IGNORECASE)
        margin_match = re.search(r"主估值模型:[^\n|]+\|\s*安全边际:\s*(N/A|-?\d+(?:\.\d+)?)%", section_1, re.IGNORECASE)
        tech_match = re.search(r"技术面约束:\s*(none|timing_caution_only|hard_cap_hold_or_watch|unknown)", section_1)
        step1_match = re.search(r"Step 1 硬性排除:\s*([^\n<]+)", section_1)
        coverage_match = re.search(r"score_coverage:\s*(\d+(?:\.\d+)?)%", section_1)
        transition_match = re.search(r"基础评级\s*→\s*最终评级:\s*([^\s]+)\s*→\s*([^\s\n]+)", section_1)
        component_match = re.search(
            r"估值\s*(\d+|N/A)/30\s*\+\s*品质\s*(\d+|N/A)/25\s*\+\s*回报\s*(\d+|N/A)/20\s*\+\s*宏观\s*(\d+|N/A)/15\s*\+\s*趋势\s*(\d+|N/A)/10",
            section_1,
            re.IGNORECASE,
        )

        required_rating_fields = [rating_match, status_match, score_match, margin_match, tech_match, step1_match, coverage_match, transition_match, component_match]
        if not all(required_rating_fields):
            issues.append({"type": "rating_basis_incomplete", "details": "Rating Basis must include coverage, Step 1, primary-model margin, five score components, technical constraint and base-to-final transition."})
        else:
            rating = rating_match.group(1).strip()
            score_raw = score_match.group(1).upper()
            margin_raw = margin_match.group(1).upper()
            tech = tech_match.group(1)
            step1 = step1_match.group(1)
            data_status = status_match.group(1)
            coverage = float(coverage_match.group(1))
            base_rating, final_rating = transition_match.group(1), transition_match.group(2)
            component_values = [value.upper() for value in component_match.groups()]
            is_unrated = headline_unrated or data_status == "暂不评级"

            if not 0 <= coverage <= 100:
                issues.append({"type": "score_coverage_bounds", "details": "score_coverage must be between 0% and 100%."})
            if coverage < 60 or is_unrated:
                if "暂不评级" not in rating or "暂不评级" not in base_rating or "暂不评级" not in final_rating:
                    issues.append({"type": "coverage_rating_gate", "details": "Insufficient data requires 暂不评级 in headline and transition."})
                if data_status != "暂不评级":
                    issues.append({"type": "data_status_mismatch", "details": "An unrated report must declare 数据状态: 暂不评级."})
            elif "价值陷阱" in step1:
                if "价值陷阱" not in rating or "价值陷阱" not in final_rating:
                    issues.append({"type": "hard_filter_rating", "details": "Value-trap hard filter must produce 价值陷阱."})
            elif "强制卖出" in step1:
                if "卖出" not in rating or "卖出" not in final_rating:
                    issues.append({"type": "hard_filter_rating", "details": "Forced-sell hard filter must produce 卖出."})
            elif "N/A" in {score_raw, margin_raw, *component_values}:
                issues.append({"type": "numeric_rating_inputs_missing", "details": "A rated report requires numeric score, primary-model margin and all five components."})
            else:
                score = int(score_raw)
                margin = float(margin_raw)
                components = [int(value) for value in component_values]
                component_caps = [30, 25, 20, 15, 10]
                if not 0 <= score <= 100 or any(not 0 <= value <= cap for value, cap in zip(components, component_caps)):
                    issues.append({"type": "composite_score_bounds", "details": "Score or a component exceeds its permitted range."})
                if sum(components) != score:
                    issues.append({"type": "composite_score_sum", "details": f"Components sum to {sum(components)}, but stated score is {score}."})
                if (60 <= coverage < 80 or no_current_quarter) and data_status != "低置信度":
                    issues.append({"type": "data_status_mismatch", "details": "Coverage from 60% to 79%, or absence of a current-year quarter, must declare 低置信度."})

                if "未触发" in step1:
                    if score >= 80 and margin >= 25 and coverage >= 80:
                        expected_base = "强烈买入"
                    elif score >= 65 and margin >= 10 and coverage >= 80:
                        expected_base = "买入"
                    elif score >= 50:
                        expected_base = "持有"
                    elif score >= 35:
                        expected_base = "观望"
                    else:
                        expected_base = "卖出"
                    if 60 <= coverage < 80 and expected_base in {"强烈买入", "买入"}:
                        expected_base = "持有"
                    if no_current_quarter and expected_base in {"强烈买入", "买入"}:
                        expected_base = "持有"

                    expected_final = expected_base
                    if tech == "hard_cap_hold_or_watch":
                        if expected_base in {"强烈买入", "买入"}:
                            expected_final = "持有"
                        elif expected_base == "持有":
                            expected_final = "观望"

                    if base_rating != expected_base or final_rating != expected_final or rating != expected_final:
                        issues.append({
                            "type": "rating_mismatch",
                            "details": f"Expected {expected_base} → {expected_final}, got {base_rating} → {final_rating} and headline {rating}.",
                        })
                macro_match = re.search(r"宏观总分\*{0,2}:\s*\*{0,2}(\d+(?:\.\d+)?)/15", text)
                quality_match = re.search(r"商业品质基础分\s*(\d+(?:\.\d+)?)/27", section_6_3)
                if len(scenario_percents) == 3 and macro_match and quality_match:
                    expected_probs = _expected_scenario_probabilities(
                        float(macro_match.group(1)),
                        float(quality_match.group(1)),
                        components[4],
                    )
                    if any(abs(actual - expected) > 0.6 for actual, expected in zip(scenario_percents, expected_probs)):
                        issues.append({"type": "scenario_probability_formula", "details": {"actual": scenario_percents, "expected": [round(v, 1) for v in expected_probs]}})

    structured_constraint = re.search(r"rating_constraint:\s*(none|timing_caution_only|hard_cap_hold_or_watch|unknown)", section_10)
    primary_trend = re.search(r"primary_trend:\s*(bullish|bearish|sideways|mixed|unknown)", section_10)
    adx = re.search(r"ADX:\s*(?:-?\d+(?:\.\d+)?|N/A)", section_10, re.IGNORECASE)
    trend_strength = re.search(r"trend_strength:\s*(weak/range-bound|developing trend|strong trend|very strong trend|unknown)", section_10)
    if not all([structured_constraint, primary_trend, adx, trend_strength]):
        issues.append({"type": "technical_constraint_fields", "details": "Section 10 must include primary_trend, ADX, trend_strength and rating_constraint."})

    if (not re.search(r"20\d{2}Q[1-4]", section_6_4) or not re.search(r"YoY|同比", section_6_4, re.IGNORECASE)) and not no_current_quarter:
        issues.append({"type": "latest_quarter_missing", "details": "Section 6.4 must identify a latest fiscal quarter and YoY comparison, or explicitly explain that no current-year quarter exists."})

    if "主人盈余 (Owner Earnings):" in section_6_1 and re.search(r"主人盈余 \(Owner Earnings\):[^\n]*N/A", section_6_1, re.IGNORECASE):
        owner_score_rows = [line for line in _table_lines(section_6_3) if "主人盈余" in line]
        if owner_score_rows and not any("N/A" in line for line in owner_score_rows):
            issues.append({"type": "missing_owner_earnings_scored", "details": "Owner Earnings is N/A but its extended diagnostic row contains a fabricated score."})

    if not headline_unrated:
        quality_labels = ["基本面状况", "盈利一致性", "竞争护城河", "管理层品质", "账面价值增长", "通胀/成本转嫁力"]
        incomplete_quality_rows = []
        for label in quality_labels:
            row = next((line for line in _table_lines(section_6_3) if label in line), "")
            if not row or not re.search(rf"{re.escape(label)}.*\|\s*\d+(?:\.\d+)?\s*\|", row):
                incomplete_quality_rows.append(label)
        if incomplete_quality_rows:
            issues.append({"type": "rated_quality_inputs_missing", "details": incomplete_quality_rows})
        if re.search(r"主模型价值:[^\n]*N/A|主模型安全边际:[^\n]*N/A", section_6_2, re.IGNORECASE):
            issues.append({"type": "rated_primary_valuation_missing", "details": "A rated report cannot leave primary-model value or margin as N/A."})
        if re.search(r"总预期年化回报:[^\n]*N/A", section_9_6, re.IGNORECASE):
            issues.append({"type": "rated_total_return_missing", "details": "A rated report cannot leave total expected annual return as N/A."})

    if not re.search(r"主估值模型", section_6_2) or not re.search(r"适用性说明", section_6_2):
        issues.append({"type": "primary_valuation_model_missing", "details": "Section 6.2 must identify and justify the industry-appropriate primary valuation model."})

    scenario_section = _section_between(text, "### **8.2", "## **9.")
    scenario_rows = [row for row in _table_lines(scenario_section) if re.search(r"Bull Case|Base Case|Bear Case|乐观|中性|悲观", row, re.IGNORECASE)]
    for row in scenario_rows:
        if headline_unrated and "N/A" in row:
            continue
        if not re.search(r"×|\*", row) or not re.search(r"历史|同行|分位|指引|依据", row):
            issues.append({"type": "scenario_multiple_basis", "details": "Each scenario row needs a per-share formula and historical/peer/guidance basis for its multiple."})
            break

    # A-Share CapEx check
    is_a_share = bool(re.search(r"\.(SS|SZ|SH)_", path.name, re.IGNORECASE))
    if is_a_share:
        has_asset_split = re.search(r"固定资产.*无形资产|无形资产.*固定资产", section_6_1)
        has_maintenance_split = re.search(r"维护性.*增长性|增长性.*维护性", section_6_1)
        has_footnote = re.search(r"附注", section_6_1)
        if not all([has_asset_split, has_maintenance_split, has_footnote]):
            issues.append({"type": "a_share_capex_verification_missing", "details": "A-Share Section 6.1 must document footnote evidence, fixed/intangible split, and maintenance/growth split separately."})

    if not re.search(r"近\s*6\s*个月重大事件预检", section_sources) or not re.search(r"访问日期|检索范围|未发现|分拆|并购|重组|融资|处罚", section_sources):
        issues.append({"type": "catalyst_scan_missing", "details": "Sources must document the six-month catalyst/report-comparability scan and its evidence date or conclusion."})

    if not re.search(r"评分覆盖率|score_coverage", section_sources, re.IGNORECASE):
        issues.append({"type": "missing_data_audit", "details": "Sources must list missing data and its score-coverage impact."})

    if "股息率" in text and "OE Yield" in text and "倒挂" not in text and "正常" not in text:
        warnings.append({"type": "yield_consistency_unclear", "details": "Dividend yield vs OE Yield check exists but result wording is unclear."})

    status = "pass" if not issues else "fail"
    return {
        "status": status,
        "path": str(path),
        "issues": issues,
        "warnings": warnings,
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
