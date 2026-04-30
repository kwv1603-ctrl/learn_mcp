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
    db_dir = "/Users/dap/Documents/work/project/python/finance/learn_st_list/data/alphavantage"
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

async def handle_run_buffett_analysis(symbol, risk_free_rate=None, override_capex=None, override_growth=None):
    ticker = await get_unified_ticker(symbol)
    return run_full_buffett_analysis(
        ticker,
        risk_free_rate=risk_free_rate,
        override_capex=override_capex,
        override_growth=override_growth
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

async def handle_run_report_readiness_check(symbol, report_path=None):
    """Summarize data availability before writing a report."""
    checks = {}
    missing = []
    for name, coro in [
        ("valuation", handle_get_stock_valuation(symbol)),
        ("financial_statements", handle_get_financial_statements(symbol)),
        ("buffett_analysis", handle_run_buffett_analysis(symbol)),
        ("strategy_scan", handle_run_finagent_strategy_scan(symbol)),
        ("reflection", handle_run_finagent_reflection(symbol)),
        ("peer_comparison", handle_get_peer_comparison(symbol)),
    ]:
        try:
            result = await coro
            ok = not (isinstance(result, dict) and result.get("error"))
            checks[name] = {"status": "ok" if ok else "missing", "summary": result.get("error") if isinstance(result, dict) else None}
            if not ok:
                missing.append(name)
        except Exception as exc:
            checks[name] = {"status": "error", "summary": str(exc)}
            missing.append(name)

    market = "CHN" if symbol.upper().endswith((".SS", ".SH", ".SZ")) else "USA"
    checks["macro_context"] = {"status": "needs_external_rate", "summary": await handle_get_macro_context(market)}
    checks["external_evidence"] = {
        "status": "required",
        "summary": "Use environment search/browser for KPI trend, official filings, regulatory events, and analyst/peer evidence.",
    }
    if report_path:
        checks["report_validation"] = await handle_validate_report(report_path)

    return {
        "symbol": symbol,
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
    """Validate the report template structure and hard-rule signals."""
    path, text, error = _read_report(report_path)
    if error:
        return {"status": "error", "path": str(path), "issues": [error]}

    issues = []
    warnings = []
    required_sections = [f"## **{i}." for i in range(1, 14)]
    missing_sections = [s for s in required_sections if s not in text]
    if missing_sections:
        issues.append({"type": "missing_sections", "details": missing_sections})

    required_markers = [
        "### **5.3 同行对比",
        "### **6.1 主人盈余",
        "### **6.2 内在价值",
        "### **6.3 量化评分表",
        "### **6.4 核心财务指标",
        "### **6.5 核心业务 KPI",
        "### **6.6 利润纯度",
        "### **9.6 全收益投影",
        "### **分析师共识",
    ]
    missing_markers = [m for m in required_markers if m not in text]
    if missing_markers:
        issues.append({"type": "missing_required_subsections", "details": missing_markers})

    if "/27" not in text or "/35" not in text:
        issues.append({"type": "buffett_score_denominator", "details": "Expected both /27 machine score and /35 adjusted score."})

    scenario_percents = _extract_percentages_near(text, "Scenario Analysis")
    if not scenario_percents:
        scenario_percents = _extract_percentages_near(text, "情景")
    if len(scenario_percents) >= 3:
        total = sum(scenario_percents[:3])
        if abs(total - 100) > 0.5:
            issues.append({"type": "scenario_probability_sum", "details": f"First three scenario probabilities sum to {total:.1f}%, expected 100%."})
        if scenario_percents[2] < 15:
            issues.append({"type": "bear_case_probability", "details": "Bear case probability appears below 15%."})
    else:
        warnings.append({"type": "scenario_probability_unverified", "details": "Could not find three scenario probabilities."})

    section_10 = text[text.find("## **10."):text.find("## **11.")] if "## **10." in text and "## **11." in text else ""
    section_1 = text[text.find("## **1."):text.find("## **2.")] if "## **1." in text and "## **2." in text else ""
    section_5_3 = _section_between(text, "### **5.3", "## **6.")
    section_6_4 = _section_between(text, "### **6.4", "### **6.5")
    section_6_5 = _section_between(text, "### **6.5", "### **6.6")
    section_6_6 = _section_between(text, "### **6.6", "## **7.")
    section_9_6 = _section_between(text, "### **9.6", "## **10.")
    section_11 = _section_between(text, "## **11.", "## **12.")

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
            ("指引", [r"\[指引\]", r"指引", r"Guidance"]),
        ],
        "kpi_trend_rows_missing",
        min_rows=4,
    ))

    if not all(str(i) in section_6_6 for i in ["1", "2", "3"]):
        issues.append({"type": "profit_purity_missing_answers", "details": "Section 6.6 must list at least 3 numbered points answering purity questions."})

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

    if re.search(r"bearish|熊市|空头|downtrend", section_10, re.IGNORECASE):
        if re.search(r"强烈买入|买入|Strong Buy|Buy", section_1) and not re.search(r"观望|持有|Watch|Hold", section_1, re.IGNORECASE):
            issues.append({"type": "bearish_rating_constraint", "details": "Bearish technical section conflicts with Buy-style top rating."})

    # A-Share CapEx check
    is_a_share = bool(re.search(r"\.(SS|SZ|SH)_", path.name, re.IGNORECASE))
    if is_a_share:
        section_6_1 = _section_between(text, "### **6.1", "### **6.2")
        if not re.search(r"附注|合并|手工|拆分", section_6_1):
            issues.append({"type": "a_share_capex_verification_missing", "details": "A-Share detected, but Section 6.1 lacks proof of manual CapEx footnote verification (missing keywords like 附注, 合并, 拆分)."})

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
