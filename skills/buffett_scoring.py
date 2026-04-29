"""
Buffett Quantitative Scoring Engine
====================================
Extracted from virattt/ai-hedge-fund (54k⭐) src/agents/warren_buffett.py

Pure Python calculations — NO LLM dependency.
All scoring uses raw financial data from yfinance.

Dimensions scored:
1. Fundamentals (ROE, Debt/Equity, Operating Margin, Current Ratio)
2. Earnings Consistency (growth trend)
3. Competitive Moat (ROE consistency, margin stability, asset efficiency)
4. Management Quality (buybacks vs dilution, dividends)
5. Owner Earnings (Buffett's preferred true earnings measure)
6. Intrinsic Value (3-stage DCF)
7. Book Value Growth (CAGR)
8. Pricing Power (gross margin trends)
"""

import math


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _fmt_money(value, currency="$") -> str:
    if _is_missing(value):
        return "N/A"
    return f"{currency}{value/1e9:.2f}B"


def _period_label(period) -> str:
    if period is None:
        return None
    try:
        return period.strftime("%Y-%m-%d")
    except AttributeError:
        return str(period)


# ─────────────────────────────────────────────
#  1. Fundamental Analysis
# ─────────────────────────────────────────────
def analyze_fundamentals(info: dict) -> dict:
    """Score fundamentals based on Buffett's criteria using yfinance info dict."""
    score = 0
    reasoning = []

    roe = info.get("returnOnEquity")
    if roe and roe > 0.15:
        score += 2
        reasoning.append(f"Strong ROE of {roe:.1%}")
    elif roe:
        reasoning.append(f"Weak ROE of {roe:.1%}")
    else:
        reasoning.append("ROE data not available")

    de = info.get("debtToEquity")
    if de is not None:
        de_ratio = de / 100  # yfinance returns as percentage
        if de_ratio < 0.5:
            score += 2
            reasoning.append(f"Conservative debt levels (D/E: {de_ratio:.2f})")
        else:
            reasoning.append(f"High debt to equity ratio of {de_ratio:.2f}")
    else:
        reasoning.append("Debt to equity data not available")

    op_margin = info.get("operatingMargins")
    if op_margin and op_margin > 0.15:
        score += 2
        reasoning.append(f"Strong operating margins ({op_margin:.1%})")
    elif op_margin:
        reasoning.append(f"Weak operating margin of {op_margin:.1%}")
    else:
        reasoning.append("Operating margin data not available")

    current_ratio = info.get("currentRatio")
    if current_ratio and current_ratio > 1.5:
        score += 1
        reasoning.append(f"Good liquidity (current ratio: {current_ratio:.2f})")
    elif current_ratio:
        reasoning.append(f"Weak liquidity (current ratio: {current_ratio:.2f})")
    else:
        reasoning.append("Current ratio data not available")

    return {"score": score, "max_score": 7, "details": "; ".join(reasoning)}


# ─────────────────────────────────────────────
#  2. Earnings Consistency
# ─────────────────────────────────────────────
def analyze_consistency(net_incomes: list) -> dict:
    """Analyze earnings consistency from annual net income list (newest first)."""
    if len(net_incomes) < 4:
        return {"score": 0, "max_score": 3, "details": "Insufficient historical data (<4 years)"}

    score = 0
    reasoning = []

    # Check growth trend
    valid = [ni for ni in net_incomes if ni is not None and not math.isnan(ni)]
    if len(valid) >= 4:
        growth = all(valid[i] > valid[i + 1] for i in range(len(valid) - 1))
        if growth:
            score += 3
            reasoning.append("Consistent earnings growth over all periods")
        else:
            reasoning.append("Inconsistent earnings growth pattern")

        if len(valid) >= 2 and valid[-1] != 0:
            total_growth = (valid[0] - valid[-1]) / abs(valid[-1])
            reasoning.append(f"Total earnings growth of {total_growth:.1%} over {len(valid)} periods")
    else:
        reasoning.append("Insufficient valid earnings data for trend analysis")

    return {"score": score, "max_score": 3, "details": "; ".join(reasoning)}


# ─────────────────────────────────────────────
#  3. Competitive Moat
# ─────────────────────────────────────────────
def analyze_moat(historical_roe: list, historical_margins: list) -> dict:
    """
    Evaluate competitive moat using multiple Buffett indicators.
    Lists should be newest-first, at least 5 periods recommended.
    """
    if len(historical_roe) < 5 and len(historical_margins) < 5:
        return {"score": 0, "max_score": 5, "details": "Insufficient data for moat analysis"}

    moat_score = 0
    reasoning = []

    # 1. ROE Consistency
    valid_roes = [r for r in historical_roe if r is not None and not math.isnan(r)]
    if len(valid_roes) >= 5:
        high_roe_periods = sum(1 for r in valid_roes if r > 0.15)
        consistency = high_roe_periods / len(valid_roes)
        if consistency >= 0.8:
            moat_score += 2
            avg_roe = sum(valid_roes) / len(valid_roes)
            reasoning.append(f"Excellent ROE consistency: {high_roe_periods}/{len(valid_roes)} periods >15% (avg: {avg_roe:.1%})")
        elif consistency >= 0.6:
            moat_score += 1
            reasoning.append(f"Good ROE: {high_roe_periods}/{len(valid_roes)} periods >15%")
        else:
            reasoning.append(f"Inconsistent ROE: only {high_roe_periods}/{len(valid_roes)} periods >15%")

    # 2. Margin Stability
    valid_margins = [m for m in historical_margins if m is not None and not math.isnan(m)]
    if len(valid_margins) >= 5:
        avg_margin = sum(valid_margins) / len(valid_margins)
        recent_avg = sum(valid_margins[:3]) / min(3, len(valid_margins))
        older_avg = sum(valid_margins[-3:]) / min(3, len(valid_margins))

        if avg_margin > 0.2 and recent_avg >= older_avg:
            moat_score += 1
            reasoning.append(f"Strong stable margins (avg: {avg_margin:.1%}) indicate pricing power moat")
        elif avg_margin > 0.15:
            reasoning.append(f"Decent margins (avg: {avg_margin:.1%}) suggest some competitive advantage")
        else:
            reasoning.append(f"Low margins (avg: {avg_margin:.1%}) suggest limited pricing power")

    # 3. Stability coefficient
    if len(valid_roes) >= 5 and len(valid_margins) >= 5:
        roe_avg = sum(valid_roes) / len(valid_roes)
        if roe_avg > 0:
            roe_var = sum((r - roe_avg) ** 2 for r in valid_roes) / len(valid_roes)
            roe_stability = 1 - (roe_var ** 0.5) / roe_avg
        else:
            roe_stability = 0

        margin_avg = sum(valid_margins) / len(valid_margins)
        if margin_avg > 0:
            margin_var = sum((m - margin_avg) ** 2 for m in valid_margins) / len(valid_margins)
            margin_stability = 1 - (margin_var ** 0.5) / margin_avg
        else:
            margin_stability = 0

        overall = (roe_stability + margin_stability) / 2
        if overall > 0.7:
            moat_score += 2
            reasoning.append(f"High performance stability ({overall:.1%}) suggests strong competitive moat")
        elif overall > 0.5:
            moat_score += 1
            reasoning.append(f"Moderate stability ({overall:.1%})")

    return {"score": min(moat_score, 5), "max_score": 5, "details": "; ".join(reasoning) or "Limited moat analysis"}


# ─────────────────────────────────────────────
#  4. Management Quality
# ─────────────────────────────────────────────
def analyze_management_quality(cashflow_df) -> dict:
    """Check buybacks vs dilution and dividend history from yfinance cashflow."""
    score = 0
    reasoning = []

    if cashflow_df is None or cashflow_df.empty:
        return {"score": 0, "max_score": 2, "details": "No cashflow data available"}

    # Check share repurchase (negative = buying back)
    for label in ["Repurchase Of Capital Stock", "Common Stock Repurchased"]:
        if label in cashflow_df.index:
            latest = cashflow_df.loc[label].iloc[0]
            if latest is not None and not math.isnan(latest) and latest < 0:
                score += 1
                reasoning.append(f"Company repurchasing shares (${abs(latest)/1e9:.1f}B) — shareholder-friendly")
                break

    # Check for stock issuance (positive = dilution)
    for label in ["Issuance Of Capital Stock", "Common Stock Issued"]:
        if label in cashflow_df.index:
            latest = cashflow_df.loc[label].iloc[0]
            if latest is not None and not math.isnan(latest) and latest > 0:
                reasoning.append(f"Recent stock issuance (${latest/1e9:.1f}B) — potential dilution")
                break

    # Check dividends
    for label in ["Cash Dividends Paid", "Payment Of Dividends", "Common Stock Dividend Paid"]:
        if label in cashflow_df.index:
            latest = cashflow_df.loc[label].iloc[0]
            if latest is not None and not math.isnan(latest) and latest < 0:
                score += 1
                reasoning.append(f"Paying dividends (${abs(latest)/1e9:.1f}B)")
                break

    if not reasoning:
        reasoning.append("No buyback/dividend data detected")

    return {"score": score, "max_score": 2, "details": "; ".join(reasoning)}


# ─────────────────────────────────────────────
#  5. Owner Earnings
# ─────────────────────────────────────────────
def calculate_owner_earnings(
    net_income: float,
    depreciation: float,
    capex: float,
    *,
    fiscal_period=None,
    currency: str = "$",
    net_income_label: str = None,
    depreciation_label: str = None,
    capex_label: str = None,
    symbol: str = None,
) -> dict:
    """
    Buffett's Owner Earnings = Net Income + D&A - Maintenance CapEx
    """
    missing = [
        name
        for name, value in [
            ("net_income", net_income),
            ("depreciation", depreciation),
            ("capex", capex),
        ]
        if _is_missing(value)
    ]
    if missing:
        return {
            "owner_earnings": None,
            "score_status": "incomplete",
            "missing_components": missing,
            "fiscal_period": _period_label(fiscal_period),
            "currency": currency,
            "data_quality": {
                "source": "yfinance",
                "net_income_label": net_income_label,
                "depreciation_label": depreciation_label,
                "capex_label": capex_label,
                "maintenance_capex_method": "max(total_capex * 0.85, depreciation)",
                "same_period": False,
                "same_statement_basis": "unknown",
                "possible_a_share_capex_trap": _is_a_share_symbol(symbol),
            },
            "details": "Missing components for owner earnings",
        }

    capex = abs(capex)
    maintenance_capex = max(capex * 0.85, depreciation)  # Conservative estimate
    owner_earnings = net_income + depreciation - maintenance_capex

    return {
        "owner_earnings": owner_earnings,
        "score_status": "complete",
        "fiscal_period": _period_label(fiscal_period),
        "currency": currency,
        "components": {
            "net_income": net_income,
            "depreciation": depreciation,
            "total_capex": capex,
            "estimated_maintenance_capex": maintenance_capex,
        },
        "data_quality": {
            "source": "yfinance",
            "net_income_label": net_income_label,
            "depreciation_label": depreciation_label,
            "capex_label": capex_label,
            "maintenance_capex_method": "max(total_capex * 0.85, depreciation)",
            "same_period": fiscal_period is not None,
            "same_statement_basis": "unknown; yfinance statement line items may mix consolidated/common-stockholder labels",
            "possible_a_share_capex_trap": _is_a_share_symbol(symbol),
        },
        "details": (
            f"Net Income: {_fmt_money(net_income, currency)} | "
            f"D&A: {_fmt_money(depreciation, currency)} | "
            f"Maint CapEx: {_fmt_money(maintenance_capex, currency)} | "
            f"Owner Earnings: {_fmt_money(owner_earnings, currency)}"
        ),
    }


def _is_a_share_symbol(symbol: str) -> bool:
    if not symbol:
        return False
    symbol = symbol.upper()
    return symbol.endswith(".SS") or symbol.endswith(".SH") or symbol.endswith(".SZ")


def score_owner_earnings(owner_earnings: float, market_cap: float) -> dict:
    """Score owner earnings yield for the adjusted /35 score."""
    if _is_missing(owner_earnings) or _is_missing(market_cap) or market_cap <= 0:
        return {
            "score": None,
            "max_score": 5,
            "score_status": "incomplete",
            "details": "Missing owner earnings or market cap; score not fabricated",
        }

    oe_yield = owner_earnings / market_cap
    if oe_yield >= 0.08:
        score = 5
    elif oe_yield >= 0.06:
        score = 4
    elif oe_yield >= 0.04:
        score = 3
    elif oe_yield >= 0.02:
        score = 2
    elif oe_yield > 0:
        score = 1
    else:
        score = 0

    return {
        "score": score,
        "max_score": 5,
        "score_status": "complete",
        "owner_earnings_yield": oe_yield,
        "details": f"Owner earnings yield: {oe_yield:.1%}",
    }


# ─────────────────────────────────────────────
#  6. Intrinsic Value (3-stage DCF)
# ─────────────────────────────────────────────
def calculate_intrinsic_value(
    owner_earnings: float,
    shares_outstanding: int,
    historical_growth: float = None,
    *,
    eps: float = None,
    risk_free_rate: float = None,
) -> dict:
    """
    3-stage DCF with Buffett's conservative assumptions.
    Returns total intrinsic value (not per-share).
    """
    if not owner_earnings or not shares_outstanding or shares_outstanding <= 0:
        return {
            "intrinsic_value": None,
            "score_status": "incomplete",
            "graham": {
                "status": "incomplete",
                "details": "Missing owner earnings or shares outstanding",
            },
            "details": "Missing data for valuation",
        }

    # Conservative growth estimation
    if historical_growth is not None:
        growth = max(-0.05, min(historical_growth, 0.15))
        conservative_growth = growth * 0.7  # 30% haircut
    else:
        conservative_growth = 0.03

    stage1_growth = min(conservative_growth, 0.08)
    stage2_growth = min(conservative_growth * 0.5, 0.04)
    terminal_growth = 0.025
    discount_rate = 0.10

    # Stage 1: 5 years high growth
    stage1_pv = sum(
        owner_earnings * (1 + stage1_growth) ** yr / (1 + discount_rate) ** yr
        for yr in range(1, 6)
    )

    # Stage 2: 5 years transition
    s1_final = owner_earnings * (1 + stage1_growth) ** 5
    stage2_pv = sum(
        s1_final * (1 + stage2_growth) ** yr / (1 + discount_rate) ** (5 + yr)
        for yr in range(1, 6)
    )

    # Terminal value
    s2_final = s1_final * (1 + stage2_growth) ** 5
    terminal_earnings = s2_final * (1 + terminal_growth)
    terminal_value = terminal_earnings / (discount_rate - terminal_growth)
    terminal_pv = terminal_value / (1 + discount_rate) ** 10

    raw_iv = stage1_pv + stage2_pv + terminal_pv
    conservative_iv = raw_iv * 0.85  # 15% additional haircut

    per_share = conservative_iv / shares_outstanding

    graham = calculate_graham_value(eps, historical_growth, risk_free_rate)

    return {
        "intrinsic_value": conservative_iv,
        "intrinsic_value_per_share": per_share,
        "score_status": "complete",
        "assumptions": {
            "stage1_growth": f"{stage1_growth:.1%}",
            "stage2_growth": f"{stage2_growth:.1%}",
            "terminal_growth": f"{terminal_growth:.1%}",
            "discount_rate": f"{discount_rate:.1%}",
        },
        "graham": graham,
        "details": (
            f"Stage1 PV: ${stage1_pv/1e9:.1f}B | "
            f"Stage2 PV: ${stage2_pv/1e9:.1f}B | "
            f"Terminal PV: ${terminal_pv/1e9:.1f}B | "
            f"Conservative IV: ${conservative_iv/1e9:.1f}B | "
            f"Per Share: ${per_share:.2f}"
        ),
    }


def calculate_graham_value(eps: float, historical_growth: float = None, risk_free_rate: float = None) -> dict:
    """Graham value requires an explicit risk-free rate for the adjusted formula."""
    if _is_missing(eps):
        return {"status": "incomplete", "details": "Missing EPS; Graham value not calculated"}
    if _is_missing(risk_free_rate) or risk_free_rate <= 0:
        return {
            "status": "incomplete",
            "eps": eps,
            "risk_free_rate": risk_free_rate,
            "details": "Missing explicit risk-free rate; adjusted Graham value not calculated",
        }

    growth = historical_growth if historical_growth is not None else 0.03
    growth = max(-0.05, min(growth, 0.15))
    growth_pct = growth * 100
    base_value = eps * (8.5 + 2 * growth_pct)
    adjusted_value = base_value * (4.4 / (risk_free_rate * 100))
    return {
        "status": "complete",
        "eps": eps,
        "growth_rate": growth,
        "risk_free_rate": risk_free_rate,
        "base_value": base_value,
        "adjusted_value": adjusted_value,
        "details": "Adjusted Graham value uses EPS * (8.5 + 2g) * (4.4 / Y)",
    }


def score_intrinsic_value(margin_of_safety: float) -> dict:
    """Score intrinsic value margin for the adjusted /35 score."""
    if _is_missing(margin_of_safety):
        return {
            "score": None,
            "max_score": 3,
            "score_status": "incomplete",
            "details": "Missing intrinsic value or market cap; score not fabricated",
        }

    if margin_of_safety >= 0.50:
        score = 3
    elif margin_of_safety >= 0.25:
        score = 2
    elif margin_of_safety >= 0:
        score = 1
    else:
        score = 0

    return {
        "score": score,
        "max_score": 3,
        "score_status": "complete",
        "margin_of_safety": margin_of_safety,
        "details": f"Margin of safety: {margin_of_safety:.1%}",
    }


# ─────────────────────────────────────────────
#  7. Book Value Growth
# ─────────────────────────────────────────────
def analyze_book_value_growth(book_values_per_share: list) -> dict:
    """Analyze book value per share growth. List newest-first."""
    valid = [b for b in book_values_per_share if b is not None and not math.isnan(b)]
    if len(valid) < 3:
        return {"score": 0, "max_score": 5, "details": "Insufficient book value data"}

    score = 0
    reasoning = []

    # Growth consistency
    growth_periods = sum(1 for i in range(len(valid) - 1) if valid[i] > valid[i + 1])
    growth_rate = growth_periods / (len(valid) - 1)

    if growth_rate >= 0.8:
        score += 3
        reasoning.append("Consistent BV/share growth (Buffett's favorite metric)")
    elif growth_rate >= 0.6:
        score += 2
        reasoning.append("Good BV/share growth pattern")
    elif growth_rate >= 0.4:
        score += 1
        reasoning.append("Moderate BV/share growth")
    else:
        reasoning.append("Inconsistent BV/share growth")

    # CAGR
    oldest, latest = valid[-1], valid[0]
    years = len(valid) - 1
    if oldest > 0 and latest > 0:
        cagr = ((latest / oldest) ** (1 / years)) - 1
        if cagr > 0.15:
            score += 2
            reasoning.append(f"Excellent BV CAGR: {cagr:.1%}")
        elif cagr > 0.1:
            score += 1
            reasoning.append(f"Good BV CAGR: {cagr:.1%}")
        else:
            reasoning.append(f"BV CAGR: {cagr:.1%}")
    elif oldest < 0 < latest:
        score += 3
        reasoning.append("Improved from negative to positive book value")

    return {"score": min(score, 5), "max_score": 5, "details": "; ".join(reasoning)}


# ─────────────────────────────────────────────
#  8. Pricing Power
# ─────────────────────────────────────────────
def analyze_pricing_power(gross_margins: list) -> dict:
    """Analyze pricing power from gross margin trend. List newest-first."""
    valid = [m for m in gross_margins if m is not None and not math.isnan(m)]
    if len(valid) < 3:
        return {"score": 0, "max_score": 5, "details": "Insufficient gross margin data"}

    score = 0
    reasoning = []

    recent_avg = sum(valid[:2]) / min(2, len(valid))
    older_avg = sum(valid[-2:]) / min(2, len(valid))

    if recent_avg > older_avg + 0.02:
        score += 3
        reasoning.append("Expanding gross margins = strong pricing power")
    elif recent_avg > older_avg:
        score += 2
        reasoning.append("Improving gross margins = good pricing power")
    elif abs(recent_avg - older_avg) < 0.01:
        score += 1
        reasoning.append("Stable gross margins")
    else:
        reasoning.append("Declining gross margins = pricing pressure")

    avg_margin = sum(valid) / len(valid)
    if avg_margin > 0.5:
        score += 2
        reasoning.append(f"Consistently high gross margins ({avg_margin:.1%})")
    elif avg_margin > 0.3:
        score += 1
        reasoning.append(f"Good gross margins ({avg_margin:.1%})")

    return {"score": min(score, 5), "max_score": 5, "details": "; ".join(reasoning)}


# ─────────────────────────────────────────────
#  Master Runner
# ─────────────────────────────────────────────
def run_full_buffett_analysis(
    ticker_obj,
    risk_free_rate: float = None,
    override_capex: float = None,
    override_growth: float = None
) -> dict:
    """
    Run all 8 Buffett scoring dimensions on a yfinance Ticker object.
    Returns structured JSON with all scores + intrinsic value + margin of safety.
    """
    info = ticker_obj.info

    # Get financial statements
    income_stmt = ticker_obj.income_stmt
    balance_sheet = ticker_obj.balance_sheet
    cashflow = ticker_obj.cashflow

    # 1. Fundamentals
    fundamentals = analyze_fundamentals(info)

    # 2. Earnings Consistency
    net_incomes = []
    if income_stmt is not None and not income_stmt.empty:
        for label in ["Net Income", "Net Income Common Stockholders"]:
            if label in income_stmt.index:
                net_incomes = income_stmt.loc[label].tolist()
                break
    consistency = analyze_consistency(net_incomes)

    # 3. Moat Analysis — collect historical ROE and margins
    historical_roe = []
    historical_margins = []
    # Use quarterly financials for more data points
    quarterly_income = ticker_obj.quarterly_income_stmt
    quarterly_balance = ticker_obj.quarterly_balance_sheet
    if quarterly_income is not None and quarterly_balance is not None:
        for col in quarterly_income.columns:
            ni = None
            eq = None
            for label in ["Net Income", "Net Income Common Stockholders"]:
                if label in quarterly_income.index:
                    val = quarterly_income.loc[label].get(col)
                    if val is not None and not math.isnan(val):
                        ni = val
                        break
            if col in quarterly_balance.columns:
                for label in ["Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity"]:
                    if label in quarterly_balance.index:
                        val = quarterly_balance.loc[label].get(col)
                        if val is not None and not math.isnan(val) and val != 0:
                            eq = val
                            break
            if ni is not None and eq is not None:
                historical_roe.append(ni / eq)

            for label in ["Operating Income", "EBIT"]:
                if label in quarterly_income.index:
                    oi = quarterly_income.loc[label].get(col)
                    for rev_label in ["Total Revenue", "Revenue"]:
                        if rev_label in quarterly_income.index:
                            rev = quarterly_income.loc[rev_label].get(col)
                            if oi is not None and rev is not None and rev != 0:
                                historical_margins.append(oi / rev)
                            break
                    break

    moat = analyze_moat(historical_roe, historical_margins)

    # 4. Management Quality
    mgmt = analyze_management_quality(cashflow)

    # 5 & 6. Owner Earnings + Intrinsic Value
    net_income_latest = None
    depreciation_latest = None
    capex_latest = None
    fiscal_period = None
    net_income_label = None
    depreciation_label = None
    capex_label = None

    if income_stmt is not None and not income_stmt.empty:
        fiscal_period = income_stmt.columns[0] if len(income_stmt.columns) else None
        for label in ["Net Income", "Net Income Common Stockholders"]:
            if label in income_stmt.index:
                net_income_latest = income_stmt.loc[label].iloc[0]
                net_income_label = label
                break

    if cashflow is not None and not cashflow.empty:
        if fiscal_period is None and len(cashflow.columns):
            fiscal_period = cashflow.columns[0]
        for label in ["Depreciation And Amortization", "Depreciation & Amortization"]:
            if label in cashflow.index:
                depreciation_latest = cashflow.loc[label].iloc[0]
                depreciation_label = label
                break
        for label in ["Capital Expenditure", "Capital Expenditures"]:
            if label in cashflow.index:
                capex_latest = cashflow.loc[label].iloc[0]
                capex_label = label
                break

    currency = info.get("financialCurrency") or info.get("currency") or "$"
    currency_symbol = "$" if currency == "USD" else f"{currency} "
    symbol = info.get("symbol", "")
    oe = calculate_owner_earnings(
        net_income_latest,
        depreciation_latest,
        capex_latest if override_capex is None else override_capex,
        fiscal_period=fiscal_period,
        currency=currency_symbol,
        net_income_label=net_income_label,
        depreciation_label=depreciation_label,
        capex_label="OVERRIDE" if override_capex is not None else capex_label,
        symbol=symbol,
    )

    shares = info.get("sharesOutstanding", 0)
    eps = info.get("trailingEps")
    
    implied_cagr = None
    valid_ni = [ni for ni in net_incomes if ni is not None and not math.isnan(ni)]
    if len(valid_ni) >= 2:
        latest_ni, oldest_ni = valid_ni[0], valid_ni[-1]
        if oldest_ni > 0 and latest_ni > 0:
            years = len(valid_ni) - 1
            implied_cagr = ((latest_ni / oldest_ni) ** (1 / years)) - 1
        elif oldest_ni < 0 < latest_ni:
            implied_cagr = 0.05
        else:
            implied_cagr = 0.0
            
    earnings_growth = override_growth
    if earnings_growth is None:
        eg_raw = info.get("earningsGrowth")
        if eg_raw is None or eg_raw > 0.3 or eg_raw < -0.3:
            earnings_growth = implied_cagr if implied_cagr is not None else 0.03
        else:
            earnings_growth = eg_raw

    iv = calculate_intrinsic_value(
        oe.get("owner_earnings"), 
        shares, 
        earnings_growth, 
        eps=eps, 
        risk_free_rate=risk_free_rate
    )

    # Margin of Safety
    current_price = info.get("currentPrice", 0)
    market_cap = info.get("marketCap", 0)
    margin_of_safety = None
    if iv.get("intrinsic_value") and market_cap:
        margin_of_safety = (iv["intrinsic_value"] - market_cap) / market_cap

    # 7. Book Value Growth
    bvps_list = []
    if balance_sheet is not None and not balance_sheet.empty:
        for eq_label in ["Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity"]:
            if eq_label in balance_sheet.index:
                for sh_label in ["Ordinary Shares Number", "Share Issued"]:
                    if sh_label in balance_sheet.index:
                        equities = balance_sheet.loc[eq_label]
                        shares_hist = balance_sheet.loc[sh_label]
                        for col in balance_sheet.columns:
                            e = equities.get(col)
                            s = shares_hist.get(col)
                            if e is not None and s is not None and s != 0:
                                bvps_list.append(e / s)
                        break
                break
    book_value = analyze_book_value_growth(bvps_list)

    # 8. Pricing Power
    gross_margins = []
    if income_stmt is not None and not income_stmt.empty:
        for col in income_stmt.columns:
            gp = None
            rev = None
            for label in ["Gross Profit"]:
                if label in income_stmt.index:
                    gp = income_stmt.loc[label].get(col)
            for label in ["Total Revenue", "Revenue"]:
                if label in income_stmt.index:
                    rev = income_stmt.loc[label].get(col)
            if gp is not None and rev is not None and rev != 0:
                gross_margins.append(gp / rev)
    pricing_power = analyze_pricing_power(gross_margins)

    # Total Score
    machine_dimensions = [fundamentals, consistency, moat, mgmt, book_value, pricing_power]
    total_score = sum(d["score"] for d in machine_dimensions)
    total_max = sum(d["max_score"] for d in machine_dimensions)
    oe_score = score_owner_earnings(oe.get("owner_earnings"), market_cap)
    iv_score = score_intrinsic_value(margin_of_safety)
    adjusted_components = [oe_score, iv_score]
    adjusted_complete = all(c.get("score_status") == "complete" for c in adjusted_components)
    adjusted_score = total_score + sum(c["score"] for c in adjusted_components if c.get("score") is not None)
    adjusted_max = total_max + sum(c["max_score"] for c in adjusted_components)

    return {
        "ticker": info.get("symbol", ""),
        "company_name": info.get("shortName", ""),
        "score_basis": {
            "machine_score_basis": "27 points from fundamentals, earnings consistency, moat, management, book value growth, and pricing power",
            "adjusted_score_basis": "35 points = machine /27 + owner earnings /5 + intrinsic value /3",
        },
        "machine_score": {
            "score": total_score,
            "max_score": total_max,
            "percentage": f"{total_score/total_max:.0%}" if total_max > 0 else "N/A",
            "status": "complete",
        },
        "adjusted_score": {
            "score": adjusted_score if adjusted_complete else None,
            "partial_score": adjusted_score,
            "max_score": adjusted_max,
            "percentage": f"{adjusted_score/adjusted_max:.0%}" if adjusted_complete and adjusted_max > 0 else "N/A",
            "status": "complete" if adjusted_complete else "incomplete",
            "details": "OE/IV scores are not fabricated when inputs are missing",
        },
        "total_score": total_score,
        "total_max_score": total_max,
        "score_percentage": f"{total_score/total_max:.0%}" if total_max > 0 else "N/A",
        "current_price": current_price,
        "market_cap": market_cap,
        "intrinsic_value": iv.get("intrinsic_value"),
        "intrinsic_value_per_share": iv.get("intrinsic_value_per_share"),
        "margin_of_safety": f"{margin_of_safety:.1%}" if margin_of_safety else "N/A",
        "dimensions": {
            "fundamentals": fundamentals,
            "earnings_consistency": consistency,
            "competitive_moat": moat,
            "management_quality": mgmt,
            "owner_earnings": oe,
            "owner_earnings_score": oe_score,
            "intrinsic_value": iv,
            "intrinsic_value_score": iv_score,
            "book_value_growth": book_value,
            "pricing_power": pricing_power,
        },
    }
