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
def calculate_owner_earnings(net_income: float, depreciation: float, capex: float) -> dict:
    """
    Buffett's Owner Earnings = Net Income + D&A - Maintenance CapEx
    """
    if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in [net_income, depreciation, capex]):
        return {"owner_earnings": None, "details": "Missing components for owner earnings"}

    capex = abs(capex)
    maintenance_capex = max(capex * 0.85, depreciation)  # Conservative estimate
    owner_earnings = net_income + depreciation - maintenance_capex

    return {
        "owner_earnings": owner_earnings,
        "components": {
            "net_income": net_income,
            "depreciation": depreciation,
            "total_capex": capex,
            "estimated_maintenance_capex": maintenance_capex,
        },
        "details": (
            f"Net Income: ${net_income/1e9:.2f}B | "
            f"D&A: ${depreciation/1e9:.2f}B | "
            f"Maint CapEx: ${maintenance_capex/1e9:.2f}B | "
            f"Owner Earnings: ${owner_earnings/1e9:.2f}B"
        ),
    }


# ─────────────────────────────────────────────
#  6. Intrinsic Value (3-stage DCF)
# ─────────────────────────────────────────────
def calculate_intrinsic_value(owner_earnings: float, shares_outstanding: int,
                               historical_growth: float = None) -> dict:
    """
    3-stage DCF with Buffett's conservative assumptions.
    Returns total intrinsic value (not per-share).
    """
    if not owner_earnings or not shares_outstanding or shares_outstanding <= 0:
        return {"intrinsic_value": None, "details": "Missing data for valuation"}

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

    return {
        "intrinsic_value": conservative_iv,
        "intrinsic_value_per_share": per_share,
        "assumptions": {
            "stage1_growth": f"{stage1_growth:.1%}",
            "stage2_growth": f"{stage2_growth:.1%}",
            "terminal_growth": f"{terminal_growth:.1%}",
            "discount_rate": f"{discount_rate:.1%}",
        },
        "details": (
            f"Stage1 PV: ${stage1_pv/1e9:.1f}B | "
            f"Stage2 PV: ${stage2_pv/1e9:.1f}B | "
            f"Terminal PV: ${terminal_pv/1e9:.1f}B | "
            f"Conservative IV: ${conservative_iv/1e9:.1f}B | "
            f"Per Share: ${per_share:.2f}"
        ),
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
def run_full_buffett_analysis(ticker_obj) -> dict:
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

    if income_stmt is not None and not income_stmt.empty:
        for label in ["Net Income", "Net Income Common Stockholders"]:
            if label in income_stmt.index:
                net_income_latest = income_stmt.loc[label].iloc[0]
                break

    if cashflow is not None and not cashflow.empty:
        for label in ["Depreciation And Amortization", "Depreciation & Amortization"]:
            if label in cashflow.index:
                depreciation_latest = cashflow.loc[label].iloc[0]
                break
        for label in ["Capital Expenditure", "Capital Expenditures"]:
            if label in cashflow.index:
                capex_latest = cashflow.loc[label].iloc[0]
                break

    oe = calculate_owner_earnings(net_income_latest, depreciation_latest, capex_latest)

    shares = info.get("sharesOutstanding", 0)
    earnings_growth = info.get("earningsGrowth")
    iv = calculate_intrinsic_value(oe.get("owner_earnings"), shares, earnings_growth)

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
    total_score = sum(d["score"] for d in [fundamentals, consistency, moat, mgmt, book_value, pricing_power])
    total_max = sum(d["max_score"] for d in [fundamentals, consistency, moat, mgmt, book_value, pricing_power])

    return {
        "ticker": info.get("symbol", ""),
        "company_name": info.get("shortName", ""),
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
            "intrinsic_value": iv,
            "book_value_growth": book_value,
            "pricing_power": pricing_power,
        },
    }
