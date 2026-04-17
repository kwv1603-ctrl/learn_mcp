"""
FinAgent Multi-Timeframe Reflection Engine
============================================
Extracted from DVampire/FinAgent finagent/prompt/trading/low_level_reflection.py

Pure Python price movement analysis — NO LLM dependency.
Computes short/medium/long-term price changes as structured data
to feed into LLM reasoning (Antigravity).
"""

import pandas as pd
import math


def compute_price_movement(prices: pd.Series, periods: int) -> str:
    """Compute percentage price change over N periods and return human-readable text."""
    if len(prices) < periods + 1:
        return "insufficient data"
    pct = (prices.iloc[-1] - prices.iloc[-1 - periods]) / prices.iloc[-1 - periods]
    if math.isnan(pct):
        return "unknown"
    if pct > 0:
        return f"increase of {abs(pct * 100):.2f}%"
    elif pct < 0:
        return f"decrease of {abs(pct * 100):.2f}%"
    else:
        return "no change"


def compute_multi_timeframe_reflection(price_df: pd.DataFrame) -> dict:
    """
    Compute short/medium/long-term price movement analysis.
    
    Input: DataFrame with 'Close' or 'Adj Close' column, datetime index.
    Output: Structured dict with multi-timeframe observations.
    
    Timeframes (following FinAgent's design):
    - Short-term: 1 trading day
    - Medium-term: 7 trading days  
    - Long-term: 14 trading days
    """
    # Find the close price column
    close_col = None
    for col_name in ['Close', 'Adj Close', 'close', 'adj_close']:
        if col_name in price_df.columns:
            close_col = col_name
            break

    if close_col is None:
        return {"error": "No close price column found"}

    prices = price_df[close_col].dropna()

    if len(prices) < 15:
        return {"error": f"Insufficient price data ({len(prices)} points, need ≥15)"}

    short_term = compute_price_movement(prices, 1)
    medium_term = compute_price_movement(prices, 7)
    long_term = compute_price_movement(prices, 14)

    latest_price = float(prices.iloc[-1])
    sma_7 = float(prices.tail(7).mean())
    sma_14 = float(prices.tail(14).mean())

    # Trend classification
    if latest_price > sma_7 > sma_14:
        trend = "bullish (price > SMA7 > SMA14)"
    elif latest_price < sma_7 < sma_14:
        trend = "bearish (price < SMA7 < SMA14)"
    else:
        trend = "mixed/transitioning"

    # Volatility (14-day)
    returns = prices.pct_change().tail(14).dropna()
    volatility = float(returns.std() * (252 ** 0.5))  # Annualized

    return {
        "latest_price": latest_price,
        "price_movements": {
            "short_term_1d": short_term,
            "medium_term_7d": medium_term,
            "long_term_14d": long_term,
        },
        "moving_averages": {
            "sma_7": round(sma_7, 2),
            "sma_14": round(sma_14, 2),
        },
        "trend_classification": trend,
        "annualized_volatility": f"{volatility:.1%}",
        "data_points": len(prices),
    }
