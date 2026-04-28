from __future__ import annotations

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
import numpy as np

try:
    import ta
except ImportError:
    ta = None


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


def compute_adx(price_df: pd.DataFrame, period: int = 14) -> float | None:
    """Compute ADX(14). Uses ta when available, otherwise a lightweight Wilder-style fallback."""
    required = {"High", "Low", "Close"}
    if not required.issubset(price_df.columns):
        return None

    df = price_df[list(required)].dropna().copy()
    if len(df) < period * 2:
        return None

    if ta:
        try:
            adx = ta.trend.ADXIndicator(
                high=df["High"], low=df["Low"], close=df["Close"], window=period
            ).adx()
            latest = adx.iloc[-1]
            return None if math.isnan(latest) else float(latest)
        except Exception:
            pass

    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=df.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=df.index,
    )

    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)

    alpha = 1 / period
    atr = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_dm_smoothed = plus_dm.ewm(alpha=alpha, adjust=False).mean()
    minus_dm_smoothed = minus_dm.ewm(alpha=alpha, adjust=False).mean()

    plus_di = 100 * (plus_dm_smoothed / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm_smoothed / atr.replace(0, np.nan))
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    adx = dx.ewm(alpha=alpha, adjust=False).mean()

    latest = adx.iloc[-1]
    return None if math.isnan(latest) else float(latest)


def classify_adx_strength(adx: float | None) -> str:
    """Traditional ADX read: strength only, not direction."""
    if adx is None:
        return "unknown"
    if adx < 20:
        return "weak/range-bound"
    if adx < 25:
        return "developing trend"
    if adx < 40:
        return "strong trend"
    return "very strong trend"


def summarize_timing_state(trend: str, adx_strength: str, ret_14d: float) -> str:
    """Compress direction + strength + short-term stretch into a simple timing label."""
    if trend.startswith("bullish"):
        if ret_14d > 0.12:
            return "uptrend_but_extended"
        return "uptrend_constructive"
    if trend.startswith("bearish"):
        return "downtrend_or_weakening"
    if trend.startswith("sideways"):
        return "range_wait_for_breakout"
    if adx_strength == "weak/range-bound":
        return "trendless_wait"
    return "mixed_wait"


def summarize_primary_trend(trend: str, weekly_trend: str, long_ma_bullish: bool, long_ma_bearish: bool) -> str:
    """Expose a simpler long-horizon label for reports and scan tables."""
    if trend.startswith("bullish") and long_ma_bullish and weekly_trend == "bullish":
        return "bullish"
    if trend.startswith("bearish") and long_ma_bearish and weekly_trend == "bearish":
        return "bearish"
    if trend.startswith("sideways"):
        return "sideways"
    return "mixed"


def summarize_rating_constraint(primary_trend: str, trend_strength: str) -> str:
    """Map trend state into report-level rating constraint semantics.

    Only a real bearish primary trend should hard-cap the rating.
    Sideways / mixed should influence timing and position sizing, but should not
    automatically ban Buy if valuation and fundamentals are compelling enough.
    """
    if primary_trend == "bearish" and trend_strength in {"developing trend", "strong trend", "very strong trend"}:
        return "hard_cap_hold_or_watch"
    if primary_trend == "sideways":
        return "timing_caution_only"
    if primary_trend == "mixed":
        return "timing_caution_only"
    return "no_technical_cap"


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
    sma_20 = float(prices.tail(20).mean()) if len(prices) >= 20 else float(prices.mean())
    sma_60 = float(prices.tail(60).mean()) if len(prices) >= 60 else float(prices.mean())
    sma_7_prev = float(prices.iloc[-8:-1].mean()) if len(prices) >= 8 else sma_7
    sma_14_prev = float(prices.iloc[-15:-1].mean()) if len(prices) >= 15 else sma_14
    sma_20_prev = float(prices.iloc[-21:-1].mean()) if len(prices) >= 21 else sma_20
    sma_60_prev = float(prices.iloc[-61:-1].mean()) if len(prices) >= 61 else sma_60

    # Traditional classification should not rely on a single short-term MA stack.
    # We combine:
    # 1. price structure (higher highs/lows or lower highs/lows),
    # 2. MA alignment + slope,
    # 3. sideways filter when movement and MA spread are both small.
    lookback = min(20, len(prices))
    recent = prices.tail(lookback)
    split = max(lookback // 2, 5)
    first_half = recent.iloc[:split]
    second_half = recent.iloc[split:]

    prev_high = float(first_half.max())
    prev_low = float(first_half.min())
    recent_high = float(second_half.max()) if len(second_half) > 0 else prev_high
    recent_low = float(second_half.min()) if len(second_half) > 0 else prev_low

    pivot_tol = 0.005  # 0.5% tolerance to avoid treating tiny wiggles as new trend pivots
    higher_high = recent_high > prev_high * (1 + pivot_tol)
    higher_low = recent_low > prev_low * (1 + pivot_tol)
    lower_high = recent_high < prev_high * (1 - pivot_tol)
    lower_low = recent_low < prev_low * (1 - pivot_tol)

    short_ma_bullish = latest_price > sma_7 > sma_14 and sma_7 >= sma_7_prev and sma_14 >= sma_14_prev
    short_ma_bearish = latest_price < sma_7 < sma_14 and sma_7 <= sma_7_prev and sma_14 <= sma_14_prev
    long_ma_bullish = latest_price > sma_20 > sma_60 and sma_20 >= sma_20_prev and sma_60 >= sma_60_prev
    long_ma_bearish = latest_price < sma_20 < sma_60 and sma_20 <= sma_20_prev and sma_60 <= sma_60_prev

    ma_spread_pct = abs(sma_7 - sma_14) / latest_price if latest_price else 0.0
    ma_spread_20_60_pct = abs(sma_20 - sma_60) / latest_price if latest_price else 0.0
    range_pct = (float(recent.max()) - float(recent.min())) / latest_price if latest_price else 0.0
    ret_14d = 0.0
    if len(prices) >= 15:
        base = float(prices.iloc[-15])
        if base:
            ret_14d = (latest_price - base) / base

    ret_60d = 0.0
    if len(prices) >= 61:
        base = float(prices.iloc[-61])
        if base:
            ret_60d = (latest_price - base) / base

    weekly_prices = None
    weekly_latest = None
    weekly_sma_4 = None
    weekly_sma_12 = None
    weekly_trend = "unknown"
    try:
        weekly_prices = prices.resample("W-FRI").last().dropna()
        if len(weekly_prices) >= 4:
            weekly_latest = float(weekly_prices.iloc[-1])
            weekly_sma_4 = float(weekly_prices.tail(4).mean())
        if len(weekly_prices) >= 12:
            weekly_sma_12 = float(weekly_prices.tail(12).mean())
        if weekly_latest is not None and weekly_sma_4 is not None and weekly_sma_12 is not None:
            if weekly_latest > weekly_sma_4 > weekly_sma_12:
                weekly_trend = "bullish"
            elif weekly_latest < weekly_sma_4 < weekly_sma_12:
                weekly_trend = "bearish"
            else:
                weekly_trend = "mixed"
    except Exception:
        weekly_trend = "unknown"

    adx_14 = compute_adx(price_df, period=14)
    adx_strength = classify_adx_strength(adx_14)

    sideways_filter = (
        ma_spread_pct < 0.01 and
        ma_spread_20_60_pct < 0.03 and
        abs(ret_14d) < 0.03 and
        abs(ret_60d) < 0.08 and
        range_pct < 0.08
    )

    if sideways_filter:
        trend = "sideways/range-bound (tight MA spread + limited 14d/60d move)"
    elif higher_high and higher_low and long_ma_bullish and weekly_trend in {"bullish", "unknown"}:
        trend = "bullish (higher highs/lows + rising SMA20/SMA60 + weekly confirmation)"
    elif lower_high and lower_low and long_ma_bearish and weekly_trend in {"bearish", "unknown"}:
        trend = "bearish (lower highs/lows + falling SMA20/SMA60 + weekly confirmation)"
    elif higher_high and higher_low and short_ma_bullish:
        trend = "bullish_short_term (price structure improving, long trend not fully confirmed)"
    elif lower_high and lower_low and short_ma_bearish:
        trend = "bearish_short_term (price structure weakening, long trend not fully confirmed)"
    else:
        trend = "mixed/transitioning"

    short_term_state = (
        "bullish" if short_ma_bullish else
        "bearish" if short_ma_bearish else
        "mixed"
    )
    primary_trend = summarize_primary_trend(trend, weekly_trend, long_ma_bullish, long_ma_bearish)
    timing_state = summarize_timing_state(trend, adx_strength, ret_14d)
    rating_constraint = summarize_rating_constraint(primary_trend, adx_strength)

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
            "sma_20": round(sma_20, 2),
            "sma_60": round(sma_60, 2),
        },
        "weekly_view": {
            "weekly_close": round(weekly_latest, 2) if weekly_latest is not None else None,
            "sma_4w": round(weekly_sma_4, 2) if weekly_sma_4 is not None else None,
            "sma_12w": round(weekly_sma_12, 2) if weekly_sma_12 is not None else None,
            "weekly_trend": weekly_trend,
        },
        "trend_evidence": {
            "recent_high": round(recent_high, 2),
            "recent_low": round(recent_low, 2),
            "previous_high": round(prev_high, 2),
            "previous_low": round(prev_low, 2),
            "ma_spread_pct": f"{ma_spread_pct:.2%}",
            "ma_spread_20_60_pct": f"{ma_spread_20_60_pct:.2%}",
            "range_pct": f"{range_pct:.2%}",
            "return_14d": f"{ret_14d:.2%}",
            "return_60d": f"{ret_60d:.2%}",
            "higher_high": higher_high,
            "higher_low": higher_low,
            "lower_high": lower_high,
            "lower_low": lower_low,
            "short_ma_bullish": short_ma_bullish,
            "short_ma_bearish": short_ma_bearish,
            "long_ma_bullish": long_ma_bullish,
            "long_ma_bearish": long_ma_bearish,
            "adx_14": round(adx_14, 2) if adx_14 is not None else None,
            "adx_strength": adx_strength,
        },
        "trend_classification": trend,
        "trend_summary": {
            "short_term_state": short_term_state,
            "primary_trend": primary_trend,
            "trend_strength": adx_strength,
            "timing_state": timing_state,
            "rating_constraint": rating_constraint,
        },
        "annualized_volatility": f"{volatility:.1%}",
        "data_points": len(prices),
    }
