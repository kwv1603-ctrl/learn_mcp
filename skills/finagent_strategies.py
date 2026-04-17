"""
FinAgent Strategy Engines
==========================
Extracted from DVampire/FinAgent finagent/tools/strategy_agents.py

Pure Python trading strategy signals — NO LLM dependency.
Only requires: pandas, numpy, ta (technical analysis library)

Strategies:
1. MACD Crossover (Trend Following)
2. KDJ + RSI Filter (Overbought/Oversold)
3. Stochastic + Bollinger Bands (Support/Resistance)
4. Mean Reversion (Z-Score)
5. Mean Reversion + ATR (Volatility + Trend)
"""

import pandas as pd
import numpy as np

try:
    import ta
except ImportError:
    ta = None


def _data_process(data: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names for strategy processing."""
    df = data.copy()
    rename_map = {}
    for col in df.columns:
        lc = col.lower()
        if lc == 'open':
            rename_map[col] = 'Open'
        elif lc == 'high':
            rename_map[col] = 'High'
        elif lc == 'low':
            rename_map[col] = 'Low'
        elif lc in ('close', 'adj close', 'adj_close'):
            rename_map[col] = 'Close'
        elif lc == 'volume':
            rename_map[col] = 'Volume'
    df = df.rename(columns=rename_map)
    return df


def _calculate_ema(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(span=window, adjust=False).mean()


# ─────────────────────────────────────────────
#  Strategy 1: MACD Crossover
# ─────────────────────────────────────────────
def strategy_macd(data: pd.DataFrame, short_window: int = 7) -> dict:
    """
    MACD Crossover Strategy — Trend Following.
    BUY when MACD crosses above signal; SELL when below.
    """
    df = _data_process(data)
    long_window = 14

    short_ema = _calculate_ema(df['Close'], short_window)
    long_ema = _calculate_ema(df['Close'], long_window)
    macd_line = short_ema - long_ema
    signal_line = _calculate_ema(macd_line, 9)

    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append('HOLD')
        elif macd_line.iloc[i] > signal_line.iloc[i] and macd_line.iloc[i-1] < signal_line.iloc[i-1]:
            signals.append('BUY')
        elif macd_line.iloc[i] < signal_line.iloc[i] and macd_line.iloc[i-1] > signal_line.iloc[i-1]:
            signals.append('SELL')
        else:
            signals.append('HOLD')

    latest = signals[-1] if signals else 'HOLD'
    latest_macd = float(macd_line.iloc[-1]) if len(macd_line) > 0 else 0
    latest_signal = float(signal_line.iloc[-1]) if len(signal_line) > 0 else 0

    return {
        "strategy": "MACD Crossover",
        "type": "Trend Following",
        "signal": latest,
        "details": f"MACD: {latest_macd:.4f} | Signal: {latest_signal:.4f}",
        "params": {"short_window": short_window, "long_window": long_window},
    }


# ─────────────────────────────────────────────
#  Strategy 2: KDJ + RSI Filter
# ─────────────────────────────────────────────
def strategy_kdj_rsi(data: pd.DataFrame,
                      ilong: int = 9, isig: int = 3,
                      rsi_overbought: int = 60, rsi_oversold: int = 40) -> dict:
    """
    KDJ with RSI Filter — Overbought/Oversold detection.
    BUY when J crosses above D and RSI is oversold; SELL when opposite.
    """
    df = _data_process(data)
    rsi_period = 14

    # KDJ
    high = df['High'].rolling(window=ilong).max()
    low = df['Low'].rolling(window=ilong).min()
    rsv = (df['Close'] - low) / (high - low) * 100
    k = rsv.rolling(window=isig).mean()
    d = k.rolling(window=isig).mean()
    j = 3 * k - 2 * d

    # RSI
    if ta:
        rsi = ta.momentum.RSIIndicator(df['Close'], window=rsi_period).rsi()
    else:
        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0).rolling(rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(rsi_period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

    min_period = max(ilong, isig, rsi_period)
    signals = []
    for i in range(len(df)):
        if i < min_period:
            signals.append('HOLD')
        elif (j.iloc[i] > d.iloc[i] and j.iloc[i-1] < d.iloc[i-1]) and rsi.iloc[i] < rsi_oversold:
            signals.append('BUY')
        elif (j.iloc[i] < d.iloc[i] and j.iloc[i-1] > d.iloc[i-1]) and rsi.iloc[i] > rsi_overbought:
            signals.append('SELL')
        else:
            signals.append('HOLD')

    latest = signals[-1] if signals else 'HOLD'
    latest_rsi = float(rsi.iloc[-1]) if len(rsi) > 0 else 0

    return {
        "strategy": "KDJ + RSI Filter",
        "type": "Overbought/Oversold",
        "signal": latest,
        "details": f"J: {float(j.iloc[-1]):.1f} | D: {float(d.iloc[-1]):.1f} | RSI: {latest_rsi:.1f}",
        "params": {"ilong": ilong, "isig": isig, "rsi_overbought": rsi_overbought, "rsi_oversold": rsi_oversold},
    }


# ─────────────────────────────────────────────
#  Strategy 3: Stochastic + Bollinger Bands
# ─────────────────────────────────────────────
def strategy_stochastic_bollinger(data: pd.DataFrame,
                                   std_dev: float = 2.0,
                                   overbought: int = 80,
                                   oversold: int = 20) -> dict:
    """
    Stochastic Oscillator + Bollinger Bands — Support/Resistance.
    BUY when price < lower BB and stochastic < oversold.
    """
    df = _data_process(data)
    lookback = 14
    sma_period = 14

    sma = df['Close'].rolling(window=sma_period).mean()
    std = df['Close'].rolling(window=sma_period).std()
    lower_bb = sma - std_dev * std
    upper_bb = sma + std_dev * std

    lowest = df['Low'].rolling(window=lookback).min()
    highest = df['High'].rolling(window=lookback).max()
    stoch = 100 * ((df['Close'] - lowest) / (highest - lowest))

    signals = []
    for i in range(len(df)):
        if i < max(lookback, sma_period):
            signals.append('HOLD')
        elif stoch.iloc[i] < oversold and df['Close'].iloc[i] < lower_bb.iloc[i]:
            signals.append('BUY')
        elif stoch.iloc[i] > overbought and df['Close'].iloc[i] > upper_bb.iloc[i]:
            signals.append('SELL')
        else:
            signals.append('HOLD')

    latest = signals[-1] if signals else 'HOLD'

    return {
        "strategy": "Stochastic + Bollinger Bands",
        "type": "Support/Resistance",
        "signal": latest,
        "details": f"Stoch: {float(stoch.iloc[-1]):.1f} | Lower BB: {float(lower_bb.iloc[-1]):.2f} | Upper BB: {float(upper_bb.iloc[-1]):.2f}",
        "params": {"std_dev": std_dev, "overbought": overbought, "oversold": oversold},
    }


# ─────────────────────────────────────────────
#  Strategy 4: Mean Reversion (Z-Score)
# ─────────────────────────────────────────────
def strategy_mean_reversion(data: pd.DataFrame, z_threshold: float = 1.0) -> dict:
    """
    Mean Reversion — Z-Score based.
    BUY when z-score < -threshold; SELL when > threshold.
    """
    df = _data_process(data)
    lookback = 14

    mean = df['Close'].rolling(window=lookback).mean()
    std = df['Close'].rolling(window=lookback).std()
    z_score = (df['Close'] - mean) / std

    signals = []
    for i in range(len(df)):
        if i < lookback:
            signals.append('HOLD')
        elif z_score.iloc[i] < -z_threshold:
            signals.append('BUY')
        elif z_score.iloc[i] > z_threshold:
            signals.append('SELL')
        else:
            signals.append('HOLD')

    latest = signals[-1] if signals else 'HOLD'
    latest_z = float(z_score.iloc[-1]) if len(z_score) > 0 else 0

    return {
        "strategy": "Mean Reversion",
        "type": "Mean Reversion",
        "signal": latest,
        "details": f"Z-Score: {latest_z:.2f} (threshold: ±{z_threshold})",
        "params": {"z_threshold": z_threshold, "lookback": lookback},
    }


# ─────────────────────────────────────────────
#  Strategy 5: Mean Reversion + ATR
# ─────────────────────────────────────────────
def strategy_mean_reversion_atr(data: pd.DataFrame,
                                 atr_length: int = 7,
                                 atr_multiplier: float = 2.0,
                                 len_volat: int = 7,
                                 len_drift: int = 7) -> dict:
    """
    Mean Reversion + ATR — Volatility anomaly + trend confirmation.
    BUY when ATR diverges significantly AND trend is upward.
    SELL when price drops below trailing stop.
    """
    df = _data_process(data)

    # ATR
    if ta:
        atr = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], window=atr_length)
    else:
        tr = pd.concat([
            df['High'] - df['Low'],
            (df['High'] - df['Close'].shift(1)).abs(),
            (df['Low'] - df['Close'].shift(1)).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(window=atr_length).mean()

    # ATR divergence
    avg_atr = atr.rolling(window=len_volat).mean()
    std_atr = atr.rolling(window=len_volat).std()
    diverted = (atr > (avg_atr + std_atr)) | (atr < (avg_atr - std_atr))

    # Log-normal drift (trend predictor)
    log_return = np.log(df['Close'] / df['Close'].shift(1))
    drift = log_return.rolling(window=len_drift).mean() - 0.5 * log_return.rolling(window=len_drift).std() ** 2
    uptrend = ((drift > drift.shift(1)) & (drift > drift.shift(2))) | (drift > 0)

    # Combined entry
    entry = diverted & uptrend

    # Trailing stop
    trailing_stop = df['Low'] - (atr * atr_multiplier)
    trailing_stop_max = trailing_stop.cummax()

    latest_entry = bool(entry.iloc[-1]) if len(entry) > 0 else False
    latest_stop_breach = bool(df['Low'].iloc[-1] < trailing_stop_max.iloc[-1]) if len(df) > 0 else False

    if latest_stop_breach:
        signal = "SELL"
        reason = "Price below trailing stop — risk management exit"
    elif latest_entry:
        signal = "BUY"
        reason = "ATR divergence + upward drift confirmed — volatile entry"
    else:
        signal = "HOLD"
        reason = "No ATR divergence or trend confirmation"

    return {
        "strategy": "Mean Reversion + ATR",
        "type": "Volatility + Trend",
        "signal": signal,
        "details": reason,
        "params": {"atr_length": atr_length, "atr_multiplier": atr_multiplier},
    }


# ─────────────────────────────────────────────
#  Master Runner
# ─────────────────────────────────────────────
def run_all_strategies(price_df: pd.DataFrame) -> dict:
    """
    Run all 5 strategies on price data.
    Returns consensus signal + individual strategy results.
    """
    results = []
    for fn in [strategy_macd, strategy_kdj_rsi, strategy_stochastic_bollinger,
               strategy_mean_reversion, strategy_mean_reversion_atr]:
        try:
            results.append(fn(price_df))
        except Exception as e:
            results.append({"strategy": fn.__name__, "signal": "ERROR", "details": str(e)})

    # Consensus
    buy_count = sum(1 for r in results if r.get("signal") == "BUY")
    sell_count = sum(1 for r in results if r.get("signal") == "SELL")
    hold_count = sum(1 for r in results if r.get("signal") == "HOLD")

    if buy_count > sell_count and buy_count > hold_count:
        consensus = "BUY"
    elif sell_count > buy_count and sell_count > hold_count:
        consensus = "SELL"
    else:
        consensus = "HOLD"

    return {
        "consensus_signal": consensus,
        "vote_summary": f"BUY:{buy_count} | SELL:{sell_count} | HOLD:{hold_count}",
        "strategies": results,
    }
