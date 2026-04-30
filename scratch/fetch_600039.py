import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

ticker = "600039.SS"
stock = yf.Ticker(ticker)

print("--- Recent Price Action ---")
hist = stock.history(period="1mo")
print(hist[['Open', 'High', 'Low', 'Close', 'Volume']].tail(10))

print("\n--- Financials ---")
try:
    print(stock.info.get('trailingPE', 'N/A'), "PE")
    print(stock.info.get('dividendYield', 'N/A'), "Div Yield")
    print(stock.info.get('marketCap', 'N/A'), "Market Cap")
except Exception as e:
    print(e)
