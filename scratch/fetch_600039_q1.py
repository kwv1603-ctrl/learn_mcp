import yfinance as yf
import pandas as pd

ticker = "600039.SS"
stock = yf.Ticker(ticker)

print("--- Quarterly Cash Flow ---")
try:
    qcf = stock.quarterly_cashflow
    if not qcf.empty:
        print(qcf.loc[['Operating Cash Flow', 'Free Cash Flow']].head())
    else:
        print("No quarterly cash flow data.")
except Exception as e:
    print(f"Error getting cash flow: {e}")

print("\n--- Quarterly Balance Sheet ---")
try:
    qbs = stock.quarterly_balancesheet
    if not qbs.empty:
        # Looking for Accounts Receivable or similar
        ar_keys = [k for k in qbs.index if 'Receivable' in k or 'receivable' in k]
        if ar_keys:
            print(qbs.loc[ar_keys].head())
        else:
            print("No accounts receivable key found. Available keys:", qbs.index.tolist()[:10])
    else:
        print("No quarterly balance sheet data.")
except Exception as e:
    print(f"Error getting balance sheet: {e}")
