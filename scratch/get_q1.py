import sys, asyncio, json
import pandas as pd
sys.path.append('/Users/dap/Documents/work/project/python/finance/learn_mcp')
import finance_mcp_tools as mcp
import yfinance as yf

meta = yf.Ticker("META")
q_income = meta.quarterly_income_stmt
if not q_income.empty:
    print("Quarterly Income Statement columns:")
    print(q_income.columns)
    
q_cf = meta.quarterly_cashflow
if not q_cf.empty:
    print("\nQuarterly Cash Flow columns:")
    print(q_cf.columns)

