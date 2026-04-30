import yfinance as yf
import pandas as pd

tickers = {
    "四川路桥 (600039)": "600039.SS",
    "山东路桥 (000498)": "000498.SZ",
    "浙江交科 (002061)": "002061.SZ",
    "安徽建工 (600502)": "600502.SS",
    "中国交建 (601800)": "601800.SS",
    "中国铁建 (601186)": "601186.SS"
}

results = []

for name, symbol in tickers.items():
    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        pe = info.get('trailingPE', 'N/A')
        div_yield = info.get('dividendYield', 'N/A')
        if div_yield != 'N/A' and div_yield is not None:
            div_yield = f"{div_yield * 100:.2f}%"
        
        results.append({
            "公司": name,
            "PE(TTM)": round(pe, 2) if isinstance(pe, (int, float)) else pe,
            "股息率": div_yield
        })
    except Exception as e:
        results.append({"公司": name, "PE(TTM)": "Error", "股息率": "Error"})

df = pd.DataFrame(results)
print(df.to_string(index=False))
