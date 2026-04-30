import yfinance as yf

ticker = "600039.SS"
stock = yf.Ticker(ticker)

try:
    info = stock.info
    pb = info.get('priceToBook', 'N/A')
    bvps = info.get('bookValue', 'N/A')
    print(f"Current PB: {pb}")
    print(f"Book Value Per Share: {bvps}")
except Exception as e:
    print(e)
