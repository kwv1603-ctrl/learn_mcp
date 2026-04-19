# Warren Buffett Value Investing Skill

## Description
This skill leverages the Yahoo Finance MCP server to analyze stocks using Warren Buffett's and Benjamin Graham's value investing principles. 

## Instructions for the Agent
1. **Understand Key Metrics**: When asked to analyze a stock, use `get_stock_metrics` to retrieve P/E ratio, P/B ratio, market cap, and dividend yield. Look for low P/E (e.g., < 15) and low P/B (e.g., < 1.5).
2. **Review Financial Statements**: Use `get_financial_statements` to check the company's historical earnings and calculate free cash flow. Buffett looks for consistent earnings growth and a high return on equity (ROE).
3. **Calculate Intrinsic Value**: Always use `calculate_valuation` to compute the simplified Graham/Buffett intrinsic value of the stock based on its EPS and growth rates. Compare this against the current price to determine if there is a margin of safety.
4. **Sentiment and Moat Analysis**: Use `get_stock_news` to observe market sentiment, but remember Buffett ignores short-term noise. Try to infer if the company has a strong economic moat (competitive advantage) from the context of its sector and financial consistency.

## Output Format
- Begin with an overview of the company's fundamentals.
- Present the Intrinsic Value vs Current Price and margin of safety.
- Conclude with a Buffett-style assessment (e.g., "This company meets the value criteria because..." or "This company is overvalued and lacks a strong margin of safety.").
