import sys, asyncio, json
import pandas as pd
sys.path.append('/Users/dap/Documents/work/project/python/finance/learn_mcp')
import finance_mcp_tools as mcp

def default_serialize(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return str(obj)

def convert_keys(obj):
    if isinstance(obj, dict):
        return {str(k): convert_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_keys(i) for i in obj]
    else:
        return obj

async def main():
    res = {}
    res['statements'] = await mcp.handle_get_financial_statements('META')
    res['peer'] = await mcp.handle_get_peer_comparison('META')
    res['valuation'] = await mcp.handle_get_stock_valuation('META')
    res['buffett'] = await mcp.handle_run_buffett_analysis('META')
    res['reflection'] = await mcp.handle_run_finagent_reflection('META')
    res['strategy'] = await mcp.handle_run_finagent_strategy_scan('META')
    
    with open('scratch/meta_data.json', 'w') as f:
        json.dump(convert_keys(res), f, indent=2, default=default_serialize)

asyncio.run(main())
