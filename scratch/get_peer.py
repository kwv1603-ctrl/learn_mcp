import sys, asyncio, json
sys.path.append('/Users/dap/Documents/work/project/python/finance/learn_mcp')
import finance_mcp_tools as mcp

async def main():
    res = await mcp.handle_get_peer_comparison('META', peers=['GOOGL', 'SNAP', 'PINS'])
    print(json.dumps(res, indent=2))

asyncio.run(main())
