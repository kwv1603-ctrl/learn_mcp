import json

with open('scratch/meta_data.json', 'r') as f:
    d = json.load(f)

cf = d.get('statements', {}).get('cash_flow', {})
if not cf:
    cf = d.get('statements', {}).get('cashflow', {})
    
print(json.dumps(cf.get('2025-12-31', {}), indent=2))
print(json.dumps(cf.get('2024-12-31', {}), indent=2))
