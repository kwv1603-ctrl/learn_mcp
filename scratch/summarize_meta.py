import json

with open('scratch/meta_data.json', 'r') as f:
    d = json.load(f)

print("--- BUFFETT ANALYSIS ---")
print(json.dumps(d.get('buffett', {}), indent=2))

print("\n--- VALUATION ---")
print(json.dumps(d.get('valuation', {}), indent=2))

print("\n--- STRATEGY ---")
print(json.dumps(d.get('strategy', {}), indent=2))

print("\n--- REFLECTION ---")
print(json.dumps(d.get('reflection', {}), indent=2))

print("\n--- PEER ---")
print(json.dumps(d.get('peer', {}), indent=2))

