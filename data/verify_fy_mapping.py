import json

with open(r"C:\Users\telha\Databridge_AI\data\apple_sec_facts.json", "r") as f:
    data = json.load(f)

us_gaap = data.get("facts", {}).get("us-gaap", {})

# Let's look at Revenue in detail to understand the FY mapping
concept = us_gaap.get("RevenueFromContractWithCustomerExcludingAssessedTax", {})
units = concept.get("units", {}).get("USD", [])

print("Revenue 10-K entries (all fiscal years):")
print(f"{'fy':>4} {'fp':>4} {'end':>12} {'filed':>12} {'val':>20} {'frame':>12}")
print("-" * 80)
for entry in units:
    if entry.get("form") == "10-K" and entry.get("fp") == "FY":
        fy = entry.get("fy", "")
        fp = entry.get("fp", "")
        end = entry.get("end", "")
        filed = entry.get("filed", "")
        val = entry.get("val", 0)
        frame = entry.get("frame", "")
        print(f"{fy:>4} {fp:>4} {end:>12} {filed:>12} {val:>20,.0f} {frame:>12}")

print("\n\nBalance Sheet - Assets 10-K entries:")
concept = us_gaap.get("Assets", {})
units = concept.get("units", {}).get("USD", [])
print(f"{'fy':>4} {'fp':>4} {'end':>12} {'filed':>12} {'val':>20} {'frame':>12}")
print("-" * 80)
for entry in units:
    if entry.get("form") == "10-K" and entry.get("fp") == "FY":
        fy = entry.get("fy", "")
        fp = entry.get("fp", "")
        end = entry.get("end", "")
        filed = entry.get("filed", "")
        val = entry.get("val", 0)
        frame = entry.get("frame", "")
        print(f"{fy:>4} {fp:>4} {end:>12} {filed:>12} {val:>20,.0f} {frame:>12}")

print("\n\nNOTE: Apple fiscal year ends in September.")
print("FY2021 in SEC = Apple's fiscal year ending Sep 2019 (for income stmt, covers 3 years back)")
print("The 'end' date is what matters. For income statement items (cumulative), the 'end' date")
print("shows the period end. For balance sheet items (point-in-time), 'end' is the balance date.")
print()
print("Let me also check if there are more recent entries with fy=2025...")

# Check for fy 2025
for concept_name in ["RevenueFromContractWithCustomerExcludingAssessedTax", "Assets", "NetIncomeLoss"]:
    concept = us_gaap.get(concept_name, {})
    units = concept.get("units", {}).get("USD", [])
    for entry in units:
        if entry.get("form") == "10-K" and entry.get("fp") == "FY" and entry.get("fy", 0) >= 2025:
            print(f"  {concept_name}: fy={entry['fy']}, end={entry['end']}, val={entry['val']:,.0f}")
