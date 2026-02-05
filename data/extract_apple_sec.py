import json
import sys

with open(r"C:\Users\telha\Databridge_AI\data\apple_sec_facts.json", "r") as f:
    data = json.load(f)

print(f"Entity: {data.get('entityName', 'N/A')}")
print(f"CIK: {data.get('cik', 'N/A')}")
print()

# Get all us-gaap concepts
us_gaap = data.get("facts", {}).get("us-gaap", {})
print(f"Total US-GAAP concepts available: {len(us_gaap)}")
print()

# Target concepts - income statement
income_targets = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "CostOfGoodsAndServicesSold",
    "CostOfGoodsSold",
    "CostOfRevenue",
    "GrossProfit",
    "OperatingExpenses",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "ResearchAndDevelopmentExpense",
    "SellingGeneralAndAdministrativeExpense",
    "IncomeTaxExpenseBenefit",
    "EarningsPerShareBasic",
    "EarningsPerShareDiluted",
    "OperatingCostsAndExpenses",
    "InterestExpense",
    "InterestIncome",
    "OtherNonoperatingIncomeExpense",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
]

# Target concepts - balance sheet
balance_targets = [
    "Assets",
    "AssetsCurrent",
    "Liabilities",
    "LiabilitiesCurrent",
    "StockholdersEquity",
    "CashAndCashEquivalentsAtCarryingValue",
    "MarketableSecuritiesCurrent",
    "AccountsReceivableNetCurrent",
    "InventoryNet",
    "PropertyPlantAndEquipmentNet",
    "Goodwill",
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "LongTermDebtCurrent",
    "CommercialPaper",
    "CommonStockSharesOutstanding",
    "RetainedEarningsAccumulatedDeficit",
    "LiabilitiesAndStockholdersEquity",
    "AssetsNoncurrent",
    "LiabilitiesNoncurrent",
    "ShortTermBorrowings",
]

# Cash flow targets
cashflow_targets = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInInvestingActivities",
    "NetCashProvidedByUsedInFinancingActivities",
    "PaymentsForRepurchaseOfCommonStock",
    "PaymentsOfDividends",
    "DepreciationDepletionAndAmortization",
]

all_targets = income_targets + balance_targets + cashflow_targets

def extract_10k_values(concept_name):
    """Extract 10-K values for fiscal years 2021-2024"""
    concept_data = us_gaap.get(concept_name)
    if not concept_data:
        return None

    label = concept_data.get("label", concept_name)
    description = concept_data.get("description", "")
    units = concept_data.get("units", {})

    results = {}

    # Check USD units first, then "shares" for share counts, then "USD/shares" for EPS
    for unit_type in ["USD", "shares", "USD/shares"]:
        if unit_type in units:
            entries = units[unit_type]
            for entry in entries:
                if entry.get("form") == "10-K":
                    fy = entry.get("fy")
                    # We want fiscal years 2021-2024
                    if fy in [2021, 2022, 2023, 2024]:
                        # For income statement items, we want entries where fp == "FY" (full year)
                        fp = entry.get("fp", "")
                        if fp == "FY":
                            key = f"FY{fy}"
                            # Keep the most recent filing for each fiscal year
                            if key not in results or entry.get("filed", "") > results[key].get("filed", ""):
                                results[key] = {
                                    "val": entry["val"],
                                    "end": entry.get("end", ""),
                                    "filed": entry.get("filed", ""),
                                    "accn": entry.get("accn", ""),
                                    "unit": unit_type,
                                }
            if results:
                break  # Found data in this unit type

    if not results:
        return None

    return {
        "concept": concept_name,
        "label": label,
        "description": description[:100] if description else "",
        "values": results,
    }


# Also search for fuzzy matches in all concepts
print("=" * 100)
print("SEARCHING FOR RELEVANT CONCEPTS IN US-GAAP")
print("=" * 100)

# First, let's find all revenue-related concepts
revenue_keywords = ["revenue", "sales"]
print("\n--- Revenue-related concepts (with 10-K FY data) ---")
for concept_name in sorted(us_gaap.keys()):
    if any(kw in concept_name.lower() for kw in revenue_keywords):
        result = extract_10k_values(concept_name)
        if result and result["values"]:
            years = sorted(result["values"].keys())
            vals = [f"{y}: ${result['values'][y]['val']:,.0f}" for y in years if result['values'][y]['unit'] == 'USD']
            if vals:
                print(f"  {concept_name}: {', '.join(vals)}")

print("\n--- Cost/Expense-related concepts ---")
cost_keywords = ["costof", "operatingexpense", "researchand", "sellinggeneral", "operatingcost"]
for concept_name in sorted(us_gaap.keys()):
    if any(kw in concept_name.lower().replace(" ", "") for kw in cost_keywords):
        result = extract_10k_values(concept_name)
        if result and result["values"]:
            years = sorted(result["values"].keys())
            vals = [f"{y}: ${result['values'][y]['val']:,.0f}" for y in years if result['values'][y]['unit'] == 'USD']
            if vals:
                print(f"  {concept_name}: {', '.join(vals)}")

print("\n")
print("=" * 100)
print("DETAILED EXTRACTION FOR ALL TARGET CONCEPTS")
print("=" * 100)

sections = [
    ("INCOME STATEMENT", income_targets),
    ("BALANCE SHEET", balance_targets),
    ("CASH FLOW", cashflow_targets),
]

all_found = {}

for section_name, targets in sections:
    print(f"\n{'='*80}")
    print(f"  {section_name}")
    print(f"{'='*80}")

    for concept in targets:
        result = extract_10k_values(concept)
        if result:
            all_found[concept] = result
            print(f"\n  Concept: {concept}")
            print(f"  Label:   {result['label']}")

            for fy_key in sorted(result["values"].keys()):
                v = result["values"][fy_key]
                if v["unit"] == "USD":
                    print(f"    {fy_key}: ${v['val']:>20,.0f}  (end: {v['end']}, filed: {v['filed']})")
                elif v["unit"] == "shares":
                    print(f"    {fy_key}: {v['val']:>20,.0f} shares  (end: {v['end']}, filed: {v['filed']})")
                elif v["unit"] == "USD/shares":
                    print(f"    {fy_key}: ${v['val']:>20,.2f}/share  (end: {v['end']}, filed: {v['filed']})")
        else:
            print(f"\n  Concept: {concept}  --> NOT FOUND or no 10-K FY data")


# Generate hardcoded data structure
print("\n\n")
print("=" * 100)
print("HARDCODED FALLBACK DATA STRUCTURE (Python dict)")
print("=" * 100)
print()
print("apple_financials = {")
print('    "entity": "Apple Inc.",')
print('    "cik": "0000320193",')
print('    "income_statement": {')

for concept in income_targets:
    if concept in all_found:
        r = all_found[concept]
        vals = {}
        for fy_key in sorted(r["values"].keys()):
            v = r["values"][fy_key]
            vals[fy_key] = v["val"]
        if vals:
            print(f'        "{concept}": {{')
            print(f'            "label": "{r["label"]}",')
            for fy_key, val in sorted(vals.items()):
                unit = r["values"][fy_key]["unit"]
                if unit == "USD":
                    print(f'            "{fy_key}": {val},  # ${val:,.0f}')
                elif unit == "USD/shares":
                    print(f'            "{fy_key}": {val},  # ${val:.2f}/share')
                else:
                    print(f'            "{fy_key}": {val},')
            print(f'        }},')

print('    },')
print('    "balance_sheet": {')

for concept in balance_targets:
    if concept in all_found:
        r = all_found[concept]
        vals = {}
        for fy_key in sorted(r["values"].keys()):
            v = r["values"][fy_key]
            vals[fy_key] = v["val"]
        if vals:
            print(f'        "{concept}": {{')
            print(f'            "label": "{r["label"]}",')
            for fy_key, val in sorted(vals.items()):
                unit = r["values"][fy_key]["unit"]
                if unit == "USD":
                    print(f'            "{fy_key}": {val},  # ${val:,.0f}')
                elif unit == "shares":
                    print(f'            "{fy_key}": {val},  # {val:,.0f} shares')
                else:
                    print(f'            "{fy_key}": {val},')
            print(f'        }},')

print('    },')
print('    "cash_flow": {')

for concept in cashflow_targets:
    if concept in all_found:
        r = all_found[concept]
        vals = {}
        for fy_key in sorted(r["values"].keys()):
            v = r["values"][fy_key]
            vals[fy_key] = v["val"]
        if vals:
            print(f'        "{concept}": {{')
            print(f'            "label": "{r["label"]}",')
            for fy_key, val in sorted(vals.items()):
                unit = r["values"][fy_key]["unit"]
                if unit == "USD":
                    print(f'            "{fy_key}": {val},  # ${val:,.0f}')
                else:
                    print(f'            "{fy_key}": {val},')
            print(f'        }},')

print('    },')
print('}')
