"""
Extract Apple SEC EDGAR data correctly, mapping by actual fiscal year end dates.

Apple fiscal years (end in late September):
- FY2021: ends 2021-09-25 (filed 2021-10-29)
- FY2022: ends 2022-09-24 (filed 2022-10-28)
- FY2023: ends 2023-09-30 (filed 2023-11-03)
- FY2024: ends 2024-09-28 (filed 2024-11-01)
- FY2025: ends 2025-09-27 (filed 2025-10-31)
"""
import json

with open(r"T:\Users\telha\Databridge_AI_Source\data\apple_sec_facts.json", "r") as f:
    data = json.load(f)

us_gaap = data.get("facts", {}).get("us-gaap", {})

# Apple fiscal year end dates
APPLE_FY_ENDS = {
    "FY2020": "2020-09-26",
    "FY2021": "2021-09-25",
    "FY2022": "2022-09-24",
    "FY2023": "2023-09-30",
    "FY2024": "2024-09-28",
    "FY2025": "2025-09-27",
}

TARGET_FYS = ["FY2021", "FY2022", "FY2023", "FY2024", "FY2025"]


def extract_by_end_date(concept_name, unit_type="USD"):
    """Extract 10-K values matched by actual fiscal year end date."""
    concept_data = us_gaap.get(concept_name)
    if not concept_data:
        return None

    label = concept_data.get("label", concept_name)
    units_data = concept_data.get("units", {})

    if unit_type not in units_data:
        # Try alternative unit types
        for alt in ["USD", "shares", "USD/shares"]:
            if alt in units_data:
                unit_type = alt
                break
        else:
            return None

    entries = units_data[unit_type]

    results = {}
    for fy_label, end_date in APPLE_FY_ENDS.items():
        if fy_label not in TARGET_FYS:
            continue
        # Find 10-K FY entry matching this end date, use latest filed
        matches = [
            e for e in entries
            if e.get("form") == "10-K"
            and e.get("fp") == "FY"
            and e.get("end") == end_date
        ]
        if matches:
            # Take the most recently filed version
            best = max(matches, key=lambda x: x.get("filed", ""))
            results[fy_label] = {
                "val": best["val"],
                "end": best["end"],
                "filed": best["filed"],
                "unit": unit_type,
            }

    if not results:
        return None

    return {"concept": concept_name, "label": label, "unit_type": unit_type, "values": results}


# ============================================================
# INCOME STATEMENT CONCEPTS
# ============================================================
income_concepts = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "CostOfGoodsAndServicesSold",
    "GrossProfit",
    "OperatingExpenses",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "ResearchAndDevelopmentExpense",
    "SellingGeneralAndAdministrativeExpense",
    "IncomeTaxExpenseBenefit",
    "EarningsPerShareBasic",
    "EarningsPerShareDiluted",
    "InterestExpense",
    "OtherNonoperatingIncomeExpense",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
]

# ============================================================
# BALANCE SHEET CONCEPTS
# ============================================================
balance_concepts = [
    "Assets",
    "AssetsCurrent",
    "AssetsNoncurrent",
    "Liabilities",
    "LiabilitiesCurrent",
    "LiabilitiesNoncurrent",
    "StockholdersEquity",
    "CashAndCashEquivalentsAtCarryingValue",
    "MarketableSecuritiesCurrent",
    "AccountsReceivableNetCurrent",
    "InventoryNet",
    "PropertyPlantAndEquipmentNet",
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "LongTermDebtCurrent",
    "CommercialPaper",
    "CommonStockSharesOutstanding",
    "RetainedEarningsAccumulatedDeficit",
    "LiabilitiesAndStockholdersEquity",
]

# ============================================================
# CASH FLOW CONCEPTS
# ============================================================
cashflow_concepts = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInInvestingActivities",
    "NetCashProvidedByUsedInFinancingActivities",
    "PaymentsForRepurchaseOfCommonStock",
    "PaymentsOfDividends",
    "DepreciationDepletionAndAmortization",
]


def print_section(title, concepts):
    print(f"\n{'=' * 100}")
    print(f"  {title}")
    print(f"{'=' * 100}")
    found = {}
    for concept in concepts:
        result = extract_by_end_date(concept)
        if result:
            found[concept] = result
            unit = result["unit_type"]
            print(f"\n  {result['concept']}")
            print(f"  Label: {result['label']}")
            for fy in TARGET_FYS:
                if fy in result["values"]:
                    v = result["values"][fy]
                    if unit == "USD":
                        print(f"    {fy} (end {v['end']}): ${v['val']:>20,.0f}")
                    elif unit == "shares":
                        print(f"    {fy} (end {v['end']}): {v['val']:>20,.0f} shares")
                    elif unit == "USD/shares":
                        print(f"    {fy} (end {v['end']}): ${v['val']:>10.2f} /share")
        else:
            print(f"\n  {concept} --> NOT FOUND")
    return found


print("APPLE INC. (CIK: 0000320193) - SEC EDGAR COMPANY FACTS")
print("Fiscal years ending in September")
print(f"Target years: {', '.join(TARGET_FYS)}")

income_found = print_section("INCOME STATEMENT", income_concepts)
balance_found = print_section("BALANCE SHEET", balance_concepts)
cashflow_found = print_section("CASH FLOW STATEMENT", cashflow_concepts)


# ============================================================
# GENERATE CLEAN HARDCODED PYTHON DICT
# ============================================================
print("\n\n")
print("=" * 100)
print("HARDCODED FALLBACK DATA (copy-paste ready Python dict)")
print("=" * 100)
print()

def gen_dict_section(name, found_data, concepts):
    print(f'    "{name}": {{')
    for concept in concepts:
        if concept in found_data:
            r = found_data[concept]
            unit = r["unit_type"]
            print(f'        "{concept}": {{')
            print(f'            "label": "{r["label"]}",')
            for fy in TARGET_FYS:
                if fy in r["values"]:
                    v = r["values"][fy]
                    val = v["val"]
                    if unit == "USD":
                        print(f'            "{fy}": {val},  # ${val:,.0f}')
                    elif unit == "shares":
                        print(f'            "{fy}": {val},  # {val:,.0f} shares')
                    elif unit == "USD/shares":
                        print(f'            "{fy}": {val},  # ${val:.2f}/share')
            print(f'        }},')
    print(f'    }},')

print('APPLE_FINANCIALS = {')
print('    "entity": "Apple Inc.",')
print('    "cik": "0000320193",')
print('    "fiscal_year_end": "Late September",')
print('    "fy_end_dates": {')
for fy, end in APPLE_FY_ENDS.items():
    if fy in TARGET_FYS:
        print(f'        "{fy}": "{end}",')
print('    },')
gen_dict_section("income_statement", income_found, income_concepts)
gen_dict_section("balance_sheet", balance_found, balance_concepts)
gen_dict_section("cash_flow", cashflow_found, cashflow_concepts)
print('}')
