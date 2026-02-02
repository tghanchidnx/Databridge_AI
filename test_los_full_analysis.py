"""
Test AI SQL Analysis on Full Upstream LOS Query.
"""

from ai_sql_orchestrator_standalone import ai_analyze_sql, create_coa_enrichment

LOS_SQL = """
-- LOS:
SELECT
    CASE
        WHEN accts.account_code = '640-990'
        AND entries.transaction_description ILIKE '%COPAS%' THEN '640-992'
        ELSE accts.account_code
    END AS acctcode,
    CASE
        WHEN entries.transaction_description LIKE '%PAC %'
        AND accts.account_code = '641-990' THEN 'PAC990'
        WHEN entries.transaction_description LIKE '%NONOP LOE %'
        AND accts.account_code IN ('641-990', '640-990') THEN 'NLOE990'
        WHEN entries.transaction_description ILIKE ANY ('%WOX %', '% LOE %')
        AND accts.account_code = '641-990' THEN 'WOX990'
        WHEN accts.account_code = '641-990' THEN 'WOX990'
        WHEN entries.transaction_description LIKE '%REST FEE%'
        AND accts.account_code = '640-990' THEN 'LOE722'
        WHEN (
            entries.transaction_description LIKE '% LOE %'
            AND accts.account_code = '641-990'
        )
        OR (
            accts.account_code = '640-990'
            AND entries.transaction_description NOT LIKE '%COPAS%'
        ) THEN 'LOE990'
        ELSE billcats.billcat
    END AS adjbillcat,
    CASE
        WHEN props.cost_center_state ILIKE ANY ('%UNKNOWN%', '%N/A%') THEN NULL
        WHEN props.cost_center_state IN ('AR', 'MS') THEN 'LA'
        ELSE props.cost_center_state
    END AS state,
    CASE
        WHEN entries.product_code = 'N/A' THEN NULL
        ELSE entries.product_code
    END AS productid,
    CASE
        WHEN account_code ILIKE '101%' THEN 'Cash'
        WHEN account_code ILIKE '125%' THEN 'Affiliate AR'
        WHEN account_code ILIKE ANY ('11%', '12%') THEN 'AR'
        WHEN account_code ILIKE '13%' THEN 'Prepaid Expenses'
        WHEN account_code ILIKE '14%' THEN 'Inventory'
        WHEN account_code ILIKE ANY ('15%', '16%') THEN 'Other Current Assets'
        WHEN account_code ILIKE '17%' THEN 'Derivative Assets'
        WHEN account_code ILIKE '18%' THEN 'Deferred Tax Assets'
        WHEN account_code IN (
            '205-102', '205-106', '205-112', '205-116', '205-117',
            '205-152', '205-190', '205-202', '205-206', '205-252',
            '205-990', '210-110', '210-140', '210-990', '215-110',
            '215-990', '220-110', '220-990', '225-110', '225-140',
            '225-990', '230-110', '230-140', '230-990', '232-200',
            '232-210', '235-105', '235-110', '235-115', '235-116',
            '235-120', '235-250', '235-275', '240-110', '240-140',
            '240-990', '242-150', '242-160', '244-110', '244-140',
            '244-410', '244-440', '244-560', '244-590', '244-610',
            '244-640', '244-990', '244-995', '244-998', '245-202',
            '245-206', '245-227', '245-252', '245-302', '245-306',
            '245-402', '245-412', '245-602', '245-902', '245-906',
            '246-312', '246-346', '246-322', '246-316'
        ) THEN 'Capex'
        WHEN account_code ILIKE ANY ('25%', '26%', '27%', '28%') THEN 'Other Assets'
        WHEN account_code ILIKE '29%' THEN 'Accumulated DD&A'
        WHEN account_code ILIKE ANY ('30%', '31%', '32%', '33%', '34%') THEN 'AP'
        WHEN account_code ILIKE ANY ('35%', '45%') THEN 'Total Debt'
        WHEN account_code ILIKE ANY ('36%', '37%', '38%') THEN 'Other Current Liabilities'
        WHEN account_code ILIKE ANY ('44%', '46%', '47%', '48%') THEN 'Other Liabilities'
        WHEN account_code ILIKE '49%' THEN 'Equity'
        WHEN account_code ILIKE '501%' THEN 'Oil Sales'
        WHEN account_code ILIKE ANY ('502%', '510%') THEN 'Gas Sales'
        WHEN account_code ILIKE ANY ('503%', '512%', '513%') THEN 'NGL Sales'
        WHEN account_code ILIKE ANY ('504%', '520%', '570%', '590-100', '590-110', '590-410', '590-510', '590-900') THEN 'Other Income'
        WHEN account_code ILIKE ANY ('514%', '519%', '518-120', '590-710', '674%', '695%') THEN 'Cashouts'
        WHEN account_code IN ('515-100', '515-990') THEN 'Service Revenue'
        WHEN account_code IN ('515-110', '515-199', '610-110', '610-120', '610-130') THEN 'Gathering Fees'
        WHEN account_code IN ('515-120', '613-110', '613-120') THEN 'Compression Fees'
        WHEN account_code IN ('515-130', '515-140', '612-110', '612-120', '614-110', '614-120', '619-990') THEN 'Treating Fees'
        WHEN account_code IN ('515-210', '610-210', '610-220') THEN 'Capital Recovery Fees'
        WHEN account_code = '517-110' THEN 'Demand Fees'
        WHEN account_code ILIKE ANY ('517%', '611-210', '611-220', '613-130', '613-140', '619-110', '619-120', '619-275', '619-991') THEN 'Transportation Fees'
        WHEN account_code ILIKE '518%' THEN 'Gas Sales'
        WHEN account_code = '530-100' THEN 'Service Income'
        WHEN account_code IN ('530-110', '530-111', '530-990') THEN 'Sand Sales'
        WHEN account_code IN ('515-205', '530-120', '530-140', '530-720', '530-990', '530-991', '530-993', '590-310') THEN 'Rental Income'
        WHEN account_code IN ('530-150', '530-994', '590-620') THEN 'Water Income'
        WHEN account_code IN ('530-160', '530-995', '590-610') THEN 'SWD Income'
        WHEN account_code IN ('530-170', '530-996') THEN 'Consulting Income'
        WHEN account_code IN ('530-180', '530-997') THEN 'Fuel Income'
        WHEN account_code ILIKE '580%' THEN 'Hedge Gains'
        WHEN account_code = '581-540' THEN 'Interest Hedge Gains'
        WHEN account_code ILIKE '581%' THEN 'Unrealized Hedge Gains'
        WHEN account_code IN ('590-210', '590-211', '590-990', '640-992') THEN 'COPAS'
        WHEN account_code = '590-555' THEN 'Compressor Recovery Income'
        WHEN account_code = '590-850' THEN 'Rig Termination Penalties'
        WHEN account_code = '590-991' THEN 'Gathering Fee Income'
        WHEN account_code IN ('601-100', '601-110', '601-113', '601-120', '601-123', '601-275', '601-990') THEN 'Oil Severance Taxes'
        WHEN account_code IN ('602-100', '602-110', '602-113', '602-120', '602-123', '602-275', '602-990') THEN 'Gas Severance Taxes'
        WHEN account_code IN ('603-100', '603-110', '603-113', '603-120', '603-123', '603-275', '603-990') THEN 'NGL Severance Taxes'
        WHEN account_code IN ('601-112', '601-122', '602-112', '602-122', '603-112', '603-122', '640-120', '640-991') THEN 'Ad Valorem Taxes'
        WHEN account_code IN ('601-111', '601-121') THEN 'Oil Conservation Taxes'
        WHEN account_code IN ('602-111', '602-121') THEN 'Gas Conservation Taxes'
        WHEN account_code IN ('603-111', '603-121') THEN 'NGL Conservation Taxes'
        WHEN account_code = '611-110' THEN 'Commodity Fees'
        WHEN account_code ILIKE ANY ('611-120', '62%') THEN 'Non-Op Fees'
        WHEN account_code ILIKE ANY ('630-1%', '710-170', '710-996') THEN 'Consulting Expenses'
        WHEN account_code IN ('640-110', '640-100', '640-275', '640-300', '640-990', '641-110', '641-100', '641-990') THEN 'Lease Operating Expenses'
        WHEN account_code IN ('641-150', '963-100') THEN 'Accretion Expense'
        WHEN account_code ILIKE '642%' THEN 'Leasehold Expenses'
        WHEN account_code ILIKE '645%' THEN 'Exploration Expenses'
        WHEN account_code ILIKE ANY ('650%', '660%', '667%', '668%') THEN 'Third Party Fees Paid'
        WHEN account_code ILIKE ANY ('665%', '666%', '669%') THEN 'Midstream Operating Expenses'
        WHEN account_code ILIKE ANY ('670%', '680%', '690%', '691%') THEN 'Cost of Purchased Gas'
        WHEN account_code IN ('700-100', '700-110', '700-800', '700-990', '701-100', '701-110') THEN 'Sand Purchases'
        WHEN account_code IN ('700-150', '700-994') THEN 'Water Purchases'
        WHEN account_code IN ('700-180', '700-997') THEN 'Fuel Purchases'
        WHEN account_code IN ('710-100', '710-120', '710-140', '710-300', '710-301', '710-991', '710-992', '710-993', '720-120', '720-985') THEN 'Rental Expenses'
        WHEN account_code IN ('710-110', '710-990') THEN 'Sand Expenses'
        WHEN account_code IN ('710-150', '710-994') THEN 'Water Expenses'
        WHEN account_code IN ('710-160', '710-995') THEN 'SWD Expenses'
        WHEN account_code IN ('710-180', '710-997') THEN 'Fuel Expenses'
        WHEN account_code ILIKE '8%' THEN 'General & Administrative'
        WHEN account_code IN ('910-100', '920-100') THEN 'Interest Income'
        WHEN account_code ILIKE ANY ('93%', '94%') THEN 'Other Gains/Losses'
        WHEN account_code ILIKE ANY ('950-1%', '950-300') THEN 'Interest Expense'
        WHEN account_code ILIKE ANY ('950-8%', '955%', '970%', '972%', '974%', '975%') THEN 'DD&A'
        WHEN account_code = '971-210' THEN 'Impairment Expense'
        WHEN account_code = '960-100' THEN 'Bad Debt Expense'
        WHEN account_code = '965-200' THEN 'Other Expenses'
    END AS gl,
    CASE
        WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', 'FC85%', 'NICC%', 'NIDC%') THEN 'CNOP'
        WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
        WHEN account_billing_category_code ILIKE ANY ('%950%', '%960%') THEN 'CACQ'
        WHEN account_billing_category_type_code IN ('ICC', 'TCC') THEN 'CFRC'
        WHEN account_billing_category_type_code IN ('IDC', 'TDC') THEN 'CDRL'
        WHEN account_billing_category_type_code IN ('IFC', 'TFC') THEN 'CFAC'
        WHEN account_billing_category_type_code IN ('LHD', 'LHU') THEN 'CLHD'
        WHEN account_billing_category_type_code = 'LHX' THEN 'LHD'
        WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%', 'MOE330', 'MOE345', 'MOE625') THEN 'LBR'
        WHEN account_billing_category_code ILIKE ANY ('LOE11%', 'LOE320', 'LOE321', 'LOE330', 'LOE331') THEN 'OHD'
        WHEN account_billing_category_code ILIKE ANY ('LOE140', 'LOE160', 'LOE161', 'LOE165', 'LOE166') THEN 'SVC'
        WHEN account_billing_category_code ILIKE ANY ('LOE24%', 'LOE25%', 'LOE26%', 'MOE24%', 'MOE26%', 'MOE27%') THEN 'CHM'
        WHEN account_billing_category_code IN ('LOE273', 'LOE274', 'LOE275', 'LOE276') THEN 'SWD'
        WHEN account_billing_category_code IN ('LOE295', 'LOE300', 'LOE301', 'LOE302', 'LOE303', 'LOE304', 'MOE295', 'MOE300', 'MOE301', 'MOE304', 'MOE320', 'MOE321', 'MOE420', 'MOE421') THEN 'RNM'
        WHEN account_billing_category_code IN ('MOE555', 'MOE556') THEN 'CMP'
        WHEN account_billing_category_code IN ('MOE340', 'MOE341') THEN 'HSE'
        WHEN account_billing_category_code ILIKE ANY ('LOE305', 'LOE306', 'LOE310', 'LOE311', 'LOE312', 'LOE325', 'LOE326', 'LOE5%', 'MOE5%', 'MOE110', 'MOE111', 'MOE115', 'MOE305', 'MOE306', 'MOE310', 'MOE311', 'MOE705') THEN 'EQU'
        WHEN account_billing_category_code ILIKE 'LOE6%' THEN 'COPAS'
        WHEN account_billing_category_code IN ('LOE720', 'LOE721') THEN 'SEV'
        WHEN account_billing_category_code IN ('LOE750', 'LOE751', 'NLOE750', 'MOE750', 'MOE751', 'MOE755') THEN 'ADV'
        WHEN account_billing_category_code ILIKE ANY ('LOE850', 'LOE851', 'LOE852', 'LOE853', 'LOE990', 'NLOE%') THEN 'NLOE'
        WHEN account_billing_category_type_code IN ('PAC', 'WOX', 'MOX') THEN account_billing_category_type_code
    END AS los_map,
    CASE
        WHEN cost_center_area_code IN ('AREA0100', 'AREA0200', 'AREA0220', 'AREA0300', 'AREA0301', 'AREA0430', 'AREA0470', 'AREA0600', 'AREA0625') THEN TRUE
        ELSE FALSE
    END AS AU_Op,
    CASE
        WHEN cost_center_area_code IN ('AREA0210', 'AREA0320', 'AREA0440', 'AREA0475', 'AREA0650') THEN TRUE
        ELSE FALSE
    END AS A3_Op,
    CASE
        WHEN cost_center_area_code IN ('AREA0250', 'AREA0350', 'AREA0440', 'AREA0475') THEN TRUE
        ELSE FALSE
    END AS AU_Nop,
    CASE
        WHEN cost_center_area_code IN ('AREA0330', 'AREA0340', 'AREA0430', 'AREA0470') THEN TRUE
        ELSE FALSE
    END AS A3_Nop,
    CASE
        WHEN corp_code IN (551, 561, 565, 578, 586, 587, 588, 598, 702, 755) THEN 'A3'
        WHEN corp_code IN (410, 420, 550, 560, 580, 585, 586, 590, 595, 599, 600, 650, 700, 701, 750, 751) THEN 'AU'
        WHEN corp_code IN (012, 043, 049, 052) THEN '4HC'
        ELSE NULL
    END AS fund,
    CASE
        WHEN corp_code IN (12, 43, 44, 540, 550, 551, 560, 561, 565, 650, 750, 751, 755) THEN 'Upstream'
        WHEN corp_code IN (41, 410, 578, 580, 585, 586, 587, 588, 590, 595) THEN 'Midstream'
        WHEN corp_code = 600 THEN 'Marketing'
        WHEN corp_code BETWEEN 700 AND 702 THEN 'Services'
        WHEN corp_code BETWEEN 597 AND 599 THEN 'Elim'
        ELSE NULL
    END AS Segment,
    CASE
        WHEN corp_code IN (550, 560, 580, 585, 590, 595, 599, 600, 650, 700, 701, 750, 751) THEN 1
        WHEN corp_code IN (410, 420) THEN 0.9
        WHEN corp_code = 586 THEN 0.225
        ELSE 0
    END AS AU_Stake,
    CASE
        WHEN corp_code IN (551, 561, 565, 587, 598, 702, 755) THEN 1
        WHEN corp_code IN (578, 587) THEN 0.9
        WHEN corp_code = 586 THEN 0.675
        ELSE 0
    END AS A3_Stake
FROM fact_financial_details
"""


def main():
    print("=" * 80)
    print("AI SQL ANALYSIS - FULL UPSTREAM LOS QUERY")
    print("=" * 80)

    # Configure COA enrichment
    coa_config = create_coa_enrichment(
        r"C:\Users\telha\Databridge_AI\Gemini\Uploads\DIM_ACCOUNT.csv",
        detail_columns=["ACCOUNT_ID", "ACCOUNT_NAME", "ACCOUNT_BILLING_CATEGORY_CODE", "ACCOUNT_CLASS"]
    )

    # Run analysis
    print("\nRunning AI analysis...")
    result = ai_analyze_sql(
        sql=LOS_SQL,
        client_id="demo_oilco",
        industry="oil_gas_upstream",
        export_path="./result_export",
        export_name="los_full_analysis",
        enrichment_config=coa_config,
    )

    print()
    print(f"Success: {result['success']}")
    print(f"Hierarchies Found: {result['summary']['hierarchies_found']}")
    print(f"Average Confidence: {result['summary']['average_confidence']:.0%}")
    print(f"Files Exported: {result['summary']['files_exported']}")
    print()

    print("HIERARCHIES DETECTED:")
    print("-" * 70)
    for h in result["hierarchies"]:
        rec = h["recommendation"][:30] if len(h["recommendation"]) > 30 else h["recommendation"]
        print(f"  {h['name']:30} | {h['confidence']:.0%} | {rec}")
    print()

    print("EXPORTED FILES:")
    print("-" * 70)
    for f in result["export"]["files"]:
        print(f"  {f}")
    print()

    # Show enrichment info
    enrichment = result.get("enrichment", {})
    if enrichment.get("detected_tables"):
        print("REFERENCE TABLES DETECTED:")
        print("-" * 70)
        for t in enrichment["detected_tables"]:
            print(f"  {t['display_name']} ({t['table_name']})")
            print(f"    Key column: {t['key_column']}")
            print(f"    Conditions: {t['condition_count']}")

    if enrichment.get("result"):
        er = enrichment["result"]
        print()
        print("ENRICHMENT RESULTS:")
        print("-" * 70)
        print(f"  Original rows: {er.get('original_rows', 'N/A')}")
        print(f"  Expanded rows: {er.get('expanded_rows', 'N/A')}")
        for src in er.get("enriched_sources", []):
            print(f"  {src['source']}: {src.get('matched_rows', 0)} matched")

    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
