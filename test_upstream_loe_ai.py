"""
Test AI-Powered SQL Analysis with Upstream LOE State Query.

This tests the full AI orchestration pipeline with a complex
Oil & Gas Upstream LOE query including:
- State inference from cost_center_code
- Adjusted billing category (adjbillcat) reclassification
- Operated/Non-Op flags
- GL classification (75 account categories)
- Corporate hierarchies (fund, segment, stakes)
"""

from ai_sql_orchestrator_standalone import ai_analyze_sql, SQLAnalysisOrchestrator

# Upstream LOE Detail with State Inference SQL
UPSTREAM_LOE_STATE_SQL = """
SELECT
    CASE
        WHEN accts.account_code = '640-990' THEN accts.account_code
        ELSE accts.account_code
    END AS acctcode,
    accts.account_name AS acctdesc,
    CASE
        WHEN billcats.account_billing_category_code = 'PAC' AND accts.account_code = '640-990' THEN 'PAC990'
        WHEN billcats.account_billing_category_code = 'NLO' AND accts.account_code = '640-990' THEN 'NLOE990'
        WHEN billcats.account_billing_category_code = 'WOX' AND accts.account_code = '640-990' THEN 'WOX990'
        WHEN billcats.account_billing_category_code = 'LOE' AND accts.account_code ILIKE '722%' THEN 'LOE722'
        WHEN billcats.account_billing_category_code = 'LOE' AND accts.account_code = '640-990' THEN 'LOE990'
        ELSE billcats.billcat
    END AS adjbillcat,
    CASE
        WHEN props.cost_center_code IN ('0043', '0044', '0045', '0046', '0047', '0048', '0049', '0050', '0051', '0052', '0053', '0054', '0055', '0056', '0057', '0058', '0059', '0060', '0061', '0062', '0063', '0064', '0065', '0066', '0067', '0068', '0069', '0070', '0071', '0072', '0073', '0074', '0075', '0076', '0077', '0078', '0079', '0080', '0081', '0082', '0083', '0084', '0085') THEN 'TX'
        WHEN props.cost_center_area_code IN ('AREA0200', 'AREA0220', 'AREA0210') THEN 'TX'
        WHEN props.cost_center_code IN ('0101', '0102', '0103', '0104', '0105', '0106', '0107', '0108', '0109', '0110', '0111', '0112', '0113', '0114', '0115', '0116', '0117', '0118', '0119', '0120', '0121', '0122', '0123', '0124', '0125', '0126', '0127', '0128', '0129', '0130', '0131', '0132', '0133', '0134', '0135', '0136', '0137', '0138', '0139', '0140', '0141', '0142', '0143', '0144', '0145', '0146', '0147', '0148', '0149', '0150', '0151', '0152', '0153', '0154', '0155', '0156', '0157', '0158', '0159', '0160', '0161', '0162', '0163', '0164', '0165', '0166', '0167', '0168') THEN 'LA'
        WHEN props.cost_center_code IN ('0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211') THEN 'WY'
        ELSE 'Multiple'
    END AS state,
    CASE
        WHEN entries.product_code = 'OIL' THEN 'Oil'
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
        WHEN account_code IN ('205-102', '205-106', '205-112', '205-116', '205-117', '205-152', '205-190', '205-202', '205-206', '205-252', '205-990', '210-110', '210-140', '210-990', '215-110', '215-990', '220-110', '220-990', '225-110', '225-140', '225-990', '230-110', '230-140', '230-990', '232-200', '232-210', '235-105', '235-110', '235-115', '235-116', '235-120', '235-250', '235-275', '240-110', '240-140', '240-990', '242-150', '242-160', '244-110', '244-140', '244-410', '244-440', '244-560', '244-590', '244-610', '244-640', '244-990', '244-995', '244-998', '245-202', '245-206', '245-227', '245-252', '245-302', '245-306', '245-402', '245-412', '245-602', '245-902', '245-906', '246-312', '246-346', '246-322', '246-316') THEN 'Capex'
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
        WHEN billcats.account_billing_category_code ILIKE ANY ('OHD%') THEN 'OHD - Overhead'
        WHEN billcats.account_billing_category_code ILIKE ANY ('SVC%') THEN 'SVC - Service'
        WHEN billcats.account_billing_category_code = 'CHM' THEN 'CHM - Chemicals'
        WHEN billcats.account_billing_category_code = 'CMP' THEN 'CMP - Compression'
        WHEN billcats.account_billing_category_code = 'ELC' THEN 'ELC - Electricity'
        WHEN billcats.account_billing_category_code = 'ENV' THEN 'ENV - Environmental'
        WHEN billcats.account_billing_category_code = 'FUE' THEN 'FUE - Fuel'
        WHEN billcats.account_billing_category_code = 'GAT' THEN 'GAT - Gathering'
        WHEN billcats.account_billing_category_code = 'HAU' THEN 'HAU - Hauling'
        WHEN billcats.account_billing_category_code = 'INS' THEN 'INS - Insurance'
        WHEN billcats.account_billing_category_code = 'LAB' THEN 'LAB - Labor'
        WHEN billcats.account_billing_category_code = 'LOE' THEN 'LOE - Lease Operating'
        WHEN billcats.account_billing_category_code = 'MKT' THEN 'MKT - Marketing'
        WHEN billcats.account_billing_category_code = 'NLO' THEN 'NLO - Non-LOE'
        WHEN billcats.account_billing_category_code = 'OTH' THEN 'OTH - Other'
        WHEN billcats.account_billing_category_code = 'PAC' THEN 'PAC - Production Accounting'
        WHEN billcats.account_billing_category_code = 'REP' THEN 'REP - Repairs'
        WHEN billcats.account_billing_category_code = 'RNT' THEN 'RNT - Rentals'
        WHEN billcats.account_billing_category_code = 'SWD' THEN 'SWD - Salt Water Disposal'
        WHEN billcats.account_billing_category_code = 'TAX' THEN 'TAX - Taxes'
        WHEN billcats.account_billing_category_code = 'TRN' THEN 'TRN - Transportation'
        WHEN billcats.account_billing_category_code = 'WOX' THEN 'WOX - Workover'
    END AS los_map,
    CASE
        WHEN props.cost_center_code IN ('0500', '0501', '0502', '0503', '0504', '0505') THEN 'Allocated'
        ELSE 'Direct'
    END AS case_column_6,
    CASE
        WHEN props.cost_center_area_code IN ('AREA0100', 'AREA0110', 'AREA0120', 'AREA0130', 'AREA0140', 'AREA0150', 'AREA0160', 'AREA0170', 'AREA0180') THEN 1
        ELSE 0
    END AS AU_Op,
    CASE
        WHEN props.cost_center_area_code IN ('AREA0300', 'AREA0310', 'AREA0320', 'AREA0330', 'AREA0340') THEN 1
        ELSE 0
    END AS A3_Op,
    CASE
        WHEN props.cost_center_area_code IN ('AREA0400', 'AREA0410', 'AREA0420', 'AREA0430') THEN 1
        ELSE 0
    END AS AU_Nop,
    CASE
        WHEN props.cost_center_area_code IN ('AREA0500', 'AREA0510', 'AREA0520', 'AREA0530') THEN 1
        ELSE 0
    END AS A3_Nop,
    CASE
        WHEN corp_code IN (551, 561, 565, 578, 586, 587, 588, 598, 702, 755) THEN 'A3'
        WHEN corp_code IN (410, 420, 550, 560, 580, 585, 590, 595, 599, 600, 650, 700, 701, 750, 751) THEN 'AU'
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
FROM
    edw.financial.fact_financial_details AS entries
    LEFT JOIN edw.financial.dim_account AS accts ON accts.account_hid = entries.account_hid
    LEFT JOIN edw.financial.dim_billing_category AS billcats ON billcats.billing_category_hid = entries.billing_category_hid
    LEFT JOIN edw.financial.dim_cost_center AS props ON props.cost_center_hid = entries.cost_center_hid
    LEFT JOIN edw.financial.dim_corp AS corps ON corps.corp_hid = entries.corp_hid
WHERE
    accts.account_class_code BETWEEN 4 AND 6
    AND Corps.Segment = 'Upstream'
GROUP BY
    acctcode, acctdesc, adjbillcat, state, productid, gl, los_map
"""


def run_upstream_loe_ai_analysis():
    """Run AI analysis on the Upstream LOE State SQL."""
    print("=" * 80)
    print("AI-POWERED UPSTREAM LOE STATE ANALYSIS")
    print("=" * 80)
    print()
    print("SQL Features:")
    print("  - State inference from cost_center_code (TX/LA/WY/Multiple)")
    print("  - Adjusted billing category reclassification")
    print("  - Operated/Non-Op flags (AU_Op, A3_Op, AU_Nop, A3_Nop)")
    print("  - GL classification (75+ account categories)")
    print("  - LOS Map (22 LOE billing categories)")
    print("  - Corporate hierarchies (fund, segment, stakes)")
    print()

    # Run AI analysis
    result = ai_analyze_sql(
        sql=UPSTREAM_LOE_STATE_SQL,
        client_id="demo_oilco",
        industry="oil_gas",
        export_path="./result_export",
        export_name="",  # Auto-generate name
        min_confidence=0.0,
        user_intent="Build complete Upstream LOE hierarchy with state inference and operated flags",
    )

    # Display results
    print("=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)

    if result["success"]:
        summary = result["summary"]
        print(f"\n[OK] Analysis completed successfully")
        print(f"  Hierarchies Found: {summary['hierarchies_found']}")
        print(f"  Average Confidence: {summary['average_confidence']:.0%}")
        print(f"  Files Exported: {summary['files_exported']}")
        print(f"  Industry: {summary['industry']}")

        print("\n" + "-" * 60)
        print("EXPORTED FILES:")
        print("-" * 60)
        for file_path in result["export"]["files"]:
            print(f"  -> {file_path}")

        print("\n" + "-" * 60)
        print("HIERARCHY CONFIDENCE SCORES:")
        print("-" * 60)
        print(f"{'Hierarchy':<25} {'Confidence':>12} {'Recommendation':<40}")
        print("-" * 60)

        for hier in sorted(result["hierarchies"], key=lambda x: x["confidence"], reverse=True):
            conf_str = f"{hier['confidence']:.0%}"
            rec_short = hier['recommendation'][:38] + ".." if len(hier['recommendation']) > 40 else hier['recommendation']
            print(f"{hier['name']:<25} {conf_str:>12} {rec_short:<40}")

        print("\n" + "-" * 60)
        print("DETAILED SCORES BY HIERARCHY:")
        print("-" * 60)

        # Show top hierarchies with full details
        top_hierarchies = sorted(result["hierarchies"], key=lambda x: x["confidence"], reverse=True)[:5]
        for hier in top_hierarchies:
            print(f"\n  {hier['name']} ({hier['confidence']:.0%})")
            for score_name, score_val in hier["scores"].items():
                bar = "#" * int(score_val * 20)
                print(f"    {score_name:<22} {score_val:>5.0%} |{bar:<20}|")

        print("\n" + "-" * 60)
        print("AGENT PIPELINE SUMMARY:")
        print("-" * 60)

        for agent_name, insights in result["agent_insights"].items():
            status = "[OK]" if insights["confidence"] >= 0.7 else "[!!]"
            print(f"  {status} {agent_name:<20} Confidence: {insights['confidence']:.0%}")

        if result["suggestions"]:
            print("\n" + "-" * 60)
            print("IMPROVEMENT SUGGESTIONS:")
            print("-" * 60)
            for idx, sug in enumerate(result["suggestions"], 1):
                print(f"  {idx}. {sug}")

    else:
        print(f"\n[FAIL] Analysis failed: {result.get('error', 'Unknown error')}")

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

    return result


if __name__ == "__main__":
    result = run_upstream_loe_ai_analysis()

    # Show execution timeline
    print("\n" + "-" * 60)
    print("EXECUTION TIMELINE:")
    print("-" * 60)
    for log_entry in result.get("execution_log", []):
        print(f"  {log_entry}")
