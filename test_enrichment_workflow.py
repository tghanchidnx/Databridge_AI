"""
Test the AI SQL Orchestrator with Enrichment Workflow.

This demonstrates:
1. Running AI analysis on SQL
2. Detecting reference tables automatically
3. Configuring COA enrichment
4. Generating enriched mapping exports with ACCOUNT_ID and ACCOUNT_BILLING_CATEGORY_CODE
"""

import json
from ai_sql_orchestrator_standalone import (
    ai_analyze_sql,
    create_coa_enrichment,
    get_enrichment_questions,
    EnrichmentConfig,
    EnrichmentSourceConfig,
)

# Upstream LOE State SQL with GL account patterns
UPSTREAM_LOE_SQL = """
SELECT
    accts.account_code,
    entries.corp_code,
    entries.gross_amount,

    -- GL Account Classification for LOE Categories
    CASE
        WHEN accts.account_code ILIKE '101%' THEN 'Cash'
        WHEN accts.account_code ILIKE '11%' THEN 'AR'
        WHEN accts.account_code ILIKE '12%' THEN 'Affiliate AR'
        WHEN accts.account_code ILIKE '13%' THEN 'Prepaid Expenses'
        WHEN accts.account_code ILIKE '14%' THEN 'Inventory'
        WHEN accts.account_code IN ('205-102', '205-106', '205-112', '205-116', '205-206',
                                     '205-252', '210-110', '210-990', '230-300', '246-340',
                                     '246-344', '246-346') THEN 'Capex'
        WHEN accts.account_code ILIKE '20%' THEN 'AP'
        WHEN accts.account_code ILIKE '30%' THEN 'AP'
        WHEN accts.account_code ILIKE '501%' THEN 'Oil Revenue'
        WHEN accts.account_code ILIKE '502%' THEN 'Gas Revenue'
        WHEN accts.account_code ILIKE '503%' THEN 'NGL Revenue'
        WHEN accts.account_code ILIKE '601%' THEN 'Oil Severance Taxes'
        WHEN accts.account_code ILIKE '602%' THEN 'Gas Severance Taxes'
        WHEN accts.account_code ILIKE '603%' THEN 'NGL Severance Taxes'
        WHEN accts.account_code ILIKE '61%' THEN 'Transportation & Gathering'
        WHEN accts.account_code ILIKE '621%' THEN 'Direct LOE - Labor'
        WHEN accts.account_code ILIKE '622%' THEN 'Direct LOE - Repairs & Maintenance'
        WHEN accts.account_code ILIKE '623%' THEN 'Direct LOE - Utilities'
        WHEN accts.account_code ILIKE '624%' THEN 'Direct LOE - Compression'
        WHEN accts.account_code ILIKE '625%' THEN 'Direct LOE - Water Handling'
        WHEN accts.account_code ILIKE '626%' THEN 'Direct LOE - Environmental'
        WHEN accts.account_code ILIKE '629%' THEN 'Direct LOE - Other'
        WHEN accts.account_code ILIKE '63%' THEN 'Workovers'
        WHEN accts.account_code ILIKE '64%' THEN 'Processing'
        WHEN accts.account_code ILIKE '65%' THEN 'Ad Valorem Taxes'
        WHEN accts.account_code ILIKE '66%' THEN 'Insurance'
        WHEN accts.account_code ILIKE '67%' THEN 'Overhead - Field'
        WHEN accts.account_code ILIKE '68%' THEN 'Overhead - G&A'
        WHEN accts.account_code ILIKE '69%' THEN 'Other Operating'
        WHEN accts.account_code ILIKE '7%' THEN 'DD&A'
        WHEN accts.account_code ILIKE '8%' THEN 'Interest & Financing'
        WHEN accts.account_code ILIKE '9%' THEN 'Income Taxes'
        ELSE 'Other'
    END AS gl,

    -- State Classification
    CASE
        WHEN props.cost_center_state = 'TX' THEN 'Texas'
        WHEN props.cost_center_state = 'LA' THEN 'Louisiana'
        WHEN props.cost_center_state = 'WY' THEN 'Wyoming'
        WHEN props.cost_center_state = 'NM' THEN 'New Mexico'
        ELSE props.cost_center_state
    END AS state,

    -- Fund/Entity Classification
    CASE
        WHEN entries.corp_code = '551' THEN 'Fund I'
        WHEN entries.corp_code = '561' THEN 'Fund II'
        WHEN entries.corp_code = '565' THEN 'Fund III'
        ELSE 'Corporate'
    END AS fund

FROM fact_gl_entries entries
JOIN dim_account accts ON entries.account_hid = accts.account_hid
JOIN dim_property props ON entries.property_hid = props.property_hid
"""


def main():
    """Run the enrichment workflow demonstration."""
    print("=" * 80)
    print("AI SQL ORCHESTRATOR WITH ENRICHMENT WORKFLOW")
    print("=" * 80)

    # Step 1: Run initial analysis (without enrichment)
    print("\n[STEP 1] Running initial SQL analysis...")
    print("-" * 60)

    result = ai_analyze_sql(
        sql=UPSTREAM_LOE_SQL,
        client_id="demo_oilco",
        industry="oil_gas_upstream",
        export_path="./result_export/enrichment_test",
        export_name="",  # Auto-generate
    )

    if not result["success"]:
        print(f"[FAIL] Analysis failed: {result.get('error')}")
        return

    print(f"[OK] Found {result['summary']['hierarchies_found']} hierarchies")
    print(f"[OK] Average confidence: {result['summary']['average_confidence']:.0%}")
    print(f"[OK] Exported {result['summary']['files_exported']} files")

    # Step 2: Check for enrichment opportunities
    print("\n[STEP 2] Checking for enrichment opportunities...")
    print("-" * 60)

    enrichment_prompt = get_enrichment_questions(result)

    if enrichment_prompt and enrichment_prompt.get("has_tables"):
        print(enrichment_prompt["message"])
        print()

        for table in enrichment_prompt["tables"]:
            print(f"  - {table['display_name']} ({table['table_name']})")
            print(f"    Key column: {table['key_column']}")
            print(f"    Conditions found: {table['condition_count']}")
            print(f"    Sample patterns: {table['sample_conditions'][:3]}")
            print(f"    Suggested columns: {', '.join(table['suggested_columns'][:4])}")
            print()

        # Step 3: Configure and run enrichment
        print("\n[STEP 3] Configuring COA enrichment...")
        print("-" * 60)

        # Create enrichment configuration with COA
        coa_path = r"C:\Users\telha\Databridge_AI\Gemini\Uploads\DIM_ACCOUNT.csv"

        enrichment_config = create_coa_enrichment(
            coa_path=coa_path,
            detail_columns=[
                "ACCOUNT_ID",
                "ACCOUNT_NAME",
                "ACCOUNT_CLASS",
                "ACCOUNT_BILLING_CATEGORY_CODE",
                "ACCOUNT_MAJOR",
                "ACCOUNT_MINOR",
            ],
        )

        print(f"[OK] Configured enrichment with:")
        print(f"    - Source: {coa_path}")
        print(f"    - Columns: {', '.join(enrichment_config.data_sources[0].detail_columns)}")

        # Step 4: Re-run with enrichment
        print("\n[STEP 4] Running analysis WITH enrichment...")
        print("-" * 60)

        enriched_result = ai_analyze_sql(
            sql=UPSTREAM_LOE_SQL,
            client_id="demo_oilco",
            industry="oil_gas_upstream",
            export_path="./result_export/enrichment_test",
            export_name="upstream_loe_enriched",
            enrichment_config=enrichment_config,
        )

        if enriched_result["success"]:
            print(f"[OK] Analysis completed successfully")
            print(f"[OK] Files exported: {enriched_result['summary']['files_exported']}")

            enrichment_data = enriched_result.get("enrichment", {})
            enrichment_result = enrichment_data.get("result")

            if enrichment_result:
                print(f"\n    Enrichment Results:")
                print(f"    - Original rows: {enrichment_result.get('original_rows', 'N/A')}")
                print(f"    - Expanded rows: {enrichment_result.get('expanded_rows', 'N/A')}")

                for source in enrichment_result.get("enriched_sources", []):
                    print(f"\n    Source: {source['source']}")
                    print(f"    - Records loaded: {source.get('records_loaded', 'N/A')}")
                    print(f"    - Matched rows: {source.get('matched_rows', 'N/A')}")
                    print(f"    - Columns added: {', '.join(source.get('columns_added', []))}")

            print(f"\n    Output files:")
            for f in enriched_result["export"]["files"]:
                print(f"    - {f}")
        else:
            print(f"[FAIL] Enrichment failed")

    else:
        print("[INFO] No reference tables detected for enrichment")

    # Summary
    print("\n" + "=" * 80)
    print("ENRICHMENT WORKFLOW COMPLETE")
    print("=" * 80)
    print("""
The AI orchestrator now:
1. Automatically detects reference tables from SQL patterns
2. Generates questions to ask users about data sources
3. Supports configurable detail columns per table
4. Enriches mapping exports with reference data (ACCOUNT_ID, ACCOUNT_BILLING_CATEGORY_CODE, etc.)

To use in your code:
    from ai_sql_orchestrator_standalone import ai_analyze_sql, create_coa_enrichment

    # Simple COA enrichment
    config = create_coa_enrichment("path/to/DIM_ACCOUNT.csv")
    result = ai_analyze_sql(sql, enrichment_config=config)

    # Custom columns
    config = create_coa_enrichment(
        "path/to/DIM_ACCOUNT.csv",
        detail_columns=["ACCOUNT_ID", "ACCOUNT_BILLING_CATEGORY_CODE", "ACCOUNT_NAME"]
    )
""")


if __name__ == "__main__":
    main()
