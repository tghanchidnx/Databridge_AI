"""
Test Script for AI-Powered SQL Hierarchy Analysis.

This script demonstrates the AI agent orchestration system for analyzing
SQL CASE statements and generating intelligent hierarchy exports.

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    ORCHESTRATOR AGENT                        │
    │  (Context-aware coordinator with client knowledge)          │
    └─────────────────────┬───────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ CASE Extract  │ │ Entity Detect │ │ Pattern Detect│
│    Agent      │ │    Agent      │ │    Agent      │
└───────────────┘ └───────────────┘ └───────────────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          ▼
                ┌───────────────────┐
                │ Confidence Scorer │
                │      Agent        │
                └───────────────────┘
                          │
                          ▼
                ┌───────────────────┐
                │   Export Agent    │
                │ (CSV Generation)  │
                └───────────────────┘
"""

from ai_sql_orchestrator_standalone import (
    SQLAnalysisOrchestrator,
    AgentContext,
    CaseExtractionAgent,
    EntityDetectionAgent,
    PatternDetectionAgent,
    ConfidenceScoringAgent,
    ExportAgent,
    ai_analyze_sql,
)


# Sample SQL - Oil & Gas Marketing Segment Report
SAMPLE_SQL = """
SELECT
    account_code AS acctcode,
    accts.account_name AS acctdesc,
    DATE_FROM_PARTS(
        SUBSTR(entries.accounting_date_key, 1, 4),
        SUBSTR(entries.accounting_date_key, 5, 2),
        1
    ) AS acctdate,
    ROUND(SUM(entries.amount_gl), 2) AS Val,
    CASE
        WHEN account_code ILIKE '51%' THEN '1 - Gas Sales'
        WHEN account_code ILIKE ANY ('65%', '66%') THEN '3 - Fees'
        ELSE '2 - COGP'
    END AS Alloc_Code
FROM
    edw.financial.fact_financial_details AS entries
    LEFT JOIN (
        SELECT
            account_hid,
            account_code,
            account_name,
            CASE
                WHEN account_code ILIKE '101%' THEN 'Cash'
                WHEN account_code ILIKE '125%' THEN 'Affiliate AR'
                WHEN account_code ILIKE ANY ('11%', '12%') THEN 'AR'
                WHEN account_code ILIKE '501%' then 'Oil Sales'
                WHEN account_code ILIKE ANY ('502%', '510%') THEN 'Gas Sales'
                WHEN account_code ILIKE ANY ('503%', '512%', '513%') THEN 'NGL Sales'
                WHEN account_code ILIKE '8%' THEN 'General & Administrative'
                WHEN account_code IN ('910-100', '920-100') THEN 'Interest Income'
                ELSE 'Other'
            END AS gl
        FROM edw.financial.dim_account
    ) AS accts ON accts.account_hid = entries.account_hid
    LEFT JOIN (
        SELECT
            Corp_HID,
            Corp_code,
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
        FROM edw.financial.dim_corp
    ) AS corps ON corps.corp_hid = entries.corp_hid
WHERE
    Corps.corp_code = 600
    AND accts.account_code ILIKE ANY ('51%', '6%')
GROUP BY acctdate, acctcode, acctdesc
"""


def run_orchestrated_analysis():
    """Run the full AI orchestrated analysis."""
    print("=" * 80)
    print("AI-POWERED SQL HIERARCHY ANALYSIS")
    print("=" * 80)
    print()
    print("Architecture:")
    print("  Orchestrator -> [Extraction, Entity, Pattern, Confidence, Export] Agents")
    print()

    # Create orchestrator
    orchestrator = SQLAnalysisOrchestrator()

    # Run analysis with Oil & Gas industry context
    result = orchestrator.analyze(
        sql=SAMPLE_SQL,
        client_id="demo_oilco",
        industry="oil_gas",
        export_path="./result_export",
        export_name="",  # Auto-generate intelligent name
        min_confidence=0.0,
        user_intent="Build financial reporting hierarchy for Oil & Gas marketing segment",
    )

    # Display results
    print()
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
        print(f"  Client: {summary['client'] or 'N/A'}")

        print("\n" + "-" * 40)
        print("EXPORTED FILES:")
        print("-" * 40)
        for file_path in result["export"]["files"]:
            print(f"  - {file_path}")

        print("\n" + "-" * 40)
        print("HIERARCHY ANALYSIS:")
        print("-" * 40)
        for hier in result["hierarchies"]:
            print(f"\n  {hier['name']}")
            print(f"    Confidence: {hier['confidence']:.0%}")
            print(f"    Recommendation: {hier['recommendation']}")
            print(f"    Scores:")
            for score_name, score_val in hier["scores"].items():
                print(f"      - {score_name}: {score_val:.0%}")

        print("\n" + "-" * 40)
        print("AGENT INSIGHTS:")
        print("-" * 40)
        for agent_name, insights in result["agent_insights"].items():
            print(f"\n  {agent_name}:")
            print(f"    Reasoning: {insights['reasoning'][:80]}...")
            print(f"    Confidence: {insights['confidence']:.0%}")
            if insights["suggestions"]:
                print(f"    Suggestions:")
                for sug in insights["suggestions"][:2]:
                    print(f"      - {sug[:60]}...")

        if result["suggestions"]:
            print("\n" + "-" * 40)
            print("IMPROVEMENT SUGGESTIONS:")
            print("-" * 40)
            for sug in result["suggestions"]:
                print(f"  - {sug}")

        print("\n" + "-" * 40)
        print("EXECUTION LOG (last 5 entries):")
        print("-" * 40)
        for log_entry in result["execution_log"][-5:]:
            print(f"  {log_entry}")

    else:
        print(f"\n[FAIL] Analysis failed: {result.get('error', 'Unknown error')}")

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

    return result


def run_individual_agents():
    """Demonstrate running individual agents separately."""
    print("\n" + "=" * 80)
    print("INDIVIDUAL AGENT DEMONSTRATION")
    print("=" * 80)

    # Create context
    context = AgentContext(
        client_id="demo_client",
        industry="oil_gas",
        user_intent="Extract hierarchies for financial reporting",
    )

    # 1. CASE Extraction Agent
    print("\n--- CaseExtractionAgent ---")
    extraction_agent = CaseExtractionAgent()
    extraction_result = extraction_agent.execute({"sql": SAMPLE_SQL}, context)
    print(f"Success: {extraction_result.success}")
    print(f"Cases Found: {len(extraction_result.data.get('cases', []))}")
    print(f"Confidence: {extraction_result.confidence:.0%}")
    print(f"Reasoning: {extraction_result.reasoning}")

    cases = extraction_result.data.get("cases", [])

    # 2. Entity Detection Agent
    print("\n--- EntityDetectionAgent ---")
    entity_agent = EntityDetectionAgent()
    entity_result = entity_agent.execute({"cases": cases}, context)
    print(f"Success: {entity_result.success}")
    print(f"Entity Summary: {entity_result.data.get('entity_summary', {})}")
    print(f"Confidence: {entity_result.confidence:.0%}")

    cases = entity_result.data.get("cases", cases)

    # 3. Pattern Detection Agent
    print("\n--- PatternDetectionAgent ---")
    pattern_agent = PatternDetectionAgent()
    pattern_result = pattern_agent.execute({"cases": cases}, context)
    print(f"Success: {pattern_result.success}")
    print(f"Recommendations: {len(pattern_result.data.get('hierarchy_recommendations', []))}")
    print(f"Confidence: {pattern_result.confidence:.0%}")

    pattern_analysis = pattern_result.data.get("pattern_analysis", [])

    # 4. Confidence Scoring Agent
    print("\n--- ConfidenceScoringAgent ---")
    confidence_agent = ConfidenceScoringAgent()
    confidence_result = confidence_agent.execute({
        "cases": cases,
        "pattern_analysis": pattern_analysis,
    }, context)
    print(f"Success: {confidence_result.success}")
    print(f"Average Confidence: {confidence_result.data.get('average_confidence', 0):.0%}")

    scored_cases = confidence_result.data.get("scored_cases", [])
    for sc in scored_cases[:3]:
        print(f"  - {sc['hierarchy_name']}: {sc['overall_score']:.0%}")

    # 5. Export Agent
    print("\n--- ExportAgent ---")
    export_agent = ExportAgent()
    export_result = export_agent.execute({
        "cases": cases,
        "scored_cases": scored_cases,
        "export_config": {
            "path": "./result_export",
            "name": "agent_demo",
            "min_confidence": 0.0,
        },
    }, context)
    print(f"Success: {export_result.success}")
    print(f"Export Name: {export_result.data.get('export_name')}")
    print(f"Files Exported: {len(export_result.data.get('exported_files', []))}")
    for f in export_result.data.get("exported_files", []):
        print(f"  - {f}")


if __name__ == "__main__":
    # Run full orchestrated analysis
    run_orchestrated_analysis()

    # Optionally run individual agent demo
    print("\n")
    run_individual_agents()
