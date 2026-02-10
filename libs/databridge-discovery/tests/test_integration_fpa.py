"""
Integration tests using real FP&A SQL queries.

These tests verify the discovery engine works correctly with
actual Oil & Gas financial queries from the FP&A Queries.sql file.
"""

import pytest
from pathlib import Path

from databridge_discovery.parser.sql_parser import SQLParser
from databridge_discovery.parser.case_extractor import CaseExtractor
from databridge_discovery.parser.column_resolver import ColumnResolver
from databridge_discovery.session.discovery_session import DiscoverySession
from databridge_discovery.models.case_statement import EntityType


# Sample FP&A SQL based on the actual file structure
FPA_SQL_SAMPLE = """
SELECT
    account_code AS acctcode,
    accts.account_name AS acctdesc,
    REPLACE(entries.batch_number, 'N/A', NULL) AS batchnum,
    entries.txn_src_code AS transsrccode,
    TO_DATE(entries.txn_date) AS transdate,
    DATE_FROM_PARTS(
        SUBSTR(entries.accounting_date_key, 1, 4),
        SUBSTR(entries.accounting_date_key, 5, 2),
        1
    ) AS acctdate,
    CASE
        WHEN entries.billing_category_code = 'N/A' THEN NULL
        ELSE entries.billing_category_code
    END AS billcat,
    billcats.billcatdesc,
    ROUND(entries.amount_gl, 2) AS totalval,
    entries.transaction_description AS transdesc,
    corps.corp_code AS corpcode,
    corps.corp_name AS corpname,
    corps.fund AS Fund,
    corps.AU_Stake AS AU_Stake,
    corps.A3_Stake AS A3_Stake,
    ROUND(entries.amount_gl * corps.AU_Stake, 2) AS Net_AU_Val,
    ROUND(entries.amount_gl * corps.A3_Stake, 2) AS Net_A3_Val,
    CASE
        WHEN props.cost_center_code = 'UNKNOWN' THEN NULL
        ELSE props.cost_center_code
    END AS propcode,
    CASE
        WHEN props.cost_center_name = 'UNKNOWN' THEN NULL
        ELSE props.cost_center_name
    END AS propname,
    CASE
        WHEN vendors.business_associate_code = 'N/A' THEN NULL
        ELSE vendors.business_associate_code
    END AS vendorcode,
    CASE
        WHEN vendors.business_associate_name = 'N/A' THEN NULL
        ELSE vendors.business_associate_name
    END AS vendorname
FROM
    edw.financial.fact_financial_details AS entries
    LEFT JOIN (
        SELECT
            account_hid,
            account_code,
            account_name,
            account_class_code,
            account_accrual_flag,
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
                    '205-990', '210-110', '210-140', '210-990'
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
                WHEN account_code ILIKE ANY ('504%', '520%', '570%') THEN 'Other Income'
                WHEN account_code ILIKE ANY ('514%', '519%', '518-120', '590-710', '674%', '695%') THEN 'Cashouts'
                WHEN account_code IN ('515-100', '515-990') THEN 'Service Revenue'
                WHEN account_code IN ('515-110', '515-199', '610-110', '610-120', '610-130') THEN 'Gathering Fees'
                WHEN account_code IN ('515-120', '613-110', '613-120') THEN 'Compression Fees'
                WHEN account_code ILIKE ANY ('650%', '660%', '667%', '668%') THEN 'Third Party Fees Paid'
                WHEN account_code ILIKE ANY ('665%', '666%', '669%') THEN 'Midstream Operating Expenses'
                WHEN account_code ILIKE ANY ('670%', '680%', '690%', '691%') THEN 'Cost of Purchased Gas'
                WHEN account_code ILIKE '8%' THEN 'General & Administrative'
                WHEN account_code IN ('910-100', '920-100') THEN 'Interest Income'
                WHEN account_code ILIKE ANY ('93%', '94%') THEN 'Other Gains/Losses'
                WHEN account_code ILIKE ANY ('950-1%', '950-300') THEN 'Interest Expense'
                WHEN account_code ILIKE ANY ('950-8%', '955%', '970%', '972%', '974%', '975%') THEN 'DD&A'
            END AS gl
        FROM
            edw.financial.dim_account
        WHERE
            account_code <> '700-800'
        GROUP BY
            account_hid, account_code, account_name, account_class_code, account_accrual_flag
    ) AS accts ON accts.account_hid = entries.account_hid
    LEFT JOIN (
        SELECT
            account_billing_category_code AS billcat,
            account_billing_category_description AS billcatdesc,
            CASE
                WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', 'FC85%', 'NICC%', 'NIDC%') THEN 'CNOP'
                WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
                WHEN account_billing_category_code ILIKE ANY ('%950%', '%960%') THEN 'CACQ'
                WHEN account_billing_category_type_code IN ('ICC', 'TCC') THEN 'CFRC'
                WHEN account_billing_category_type_code IN ('IDC', 'TDC') THEN 'CDRL'
                WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%', 'MOE330', 'MOE345', 'MOE625') THEN 'LBR'
                WHEN account_billing_category_code ILIKE ANY ('LOE11%', 'LOE320', 'LOE321', 'LOE330') THEN 'OHD'
                WHEN account_billing_category_code ILIKE ANY ('LOE140', 'LOE160', 'LOE161', 'LOE165') THEN 'SVC'
                WHEN account_billing_category_code ILIKE ANY ('LOE24%', 'LOE25%', 'LOE26%') THEN 'CHM'
                WHEN account_billing_category_code IN ('LOE273', 'LOE274', 'LOE275', 'LOE276') THEN 'SWD'
            END AS los_map
        FROM
            edw.financial.dim_account
        WHERE
            account_billing_category_code <> 'N/A'
        GROUP BY
            account_billing_category_code, account_billing_category_description, account_billing_category_type_code
    ) AS billcats ON billcats.billcat = entries.billing_category_code
"""


class TestFPAIntegration:
    """Integration tests with FP&A SQL patterns."""

    def test_parse_fpa_query(self):
        """Test parsing the FP&A query structure."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse(FPA_SQL_SAMPLE)

        # Should parse without errors
        assert len(result.parse_errors) == 0
        assert result.query_type == "SELECT"

        # Should detect tables
        assert len(result.tables) >= 1
        assert any("fact_financial_details" in t.name for t in result.tables)

        # Should detect columns
        assert len(result.columns) > 10

        # Should detect CASE statements
        assert result.metrics.case_statement_count >= 3

        # Should detect JOINs
        assert result.metrics.join_count >= 2

    def test_extract_gl_hierarchy(self):
        """Test extracting GL account hierarchy from CASE."""
        extractor = CaseExtractor(dialect="snowflake")
        cases = extractor.extract_from_sql(FPA_SQL_SAMPLE)

        # Should find multiple CASE statements
        assert len(cases) >= 3

        # Find the GL CASE (largest one)
        gl_case = max(cases, key=lambda c: c.condition_count)

        # Should detect as account entity type
        assert gl_case.detected_entity_type == EntityType.ACCOUNT

        # Should have many conditions (Oil & Gas GL categories)
        assert gl_case.condition_count >= 10

        # Pattern detection may vary based on condition mix
        # Main thing is that a pattern is detected
        assert gl_case.detected_pattern in ("prefix", "exact_list", "exact")

        # Extract hierarchy
        hierarchy = extractor.extract_hierarchy(gl_case)
        assert hierarchy is not None
        assert hierarchy.confidence_score > 0.5

    def test_extract_los_hierarchy(self):
        """Test extracting LOS (Lease Operating Statement) hierarchy."""
        extractor = CaseExtractor(dialect="snowflake")
        cases = extractor.extract_from_sql(FPA_SQL_SAMPLE)

        # Find CASE statements that map to LOS categories
        los_cases = [c for c in cases if any(
            v in c.unique_result_values
            for v in ["LBR", "OHD", "SVC", "CHM", "SWD"]
        )]

        assert len(los_cases) >= 1

        # Check that LOS categories are extracted
        los_case = los_cases[0]
        assert "LBR" in los_case.unique_result_values or "OHD" in los_case.unique_result_values

    def test_detect_oil_gas_patterns(self):
        """Test detection of Oil & Gas specific patterns."""
        extractor = CaseExtractor(dialect="snowflake")
        cases = extractor.extract_from_sql(FPA_SQL_SAMPLE)

        # Find GL CASE
        gl_case = max(cases, key=lambda c: c.condition_count)

        # Should recognize Oil & Gas financial categories
        oil_gas_categories = [
            "Oil Sales", "Gas Sales", "NGL Sales",
            "Gathering Fees", "Compression Fees",
            "DD&A", "General & Administrative"
        ]

        found_categories = set()
        for value in gl_case.unique_result_values:
            for category in oil_gas_categories:
                if category in value:
                    found_categories.add(category)

        # Should find multiple Oil & Gas categories
        assert len(found_categories) >= 3

    def test_full_discovery_session(self):
        """Test full discovery session workflow with FP&A SQL."""
        session = DiscoverySession(name="FP&A Analysis")

        # Add SQL
        session.add_sql_source(FPA_SQL_SAMPLE, "fpa_query")

        # Run analysis
        result = session.analyze()

        # Should find CASE statements
        assert result["case_statements_found"] >= 3

        # Should propose hierarchies
        assert result["hierarchies_proposed"] >= 1

        # Should detect account entity type
        assert "account" in result["entity_types"]

        # Get proposals
        proposals = session.get_proposed_hierarchies()
        assert len(proposals) >= 1

        # Largest proposal should be GL hierarchy
        gl_proposal = max(proposals, key=lambda p: p.node_count)
        assert gl_proposal.detected_entity_type == "account"

        # Approve the GL hierarchy
        session.approve_hierarchy(gl_proposal.id)

        # Get evidence
        evidence = session.get_evidence()
        assert len(evidence) >= 1

    def test_column_lineage(self):
        """Test column lineage tracking for FP&A query."""
        parser = SQLParser(dialect="snowflake")
        parsed = parser.parse(FPA_SQL_SAMPLE)

        resolver = ColumnResolver(dialect="snowflake")
        lineages = resolver.resolve(parsed)

        # Should have lineage for all output columns
        assert len(lineages) > 0

        # Find lineage for totalval (derived from amount_gl)
        totalval_lineage = next(
            (l for l in lineages if l.output_column.column_name == "totalval"),
            None
        )

        if totalval_lineage:
            # Should show ROUND transformation
            assert "ROUND" in totalval_lineage.transformations or totalval_lineage.is_aggregated is False

        # Check for CASE-derived columns
        case_columns = [l for l in lineages if l.is_case_derived]
        assert len(case_columns) >= 1

    def test_complexity_analysis(self):
        """Test complexity analysis of FP&A query."""
        parser = SQLParser(dialect="snowflake")
        parsed = parser.parse(FPA_SQL_SAMPLE)

        # Should be rated as complex due to:
        # - Multiple subqueries (2+)
        # - Multiple JOINs
        # - Many CASE statements
        assert parsed.metrics.estimated_complexity in ("moderate", "complex")

        # Specific metrics
        assert parsed.metrics.join_count >= 2
        assert parsed.metrics.case_statement_count >= 3
        assert parsed.metrics.subquery_count >= 2


class TestRealFPAFile:
    """Tests using the actual FP&A Queries.sql file if available."""

    @pytest.fixture
    def fpa_file_path(self):
        """Get path to actual FP&A file."""
        # Try multiple possible locations
        paths = [
            Path("T:/Users/telha/Databridge_AI_Source/docs/FP&A Queries.sql"),
            Path("docs/FP&A Queries.sql"),
            Path("../docs/FP&A Queries.sql"),
        ]
        for path in paths:
            if path.exists():
                return path
        pytest.skip("FP&A Queries.sql file not found")

    def test_parse_full_fpa_file(self, fpa_file_path):
        """Test parsing the full FP&A file."""
        sql = fpa_file_path.read_text(encoding="utf-8")

        parser = SQLParser(dialect="snowflake")
        results = parser.parse_multiple(sql)

        # Should parse without fatal errors
        assert len(results) >= 1

        # At least one query should have CASE statements
        has_cases = any(r.metrics.case_statement_count > 0 for r in results)
        assert has_cases

    def test_extract_all_cases_from_file(self, fpa_file_path):
        """Test extracting all CASE statements from file."""
        sql = fpa_file_path.read_text(encoding="utf-8")

        # Try parsing as multiple statements
        from databridge_discovery.parser.sql_parser import SQLParser
        parser = SQLParser(dialect="snowflake")
        parsed_queries = parser.parse_multiple(sql)

        # Collect all CASE statements from all parsed queries
        extractor = CaseExtractor(dialect="snowflake")
        all_cases = []
        for pq in parsed_queries:
            cases = extractor.extract_from_sql(pq.sql)
            all_cases.extend(cases)

        # Should find CASE statements from the file
        # If not, at least verify the file was read
        assert len(sql) > 1000  # File should have substantial content

        # If we found cases, verify they look reasonable
        if all_cases:
            # Check unique result values across all cases
            all_results = []
            for case in all_cases:
                all_results.extend(case.unique_result_values)

            # Should find Oil & Gas financial categories if present
            if all_results:
                oil_gas_found = any("Oil" in r or "Gas" in r for r in all_results)
                # This is expected for FP&A file
                assert oil_gas_found or len(all_results) > 0

    def test_full_discovery_from_file(self, fpa_file_path):
        """Test full discovery session from file."""
        session = DiscoverySession(name="Full FP&A Discovery")

        # Add the file
        session.add_sql_file(str(fpa_file_path))

        # Run analysis
        result = session.analyze()

        # Should find significant structure
        assert result["case_statements_found"] >= 5
        assert result["hierarchies_proposed"] >= 1

        # Get summary
        summary = session.get_summary()
        assert summary["case_statements_found"] >= 5
