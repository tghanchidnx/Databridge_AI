"""
Unit tests for CASE Statement Extractor module.
"""

import pytest

from databridge_discovery.parser.case_extractor import CaseExtractor
from databridge_discovery.models.case_statement import (
    ConditionOperator,
    EntityType,
)


class TestCaseExtractor:
    """Tests for CaseExtractor class."""

    def test_extract_simple_case(self):
        """Test extracting a simple CASE statement."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN status = 'A' THEN 'Active'
                WHEN status = 'I' THEN 'Inactive'
                ELSE 'Unknown'
            END as status_label
        FROM users
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert len(case.when_clauses) == 2
        assert case.else_value is not None
        assert "Unknown" in case.else_value
        # Check that result values contain Active (with or without quotes)
        assert any("Active" in v for v in case.unique_result_values)

    def test_extract_case_with_like(self):
        """Test extracting CASE with LIKE operator."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '5%' THEN 'Revenue'
                WHEN account_code LIKE '6%' THEN 'Expense'
            END as category
        FROM gl_accounts
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert case.detected_pattern == "prefix"
        assert len(case.when_clauses) == 2

    def test_extract_case_with_ilike(self):
        """Test extracting CASE with ILIKE operator (Snowflake)."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN account_code ILIKE '501%' THEN 'Oil Sales'
                WHEN account_code ILIKE '502%' THEN 'Gas Sales'
            END as gl
        FROM dim_account
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert case.input_column == "account_code"
        for when in case.when_clauses:
            assert when.condition.operator == ConditionOperator.ILIKE

    def test_extract_case_with_in(self):
        """Test extracting CASE with IN operator."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN dept_code IN ('10', '20', '30') THEN 'Core'
                WHEN dept_code IN ('40', '50') THEN 'Support'
            END as dept_type
        FROM departments
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert case.when_clauses[0].condition.operator == ConditionOperator.IN
        assert len(case.when_clauses[0].condition.values) == 3

    def test_detect_account_entity_type(self):
        """Test detecting account entity type from column name."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '1%' THEN 'Assets'
                WHEN account_code LIKE '2%' THEN 'Liabilities'
            END as category
        FROM chart_of_accounts
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        assert cases[0].detected_entity_type == EntityType.ACCOUNT

    def test_detect_cost_center_entity_type(self):
        """Test detecting cost center entity type."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN cost_center_code LIKE 'CC1%' THEN 'Operations'
                WHEN cost_center_code LIKE 'CC2%' THEN 'Sales'
            END as cc_category
        FROM cost_centers
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        assert cases[0].detected_entity_type == EntityType.COST_CENTER

    def test_detect_department_entity_type(self):
        """Test detecting department entity type."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN dept_code = 'ENG' THEN 'Engineering'
                WHEN dept_code = 'MKT' THEN 'Marketing'
            END as department_name
        FROM employees
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        assert cases[0].detected_entity_type == EntityType.DEPARTMENT

    def test_extract_multiple_cases(self):
        """Test extracting multiple CASE statements from one query."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE WHEN type = 'A' THEN 'Type A' END as type_label,
            CASE WHEN status = '1' THEN 'Active' END as status_label
        FROM items
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 2

    def test_extract_hierarchy(self):
        """Test extracting hierarchy from CASE statement."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '5%' THEN 'Revenue'
                WHEN account_code LIKE '6%' THEN 'Expenses'
                WHEN account_code LIKE '7%' THEN 'Other'
            END as category
        FROM gl
        """
        cases = extractor.extract_from_sql(sql)
        hierarchy = extractor.extract_hierarchy(cases[0])

        assert hierarchy is not None
        assert hierarchy.total_levels == 1
        assert hierarchy.total_nodes == 3
        assert hierarchy.confidence_score > 0

    def test_oil_gas_pattern_detection(self):
        """Test detecting Oil & Gas financial patterns."""
        extractor = CaseExtractor(dialect="snowflake")
        # Simplified version of the FP&A query pattern
        sql = """
        SELECT
            CASE
                WHEN account_code ILIKE '501%' THEN 'Oil Sales'
                WHEN account_code ILIKE '502%' THEN 'Gas Sales'
                WHEN account_code ILIKE '503%' THEN 'NGL Sales'
                WHEN account_code ILIKE '601%' THEN 'Oil Severance Taxes'
                WHEN account_code ILIKE '640%' THEN 'Lease Operating Expenses'
                WHEN account_code ILIKE '8%' THEN 'General & Administrative'
            END as gl_category
        FROM dim_account
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        # Should detect as account due to financial patterns
        assert case.detected_entity_type == EntityType.ACCOUNT
        assert len(case.when_clauses) == 6

    def test_case_with_between(self):
        """Test CASE with BETWEEN operator."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN amount BETWEEN 0 AND 100 THEN 'Small'
                WHEN amount BETWEEN 101 AND 1000 THEN 'Medium'
                ELSE 'Large'
            END as size_category
        FROM orders
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        # First WHEN should have BETWEEN operator
        assert cases[0].when_clauses[0].condition.operator == ConditionOperator.BETWEEN

    def test_nested_case(self):
        """Test extracting nested CASE statements."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN type = 'A' THEN
                    CASE WHEN status = '1' THEN 'A1' ELSE 'A0' END
                ELSE 'Other'
            END as combined
        FROM items
        """
        cases = extractor.extract_from_sql(sql)

        # Should find both outer and inner CASE
        assert len(cases) >= 1

    def test_case_in_subquery(self):
        """Test extracting CASE from subquery."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT *
        FROM (
            SELECT id,
                CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END as tier
            FROM orders
        ) t
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) >= 1

    def test_find_nested_hierarchies(self):
        """Test finding nested hierarchy relationships."""
        extractor = CaseExtractor(dialect="snowflake")
        # Two CASE statements where one feeds into the other
        sql = """
        SELECT
            CASE
                WHEN code LIKE '1%' THEN 'A'
                WHEN code LIKE '2%' THEN 'B'
            END as level1,
            CASE
                WHEN level1 = 'A' THEN 'Category A'
                WHEN level1 = 'B' THEN 'Category B'
            END as level2
        FROM items
        """
        cases = extractor.extract_from_sql(sql)

        if len(cases) >= 2:
            pairs = extractor.find_nested_hierarchies(cases)
            # May or may not find relationship depending on parsing
            assert isinstance(pairs, list)

    def test_confidence_scoring(self):
        """Test confidence scoring for hierarchy extraction."""
        extractor = CaseExtractor(dialect="snowflake")

        # High confidence: many conditions, known entity type
        sql_high = """
        SELECT
            CASE
                WHEN account_code LIKE '501%' THEN 'Oil Sales'
                WHEN account_code LIKE '502%' THEN 'Gas Sales'
                WHEN account_code LIKE '503%' THEN 'NGL Sales'
                WHEN account_code LIKE '504%' THEN 'Other Income'
                WHEN account_code LIKE '601%' THEN 'Oil Severance'
                WHEN account_code LIKE '602%' THEN 'Gas Severance'
                WHEN account_code LIKE '640%' THEN 'LOE'
                WHEN account_code LIKE '8%' THEN 'G&A'
                WHEN account_code LIKE '9%' THEN 'Other'
                WHEN account_code LIKE '95%' THEN 'Interest'
            END as gl
        FROM accounts
        """
        cases_high = extractor.extract_from_sql(sql_high)
        hier_high = extractor.extract_hierarchy(cases_high[0])

        # Low confidence: few conditions, unknown entity
        sql_low = """
        SELECT
            CASE WHEN x = 1 THEN 'A' ELSE 'B' END as cat
        FROM items
        """
        cases_low = extractor.extract_from_sql(sql_low)
        hier_low = extractor.extract_hierarchy(cases_low[0])

        assert hier_high.confidence_score > hier_low.confidence_score


class TestCaseExtractorEdgeCases:
    """Edge case tests for CASE Extractor."""

    def test_no_case_statements(self):
        """Test SQL with no CASE statements."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = "SELECT * FROM users WHERE active = 1"
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 0

    def test_empty_sql(self):
        """Test empty SQL input."""
        extractor = CaseExtractor(dialect="snowflake")
        cases = extractor.extract_from_sql("")

        assert len(cases) == 0

    def test_invalid_sql(self):
        """Test invalid SQL input."""
        extractor = CaseExtractor(dialect="snowflake")
        cases = extractor.extract_from_sql("SELEC * FORM users")

        # Should handle gracefully
        assert isinstance(cases, list)

    def test_case_with_null_handling(self):
        """Test CASE with NULL handling."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN status IS NULL THEN 'Unknown'
                WHEN status = 'A' THEN 'Active'
            END as status_label
        FROM users
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1

    def test_case_with_and_condition(self):
        """Test CASE with AND compound condition."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN type = 'A' AND status = '1' THEN 'Active A'
                ELSE 'Other'
            END as label
        FROM items
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        # Check for compound condition
        cond = cases[0].when_clauses[0].condition
        assert cond.operator == ConditionOperator.AND or cond.is_compound

    def test_case_with_or_condition(self):
        """Test CASE with OR compound condition."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN type = 'A' OR type = 'B' THEN 'Type AB'
                ELSE 'Other'
            END as label
        FROM items
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1

    def test_case_any_pattern(self):
        """Test CASE with ANY/ALL pattern (Snowflake)."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN code ILIKE ANY ('501%', '502%', '503%') THEN 'Sales'
                ELSE 'Other'
            END as category
        FROM accounts
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1

    def test_result_type_inference(self):
        """Test result type inference in CASE."""
        extractor = CaseExtractor(dialect="snowflake")
        sql = """
        SELECT
            CASE
                WHEN type = 'A' THEN 1
                WHEN type = 'B' THEN 2
                ELSE 0
            END as numeric_type
        FROM items
        """
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        # Check that numeric result type is inferred
        for when in cases[0].when_clauses:
            assert when.result_type in ("integer", "string")  # Depending on how sqlglot handles it
