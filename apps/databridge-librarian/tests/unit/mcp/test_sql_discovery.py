"""Tests for SQL discovery tools."""

import pytest
from src.mcp.tools.sql_discovery import (
    SimpleCaseExtractor,
    CaseToHierarchyConverter,
    EntityType,
    ConditionOperator,
)


class TestSimpleCaseExtractor:
    """Tests for the SimpleCaseExtractor class."""

    def test_extract_simple_case(self):
        """Test extracting a simple CASE statement."""
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '4%' THEN 'Revenue'
                WHEN account_code LIKE '5%' THEN 'COGS'
                WHEN account_code LIKE '6%' THEN 'OpEx'
                ELSE 'Other'
            END AS account_category
        FROM gl_transactions
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert case.source_column == "account_category"
        assert case.input_column == "account_code"
        assert len(case.when_clauses) == 3
        assert case.else_value == "Other"
        assert "Revenue" in case.unique_results
        assert "COGS" in case.unique_results
        assert "OpEx" in case.unique_results

    def test_extract_with_equals(self):
        """Test extracting CASE with equals conditions."""
        sql = """
        CASE
            WHEN status = 'A' THEN 'Active'
            WHEN status = 'I' THEN 'Inactive'
            WHEN status = 'P' THEN 'Pending'
        END AS status_desc
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert len(case.when_clauses) == 3
        assert case.when_clauses[0].condition.operator == ConditionOperator.EQUALS

    def test_extract_with_in_clause(self):
        """Test extracting CASE with IN conditions."""
        sql = """
        CASE
            WHEN dept IN ('ENG', 'IT', 'QA') THEN 'Technology'
            WHEN dept IN ('HR', 'FIN', 'LEGAL') THEN 'Corporate'
        END AS dept_group
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]
        assert len(case.when_clauses) == 2
        assert case.when_clauses[0].condition.operator == ConditionOperator.IN
        assert "ENG" in case.when_clauses[0].condition.values

    def test_detect_account_entity_type(self):
        """Test entity type detection for account codes."""
        sql = """
        CASE
            WHEN gl_account LIKE '1%' THEN 'Assets'
            WHEN gl_account LIKE '2%' THEN 'Liabilities'
        END
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        assert cases[0].entity_type == EntityType.ACCOUNT

    def test_detect_department_entity_type(self):
        """Test entity type detection for departments."""
        sql = """
        CASE
            WHEN department_code = '100' THEN 'Engineering'
            WHEN department_code = '200' THEN 'Sales'
        END
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        assert cases[0].entity_type == EntityType.DEPARTMENT

    def test_detect_prefix_pattern(self):
        """Test pattern detection for prefix matching."""
        sql = """
        CASE
            WHEN code LIKE '100%' THEN 'Group A'
            WHEN code LIKE '200%' THEN 'Group B'
        END
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        assert cases[0].pattern_type == "prefix"

    def test_multiple_cases(self):
        """Test extracting multiple CASE statements."""
        sql = """
        SELECT
            CASE WHEN type = 'A' THEN 'Type A' END AS type_desc,
            CASE WHEN status = '1' THEN 'Active' END AS status_desc
        FROM table1
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 2


class TestCaseToHierarchyConverter:
    """Tests for the CaseToHierarchyConverter class."""

    def test_convert_to_hierarchy(self):
        """Test converting a CASE to hierarchy."""
        sql = """
        CASE
            WHEN account LIKE '4%' THEN 'Revenue'
            WHEN account LIKE '5%' THEN 'COGS'
            WHEN account LIKE '6%' THEN 'OpEx'
        END AS account_category
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        converter = CaseToHierarchyConverter()
        hierarchy = converter.convert(cases[0])

        assert hierarchy.total_nodes == 3
        assert hierarchy.level_count == 1
        assert len(hierarchy.root_nodes) == 3

        # Check nodes exist
        node_names = [n.name for n in hierarchy.nodes.values()]
        assert "Revenue" in node_names
        assert "COGS" in node_names
        assert "OpEx" in node_names

    def test_convert_to_librarian_rows(self):
        """Test converting to Librarian CSV format."""
        sql = """
        CASE
            WHEN code LIKE '1%' THEN 'Category A'
            WHEN code LIKE '2%' THEN 'Category B'
        END AS category
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        converter = CaseToHierarchyConverter()
        hierarchy = converter.convert(cases[0])

        rows = converter.to_librarian_hierarchy_rows(hierarchy)

        assert len(rows) == 2
        assert all("hierarchy_id" in row for row in rows)
        assert all("hierarchy_name" in row for row in rows)
        assert all("level_1" in row for row in rows)

    def test_convert_to_mapping_rows(self):
        """Test converting to Librarian mapping CSV format."""
        sql = """
        CASE
            WHEN account LIKE '4%' THEN 'Revenue'
            WHEN account LIKE '5%' THEN 'Expenses'
        END
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        converter = CaseToHierarchyConverter()
        hierarchy = converter.convert(cases[0])

        mappings = converter.to_librarian_mapping_rows(
            hierarchy,
            source_database="WAREHOUSE",
            source_schema="FINANCE",
            source_table="GL_TRANS",
            source_column="ACCOUNT_CODE",
        )

        assert len(mappings) == 2
        assert all(m["source_database"] == "WAREHOUSE" for m in mappings)
        assert all(m["source_schema"] == "FINANCE" for m in mappings)

    def test_confidence_calculation(self):
        """Test confidence score calculation."""
        # High confidence: many conditions, known entity type
        sql = """
        CASE
            WHEN gl_account LIKE '1%' THEN 'Assets'
            WHEN gl_account LIKE '2%' THEN 'Liabilities'
            WHEN gl_account LIKE '3%' THEN 'Equity'
            WHEN gl_account LIKE '4%' THEN 'Revenue'
            WHEN gl_account LIKE '5%' THEN 'COGS'
            WHEN gl_account LIKE '6%' THEN 'Operating Expenses'
            WHEN gl_account LIKE '7%' THEN 'Other Income'
            WHEN gl_account LIKE '8%' THEN 'Other Expenses'
            WHEN gl_account LIKE '9%' THEN 'Taxes'
        END
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        converter = CaseToHierarchyConverter()
        hierarchy = converter.convert(cases[0])

        # Should have higher confidence due to many conditions and account pattern
        assert hierarchy.confidence >= 0.7


class TestIntegration:
    """Integration tests for the full workflow."""

    def test_full_workflow(self):
        """Test complete SQL to hierarchy workflow."""
        sql = """
        SELECT
            order_id,
            customer_name,
            CASE
                WHEN product_category IN ('LAPTOP', 'DESKTOP', 'TABLET') THEN 'Computing'
                WHEN product_category IN ('PHONE', 'SMARTWATCH') THEN 'Mobile'
                WHEN product_category IN ('TV', 'SPEAKER', 'HEADPHONES') THEN 'Entertainment'
                ELSE 'Other'
            END AS product_group,
            amount
        FROM sales_orders
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1

        converter = CaseToHierarchyConverter()
        hierarchy = converter.convert(cases[0])

        # Check hierarchy structure
        assert hierarchy.total_nodes == 4  # 3 groups + Other

        # Check mappings include all IN values
        assert len(hierarchy.mapping) >= 8  # All product categories

    def test_oil_gas_case(self):
        """Test with oil & gas accounting CASE statement."""
        sql = """
        CASE
            WHEN account_code LIKE '4100%' THEN 'Oil Revenue'
            WHEN account_code LIKE '4200%' THEN 'Gas Revenue'
            WHEN account_code LIKE '4300%' THEN 'NGL Revenue'
            WHEN account_code LIKE '5100%' THEN 'Severance Tax'
            WHEN account_code LIKE '5200%' THEN 'Ad Valorem Tax'
            WHEN account_code LIKE '6100%' THEN 'Direct LOE'
            WHEN account_code LIKE '6200%' THEN 'Workover Expense'
            WHEN account_code LIKE '7100%' THEN 'G&A'
            WHEN account_code LIKE '8100%' THEN 'DD&A'
        END AS los_category
        """

        extractor = SimpleCaseExtractor()
        cases = extractor.extract_from_sql(sql)

        assert len(cases) == 1
        case = cases[0]

        # Should detect as account type
        assert case.entity_type == EntityType.ACCOUNT

        # Should detect prefix pattern
        assert case.pattern_type == "prefix"

        # Convert to hierarchy
        converter = CaseToHierarchyConverter()
        hierarchy = converter.convert(case)

        assert hierarchy.total_nodes == 9

        # Check CSV output
        rows = converter.to_librarian_hierarchy_rows(hierarchy, "LOS Categories")
        assert len(rows) == 9
        assert all(row["hierarchy_name"] == "LOS Categories" for row in rows)
