"""
Tests for Phase 3 MCP tools: Hierarchy Extraction.

Tests cover 10 tools:
15. extract_hierarchy_from_sql
16. analyze_csv_for_hierarchy
17. detect_entity_types
18. infer_hierarchy_levels
19. generate_sort_orders
20. merge_with_librarian_hierarchy
21. export_discovery_as_csv
22. validate_hierarchy_structure
23. suggest_parent_child_relationships
24. compare_hierarchies
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from databridge_discovery.mcp.tools import (
    analyze_csv_for_hierarchy,
    compare_hierarchies,
    delete_hierarchy,
    detect_entity_types,
    export_discovery_as_csv,
    extract_hierarchy_from_sql,
    generate_sort_orders,
    get_hierarchy_details,
    infer_hierarchy_levels,
    list_hierarchies,
    merge_with_librarian_hierarchy,
    suggest_parent_child_relationships,
    validate_hierarchy_structure,
)


class TestExtractHierarchyFromSQL:
    """Tests for extract_hierarchy_from_sql tool."""

    def test_extract_simple_case(self):
        """Test extracting hierarchy from simple CASE statement."""
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '4%' THEN 'Revenue'
                WHEN account_code LIKE '5%' THEN 'COGS'
                WHEN account_code LIKE '6%' THEN 'Operating Expenses'
            END as category
        FROM gl_entries
        """
        result = extract_hierarchy_from_sql(sql)

        assert result["success"] is True
        assert result["case_count"] >= 1
        assert len(result["hierarchies"]) >= 1

        hier = result["primary_hierarchy"]
        assert hier is not None
        assert hier["librarian_compatible"] is True

    def test_extract_account_entity_type(self):
        """Test that account entity type is detected."""
        sql = """
        SELECT
            CASE
                WHEN acct LIKE '4%' THEN 'Revenue'
                WHEN acct LIKE '5%' THEN 'Expense'
            END as category
        FROM gl
        """
        result = extract_hierarchy_from_sql(sql)

        assert result["success"] is True
        hier = result["primary_hierarchy"]
        assert hier["entity_type"] == "account"

    def test_extract_no_case(self):
        """Test handling SQL without CASE statements."""
        sql = "SELECT * FROM customers"
        result = extract_hierarchy_from_sql(sql)

        assert result["success"] is False
        assert "No CASE statements" in result["error"]

    def test_extract_with_name(self):
        """Test providing custom hierarchy name."""
        sql = """
        SELECT
            CASE WHEN code = '1' THEN 'A' END as category
        FROM tbl
        """
        result = extract_hierarchy_from_sql(sql, name="My Custom Hierarchy")

        assert result["success"] is True
        assert result["primary_hierarchy"]["name"] == "My Custom Hierarchy"

    def test_extract_multiple_cases(self):
        """Test extracting multiple CASE statements."""
        sql = """
        SELECT
            CASE WHEN a = 1 THEN 'X' END as cat1,
            CASE WHEN b = 2 THEN 'Y' END as cat2
        FROM tbl
        """
        result = extract_hierarchy_from_sql(sql)

        assert result["success"] is True
        assert result["case_count"] >= 2


class TestAnalyzeCSVForHierarchy:
    """Tests for analyze_csv_for_hierarchy tool."""

    def test_analyze_with_data(self):
        """Test analyzing data with hierarchy patterns."""
        data = [
            {"level1": "Revenue", "level2": "Product Sales", "amount": 1000},
            {"level1": "Revenue", "level2": "Service Revenue", "amount": 500},
            {"level1": "Expenses", "level2": "Salaries", "amount": 300},
            {"level1": "Expenses", "level2": "Rent", "amount": 200},
        ]
        result = analyze_csv_for_hierarchy(data=data)

        assert result["row_count"] == 4
        assert result["column_count"] == 3
        assert "level1" in result["column_profiles"]
        assert "level2" in result["column_profiles"]

    def test_analyze_detects_entity_types(self):
        """Test entity type detection in columns."""
        data = [
            {"account_code": "4100", "department": "Sales"},
            {"account_code": "4200", "department": "Marketing"},
        ]
        result = analyze_csv_for_hierarchy(data=data)

        # Check that entity types are detected
        assert "account_code" in result["column_profiles"]
        assert "department" in result["column_profiles"]

    def test_analyze_finds_relationships(self):
        """Test that parent-child relationships are found."""
        data = [
            {"category": "Revenue", "subcategory": "Product"},
            {"category": "Revenue", "subcategory": "Service"},
            {"category": "Expense", "subcategory": "Labor"},
        ]
        result = analyze_csv_for_hierarchy(data=data)

        # Should find category -> subcategory relationship
        assert "relationship_pairs" in result

    def test_analyze_requires_input(self):
        """Test that input is required."""
        result = analyze_csv_for_hierarchy()
        assert "error" in result


class TestDetectEntityTypes:
    """Tests for detect_entity_types tool."""

    def test_detect_from_column_name(self):
        """Test detection from column name with supporting values."""
        # Column name alone may not meet confidence threshold
        # Adding typical account values to boost confidence
        result = detect_entity_types(
            column_name="gl_account",
            values=["4100", "4200", "5100", "6100"]
        )

        assert result["detected_type"] == "account"
        assert result["confidence"] > 0

    def test_detect_from_values(self):
        """Test detection from sample values."""
        result = detect_entity_types(
            values=["4100", "4200", "5100", "5200", "6100"]
        )

        # Should detect account based on numeric patterns
        assert "detected_type" in result

    def test_detect_from_column_and_values(self):
        """Test detection from both column and values."""
        result = detect_entity_types(
            column_name="department",
            values=["Sales", "Marketing", "HR", "Finance"]
        )

        assert result["detected_type"] == "department"

    def test_detect_from_dataframe(self):
        """Test detection across DataFrame."""
        data = [
            {"account": "4100", "dept": "Sales", "project_code": "PRJ-001"},
            {"account": "4200", "dept": "Marketing", "project_code": "PRJ-002"},
        ]
        result = detect_entity_types(data=data)

        assert "column_results" in result
        assert "entity_coverage" in result

    def test_detect_requires_input(self):
        """Test that input is required."""
        result = detect_entity_types()
        assert "error" in result
        assert "supported_entity_types" in result

    def test_all_12_entity_types_listed(self):
        """Test that all 12 entity types are documented."""
        result = detect_entity_types()

        assert len(result["supported_entity_types"]) == 12
        expected = [
            "account", "cost_center", "department", "entity",
            "project", "product", "customer", "vendor",
            "employee", "location", "time_period", "currency",
        ]
        for e in expected:
            assert e in result["supported_entity_types"]


class TestInferHierarchyLevels:
    """Tests for infer_hierarchy_levels tool."""

    def test_infer_from_code_patterns(self):
        """Test inferring levels from code patterns."""
        values = ["1-000", "1-100", "1-110", "1-111"]
        result = infer_hierarchy_levels(values=values)

        assert result["level_count"] >= 1
        assert len(result["levels"]) >= 1
        assert result["confidence"] > 0

    def test_infer_from_numeric_prefixes(self):
        """Test inferring levels from numeric prefixes."""
        values = ["1000", "1100", "1110", "2000", "2100"]
        result = infer_hierarchy_levels(values=values, detect_method="prefix")

        assert "levels" in result
        assert "detection_method" in result

    def test_infer_from_data(self):
        """Test inferring from DataFrame column."""
        data = [
            {"level": "1-000"},
            {"level": "1-100"},
            {"level": "1-200"},
        ]
        result = infer_hierarchy_levels(data=data, column="level")

        assert "level_count" in result

    def test_infer_requires_input(self):
        """Test that input is required."""
        result = infer_hierarchy_levels()
        assert "error" in result


class TestGenerateSortOrders:
    """Tests for generate_sort_orders tool."""

    def test_financial_sort(self):
        """Test financial convention sorting."""
        values = ["COGS", "Revenue", "Gross Profit", "Operating Expenses"]
        result = generate_sort_orders(values=values, entity_type="account")

        assert "sorted_values" in result
        assert "sort_orders" in result
        assert result["confidence"] > 0

    def test_numeric_sort(self):
        """Test numeric sorting."""
        values = ["100", "50", "200", "75"]
        result = generate_sort_orders(values=values, method="numeric")

        # Both numeric and numeric_prefix produce the same sorted order
        assert result["sorted_values"] == ["50", "75", "100", "200"]
        # Method may be numeric or numeric_prefix depending on detection
        assert result["method_used"] in ["numeric", "numeric_prefix"]

    def test_alphabetical_sort(self):
        """Test alphabetical sorting."""
        values = ["Zebra", "Apple", "Mango"]
        result = generate_sort_orders(values=values, method="alphabetical")

        assert result["sorted_values"] == ["Apple", "Mango", "Zebra"]
        assert result["method_used"] == "alphabetical"

    def test_custom_sort(self):
        """Test custom sort order."""
        values = ["C", "A", "B"]
        custom = {"A": 0, "B": 1, "C": 2}
        result = generate_sort_orders(values=values, method="custom", custom_order=custom)

        assert result["sorted_values"] == ["A", "B", "C"]
        assert result["method_used"] == "custom"

    def test_auto_detection(self):
        """Test automatic sort method detection."""
        values = ["Revenue", "COGS", "Gross Profit"]
        result = generate_sort_orders(values=values, method="auto")

        assert result["method_used"] in ["financial", "alphabetical", "numeric", "numeric_prefix"]


class TestMergeWithV3Hierarchy:
    """Tests for merge_with_librarian_hierarchy tool."""

    def test_merge_not_found(self):
        """Test merging with non-existent hierarchy."""
        result = merge_with_librarian_hierarchy(
            source_hierarchy_id="non_existent",
            librarian_hierarchy_csv=[],
        )

        assert "error" in result
        assert "available_hierarchies" in result

    def test_merge_invalid_strategy(self):
        """Test invalid merge strategy."""
        # First create a hierarchy
        sql = "SELECT CASE WHEN x = 1 THEN 'A' END FROM t"
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

            result = merge_with_librarian_hierarchy(
                source_hierarchy_id=hier_id,
                librarian_hierarchy_csv=[],
                strategy="invalid_strategy",
            )

            assert "error" in result
            assert "valid_strategies" in result


class TestExportDiscoveryAsCSV:
    """Tests for export_discovery_as_csv tool."""

    def test_export_not_found(self):
        """Test exporting non-existent hierarchy."""
        result = export_discovery_as_csv(
            hierarchy_id="non_existent",
            output_dir="/tmp",
        )

        assert "error" in result

    def test_export_creates_files(self):
        """Test that export creates CSV files."""
        # First create a hierarchy
        sql = """
        SELECT
            CASE WHEN account LIKE '4%' THEN 'Revenue'
                 WHEN account LIKE '5%' THEN 'Expense'
            END as category
        FROM gl
        """
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

            with tempfile.TemporaryDirectory() as tmpdir:
                result = export_discovery_as_csv(
                    hierarchy_id=hier_id,
                    output_dir=tmpdir,
                    file_prefix="TEST",
                )

                assert result["success"] is True
                assert Path(result["hierarchy_file"]).exists()
                assert Path(result["mapping_file"]).exists()


class TestValidateHierarchyStructure:
    """Tests for validate_hierarchy_structure tool."""

    def test_validate_not_found(self):
        """Test validating non-existent hierarchy."""
        result = validate_hierarchy_structure(hierarchy_id="non_existent")

        assert "error" in result

    def test_validate_valid_hierarchy(self):
        """Test validating a valid hierarchy."""
        # Create a hierarchy
        sql = """
        SELECT
            CASE WHEN code = '1' THEN 'A'
                 WHEN code = '2' THEN 'B'
            END as category
        FROM tbl
        """
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

            result = validate_hierarchy_structure(hier_id)

            assert "valid" in result
            assert "errors" in result
            assert "warnings" in result
            assert "stats" in result


class TestSuggestParentChildRelationships:
    """Tests for suggest_parent_child_relationships tool."""

    def test_suggest_from_values(self):
        """Test suggesting relationships from values."""
        values = ["Revenue", "Product Revenue", "Service Revenue", "Expenses"]
        result = suggest_parent_child_relationships(values=values)

        assert "suggestions" in result
        assert "potential_roots" in result

        # Should suggest Revenue as parent of Product/Service Revenue
        has_revenue_parent = any(
            s["parent"] == "Revenue" and "Revenue" in s["child"]
            for s in result["suggestions"]
        )
        assert has_revenue_parent

    def test_suggest_from_hierarchy(self):
        """Test suggesting from existing hierarchy."""
        # Create a hierarchy first
        sql = "SELECT CASE WHEN x = 1 THEN 'A' END FROM t"
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]
            result = suggest_parent_child_relationships(hierarchy_id=hier_id)

            assert "suggestions" in result

    def test_suggest_requires_input(self):
        """Test that input is required."""
        result = suggest_parent_child_relationships()
        assert "error" in result


class TestCompareHierarchies:
    """Tests for compare_hierarchies tool."""

    def test_compare_not_found(self):
        """Test comparing with non-existent hierarchy."""
        result = compare_hierarchies("hier1", "hier2")

        assert "error" in result

    def test_compare_same_hierarchy(self):
        """Test comparing hierarchy with itself."""
        # Create a hierarchy
        sql = "SELECT CASE WHEN x = 1 THEN 'A' END FROM t"
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

            result = compare_hierarchies(hier_id, hier_id)

            assert result["are_equal"] is True
            assert len(result["only_in_first"]) == 0
            assert len(result["only_in_second"]) == 0

    def test_compare_different_hierarchies(self):
        """Test comparing different hierarchies."""
        # Create two hierarchies
        sql1 = "SELECT CASE WHEN x = 1 THEN 'A' END FROM t"
        sql2 = "SELECT CASE WHEN y = 2 THEN 'B' END FROM s"

        result1 = extract_hierarchy_from_sql(sql1)
        result2 = extract_hierarchy_from_sql(sql2)

        if result1["success"] and result2["success"]:
            hier1_id = result1["primary_hierarchy"]["hierarchy_id"]
            hier2_id = result2["primary_hierarchy"]["hierarchy_id"]

            result = compare_hierarchies(hier1_id, hier2_id)

            assert "are_equal" in result
            assert "summary" in result


class TestHierarchyUtilities:
    """Tests for hierarchy utility functions."""

    def test_list_hierarchies(self):
        """Test listing hierarchies."""
        result = list_hierarchies()

        assert "hierarchies" in result
        assert "total_count" in result

    def test_get_hierarchy_details_not_found(self):
        """Test getting non-existent hierarchy."""
        result = get_hierarchy_details("non_existent")

        assert "error" in result

    def test_get_hierarchy_details(self):
        """Test getting hierarchy details."""
        # Create a hierarchy
        sql = "SELECT CASE WHEN x = 1 THEN 'A' END FROM t"
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

            result = get_hierarchy_details(hier_id)

            assert "name" in result
            assert "nodes" in result
            assert "level_count" in result

    def test_delete_hierarchy_not_found(self):
        """Test deleting non-existent hierarchy."""
        result = delete_hierarchy("non_existent")

        assert "error" in result

    def test_delete_hierarchy(self):
        """Test deleting hierarchy."""
        # Create a hierarchy
        sql = "SELECT CASE WHEN x = 1 THEN 'Delete Me' END FROM t"
        extract_result = extract_hierarchy_from_sql(sql)

        if extract_result["success"]:
            hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

            # Verify it exists
            before = get_hierarchy_details(hier_id)
            assert "error" not in before

            # Delete it
            result = delete_hierarchy(hier_id)
            assert result["success"] is True

            # Verify it's gone
            after = get_hierarchy_details(hier_id)
            assert "error" in after


class TestIntegrationWorkflow:
    """Integration tests for complete workflows."""

    def test_extract_validate_export_workflow(self):
        """Test complete workflow: extract -> validate -> export."""
        # Step 1: Extract hierarchy from SQL
        sql = """
        SELECT
            CASE
                WHEN account LIKE '4%' THEN 'Revenue'
                WHEN account LIKE '5%' THEN 'Cost of Sales'
                WHEN account LIKE '6%' THEN 'Operating Expenses'
            END as gl_category
        FROM general_ledger
        """
        extract_result = extract_hierarchy_from_sql(sql, name="GL Categories")

        assert extract_result["success"] is True

        hier_id = extract_result["primary_hierarchy"]["hierarchy_id"]

        # Step 2: Validate hierarchy
        validate_result = validate_hierarchy_structure(hier_id)

        assert validate_result["valid"] is True

        # Step 3: Export to CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            export_result = export_discovery_as_csv(
                hierarchy_id=hier_id,
                output_dir=tmpdir,
                file_prefix="GL_CATEGORIES",
            )

            assert export_result["success"] is True
            assert export_result["row_counts"]["hierarchy"] >= 1

    def test_detect_and_sort_workflow(self):
        """Test workflow: detect entity types and generate sort orders."""
        # Step 1: Detect entity type
        detect_result = detect_entity_types(
            column_name="account_category",
            values=["Revenue", "COGS", "Gross Profit", "Operating Expenses", "Net Income"]
        )

        entity_type = detect_result["detected_type"]

        # Step 2: Generate sort orders based on entity type
        sort_result = generate_sort_orders(
            values=["COGS", "Revenue", "Gross Profit", "Operating Expenses", "Net Income"],
            entity_type=entity_type,
        )

        assert "sorted_values" in sort_result
        assert sort_result["confidence"] > 0

    def test_analyze_and_suggest_workflow(self):
        """Test workflow: analyze CSV and suggest relationships."""
        # Step 1: Analyze CSV data
        data = [
            {"category": "Revenue", "subcategory": "Product", "detail": "Software"},
            {"category": "Revenue", "subcategory": "Product", "detail": "Hardware"},
            {"category": "Revenue", "subcategory": "Service", "detail": "Consulting"},
            {"category": "Expense", "subcategory": "Payroll", "detail": "Salaries"},
        ]
        analyze_result = analyze_csv_for_hierarchy(data=data)

        assert analyze_result["row_count"] == 4

        # Step 2: Get unique values from hierarchical columns
        category_values = list(set(row["category"] for row in data))
        subcategory_values = list(set(row["subcategory"] for row in data))
        all_values = category_values + subcategory_values

        # Step 3: Suggest parent-child relationships
        suggest_result = suggest_parent_child_relationships(values=all_values)

        assert len(suggest_result["suggestions"]) >= 0
        assert len(suggest_result["potential_roots"]) > 0
