"""
Tests for Wright Module Phase 31 Enhancements.

Tests:
- HierarchyQualityValidator: ID_SOURCE typo detection, orphan detection
- IDSourceNormalizer: Alias handling, fuzzy matching
- GroupFilterPrecedenceEngine: Multi-round filter analysis
- DDLDiffComparator: DDL comparison and impact analysis
"""

import pytest
from typing import Dict, List, Any


# ============================================================
# Quality Validator Tests
# ============================================================

class TestHierarchyQualityValidator:
    """Tests for HierarchyQualityValidator."""

    def test_detect_id_source_typo(self):
        """Test detection of known ID_SOURCE typos."""
        from src.wright.quality_validator import (
            HierarchyQualityValidator,
            HierarchyIssueType,
        )

        validator = HierarchyQualityValidator()

        hierarchies = [
            {"HIERARCHY_ID": 1, "ACTIVE_FLAG": True, "CALCULATION_FLAG": False},
        ]
        mappings = [
            {"FK_REPORT_KEY": 1, "ID_SOURCE": "BILLING_CATEGRY_CODE", "ID": "4100"},
            {"FK_REPORT_KEY": 1, "ID_SOURCE": "BILLING_CATEGORY_TYPE", "ID": "REV"},
        ]

        result = validator.validate_hierarchy_data(hierarchies, mappings)

        # Should detect both typos
        typo_issues = [
            i for i in result.issues
            if i.issue_type == HierarchyIssueType.ID_SOURCE_TYPO.value
        ]
        assert len(typo_issues) >= 2

        # Should suggest corrections
        assert "BILLING_CATEGRY_CODE" in result.typo_suggestions
        assert result.typo_suggestions["BILLING_CATEGRY_CODE"] == "BILLING_CATEGORY_CODE"

    def test_detect_orphan_nodes(self):
        """Test detection of orphan nodes without mappings."""
        from src.wright.quality_validator import (
            HierarchyQualityValidator,
            HierarchyIssueType,
        )

        validator = HierarchyQualityValidator()

        hierarchies = [
            {"HIERARCHY_ID": 1, "ACTIVE_FLAG": True, "CALCULATION_FLAG": False},
            {"HIERARCHY_ID": 2, "ACTIVE_FLAG": True, "CALCULATION_FLAG": False},  # No mapping
        ]
        mappings = [
            {"FK_REPORT_KEY": 1, "ID_SOURCE": "ACCOUNT_CODE", "ID": "4100"},
        ]

        result = validator.validate_hierarchy_data(hierarchies, mappings)

        orphan_issues = [
            i for i in result.issues
            if i.issue_type == HierarchyIssueType.ORPHAN_NODE.value
        ]
        assert len(orphan_issues) == 1
        assert "2" in orphan_issues[0].affected_values[0]

    def test_detect_duplicate_keys(self):
        """Test detection of duplicate hierarchy keys."""
        from src.wright.quality_validator import (
            HierarchyQualityValidator,
            HierarchyIssueType,
        )

        validator = HierarchyQualityValidator()

        hierarchies = [
            {"HIERARCHY_ID": 1, "ACTIVE_FLAG": True},
            {"HIERARCHY_ID": 1, "ACTIVE_FLAG": True},  # Duplicate
        ]
        mappings = []

        result = validator.validate_hierarchy_data(hierarchies, mappings)

        dup_issues = [
            i for i in result.issues
            if i.issue_type == HierarchyIssueType.DUPLICATE_KEY.value
        ]
        assert len(dup_issues) == 1

    def test_valid_hierarchy_passes(self):
        """Test that valid hierarchy passes validation."""
        from src.wright.quality_validator import HierarchyQualityValidator

        validator = HierarchyQualityValidator()

        hierarchies = [
            {"HIERARCHY_ID": 1, "ACTIVE_FLAG": True, "CALCULATION_FLAG": False},
            {"HIERARCHY_ID": 2, "ACTIVE_FLAG": True, "CALCULATION_FLAG": True,
             "FORMULA_GROUP": "TOTAL", "FORMULA_LOGIC": "SUM", "FORMULA_PARAM_REF": "TOTAL"},
        ]
        mappings = [
            {"FK_REPORT_KEY": 1, "ID_SOURCE": "ACCOUNT_CODE", "ID": "4100"},
        ]

        result = validator.validate_hierarchy_data(hierarchies, mappings)

        # Should have no high-severity issues
        high_severity = [i for i in result.issues if i.severity in ("HIGH", "CRITICAL")]
        assert len(high_severity) == 0


# ============================================================
# Alias Normalizer Tests
# ============================================================

class TestIDSourceNormalizer:
    """Tests for IDSourceNormalizer."""

    def test_normalize_known_typo(self):
        """Test normalization of known typos."""
        from src.wright.alias_normalizer import IDSourceNormalizer

        normalizer = IDSourceNormalizer()

        result = normalizer.normalize("BILLING_CATEGRY_CODE")
        assert result.normalized == "BILLING_CATEGORY_CODE"
        assert result.was_aliased is True
        assert result.confidence == 1.0

    def test_normalize_canonical_value(self):
        """Test that canonical values are unchanged."""
        from src.wright.alias_normalizer import IDSourceNormalizer

        normalizer = IDSourceNormalizer()

        result = normalizer.normalize("ACCOUNT_CODE")
        assert result.normalized == "ACCOUNT_CODE"
        assert result.was_aliased is False

    def test_auto_detect_similar(self):
        """Test auto-detection of similar values."""
        from src.wright.alias_normalizer import IDSourceNormalizer

        normalizer = IDSourceNormalizer(auto_detect_threshold=0.7)

        # This should auto-detect as similar to BILLING_CATEGORY_CODE
        result = normalizer.normalize("BILLING_CATGORY_CODE", auto_detect=True)
        assert result.was_aliased is True
        assert result.confidence > 0.7

    def test_get_physical_column(self):
        """Test physical column lookup."""
        from src.wright.alias_normalizer import IDSourceNormalizer

        normalizer = IDSourceNormalizer()

        physical, aliased = normalizer.get_physical_column("BILLING_CATEGRY_CODE")
        assert physical == "ACCT.ACCOUNT_BILLING_CATEGORY_CODE"
        assert aliased is True

    def test_normalize_mapping_data(self):
        """Test normalization of mapping data."""
        from src.wright.alias_normalizer import IDSourceNormalizer

        normalizer = IDSourceNormalizer()

        mappings = [
            {"ID_SOURCE": "BILLING_CATEGRY_CODE", "ID": "4100"},
            {"ID_SOURCE": "ACCOUNT_CODE", "ID": "4200"},
        ]

        normalized, results = normalizer.normalize_mapping_data(mappings)

        assert normalized[0]["ID_SOURCE"] == "BILLING_CATEGORY_CODE"
        assert normalized[1]["ID_SOURCE"] == "ACCOUNT_CODE"
        assert sum(1 for r in results if r.was_aliased) == 1

    def test_generate_case_statement(self):
        """Test CASE statement generation."""
        from src.wright.alias_normalizer import IDSourceNormalizer

        normalizer = IDSourceNormalizer()

        sql = normalizer.generate_case_statement(include_aliases=True)

        assert "CASE" in sql
        assert "BILLING_CATEGORY_CODE" in sql
        assert "BILLING_CATEGRY_CODE" in sql  # Alias included


# ============================================================
# Filter Engine Tests
# ============================================================

class TestGroupFilterPrecedenceEngine:
    """Tests for GroupFilterPrecedenceEngine."""

    def test_analyze_mappings(self):
        """Test analysis of GROUP_FILTER_PRECEDENCE mappings."""
        from src.wright.filter_engine import GroupFilterPrecedenceEngine

        engine = GroupFilterPrecedenceEngine()

        mappings = [
            {"FILTER_GROUP_1": "Revenue", "GROUP_FILTER_PRECEDENCE": 1, "ID_TABLE": "DIM_ACCOUNT"},
            {"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 1, "ID_TABLE": "DIM_DEDUCT"},
            {"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 2, "ID_TABLE": "DIM_PRODUCT"},
            {"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 3, "ID_TABLE": "DIM_ACCOUNT"},
        ]

        patterns = engine.analyze_mappings(mappings)

        assert "Revenue" in patterns
        assert "Deducts" in patterns
        assert patterns["Revenue"].max_precedence == 1
        assert patterns["Deducts"].max_precedence == 3

    def test_generate_dt2_ctes(self):
        """Test DT_2 CTE generation."""
        from src.wright.filter_engine import GroupFilterPrecedenceEngine

        engine = GroupFilterPrecedenceEngine()

        mappings = [
            {"FILTER_GROUP_1": "Taxes", "GROUP_FILTER_PRECEDENCE": 1, "ID_TABLE": "DIM_ACCOUNT"},
            {"FILTER_GROUP_1": "Taxes", "GROUP_FILTER_PRECEDENCE": 2, "ID_TABLE": "DIM_DEDUCT"},
        ]

        engine.analyze_mappings(mappings)
        sql = engine.generate_dt2_ctes()

        assert "PRECEDENCE_1_RESOLVED" in sql
        assert "PRECEDENCE_2_RESOLVED" in sql

    def test_generate_union_branches(self):
        """Test UNION ALL branch generation."""
        from src.wright.filter_engine import GroupFilterPrecedenceEngine

        engine = GroupFilterPrecedenceEngine()

        mappings = [
            {"FILTER_GROUP_1": "Revenue", "GROUP_FILTER_PRECEDENCE": 1, "ID_TABLE": "DIM_ACCOUNT"},
            {"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 1, "ID_TABLE": "DIM_DEDUCT"},
            {"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 2, "ID_TABLE": "DIM_PRODUCT"},
        ]

        engine.analyze_mappings(mappings)
        branches = engine.generate_union_branches()

        assert len(branches) >= 2


# ============================================================
# DDL Diff Tests
# ============================================================

class TestDDLDiffComparator:
    """Tests for DDLDiffComparator."""

    def test_compare_identical_ddl(self):
        """Test comparison of identical DDL."""
        from src.wright.ddl_diff import DDLDiffComparator

        comparator = DDLDiffComparator()

        ddl = """
        CREATE VIEW VW_TEST AS
        SELECT col1, col2 FROM table1
        """

        result = comparator.compare_ddl(ddl, ddl)

        assert result.is_identical is True
        assert result.similarity == 1.0

    def test_detect_column_removal(self):
        """Test detection of removed columns."""
        from src.wright.ddl_diff import DDLDiffComparator

        comparator = DDLDiffComparator()

        baseline = """
        CREATE VIEW VW_TEST AS
        SELECT col1, col2, col3 FROM table1
        """
        generated = """
        CREATE VIEW VW_TEST AS
        SELECT col1, col2 FROM table1
        """

        result = comparator.compare_ddl(generated, baseline)

        assert result.is_identical is False
        removed = [c for c in result.column_diffs if c.status == "removed"]
        assert len(removed) == 1
        assert removed[0].column_name == "COL3"

    def test_detect_column_addition(self):
        """Test detection of added columns."""
        from src.wright.ddl_diff import DDLDiffComparator

        comparator = DDLDiffComparator()

        baseline = """
        CREATE VIEW VW_TEST AS
        SELECT col1 FROM table1
        """
        generated = """
        CREATE VIEW VW_TEST AS
        SELECT col1, new_col FROM table1
        """

        result = comparator.compare_ddl(generated, baseline)

        added = [c for c in result.column_diffs if c.status == "added"]
        assert len(added) == 1
        assert added[0].column_name == "NEW_COL"

    def test_detect_breaking_changes(self):
        """Test detection of breaking changes."""
        from src.wright.ddl_diff import DDLDiffComparator

        comparator = DDLDiffComparator()

        baseline = """
        CREATE VIEW VW_TEST AS
        SELECT key_column, data_column FROM table1
        LEFT JOIN table2 ON table1.id = table2.id
        """
        generated = """
        CREATE VIEW VW_TEST AS
        SELECT key_column FROM table1
        """

        result = comparator.compare_ddl(generated, baseline)

        assert len(result.breaking_changes) > 0
        # Should flag column removal and JOIN removal as breaking

    def test_unified_diff_generation(self):
        """Test unified diff generation."""
        from src.wright.ddl_diff import DDLDiffComparator

        comparator = DDLDiffComparator()

        baseline = "CREATE VIEW V1 AS SELECT a FROM t"
        generated = "CREATE VIEW V1 AS SELECT b FROM t"

        result = comparator.compare_ddl(generated, baseline)

        assert result.unified_diff != ""
        assert "-" in result.unified_diff or "+" in result.unified_diff


# ============================================================
# Integration Tests
# ============================================================

class TestWrightIntegration:
    """Integration tests for Wright module enhancements."""

    def test_quality_to_normalizer_flow(self):
        """Test flow from quality validation to normalization."""
        from src.wright.quality_validator import validate_hierarchy_quality
        from src.wright.alias_normalizer import IDSourceNormalizer

        # First validate to detect typos
        hierarchies = [{"HIERARCHY_ID": 1, "ACTIVE_FLAG": True}]
        mappings = [
            {"FK_REPORT_KEY": 1, "ID_SOURCE": "BILLING_CATEGRY_CODE", "ID": "4100"},
        ]

        validation = validate_hierarchy_quality(hierarchies, mappings)

        # Use detected typos to configure normalizer
        normalizer = IDSourceNormalizer()
        for alias, canonical in validation.typo_suggestions.items():
            normalizer.add_alias(alias, canonical)

        # Normalize the data
        normalized, _ = normalizer.normalize_mapping_data(mappings)
        assert normalized[0]["ID_SOURCE"] == "BILLING_CATEGORY_CODE"

    def test_filter_engine_with_mart_config(self):
        """Test filter engine integration with mart config."""
        from src.wright.filter_engine import GroupFilterPrecedenceEngine
        from src.wright.config_generator import MartConfigGenerator

        # Analyze patterns
        mappings = [
            {"FILTER_GROUP_1": "Taxes", "GROUP_FILTER_PRECEDENCE": 1,
             "ID_TABLE": "DIM_ACCOUNT", "ID": "4%"},
            {"FILTER_GROUP_1": "Taxes", "GROUP_FILTER_PRECEDENCE": 2,
             "ID_TABLE": "DIM_DEDUCT", "ID": "D1"},
        ]

        engine = GroupFilterPrecedenceEngine()
        patterns = engine.analyze_mappings(mappings)
        branches = engine.generate_union_branches()

        # Create mart config with detected branches
        config_gen = MartConfigGenerator(output_dir="data/test_mart_configs")

        try:
            config = config_gen.create_config(
                project_name="test_filter_integration",
                report_type="TEST",
                hierarchy_table="TEST.HIERARCHY",
                mapping_table="TEST.MAPPING",
                account_segment="TEST",
                has_group_filter_precedence=True,
            )

            # Add branches from engine
            for branch in branches:
                config_gen.add_join_pattern(
                    config_name="test_filter_integration",
                    name=branch["name"],
                    join_keys=branch["join_keys"],
                    fact_keys=branch["fact_keys"],
                )

            # Verify config has patterns
            saved_config = config_gen.get_config("test_filter_integration")
            assert len(saved_config.join_patterns) == len(branches)

        finally:
            config_gen.delete_config("test_filter_integration")


# ============================================================
# Module Import Tests
# ============================================================

class TestWrightModuleImports:
    """Test that all module imports work correctly."""

    def test_import_quality_validator(self):
        """Test quality_validator imports."""
        from src.wright.quality_validator import (
            HierarchyQualityValidator,
            HierarchyIssueType,
            HierarchyValidationResult,
            validate_hierarchy_quality,
        )
        assert HierarchyQualityValidator is not None
        assert HierarchyIssueType is not None

    def test_import_alias_normalizer(self):
        """Test alias_normalizer imports."""
        from src.wright.alias_normalizer import (
            IDSourceNormalizer,
            AliasMapping,
            NormalizationResult,
            get_normalizer,
            normalize_id_source,
        )
        assert IDSourceNormalizer is not None
        assert get_normalizer is not None

    def test_import_filter_engine(self):
        """Test filter_engine imports."""
        from src.wright.filter_engine import (
            GroupFilterPrecedenceEngine,
            FilterPrecedence,
            FilterPattern,
            FilterRound,
            analyze_group_filter_precedence,
        )
        assert GroupFilterPrecedenceEngine is not None
        assert FilterPrecedence is not None

    def test_import_ddl_diff(self):
        """Test ddl_diff imports."""
        from src.wright.ddl_diff import (
            DDLDiffComparator,
            DDLDiffResult,
            ColumnDiff,
            compare_generated_ddl,
        )
        assert DDLDiffComparator is not None
        assert compare_generated_ddl is not None

    def test_import_from_wright_package(self):
        """Test imports from wright package __init__."""
        from src.wright import (
            # Phase 31 enhancements
            HierarchyQualityValidator,
            IDSourceNormalizer,
            GroupFilterPrecedenceEngine,
            DDLDiffComparator,
            validate_hierarchy_quality,
            normalize_id_source,
            analyze_group_filter_precedence,
            compare_generated_ddl,
        )
        assert HierarchyQualityValidator is not None
        assert IDSourceNormalizer is not None
        assert GroupFilterPrecedenceEngine is not None
        assert DDLDiffComparator is not None
