"""
Unit tests for Hierarchy Types.
"""

import pytest
from dataclasses import dataclass
from typing import List, Optional

from src.hierarchy.types import (
    HierarchyType,
    AggregationMethod,
    TransformationType,
    HierarchyTypeConfig,
    TransformationConfig,
    AggregationConfig,
    get_type_config,
    get_all_hierarchy_types,
    TYPE_CONFIGS,
)


class TestHierarchyType:
    """Tests for HierarchyType enum."""

    def test_all_types_exist(self):
        """Test all expected hierarchy types exist."""
        assert HierarchyType.STANDARD
        assert HierarchyType.GROUPING
        assert HierarchyType.XREF
        assert HierarchyType.CALCULATION
        assert HierarchyType.ALLOCATION

    def test_type_values(self):
        """Test type string values."""
        assert HierarchyType.STANDARD.value == "standard"
        assert HierarchyType.GROUPING.value == "grouping"
        assert HierarchyType.XREF.value == "xref"
        assert HierarchyType.CALCULATION.value == "calculation"
        assert HierarchyType.ALLOCATION.value == "allocation"


class TestAggregationMethod:
    """Tests for AggregationMethod enum."""

    def test_all_methods_exist(self):
        """Test all expected aggregation methods exist."""
        assert AggregationMethod.SUM
        assert AggregationMethod.AVG
        assert AggregationMethod.MIN
        assert AggregationMethod.MAX
        assert AggregationMethod.COUNT
        assert AggregationMethod.FIRST
        assert AggregationMethod.LAST
        assert AggregationMethod.WEIGHTED_AVG


class TestTransformationType:
    """Tests for TransformationType enum."""

    def test_all_transformations_exist(self):
        """Test all expected transformation types exist."""
        assert TransformationType.PASSTHROUGH
        assert TransformationType.NEGATE
        assert TransformationType.ABSOLUTE
        assert TransformationType.PERCENTAGE
        assert TransformationType.SCALE
        assert TransformationType.REMAP


class TestHierarchyTypeConfig:
    """Tests for HierarchyTypeConfig."""

    def test_standard_config(self):
        """Test standard type configuration."""
        config = get_type_config(HierarchyType.STANDARD)

        assert config.hierarchy_type == HierarchyType.STANDARD
        assert config.display_name == "Standard"
        assert config.supports_aggregation is True
        assert config.supports_formulas is False
        assert config.supports_source_mappings is True
        assert config.generate_unnest_view is True

    def test_grouping_config(self):
        """Test grouping type configuration."""
        config = get_type_config(HierarchyType.GROUPING)

        assert config.hierarchy_type == HierarchyType.GROUPING
        assert config.supports_aggregation is True
        assert config.generate_aggregation_table is True

    def test_xref_config(self):
        """Test cross-reference type configuration."""
        config = get_type_config(HierarchyType.XREF)

        assert config.hierarchy_type == HierarchyType.XREF
        assert config.supports_aggregation is False
        assert config.generate_dimension_table is False

    def test_calculation_config(self):
        """Test calculation type configuration."""
        config = get_type_config(HierarchyType.CALCULATION)

        assert config.hierarchy_type == HierarchyType.CALCULATION
        assert config.supports_formulas is True
        assert config.supports_source_mappings is False
        assert config.generate_unnest_view is False

    def test_allocation_config(self):
        """Test allocation type configuration."""
        config = get_type_config(HierarchyType.ALLOCATION)

        assert config.hierarchy_type == HierarchyType.ALLOCATION
        assert config.supports_allocation is True
        assert config.generate_aggregation_table is True


class TestTransformationConfig:
    """Tests for TransformationConfig."""

    def test_passthrough(self):
        """Test passthrough transformation."""
        config = TransformationConfig(
            transformation_type=TransformationType.PASSTHROUGH
        )

        assert config.apply(100) == 100
        assert config.apply(-50) == -50
        assert config.apply("text") == "text"

    def test_negate(self):
        """Test negate transformation."""
        config = TransformationConfig(
            transformation_type=TransformationType.NEGATE
        )

        assert config.apply(100) == -100
        assert config.apply(-50) == 50
        assert config.apply(0) == 0

    def test_absolute(self):
        """Test absolute transformation."""
        config = TransformationConfig(
            transformation_type=TransformationType.ABSOLUTE
        )

        assert config.apply(100) == 100
        assert config.apply(-50) == 50

    def test_scale(self):
        """Test scale transformation."""
        config = TransformationConfig(
            transformation_type=TransformationType.SCALE,
            scale_factor=1.5,
        )

        assert config.apply(100) == 150
        assert config.apply(-20) == -30

    def test_remap(self):
        """Test remap transformation."""
        config = TransformationConfig(
            transformation_type=TransformationType.REMAP,
            remap_values={"A": "X", "B": "Y"},
        )

        assert config.apply("A") == "X"
        assert config.apply("B") == "Y"
        assert config.apply("C") == "C"  # Unmapped value


class TestAggregationConfig:
    """Tests for AggregationConfig."""

    def test_get_sql_function(self):
        """Test SQL function mapping."""
        config_sum = AggregationConfig(method=AggregationMethod.SUM)
        assert config_sum.get_sql_function() == "SUM"

        config_avg = AggregationConfig(method=AggregationMethod.AVG)
        assert config_avg.get_sql_function() == "AVG"

        config_count = AggregationConfig(method=AggregationMethod.COUNT)
        assert config_count.get_sql_function() == "COUNT"


class TestGetAllHierarchyTypes:
    """Tests for get_all_hierarchy_types function."""

    def test_returns_all_types(self):
        """Test that all types are returned."""
        types = get_all_hierarchy_types()

        assert len(types) == len(HierarchyType)

    def test_type_structure(self):
        """Test the structure of returned types."""
        types = get_all_hierarchy_types()

        for t in types:
            assert "type" in t
            assert "display_name" in t
            assert "description" in t
            assert "supports_aggregation" in t
            assert "supports_formulas" in t
            assert "supports_source_mappings" in t

    def test_standard_type_in_list(self):
        """Test that standard type is in the list."""
        types = get_all_hierarchy_types()

        standard = next((t for t in types if t["type"] == "standard"), None)
        assert standard is not None
        assert standard["display_name"] == "Standard"


class TestTypeConfigs:
    """Tests for TYPE_CONFIGS dictionary."""

    def test_all_types_have_configs(self):
        """Test all hierarchy types have configurations."""
        for h_type in HierarchyType:
            assert h_type in TYPE_CONFIGS

    def test_config_consistency(self):
        """Test that configs match their type."""
        for h_type, config in TYPE_CONFIGS.items():
            assert config.hierarchy_type == h_type
