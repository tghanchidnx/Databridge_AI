"""Integration tests for generation workflow.

Tests the hierarchy type system and basic generation components.
"""

import pytest

from src.hierarchy.types import (
    HierarchyType,
    AggregationMethod,
    TransformationType,
    HierarchyTypeConfig,
    TYPE_CONFIGS,
)
from src.generation.ddl_generator import SQLDialect, DDLType, GeneratedDDL, DDLConfig


class TestHierarchyTypeSystem:
    """Integration tests for hierarchy type system."""

    def test_all_types_have_configs(self):
        """Test that all hierarchy types have configurations."""
        for h_type in HierarchyType:
            assert h_type in TYPE_CONFIGS
            config = TYPE_CONFIGS[h_type]
            assert isinstance(config, HierarchyTypeConfig)
            assert config.hierarchy_type == h_type

    def test_aggregation_methods_available(self):
        """Test all aggregation methods are available."""
        methods = list(AggregationMethod)
        assert len(methods) > 0
        assert AggregationMethod.SUM in methods
        assert AggregationMethod.AVG in methods

    def test_transformation_types_available(self):
        """Test all transformation types are available."""
        transformations = list(TransformationType)
        assert len(transformations) > 0
        assert TransformationType.PASSTHROUGH in transformations

    def test_type_configs_have_required_attributes(self):
        """Test type configs have all required attributes."""
        for h_type, config in TYPE_CONFIGS.items():
            assert hasattr(config, "display_name")
            assert hasattr(config, "description")
            assert hasattr(config, "supports_formulas")
            assert hasattr(config, "default_aggregation")
            assert hasattr(config, "default_transformation")


class TestDDLGeneratorStructures:
    """Tests for DDL generator data structures."""

    def test_sql_dialect_enum(self):
        """Test SQL dialect enum values."""
        assert SQLDialect.SNOWFLAKE.value == "snowflake"
        assert SQLDialect.POSTGRESQL.value == "postgresql"
        assert SQLDialect.BIGQUERY.value == "bigquery"
        assert SQLDialect.TSQL.value == "tsql"
        assert SQLDialect.MYSQL.value == "mysql"

    def test_ddl_type_enum(self):
        """Test DDL type enum values."""
        assert DDLType.CREATE_TABLE.value == "create_table"
        assert DDLType.CREATE_VIEW.value == "create_view"
        assert DDLType.INSERT.value == "insert"

    def test_generated_ddl_dataclass(self):
        """Test GeneratedDDL dataclass."""
        ddl = GeneratedDDL(
            ddl_type=DDLType.CREATE_TABLE,
            object_name="test_table",
            schema_name="public",
            sql="CREATE TABLE public.test_table (id INT)",
            dialect=SQLDialect.POSTGRESQL,
        )

        assert ddl.full_name == "public.test_table"
        assert ddl.ddl_type == DDLType.CREATE_TABLE

    def test_ddl_config_defaults(self):
        """Test DDLConfig default values."""
        config = DDLConfig()

        assert config.dialect == SQLDialect.SNOWFLAKE
        assert config.target_schema == "HIERARCHIES"
        assert config.use_create_or_replace is True

    def test_ddl_config_custom(self):
        """Test DDLConfig with custom values."""
        config = DDLConfig(
            dialect=SQLDialect.POSTGRESQL,
            target_database="my_db",
            target_schema="my_schema",
            include_drop=False,
        )

        assert config.dialect == SQLDialect.POSTGRESQL
        assert config.target_database == "my_db"
        assert config.target_schema == "my_schema"
        assert config.include_drop is False


class TestTypeConfigBehavior:
    """Tests for hierarchy type configuration behavior."""

    def test_standard_type_defaults(self):
        """Test STANDARD type has expected defaults."""
        config = TYPE_CONFIGS[HierarchyType.STANDARD]

        assert config.display_name == "Standard"
        assert config.default_aggregation == AggregationMethod.SUM

    def test_calculation_type_supports_formulas(self):
        """Test CALCULATION type supports formulas."""
        config = TYPE_CONFIGS[HierarchyType.CALCULATION]

        assert config.supports_formulas is True

    def test_allocation_type_config(self):
        """Test ALLOCATION type has allocation support."""
        config = TYPE_CONFIGS[HierarchyType.ALLOCATION]

        assert config.supports_allocation is True

    def test_type_display_names_unique(self):
        """Test all type display names are unique."""
        display_names = [c.display_name for c in TYPE_CONFIGS.values()]
        assert len(display_names) == len(set(display_names))
