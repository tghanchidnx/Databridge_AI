"""
Unit tests for DDL Generator.
"""

import pytest
from dataclasses import dataclass
from typing import List, Optional, Any

from src.generation.ddl_generator import (
    DDLGenerator,
    DDLConfig,
    GeneratedDDL,
    SQLDialect,
    DDLType,
)


@dataclass
class MockHierarchy:
    """Mock hierarchy for testing."""
    hierarchy_id: str = "TEST_001"
    project_id: str = "PROJECT_001"
    hierarchy_name: str = "Test Hierarchy"
    description: Optional[str] = "Test description"
    parent_id: Optional[str] = None
    hierarchy_type: str = "standard"
    aggregation_method: str = "sum"
    level_1: Optional[str] = "Level 1"
    level_2: Optional[str] = "Level 2"
    level_3: Optional[str] = None
    level_4: Optional[str] = None
    level_5: Optional[str] = None
    level_1_sort: int = 1
    level_2_sort: int = 1
    level_3_sort: int = 1
    level_4_sort: int = 1
    level_5_sort: int = 1
    include_flag: bool = True
    exclude_flag: bool = False
    transform_flag: bool = False
    calculation_flag: bool = False
    active_flag: bool = True
    is_leaf_node: bool = True
    source_mappings: List[dict] = None
    formula_config: Optional[dict] = None
    sort_order: int = 1

    def __post_init__(self):
        if self.source_mappings is None:
            self.source_mappings = [
                {
                    "source_database": "RAW",
                    "source_schema": "FINANCE",
                    "source_table": "GL_ACCOUNTS",
                    "source_column": "ACCOUNT_CODE",
                    "source_uid": "4100",
                    "precedence_group": "1",
                }
            ]


@dataclass
class MockProject:
    """Mock project for testing."""
    id: str = "PROJECT_001"
    name: str = "Test Project"


class TestDDLConfig:
    """Tests for DDLConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DDLConfig()

        assert config.dialect == SQLDialect.SNOWFLAKE
        assert config.target_schema == "HIERARCHIES"
        assert config.include_drop is True
        assert config.use_create_or_replace is True
        assert config.generate_tbl_0 is True
        assert config.generate_vw_1 is True
        assert config.generate_dt_2 is False

    def test_custom_config(self):
        """Test custom configuration."""
        config = DDLConfig(
            dialect=SQLDialect.POSTGRESQL,
            target_schema="ANALYTICS",
            generate_dt_2=True,
        )

        assert config.dialect == SQLDialect.POSTGRESQL
        assert config.target_schema == "ANALYTICS"
        assert config.generate_dt_2 is True


class TestSQLDialect:
    """Tests for SQLDialect enum."""

    def test_all_dialects_exist(self):
        """Test all expected dialects exist."""
        assert SQLDialect.SNOWFLAKE
        assert SQLDialect.POSTGRESQL
        assert SQLDialect.BIGQUERY
        assert SQLDialect.TSQL
        assert SQLDialect.MYSQL

    def test_dialect_values(self):
        """Test dialect string values."""
        assert SQLDialect.SNOWFLAKE.value == "snowflake"
        assert SQLDialect.POSTGRESQL.value == "postgresql"


class TestDDLGenerator:
    """Tests for DDLGenerator."""

    @pytest.fixture
    def generator(self):
        """Create a DDL generator."""
        return DDLGenerator()

    @pytest.fixture
    def mock_project(self):
        """Create a mock project."""
        return MockProject()

    @pytest.fixture
    def mock_hierarchies(self):
        """Create mock hierarchies."""
        return [
            MockHierarchy(
                hierarchy_id="ROOT_001",
                hierarchy_name="Root",
                level_1="Revenue",
                is_leaf_node=False,
            ),
            MockHierarchy(
                hierarchy_id="LEAF_001",
                hierarchy_name="Product Sales",
                parent_id="ROOT_001",
                level_1="Revenue",
                level_2="Product Sales",
                is_leaf_node=True,
            ),
        ]

    def test_generate_tbl_0(self, generator, mock_project, mock_hierarchies):
        """Test TBL_0 generation."""
        config = DDLConfig(
            generate_tbl_0=True,
            generate_vw_1=False,
        )

        scripts = generator.generate(mock_project, mock_hierarchies, config)

        # Should have TBL_0 and INSERT scripts
        assert len(scripts) >= 1
        tbl_script = next(s for s in scripts if s.tier == "TBL_0" and s.ddl_type == DDLType.CREATE_TABLE)
        assert tbl_script is not None
        assert "CREATE" in tbl_script.sql
        assert "TBL_0_TEST_PROJECT_HIERARCHY" in tbl_script.sql

    def test_generate_vw_1(self, generator, mock_project, mock_hierarchies):
        """Test VW_1 generation."""
        config = DDLConfig(
            generate_tbl_0=False,
            generate_vw_1=True,
        )

        scripts = generator.generate(mock_project, mock_hierarchies, config)

        vw_script = next((s for s in scripts if s.tier == "VW_1"), None)
        assert vw_script is not None
        assert "CREATE" in vw_script.sql
        assert "VIEW" in vw_script.sql
        assert "VW_1_TEST_PROJECT_MAPPING" in vw_script.sql

    def test_generate_all_tiers(self, generator, mock_project, mock_hierarchies):
        """Test generation of all tiers."""
        config = DDLConfig(
            generate_tbl_0=True,
            generate_vw_1=True,
            generate_dt_2=True,
            generate_dt_3a=True,
            generate_dt_3=True,
        )

        scripts = generator.generate(mock_project, mock_hierarchies, config)

        # Should have TBL_0, VW_1, DT_2, DT_3A, DT_3, and INSERT
        tiers = {s.tier for s in scripts if s.tier}
        assert "TBL_0" in tiers
        assert "VW_1" in tiers
        assert "DT_2" in tiers
        assert "DT_3A" in tiers
        assert "DT_3" in tiers

    def test_snowflake_dialect(self, generator, mock_project, mock_hierarchies):
        """Test Snowflake-specific syntax."""
        config = DDLConfig(
            dialect=SQLDialect.SNOWFLAKE,
            generate_tbl_0=True,
            generate_vw_1=False,
        )

        scripts = generator.generate(mock_project, mock_hierarchies, config)
        tbl_script = next(s for s in scripts if s.tier == "TBL_0" and s.ddl_type == DDLType.CREATE_TABLE)

        assert "CREATE OR REPLACE TABLE" in tbl_script.sql
        assert "VARIANT" in tbl_script.sql  # JSON type
        assert "TIMESTAMP_NTZ" in tbl_script.sql

    def test_postgresql_dialect(self, generator, mock_project, mock_hierarchies):
        """Test PostgreSQL-specific syntax."""
        config = DDLConfig(
            dialect=SQLDialect.POSTGRESQL,
            generate_tbl_0=True,
            generate_vw_1=False,
            use_create_or_replace=False,
        )

        scripts = generator.generate(mock_project, mock_hierarchies, config)
        tbl_script = next(s for s in scripts if s.tier == "TBL_0" and s.ddl_type == DDLType.CREATE_TABLE)

        assert "CREATE TABLE IF NOT EXISTS" in tbl_script.sql
        assert "JSONB" in tbl_script.sql  # JSON type
        assert "TIMESTAMP" in tbl_script.sql

    def test_generate_preview(self, generator, mock_project, mock_hierarchies):
        """Test preview generation."""
        config = DDLConfig(
            generate_tbl_0=True,
            generate_vw_1=True,
            generate_dt_2=True,
        )

        preview = generator.generate_preview(mock_project, mock_hierarchies, config)

        assert preview["project_name"] == "Test Project"
        assert preview["hierarchy_count"] == 2
        assert len(preview["objects"]) >= 3  # TBL_0, VW_1, DT_2

    def test_insert_generation(self, generator, mock_project, mock_hierarchies):
        """Test INSERT statement generation."""
        config = DDLConfig(
            generate_tbl_0=True,
            generate_vw_1=False,
        )

        scripts = generator.generate(mock_project, mock_hierarchies, config)
        insert_script = next((s for s in scripts if s.ddl_type == DDLType.INSERT), None)

        assert insert_script is not None
        assert "INSERT INTO" in insert_script.sql
        assert "HIERARCHY_ID" in insert_script.sql
        assert "ROOT_001" in insert_script.sql
        assert "LEAF_001" in insert_script.sql


class TestGeneratedDDL:
    """Tests for GeneratedDDL dataclass."""

    def test_full_name(self):
        """Test full_name property."""
        ddl = GeneratedDDL(
            ddl_type=DDLType.CREATE_TABLE,
            object_name="TEST_TABLE",
            schema_name="ANALYTICS",
            sql="CREATE TABLE...",
            dialect=SQLDialect.SNOWFLAKE,
        )

        assert ddl.full_name == "ANALYTICS.TEST_TABLE"

    def test_tier(self):
        """Test tier field."""
        ddl = GeneratedDDL(
            ddl_type=DDLType.CREATE_TABLE,
            object_name="TBL_0_TEST",
            schema_name="HIERARCHIES",
            sql="CREATE TABLE...",
            dialect=SQLDialect.SNOWFLAKE,
            tier="TBL_0",
        )

        assert ddl.tier == "TBL_0"


class TestDDLTypeMapping:
    """Tests for DDL type mappings."""

    def test_snowflake_type_mapping(self):
        """Test Snowflake type mappings."""
        generator = DDLGenerator()

        assert generator._get_type("string", SQLDialect.SNOWFLAKE) == "VARCHAR"
        assert generator._get_type("json", SQLDialect.SNOWFLAKE) == "VARIANT"
        assert generator._get_type("timestamp", SQLDialect.SNOWFLAKE) == "TIMESTAMP_NTZ"

    def test_postgresql_type_mapping(self):
        """Test PostgreSQL type mappings."""
        generator = DDLGenerator()

        assert "VARCHAR" in generator._get_type("string", SQLDialect.POSTGRESQL)
        assert generator._get_type("json", SQLDialect.POSTGRESQL) == "JSONB"
        assert generator._get_type("timestamp", SQLDialect.POSTGRESQL) == "TIMESTAMP"

    def test_bigquery_type_mapping(self):
        """Test BigQuery type mappings."""
        generator = DDLGenerator()

        assert generator._get_type("string", SQLDialect.BIGQUERY) == "STRING"
        assert generator._get_type("integer", SQLDialect.BIGQUERY) == "INT64"
        assert generator._get_type("boolean", SQLDialect.BIGQUERY) == "BOOL"
