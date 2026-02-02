"""
Unit tests for dbt Project Generator.
"""

import pytest
from dataclasses import dataclass
from typing import List, Optional

from src.generation.dbt_generator import (
    DbtProjectGenerator,
    DbtConfig,
    GeneratedDbtProject,
    DbtMaterialization,
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
    include_flag: bool = True
    exclude_flag: bool = False
    transform_flag: bool = False
    calculation_flag: bool = False
    active_flag: bool = True
    is_leaf_node: bool = True
    source_mappings: List[dict] = None
    formula_config: Optional[dict] = None
    sort_order: int = 1
    created_at: str = "2024-01-01"
    updated_at: str = "2024-01-01"

    def __post_init__(self):
        if self.source_mappings is None:
            self.source_mappings = []


@dataclass
class MockProject:
    """Mock project for testing."""
    id: str = "PROJECT_001"
    name: str = "Test Project"


class TestDbtConfig:
    """Tests for DbtConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DbtConfig(project_name="test_project")

        assert config.project_name == "test_project"
        assert config.source_database == "RAW"
        assert config.source_schema == "HIERARCHIES"
        assert config.target_schema == "ANALYTICS"
        assert config.materialization == DbtMaterialization.TABLE
        assert config.generate_tests is True
        assert config.generate_docs is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = DbtConfig(
            project_name="custom_project",
            source_database="WAREHOUSE",
            materialization=DbtMaterialization.VIEW,
        )

        assert config.project_name == "custom_project"
        assert config.source_database == "WAREHOUSE"
        assert config.materialization == DbtMaterialization.VIEW


class TestDbtMaterialization:
    """Tests for DbtMaterialization enum."""

    def test_all_materializations_exist(self):
        """Test all expected materializations exist."""
        assert DbtMaterialization.TABLE
        assert DbtMaterialization.VIEW
        assert DbtMaterialization.INCREMENTAL
        assert DbtMaterialization.EPHEMERAL

    def test_materialization_values(self):
        """Test materialization string values."""
        assert DbtMaterialization.TABLE.value == "table"
        assert DbtMaterialization.VIEW.value == "view"


class TestDbtProjectGenerator:
    """Tests for DbtProjectGenerator."""

    @pytest.fixture
    def generator(self):
        """Create a dbt generator."""
        return DbtProjectGenerator()

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

    def test_generate_project(self, generator, mock_project, mock_hierarchies):
        """Test basic project generation."""
        config = DbtConfig(project_name="test_hierarchy")

        result = generator.generate(mock_project, mock_hierarchies, config)

        assert isinstance(result, GeneratedDbtProject)
        assert result.project_name == "test_hierarchy"
        assert result.file_count > 0
        assert result.model_count > 0

    def test_generate_dbt_project_yml(self, generator, mock_project, mock_hierarchies):
        """Test dbt_project.yml generation."""
        config = DbtConfig(project_name="test_hierarchy")

        result = generator.generate(mock_project, mock_hierarchies, config)

        project_yml = next((f for f in result.files if f.name == "dbt_project.yml"), None)
        assert project_yml is not None
        assert "name:" in project_yml.content
        assert "version:" in project_yml.content
        assert "model-paths:" in project_yml.content

    def test_generate_sources_yml(self, generator, mock_project, mock_hierarchies):
        """Test sources.yml generation."""
        config = DbtConfig(project_name="test_hierarchy")

        result = generator.generate(mock_project, mock_hierarchies, config)

        sources_yml = next((f for f in result.files if f.name == "sources.yml"), None)
        assert sources_yml is not None
        assert "sources:" in sources_yml.content
        assert "tables:" in sources_yml.content

    def test_generate_staging_models(self, generator, mock_project, mock_hierarchies):
        """Test staging model generation."""
        config = DbtConfig(
            project_name="test_hierarchy",
            generate_staging=True,
        )

        result = generator.generate(mock_project, mock_hierarchies, config)

        staging_models = [f for f in result.files if "staging" in f.path and f.file_type == "sql"]
        assert len(staging_models) >= 1

        # Check staging model content
        stg_model = staging_models[0]
        assert "{{ config(" in stg_model.content
        assert "WITH source AS" in stg_model.content
        assert "renamed AS" in stg_model.content

    def test_generate_mart_models(self, generator, mock_project, mock_hierarchies):
        """Test mart model generation."""
        config = DbtConfig(
            project_name="test_hierarchy",
            generate_marts=True,
        )

        result = generator.generate(mock_project, mock_hierarchies, config)

        mart_models = [f for f in result.files if "marts" in f.path and f.file_type == "sql"]
        assert len(mart_models) >= 1

        # Should have dimension and fact models
        model_names = [f.name for f in mart_models]
        assert any("dim_" in name for name in model_names)
        assert any("fct_" in name for name in model_names)

    def test_generate_tests(self, generator, mock_project, mock_hierarchies):
        """Test that tests are generated when enabled."""
        config = DbtConfig(
            project_name="test_hierarchy",
            generate_tests=True,
        )

        result = generator.generate(mock_project, mock_hierarchies, config)

        schema_files = [f for f in result.files if "schema" in f.name.lower() and f.file_type == "yml"]
        assert len(schema_files) >= 1

        # Check for test definitions
        schema_content = schema_files[0].content
        assert "tests:" in schema_content or "unique" in schema_content or "not_null" in schema_content

    def test_generate_readme(self, generator, mock_project, mock_hierarchies):
        """Test README generation."""
        config = DbtConfig(
            project_name="test_hierarchy",
            generate_docs=True,
        )

        result = generator.generate(mock_project, mock_hierarchies, config)

        readme = next((f for f in result.files if f.name == "README.md"), None)
        assert readme is not None
        assert "test_hierarchy" in readme.content
        assert "Usage" in readme.content

    def test_no_staging_when_disabled(self, generator, mock_project, mock_hierarchies):
        """Test staging models not generated when disabled."""
        config = DbtConfig(
            project_name="test_hierarchy",
            generate_staging=False,
        )

        result = generator.generate(mock_project, mock_hierarchies, config)

        staging_models = [f for f in result.files if "staging" in f.path and f.file_type == "sql"]
        assert len(staging_models) == 0

    def test_materialization_in_models(self, generator, mock_project, mock_hierarchies):
        """Test materialization configuration in models."""
        config = DbtConfig(
            project_name="test_hierarchy",
            materialization=DbtMaterialization.VIEW,
        )

        result = generator.generate(mock_project, mock_hierarchies, config)

        mart_models = [f for f in result.files if "marts" in f.path and f.file_type == "sql"]
        assert len(mart_models) > 0

        # Check materialization in config
        mart_content = mart_models[0].content
        assert "materialized='view'" in mart_content


class TestGeneratedDbtProject:
    """Tests for GeneratedDbtProject dataclass."""

    def test_file_count(self):
        """Test file_count property."""
        from src.generation.dbt_generator import GeneratedFile

        project = GeneratedDbtProject(
            project_name="test",
            files=[
                GeneratedFile(name="a.sql", path="a.sql", content="", file_type="sql"),
                GeneratedFile(name="b.yml", path="b.yml", content="", file_type="yml"),
            ],
            model_count=1,
            source_count=1,
        )

        assert project.file_count == 2

    def test_model_count(self):
        """Test model_count field."""
        project = GeneratedDbtProject(
            project_name="test",
            files=[],
            model_count=5,
            source_count=1,
        )

        assert project.model_count == 5
