"""
Tests for Phase 24: dbt Integration.

Tests the dbt project generation, model generation, source generation,
CI/CD pipeline generation, and MCP tools.
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

# Import types
from src.dbt_integration.types import (
    DbtMaterialization,
    DbtModelType,
    CiCdPlatform,
    DbtProjectConfig,
    DbtProject,
    DbtSource,
    DbtSourceTable,
    DbtColumn,
    DbtModelConfig,
    DbtMetric,
    CiCdConfig,
    ValidationResult,
)

# Import generators
from src.dbt_integration.project_generator import DbtProjectGenerator
from src.dbt_integration.model_generator import DbtModelGenerator
from src.dbt_integration.source_generator import DbtSourceGenerator, DbtMetricsGenerator
from src.dbt_integration.cicd_generator import CiCdGenerator


class TestDbtTypes:
    """Test dbt types and enums."""

    def test_materialization_enum(self):
        """Test DbtMaterialization enum."""
        assert DbtMaterialization.VIEW.value == "view"
        assert DbtMaterialization.TABLE.value == "table"
        assert DbtMaterialization.INCREMENTAL.value == "incremental"

    def test_model_type_enum(self):
        """Test DbtModelType enum."""
        assert DbtModelType.STAGING.value == "staging"
        assert DbtModelType.DIM.value == "dimension"
        assert DbtModelType.FACT.value == "fact"

    def test_cicd_platform_enum(self):
        """Test CiCdPlatform enum."""
        assert CiCdPlatform.GITHUB_ACTIONS.value == "github_actions"
        assert CiCdPlatform.GITLAB_CI.value == "gitlab_ci"
        assert CiCdPlatform.AZURE_DEVOPS.value == "azure_devops"

    def test_project_config(self):
        """Test DbtProjectConfig creation."""
        config = DbtProjectConfig(
            name="test_project",
            profile="snowflake_dev",
            target_database="ANALYTICS",
        )
        assert config.name == "test_project"
        assert config.profile == "snowflake_dev"
        assert config.version == "1.0.0"

    def test_dbt_column(self):
        """Test DbtColumn creation."""
        col = DbtColumn(
            name="account_code",
            description="GL account code",
            tests=["not_null", "unique"],
        )
        assert col.name == "account_code"
        assert "not_null" in col.tests

    def test_dbt_source_table(self):
        """Test DbtSourceTable creation."""
        table = DbtSourceTable(
            name="gl_accounts",
            description="General ledger accounts",
            columns=[
                DbtColumn(name="account_code"),
                DbtColumn(name="account_name"),
            ],
        )
        assert table.name == "gl_accounts"
        assert len(table.columns) == 2

    def test_dbt_source(self):
        """Test DbtSource creation."""
        source = DbtSource(
            name="raw",
            database="RAW_DB",
            schema_name="FINANCE",
            tables=[DbtSourceTable(name="gl_accounts")],
        )
        assert source.name == "raw"
        assert len(source.tables) == 1

    def test_cicd_config(self):
        """Test CiCdConfig creation."""
        config = CiCdConfig(
            platform=CiCdPlatform.GITHUB_ACTIONS,
            trigger_branches=["main", "develop"],
            run_tests=True,
        )
        assert config.platform == CiCdPlatform.GITHUB_ACTIONS
        assert "main" in config.trigger_branches


class TestDbtProjectGenerator:
    """Test project generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = DbtProjectGenerator(output_dir=self.temp_dir)

    def test_create_project(self):
        """Test creating a project."""
        project = self.generator.create_project(
            name="Test Project",
            profile="snowflake_dev",
            target_database="ANALYTICS",
        )

        assert project.config.name == "test_project"
        assert project.config.profile == "snowflake_dev"

    def test_create_duplicate_project_fails(self):
        """Test that duplicate project fails."""
        self.generator.create_project(name="test", profile="dev")

        with pytest.raises(ValueError, match="already exists"):
            self.generator.create_project(name="test", profile="dev")

    def test_get_project(self):
        """Test getting a project."""
        created = self.generator.create_project(name="myproject", profile="dev")
        retrieved = self.generator.get_project("myproject")

        assert retrieved is not None
        assert retrieved.id == created.id

    def test_list_projects(self):
        """Test listing projects."""
        self.generator.create_project(name="project1", profile="dev")
        self.generator.create_project(name="project2", profile="dev")

        projects = self.generator.list_projects()
        assert len(projects) == 2

    def test_generate_project_yml(self):
        """Test generating dbt_project.yml."""
        project = self.generator.create_project(name="test", profile="snowflake")
        yml = self.generator.generate_project_yml(project)

        assert "name: test" in yml
        assert "profile: snowflake" in yml
        assert "model-paths:" in yml

    def test_generate_profiles_yml(self):
        """Test generating profiles.yml."""
        project = self.generator.create_project(
            name="test",
            profile="snowflake_prod",
            target_database="ANALYTICS",
        )
        yml = self.generator.generate_profiles_yml(project)

        assert "snowflake_prod:" in yml
        assert "type: snowflake" in yml
        assert "ANALYTICS" in yml

    def test_scaffold_project(self):
        """Test scaffolding a complete project."""
        project = self.generator.create_project(name="test", profile="dev")
        files = self.generator.scaffold_project(project)

        assert "dbt_project.yml" in files
        assert "profiles.yml.template" in files
        assert ".gitignore" in files
        assert "README.md" in files
        assert "packages.yml" in files


class TestDbtModelGenerator:
    """Test model generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = DbtModelGenerator()

    def test_generate_staging_model(self):
        """Test generating a staging model."""
        sql = self.generator.generate_staging_model(
            model_name="gl_accounts",
            source_name="raw",
            source_table="GL_ACCOUNTS",
            columns=["ACCOUNT_CODE", "ACCOUNT_NAME"],
        )

        assert "{{ config(materialized='view') }}" in sql
        assert "ACCOUNT_CODE" in sql
        assert "{{ source('raw', 'GL_ACCOUNTS') }}" in sql

    def test_generate_staging_with_case_mappings(self):
        """Test staging model with CASE mappings."""
        sql = self.generator.generate_staging_model(
            model_name="gl_accounts",
            source_name="raw",
            source_table="GL_ACCOUNTS",
            case_mappings=[
                {"condition": "ACCOUNT_CODE LIKE '4%'", "result": "Revenue"},
                {"condition": "ACCOUNT_CODE LIKE '5%'", "result": "COGS"},
            ],
        )

        assert "CASE" in sql
        assert "WHEN ACCOUNT_CODE LIKE '4%' THEN 'Revenue'" in sql

    def test_generate_intermediate_model(self):
        """Test generating an intermediate model."""
        sql = self.generator.generate_intermediate_model(
            model_name="account_hierarchy",
            refs=["stg_gl_accounts"],
        )

        assert "{{ config(materialized='view') }}" in sql
        assert "{{ ref('stg_gl_accounts') }}" in sql
        assert "WITH" in sql

    def test_generate_dimension_model(self):
        """Test generating a dimension model."""
        sql = self.generator.generate_dimension_model(
            model_name="account",
            ref_model="int_account_hierarchy",
            hierarchy_columns=["level_1", "level_2", "level_3"],
        )

        assert "{{ config(materialized='table') }}" in sql
        assert "{{ ref('int_account_hierarchy') }}" in sql
        assert "level_1" in sql

    def test_generate_fact_model(self):
        """Test generating a fact model."""
        sql = self.generator.generate_fact_model(
            model_name="transactions",
            ref_model="stg_transactions",
            dimension_refs=[{"name": "dim_account", "ref": "dim_account"}],
            measure_columns=["amount", "quantity"],
        )

        assert "{{ config(materialized='table') }}" in sql
        assert "amount" in sql
        assert "quantity" in sql


class TestDbtSourceGenerator:
    """Test source generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = DbtSourceGenerator()

    def test_generate_sources_yml(self):
        """Test generating sources.yml."""
        source = DbtSource(
            name="raw",
            database="RAW_DB",
            schema_name="FINANCE",
            tables=[
                DbtSourceTable(name="gl_accounts"),
                DbtSourceTable(name="transactions"),
            ],
        )

        yml = self.generator.generate_sources_yml([source])

        assert "version: 2" in yml
        assert "name: raw" in yml
        assert "database: RAW_DB" in yml
        assert "gl_accounts" in yml

    def test_generate_from_hierarchy_mappings(self):
        """Test generating source from hierarchy mappings."""
        mappings = [
            {
                "source_database": "RAW_DB",
                "source_schema": "FINANCE",
                "source_table": "GL_ACCOUNTS",
                "source_column": "ACCOUNT_CODE",
            },
            {
                "source_database": "RAW_DB",
                "source_schema": "FINANCE",
                "source_table": "GL_ACCOUNTS",
                "source_column": "ACCOUNT_NAME",
            },
        ]

        source = self.generator.generate_from_hierarchy_mappings(mappings, "raw")

        assert source.name == "raw"
        assert len(source.tables) == 1
        assert source.tables[0].name == "gl_accounts"
        assert len(source.tables[0].columns) == 2

    def test_generate_schema_yml(self):
        """Test generating schema.yml."""
        models = [
            {
                "name": "stg_gl_accounts",
                "description": "Staged GL accounts",
                "columns": [
                    {"name": "account_code", "description": "Account code"},
                ],
            },
        ]

        yml = self.generator.generate_schema_yml(models)

        assert "version: 2" in yml
        assert "stg_gl_accounts" in yml
        assert "account_code" in yml


class TestDbtMetricsGenerator:
    """Test metrics generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = DbtMetricsGenerator()

    def test_generate_metrics_yml(self):
        """Test generating metrics.yml."""
        metrics = [
            {
                "name": "total_revenue",
                "label": "Total Revenue",
                "type": "derived",
                "expression": "SUM(amount)",
            },
        ]

        yml = self.generator.generate_metrics_yml(metrics)

        assert "version: 2" in yml
        assert "total_revenue" in yml
        assert "SUM(amount)" in yml

    def test_generate_from_formula_groups(self):
        """Test generating metrics from formula groups."""
        formula_groups = [
            {
                "name": "revenue",
                "rules": [
                    {"operation": "SUM", "target": "total", "operands": ["a", "b"]},
                ],
            },
        ]

        metrics = self.generator.generate_from_formula_groups(formula_groups)

        assert len(metrics) == 1
        assert "revenue" in metrics[0]["name"]


class TestCiCdGenerator:
    """Test CI/CD generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = CiCdGenerator()

    def test_generate_github_actions(self):
        """Test generating GitHub Actions workflow."""
        config = CiCdConfig(
            platform=CiCdPlatform.GITHUB_ACTIONS,
            trigger_branches=["main"],
            run_tests=True,
        )

        yml = self.generator.generate_github_actions(config, "test_project")

        assert "name: dbt CI - test_project" in yml
        assert "push:" in yml
        assert "dbt build" in yml

    def test_generate_gitlab_ci(self):
        """Test generating GitLab CI configuration."""
        config = CiCdConfig(
            platform=CiCdPlatform.GITLAB_CI,
            trigger_branches=["main"],
        )

        yml = self.generator.generate_gitlab_ci(config, "test_project")

        assert "stages:" in yml
        assert "dbt deps" in yml

    def test_generate_azure_devops(self):
        """Test generating Azure DevOps pipeline."""
        config = CiCdConfig(
            platform=CiCdPlatform.AZURE_DEVOPS,
            trigger_branches=["main"],
        )

        yml = self.generator.generate_azure_devops(config, "test_project")

        assert "trigger:" in yml
        assert "pool:" in yml
        assert "vmImage: ubuntu-latest" in yml

    def test_get_pipeline_path(self):
        """Test getting pipeline file paths."""
        assert self.generator.get_pipeline_path(CiCdPlatform.GITHUB_ACTIONS) == ".github/workflows/dbt_ci.yml"
        assert self.generator.get_pipeline_path(CiCdPlatform.GITLAB_CI) == ".gitlab-ci.yml"
        assert self.generator.get_pipeline_path(CiCdPlatform.AZURE_DEVOPS) == "azure-pipelines.yml"


class TestMCPTools:
    """Test MCP tools registration."""

    def test_register_dbt_tools(self):
        """Test registering dbt tools."""
        from src.dbt_integration.mcp_tools import register_dbt_tools

        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        result = register_dbt_tools(mock_mcp)

        assert result["tools_registered"] == 8
        assert "create_dbt_project" in result["tools"]
        assert "generate_dbt_model" in result["tools"]
        assert "generate_dbt_sources" in result["tools"]
        assert "generate_dbt_schema" in result["tools"]
        assert "generate_dbt_metrics" in result["tools"]
        assert "generate_cicd_pipeline" in result["tools"]
        assert "validate_dbt_project" in result["tools"]
        assert "export_dbt_project" in result["tools"]


class TestModuleExports:
    """Test module exports."""

    def test_all_exports(self):
        """Test that all expected items are exported."""
        from src.dbt_integration import (
            # Types
            DbtMaterialization,
            DbtModelType,
            CiCdPlatform,
            DbtProjectConfig,
            DbtProject,
            DbtSource,
            DbtSourceTable,
            DbtColumn,
            DbtModelConfig,
            DbtMetric,
            CiCdConfig,
            ValidationResult,
            # Generators
            DbtProjectGenerator,
            DbtModelGenerator,
            DbtSourceGenerator,
            DbtMetricsGenerator,
            CiCdGenerator,
            # MCP
            register_dbt_tools,
        )

        # Just verify imports work
        assert DbtMaterialization.VIEW is not None
        assert DbtModelType.STAGING is not None
        assert CiCdPlatform.GITHUB_ACTIONS is not None
