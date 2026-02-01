"""
Unit tests for Phase 4 MCP Tools - Project Generation & Documentation.

Tests for 8 tools:
25. generate_librarian_project - Complete Librarian project
26. generate_hierarchy_from_discovery - Create hierarchy
27. generate_vw1_views - VW_1 tier views
28. generate_dbt_models - dbt model files
29. generate_data_dictionary - Auto dictionary
30. export_lineage_diagram - Mermaid/D2 diagram
31. validate_generated_project - Completeness check
32. preview_deployment_scripts - DDL preview
"""

import json
import os
import tempfile

import pytest


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def sample_hierarchy():
    """Create a sample ConvertedHierarchy for testing."""
    from databridge_discovery.hierarchy.case_to_hierarchy import (
        ConvertedHierarchy,
        HierarchyNode,
    )

    nodes = {
        "root": HierarchyNode(
            id="root",
            name="Total",
            value="Total",
            level=0,
            parent_id=None,
            children=["rev", "exp"],
            sort_order=0,
        ),
        "rev": HierarchyNode(
            id="rev",
            name="Revenue",
            value="Revenue",
            level=1,
            parent_id="root",
            children=["rev_prod", "rev_svc"],
            sort_order=100,
        ),
        "exp": HierarchyNode(
            id="exp",
            name="Expense",
            value="Expense",
            level=1,
            parent_id="root",
            children=[],
            sort_order=200,
        ),
        "rev_prod": HierarchyNode(
            id="rev_prod",
            name="Product Revenue",
            value="Product Revenue",
            level=2,
            parent_id="rev",
            children=[],
            sort_order=110,
        ),
        "rev_svc": HierarchyNode(
            id="rev_svc",
            name="Service Revenue",
            value="Service Revenue",
            level=2,
            parent_id="rev",
            children=[],
            sort_order=120,
        ),
    }

    return ConvertedHierarchy(
        id="test_hier_1",
        name="GL_HIERARCHY",
        entity_type="account",
        source_column="account_code",
        source_table="gl_accounts",
        level_count=2,
        total_nodes=5,
        source_case_id="case_1",
        nodes=nodes,
        root_nodes=["root"],
        mapping={
            "50%": "rev_prod",
            "51%": "rev_svc",
            "60%": "exp",
        },
        confidence=0.85,
    )


@pytest.fixture
def setup_hierarchy_storage(sample_hierarchy):
    """Set up hierarchy in global storage."""
    from databridge_discovery.mcp import tools

    tools._hierarchies["test_hier_1"] = sample_hierarchy
    yield
    # Cleanup
    if "test_hier_1" in tools._hierarchies:
        del tools._hierarchies["test_hier_1"]


# =============================================================================
# Project Generator Tests
# =============================================================================


class TestProjectGenerator:
    """Tests for project_generator module."""

    def test_project_config_defaults(self):
        """Test ProjectConfig default values."""
        from databridge_discovery.generation.project_generator import (
            ProjectConfig,
            OutputFormat,
            ProjectTier,
        )

        config = ProjectConfig(project_name="TEST")

        assert config.project_name == "TEST"
        assert config.output_format == OutputFormat.SNOWFLAKE
        assert config.target_schema == "HIERARCHIES"
        assert ProjectTier.TBL_0 in config.include_tiers
        assert ProjectTier.VW_1 in config.include_tiers

    def test_generator_creates_tbl_0(self, sample_hierarchy):
        """Test TBL_0 generation."""
        from databridge_discovery.generation.project_generator import (
            ProjectGenerator,
            ProjectConfig,
            ProjectTier,
        )

        generator = ProjectGenerator()
        config = ProjectConfig(
            project_name="TEST_PROJECT",
            include_tiers=[ProjectTier.TBL_0],
        )

        project = generator.generate([sample_hierarchy], config)

        # Should have TBL_0 files
        tbl_files = [f for f in project.files if f.tier == ProjectTier.TBL_0]
        assert len(tbl_files) > 0
        assert any("CREATE" in f.content for f in tbl_files)

    def test_generator_creates_vw_1(self, sample_hierarchy):
        """Test VW_1 generation."""
        from databridge_discovery.generation.project_generator import (
            ProjectGenerator,
            ProjectConfig,
            ProjectTier,
        )

        generator = ProjectGenerator()
        config = ProjectConfig(
            project_name="TEST_PROJECT",
            include_tiers=[ProjectTier.TBL_0, ProjectTier.VW_1],
        )

        project = generator.generate([sample_hierarchy], config)

        # Should have VW_1 files
        vw_files = [f for f in project.files if f.tier == ProjectTier.VW_1]
        assert len(vw_files) > 0
        assert any("CREATE" in f.content and "VIEW" in f.content for f in vw_files)

    def test_generator_creates_deployment_script(self, sample_hierarchy):
        """Test deployment script generation."""
        from databridge_discovery.generation.project_generator import (
            ProjectGenerator,
            ProjectConfig,
        )

        generator = ProjectGenerator()
        config = ProjectConfig(project_name="TEST_PROJECT")

        project = generator.generate([sample_hierarchy], config)

        # Should have deployment script
        deploy_files = [f for f in project.files if f.name == "deploy_all.sql"]
        assert len(deploy_files) == 1
        assert "Deployment Script" in deploy_files[0].content

    def test_generator_write_project(self, sample_hierarchy):
        """Test writing project to disk."""
        from databridge_discovery.generation.project_generator import (
            ProjectGenerator,
            ProjectConfig,
        )

        generator = ProjectGenerator()
        config = ProjectConfig(project_name="TEST_PROJECT")

        project = generator.generate([sample_hierarchy], config)

        with tempfile.TemporaryDirectory() as tmpdir:
            files_written = generator.write_project(project, tmpdir)

            assert len(files_written) > 0
            for name, path in files_written.items():
                assert os.path.exists(path)


# =============================================================================
# View Generator Tests
# =============================================================================


class TestViewGenerator:
    """Tests for view_generator module."""

    def test_view_config_defaults(self):
        """Test ViewConfig default values."""
        from databridge_discovery.generation.view_generator import (
            ViewConfig,
            ViewDialect,
        )

        config = ViewConfig()

        assert config.target_schema == "HIERARCHIES"
        assert config.dialect == ViewDialect.SNOWFLAKE

    def test_generate_mapping_view(self, sample_hierarchy):
        """Test mapping view generation."""
        from databridge_discovery.generation.view_generator import (
            ViewGenerator,
            ViewDialect,
            ViewType,
        )

        generator = ViewGenerator(dialect=ViewDialect.SNOWFLAKE)
        view = generator.generate_mapping_view(sample_hierarchy)

        assert view.view_type == ViewType.VW_1_MAPPING
        assert "CREATE" in view.ddl
        assert "VIEW" in view.ddl
        assert "VW_1" in view.name

    def test_generate_unnest_view_snowflake(self, sample_hierarchy):
        """Test unnest view for Snowflake."""
        from databridge_discovery.generation.view_generator import (
            ViewGenerator,
            ViewDialect,
        )

        generator = ViewGenerator(dialect=ViewDialect.SNOWFLAKE)
        view = generator.generate_unnest_view(sample_hierarchy)

        assert "LATERAL FLATTEN" in view.ddl
        assert "PARSE_JSON" in view.ddl

    def test_generate_unnest_view_bigquery(self, sample_hierarchy):
        """Test unnest view for BigQuery."""
        from databridge_discovery.generation.view_generator import (
            ViewGenerator,
            ViewDialect,
            ViewConfig,
        )

        generator = ViewGenerator(dialect=ViewDialect.BIGQUERY)
        config = ViewConfig(dialect=ViewDialect.BIGQUERY)
        view = generator.generate_unnest_view(sample_hierarchy, config)

        assert "UNNEST" in view.ddl

    def test_generate_rollup_view(self, sample_hierarchy):
        """Test rollup view generation."""
        from databridge_discovery.generation.view_generator import ViewGenerator

        generator = ViewGenerator()
        view = generator.generate_rollup_view(sample_hierarchy)

        assert "ROLLUP" in view.name
        assert "GROUP BY" in view.ddl

    def test_generate_all_views(self, sample_hierarchy):
        """Test generating multiple view types."""
        from databridge_discovery.generation.view_generator import (
            ViewGenerator,
            ViewType,
        )

        generator = ViewGenerator()
        views = generator.generate_all_views(
            sample_hierarchy,
            view_types=[ViewType.VW_1_MAPPING, ViewType.VW_1_FILTERED],
        )

        assert len(views) == 2
        assert any(v.view_type == ViewType.VW_1_MAPPING for v in views)
        assert any(v.view_type == ViewType.VW_1_FILTERED for v in views)


# =============================================================================
# dbt Generator Tests
# =============================================================================


class TestDbtGenerator:
    """Tests for dbt_generator module."""

    def test_dbt_config_defaults(self):
        """Test DbtGeneratorConfig default values."""
        from databridge_discovery.generation.dbt_generator import DbtGeneratorConfig

        config = DbtGeneratorConfig(project_name="test")

        assert config.source_database == "RAW"
        assert config.include_staging is True

    def test_generate_project(self, sample_hierarchy):
        """Test dbt project generation."""
        from databridge_discovery.generation.dbt_generator import (
            DbtGenerator,
            DbtGeneratorConfig,
        )

        generator = DbtGenerator()
        config = DbtGeneratorConfig(project_name="test_analytics")

        project = generator.generate_project([sample_hierarchy], config)

        assert project.name == "test_analytics"
        assert project.model_count > 0
        assert len(project.sources) > 0

    def test_generate_staging_model(self, sample_hierarchy):
        """Test staging model generation."""
        from databridge_discovery.generation.dbt_generator import (
            DbtGenerator,
            DbtGeneratorConfig,
            DbtModelLayer,
        )

        generator = DbtGenerator()
        config = DbtGeneratorConfig(project_name="test")

        model = generator.generate_model(sample_hierarchy, DbtModelLayer.STAGING, config)

        assert model.layer == DbtModelLayer.STAGING
        assert "stg_" in model.name
        assert "{{ source(" in model.sql

    def test_generate_dimension_model(self, sample_hierarchy):
        """Test dimension mart model generation."""
        from databridge_discovery.generation.dbt_generator import (
            DbtGenerator,
            DbtGeneratorConfig,
            DbtModelLayer,
        )

        generator = DbtGenerator()
        config = DbtGeneratorConfig(project_name="test")

        model = generator.generate_model(sample_hierarchy, DbtModelLayer.MARTS, config)

        assert model.layer == DbtModelLayer.MARTS
        assert "dim_" in model.name
        assert "hierarchy_sk" in model.sql

    def test_write_project(self, sample_hierarchy):
        """Test writing dbt project to disk."""
        from databridge_discovery.generation.dbt_generator import (
            DbtGenerator,
            DbtGeneratorConfig,
        )

        generator = DbtGenerator()
        config = DbtGeneratorConfig(project_name="test_project")

        project = generator.generate_project([sample_hierarchy], config)

        with tempfile.TemporaryDirectory() as tmpdir:
            files = generator.write_project(project, tmpdir)

            assert "dbt_project.yml" in files
            assert os.path.exists(files["dbt_project.yml"])


# =============================================================================
# SQL Generator Tests
# =============================================================================


class TestSQLGenerator:
    """Tests for sql_generator module."""

    def test_generate_table_ddl_snowflake(self, sample_hierarchy):
        """Test table DDL for Snowflake."""
        from databridge_discovery.generation.sql_generator import (
            SQLGenerator,
            SQLDialect,
        )

        generator = SQLGenerator(dialect=SQLDialect.SNOWFLAKE)
        ddl = generator.generate_table_ddl(sample_hierarchy)

        assert "CREATE OR REPLACE TABLE" in ddl.sql
        assert "HIERARCHY_ID" in ddl.sql
        assert "VARCHAR" in ddl.sql

    def test_generate_table_ddl_postgresql(self, sample_hierarchy):
        """Test table DDL for PostgreSQL."""
        from databridge_discovery.generation.sql_generator import (
            SQLGenerator,
            SQLDialect,
            SQLGeneratorConfig,
        )

        generator = SQLGenerator(dialect=SQLDialect.POSTGRESQL)
        config = SQLGeneratorConfig(dialect=SQLDialect.POSTGRESQL)
        ddl = generator.generate_table_ddl(sample_hierarchy, config)

        assert "CREATE TABLE" in ddl.sql
        assert "TIMESTAMP" in ddl.sql

    def test_generate_view_ddl(self, sample_hierarchy):
        """Test view DDL generation."""
        from databridge_discovery.generation.sql_generator import SQLGenerator

        generator = SQLGenerator()
        ddl = generator.generate_view_ddl(sample_hierarchy)

        assert "CREATE" in ddl.sql
        assert "VIEW" in ddl.sql
        assert "VW_1" in ddl.object_name

    def test_generate_merge_ddl(self, sample_hierarchy):
        """Test MERGE DDL generation."""
        from databridge_discovery.generation.sql_generator import SQLGenerator

        generator = SQLGenerator()
        ddl = generator.generate_merge_ddl(sample_hierarchy)

        assert "MERGE INTO" in ddl.sql
        assert "WHEN MATCHED" in ddl.sql
        assert "WHEN NOT MATCHED" in ddl.sql

    def test_generate_deployment_script(self, sample_hierarchy):
        """Test deployment script generation."""
        from databridge_discovery.generation.sql_generator import SQLGenerator

        generator = SQLGenerator()
        script = generator.generate_deployment_script([sample_hierarchy])

        assert "Deployment Script" in script
        assert "CREATE SCHEMA" in script
        assert "CREATE" in script


# =============================================================================
# Data Dictionary Tests
# =============================================================================


class TestDataDictionary:
    """Tests for data_dictionary module."""

    def test_generate_dictionary(self, sample_hierarchy):
        """Test dictionary generation."""
        from databridge_discovery.documentation.data_dictionary import (
            DataDictionaryGenerator,
        )

        generator = DataDictionaryGenerator()
        dictionary = generator.generate([sample_hierarchy], "Test Dictionary")

        assert dictionary.name == "Test Dictionary"
        assert dictionary.table_count > 0
        assert dictionary.total_columns > 0

    def test_to_markdown(self, sample_hierarchy):
        """Test markdown export."""
        from databridge_discovery.documentation.data_dictionary import (
            DataDictionaryGenerator,
        )

        generator = DataDictionaryGenerator()
        dictionary = generator.generate([sample_hierarchy], "Test")
        markdown = generator.to_markdown(dictionary)

        assert "# Test" in markdown
        assert "| Column |" in markdown
        assert "HIERARCHY_ID" in markdown

    def test_to_csv(self, sample_hierarchy):
        """Test CSV export."""
        from databridge_discovery.documentation.data_dictionary import (
            DataDictionaryGenerator,
        )

        generator = DataDictionaryGenerator()
        dictionary = generator.generate([sample_hierarchy], "Test")
        csv_content = generator.to_csv(dictionary)

        assert "Table,Column,DataType" in csv_content
        assert "HIERARCHY_ID" in csv_content

    def test_column_category_inference(self):
        """Test column category inference."""
        from databridge_discovery.documentation.data_dictionary import (
            DataDictionaryGenerator,
            ColumnCategory,
        )

        generator = DataDictionaryGenerator()

        assert generator._infer_category("USER_ID") == ColumnCategory.KEY
        assert generator._infer_category("IS_ACTIVE") == ColumnCategory.FLAG
        assert generator._infer_category("CREATED_AT") == ColumnCategory.AUDIT
        assert generator._infer_category("LEVEL_1") == ColumnCategory.HIERARCHY


# =============================================================================
# Lineage Documenter Tests
# =============================================================================


class TestLineageDocumenter:
    """Tests for lineage_documenter module."""

    def test_build_lineage(self, sample_hierarchy):
        """Test lineage building."""
        from databridge_discovery.documentation.lineage_documenter import (
            LineageDocumenter,
        )

        documenter = LineageDocumenter()
        diagram = documenter.build_lineage([sample_hierarchy], "Test Lineage")

        assert diagram.name == "Test Lineage"
        assert diagram.node_count > 0
        assert diagram.edge_count > 0

    def test_to_mermaid(self, sample_hierarchy):
        """Test Mermaid export."""
        from databridge_discovery.documentation.lineage_documenter import (
            LineageDocumenter,
        )

        documenter = LineageDocumenter()
        diagram = documenter.build_lineage([sample_hierarchy], "Test")
        mermaid = documenter.to_mermaid(diagram)

        assert "```mermaid" in mermaid
        assert "flowchart" in mermaid
        assert "```" in mermaid

    def test_to_d2(self, sample_hierarchy):
        """Test D2 export."""
        from databridge_discovery.documentation.lineage_documenter import (
            LineageDocumenter,
        )

        documenter = LineageDocumenter()
        diagram = documenter.build_lineage([sample_hierarchy], "Test")
        d2 = documenter.to_d2(diagram)

        assert "shape:" in d2

    def test_to_html(self, sample_hierarchy):
        """Test HTML export."""
        from databridge_discovery.documentation.lineage_documenter import (
            LineageDocumenter,
        )

        documenter = LineageDocumenter()
        diagram = documenter.build_lineage([sample_hierarchy], "Test")
        html = documenter.to_html(diagram)

        assert "<!DOCTYPE html>" in html
        assert "mermaid" in html
        assert "<script" in html


# =============================================================================
# Markdown Exporter Tests
# =============================================================================


class TestMarkdownExporter:
    """Tests for markdown_exporter module."""

    def test_export_hierarchies(self, sample_hierarchy):
        """Test hierarchy export."""
        from databridge_discovery.documentation.markdown_exporter import (
            MarkdownExporter,
            ExportConfig,
        )

        exporter = MarkdownExporter()
        config = ExportConfig(title="Test Export")
        result = exporter.export_hierarchies([sample_hierarchy], config)

        assert "# Test Export" in result.content
        assert "GL_HIERARCHY" in result.content

    def test_export_project(self, sample_hierarchy):
        """Test project report export."""
        from databridge_discovery.documentation.markdown_exporter import (
            MarkdownExporter,
        )

        exporter = MarkdownExporter()
        result = exporter.export_project(
            project_name="Test Project",
            hierarchies=[sample_hierarchy],
        )

        assert "# Test Project" in result.content
        assert "Executive Summary" in result.content
        assert "Key Metrics" in result.content

    def test_write_to_file(self, sample_hierarchy):
        """Test writing export to file."""
        from databridge_discovery.documentation.markdown_exporter import (
            MarkdownExporter,
        )

        exporter = MarkdownExporter()
        result = exporter.export_hierarchies([sample_hierarchy])

        with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as f:
            path = f.name

        try:
            written_path = exporter.write(result, path)
            assert os.path.exists(written_path)

            with open(written_path, "r") as f:
                content = f.read()
                assert "GL_HIERARCHY" in content
        finally:
            if os.path.exists(path):
                os.unlink(path)


# =============================================================================
# MCP Tools Integration Tests
# =============================================================================


class TestPhase4MCPTools:
    """Integration tests for Phase 4 MCP tools."""

    def test_generate_librarian_project(self, setup_hierarchy_storage):
        """Test generate_librarian_project tool."""
        from databridge_discovery.mcp.tools import generate_librarian_project

        result = generate_librarian_project(
            hierarchy_ids=["test_hier_1"],
            project_name="TEST_PROJECT",
        )

        assert "project_id" in result
        assert result["project_name"] == "TEST_PROJECT"
        assert result["file_count"] > 0

    def test_generate_vw1_views(self, setup_hierarchy_storage):
        """Test generate_vw1_views tool."""
        from databridge_discovery.mcp.tools import generate_vw1_views

        result = generate_vw1_views(
            hierarchy_ids=["test_hier_1"],
            dialect="snowflake",
        )

        assert "view_count" in result
        assert result["view_count"] > 0
        assert "views" in result

    def test_generate_dbt_models(self, setup_hierarchy_storage):
        """Test generate_dbt_models tool."""
        from databridge_discovery.mcp.tools import generate_dbt_models

        result = generate_dbt_models(
            hierarchy_ids=["test_hier_1"],
            project_name="test_analytics",
        )

        assert result["project_name"] == "test_analytics"
        assert result["model_count"] > 0

    def test_generate_data_dictionary(self, setup_hierarchy_storage):
        """Test generate_data_dictionary tool."""
        from databridge_discovery.mcp.tools import generate_data_dictionary

        result = generate_data_dictionary(
            hierarchy_ids=["test_hier_1"],
            output_format="markdown",
        )

        assert "table_count" in result
        assert result["table_count"] > 0
        assert "content" in result

    def test_export_lineage_diagram(self, setup_hierarchy_storage):
        """Test export_lineage_diagram tool."""
        from databridge_discovery.mcp.tools import export_lineage_diagram

        result = export_lineage_diagram(
            hierarchy_ids=["test_hier_1"],
            output_format="mermaid",
        )

        assert "node_count" in result
        assert result["node_count"] > 0
        assert "content" in result
        assert "mermaid" in result["content"]

    def test_preview_deployment_scripts(self, setup_hierarchy_storage):
        """Test preview_deployment_scripts tool."""
        from databridge_discovery.mcp.tools import preview_deployment_scripts

        result = preview_deployment_scripts(
            hierarchy_ids=["test_hier_1"],
            dialect="snowflake",
        )

        assert "script" in result
        assert "CREATE" in result["script"]
        assert result["hierarchy_count"] == 1

    def test_validate_generated_project(self, setup_hierarchy_storage):
        """Test validate_generated_project tool."""
        from databridge_discovery.mcp.tools import (
            generate_librarian_project,
            validate_generated_project,
        )

        # First generate a project
        gen_result = generate_librarian_project(
            hierarchy_ids=["test_hier_1"],
            project_name="VALIDATION_TEST",
        )

        # Then validate it
        val_result = validate_generated_project(gen_result["project_id"])

        assert "valid" in val_result
        assert "errors" in val_result
        assert "warnings" in val_result

    def test_no_hierarchies_error(self):
        """Test error handling when no hierarchies exist."""
        from databridge_discovery.mcp.tools import generate_librarian_project

        result = generate_librarian_project(
            hierarchy_ids=["nonexistent"],
        )

        assert "error" in result


# =============================================================================
# Output Format Tests
# =============================================================================


class TestOutputFormats:
    """Tests for different output format support."""

    def test_snowflake_dialect(self, sample_hierarchy):
        """Test Snowflake-specific syntax."""
        from databridge_discovery.generation.sql_generator import (
            SQLGenerator,
            SQLDialect,
        )

        generator = SQLGenerator(dialect=SQLDialect.SNOWFLAKE)
        ddl = generator.generate_table_ddl(sample_hierarchy)

        assert "CREATE OR REPLACE TABLE" in ddl.sql
        assert "TIMESTAMP_NTZ" in ddl.sql

    def test_postgresql_dialect(self, sample_hierarchy):
        """Test PostgreSQL-specific syntax."""
        from databridge_discovery.generation.sql_generator import (
            SQLGenerator,
            SQLDialect,
            SQLGeneratorConfig,
        )

        generator = SQLGenerator(dialect=SQLDialect.POSTGRESQL)
        config = SQLGeneratorConfig(dialect=SQLDialect.POSTGRESQL)
        ddl = generator.generate_table_ddl(sample_hierarchy, config)

        assert "CREATE TABLE" in ddl.sql
        assert "TIMESTAMP" in ddl.sql

    def test_bigquery_dialect(self, sample_hierarchy):
        """Test BigQuery-specific syntax."""
        from databridge_discovery.generation.sql_generator import (
            SQLGenerator,
            SQLDialect,
            SQLGeneratorConfig,
        )

        generator = SQLGenerator(dialect=SQLDialect.BIGQUERY)
        config = SQLGeneratorConfig(dialect=SQLDialect.BIGQUERY)
        ddl = generator.generate_table_ddl(sample_hierarchy, config)

        assert "STRING" in ddl.sql
        assert "INT64" in ddl.sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
