"""Tests for Faux Objects - Semantic View wrapper generation."""
import json
import os
import shutil
import tempfile
import pytest

from src.faux_objects.service import FauxObjectsService
from src.faux_objects.types import (
    FauxProject,
    FauxObjectType,
    SemanticColumnType,
    SemanticColumn,
    SemanticTable,
    SemanticRelationship,
    SemanticViewDefinition,
    FauxObjectConfig,
    ProcedureParameter,
    GeneratedScript,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


@pytest.fixture
def service(temp_dir):
    """Create a FauxObjectsService with a temp directory."""
    return FauxObjectsService(data_dir=temp_dir)


@pytest.fixture
def sample_project(service):
    """Create a fully configured sample project for testing."""
    project = service.create_project("Test P&L Wrappers", "Test project for P&L semantic view")

    # Define semantic view
    service.define_semantic_view(
        project.id, "pl_analysis", "FINANCE", "SEMANTIC",
        comment="Profit & Loss analysis",
        ai_sql_generation="Revenue is recognized at point of sale.",
    )

    # Add tables
    service.add_semantic_table(project.id, "gl_entries", "FINANCE.GL.FACT_JOURNAL_ENTRIES", "journal_entry_id")
    service.add_semantic_table(project.id, "accounts", "FINANCE.GL.DIM_ACCOUNT", "account_code")
    service.add_semantic_table(project.id, "periods", "FINANCE.GL.DIM_PERIOD", "period_id")

    # Add relationships
    service.add_semantic_relationship(project.id, "gl_entries", "account_code", "accounts")
    service.add_semantic_relationship(project.id, "gl_entries", "period_id", "periods")

    # Add dimensions
    service.add_semantic_column(project.id, "account_name", "dimension", "VARCHAR", "accounts", synonyms=["GL account"])
    service.add_semantic_column(project.id, "account_category", "dimension", "VARCHAR", "accounts")
    service.add_semantic_column(project.id, "fiscal_year", "dimension", "INT", "periods")
    service.add_semantic_column(project.id, "fiscal_quarter", "dimension", "VARCHAR", "periods")

    # Add facts
    service.add_semantic_column(project.id, "debit_amount", "fact", "FLOAT", "gl_entries")
    service.add_semantic_column(project.id, "credit_amount", "fact", "FLOAT", "gl_entries")

    # Add metrics
    service.add_semantic_column(
        project.id, "total_revenue", "metric", "FLOAT", "gl_entries",
        expression="SUM(CASE WHEN accounts.account_category = 'Revenue' THEN net_amount ELSE 0 END)",
        comment="Total revenue across all accounts",
    )
    service.add_semantic_column(
        project.id, "gross_profit", "metric", "FLOAT", "gl_entries",
        expression="SUM(CASE WHEN account_category = 'Revenue' THEN net_amount ELSE 0 END) - SUM(CASE WHEN account_category = 'COGS' THEN net_amount ELSE 0 END)",
        comment="Revenue minus COGS",
    )
    service.add_semantic_column(
        project.id, "net_income", "metric", "FLOAT", "gl_entries",
        expression="SUM(net_amount)",
        comment="Bottom line net income",
    )

    return service.get_project(project.id)


# =============================================================================
# Type Tests
# =============================================================================

class TestTypes:
    def test_semantic_column_qualified_name_with_alias(self):
        col = SemanticColumn(name="account_name", column_type=SemanticColumnType.DIMENSION, table_alias="accounts")
        assert col.qualified_name == "accounts.account_name"

    def test_semantic_column_qualified_name_without_alias(self):
        col = SemanticColumn(name="total_revenue", column_type=SemanticColumnType.METRIC)
        assert col.qualified_name == "total_revenue"

    def test_semantic_view_fully_qualified_name(self):
        sv = SemanticViewDefinition(name="pl_analysis", database="FINANCE", schema_name="SEMANTIC")
        assert sv.fully_qualified_name == "FINANCE.SEMANTIC.pl_analysis"

    def test_semantic_view_get_all_columns(self):
        sv = SemanticViewDefinition(
            name="test", database="DB", schema_name="SCH",
            dimensions=[SemanticColumn(name="d1", column_type=SemanticColumnType.DIMENSION)],
            metrics=[SemanticColumn(name="m1", column_type=SemanticColumnType.METRIC)],
            facts=[SemanticColumn(name="f1", column_type=SemanticColumnType.FACT)],
        )
        assert len(sv.get_all_columns()) == 3

    def test_faux_object_config_fully_qualified_name(self):
        config = FauxObjectConfig(
            name="V_PL", faux_type=FauxObjectType.VIEW,
            target_database="REPORTING", target_schema="PUBLIC",
        )
        assert config.fully_qualified_name == "REPORTING.PUBLIC.V_PL"

    def test_faux_object_type_enum(self):
        assert FauxObjectType.VIEW.value == "view"
        assert FauxObjectType.STORED_PROCEDURE.value == "stored_procedure"
        assert FauxObjectType.DYNAMIC_TABLE.value == "dynamic_table"
        assert FauxObjectType.TASK.value == "task"


# =============================================================================
# Project CRUD Tests
# =============================================================================

class TestProjectCRUD:
    def test_create_project(self, service):
        project = service.create_project("Test", "A test project")
        assert project.id is not None
        assert project.name == "Test"
        assert project.description == "A test project"
        assert project.created_at is not None

    def test_get_project(self, service):
        created = service.create_project("Test")
        fetched = service.get_project(created.id)
        assert fetched is not None
        assert fetched.id == created.id
        assert fetched.name == "Test"

    def test_get_nonexistent_project(self, service):
        assert service.get_project("nonexistent") is None

    def test_list_projects(self, service):
        service.create_project("Project A")
        service.create_project("Project B")
        projects = service.list_projects()
        assert len(projects) == 2
        names = {p["name"] for p in projects}
        assert names == {"Project A", "Project B"}

    def test_delete_project(self, service):
        project = service.create_project("To Delete")
        assert service.delete_project(project.id) is True
        assert service.get_project(project.id) is None

    def test_delete_nonexistent(self, service):
        assert service.delete_project("nonexistent") is False


# =============================================================================
# Semantic View Definition Tests
# =============================================================================

class TestSemanticViewDefinition:
    def test_define_semantic_view(self, service):
        project = service.create_project("Test")
        service.define_semantic_view(project.id, "my_view", "DB", "SCH", comment="Test view")
        updated = service.get_project(project.id)
        assert updated.semantic_view is not None
        assert updated.semantic_view.name == "my_view"
        assert updated.semantic_view.database == "DB"
        assert updated.semantic_view.schema_name == "SCH"
        assert updated.semantic_view.comment == "Test view"

    def test_add_tables(self, service):
        project = service.create_project("Test")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_table(project.id, "fact", "DB.SCH.FACT_TABLE", "id")
        service.add_semantic_table(project.id, "dim", "DB.SCH.DIM_TABLE", "code")
        updated = service.get_project(project.id)
        assert len(updated.semantic_view.tables) == 2
        assert updated.semantic_view.tables[0].alias == "fact"
        assert updated.semantic_view.tables[1].primary_key == "code"

    def test_add_columns(self, service):
        project = service.create_project("Test")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_column(project.id, "region", "dimension", "VARCHAR", "customers")
        service.add_semantic_column(project.id, "amount", "fact", "FLOAT", "orders")
        service.add_semantic_column(project.id, "total_sales", "metric", "FLOAT", "orders", expression="SUM(amount)")
        updated = service.get_project(project.id)
        assert len(updated.semantic_view.dimensions) == 1
        assert len(updated.semantic_view.facts) == 1
        assert len(updated.semantic_view.metrics) == 1
        assert updated.semantic_view.metrics[0].expression == "SUM(amount)"

    def test_add_relationships(self, service):
        project = service.create_project("Test")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_relationship(project.id, "orders", "customer_id", "customers")
        updated = service.get_project(project.id)
        assert len(updated.semantic_view.relationships) == 1
        assert updated.semantic_view.relationships[0].from_table == "orders"

    def test_define_without_project(self, service):
        with pytest.raises(ValueError):
            service.define_semantic_view("nonexistent", "v", "DB", "SCH")


# =============================================================================
# Faux Object Configuration Tests
# =============================================================================

class TestFauxObjectConfig:
    def test_add_view(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "V_PL_SUMMARY", "view",
            "REPORTING", "PUBLIC",
        )
        updated = service.get_project(sample_project.id)
        assert len(updated.faux_objects) == 1
        assert updated.faux_objects[0].faux_type == FauxObjectType.VIEW

    def test_add_stored_procedure(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "GET_PL_DATA", "stored_procedure",
            "REPORTING", "PUBLIC",
            parameters=[
                {"name": "FISCAL_YEAR", "data_type": "INT", "default_value": "2025"},
                {"name": "BUSINESS_UNIT", "data_type": "VARCHAR", "default_value": "NULL"},
            ],
            selected_dimensions=["account_name", "fiscal_year"],
            selected_metrics=["total_revenue", "net_income"],
        )
        updated = service.get_project(sample_project.id)
        obj = updated.faux_objects[0]
        assert obj.faux_type == FauxObjectType.STORED_PROCEDURE
        assert len(obj.parameters) == 2
        assert obj.parameters[0].name == "FISCAL_YEAR"
        assert len(obj.selected_dimensions) == 2

    def test_add_dynamic_table(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "DT_PL_SUMMARY", "dynamic_table",
            "REPORTING", "PUBLIC",
            warehouse="ANALYTICS_WH",
            target_lag="2 hours",
        )
        updated = service.get_project(sample_project.id)
        obj = updated.faux_objects[0]
        assert obj.faux_type == FauxObjectType.DYNAMIC_TABLE
        assert obj.warehouse == "ANALYTICS_WH"
        assert obj.target_lag == "2 hours"

    def test_add_task(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "PL_REPORT", "task",
            "REPORTING", "PUBLIC",
            warehouse="ANALYTICS_WH",
            schedule="USING CRON 0 6 * * * America/Chicago",
            materialized_table="REPORTING.PUBLIC.PL_REPORT_MAT",
        )
        updated = service.get_project(sample_project.id)
        obj = updated.faux_objects[0]
        assert obj.faux_type == FauxObjectType.TASK
        assert "CRON" in obj.schedule

    def test_defaults_to_all_columns(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "V_ALL", "view", "DB", "SCH",
        )
        updated = service.get_project(sample_project.id)
        obj = updated.faux_objects[0]
        assert len(obj.selected_dimensions) == 4  # all dimensions
        assert len(obj.selected_metrics) == 3  # all metrics

    def test_remove_faux_object(self, sample_project, service):
        service.add_faux_object(sample_project.id, "V_1", "view", "DB", "SCH")
        service.add_faux_object(sample_project.id, "V_2", "view", "DB", "SCH")
        service.remove_faux_object(sample_project.id, "V_1")
        updated = service.get_project(sample_project.id)
        assert len(updated.faux_objects) == 1
        assert updated.faux_objects[0].name == "V_2"


# =============================================================================
# SQL Generation Tests
# =============================================================================

class TestViewGeneration:
    def test_generate_view_sql(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "V_PL", "view", "REPORTING", "PUBLIC",
            comment="P&L summary view",
        )
        project = service.get_project(sample_project.id)
        sv = project.semantic_view
        config = project.faux_objects[0]
        sql = service.generate_view_sql(sv, config)

        assert "CREATE OR REPLACE VIEW REPORTING.PUBLIC.V_PL" in sql
        assert "SEMANTIC_VIEW(" in sql
        assert "FINANCE.SEMANTIC.pl_analysis" in sql
        assert "DIMENSIONS" in sql
        assert "METRICS" in sql
        assert "P&L summary view" in sql

    def test_view_with_where_clause(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "V_PL_2025", "view", "REPORTING", "PUBLIC",
            where_clause="periods.fiscal_year = 2025",
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])
        assert "WHERE periods.fiscal_year = 2025" in sql

    def test_view_with_selected_columns(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "V_REVENUE", "view", "REPORTING", "PUBLIC",
            selected_dimensions=["account_name", "fiscal_year"],
            selected_metrics=["total_revenue"],
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])
        assert "accounts.account_name" in sql
        assert "periods.fiscal_year" in sql
        assert "gl_entries.total_revenue" in sql
        # Should not include unselected columns
        assert "fiscal_quarter" not in sql
        assert "gross_profit" not in sql


class TestStoredProcedureGeneration:
    def test_generate_procedure_sql(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "GET_PL", "stored_procedure", "REPORTING", "PUBLIC",
            parameters=[
                {"name": "FISCAL_YEAR", "data_type": "INT", "default_value": "2025"},
            ],
            selected_dimensions=["account_name", "fiscal_year"],
            selected_metrics=["total_revenue", "net_income"],
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.PUBLIC.GET_PL" in sql
        assert "FISCAL_YEAR INT DEFAULT 2025" in sql
        assert "RETURNS TABLE(" in sql
        assert "LANGUAGE PYTHON" in sql
        assert "RUNTIME_VERSION = '3.11'" in sql
        assert "HANDLER = 'run'" in sql
        assert "SEMANTIC_VIEW(" in sql
        assert "def run(" in sql

    def test_procedure_returns_table_columns(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "GET_PL", "stored_procedure", "REPORTING", "PUBLIC",
            selected_dimensions=["account_name"],
            selected_metrics=["total_revenue"],
            selected_facts=["debit_amount"],
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])
        assert "ACCOUNT_NAME VARCHAR" in sql
        assert "TOTAL_REVENUE FLOAT" in sql
        assert "DEBIT_AMOUNT FLOAT" in sql

    def test_procedure_multiple_parameters(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "GET_PL", "stored_procedure", "REPORTING", "PUBLIC",
            parameters=[
                {"name": "FISCAL_YEAR", "data_type": "INT", "default_value": "2025"},
                {"name": "REGION", "data_type": "VARCHAR"},
            ],
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])
        assert "FISCAL_YEAR INT DEFAULT 2025" in sql
        assert "REGION VARCHAR" in sql


class TestDynamicTableGeneration:
    def test_generate_dynamic_table_sql(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "DT_PL", "dynamic_table", "REPORTING", "PUBLIC",
            warehouse="ANALYTICS_WH",
            target_lag="4 hours",
            comment="Auto-refreshing P&L summary",
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_dynamic_table_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE DYNAMIC TABLE REPORTING.PUBLIC.DT_PL" in sql
        assert "TARGET_LAG = '4 hours'" in sql
        assert "WAREHOUSE = ANALYTICS_WH" in sql
        assert "SEMANTIC_VIEW(" in sql
        assert "Auto-refreshing P&L summary" in sql

    def test_dynamic_table_defaults(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "DT_PL", "dynamic_table", "REPORTING", "PUBLIC",
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_dynamic_table_sql(project.semantic_view, project.faux_objects[0])
        assert "TARGET_LAG = '2 hours'" in sql
        assert "WAREHOUSE = COMPUTE_WH" in sql


class TestTaskGeneration:
    def test_generate_task_sql(self, sample_project, service):
        service.add_faux_object(
            sample_project.id, "PL_REPORT", "task", "REPORTING", "PUBLIC",
            warehouse="ANALYTICS_WH",
            schedule="USING CRON 0 6 * * * America/Chicago",
            materialized_table="REPORTING.PUBLIC.PL_REPORT_MAT",
        )
        project = service.get_project(sample_project.id)
        sql = service.generate_task_sql(project.semantic_view, project.faux_objects[0])

        # Materializer procedure
        assert "CREATE OR REPLACE PROCEDURE REPORTING.PUBLIC.PL_REPORT_MATERIALIZER" in sql
        assert "RETURNS TABLE(status VARCHAR, row_count INT, refreshed_at TIMESTAMP_NTZ)" in sql
        assert "save_as_table" in sql
        assert "'REPORTING.PUBLIC.PL_REPORT_MAT'" in sql

        # Task
        assert "CREATE OR REPLACE TASK REPORTING.PUBLIC.PL_REPORT_REFRESH" in sql
        assert "WAREHOUSE = ANALYTICS_WH" in sql
        assert "USING CRON 0 6 * * * America/Chicago" in sql
        assert "CALL REPORTING.PUBLIC.PL_REPORT_MATERIALIZER()" in sql

        # Resume
        assert "ALTER TASK REPORTING.PUBLIC.PL_REPORT_REFRESH RESUME" in sql


class TestSemanticViewDDL:
    def test_generate_ddl(self, sample_project, service):
        sv = sample_project.semantic_view
        ddl = service.generate_semantic_view_ddl(sv)

        assert "CREATE OR REPLACE SEMANTIC VIEW FINANCE.SEMANTIC.pl_analysis" in ddl
        assert "COMMENT = 'Profit & Loss analysis'" in ddl
        assert "AI_SQL_GENERATION" in ddl
        assert "TABLES (" in ddl
        assert "gl_entries AS FINANCE.GL.FACT_JOURNAL_ENTRIES" in ddl
        assert "RELATIONSHIPS (" in ddl
        assert "gl_entries (account_code) REFERENCES accounts" in ddl
        assert "DIMENSIONS (" in ddl
        assert "accounts.account_name" in ddl
        assert "METRICS (" in ddl
        assert "gl_entries.total_revenue" in ddl
        assert "FACTS (" in ddl


# =============================================================================
# Batch Generation Tests
# =============================================================================

class TestBatchGeneration:
    def test_generate_all_scripts(self, sample_project, service):
        service.add_faux_object(sample_project.id, "V_PL", "view", "RPT", "PUBLIC")
        service.add_faux_object(
            sample_project.id, "GET_PL", "stored_procedure", "RPT", "PUBLIC",
            parameters=[{"name": "YEAR", "data_type": "INT", "default_value": "2025"}],
        )
        service.add_faux_object(
            sample_project.id, "DT_PL", "dynamic_table", "RPT", "PUBLIC",
            warehouse="WH", target_lag="1 hour",
        )

        scripts = service.generate_all_scripts(sample_project.id)
        # Semantic view DDL + 3 faux objects
        assert len(scripts) == 4
        types = {s.object_type for s in scripts}
        assert FauxObjectType.VIEW in types
        assert FauxObjectType.STORED_PROCEDURE in types
        assert FauxObjectType.DYNAMIC_TABLE in types

    def test_generate_deployment_bundle(self, sample_project, service):
        service.add_faux_object(sample_project.id, "V_PL", "view", "RPT", "PUBLIC")
        bundle = service.generate_deployment_bundle(sample_project.id)

        assert "FAUX OBJECTS DEPLOYMENT BUNDLE" in bundle
        assert "Test P&L Wrappers" in bundle
        assert "CREATE OR REPLACE VIEW" in bundle
        assert "END OF DEPLOYMENT BUNDLE" in bundle

    def test_export_scripts(self, sample_project, service, temp_dir):
        service.add_faux_object(sample_project.id, "V_PL", "view", "RPT", "PUBLIC")
        output_dir = os.path.join(temp_dir, "exports")
        exported = service.export_scripts(sample_project.id, output_dir)

        assert len(exported) >= 2  # At least semantic view + 1 faux object + bundle
        assert "deployment_bundle" in exported
        assert os.path.exists(exported["deployment_bundle"])

        # Verify bundle file content
        with open(exported["deployment_bundle"], "r") as f:
            content = f.read()
        assert "FAUX OBJECTS DEPLOYMENT BUNDLE" in content


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEdgeCases:
    def test_empty_project_generation_fails(self, service):
        project = service.create_project("Empty")
        with pytest.raises(ValueError):
            service.generate_all_scripts(project.id)

    def test_view_no_metrics(self, service):
        project = service.create_project("Dims Only")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_column(project.id, "region", "dimension", "VARCHAR")
        service.add_semantic_column(project.id, "city", "dimension", "VARCHAR")
        service.add_faux_object(project.id, "V_GEO", "view", "RPT", "PUBLIC")

        updated = service.get_project(project.id)
        sql = service.generate_view_sql(updated.semantic_view, updated.faux_objects[0])
        assert "DIMENSIONS" in sql
        assert "METRICS" not in sql  # No metrics defined

    def test_procedure_no_parameters(self, service):
        project = service.create_project("No Params")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_column(project.id, "region", "dimension", "VARCHAR")
        service.add_semantic_column(project.id, "sales", "metric", "FLOAT", expression="SUM(amount)")
        service.add_faux_object(project.id, "GET_DATA", "stored_procedure", "RPT", "PUBLIC")

        updated = service.get_project(project.id)
        sql = service.generate_stored_procedure_sql(updated.semantic_view, updated.faux_objects[0])
        assert "CREATE OR REPLACE PROCEDURE" in sql
        assert "def run(session):" in sql

    def test_special_characters_in_comment(self, service):
        project = service.create_project("Special")
        service.define_semantic_view(project.id, "v", "DB", "SCH", comment="It's a test with 'quotes'")
        updated = service.get_project(project.id)
        ddl = service.generate_semantic_view_ddl(updated.semantic_view)
        assert "It''s a test with ''quotes''" in ddl

    def test_synonyms_in_ddl(self, service):
        project = service.create_project("Synonyms")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_column(
            project.id, "account_name", "dimension", "VARCHAR", "accts",
            synonyms=["GL account", "ledger account"],
        )
        updated = service.get_project(project.id)
        ddl = service.generate_semantic_view_ddl(updated.semantic_view)
        assert "WITH SYNONYMS" in ddl
        assert "'GL account'" in ddl
        assert "'ledger account'" in ddl

    def test_project_persistence(self, service):
        """Verify projects survive save/load cycle."""
        project = service.create_project("Persist")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_column(project.id, "col1", "dimension", "VARCHAR")
        service.add_faux_object(project.id, "V_1", "view", "RPT", "PUBLIC")

        # Load fresh
        loaded = service.get_project(project.id)
        assert loaded.name == "Persist"
        assert loaded.semantic_view.name == "v"
        assert len(loaded.semantic_view.dimensions) == 1
        assert len(loaded.faux_objects) == 1
