"""Persona-based tests for Faux Objects - Semantic View wrapper generation.

Each test class represents a real-world persona (one per DataBridge skill)
that builds a realistic semantic view and generates faux objects from it.

Personas:
1. Financial Analyst     - GL Reconciliation semantic view
2. FP&A Oil & Gas        - Drilling Economics semantic view
3. Operations Analyst    - Geographic Operations semantic view
4. FP&A Cost Analyst     - Cost Allocation semantic view
5. Manufacturing Analyst - Plant Operations semantic view
6. SaaS Metrics Analyst  - Subscription Metrics semantic view
7. Transportation Analyst - Fleet Operations semantic view
8. Cross-Persona Integration Tests
"""
import os
import shutil
import tempfile
import pytest

from src.faux_objects.service import FauxObjectsService
from src.faux_objects.types import FauxObjectType


# =============================================================================
# Shared Fixtures
# =============================================================================


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


# =============================================================================
# Persona 1: Financial Analyst - GL Reconciliation
# =============================================================================


@pytest.fixture
def financial_project(service):
    """Financial Analyst: GL Reconciliation semantic view."""
    project = service.create_project(
        "GL Reconciliation Wrappers",
        "Trial balance and GL reconciliation views for Finance team",
    )
    service.define_semantic_view(
        project.id, "gl_reconciliation", "FINANCE", "SEMANTIC",
        comment="General Ledger reconciliation for trial balance reporting",
        ai_sql_generation="Debits are positive, credits are negative. Net balance = debits - credits.",
    )

    # Tables
    service.add_semantic_table(project.id, "journal_entries", "FINANCE.GL.FACT_JOURNAL_ENTRIES", "journal_entry_id")
    service.add_semantic_table(project.id, "accounts", "FINANCE.GL.DIM_ACCOUNT", "account_code")
    service.add_semantic_table(project.id, "periods", "FINANCE.GL.DIM_PERIOD", "period_id")
    service.add_semantic_table(project.id, "cost_centers", "FINANCE.GL.DIM_COST_CENTER", "cost_center_id")

    # Relationships
    service.add_semantic_relationship(project.id, "journal_entries", "account_code", "accounts")
    service.add_semantic_relationship(project.id, "journal_entries", "period_id", "periods")
    service.add_semantic_relationship(project.id, "journal_entries", "cost_center_id", "cost_centers")

    # Dimensions
    service.add_semantic_column(project.id, "account_name", "dimension", "VARCHAR", "accounts",
                                synonyms=["GL account", "ledger account"])
    service.add_semantic_column(project.id, "account_category", "dimension", "VARCHAR", "accounts")
    service.add_semantic_column(project.id, "fiscal_year", "dimension", "INT", "periods")
    service.add_semantic_column(project.id, "fiscal_quarter", "dimension", "VARCHAR", "periods")
    service.add_semantic_column(project.id, "cost_center", "dimension", "VARCHAR", "cost_centers")

    # Facts
    service.add_semantic_column(project.id, "debit_amount", "fact", "FLOAT", "journal_entries")
    service.add_semantic_column(project.id, "credit_amount", "fact", "FLOAT", "journal_entries")

    # Metrics
    service.add_semantic_column(
        project.id, "total_debits", "metric", "FLOAT", "journal_entries",
        expression="SUM(debit_amount)",
        comment="Sum of all debit entries",
    )
    service.add_semantic_column(
        project.id, "total_credits", "metric", "FLOAT", "journal_entries",
        expression="SUM(credit_amount)",
        comment="Sum of all credit entries",
    )
    service.add_semantic_column(
        project.id, "net_balance", "metric", "FLOAT", "journal_entries",
        expression="SUM(debit_amount) - SUM(credit_amount)",
        comment="Net balance (debits minus credits)",
    )
    service.add_semantic_column(
        project.id, "variance_amount", "metric", "FLOAT", "journal_entries",
        expression="ABS(SUM(debit_amount) - SUM(credit_amount))",
        comment="Absolute variance for reconciliation",
    )

    return service.get_project(project.id)


class TestFinancialAnalyst:
    """Persona 1: Financial Analyst - GL Reconciliation."""

    def test_trial_balance_view(self, financial_project, service):
        """Generate V_TRIAL_BALANCE view with all dimensions and metrics."""
        service.add_faux_object(
            financial_project.id, "V_TRIAL_BALANCE", "view",
            "REPORTING", "FINANCE",
            comment="Trial balance summary view",
        )
        project = service.get_project(financial_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE VIEW REPORTING.FINANCE.V_TRIAL_BALANCE" in sql
        assert "SEMANTIC_VIEW(" in sql
        assert "FINANCE.SEMANTIC.gl_reconciliation" in sql
        assert "DIMENSIONS" in sql
        assert "METRICS" in sql
        assert "accounts.account_name" in sql
        assert "journal_entries.total_debits" in sql

    def test_reconcile_period_procedure(self, financial_project, service):
        """Generate RECONCILE_PERIOD stored procedure with 3 parameters."""
        service.add_faux_object(
            financial_project.id, "RECONCILE_PERIOD", "stored_procedure",
            "REPORTING", "FINANCE",
            parameters=[
                {"name": "FISCAL_YEAR", "data_type": "INT", "default_value": "2025"},
                {"name": "FISCAL_QUARTER", "data_type": "VARCHAR", "default_value": "NULL"},
                {"name": "COST_CENTER", "data_type": "VARCHAR", "default_value": "NULL"},
            ],
            selected_dimensions=["account_name", "account_category", "fiscal_year", "fiscal_quarter", "cost_center"],
            selected_metrics=["total_debits", "total_credits", "net_balance"],
        )
        project = service.get_project(financial_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.FINANCE.RECONCILE_PERIOD" in sql
        assert "FISCAL_YEAR INT DEFAULT 2025" in sql
        assert "FISCAL_QUARTER VARCHAR" in sql
        assert "COST_CENTER VARCHAR" in sql
        assert "RETURNS TABLE(" in sql
        assert "LANGUAGE PYTHON" in sql
        assert "HANDLER = 'run'" in sql

    def test_filtered_view_2025(self, financial_project, service):
        """Generate V_TB_2025 view filtered to fiscal year 2025."""
        service.add_faux_object(
            financial_project.id, "V_TB_2025", "view",
            "REPORTING", "FINANCE",
            where_clause="periods.fiscal_year = 2025",
            comment="Trial balance for fiscal year 2025",
        )
        project = service.get_project(financial_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE VIEW REPORTING.FINANCE.V_TB_2025" in sql
        assert "WHERE periods.fiscal_year = 2025" in sql

    def test_subset_columns_view(self, financial_project, service):
        """Generate a view with only selected dimensions and metrics."""
        service.add_faux_object(
            financial_project.id, "V_TB_SUMMARY", "view",
            "REPORTING", "FINANCE",
            selected_dimensions=["account_category", "fiscal_year"],
            selected_metrics=["net_balance"],
            selected_facts=[],
        )
        project = service.get_project(financial_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "accounts.account_category" in sql
        assert "periods.fiscal_year" in sql
        assert "journal_entries.net_balance" in sql
        # Should NOT include unselected columns
        assert "account_name" not in sql
        assert "total_debits" not in sql

    def test_ddl_with_synonyms(self, financial_project, service):
        """Verify semantic view DDL includes synonyms for account_name."""
        ddl = service.generate_semantic_view_ddl(financial_project.semantic_view)

        assert "CREATE OR REPLACE SEMANTIC VIEW FINANCE.SEMANTIC.gl_reconciliation" in ddl
        assert "WITH SYNONYMS" in ddl
        assert "'GL account'" in ddl
        assert "'ledger account'" in ddl
        assert "TABLES (" in ddl
        assert "journal_entries AS FINANCE.GL.FACT_JOURNAL_ENTRIES" in ddl


# =============================================================================
# Persona 2: FP&A Oil & Gas Analyst - Drilling Economics
# =============================================================================


@pytest.fixture
def oil_gas_project(service):
    """FP&A Oil & Gas Analyst: Drilling Economics semantic view."""
    project = service.create_project(
        "Drilling Economics Wrappers",
        "Well-level economics for upstream oil & gas operations",
    )
    service.define_semantic_view(
        project.id, "drilling_economics", "PRODUCTION", "SEMANTIC",
        comment="Drilling economics for upstream E&P operations",
        ai_sql_generation="BOE = barrels of oil equivalent. LOE = lease operating expense. NPV uses 10% discount rate.",
    )

    # Tables
    service.add_semantic_table(project.id, "well_economics", "PRODUCTION.DW.FACT_WELL_ECONOMICS", "well_economics_id")
    service.add_semantic_table(project.id, "wells", "PRODUCTION.DW.DIM_WELL", "well_id")
    service.add_semantic_table(project.id, "basins", "PRODUCTION.DW.DIM_BASIN", "basin_id")
    service.add_semantic_table(project.id, "periods", "PRODUCTION.DW.DIM_PERIOD", "period_id")

    # Relationships
    service.add_semantic_relationship(project.id, "well_economics", "well_id", "wells")
    service.add_semantic_relationship(project.id, "well_economics", "basin_id", "basins")
    service.add_semantic_relationship(project.id, "well_economics", "period_id", "periods")

    # Dimensions
    service.add_semantic_column(project.id, "well_name", "dimension", "VARCHAR", "wells",
                                synonyms=["well", "wellbore"])
    service.add_semantic_column(project.id, "basin_name", "dimension", "VARCHAR", "basins",
                                synonyms=["basin", "play"])
    service.add_semantic_column(project.id, "formation", "dimension", "VARCHAR", "wells")
    service.add_semantic_column(project.id, "operator", "dimension", "VARCHAR", "wells")
    service.add_semantic_column(project.id, "fiscal_year", "dimension", "INT", "periods")

    # Facts
    service.add_semantic_column(project.id, "production_boe", "fact", "FLOAT", "well_economics")
    service.add_semantic_column(project.id, "oil_revenue", "fact", "FLOAT", "well_economics")
    service.add_semantic_column(project.id, "gas_revenue", "fact", "FLOAT", "well_economics")
    service.add_semantic_column(project.id, "loe_cost", "fact", "FLOAT", "well_economics")

    # Metrics
    service.add_semantic_column(
        project.id, "total_revenue", "metric", "FLOAT", "well_economics",
        expression="SUM(oil_revenue + gas_revenue)",
        comment="Total oil + gas revenue",
    )
    service.add_semantic_column(
        project.id, "loe_per_boe", "metric", "FLOAT", "well_economics",
        expression="SUM(loe_cost) / NULLIF(SUM(production_boe), 0)",
        comment="Lease operating expense per BOE",
    )
    service.add_semantic_column(
        project.id, "npv", "metric", "FLOAT", "well_economics",
        expression="SUM(oil_revenue + gas_revenue - loe_cost) / POWER(1.10, periods.fiscal_year - 2024)",
        comment="Net present value at 10% discount rate",
    )
    service.add_semantic_column(
        project.id, "irr", "metric", "FLOAT", "well_economics",
        expression="SUM(oil_revenue + gas_revenue - loe_cost) / NULLIF(SUM(loe_cost), 0)",
        comment="Internal rate of return proxy",
    )
    service.add_semantic_column(
        project.id, "eur", "metric", "FLOAT", "well_economics",
        expression="SUM(production_boe) * 1.15",
        comment="Estimated ultimate recovery (BOE)",
    )

    return service.get_project(project.id)


class TestOilGasAnalyst:
    """Persona 2: FP&A Oil & Gas Analyst - Drilling Economics."""

    def test_well_economics_procedure(self, oil_gas_project, service):
        """Generate GET_WELL_ECONOMICS stored procedure with well/basin params."""
        service.add_faux_object(
            oil_gas_project.id, "GET_WELL_ECONOMICS", "stored_procedure",
            "REPORTING", "PRODUCTION",
            parameters=[
                {"name": "WELL_ID", "data_type": "VARCHAR", "default_value": "NULL"},
                {"name": "BASIN", "data_type": "VARCHAR", "default_value": "NULL"},
            ],
            selected_dimensions=["well_name", "basin_name", "formation", "fiscal_year"],
            selected_facts=["production_boe", "oil_revenue", "gas_revenue", "loe_cost"],
            selected_metrics=["total_revenue", "loe_per_boe", "npv"],
        )
        project = service.get_project(oil_gas_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.PRODUCTION.GET_WELL_ECONOMICS" in sql
        assert "WELL_ID VARCHAR" in sql
        assert "BASIN VARCHAR" in sql
        assert "RETURNS TABLE(" in sql
        assert "PRODUCTION_BOE FLOAT" in sql
        assert "TOTAL_REVENUE FLOAT" in sql

    def test_basin_dashboard_dynamic_table(self, oil_gas_project, service):
        """Generate DT_BASIN_DASHBOARD dynamic table with 4-hour refresh."""
        service.add_faux_object(
            oil_gas_project.id, "DT_BASIN_DASHBOARD", "dynamic_table",
            "REPORTING", "PRODUCTION",
            warehouse="ANALYTICS_WH",
            target_lag="4 hours",
            selected_dimensions=["basin_name", "fiscal_year"],
            selected_metrics=["total_revenue", "loe_per_boe", "npv", "eur"],
            comment="Auto-refreshing basin-level economics dashboard",
        )
        project = service.get_project(oil_gas_project.id)
        sql = service.generate_dynamic_table_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE DYNAMIC TABLE REPORTING.PRODUCTION.DT_BASIN_DASHBOARD" in sql
        assert "TARGET_LAG = '4 hours'" in sql
        assert "WAREHOUSE = ANALYTICS_WH" in sql
        assert "basins.basin_name" in sql
        assert "SEMANTIC_VIEW(" in sql

    def test_permian_filter(self, oil_gas_project, service):
        """Generate a view filtered to Permian Basin only."""
        service.add_faux_object(
            oil_gas_project.id, "V_PERMIAN_ECONOMICS", "view",
            "REPORTING", "PRODUCTION",
            where_clause="basins.basin_name = 'Permian'",
            selected_dimensions=["well_name", "formation", "fiscal_year"],
            selected_metrics=["total_revenue", "loe_per_boe"],
        )
        project = service.get_project(oil_gas_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "WHERE basins.basin_name = 'Permian'" in sql
        assert "wells.well_name" in sql

    def test_default_facts_included(self, oil_gas_project, service):
        """When no facts specified, all facts should be included by default."""
        service.add_faux_object(
            oil_gas_project.id, "V_ALL_DATA", "view",
            "REPORTING", "PRODUCTION",
        )
        project = service.get_project(oil_gas_project.id)
        obj = project.faux_objects[0]

        assert "production_boe" in obj.selected_facts
        assert "oil_revenue" in obj.selected_facts
        assert "gas_revenue" in obj.selected_facts
        assert "loe_cost" in obj.selected_facts
        assert len(obj.selected_dimensions) == 5
        assert len(obj.selected_metrics) == 5

    def test_deployment_bundle(self, oil_gas_project, service):
        """Generate complete deployment bundle with procedure + dynamic table."""
        service.add_faux_object(
            oil_gas_project.id, "GET_WELL_ECONOMICS", "stored_procedure",
            "REPORTING", "PRODUCTION",
            parameters=[{"name": "BASIN", "data_type": "VARCHAR"}],
        )
        service.add_faux_object(
            oil_gas_project.id, "DT_BASIN_DASHBOARD", "dynamic_table",
            "REPORTING", "PRODUCTION",
            warehouse="ANALYTICS_WH", target_lag="4 hours",
        )
        bundle = service.generate_deployment_bundle(oil_gas_project.id)

        assert "FAUX OBJECTS DEPLOYMENT BUNDLE" in bundle
        assert "Drilling Economics Wrappers" in bundle
        assert "CREATE OR REPLACE PROCEDURE" in bundle
        assert "CREATE OR REPLACE DYNAMIC TABLE" in bundle
        assert "END OF DEPLOYMENT BUNDLE" in bundle


# =============================================================================
# Persona 3: Operations Analyst - Geographic Operations
# =============================================================================


@pytest.fixture
def operations_project(service):
    """Operations Analyst: Geographic Operations semantic view."""
    project = service.create_project(
        "Geographic Operations Wrappers",
        "Regional operations metrics for executive dashboards",
    )
    service.define_semantic_view(
        project.id, "geo_operations", "ENTERPRISE", "SEMANTIC",
        comment="Geographic operations for regional performance tracking",
    )

    # Tables
    service.add_semantic_table(project.id, "operations", "ENTERPRISE.DW.FACT_OPERATIONS", "operation_id")
    service.add_semantic_table(project.id, "locations", "ENTERPRISE.DW.DIM_LOCATION", "location_id")
    service.add_semantic_table(project.id, "departments", "ENTERPRISE.DW.DIM_DEPARTMENT", "department_id")
    service.add_semantic_table(project.id, "assets", "ENTERPRISE.DW.DIM_ASSET", "asset_id")

    # Relationships
    service.add_semantic_relationship(project.id, "operations", "location_id", "locations")
    service.add_semantic_relationship(project.id, "operations", "department_id", "departments")
    service.add_semantic_relationship(project.id, "operations", "asset_id", "assets")

    # Dimensions
    service.add_semantic_column(project.id, "region", "dimension", "VARCHAR", "locations",
                                synonyms=["geographic region"])
    service.add_semantic_column(project.id, "country", "dimension", "VARCHAR", "locations")
    service.add_semantic_column(project.id, "city", "dimension", "VARCHAR", "locations")
    service.add_semantic_column(project.id, "department", "dimension", "VARCHAR", "departments")
    service.add_semantic_column(project.id, "asset_class", "dimension", "VARCHAR", "assets")

    # Facts
    service.add_semantic_column(project.id, "headcount", "fact", "INT", "operations")
    service.add_semantic_column(project.id, "square_footage", "fact", "FLOAT", "operations")

    # Metrics
    service.add_semantic_column(
        project.id, "total_headcount", "metric", "INT", "operations",
        expression="SUM(headcount)",
        comment="Total employee headcount",
    )
    service.add_semantic_column(
        project.id, "total_sqft", "metric", "FLOAT", "operations",
        expression="SUM(square_footage)",
        comment="Total occupied square footage",
    )
    service.add_semantic_column(
        project.id, "utilization_rate", "metric", "FLOAT", "operations",
        expression="SUM(headcount) / NULLIF(SUM(square_footage) / 150, 0)",
        comment="Space utilization rate (150 sqft per person standard)",
    )

    return service.get_project(project.id)


class TestOperationsAnalyst:
    """Persona 3: Operations Analyst - Geographic Operations."""

    def test_regional_summary_view(self, operations_project, service):
        """Generate V_REGIONAL_SUMMARY view with all dimensions and metrics."""
        service.add_faux_object(
            operations_project.id, "V_REGIONAL_SUMMARY", "view",
            "REPORTING", "OPS",
            comment="Regional operations summary for executive dashboard",
        )
        project = service.get_project(operations_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE VIEW REPORTING.OPS.V_REGIONAL_SUMMARY" in sql
        assert "ENTERPRISE.SEMANTIC.geo_operations" in sql
        assert "locations.region" in sql
        assert "departments.department" in sql
        assert "METRICS" in sql

    def test_weekly_ops_report_task(self, operations_project, service):
        """Generate WEEKLY_OPS_REPORT task with Monday 8am CRON."""
        service.add_faux_object(
            operations_project.id, "WEEKLY_OPS_REPORT", "task",
            "REPORTING", "OPS",
            warehouse="ANALYTICS_WH",
            schedule="USING CRON 0 8 * * 1 America/Chicago",
            materialized_table="REPORTING.OPS.WEEKLY_OPS_REPORT_MAT",
            comment="Weekly operations report materialized every Monday at 8am",
        )
        project = service.get_project(operations_project.id)
        sql = service.generate_task_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.OPS.WEEKLY_OPS_REPORT_MATERIALIZER" in sql
        assert "CREATE OR REPLACE TASK REPORTING.OPS.WEEKLY_OPS_REPORT_REFRESH" in sql
        assert "USING CRON 0 8 * * 1 America/Chicago" in sql
        assert "WAREHOUSE = ANALYTICS_WH" in sql
        assert "save_as_table" in sql
        assert "'REPORTING.OPS.WEEKLY_OPS_REPORT_MAT'" in sql
        assert "ALTER TASK" in sql
        assert "RESUME" in sql

    def test_region_filter_view(self, operations_project, service):
        """Generate view filtered to North America region."""
        service.add_faux_object(
            operations_project.id, "V_NORTH_AMERICA", "view",
            "REPORTING", "OPS",
            where_clause="locations.region = 'North America'",
            selected_dimensions=["country", "city", "department"],
            selected_metrics=["total_headcount", "utilization_rate"],
        )
        project = service.get_project(operations_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "WHERE locations.region = 'North America'" in sql
        assert "locations.country" in sql
        assert "locations.city" in sql

    def test_dimensions_only_view(self, service):
        """Generate view with dimensions only, no metrics defined."""
        # Create a project with NO metrics or facts defined at all
        project = service.create_project("Dims Only Ops")
        service.define_semantic_view(project.id, "geo_dims", "ENTERPRISE", "SEMANTIC")
        service.add_semantic_column(project.id, "region", "dimension", "VARCHAR", "locations")
        service.add_semantic_column(project.id, "country", "dimension", "VARCHAR", "locations")
        service.add_semantic_column(project.id, "city", "dimension", "VARCHAR", "locations")

        service.add_faux_object(
            project.id, "V_LOCATION_LIST", "view",
            "REPORTING", "OPS",
        )
        updated = service.get_project(project.id)
        sql = service.generate_view_sql(updated.semantic_view, updated.faux_objects[0])

        assert "DIMENSIONS" in sql
        assert "locations.region" in sql
        assert "METRICS" not in sql

    def test_default_task_schedule(self, operations_project, service):
        """When no schedule specified, task should get a default schedule."""
        service.add_faux_object(
            operations_project.id, "DEFAULT_TASK", "task",
            "REPORTING", "OPS",
        )
        project = service.get_project(operations_project.id)
        sql = service.generate_task_sql(project.semantic_view, project.faux_objects[0])

        # Default schedule should be applied
        assert "CRON" in sql
        assert "CREATE OR REPLACE TASK" in sql


# =============================================================================
# Persona 4: FP&A Cost Analyst - Cost Allocation
# =============================================================================


@pytest.fixture
def cost_project(service):
    """FP&A Cost Analyst: Cost Allocation semantic view."""
    project = service.create_project(
        "Cost Allocation Wrappers",
        "Budget vs actual analysis and cost allocation reporting",
    )
    service.define_semantic_view(
        project.id, "cost_allocation", "FINANCE", "SEMANTIC",
        comment="Cost allocation and budget variance analysis",
        ai_sql_generation="Variance = Budget - Actual. Positive variance means under budget.",
    )

    # Tables
    service.add_semantic_table(project.id, "gl_transactions", "FINANCE.GL.FACT_GL_TRANSACTIONS", "transaction_id")
    service.add_semantic_table(project.id, "cost_centers", "FINANCE.GL.DIM_COST_CENTER", "cost_center_id")
    service.add_semantic_table(project.id, "periods", "FINANCE.GL.DIM_PERIOD", "period_id")
    service.add_semantic_table(project.id, "allocations", "FINANCE.GL.DIM_ALLOCATION", "allocation_id")

    # Relationships
    service.add_semantic_relationship(project.id, "gl_transactions", "cost_center_id", "cost_centers")
    service.add_semantic_relationship(project.id, "gl_transactions", "period_id", "periods")
    service.add_semantic_relationship(project.id, "gl_transactions", "allocation_id", "allocations")

    # Dimensions
    service.add_semantic_column(project.id, "cost_center", "dimension", "VARCHAR", "cost_centers")
    service.add_semantic_column(project.id, "department", "dimension", "VARCHAR", "cost_centers")
    service.add_semantic_column(project.id, "allocation_method", "dimension", "VARCHAR", "allocations",
                                synonyms=["alloc method", "distribution method"])
    service.add_semantic_column(project.id, "fiscal_period", "dimension", "VARCHAR", "periods")

    # Facts
    service.add_semantic_column(project.id, "transaction_amount", "fact", "FLOAT", "gl_transactions")

    # Metrics
    service.add_semantic_column(
        project.id, "budget_amount", "metric", "FLOAT", "gl_transactions",
        expression="SUM(CASE WHEN transaction_type = 'BUDGET' THEN transaction_amount ELSE 0 END)",
        comment="Total budgeted amount",
    )
    service.add_semantic_column(
        project.id, "actual_amount", "metric", "FLOAT", "gl_transactions",
        expression="SUM(CASE WHEN transaction_type = 'ACTUAL' THEN transaction_amount ELSE 0 END)",
        comment="Total actual amount",
    )
    service.add_semantic_column(
        project.id, "variance", "metric", "FLOAT", "gl_transactions",
        expression="SUM(CASE WHEN transaction_type = 'BUDGET' THEN transaction_amount ELSE 0 END) - SUM(CASE WHEN transaction_type = 'ACTUAL' THEN transaction_amount ELSE 0 END)",
        comment="Budget minus actual variance",
    )
    service.add_semantic_column(
        project.id, "allocation_total", "metric", "FLOAT", "gl_transactions",
        expression="SUM(transaction_amount)",
        comment="Total allocated amount",
    )

    return service.get_project(project.id)


class TestCostAnalyst:
    """Persona 4: FP&A Cost Analyst - Cost Allocation."""

    def test_budget_vs_actual_procedure(self, cost_project, service):
        """Generate GET_BUDGET_VS_ACTUAL procedure with cost_center/period params."""
        service.add_faux_object(
            cost_project.id, "GET_BUDGET_VS_ACTUAL", "stored_procedure",
            "REPORTING", "FINANCE",
            parameters=[
                {"name": "COST_CENTER", "data_type": "VARCHAR", "default_value": "NULL"},
                {"name": "FISCAL_PERIOD", "data_type": "VARCHAR", "default_value": "NULL"},
            ],
            selected_dimensions=["cost_center", "department", "fiscal_period"],
            selected_metrics=["budget_amount", "actual_amount", "variance"],
        )
        project = service.get_project(cost_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.FINANCE.GET_BUDGET_VS_ACTUAL" in sql
        assert "COST_CENTER VARCHAR" in sql
        assert "FISCAL_PERIOD VARCHAR" in sql
        assert "BUDGET_AMOUNT FLOAT" in sql
        assert "VARIANCE FLOAT" in sql

    def test_allocation_summary_view(self, cost_project, service):
        """Generate V_ALLOCATION_SUMMARY view focused on allocation methods."""
        service.add_faux_object(
            cost_project.id, "V_ALLOCATION_SUMMARY", "view",
            "REPORTING", "FINANCE",
            selected_dimensions=["allocation_method", "cost_center", "fiscal_period"],
            selected_metrics=["allocation_total", "actual_amount"],
            comment="Allocation method summary view",
        )
        project = service.get_project(cost_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE VIEW REPORTING.FINANCE.V_ALLOCATION_SUMMARY" in sql
        assert "allocations.allocation_method" in sql
        assert "gl_transactions.allocation_total" in sql

    def test_procedure_with_static_where(self, cost_project, service):
        """Generate procedure with both parameters and a static WHERE clause."""
        service.add_faux_object(
            cost_project.id, "GET_OPEX_ONLY", "stored_procedure",
            "REPORTING", "FINANCE",
            parameters=[
                {"name": "DEPARTMENT", "data_type": "VARCHAR", "default_value": "NULL"},
            ],
            where_clause="cost_centers.cost_center LIKE 'OPEX%'",
            selected_dimensions=["cost_center", "department"],
            selected_metrics=["budget_amount", "actual_amount", "variance"],
        )
        project = service.get_project(cost_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.FINANCE.GET_OPEX_ONLY" in sql
        assert "OPEX" in sql
        assert "DEPARTMENT VARCHAR" in sql

    def test_default_all_columns(self, cost_project, service):
        """When no columns specified, all should be included."""
        service.add_faux_object(
            cost_project.id, "V_ALL", "view",
            "REPORTING", "FINANCE",
        )
        project = service.get_project(cost_project.id)
        obj = project.faux_objects[0]

        assert len(obj.selected_dimensions) == 4
        assert len(obj.selected_metrics) == 4
        assert len(obj.selected_facts) == 1
        assert "transaction_amount" in obj.selected_facts

    def test_case_when_persistence(self, cost_project, service):
        """Verify CASE WHEN expressions survive save/load cycle."""
        loaded = service.get_project(cost_project.id)
        variance_metric = next(m for m in loaded.semantic_view.metrics if m.name == "variance")

        assert "CASE WHEN" in variance_metric.expression
        assert "BUDGET" in variance_metric.expression
        assert "ACTUAL" in variance_metric.expression


# =============================================================================
# Persona 5: Manufacturing Analyst - Plant Operations
# =============================================================================


@pytest.fixture
def manufacturing_project(service):
    """Manufacturing Analyst: Plant Operations semantic view."""
    project = service.create_project(
        "Plant Operations Wrappers",
        "Manufacturing plant performance and variance analysis",
    )
    service.define_semantic_view(
        project.id, "plant_operations", "MANUFACTURING", "SEMANTIC",
        comment="Plant operations for production and variance tracking",
    )

    # Tables
    service.add_semantic_table(project.id, "production", "MANUFACTURING.DW.FACT_PRODUCTION", "production_id")
    service.add_semantic_table(project.id, "plants", "MANUFACTURING.DW.DIM_PLANT", "plant_id")
    service.add_semantic_table(project.id, "products", "MANUFACTURING.DW.DIM_PRODUCT", "product_id")
    service.add_semantic_table(project.id, "periods", "MANUFACTURING.DW.DIM_PERIOD", "period_id")

    # Relationships
    service.add_semantic_relationship(project.id, "production", "plant_id", "plants")
    service.add_semantic_relationship(project.id, "production", "product_id", "products")
    service.add_semantic_relationship(project.id, "production", "period_id", "periods")

    # Dimensions
    service.add_semantic_column(project.id, "plant_name", "dimension", "VARCHAR", "plants")
    service.add_semantic_column(project.id, "product_line", "dimension", "VARCHAR", "products")
    service.add_semantic_column(project.id, "region", "dimension", "VARCHAR", "plants")
    service.add_semantic_column(project.id, "fiscal_month", "dimension", "VARCHAR", "periods")

    # Facts - note INT type for units
    service.add_semantic_column(project.id, "units_produced", "fact", "INT", "production")
    service.add_semantic_column(project.id, "labor_hours", "fact", "FLOAT", "production")
    service.add_semantic_column(project.id, "material_cost", "fact", "FLOAT", "production")

    # Metrics
    service.add_semantic_column(
        project.id, "total_output", "metric", "INT", "production",
        expression="SUM(units_produced)",
        comment="Total units produced",
    )
    service.add_semantic_column(
        project.id, "standard_cost", "metric", "FLOAT", "production",
        expression="SUM(units_produced) * 12.50",
        comment="Standard cost at $12.50 per unit",
    )
    service.add_semantic_column(
        project.id, "actual_cost", "metric", "FLOAT", "production",
        expression="SUM(labor_hours * 25.00 + material_cost)",
        comment="Actual cost (labor at $25/hr + materials)",
    )
    service.add_semantic_column(
        project.id, "volume_variance", "metric", "FLOAT", "production",
        expression="(SUM(units_produced) - 1000) * 12.50",
        comment="Volume variance vs 1000 unit standard",
    )
    service.add_semantic_column(
        project.id, "efficiency_variance", "metric", "FLOAT", "production",
        expression="SUM(labor_hours * 25.00 + material_cost) - SUM(units_produced) * 12.50",
        comment="Efficiency variance (actual - standard)",
    )

    return service.get_project(project.id)


class TestManufacturingAnalyst:
    """Persona 5: Manufacturing Analyst - Plant Operations."""

    def test_plant_dashboard_dynamic_table(self, manufacturing_project, service):
        """Generate DT_PLANT_DASHBOARD dynamic table with 1-hour refresh."""
        service.add_faux_object(
            manufacturing_project.id, "DT_PLANT_DASHBOARD", "dynamic_table",
            "REPORTING", "MFG",
            warehouse="MFG_WH",
            target_lag="1 hour",
            comment="Auto-refreshing plant performance dashboard",
        )
        project = service.get_project(manufacturing_project.id)
        sql = service.generate_dynamic_table_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE DYNAMIC TABLE REPORTING.MFG.DT_PLANT_DASHBOARD" in sql
        assert "TARGET_LAG = '1 hour'" in sql
        assert "WAREHOUSE = MFG_WH" in sql
        assert "MANUFACTURING.SEMANTIC.plant_operations" in sql

    def test_variance_summary_view(self, manufacturing_project, service):
        """Generate V_VARIANCE_SUMMARY view with only variance metrics."""
        service.add_faux_object(
            manufacturing_project.id, "V_VARIANCE_SUMMARY", "view",
            "REPORTING", "MFG",
            selected_dimensions=["plant_name", "product_line", "fiscal_month"],
            selected_metrics=["standard_cost", "actual_cost", "volume_variance", "efficiency_variance"],
            selected_facts=[],
            comment="Manufacturing variance analysis view",
        )
        project = service.get_project(manufacturing_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE VIEW REPORTING.MFG.V_VARIANCE_SUMMARY" in sql
        assert "plants.plant_name" in sql
        assert "production.volume_variance" in sql
        assert "production.efficiency_variance" in sql

    def test_plant_filter(self, manufacturing_project, service):
        """Generate view filtered to a specific plant."""
        service.add_faux_object(
            manufacturing_project.id, "V_PLANT_A_OPS", "view",
            "REPORTING", "MFG",
            where_clause="plants.plant_name = 'Plant A - Houston'",
        )
        project = service.get_project(manufacturing_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "WHERE plants.plant_name = 'Plant A - Houston'" in sql

    def test_int_fact_type(self, manufacturing_project, service):
        """Verify INT fact type is preserved in RETURNS TABLE for procedures."""
        service.add_faux_object(
            manufacturing_project.id, "GET_PRODUCTION", "stored_procedure",
            "REPORTING", "MFG",
            selected_dimensions=["plant_name"],
            selected_facts=["units_produced"],
            selected_metrics=["total_output"],
        )
        project = service.get_project(manufacturing_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "UNITS_PRODUCED INT" in sql
        assert "TOTAL_OUTPUT INT" in sql

    def test_batch_generation(self, manufacturing_project, service):
        """Generate all scripts for multiple faux objects."""
        service.add_faux_object(
            manufacturing_project.id, "V_VARIANCE", "view", "RPT", "MFG",
            selected_metrics=["volume_variance", "efficiency_variance"],
        )
        service.add_faux_object(
            manufacturing_project.id, "DT_PLANT", "dynamic_table", "RPT", "MFG",
            warehouse="MFG_WH", target_lag="1 hour",
        )

        scripts = service.generate_all_scripts(manufacturing_project.id)
        # Semantic view DDL + 2 faux objects
        assert len(scripts) == 3
        types = {s.object_type for s in scripts}
        assert FauxObjectType.VIEW in types
        assert FauxObjectType.DYNAMIC_TABLE in types


# =============================================================================
# Persona 6: SaaS Metrics Analyst - Subscription Metrics
# =============================================================================


@pytest.fixture
def saas_project(service):
    """SaaS Metrics Analyst: Subscription Metrics semantic view."""
    project = service.create_project(
        "Subscription Metrics Wrappers",
        "SaaS subscription metrics for investor reporting and ops dashboards",
    )
    service.define_semantic_view(
        project.id, "subscription_metrics", "SAAS", "SEMANTIC",
        comment="SaaS subscription metrics for ARR/MRR tracking",
        ai_sql_generation="MRR is monthly recurring revenue. ARR = MRR * 12. Net retention includes expansion.",
    )

    # Tables
    service.add_semantic_table(project.id, "subscriptions", "SAAS.DW.FACT_SUBSCRIPTIONS", "subscription_id")
    service.add_semantic_table(project.id, "customers", "SAAS.DW.DIM_CUSTOMER", "customer_id")
    service.add_semantic_table(project.id, "products", "SAAS.DW.DIM_PRODUCT", "product_id")
    service.add_semantic_table(project.id, "periods", "SAAS.DW.DIM_PERIOD", "period_id")

    # Relationships
    service.add_semantic_relationship(project.id, "subscriptions", "customer_id", "customers")
    service.add_semantic_relationship(project.id, "subscriptions", "product_id", "products")
    service.add_semantic_relationship(project.id, "subscriptions", "period_id", "periods")

    # Dimensions
    service.add_semantic_column(project.id, "customer_segment", "dimension", "VARCHAR", "customers",
                                synonyms=["segment", "customer type"])
    service.add_semantic_column(project.id, "product_tier", "dimension", "VARCHAR", "products",
                                synonyms=["tier", "plan"])
    service.add_semantic_column(project.id, "acquisition_cohort", "dimension", "VARCHAR", "customers")
    service.add_semantic_column(project.id, "fiscal_month", "dimension", "VARCHAR", "periods")

    # Facts
    service.add_semantic_column(project.id, "monthly_revenue", "fact", "FLOAT", "subscriptions")
    service.add_semantic_column(project.id, "customer_count", "fact", "INT", "subscriptions")

    # Metrics - increasingly complex
    service.add_semantic_column(
        project.id, "mrr", "metric", "FLOAT", "subscriptions",
        expression="SUM(monthly_revenue)",
        comment="Monthly recurring revenue",
    )
    service.add_semantic_column(
        project.id, "arr", "metric", "FLOAT", "subscriptions",
        expression="SUM(monthly_revenue) * 12",
        comment="Annual recurring revenue (MRR x 12)",
    )
    service.add_semantic_column(
        project.id, "net_retention", "metric", "FLOAT", "subscriptions",
        expression="CASE WHEN SUM(CASE WHEN periods.fiscal_month = LAG(periods.fiscal_month) THEN monthly_revenue ELSE 0 END) > 0 THEN SUM(monthly_revenue) / NULLIF(SUM(CASE WHEN periods.fiscal_month = LAG(periods.fiscal_month) THEN monthly_revenue ELSE 0 END), 0) ELSE NULL END",
        comment="Net dollar retention rate",
    )
    service.add_semantic_column(
        project.id, "churn_rate", "metric", "FLOAT", "subscriptions",
        expression="1 - (SUM(customer_count) / NULLIF(LAG(SUM(customer_count)), 0))",
        comment="Customer churn rate",
    )
    service.add_semantic_column(
        project.id, "ltv", "metric", "FLOAT", "subscriptions",
        expression="SUM(monthly_revenue) / NULLIF(1 - (SUM(customer_count) / NULLIF(LAG(SUM(customer_count)), 0)), 0)",
        comment="Customer lifetime value",
    )
    service.add_semantic_column(
        project.id, "cac", "metric", "FLOAT", "subscriptions",
        expression="SUM(monthly_revenue) * 0.3",
        comment="Estimated customer acquisition cost (30% of revenue)",
    )

    return service.get_project(project.id)


class TestSaaSAnalyst:
    """Persona 6: SaaS Metrics Analyst - Subscription Metrics."""

    def test_cohort_analysis_procedure(self, saas_project, service):
        """Generate ANALYZE_COHORT procedure with cohort/tier params."""
        service.add_faux_object(
            saas_project.id, "ANALYZE_COHORT", "stored_procedure",
            "REPORTING", "SAAS",
            parameters=[
                {"name": "COHORT", "data_type": "VARCHAR", "default_value": "NULL"},
                {"name": "TIER", "data_type": "VARCHAR", "default_value": "NULL"},
            ],
            selected_dimensions=["customer_segment", "product_tier", "acquisition_cohort", "fiscal_month"],
            selected_metrics=["mrr", "arr", "net_retention", "churn_rate"],
        )
        project = service.get_project(saas_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.SAAS.ANALYZE_COHORT" in sql
        assert "COHORT VARCHAR" in sql
        assert "TIER VARCHAR" in sql
        assert "MRR FLOAT" in sql
        assert "CHURN_RATE FLOAT" in sql
        assert "RETURNS TABLE(" in sql

    def test_mrr_dashboard_dynamic_table(self, saas_project, service):
        """Generate DT_MRR_DASHBOARD dynamic table with 30-minute refresh."""
        service.add_faux_object(
            saas_project.id, "DT_MRR_DASHBOARD", "dynamic_table",
            "REPORTING", "SAAS",
            warehouse="ANALYTICS_WH",
            target_lag="30 minutes",
            selected_dimensions=["customer_segment", "product_tier", "fiscal_month"],
            selected_metrics=["mrr", "arr", "churn_rate"],
            comment="Near real-time MRR dashboard",
        )
        project = service.get_project(saas_project.id)
        sql = service.generate_dynamic_table_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE DYNAMIC TABLE REPORTING.SAAS.DT_MRR_DASHBOARD" in sql
        assert "TARGET_LAG = '30 minutes'" in sql
        assert "WAREHOUSE = ANALYTICS_WH" in sql
        assert "customers.customer_segment" in sql

    def test_enterprise_filter(self, saas_project, service):
        """Generate view filtered to Enterprise segment only."""
        service.add_faux_object(
            saas_project.id, "V_ENTERPRISE_METRICS", "view",
            "REPORTING", "SAAS",
            where_clause="customers.customer_segment = 'Enterprise'",
            selected_dimensions=["product_tier", "fiscal_month"],
            selected_metrics=["mrr", "arr", "net_retention", "ltv"],
        )
        project = service.get_project(saas_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "WHERE customers.customer_segment = 'Enterprise'" in sql
        assert "products.product_tier" in sql
        assert "SAAS.SEMANTIC.subscription_metrics" in sql

    def test_nested_nullif_persistence(self, saas_project, service):
        """Verify complex NULLIF expressions survive save/load cycle."""
        loaded = service.get_project(saas_project.id)
        net_retention = next(m for m in loaded.semantic_view.metrics if m.name == "net_retention")

        assert "NULLIF" in net_retention.expression
        assert "CASE WHEN" in net_retention.expression
        assert "LAG" in net_retention.expression

    def test_ai_context_in_ddl(self, saas_project, service):
        """Verify AI SQL generation context appears in DDL."""
        ddl = service.generate_semantic_view_ddl(saas_project.semantic_view)

        assert "CREATE OR REPLACE SEMANTIC VIEW SAAS.SEMANTIC.subscription_metrics" in ddl
        assert "AI_SQL_GENERATION" in ddl
        assert "MRR is monthly recurring revenue" in ddl
        assert "ARR = MRR * 12" in ddl


# =============================================================================
# Persona 7: Transportation Analyst - Fleet Operations
# =============================================================================


@pytest.fixture
def transportation_project(service):
    """Transportation Analyst: Fleet Operations semantic view with 5 tables."""
    project = service.create_project(
        "Fleet Operations Wrappers",
        "Fleet operations and lane profitability for logistics",
    )
    service.define_semantic_view(
        project.id, "fleet_operations", "LOGISTICS", "SEMANTIC",
        comment="Fleet operations for trucking and logistics companies",
        ai_sql_generation="Operating ratio = total cost / revenue. Below 100% means profitable.",
    )

    # 5 Tables
    service.add_semantic_table(project.id, "trips", "LOGISTICS.DW.FACT_TRIPS", "trip_id")
    service.add_semantic_table(project.id, "drivers", "LOGISTICS.DW.DIM_DRIVER", "driver_id")
    service.add_semantic_table(project.id, "equipment", "LOGISTICS.DW.DIM_EQUIPMENT", "equipment_id")
    service.add_semantic_table(project.id, "lanes", "LOGISTICS.DW.DIM_LANE", "lane_id")
    service.add_semantic_table(project.id, "terminals", "LOGISTICS.DW.DIM_TERMINAL", "terminal_id")

    # Relationships
    service.add_semantic_relationship(project.id, "trips", "driver_id", "drivers")
    service.add_semantic_relationship(project.id, "trips", "equipment_id", "equipment")
    service.add_semantic_relationship(project.id, "trips", "lane_id", "lanes")
    service.add_semantic_relationship(project.id, "trips", "origin_terminal_id", "terminals")

    # Dimensions
    service.add_semantic_column(project.id, "driver_name", "dimension", "VARCHAR", "drivers",
                                synonyms=["driver", "operator"])
    service.add_semantic_column(project.id, "equipment_type", "dimension", "VARCHAR", "equipment",
                                synonyms=["truck type", "asset type"])
    service.add_semantic_column(project.id, "origin_terminal", "dimension", "VARCHAR", "terminals")
    service.add_semantic_column(project.id, "destination_terminal", "dimension", "VARCHAR", "lanes")
    service.add_semantic_column(project.id, "lane_id", "dimension", "VARCHAR", "lanes")

    # Facts
    service.add_semantic_column(project.id, "loaded_miles", "fact", "FLOAT", "trips")
    service.add_semantic_column(project.id, "empty_miles", "fact", "FLOAT", "trips")
    service.add_semantic_column(project.id, "fuel_gallons", "fact", "FLOAT", "trips")
    service.add_semantic_column(project.id, "freight_revenue", "fact", "FLOAT", "trips")

    # Metrics
    service.add_semantic_column(
        project.id, "revenue_per_mile", "metric", "FLOAT", "trips",
        expression="SUM(freight_revenue) / NULLIF(SUM(loaded_miles + empty_miles), 0)",
        comment="Revenue per total mile",
    )
    service.add_semantic_column(
        project.id, "cost_per_mile", "metric", "FLOAT", "trips",
        expression="(SUM(fuel_gallons) * 3.50 + SUM(loaded_miles + empty_miles) * 0.15) / NULLIF(SUM(loaded_miles + empty_miles), 0)",
        comment="Cost per mile (fuel at $3.50/gal + $0.15/mile maintenance)",
    )
    service.add_semantic_column(
        project.id, "operating_ratio", "metric", "FLOAT", "trips",
        expression="(SUM(fuel_gallons) * 3.50 + SUM(loaded_miles + empty_miles) * 0.15) / NULLIF(SUM(freight_revenue), 0) * 100",
        comment="Operating ratio percentage (below 100 = profitable)",
    )
    service.add_semantic_column(
        project.id, "utilization_rate", "metric", "FLOAT", "trips",
        expression="SUM(loaded_miles) / NULLIF(SUM(loaded_miles + empty_miles), 0) * 100",
        comment="Loaded miles as percentage of total miles",
    )

    return service.get_project(project.id)


class TestTransportationAnalyst:
    """Persona 7: Transportation Analyst - Fleet Operations."""

    def test_daily_fleet_report_task(self, transportation_project, service):
        """Generate DAILY_FLEET_REPORT task with 5am daily CRON."""
        service.add_faux_object(
            transportation_project.id, "DAILY_FLEET_REPORT", "task",
            "REPORTING", "LOGISTICS",
            warehouse="LOGISTICS_WH",
            schedule="USING CRON 0 5 * * * America/Chicago",
            materialized_table="REPORTING.LOGISTICS.DAILY_FLEET_REPORT_MAT",
            comment="Daily fleet operations report materialized at 5am",
        )
        project = service.get_project(transportation_project.id)
        sql = service.generate_task_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.LOGISTICS.DAILY_FLEET_REPORT_MATERIALIZER" in sql
        assert "CREATE OR REPLACE TASK REPORTING.LOGISTICS.DAILY_FLEET_REPORT_REFRESH" in sql
        assert "USING CRON 0 5 * * * America/Chicago" in sql
        assert "WAREHOUSE = LOGISTICS_WH" in sql
        assert "'REPORTING.LOGISTICS.DAILY_FLEET_REPORT_MAT'" in sql
        assert "ALTER TASK" in sql
        assert "RESUME" in sql

    def test_lane_profitability_view(self, transportation_project, service):
        """Generate V_LANE_PROFITABILITY view for lane-level analysis."""
        service.add_faux_object(
            transportation_project.id, "V_LANE_PROFITABILITY", "view",
            "REPORTING", "LOGISTICS",
            selected_dimensions=["lane_id", "origin_terminal", "destination_terminal"],
            selected_metrics=["revenue_per_mile", "cost_per_mile", "operating_ratio"],
            selected_facts=["loaded_miles", "freight_revenue"],
            comment="Lane-level profitability analysis",
        )
        project = service.get_project(transportation_project.id)
        sql = service.generate_view_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE VIEW REPORTING.LOGISTICS.V_LANE_PROFITABILITY" in sql
        assert "lanes.lane_id" in sql
        assert "trips.revenue_per_mile" in sql
        assert "trips.operating_ratio" in sql

    def test_driver_stats_procedure_with_date(self, transportation_project, service):
        """Generate GET_DRIVER_STATS procedure with DATE type parameters."""
        service.add_faux_object(
            transportation_project.id, "GET_DRIVER_STATS", "stored_procedure",
            "REPORTING", "LOGISTICS",
            parameters=[
                {"name": "DRIVER_ID", "data_type": "VARCHAR", "default_value": "NULL"},
                {"name": "START_DATE", "data_type": "DATE", "default_value": "NULL"},
                {"name": "END_DATE", "data_type": "DATE", "default_value": "NULL"},
            ],
            selected_dimensions=["driver_name", "equipment_type"],
            selected_facts=["loaded_miles", "empty_miles", "fuel_gallons", "freight_revenue"],
            selected_metrics=["revenue_per_mile", "utilization_rate"],
        )
        project = service.get_project(transportation_project.id)
        sql = service.generate_stored_procedure_sql(project.semantic_view, project.faux_objects[0])

        assert "CREATE OR REPLACE PROCEDURE REPORTING.LOGISTICS.GET_DRIVER_STATS" in sql
        assert "DRIVER_ID VARCHAR" in sql
        assert "START_DATE DATE" in sql
        assert "END_DATE DATE" in sql
        assert "LOADED_MILES FLOAT" in sql
        assert "REVENUE_PER_MILE FLOAT" in sql

    def test_five_table_ddl(self, transportation_project, service):
        """Verify semantic view DDL includes all 5 tables."""
        ddl = service.generate_semantic_view_ddl(transportation_project.semantic_view)

        assert "CREATE OR REPLACE SEMANTIC VIEW LOGISTICS.SEMANTIC.fleet_operations" in ddl
        assert "trips AS LOGISTICS.DW.FACT_TRIPS" in ddl
        assert "drivers AS LOGISTICS.DW.DIM_DRIVER" in ddl
        assert "equipment AS LOGISTICS.DW.DIM_EQUIPMENT" in ddl
        assert "lanes AS LOGISTICS.DW.DIM_LANE" in ddl
        assert "terminals AS LOGISTICS.DW.DIM_TERMINAL" in ddl
        assert "TABLES (" in ddl
        assert "RELATIONSHIPS (" in ddl

    def test_export_scripts(self, transportation_project, service, temp_dir):
        """Generate and export all scripts to individual files."""
        service.add_faux_object(
            transportation_project.id, "V_LANE_PROFIT", "view",
            "RPT", "LOGISTICS",
            selected_dimensions=["lane_id"],
            selected_metrics=["operating_ratio"],
        )
        service.add_faux_object(
            transportation_project.id, "GET_DRIVER", "stored_procedure",
            "RPT", "LOGISTICS",
            parameters=[{"name": "DRIVER", "data_type": "VARCHAR"}],
        )

        output_dir = os.path.join(temp_dir, "fleet_exports")
        exported = service.export_scripts(transportation_project.id, output_dir)

        assert len(exported) >= 3  # Semantic view + 2 faux objects + bundle
        assert "deployment_bundle" in exported
        assert os.path.exists(exported["deployment_bundle"])

        # Verify bundle content
        with open(exported["deployment_bundle"], "r") as f:
            content = f.read()
        assert "Fleet Operations Wrappers" in content
        assert "CREATE OR REPLACE VIEW" in content
        assert "CREATE OR REPLACE PROCEDURE" in content


# =============================================================================
# Cross-Persona Integration Tests
# =============================================================================


class TestCrossPersonaIntegration:
    """Cross-persona integration tests verifying isolation and completeness."""

    def test_all_personas_create_successfully(self, service):
        """All 7 persona projects can be created in the same service instance."""
        projects = []
        names = [
            "GL Reconciliation",
            "Drilling Economics",
            "Geographic Operations",
            "Cost Allocation",
            "Plant Operations",
            "Subscription Metrics",
            "Fleet Operations",
        ]
        for name in names:
            p = service.create_project(name)
            projects.append(p)

        listed = service.list_projects()
        assert len(listed) == 7
        listed_names = {p["name"] for p in listed}
        assert listed_names == set(names)

    def test_all_faux_types_covered(self, service):
        """All 4 faux object types (view, procedure, dynamic_table, task) work."""
        project = service.create_project("All Types Test")
        service.define_semantic_view(project.id, "test_view", "DB", "SCH")
        service.add_semantic_column(project.id, "dim1", "dimension", "VARCHAR")
        service.add_semantic_column(project.id, "met1", "metric", "FLOAT", expression="SUM(val)")

        # Add one of each type
        service.add_faux_object(project.id, "V_TEST", "view", "RPT", "PUB")
        service.add_faux_object(
            project.id, "GET_TEST", "stored_procedure", "RPT", "PUB",
            parameters=[{"name": "P1", "data_type": "VARCHAR"}],
        )
        service.add_faux_object(
            project.id, "DT_TEST", "dynamic_table", "RPT", "PUB",
            warehouse="WH", target_lag="1 hour",
        )
        service.add_faux_object(
            project.id, "TASK_TEST", "task", "RPT", "PUB",
            warehouse="WH", schedule="USING CRON 0 6 * * * UTC",
        )

        scripts = service.generate_all_scripts(project.id)
        # 1 semantic view DDL + 4 faux objects
        assert len(scripts) == 5
        types = {s.object_type for s in scripts}
        assert FauxObjectType.VIEW in types
        assert FauxObjectType.STORED_PROCEDURE in types
        assert FauxObjectType.DYNAMIC_TABLE in types
        assert FauxObjectType.TASK in types

    def test_project_isolation(self, service):
        """Changes to one project don't affect another."""
        p1 = service.create_project("Project A")
        p2 = service.create_project("Project B")

        service.define_semantic_view(p1.id, "view_a", "DB_A", "SCH_A")
        service.add_semantic_column(p1.id, "col_a", "dimension", "VARCHAR")

        service.define_semantic_view(p2.id, "view_b", "DB_B", "SCH_B")
        service.add_semantic_column(p2.id, "col_b", "dimension", "VARCHAR")
        service.add_semantic_column(p2.id, "col_c", "dimension", "VARCHAR")

        loaded_p1 = service.get_project(p1.id)
        loaded_p2 = service.get_project(p2.id)

        assert len(loaded_p1.semantic_view.dimensions) == 1
        assert len(loaded_p2.semantic_view.dimensions) == 2
        assert loaded_p1.semantic_view.database == "DB_A"
        assert loaded_p2.semantic_view.database == "DB_B"

    def test_project_deletion(self, service):
        """Deleting one project doesn't affect others."""
        p1 = service.create_project("Keep Me")
        p2 = service.create_project("Delete Me")

        assert service.delete_project(p2.id) is True
        assert service.get_project(p2.id) is None
        assert service.get_project(p1.id) is not None
        assert service.get_project(p1.id).name == "Keep Me"

    def test_remove_faux_object_from_multi_object_project(self, service):
        """Remove one faux object while keeping others."""
        project = service.create_project("Multi Object")
        service.define_semantic_view(project.id, "v", "DB", "SCH")
        service.add_semantic_column(project.id, "d1", "dimension", "VARCHAR")
        service.add_semantic_column(project.id, "m1", "metric", "FLOAT", expression="SUM(x)")

        service.add_faux_object(project.id, "OBJ_A", "view", "RPT", "PUB")
        service.add_faux_object(project.id, "OBJ_B", "view", "RPT", "PUB")
        service.add_faux_object(project.id, "OBJ_C", "view", "RPT", "PUB")

        service.remove_faux_object(project.id, "OBJ_B")

        updated = service.get_project(project.id)
        assert len(updated.faux_objects) == 2
        names = {o.name for o in updated.faux_objects}
        assert names == {"OBJ_A", "OBJ_C"}
