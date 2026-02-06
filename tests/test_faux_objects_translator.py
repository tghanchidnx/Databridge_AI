"""Tests for SQL Translator - Faux Objects SQL translation layer."""
import json
import os
import shutil
import tempfile
import pytest

from src.faux_objects.sql_translator import (
    SQLTranslator,
    SQLInputFormat,
    TranslationResult,
)
from src.faux_objects.service import FauxObjectsService
from src.faux_objects.types import (
    SemanticViewDefinition,
    SemanticColumn,
    SemanticColumnType,
    FauxObjectType,
)


@pytest.fixture
def translator():
    """Create a SQLTranslator instance."""
    return SQLTranslator()


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
# Format Detection Tests
# =============================================================================

class TestFormatDetection:
    """Tests for SQL format detection."""

    def test_detect_create_view(self, translator):
        sql = "CREATE VIEW my_view AS SELECT * FROM orders"
        assert translator.detect_format(sql) == SQLInputFormat.CREATE_VIEW

    def test_detect_create_or_replace_view(self, translator):
        sql = "CREATE OR REPLACE VIEW my_db.my_schema.my_view AS SELECT id FROM t"
        assert translator.detect_format(sql) == SQLInputFormat.CREATE_VIEW

    def test_detect_select_query(self, translator):
        sql = "SELECT region, SUM(amount) FROM orders GROUP BY region"
        assert translator.detect_format(sql) == SQLInputFormat.SELECT_QUERY

    def test_detect_select_with_cte(self, translator):
        sql = "WITH cte AS (SELECT * FROM t) SELECT * FROM cte"
        assert translator.detect_format(sql) == SQLInputFormat.SELECT_QUERY

    def test_detect_semantic_view(self, translator):
        sql = "CREATE SEMANTIC VIEW my_sv TABLES (t AS db.schema.table)"
        assert translator.detect_format(sql) == SQLInputFormat.CREATE_SEMANTIC_VIEW

    def test_detect_semantic_view_with_replace(self, translator):
        sql = "CREATE OR REPLACE SEMANTIC VIEW DB.SCH.my_sv TABLES (t AS db.schema.table)"
        assert translator.detect_format(sql) == SQLInputFormat.CREATE_SEMANTIC_VIEW

    def test_detect_with_comments(self, translator):
        sql = """
        -- This is a comment
        /* Multi-line
           comment */
        SELECT * FROM orders
        """
        assert translator.detect_format(sql) == SQLInputFormat.SELECT_QUERY

    def test_detect_unknown(self, translator):
        sql = "INSERT INTO t VALUES (1, 2, 3)"
        assert translator.detect_format(sql) == SQLInputFormat.UNKNOWN

    def test_detect_empty(self, translator):
        assert translator.detect_format("") == SQLInputFormat.UNKNOWN
        assert translator.detect_format("   ") == SQLInputFormat.UNKNOWN


# =============================================================================
# SELECT Query Translation Tests
# =============================================================================

class TestSelectQueryTranslation:
    """Tests for SELECT query parsing and translation."""

    def test_simple_select_all_facts(self, translator):
        """SELECT without GROUP BY should classify columns as FACT."""
        sql = "SELECT id, name, amount FROM orders"
        result = translator.translate(sql, name="test_view", database="DB", schema_name="SCH")

        assert result.input_format == SQLInputFormat.SELECT_QUERY
        assert result.semantic_view.name == "test_view"
        assert len(result.semantic_view.facts) == 3
        assert len(result.semantic_view.dimensions) == 0
        assert len(result.semantic_view.metrics) == 0

    def test_select_with_group_by(self, translator):
        """Columns in GROUP BY become DIMENSION, aggregations become METRIC."""
        sql = """
        SELECT region, product_type, SUM(amount) as total_sales
        FROM orders
        GROUP BY region, product_type
        """
        result = translator.translate(sql)

        # GROUP BY columns → DIMENSION
        assert len(result.semantic_view.dimensions) >= 2
        dim_names = {d.name.lower() for d in result.semantic_view.dimensions}
        assert "region" in dim_names
        assert "product_type" in dim_names

        # SUM(amount) → METRIC
        assert len(result.semantic_view.metrics) >= 1
        metric_names = {m.name.lower() for m in result.semantic_view.metrics}
        assert "total_sales" in metric_names

    def test_multiple_aggregations(self, translator):
        """Multiple aggregation functions create multiple metrics."""
        sql = """
        SELECT
            category,
            SUM(amount) as total,
            COUNT(*) as cnt,
            AVG(price) as avg_price
        FROM products
        GROUP BY category
        """
        result = translator.translate(sql)

        # Should have 3 metrics
        assert len(result.semantic_view.metrics) >= 3
        metric_names = {m.name.lower() for m in result.semantic_view.metrics}
        assert "total" in metric_names
        assert "cnt" in metric_names
        assert "avg_price" in metric_names

    def test_table_aliases(self, translator):
        """Table aliases should be extracted."""
        sql = """
        SELECT o.id, o.amount
        FROM orders o
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.tables) == 1
        # The table alias 'o' should be captured
        assert result.semantic_view.tables[0].alias == "o"

    def test_joins(self, translator):
        """JOINs should create relationships."""
        sql = """
        SELECT o.id, c.name, SUM(o.amount) as total
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        GROUP BY o.id, c.name
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.tables) == 2
        # Should have at least one relationship
        assert len(result.semantic_view.relationships) >= 1

    def test_case_when_in_sum(self, translator):
        """CASE WHEN inside SUM should be a METRIC."""
        sql = """
        SELECT
            region,
            SUM(CASE WHEN category = 'A' THEN amount ELSE 0 END) as cat_a_total
        FROM orders
        GROUP BY region
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.metrics) >= 1
        metric_names = {m.name.lower() for m in result.semantic_view.metrics}
        assert "cat_a_total" in metric_names

    def test_where_clause_preserved(self, translator):
        """WHERE clause doesn't affect classification."""
        sql = """
        SELECT region, SUM(amount) as total
        FROM orders
        WHERE year = 2024
        GROUP BY region
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.dimensions) >= 1
        assert len(result.semantic_view.metrics) >= 1

    def test_data_type_inference(self, translator):
        """Data types should be inferred from context."""
        sql = """
        SELECT
            name,
            SUM(amount) as total_amount,
            COUNT(*) as row_count
        FROM orders
        GROUP BY name
        """
        result = translator.translate(sql)

        # SUM should infer FLOAT
        for m in result.semantic_view.metrics:
            if "amount" in m.name.lower():
                assert m.data_type == "FLOAT"
            if "count" in m.name.lower():
                assert m.data_type == "INT"

    def test_complex_expressions(self, translator):
        """Complex expressions like NULLIF should be handled."""
        sql = """
        SELECT
            product,
            SUM(revenue) / NULLIF(SUM(units), 0) as avg_revenue_per_unit
        FROM sales
        GROUP BY product
        """
        result = translator.translate(sql)

        # Should parse without error and have a metric
        assert len(result.semantic_view.dimensions) >= 1
        assert len(result.semantic_view.metrics) >= 1

    def test_select_star_warning(self, translator):
        """SELECT * should produce a warning."""
        sql = "SELECT * FROM orders"
        result = translator.translate(sql)

        assert any("SELECT *" in w for w in result.warnings)


# =============================================================================
# CREATE VIEW Translation Tests
# =============================================================================

class TestCreateViewTranslation:
    """Tests for CREATE VIEW parsing and translation."""

    def test_simple_create_view(self, translator):
        """Basic CREATE VIEW should extract view name and parse SELECT."""
        sql = """
        CREATE VIEW REPORTING.PUBLIC.V_SALES AS
        SELECT region, SUM(amount) as total
        FROM orders
        GROUP BY region
        """
        result = translator.translate(sql)

        assert result.input_format == SQLInputFormat.CREATE_VIEW
        # Faux config should be created
        assert result.faux_config is not None
        assert "V_SALES" in result.faux_config.name

    def test_view_with_semantic_view_call(self, translator):
        """CREATE VIEW wrapping SEMANTIC_VIEW() should parse the call."""
        sql = """
        CREATE VIEW REPORTING.PUBLIC.V_PL AS
        SELECT * FROM SEMANTIC_VIEW(
            FINANCE.SEMANTIC.pl_analysis
            DIMENSIONS accounts.account_name, periods.fiscal_year
            METRICS gl_entries.total_revenue, gl_entries.net_income
        )
        """
        result = translator.translate(sql)

        assert result.input_format == SQLInputFormat.CREATE_VIEW
        assert len(result.semantic_view.dimensions) >= 2
        assert len(result.semantic_view.metrics) >= 2

    def test_extract_view_name_components(self, translator):
        """View name should be decomposed into database.schema.name."""
        sql = """
        CREATE OR REPLACE VIEW MY_DB.MY_SCHEMA.MY_VIEW AS
        SELECT id FROM t
        """
        result = translator.translate(sql)

        # The faux_config should have the extracted names
        assert result.faux_config is not None
        assert result.faux_config.target_database == "MY_DB"
        assert result.faux_config.target_schema == "MY_SCHEMA"

    def test_view_with_where(self, translator):
        """CREATE VIEW with WHERE clause should parse correctly."""
        sql = """
        CREATE VIEW v AS
        SELECT region, SUM(amount) as total
        FROM orders
        WHERE year = 2024
        GROUP BY region
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.dimensions) >= 1
        assert len(result.semantic_view.metrics) >= 1

    def test_creates_faux_object_config(self, translator):
        """CREATE VIEW should create a FauxObjectConfig."""
        sql = """
        CREATE VIEW DB.SCH.V_TEST AS
        SELECT id, name FROM users
        """
        result = translator.translate(sql)

        assert result.faux_config is not None
        assert result.faux_config.faux_type == FauxObjectType.VIEW

    def test_view_with_comment(self, translator):
        """CREATE VIEW with COMMENT should extract the comment."""
        sql = """
        CREATE VIEW v
            COMMENT = 'Sales summary view'
        AS SELECT * FROM t
        """
        result = translator.translate(sql)

        assert result.faux_config is not None
        assert result.faux_config.comment == "Sales summary view"


# =============================================================================
# CREATE SEMANTIC VIEW DDL Translation Tests
# =============================================================================

class TestSemanticViewDDLTranslation:
    """Tests for CREATE SEMANTIC VIEW DDL parsing."""

    def test_minimal_semantic_view(self, translator):
        """Minimal SEMANTIC VIEW with TABLES and DIMENSIONS only."""
        sql = """
        CREATE SEMANTIC VIEW DB.SCH.my_sv
        TABLES (
            orders AS WAREHOUSE.SALES.FACT_ORDERS
        )
        DIMENSIONS (
            orders.region,
            orders.product
        )
        """
        result = translator.translate(sql)

        assert result.input_format == SQLInputFormat.CREATE_SEMANTIC_VIEW
        assert result.semantic_view.name == "my_sv"
        assert len(result.semantic_view.tables) == 1
        assert len(result.semantic_view.dimensions) == 2

    def test_full_semantic_view(self, translator):
        """Full SEMANTIC VIEW with all blocks."""
        sql = """
        CREATE OR REPLACE SEMANTIC VIEW FINANCE.SEMANTIC.pl_analysis
            COMMENT = 'P&L analysis view'
            AI_SQL_GENERATION = 'Revenue is recognized at point of sale.'

        TABLES (
            gl_entries AS FINANCE.GL.FACT_JOURNAL_ENTRIES
                PRIMARY KEY (journal_entry_id),
            accounts AS FINANCE.GL.DIM_ACCOUNT
                PRIMARY KEY (account_code)
        )

        RELATIONSHIPS (
            gl_entries (account_code) REFERENCES accounts (account_code)
        )

        FACTS (
            gl_entries.debit_amount,
            gl_entries.credit_amount
        )

        DIMENSIONS (
            accounts.account_name AS account_name
                WITH SYNONYMS = ('GL account', 'ledger account'),
            accounts.account_category AS category
        )

        METRICS (
            gl_entries.total_revenue AS SUM(CASE WHEN account_category = 'Revenue' THEN net_amount ELSE 0 END)
                COMMENT = 'Total revenue'
        )
        """
        result = translator.translate(sql)

        sv = result.semantic_view
        assert sv.name == "pl_analysis"
        assert sv.database == "FINANCE"
        assert sv.schema_name == "SEMANTIC"
        assert sv.comment == "P&L analysis view"
        assert sv.ai_sql_generation == "Revenue is recognized at point of sale."
        assert len(sv.tables) == 2
        assert len(sv.relationships) == 1
        assert len(sv.facts) == 2
        assert len(sv.dimensions) == 2
        assert len(sv.metrics) == 1

    def test_comment_and_ai_sql_generation(self, translator):
        """COMMENT and AI_SQL_GENERATION should be extracted."""
        sql = """
        CREATE SEMANTIC VIEW sv
            COMMENT = 'Test comment with ''quotes'''
            AI_SQL_GENERATION = 'AI hint text'
        TABLES (t AS db.sch.tbl)
        """
        result = translator.translate(sql)

        assert result.semantic_view.comment == "Test comment with 'quotes'"
        assert result.semantic_view.ai_sql_generation == "AI hint text"

    def test_primary_key(self, translator):
        """PRIMARY KEY should be extracted from tables."""
        sql = """
        CREATE SEMANTIC VIEW sv
        TABLES (
            orders AS db.sch.orders
                PRIMARY KEY (order_id)
        )
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.tables) == 1
        assert result.semantic_view.tables[0].primary_key == "order_id"

    def test_with_synonyms(self, translator):
        """WITH SYNONYMS should be parsed."""
        sql = """
        CREATE SEMANTIC VIEW sv
        TABLES (t AS db.sch.tbl)
        DIMENSIONS (
            t.col AS my_col
                WITH SYNONYMS = ('alias1', 'alias2')
        )
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.dimensions) == 1
        dim = result.semantic_view.dimensions[0]
        assert len(dim.synonyms) == 2
        assert "alias1" in dim.synonyms
        assert "alias2" in dim.synonyms

    def test_complex_metric_expressions(self, translator):
        """Complex metric expressions should be captured."""
        sql = """
        CREATE SEMANTIC VIEW sv
        TABLES (t AS db.sch.tbl)
        METRICS (
            t.gross_profit AS SUM(revenue) - SUM(cost)
                COMMENT = 'Revenue minus cost'
        )
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.metrics) == 1
        metric = result.semantic_view.metrics[0]
        assert "SUM" in metric.expression
        assert metric.comment == "Revenue minus cost"

    def test_relationships_with_target_column(self, translator):
        """RELATIONSHIPS with explicit target column."""
        sql = """
        CREATE SEMANTIC VIEW sv
        TABLES (
            orders AS db.sch.orders,
            customers AS db.sch.customers
        )
        RELATIONSHIPS (
            orders (customer_id) REFERENCES customers (id)
        )
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.relationships) == 1
        rel = result.semantic_view.relationships[0]
        assert rel.from_table == "orders"
        assert rel.from_column == "customer_id"
        assert rel.to_table == "customers"
        assert rel.to_column == "id"

    def test_relationships_without_target_column(self, translator):
        """RELATIONSHIPS without explicit target column (uses primary key)."""
        sql = """
        CREATE SEMANTIC VIEW sv
        TABLES (
            orders AS db.sch.orders,
            customers AS db.sch.customers PRIMARY KEY (id)
        )
        RELATIONSHIPS (
            orders (customer_id) REFERENCES customers
        )
        """
        result = translator.translate(sql)

        assert len(result.semantic_view.relationships) == 1
        rel = result.semantic_view.relationships[0]
        assert rel.to_column is None  # Uses primary key

    def test_roundtrip_parse_regenerate(self, translator, temp_dir):
        """Parse DDL, regenerate it, and verify structure is preserved."""
        original_sql = """
        CREATE SEMANTIC VIEW FINANCE.SEMANTIC.test_sv
            COMMENT = 'Test view'
        TABLES (
            orders AS WAREHOUSE.SALES.ORDERS
                PRIMARY KEY (order_id)
        )
        DIMENSIONS (
            orders.region
        )
        METRICS (
            orders.total AS SUM(amount)
        )
        """
        result1 = translator.translate(original_sql)
        sv1 = result1.semantic_view

        # Regenerate DDL using service
        service = FauxObjectsService(data_dir=temp_dir)
        regenerated = service.generate_semantic_view_ddl(sv1)

        # Parse the regenerated DDL
        result2 = translator.translate(regenerated)
        sv2 = result2.semantic_view

        # Verify structure is preserved
        assert sv2.name == sv1.name
        assert len(sv2.tables) == len(sv1.tables)
        assert len(sv2.dimensions) == len(sv1.dimensions)
        assert len(sv2.metrics) == len(sv1.metrics)


# =============================================================================
# Convert Format Tests
# =============================================================================

class TestConvertFormat:
    """Tests for SQL format conversion."""

    def test_select_to_semantic_view_ddl(self, translator):
        """Convert SELECT query to CREATE SEMANTIC VIEW DDL."""
        sql = """
        SELECT region, SUM(amount) as total_sales
        FROM WAREHOUSE.SALES.ORDERS o
        GROUP BY region
        """
        converted = translator.convert(
            sql,
            "semantic_view_ddl",
            name="sales_analysis",
            database="ANALYTICS",
            schema_name="SEMANTIC",
        )

        assert "CREATE OR REPLACE SEMANTIC VIEW" in converted
        assert "ANALYTICS.SEMANTIC.sales_analysis" in converted
        assert "DIMENSIONS" in converted
        assert "METRICS" in converted

    def test_semantic_ddl_to_create_view(self, translator):
        """Convert CREATE SEMANTIC VIEW DDL to CREATE VIEW."""
        sql = """
        CREATE SEMANTIC VIEW DB.SCH.my_sv
        TABLES (orders AS WAREHOUSE.SALES.ORDERS)
        DIMENSIONS (orders.region)
        METRICS (orders.total_sales AS SUM(amount))
        """
        converted = translator.convert(
            sql,
            "create_view",
            target_database="REPORTING",
            target_schema="PUBLIC",
        )

        assert "CREATE OR REPLACE VIEW" in converted
        assert "SEMANTIC_VIEW(" in converted

    def test_create_view_to_semantic_ddl(self, translator):
        """Convert CREATE VIEW to CREATE SEMANTIC VIEW DDL."""
        sql = """
        CREATE VIEW REPORTING.PUBLIC.V_SALES AS
        SELECT region, SUM(amount) as total
        FROM orders
        GROUP BY region
        """
        converted = translator.convert(sql, "semantic_view_ddl")

        assert "CREATE OR REPLACE SEMANTIC VIEW" in converted

    def test_roundtrip_conversion(self, translator):
        """Convert SELECT → DDL → SELECT should preserve structure."""
        original_sql = """
        SELECT region, product, SUM(amount) as total
        FROM WAREHOUSE.SALES.ORDERS
        GROUP BY region, product
        """

        # SELECT → SEMANTIC VIEW DDL
        ddl = translator.convert(
            original_sql,
            "semantic_view_ddl",
            name="test_sv",
            database="DB",
            schema_name="SCH",
        )

        # SEMANTIC VIEW DDL → SELECT
        select_sql = translator.convert(ddl, "select_query")

        # Parse both and compare structure
        result1 = translator.translate(original_sql)
        result2 = translator.translate(select_sql)

        # Both should have same number of dimensions/metrics
        assert len(result1.semantic_view.dimensions) == len(result2.semantic_view.dimensions)
        assert len(result1.semantic_view.metrics) == len(result2.semantic_view.metrics)


# =============================================================================
# Translate to Project Tests
# =============================================================================

class TestTranslateToProject:
    """Tests for translate_to_project functionality."""

    def test_project_from_select(self, translator, service):
        """Create a project from a SELECT query."""
        sql = """
        SELECT region, SUM(amount) as total_sales
        FROM WAREHOUSE.SALES.ORDERS o
        GROUP BY region
        """
        project = translator.translate_to_project(
            sql,
            "Sales Analysis",
            service,
            description="Auto-generated from SQL",
        )

        assert project.name == "Sales Analysis"
        assert project.description == "Auto-generated from SQL"
        assert project.semantic_view is not None
        assert len(project.semantic_view.dimensions) >= 1
        assert len(project.semantic_view.metrics) >= 1

    def test_project_from_semantic_ddl(self, translator, service):
        """Create a project from a SEMANTIC VIEW DDL."""
        sql = """
        CREATE SEMANTIC VIEW FINANCE.SEMANTIC.pl_analysis
            COMMENT = 'P&L Analysis'
        TABLES (
            gl AS FINANCE.GL.JOURNAL_ENTRIES PRIMARY KEY (entry_id)
        )
        DIMENSIONS (gl.account_name)
        METRICS (gl.total AS SUM(amount))
        """
        project = translator.translate_to_project(
            sql,
            "P&L Project",
            service,
        )

        assert project.semantic_view is not None
        assert project.semantic_view.name == "pl_analysis"
        assert project.semantic_view.comment == "P&L Analysis"
        assert len(project.semantic_view.tables) == 1
        assert project.semantic_view.tables[0].primary_key == "entry_id"

    def test_project_with_faux_object(self, translator, service):
        """Create a project with a faux object configured."""
        sql = """
        SELECT region, SUM(amount) as total
        FROM orders
        GROUP BY region
        """
        project = translator.translate_to_project(
            sql,
            "Sales Project",
            service,
            faux_type="view",
            target_database="REPORTING",
            target_schema="PUBLIC",
        )

        assert len(project.faux_objects) == 1
        assert project.faux_objects[0].faux_type == FauxObjectType.VIEW
        assert project.faux_objects[0].target_database == "REPORTING"


# =============================================================================
# Edge Cases Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_sql(self, translator):
        """Empty SQL should raise an error."""
        with pytest.raises(ValueError):
            translator.translate("")

    def test_malformed_sql(self, translator):
        """Malformed SQL should raise an error."""
        with pytest.raises(ValueError):
            translator.translate("SELECT * FROM")

    def test_no_tables(self, translator):
        """SQL with no tables should still parse."""
        sql = "SELECT 1 + 1 as result"
        result = translator.translate(sql)

        # Should parse without error
        assert result.semantic_view is not None

    def test_very_simple_select(self, translator):
        """Very simple SELECT should parse."""
        sql = "SELECT 1 + 1"
        result = translator.translate(sql)

        assert result.input_format == SQLInputFormat.SELECT_QUERY

    def test_unknown_format_raises(self, translator):
        """Unknown format should raise an error."""
        with pytest.raises(ValueError):
            translator.translate("DROP TABLE users")


# =============================================================================
# MCP Tool Tests
# =============================================================================

class TestMCPToolIntegration:
    """Tests for MCP tool wrappers."""

    def test_detect_sql_format_tool(self, temp_dir):
        """Test detect_sql_format MCP tool."""
        # Import and register tools
        from src.faux_objects.mcp_tools import register_faux_objects_tools

        class MockMCP:
            def __init__(self):
                self.tools = {}

            def tool(self):
                def decorator(func):
                    self.tools[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_faux_objects_tools(mcp, temp_dir)

        # Test the tool
        result = mcp.tools["detect_sql_format"]("SELECT * FROM orders")
        data = json.loads(result)

        assert data["status"] == "success"
        assert data["format"] == "select_query"

    def test_translate_sql_to_semantic_view_tool(self, temp_dir):
        """Test translate_sql_to_semantic_view MCP tool."""
        from src.faux_objects.mcp_tools import register_faux_objects_tools

        class MockMCP:
            def __init__(self):
                self.tools = {}

            def tool(self):
                def decorator(func):
                    self.tools[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_faux_objects_tools(mcp, temp_dir)

        sql = "SELECT region, SUM(amount) as total FROM orders GROUP BY region"
        result = mcp.tools["translate_sql_to_semantic_view"](
            sql,
            name="test_sv",
            database="DB",
            schema_name="SCH",
        )
        data = json.loads(result)

        assert data["status"] == "success"
        assert data["semantic_view"]["name"] == "test_sv"
        assert len(data["dimensions"]) >= 1
        assert len(data["metrics"]) >= 1

    def test_convert_sql_format_tool(self, temp_dir):
        """Test convert_sql_format MCP tool."""
        from src.faux_objects.mcp_tools import register_faux_objects_tools

        class MockMCP:
            def __init__(self):
                self.tools = {}

            def tool(self):
                def decorator(func):
                    self.tools[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_faux_objects_tools(mcp, temp_dir)

        sql = "SELECT region, SUM(amount) as total FROM orders GROUP BY region"
        result = mcp.tools["convert_sql_format"](
            sql,
            "semantic_view_ddl",
            name="test_sv",
            database="DB",
            schema_name="SCH",
        )
        data = json.loads(result)

        assert data["status"] == "success"
        assert "CREATE OR REPLACE SEMANTIC VIEW" in data["sql"]
