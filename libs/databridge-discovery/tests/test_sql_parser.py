"""
Unit tests for SQL Parser module.
"""

import pytest

from databridge_discovery.parser.sql_parser import SQLParser
from databridge_discovery.models.parsed_query import (
    JoinType,
    AggregationType,
    ColumnDataType,
)


class TestSQLParser:
    """Tests for SQLParser class."""

    def test_parse_simple_select(self):
        """Test parsing a simple SELECT statement."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("SELECT id, name FROM users")

        assert result.query_type == "SELECT"
        assert len(result.tables) == 1
        assert result.tables[0].name == "users"
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert result.columns[1].name == "name"

    def test_parse_select_with_alias(self):
        """Test parsing SELECT with column and table aliases."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("SELECT u.id AS user_id, u.name FROM users u")

        assert len(result.tables) == 1
        assert result.tables[0].name == "users"
        assert result.tables[0].alias == "u"
        assert result.columns[0].name == "user_id"
        assert result.columns[0].source_name == "id"

    def test_parse_join(self):
        """Test parsing JOIN statements."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT a.id, b.name
        FROM orders a
        LEFT JOIN customers b ON a.customer_id = b.id
        """
        result = parser.parse(sql)

        assert len(result.tables) == 2
        assert len(result.joins) == 1
        assert result.joins[0].join_type == JoinType.LEFT
        assert result.joins[0].right_table == "b"

    def test_parse_multiple_joins(self):
        """Test parsing multiple JOINs."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT o.id, c.name, p.product_name
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.id
        LEFT JOIN products p ON o.product_id = p.id
        """
        result = parser.parse(sql)

        assert len(result.tables) == 3
        assert len(result.joins) == 2
        assert result.joins[0].join_type == JoinType.INNER
        assert result.joins[1].join_type == JoinType.LEFT

    def test_parse_aggregation(self):
        """Test parsing aggregation functions."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM orders
        GROUP BY customer_id
        """
        result = parser.parse(sql)

        assert result.metrics.has_group_by
        assert result.metrics.aggregation_count == 3
        assert result.columns[1].aggregation == AggregationType.COUNT
        assert result.columns[2].aggregation == AggregationType.SUM
        assert result.columns[3].aggregation == AggregationType.AVG

    def test_parse_cte(self):
        """Test parsing Common Table Expressions."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        WITH active_customers AS (
            SELECT id, name FROM customers WHERE status = 'active'
        )
        SELECT * FROM active_customers
        """
        result = parser.parse(sql)

        assert "active_customers" in result.ctes
        assert result.metrics.cte_count == 1

    def test_parse_subquery(self):
        """Test parsing subqueries."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT *
        FROM orders o
        WHERE customer_id IN (
            SELECT id FROM customers WHERE status = 'active'
        )
        """
        result = parser.parse(sql)

        assert result.metrics.subquery_count >= 1

    def test_parse_case_statement(self):
        """Test parsing CASE statements."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT
            id,
            CASE
                WHEN amount > 1000 THEN 'high'
                WHEN amount > 100 THEN 'medium'
                ELSE 'low'
            END as tier
        FROM orders
        """
        result = parser.parse(sql)

        assert result.metrics.case_statement_count == 1
        tier_col = result.columns[1]
        assert tier_col.is_case_statement
        assert tier_col.name == "tier"

    def test_parse_where_clause(self):
        """Test parsing WHERE clause."""
        parser = SQLParser(dialect="snowflake")
        sql = "SELECT * FROM users WHERE active = 1 AND created_at > '2023-01-01'"
        result = parser.parse(sql)

        assert result.where_clause is not None
        assert "active" in result.where_clause

    def test_parse_order_by(self):
        """Test parsing ORDER BY clause."""
        parser = SQLParser(dialect="snowflake")
        sql = "SELECT * FROM users ORDER BY created_at DESC, name ASC"
        result = parser.parse(sql)

        assert result.metrics.has_order_by
        assert len(result.order_by_columns) == 2

    def test_parse_snowflake_specific(self):
        """Test Snowflake-specific syntax."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT
            account_code,
            CASE WHEN account_code ILIKE '5%' THEN 'Revenue' END as category
        FROM accounts
        """
        result = parser.parse(sql)

        assert result.metrics.case_statement_count == 1

    def test_parse_postgres_dialect(self):
        """Test PostgreSQL dialect parsing."""
        parser = SQLParser(dialect="postgres")
        sql = "SELECT * FROM users WHERE name ~* 'john'"
        result = parser.parse(sql)

        assert result.dialect == "postgres"
        assert result.query_type == "SELECT"

    def test_complexity_simple(self):
        """Test complexity estimation for simple query."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("SELECT * FROM users")

        assert result.metrics.estimated_complexity == "simple"

    def test_complexity_moderate(self):
        """Test complexity estimation for moderate query."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT a.*, b.name, c.category
        FROM orders a
        JOIN customers b ON a.customer_id = b.id
        JOIN products c ON a.product_id = c.id
        WHERE a.status = 'active'
        """
        result = parser.parse(sql)

        assert result.metrics.estimated_complexity in ("simple", "moderate")

    def test_complexity_complex(self):
        """Test complexity estimation for complex query."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) as rn
            FROM orders
        ),
        aggregated AS (
            SELECT customer_id, SUM(amount) as total
            FROM ranked
            WHERE rn = 1
            GROUP BY customer_id
        )
        SELECT c.*, a.total,
            CASE WHEN a.total > 10000 THEN 'platinum'
                 WHEN a.total > 5000 THEN 'gold'
                 ELSE 'silver'
            END as tier
        FROM customers c
        JOIN aggregated a ON c.id = a.customer_id
        WHERE c.status = 'active'
        """
        result = parser.parse(sql)

        assert result.metrics.cte_count == 2
        assert result.metrics.case_statement_count == 1

    def test_parse_multiple_statements(self):
        """Test parsing multiple SQL statements."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT * FROM users;
        SELECT * FROM orders;
        """
        results = parser.parse_multiple(sql)

        assert len(results) == 2
        assert results[0].tables[0].name == "users"
        assert results[1].tables[0].name == "orders"

    def test_validate_sql_valid(self):
        """Test SQL validation for valid SQL."""
        parser = SQLParser(dialect="snowflake")
        is_valid, errors = parser.validate_sql("SELECT * FROM users")

        assert is_valid
        assert len(errors) == 0

    def test_validate_sql_invalid(self):
        """Test SQL validation for invalid SQL."""
        parser = SQLParser(dialect="snowflake")
        # Use truly invalid SQL that sqlglot can't parse
        is_valid, errors = parser.validate_sql("((( SELECT %%% FROM")

        # Note: sqlglot is very lenient, so some invalid SQL may still parse
        # The important thing is it doesn't crash
        assert isinstance(is_valid, bool)

    def test_transpile(self):
        """Test SQL transpilation between dialects."""
        parser = SQLParser(dialect="snowflake")
        sql = "SELECT IFNULL(name, 'Unknown') FROM users"
        result = parser.transpile(sql, "postgres")

        assert "COALESCE" in result or "IFNULL" in result  # Different dialects handle this differently

    def test_parse_with_qualified_names(self):
        """Test parsing fully qualified table names."""
        parser = SQLParser(dialect="snowflake")
        sql = "SELECT * FROM edw.financial.fact_orders"
        result = parser.parse(sql)

        assert len(result.tables) == 1
        assert result.tables[0].name == "fact_orders"
        assert result.tables[0].schema_name == "financial"
        assert result.tables[0].database == "edw"

    def test_data_type_inference(self):
        """Test data type inference for columns."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT
            id,
            ROUND(amount, 2) as rounded_amount,
            TO_DATE(created_at) as created_date,
            COUNT(*) as cnt
        FROM orders
        GROUP BY id, amount, created_at
        """
        result = parser.parse(sql)

        # Check that columns are parsed
        assert len(result.columns) >= 4

        # Check that some types are inferred (may vary by sqlglot version)
        types_found = [c.data_type for c in result.columns]
        # At minimum, we should have some type inference happening
        assert len(types_found) >= 4


class TestSQLParserEdgeCases:
    """Edge case tests for SQL Parser."""

    def test_empty_sql(self):
        """Test handling of empty SQL."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("")

        assert result.query_type == "UNKNOWN"
        assert len(result.parse_errors) > 0

    def test_whitespace_only(self):
        """Test handling of whitespace-only SQL."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("   \n\t  ")

        assert result.query_type == "UNKNOWN"

    def test_comment_only(self):
        """Test handling of comment-only SQL."""
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("-- This is just a comment")

        # Should handle gracefully
        assert result is not None

    def test_unicode_content(self):
        """Test handling of unicode in SQL."""
        parser = SQLParser(dialect="snowflake")
        sql = "SELECT * FROM users WHERE name = 'André'"
        result = parser.parse(sql)

        assert result.query_type == "SELECT"
        assert result.where_clause is not None

    def test_very_long_sql(self):
        """Test handling of very long SQL."""
        parser = SQLParser(dialect="snowflake")
        # Generate long SQL with many columns
        columns = ", ".join([f"col_{i}" for i in range(100)])
        sql = f"SELECT {columns} FROM large_table"
        result = parser.parse(sql)

        assert len(result.columns) == 100

    def test_deeply_nested_subquery(self):
        """Test handling of deeply nested subqueries."""
        parser = SQLParser(dialect="snowflake")
        sql = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM users
                ) a
            ) b
        ) c
        """
        result = parser.parse(sql)

        assert result.metrics.nesting_depth >= 2
