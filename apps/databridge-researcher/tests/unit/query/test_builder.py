"""Unit tests for the QueryBuilder class."""

import pytest

from src.query.builder import QueryBuilder, Query, select, from_table
from src.query.dialects import (
    SnowflakeDialect,
    TSQLDialect,
    SparkDialect,
    PostgreSQLDialect,
)


class TestQueryBuilder:
    """Tests for QueryBuilder."""

    def test_simple_select(self):
        """Test simple SELECT query."""
        query = (QueryBuilder()
            .select("id", "name", "amount")
            .from_table("sales")
            .build())

        assert query.sql is not None
        assert "SELECT" in query.sql
        assert "id" in query.sql
        assert "name" in query.sql
        assert "amount" in query.sql
        assert "FROM sales" in query.sql

    def test_select_all(self):
        """Test SELECT * query."""
        query = (QueryBuilder()
            .from_table("sales")
            .build())

        assert "SELECT *" in query.sql

    def test_select_distinct(self):
        """Test SELECT DISTINCT."""
        query = (QueryBuilder()
            .distinct()
            .select("category")
            .from_table("products")
            .build())

        assert "SELECT DISTINCT" in query.sql

    def test_where_clause(self):
        """Test WHERE clause."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .where("amount > 100")
            .where("status = 'active'")
            .build())

        assert "WHERE" in query.sql
        assert "amount > 100" in query.sql
        assert "AND status = 'active'" in query.sql

    def test_where_or(self):
        """Test WHERE with OR connector."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .where("amount > 100")
            .where_or("priority = 'high'")
            .build())

        assert "OR priority = 'high'" in query.sql

    def test_group_by(self):
        """Test GROUP BY clause."""
        query = (QueryBuilder()
            .select("category", "COUNT(*) AS count")
            .from_table("products")
            .group_by("category")
            .build())

        assert "GROUP BY category" in query.sql

    def test_having(self):
        """Test HAVING clause."""
        query = (QueryBuilder()
            .select("category", "SUM(amount) AS total")
            .from_table("sales")
            .group_by("category")
            .having("SUM(amount) > 1000")
            .build())

        assert "HAVING" in query.sql
        assert "SUM(amount) > 1000" in query.sql

    def test_order_by(self):
        """Test ORDER BY clause."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .order_by("amount", "DESC")
            .build())

        assert "ORDER BY amount DESC" in query.sql

    def test_order_by_nulls(self):
        """Test ORDER BY with NULLS handling."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .order_by("amount", "DESC", nulls="LAST")
            .build())

        assert "NULLS LAST" in query.sql

    def test_limit_offset(self):
        """Test LIMIT and OFFSET."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .limit(10)
            .offset(20)
            .build())

        assert "LIMIT 10" in query.sql
        assert "OFFSET 20" in query.sql

    def test_join(self):
        """Test JOIN clause."""
        query = (QueryBuilder()
            .select("s.id", "c.name")
            .from_table("sales", alias="s")
            .join("customers", "s.customer_id = customers.id")
            .build())

        assert "INNER JOIN customers" in query.sql
        assert "ON s.customer_id = customers.id" in query.sql

    def test_left_join(self):
        """Test LEFT JOIN."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales", alias="s")
            .left_join("customers", "s.customer_id = customers.id", alias="c")
            .build())

        assert "LEFT JOIN customers" in query.sql

    def test_multiple_joins(self):
        """Test multiple JOINs."""
        query = (QueryBuilder()
            .select("*")
            .from_table("orders")
            .join("customers", "orders.customer_id = customers.id")
            .join("products", "orders.product_id = products.id")
            .build())

        assert query.sql.count("JOIN") == 2

    def test_select_aggregate(self):
        """Test aggregate function in SELECT."""
        query = (QueryBuilder()
            .select_aggregate("SUM", "amount", "total_amount")
            .from_table("sales")
            .build())

        assert "SUM(amount)" in query.sql
        assert "total_amount" in query.sql

    def test_cte(self):
        """Test Common Table Expression."""
        query = (QueryBuilder()
            .with_cte("top_sales", "SELECT * FROM sales WHERE amount > 1000")
            .select("*")
            .from_table("top_sales")
            .build())

        assert "WITH top_sales AS" in query.sql

    def test_union(self):
        """Test UNION query."""
        query = (QueryBuilder()
            .select("id", "name")
            .from_table("customers")
            .union("SELECT id, name FROM prospects")
            .build())

        assert "UNION" in query.sql

    def test_union_all(self):
        """Test UNION ALL."""
        query = (QueryBuilder()
            .select("id", "name")
            .from_table("customers")
            .union("SELECT id, name FROM prospects", all=True)
            .build())

        assert "UNION ALL" in query.sql

    def test_parameters(self):
        """Test query parameters."""
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .where("amount > :min_amount")
            .param("min_amount", 100)
            .build())

        assert ":min_amount" in query.sql
        assert query.parameters["min_amount"] == 100

    def test_clone(self):
        """Test query builder cloning."""
        builder1 = QueryBuilder().select("*").from_table("sales")
        builder2 = builder1.clone().where("amount > 100")

        query1 = builder1.build()
        query2 = builder2.build()

        assert "WHERE" not in query1.sql
        assert "WHERE" in query2.sql

    def test_query_metadata(self):
        """Test query metadata (tables, columns)."""
        query = (QueryBuilder()
            .select("id", "name")
            .from_table("sales")
            .join("customers", "sales.customer_id = customers.id")
            .build())

        assert "sales" in query.tables
        assert "customers" in query.tables


class TestDialects:
    """Tests for SQL dialects."""

    def test_postgresql_limit(self):
        """Test PostgreSQL LIMIT syntax."""
        query = (QueryBuilder(dialect="postgresql")
            .select("*")
            .from_table("sales")
            .limit(10)
            .offset(5)
            .build())

        assert "LIMIT 10 OFFSET 5" in query.sql

    def test_snowflake_limit(self):
        """Test Snowflake LIMIT syntax."""
        query = (QueryBuilder(dialect="snowflake")
            .select("*")
            .from_table("sales")
            .limit(10)
            .build())

        assert "LIMIT 10" in query.sql

    def test_tsql_top(self):
        """Test T-SQL TOP syntax."""
        query = (QueryBuilder(dialect="tsql")
            .select("*")
            .from_table("sales")
            .limit(10)
            .build())

        assert "TOP (10)" in query.sql

    def test_tsql_offset_fetch(self):
        """Test T-SQL OFFSET/FETCH syntax."""
        query = (QueryBuilder(dialect="tsql")
            .select("*")
            .from_table("sales")
            .order_by("id")
            .limit(10)
            .offset(5)
            .build())

        assert "OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY" in query.sql

    def test_spark_limit(self):
        """Test Spark SQL LIMIT syntax."""
        query = (QueryBuilder(dialect="spark")
            .select("*")
            .from_table("sales")
            .limit(10)
            .build())

        assert "LIMIT 10" in query.sql

    def test_dialect_date_trunc(self):
        """Test date truncation across dialects."""
        pg = PostgreSQLDialect()
        sf = SnowflakeDialect()

        pg_result = pg.format_date_trunc("order_date", "month")
        sf_result = sf.format_date_trunc("order_date", "month")

        assert "DATE_TRUNC" in pg_result
        assert "DATE_TRUNC" in sf_result

    def test_dialect_identifier_quoting(self):
        """Test identifier quoting across dialects."""
        pg = PostgreSQLDialect()
        tsql = TSQLDialect()
        spark = SparkDialect()

        assert pg.quote_identifier("column") == '"column"'
        assert tsql.quote_identifier("column") == "[column]"
        assert spark.quote_identifier("column") == "`column`"


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_select_function(self):
        """Test select() convenience function."""
        query = (select("id", "name")
            .from_table("sales")
            .build())

        assert "SELECT id, name" in query.sql

    def test_from_table_function(self):
        """Test from_table() convenience function."""
        query = (from_table("sales")
            .select("*")
            .build())

        assert "FROM sales" in query.sql


class TestQueryToDict:
    """Tests for Query serialization."""

    def test_to_dict(self):
        """Test Query to_dict method."""
        query = (QueryBuilder()
            .select("id", "name")
            .from_table("sales")
            .param("limit", 10)
            .build())

        result = query.to_dict()

        assert "sql" in result
        assert "dialect" in result
        assert "parameters" in result
        assert "columns" in result
        assert "tables" in result
