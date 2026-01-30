"""
MCP Tools for Query Building and NL-to-SQL in DataBridge AI V4.

Provides 10 tools for SQL query construction and natural language translation.
"""

from typing import Optional, List, Dict, Any

from fastmcp import FastMCP

from ...query.builder import QueryBuilder
from ...query.dialects import get_dialect
from ...nlp import NLToSQLEngine, IntentClassifier, EntityExtractor, CatalogEntry


def register_query_tools(mcp: FastMCP) -> None:
    """Register all query MCP tools."""

    # Shared state
    _catalog: List[Dict[str, Any]] = []
    _nl_engine: Optional[NLToSQLEngine] = None
    _query_history: List[Dict[str, Any]] = []

    def _get_nl_engine() -> NLToSQLEngine:
        """Get or create the NL-to-SQL engine."""
        nonlocal _nl_engine
        if _nl_engine is None:
            _nl_engine = NLToSQLEngine()
            if _catalog:
                _nl_engine.load_catalog(_catalog)
        return _nl_engine

    @mcp.tool()
    def build_query(
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        order_direction: str = "ASC",
        limit: Optional[int] = None,
        dialect: str = "postgresql",
    ) -> Dict[str, Any]:
        """
        Build a SQL query using a fluent interface.

        Args:
            table: Main table name.
            columns: List of columns to select (None for all).
            where: List of WHERE conditions.
            group_by: List of columns to group by.
            order_by: Column to order by.
            order_direction: Sort direction (ASC or DESC).
            limit: Maximum rows to return.
            dialect: SQL dialect (postgresql, snowflake, tsql, spark).

        Returns:
            Dictionary with the generated SQL query.
        """
        builder = QueryBuilder(dialect=dialect)
        builder.from_table(table)

        if columns:
            builder.select(*columns)
        else:
            builder.select("*")

        if where:
            for condition in where:
                builder.where(condition)

        if group_by:
            builder.group_by(*group_by)

        if order_by:
            builder.order_by(order_by, order_direction)

        if limit:
            builder.limit(limit)

        query = builder.build()

        # Add to history
        _query_history.append({
            "sql": query.sql,
            "dialect": query.dialect,
            "type": "build_query",
        })

        return query.to_dict()

    @mcp.tool()
    def build_aggregation_query(
        table: str,
        metric_column: str,
        aggregation: str = "SUM",
        group_by: Optional[List[str]] = None,
        where: Optional[List[str]] = None,
        having: Optional[str] = None,
        order_by_metric: bool = True,
        order_direction: str = "DESC",
        limit: Optional[int] = None,
        dialect: str = "postgresql",
    ) -> Dict[str, Any]:
        """
        Build an aggregation query with grouping.

        Args:
            table: Main table name.
            metric_column: Column to aggregate.
            aggregation: Aggregation function (SUM, AVG, COUNT, MIN, MAX).
            group_by: Columns to group by.
            where: List of WHERE conditions.
            having: HAVING condition for aggregate filtering.
            order_by_metric: Whether to order by the aggregated metric.
            order_direction: Sort direction.
            limit: Maximum rows to return.
            dialect: SQL dialect.

        Returns:
            Dictionary with the generated SQL query.
        """
        builder = QueryBuilder(dialect=dialect)
        builder.from_table(table)

        # Add group by columns to select
        if group_by:
            for col in group_by:
                builder.select(col)
            builder.group_by(*group_by)

        # Add aggregation
        alias = f"{aggregation.lower()}_{metric_column}"
        builder.select_aggregate(aggregation, metric_column, alias)

        # Add WHERE conditions
        if where:
            for condition in where:
                builder.where(condition)

        # Add HAVING
        if having:
            builder.having(having)

        # Add ORDER BY
        if order_by_metric:
            builder.order_by(alias, order_direction)

        if limit:
            builder.limit(limit)

        query = builder.build()

        _query_history.append({
            "sql": query.sql,
            "dialect": query.dialect,
            "type": "aggregation_query",
        })

        return query.to_dict()

    @mcp.tool()
    def build_join_query(
        main_table: str,
        joins: List[Dict[str, str]],
        columns: Optional[List[str]] = None,
        where: Optional[List[str]] = None,
        dialect: str = "postgresql",
    ) -> Dict[str, Any]:
        """
        Build a query with JOINs.

        Args:
            main_table: Main table name.
            joins: List of join specifications, each with 'table', 'on', and optionally 'type'.
            columns: Columns to select.
            where: WHERE conditions.
            dialect: SQL dialect.

        Returns:
            Dictionary with the generated SQL query.

        Example joins:
            [{"table": "orders", "on": "customers.id = orders.customer_id", "type": "LEFT"}]
        """
        builder = QueryBuilder(dialect=dialect)
        builder.from_table(main_table)

        for join_spec in joins:
            join_type = join_spec.get("type", "INNER")
            builder.join(
                table=join_spec["table"],
                on=join_spec["on"],
                join_type=join_type,
            )

        if columns:
            builder.select(*columns)
        else:
            builder.select("*")

        if where:
            for condition in where:
                builder.where(condition)

        query = builder.build()

        return query.to_dict()

    @mcp.tool()
    def load_metadata_catalog(
        catalog_entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Load metadata catalog for NL-to-SQL entity extraction.

        Args:
            catalog_entries: List of catalog entries with fields:
                - name: Column or table name
                - entity_type: 'table', 'column', 'metric', 'dimension'
                - table: Parent table name (for columns)
                - data_type: Data type (string, integer, date, etc.)
                - aliases: Alternative names
                - is_metric: Whether it's a numeric measure
                - is_dimension: Whether it's a categorical dimension

        Returns:
            Dictionary confirming catalog loaded.
        """
        nonlocal _catalog, _nl_engine
        _catalog = catalog_entries
        _nl_engine = None  # Reset engine to reload catalog

        return {
            "success": True,
            "entries_loaded": len(catalog_entries),
            "tables": len([e for e in catalog_entries if e.get("entity_type") == "table"]),
            "columns": len([e for e in catalog_entries if e.get("entity_type") != "table"]),
        }

    @mcp.tool()
    def nl_to_sql(
        query: str,
        dialect: str = "postgresql",
        default_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Translate a natural language query to SQL.

        Args:
            query: Natural language query (e.g., "total sales by region this year").
            dialect: SQL dialect for output.
            default_table: Default table if not detected from query.

        Returns:
            Dictionary with generated SQL, intent, entities, and confidence.
        """
        engine = _get_nl_engine()
        engine.dialect = get_dialect(dialect)
        if default_table:
            engine.default_table = default_table

        result = engine.translate(query)

        if result.success and result.query:
            _query_history.append({
                "sql": result.query.sql,
                "dialect": result.query.dialect,
                "type": "nl_to_sql",
                "original_query": query,
            })

        return result.to_dict()

    @mcp.tool()
    def classify_query_intent(
        query: str,
    ) -> Dict[str, Any]:
        """
        Classify the intent of a natural language query.

        Args:
            query: Natural language query string.

        Returns:
            Dictionary with intent classification (aggregation, trend, ranking, etc.).
        """
        classifier = IntentClassifier()
        intent = classifier.classify(query)
        return intent.to_dict()

    @mcp.tool()
    def extract_query_entities(
        query: str,
    ) -> Dict[str, Any]:
        """
        Extract database entities (tables, columns) from a natural language query.

        Args:
            query: Natural language query string.

        Returns:
            Dictionary with extracted entities and their confidence scores.
        """
        engine = _get_nl_engine()
        entities = engine.entity_extractor.extract(query)

        return {
            "success": True,
            "entities": [e.to_dict() for e in entities],
            "tables": [e.name for e in entities if e.entity_type.value == "table"],
            "columns": [e.name for e in entities if e.entity_type.value != "table"],
        }

    @mcp.tool()
    def suggest_questions(
        context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Suggest sample analytical questions based on the loaded catalog.

        Args:
            context: Optional context to tailor suggestions.

        Returns:
            Dictionary with suggested questions.
        """
        engine = _get_nl_engine()
        suggestions = engine.suggest_questions(context)

        return {
            "success": True,
            "suggestions": suggestions,
        }

    @mcp.tool()
    def get_query_history(
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        Get recent query history.

        Args:
            limit: Maximum number of queries to return.

        Returns:
            Dictionary with recent queries.
        """
        return {
            "success": True,
            "total_queries": len(_query_history),
            "queries": _query_history[-limit:],
        }

    @mcp.tool()
    def explain_query(
        sql: str,
    ) -> Dict[str, Any]:
        """
        Explain what a SQL query does in plain language.

        Args:
            sql: SQL query string to explain.

        Returns:
            Dictionary with query explanation.
        """
        # Simple SQL parser to extract components
        sql_upper = sql.upper()
        explanation_parts = []

        # Detect query type
        if "SELECT" in sql_upper:
            explanation_parts.append("This is a SELECT query that retrieves data")

        # Detect tables
        if "FROM" in sql_upper:
            from_idx = sql_upper.find("FROM")
            # Simple extraction (would need proper parser for complex cases)
            explanation_parts.append("from the database")

        # Detect aggregations
        aggs = []
        for agg in ["SUM", "AVG", "COUNT", "MIN", "MAX"]:
            if agg + "(" in sql_upper:
                aggs.append(agg)
        if aggs:
            explanation_parts.append(f"calculating {', '.join(aggs)} aggregations")

        # Detect grouping
        if "GROUP BY" in sql_upper:
            explanation_parts.append("grouped by one or more dimensions")

        # Detect filtering
        if "WHERE" in sql_upper:
            explanation_parts.append("with filtering conditions")

        # Detect ordering
        if "ORDER BY" in sql_upper:
            direction = "descending" if "DESC" in sql_upper else "ascending"
            explanation_parts.append(f"sorted in {direction} order")

        # Detect limits
        if "LIMIT" in sql_upper or "TOP" in sql_upper:
            explanation_parts.append("limited to a subset of rows")

        return {
            "success": True,
            "sql": sql,
            "explanation": " ".join(explanation_parts) + "." if explanation_parts else "Unable to explain query.",
            "detected_features": {
                "has_aggregation": len(aggs) > 0,
                "has_grouping": "GROUP BY" in sql_upper,
                "has_filtering": "WHERE" in sql_upper,
                "has_ordering": "ORDER BY" in sql_upper,
                "has_limit": "LIMIT" in sql_upper or "TOP" in sql_upper,
            },
        }
