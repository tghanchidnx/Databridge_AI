"""
SQL Parser using sqlglot for multi-dialect AST analysis.

This module provides the main SQL parsing functionality for the
Discovery Engine, supporting multiple SQL dialects and extracting
structured information from SQL queries.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from databridge_discovery.models.parsed_query import (
    AggregationType,
    ColumnDataType,
    JoinType,
    ParsedColumn,
    ParsedJoin,
    ParsedQuery,
    ParsedTable,
    QueryMetrics,
)


# Mapping of sqlglot dialect names to our standard names
DIALECT_MAP = {
    "snowflake": "snowflake",
    "postgres": "postgres",
    "postgresql": "postgres",
    "tsql": "tsql",
    "mssql": "tsql",
    "sqlserver": "tsql",
    "mysql": "mysql",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "spark": "spark",
    "redshift": "redshift",
    "duckdb": "duckdb",
    "sqlite": "sqlite",
}

# Aggregation function mapping
AGG_FUNCTION_MAP = {
    "sum": AggregationType.SUM,
    "avg": AggregationType.AVG,
    "count": AggregationType.COUNT,
    "min": AggregationType.MIN,
    "max": AggregationType.MAX,
    "first": AggregationType.FIRST,
    "last": AggregationType.LAST,
    "median": AggregationType.MEDIAN,
    "stddev": AggregationType.STDDEV,
    "variance": AggregationType.VARIANCE,
    "listagg": AggregationType.LISTAGG,
    "array_agg": AggregationType.ARRAY_AGG,
    "count_distinct": AggregationType.COUNT_DISTINCT,
}


class SQLParser:
    """
    SQL Parser that uses sqlglot for multi-dialect SQL analysis.

    Parses SQL statements into structured ParsedQuery objects containing
    tables, columns, joins, and query metrics.

    Example:
        parser = SQLParser(dialect="snowflake")
        result = parser.parse("SELECT * FROM users")
        print(result.tables)  # [ParsedTable(name="users", ...)]
    """

    def __init__(self, dialect: str = "snowflake"):
        """
        Initialize the SQL parser.

        Args:
            dialect: SQL dialect to use for parsing. Supported dialects:
                     snowflake, postgres, tsql, mysql, bigquery, databricks,
                     redshift, duckdb, sqlite
        """
        self.dialect = DIALECT_MAP.get(dialect.lower(), dialect.lower())

    def parse(self, sql: str) -> ParsedQuery:
        """
        Parse a SQL statement into a structured ParsedQuery object.

        Args:
            sql: SQL statement to parse

        Returns:
            ParsedQuery object containing parsed information

        Raises:
            ValueError: If SQL cannot be parsed
        """
        start_time = time.time()
        errors: list[str] = []

        try:
            # Handle empty or whitespace-only SQL
            if not sql or not sql.strip():
                return ParsedQuery(
                    sql=sql,
                    dialect=self.dialect,
                    query_type="UNKNOWN",
                    parse_errors=["Empty or whitespace-only SQL"],
                    parse_time_ms=(time.time() - start_time) * 1000,
                )

            # Parse the SQL using sqlglot
            statements = sqlglot.parse(sql, dialect=self.dialect)

            if not statements or statements[0] is None:
                return ParsedQuery(
                    sql=sql,
                    dialect=self.dialect,
                    query_type="UNKNOWN",
                    parse_errors=["No SQL statements found"],
                    parse_time_ms=(time.time() - start_time) * 1000,
                )

            # Take the first statement
            ast = statements[0]

            # Determine query type
            query_type = self._get_query_type(ast)

            # Extract components
            tables = self._extract_tables(ast)
            columns = self._extract_columns(ast)
            joins = self._extract_joins(ast)
            ctes = self._extract_ctes(ast)

            # Extract clauses
            where_clause = self._extract_where(ast)
            group_by = self._extract_group_by(ast)
            having_clause = self._extract_having(ast)
            order_by = self._extract_order_by(ast)

            # Calculate metrics
            metrics = self._calculate_metrics(
                ast=ast,
                tables=tables,
                columns=columns,
                joins=joins,
                ctes=ctes,
                group_by=group_by,
            )

            # Build AST JSON for advanced use
            ast_json = self._ast_to_json(ast)

            parse_time = (time.time() - start_time) * 1000

            return ParsedQuery(
                sql=sql,
                dialect=self.dialect,
                query_type=query_type,
                tables=tables,
                columns=columns,
                joins=joins,
                ctes=ctes,
                where_clause=where_clause,
                group_by_columns=group_by,
                having_clause=having_clause,
                order_by_columns=order_by,
                metrics=metrics,
                ast_json=ast_json,
                parse_errors=errors,
                parse_time_ms=parse_time,
            )

        except ParseError as e:
            errors.append(f"Parse error: {str(e)}")
            parse_time = (time.time() - start_time) * 1000
            return ParsedQuery(
                sql=sql,
                dialect=self.dialect,
                query_type="UNKNOWN",
                parse_errors=errors,
                parse_time_ms=parse_time,
            )

    def parse_multiple(self, sql: str) -> list[ParsedQuery]:
        """
        Parse multiple SQL statements from a single string.

        Args:
            sql: SQL string containing one or more statements

        Returns:
            List of ParsedQuery objects
        """
        results = []
        try:
            statements = sqlglot.parse(sql, dialect=self.dialect)
            for stmt in statements:
                if stmt:
                    # Regenerate SQL for each statement
                    stmt_sql = stmt.sql(dialect=self.dialect)
                    result = self.parse(stmt_sql)
                    results.append(result)
        except ParseError as e:
            # Try splitting by semicolon and parsing individually
            parts = sql.split(";")
            for part in parts:
                part = part.strip()
                if part:
                    result = self.parse(part)
                    results.append(result)

        return results

    def _get_query_type(self, ast: exp.Expression) -> str:
        """Determine the type of SQL statement."""
        if isinstance(ast, exp.Select):
            return "SELECT"
        elif isinstance(ast, exp.Insert):
            return "INSERT"
        elif isinstance(ast, exp.Update):
            return "UPDATE"
        elif isinstance(ast, exp.Delete):
            return "DELETE"
        elif isinstance(ast, exp.Create):
            return "CREATE"
        elif isinstance(ast, exp.Drop):
            return "DROP"
        elif isinstance(ast, exp.Merge):
            return "MERGE"
        elif isinstance(ast, exp.Union):
            return "UNION"
        elif hasattr(exp, 'Alter') and isinstance(ast, exp.Alter):
            return "ALTER"
        else:
            return type(ast).__name__.upper()

    def _extract_tables(self, ast: exp.Expression) -> list[ParsedTable]:
        """Extract all table references from the AST."""
        tables: list[ParsedTable] = []
        seen = set()

        # Find all table references
        for table in ast.find_all(exp.Table):
            key = (table.db, table.catalog, table.name, table.alias)
            if key in seen:
                continue
            seen.add(key)

            parsed = ParsedTable(
                name=table.name,
                schema_name=table.db if table.db else None,
                database=table.catalog if table.catalog else None,
                alias=table.alias if table.alias else None,
            )
            tables.append(parsed)

        # Find subqueries
        for subquery in ast.find_all(exp.Subquery):
            alias = subquery.alias if subquery.alias else f"subquery_{len(tables)}"
            parsed = ParsedTable(
                name=alias,
                alias=alias,
                is_subquery=True,
                subquery_sql=subquery.sql(dialect=self.dialect),
            )
            tables.append(parsed)

        return tables

    def _extract_columns(self, ast: exp.Expression) -> list[ParsedColumn]:
        """Extract all columns from SELECT clause."""
        columns: list[ParsedColumn] = []

        # Get SELECT expressions
        if not isinstance(ast, exp.Select):
            # Handle UNION etc.
            select = ast.find(exp.Select)
            if not select:
                return columns
        else:
            select = ast

        # Get the select expressions
        for idx, expr in enumerate(select.expressions):
            column = self._parse_column_expression(expr, idx)
            columns.append(column)

        return columns

    def _parse_column_expression(self, expr: exp.Expression, position: int) -> ParsedColumn:
        """Parse a single column expression from SELECT."""
        # Get alias if present
        alias = None
        source_expr = expr
        if isinstance(expr, exp.Alias):
            alias = expr.alias
            source_expr = expr.this

        # Determine column name
        if alias:
            name = alias
        elif isinstance(source_expr, exp.Column):
            name = source_expr.name
        else:
            name = source_expr.sql(dialect=self.dialect)[:50]

        # Check for CASE statement
        is_case = bool(source_expr.find(exp.Case))
        case_id = None
        if is_case:
            # Generate deterministic ID from CASE SQL
            case_sql = source_expr.find(exp.Case).sql(dialect=self.dialect)
            case_id = hashlib.md5(case_sql.encode()).hexdigest()[:12]

        # Get table reference
        table_ref = None
        if isinstance(source_expr, exp.Column) and source_expr.table:
            table_ref = source_expr.table

        # Check for aggregation
        aggregation = self._detect_aggregation(source_expr)

        # Determine if derived
        is_derived = bool(
            aggregation is not None
            or is_case
            or isinstance(source_expr, (exp.Anonymous, exp.Func))
            or source_expr.find(exp.Binary) is not None
        )

        # Get expression SQL if derived
        expression = None
        if is_derived:
            expression = source_expr.sql(dialect=self.dialect)

        # Infer data type
        data_type = self._infer_data_type(source_expr)

        # Get source name (original column name before aliasing)
        source_name = None
        if isinstance(source_expr, exp.Column):
            source_name = source_expr.name

        return ParsedColumn(
            name=name,
            source_name=source_name,
            table_ref=table_ref,
            data_type=data_type,
            is_derived=is_derived,
            expression=expression,
            aggregation=aggregation,
            is_case_statement=is_case,
            case_statement_id=case_id,
            position=position,
        )

    def _detect_aggregation(self, expr: exp.Expression) -> AggregationType | None:
        """Detect if expression contains an aggregation function."""
        for func in expr.find_all(exp.Func):
            func_name = type(func).__name__.lower()

            # Map sqlglot function types to our aggregation types
            if func_name in ("sum",):
                return AggregationType.SUM
            elif func_name in ("avg", "average"):
                return AggregationType.AVG
            elif func_name == "count":
                # Check for COUNT(DISTINCT)
                if func.find(exp.Distinct):
                    return AggregationType.COUNT_DISTINCT
                return AggregationType.COUNT
            elif func_name in ("min",):
                return AggregationType.MIN
            elif func_name in ("max",):
                return AggregationType.MAX
            elif func_name in AGG_FUNCTION_MAP:
                return AGG_FUNCTION_MAP[func_name]

        return None

    def _infer_data_type(self, expr: exp.Expression) -> ColumnDataType:
        """Infer the data type of an expression."""
        # Check for literal types
        if isinstance(expr, exp.Literal):
            if expr.is_string:
                return ColumnDataType.STRING
            elif expr.is_int:
                return ColumnDataType.INTEGER
            elif expr.is_number:
                return ColumnDataType.FLOAT
            else:
                return ColumnDataType.UNKNOWN

        # Check for CASE - usually returns string based on THEN values
        if expr.find(exp.Case):
            return ColumnDataType.STRING

        # Check for date functions
        date_funcs = ("date", "to_date", "dateadd", "datediff", "date_trunc", "date_from_parts")
        for func in expr.find_all(exp.Func):
            if type(func).__name__.lower() in date_funcs:
                return ColumnDataType.DATE

        # Check for timestamp functions
        ts_funcs = ("timestamp", "to_timestamp", "current_timestamp", "now")
        for func in expr.find_all(exp.Func):
            if type(func).__name__.lower() in ts_funcs:
                return ColumnDataType.TIMESTAMP

        # Check for numeric operations
        if expr.find(exp.Mul) or expr.find(exp.Div):
            return ColumnDataType.DECIMAL
        if isinstance(expr, (exp.Sum, exp.Avg)):
            return ColumnDataType.DECIMAL
        if isinstance(expr, exp.Count):
            return ColumnDataType.INTEGER

        # Check for ROUND - implies decimal
        for func in expr.find_all(exp.Func):
            if type(func).__name__.lower() == "round":
                return ColumnDataType.DECIMAL

        return ColumnDataType.UNKNOWN

    def _extract_joins(self, ast: exp.Expression) -> list[ParsedJoin]:
        """Extract all JOIN relationships from the AST."""
        joins: list[ParsedJoin] = []

        for join in ast.find_all(exp.Join):
            join_type = self._get_join_type(join)

            # Get the joined table
            right_table = ""
            if join.this:
                if isinstance(join.this, exp.Table):
                    right_table = join.this.alias or join.this.name
                elif isinstance(join.this, exp.Subquery):
                    right_table = join.this.alias or "subquery"

            # Get join condition
            condition = ""
            left_table = ""
            left_column = ""
            right_column = ""
            additional_conditions: list[str] = []

            if join.args.get("on"):
                on_clause = join.args["on"]
                condition = on_clause.sql(dialect=self.dialect)

                # Try to extract simple equality condition
                eq_exprs = list(on_clause.find_all(exp.EQ))
                if eq_exprs:
                    first_eq = eq_exprs[0]
                    if isinstance(first_eq.left, exp.Column):
                        left_table = first_eq.left.table or ""
                        left_column = first_eq.left.name
                    if isinstance(first_eq.right, exp.Column):
                        if not right_table:
                            right_table = first_eq.right.table or ""
                        right_column = first_eq.right.name

                    # Additional conditions
                    if len(eq_exprs) > 1:
                        for eq in eq_exprs[1:]:
                            additional_conditions.append(eq.sql(dialect=self.dialect))

            parsed_join = ParsedJoin(
                join_type=join_type,
                left_table=left_table,
                right_table=right_table,
                left_column=left_column,
                right_column=right_column,
                condition=condition,
                is_complex=len(additional_conditions) > 0,
                additional_conditions=additional_conditions,
            )
            joins.append(parsed_join)

        return joins

    def _get_join_type(self, join: exp.Join) -> JoinType:
        """Get the type of a JOIN."""
        kind = join.kind or ""
        side = join.side or ""

        kind_lower = kind.lower() if kind else ""
        side_lower = side.lower() if side else ""

        if side_lower == "left":
            if kind_lower == "outer":
                return JoinType.LEFT_OUTER
            return JoinType.LEFT
        elif side_lower == "right":
            if kind_lower == "outer":
                return JoinType.RIGHT_OUTER
            return JoinType.RIGHT
        elif side_lower == "full":
            if kind_lower == "outer":
                return JoinType.FULL_OUTER
            return JoinType.FULL
        elif kind_lower == "cross":
            return JoinType.CROSS
        else:
            return JoinType.INNER

    def _extract_ctes(self, ast: exp.Expression) -> dict[str, str]:
        """Extract Common Table Expressions (CTEs)."""
        ctes: dict[str, str] = {}

        # Find WITH clause
        with_clause = ast.find(exp.With)
        if with_clause:
            for cte in with_clause.expressions:
                if isinstance(cte, exp.CTE):
                    name = cte.alias
                    sql = cte.this.sql(dialect=self.dialect)
                    ctes[name] = sql

        return ctes

    def _extract_where(self, ast: exp.Expression) -> str | None:
        """Extract WHERE clause."""
        where = ast.find(exp.Where)
        if where:
            return where.this.sql(dialect=self.dialect)
        return None

    def _extract_group_by(self, ast: exp.Expression) -> list[str]:
        """Extract GROUP BY columns."""
        columns: list[str] = []
        group = ast.find(exp.Group)
        if group:
            for expr in group.expressions:
                columns.append(expr.sql(dialect=self.dialect))
        return columns

    def _extract_having(self, ast: exp.Expression) -> str | None:
        """Extract HAVING clause."""
        having = ast.find(exp.Having)
        if having:
            return having.this.sql(dialect=self.dialect)
        return None

    def _extract_order_by(self, ast: exp.Expression) -> list[str]:
        """Extract ORDER BY columns."""
        columns: list[str] = []
        order = ast.find(exp.Order)
        if order:
            for expr in order.expressions:
                columns.append(expr.sql(dialect=self.dialect))
        return columns

    def _calculate_metrics(
        self,
        ast: exp.Expression,
        tables: list[ParsedTable],
        columns: list[ParsedColumn],
        joins: list[ParsedJoin],
        ctes: dict[str, str],
        group_by: list[str],
    ) -> QueryMetrics:
        """Calculate query complexity metrics."""
        # Count CASE statements
        case_count = len(list(ast.find_all(exp.Case)))

        # Count subqueries
        subquery_count = len([t for t in tables if t.is_subquery])

        # Count aggregations
        agg_count = len([c for c in columns if c.aggregation])

        # Check for various clauses
        has_group_by = len(group_by) > 0
        has_having = ast.find(exp.Having) is not None
        has_order_by = ast.find(exp.Order) is not None
        has_limit = ast.find(exp.Limit) is not None
        has_union = ast.find(exp.Union) is not None

        # Check for window functions
        has_window = any(ast.find_all(exp.Window))

        # Calculate nesting depth
        nesting_depth = self._calculate_nesting_depth(ast)

        # Estimate complexity
        complexity = self._estimate_complexity(
            table_count=len(tables),
            join_count=len(joins),
            case_count=case_count,
            subquery_count=subquery_count,
            cte_count=len(ctes),
            nesting_depth=nesting_depth,
        )

        return QueryMetrics(
            table_count=len(tables),
            join_count=len(joins),
            column_count=len(columns),
            case_statement_count=case_count,
            subquery_count=subquery_count,
            cte_count=len(ctes),
            aggregation_count=agg_count,
            has_group_by=has_group_by,
            has_having=has_having,
            has_order_by=has_order_by,
            has_limit=has_limit,
            has_union=has_union,
            has_window_functions=has_window,
            estimated_complexity=complexity,
            nesting_depth=nesting_depth,
        )

    def _calculate_nesting_depth(self, ast: exp.Expression, current_depth: int = 0) -> int:
        """Calculate maximum subquery nesting depth."""
        max_depth = current_depth

        for subquery in ast.find_all(exp.Subquery):
            depth = self._calculate_nesting_depth(subquery.this, current_depth + 1)
            max_depth = max(max_depth, depth)

        return max_depth

    def _estimate_complexity(
        self,
        table_count: int,
        join_count: int,
        case_count: int,
        subquery_count: int,
        cte_count: int,
        nesting_depth: int,
    ) -> str:
        """Estimate overall query complexity."""
        # Simple scoring
        score = 0
        score += table_count * 1
        score += join_count * 2
        score += case_count * 2
        score += subquery_count * 3
        score += cte_count * 2
        score += nesting_depth * 3

        if score <= 5:
            return "simple"
        elif score <= 15:
            return "moderate"
        else:
            return "complex"

    def _ast_to_json(self, ast: exp.Expression) -> dict[str, Any]:
        """Convert AST to JSON-serializable dict."""
        try:
            return {
                "type": type(ast).__name__,
                "sql": ast.sql(dialect=self.dialect),
                "depth": ast.depth,
            }
        except Exception:
            return {"type": type(ast).__name__, "error": "Could not serialize"}

    def validate_sql(self, sql: str) -> tuple[bool, list[str]]:
        """
        Validate SQL syntax without full parsing.

        Args:
            sql: SQL to validate

        Returns:
            Tuple of (is_valid, list of errors)
        """
        try:
            sqlglot.parse(sql, dialect=self.dialect)
            return True, []
        except ParseError as e:
            return False, [str(e)]

    def transpile(self, sql: str, target_dialect: str) -> str:
        """
        Transpile SQL from current dialect to target dialect.

        Args:
            sql: SQL to transpile
            target_dialect: Target dialect name

        Returns:
            Transpiled SQL string
        """
        target = DIALECT_MAP.get(target_dialect.lower(), target_dialect.lower())
        return sqlglot.transpile(sql, read=self.dialect, write=target)[0]
