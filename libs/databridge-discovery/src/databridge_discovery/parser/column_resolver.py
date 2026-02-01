"""
Column Resolver for tracking column lineage through SQL queries.

This module resolves column references across joins, subqueries, and CTEs
to build complete column lineage for data governance and impact analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

from databridge_discovery.models.parsed_query import ParsedColumn, ParsedQuery


@dataclass
class ColumnReference:
    """Represents a reference to a column in a specific context."""

    column_name: str
    table_name: str | None = None
    schema_name: str | None = None
    database_name: str | None = None
    alias: str | None = None
    is_derived: bool = False
    expression: str | None = None

    @property
    def full_name(self) -> str:
        """Return fully qualified column name."""
        parts = [p for p in [self.database_name, self.schema_name, self.table_name, self.column_name] if p]
        return ".".join(parts)

    @property
    def key(self) -> str:
        """Return a unique key for this column reference."""
        table = self.table_name or "_"
        return f"{table}.{self.column_name}"


@dataclass
class ColumnLineage:
    """Represents the lineage of a single output column."""

    output_column: ColumnReference
    source_columns: list[ColumnReference] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)
    is_aggregated: bool = False
    aggregation_function: str | None = None
    is_case_derived: bool = False
    case_inputs: list[ColumnReference] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "output": self.output_column.full_name,
            "sources": [c.full_name for c in self.source_columns],
            "transformations": self.transformations,
            "is_aggregated": self.is_aggregated,
            "aggregation": self.aggregation_function,
            "is_case": self.is_case_derived,
        }


@dataclass
class TableScope:
    """Represents tables available in a specific scope."""

    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    parent_scope: TableScope | None = None

    def add_table(
        self,
        name: str,
        alias: str | None = None,
        columns: list[str] | None = None,
        schema: str | None = None,
        database: str | None = None,
    ) -> None:
        """Add a table to the scope."""
        key = alias or name
        self.tables[key] = {
            "name": name,
            "alias": alias,
            "columns": columns or [],
            "schema": schema,
            "database": database,
        }

    def resolve_table(self, ref: str) -> dict[str, Any] | None:
        """Resolve a table reference in this scope or parent scopes."""
        if ref in self.tables:
            return self.tables[ref]
        if self.parent_scope:
            return self.parent_scope.resolve_table(ref)
        return None

    def get_all_tables(self) -> dict[str, dict[str, Any]]:
        """Get all tables including parent scope."""
        result = {}
        if self.parent_scope:
            result.update(self.parent_scope.get_all_tables())
        result.update(self.tables)
        return result


class ColumnResolver:
    """
    Resolves column references and builds lineage through SQL queries.

    This class tracks how columns flow through joins, subqueries, CTEs,
    and transformations to build complete data lineage.

    Example:
        resolver = ColumnResolver()
        lineage = resolver.resolve(parsed_query)
        for col_lineage in lineage:
            print(f"{col_lineage.output_column} <- {col_lineage.source_columns}")
    """

    def __init__(self, dialect: str = "snowflake"):
        """
        Initialize the column resolver.

        Args:
            dialect: SQL dialect for parsing
        """
        self.dialect = dialect

    def resolve(self, parsed_query: ParsedQuery) -> list[ColumnLineage]:
        """
        Resolve column lineage for all output columns in a query.

        Args:
            parsed_query: Parsed query to analyze

        Returns:
            List of ColumnLineage objects for each output column
        """
        # Re-parse to get fresh AST
        try:
            statements = sqlglot.parse(parsed_query.sql, dialect=self.dialect)
            if not statements:
                return []
            ast = statements[0]
        except Exception:
            return []

        # Build table scope
        scope = self._build_table_scope(ast, parsed_query)

        # Resolve each output column
        lineages: list[ColumnLineage] = []
        for column in parsed_query.columns:
            lineage = self._resolve_column(column, ast, scope)
            lineages.append(lineage)

        return lineages

    def resolve_from_sql(self, sql: str) -> list[ColumnLineage]:
        """
        Resolve column lineage directly from SQL.

        Args:
            sql: SQL query to analyze

        Returns:
            List of ColumnLineage objects
        """
        from databridge_discovery.parser.sql_parser import SQLParser

        parser = SQLParser(dialect=self.dialect)
        parsed = parser.parse(sql)
        return self.resolve(parsed)

    def _build_table_scope(self, ast: exp.Expression, parsed_query: ParsedQuery) -> TableScope:
        """Build the table scope for column resolution."""
        scope = TableScope()

        # Add CTEs to scope first
        for cte_name, cte_sql in parsed_query.ctes.items():
            # Parse CTE to get its columns
            cte_columns = self._get_cte_columns(cte_sql)
            scope.add_table(name=cte_name, columns=cte_columns)

        # Add regular tables
        for table in parsed_query.tables:
            if table.is_cte:
                continue  # Already added

            scope.add_table(
                name=table.name,
                alias=table.alias,
                schema=table.schema_name,
                database=table.database,
            )

        return scope

    def _get_cte_columns(self, cte_sql: str) -> list[str]:
        """Get column names from a CTE definition."""
        try:
            statements = sqlglot.parse(cte_sql, dialect=self.dialect)
            if not statements:
                return []

            ast = statements[0]
            columns = []

            # Get SELECT expressions
            if isinstance(ast, exp.Select):
                for expr in ast.expressions:
                    if isinstance(expr, exp.Alias):
                        columns.append(expr.alias)
                    elif isinstance(expr, exp.Column):
                        columns.append(expr.name)
                    else:
                        # Use expression SQL as name
                        columns.append(expr.sql(dialect=self.dialect)[:30])

            return columns

        except Exception:
            return []

    def _resolve_column(
        self, column: ParsedColumn, ast: exp.Expression, scope: TableScope
    ) -> ColumnLineage:
        """Resolve lineage for a single output column."""
        output_ref = ColumnReference(
            column_name=column.name,
            table_name=None,  # Output column
            alias=column.name,
            is_derived=column.is_derived,
            expression=column.expression,
        )

        lineage = ColumnLineage(
            output_column=output_ref,
            is_aggregated=column.aggregation is not None,
            aggregation_function=column.aggregation.value if column.aggregation else None,
            is_case_derived=column.is_case_statement,
        )

        # If simple column reference
        if not column.is_derived and column.source_name:
            source_ref = ColumnReference(
                column_name=column.source_name,
                table_name=column.table_ref,
            )
            lineage.source_columns.append(source_ref)
            return lineage

        # For derived columns, parse the expression
        if column.expression:
            sources = self._extract_source_columns(column.expression, scope)
            lineage.source_columns.extend(sources)

            # Track transformations
            transformations = self._identify_transformations(column.expression)
            lineage.transformations.extend(transformations)

        # For CASE statements, extract input columns
        if column.is_case_statement and column.expression:
            case_inputs = self._extract_case_inputs(column.expression, scope)
            lineage.case_inputs.extend(case_inputs)

        return lineage

    def _extract_source_columns(
        self, expression: str, scope: TableScope
    ) -> list[ColumnReference]:
        """Extract source column references from an expression."""
        sources: list[ColumnReference] = []

        try:
            # Parse the expression
            parsed = sqlglot.parse(f"SELECT {expression}", dialect=self.dialect)
            if not parsed:
                return sources

            ast = parsed[0]

            # Find all column references
            for col in ast.find_all(exp.Column):
                ref = ColumnReference(
                    column_name=col.name,
                    table_name=col.table if col.table else None,
                )

                # Try to resolve table from scope
                if ref.table_name:
                    table_info = scope.resolve_table(ref.table_name)
                    if table_info:
                        ref.schema_name = table_info.get("schema")
                        ref.database_name = table_info.get("database")

                sources.append(ref)

        except Exception:
            pass

        return sources

    def _extract_case_inputs(
        self, expression: str, scope: TableScope
    ) -> list[ColumnReference]:
        """Extract input columns from CASE statement conditions."""
        inputs: list[ColumnReference] = []

        try:
            parsed = sqlglot.parse(f"SELECT {expression}", dialect=self.dialect)
            if not parsed:
                return inputs

            ast = parsed[0]

            # Find CASE expressions
            for case_expr in ast.find_all(exp.Case):
                # Get columns from WHEN conditions
                for when in case_expr.args.get("ifs", []):
                    if hasattr(when, "this"):
                        for col in when.this.find_all(exp.Column):
                            ref = ColumnReference(
                                column_name=col.name,
                                table_name=col.table if col.table else None,
                            )
                            if ref not in inputs:
                                inputs.append(ref)

        except Exception:
            pass

        return inputs

    def _identify_transformations(self, expression: str) -> list[str]:
        """Identify transformations applied in an expression."""
        transformations: list[str] = []

        try:
            parsed = sqlglot.parse(f"SELECT {expression}", dialect=self.dialect)
            if not parsed:
                return transformations

            ast = parsed[0]

            # Check for various transformation types
            if ast.find(exp.Case):
                transformations.append("CASE")

            if ast.find(exp.Cast):
                transformations.append("CAST")

            if ast.find(exp.Coalesce):
                transformations.append("COALESCE")

            if ast.find(exp.If) or ast.find(exp.IIf):
                transformations.append("IF")

            # Check for string functions
            string_funcs = ("concat", "substring", "replace", "trim", "upper", "lower")
            for func in ast.find_all(exp.Func):
                func_name = type(func).__name__.lower()
                if func_name in string_funcs:
                    transformations.append(func_name.upper())

            # Check for date functions
            date_funcs = ("dateadd", "datediff", "date_trunc", "to_date", "date_from_parts")
            for func in ast.find_all(exp.Func):
                func_name = type(func).__name__.lower()
                if func_name in date_funcs:
                    transformations.append(func_name.upper())

            # Check for math operations
            if ast.find(exp.Add):
                transformations.append("ADD")
            if ast.find(exp.Sub):
                transformations.append("SUBTRACT")
            if ast.find(exp.Mul):
                transformations.append("MULTIPLY")
            if ast.find(exp.Div):
                transformations.append("DIVIDE")

            # Check for aggregations
            agg_funcs = ("sum", "avg", "count", "min", "max")
            for func in ast.find_all(exp.Func):
                func_name = type(func).__name__.lower()
                if func_name in agg_funcs:
                    transformations.append(func_name.upper())

            # Round
            for func in ast.find_all(exp.Func):
                if type(func).__name__.lower() == "round":
                    transformations.append("ROUND")

        except Exception:
            pass

        return list(set(transformations))  # Remove duplicates

    def get_column_dependencies(
        self, parsed_query: ParsedQuery, column_name: str
    ) -> dict[str, Any]:
        """
        Get all dependencies for a specific output column.

        Args:
            parsed_query: Parsed query
            column_name: Name of the output column

        Returns:
            Dictionary with dependency information
        """
        lineages = self.resolve(parsed_query)

        for lineage in lineages:
            if lineage.output_column.column_name == column_name:
                return {
                    "column": column_name,
                    "sources": [s.full_name for s in lineage.source_columns],
                    "tables": list(set(s.table_name for s in lineage.source_columns if s.table_name)),
                    "transformations": lineage.transformations,
                    "is_aggregated": lineage.is_aggregated,
                    "is_case": lineage.is_case_derived,
                }

        return {"column": column_name, "sources": [], "tables": [], "transformations": []}

    def get_table_column_map(self, parsed_query: ParsedQuery) -> dict[str, list[str]]:
        """
        Get a map of tables to the columns used from each.

        Args:
            parsed_query: Parsed query

        Returns:
            Dictionary mapping table name to list of columns
        """
        lineages = self.resolve(parsed_query)

        table_columns: dict[str, set[str]] = {}

        for lineage in lineages:
            for source in lineage.source_columns:
                table = source.table_name or "_unknown"
                if table not in table_columns:
                    table_columns[table] = set()
                table_columns[table].add(source.column_name)

        return {k: sorted(v) for k, v in table_columns.items()}

    def build_lineage_graph(
        self, parsed_query: ParsedQuery
    ) -> dict[str, Any]:
        """
        Build a graph representation of column lineage.

        Args:
            parsed_query: Parsed query

        Returns:
            Graph structure with nodes and edges
        """
        lineages = self.resolve(parsed_query)

        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        node_ids: dict[str, str] = {}

        # Add output columns as nodes
        for lineage in lineages:
            out_id = f"out_{lineage.output_column.column_name}"
            node_ids[lineage.output_column.full_name] = out_id
            nodes.append({
                "id": out_id,
                "label": lineage.output_column.column_name,
                "type": "output",
                "is_aggregated": lineage.is_aggregated,
                "is_case": lineage.is_case_derived,
            })

            # Add source columns as nodes
            for idx, source in enumerate(lineage.source_columns):
                src_key = source.full_name
                if src_key not in node_ids:
                    src_id = f"src_{source.table_name or 'unknown'}_{source.column_name}"
                    node_ids[src_key] = src_id
                    nodes.append({
                        "id": src_id,
                        "label": source.column_name,
                        "table": source.table_name,
                        "type": "source",
                    })

                # Add edge
                edges.append({
                    "source": node_ids[src_key],
                    "target": out_id,
                    "transformations": lineage.transformations,
                })

        return {
            "nodes": nodes,
            "edges": edges,
            "summary": {
                "output_columns": len([n for n in nodes if n["type"] == "output"]),
                "source_columns": len([n for n in nodes if n["type"] == "source"]),
                "total_edges": len(edges),
            },
        }
