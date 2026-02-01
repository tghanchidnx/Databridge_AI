"""
Pydantic models for parsed SQL queries.

These models represent the structured output of SQL parsing,
including tables, columns, joins, and query metrics.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ColumnDataType(str, Enum):
    """Inferred or declared column data types."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"
    VARIANT = "variant"
    UNKNOWN = "unknown"


class JoinType(str, Enum):
    """SQL join types."""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    LEFT_OUTER = "LEFT OUTER"
    RIGHT_OUTER = "RIGHT OUTER"
    FULL_OUTER = "FULL OUTER"


class AggregationType(str, Enum):
    """Aggregation function types."""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    FIRST = "FIRST"
    LAST = "LAST"
    MEDIAN = "MEDIAN"
    STDDEV = "STDDEV"
    VARIANCE = "VARIANCE"
    LISTAGG = "LISTAGG"
    ARRAY_AGG = "ARRAY_AGG"


class ParsedColumn(BaseModel):
    """Represents a column in a parsed SQL query."""

    name: str = Field(..., description="Column name or alias")
    source_name: str | None = Field(None, description="Original column name if aliased")
    table_ref: str | None = Field(None, description="Table or alias this column comes from")
    data_type: ColumnDataType = Field(default=ColumnDataType.UNKNOWN, description="Inferred data type")
    is_derived: bool = Field(default=False, description="True if column is calculated/derived")
    expression: str | None = Field(None, description="SQL expression if derived")
    aggregation: AggregationType | None = Field(None, description="Aggregation function if applied")
    is_case_statement: bool = Field(default=False, description="True if column contains CASE")
    case_statement_id: str | None = Field(None, description="ID linking to extracted CASE statement")
    position: int = Field(default=0, description="Position in SELECT clause")

    model_config = {"extra": "allow"}


class ParsedTable(BaseModel):
    """Represents a table reference in a parsed SQL query."""

    name: str = Field(..., description="Table name")
    schema_name: str | None = Field(None, description="Schema name")
    database: str | None = Field(None, description="Database name")
    alias: str | None = Field(None, description="Table alias")
    is_subquery: bool = Field(default=False, description="True if this is a subquery")
    subquery_sql: str | None = Field(None, description="Subquery SQL if applicable")
    is_cte: bool = Field(default=False, description="True if this references a CTE")
    cte_name: str | None = Field(None, description="CTE name if applicable")

    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        parts = [p for p in [self.database, self.schema_name, self.name] if p]
        return ".".join(parts)

    @property
    def reference_name(self) -> str:
        """Return the name used to reference this table (alias or name)."""
        return self.alias or self.name

    model_config = {"extra": "allow"}


class ParsedJoin(BaseModel):
    """Represents a JOIN in a parsed SQL query."""

    join_type: JoinType = Field(..., description="Type of join")
    left_table: str = Field(..., description="Left table or alias")
    right_table: str = Field(..., description="Right table or alias")
    left_column: str = Field(..., description="Left join column")
    right_column: str = Field(..., description="Right join column")
    condition: str = Field(..., description="Full join condition SQL")
    is_complex: bool = Field(default=False, description="True if join has multiple conditions")
    additional_conditions: list[str] = Field(default_factory=list, description="Additional join conditions")

    model_config = {"extra": "allow"}


class QueryMetrics(BaseModel):
    """Metrics about a parsed SQL query."""

    table_count: int = Field(default=0, description="Number of tables referenced")
    join_count: int = Field(default=0, description="Number of joins")
    column_count: int = Field(default=0, description="Number of columns in SELECT")
    case_statement_count: int = Field(default=0, description="Number of CASE statements")
    subquery_count: int = Field(default=0, description="Number of subqueries")
    cte_count: int = Field(default=0, description="Number of CTEs")
    aggregation_count: int = Field(default=0, description="Number of aggregations")
    has_group_by: bool = Field(default=False, description="True if query has GROUP BY")
    has_having: bool = Field(default=False, description="True if query has HAVING")
    has_order_by: bool = Field(default=False, description="True if query has ORDER BY")
    has_limit: bool = Field(default=False, description="True if query has LIMIT/TOP")
    has_union: bool = Field(default=False, description="True if query has UNION")
    has_window_functions: bool = Field(default=False, description="True if query has window functions")
    estimated_complexity: str = Field(default="simple", description="Complexity: simple, moderate, complex")
    nesting_depth: int = Field(default=0, description="Maximum subquery nesting depth")

    model_config = {"extra": "allow"}


class ParsedQuery(BaseModel):
    """
    Complete parsed representation of a SQL query.

    This is the main output from SQLParser.parse() containing
    all extracted information from the SQL statement.
    """

    sql: str = Field(..., description="Original SQL statement")
    dialect: str = Field(default="snowflake", description="SQL dialect used for parsing")
    query_type: str = Field(default="SELECT", description="Query type: SELECT, INSERT, etc.")

    # Main components
    tables: list[ParsedTable] = Field(default_factory=list, description="Tables referenced")
    columns: list[ParsedColumn] = Field(default_factory=list, description="Columns in SELECT")
    joins: list[ParsedJoin] = Field(default_factory=list, description="Join relationships")

    # CTEs
    ctes: dict[str, str] = Field(default_factory=dict, description="CTE name -> SQL mapping")

    # Clauses
    where_clause: str | None = Field(None, description="WHERE clause if present")
    group_by_columns: list[str] = Field(default_factory=list, description="GROUP BY columns")
    having_clause: str | None = Field(None, description="HAVING clause if present")
    order_by_columns: list[str] = Field(default_factory=list, description="ORDER BY columns")

    # Metrics
    metrics: QueryMetrics = Field(default_factory=QueryMetrics, description="Query metrics")

    # Raw AST (optional, for advanced use)
    ast_json: dict[str, Any] | None = Field(None, description="JSON representation of AST")

    # Parsing metadata
    parse_errors: list[str] = Field(default_factory=list, description="Any parsing errors/warnings")
    parse_time_ms: float = Field(default=0.0, description="Time to parse in milliseconds")

    model_config = {"extra": "allow"}

    def get_table_by_alias(self, alias: str) -> ParsedTable | None:
        """Find a table by its alias or name."""
        for table in self.tables:
            if table.alias == alias or table.name == alias:
                return table
        return None

    def get_columns_from_table(self, table_ref: str) -> list[ParsedColumn]:
        """Get all columns that reference a specific table."""
        return [col for col in self.columns if col.table_ref == table_ref]

    def get_case_columns(self) -> list[ParsedColumn]:
        """Get all columns that contain CASE statements."""
        return [col for col in self.columns if col.is_case_statement]

    @property
    def has_case(self) -> bool:
        """Check if query contains any CASE statements."""
        return self.metrics.case_statement_count > 0 or any(col.is_case_statement for col in self.columns)

    @property
    def has_subquery(self) -> bool:
        """Check if query contains any subqueries."""
        return self.metrics.subquery_count > 0 or any(t.is_subquery for t in self.tables)
