"""
SQL Dialects for DataBridge AI Researcher Analytics Engine.

Provides dialect-specific SQL generation for:
- Snowflake
- SQL Server (TSQL)
- Spark SQL
- PostgreSQL
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum


class JoinType(str, Enum):
    """SQL Join types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"


class AggregateFunction(str, Enum):
    """SQL Aggregate functions."""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class OrderDirection(str, Enum):
    """SQL Order direction."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class Column:
    """Represents a column in a query."""
    name: str
    table: Optional[str] = None
    alias: Optional[str] = None
    aggregate: Optional[AggregateFunction] = None

    def full_name(self) -> str:
        """Get the full column reference."""
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name


@dataclass
class Table:
    """Represents a table in a query."""
    name: str
    schema: Optional[str] = None
    database: Optional[str] = None
    alias: Optional[str] = None

    def full_name(self) -> str:
        """Get the fully qualified table name."""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.name)
        return ".".join(parts)


@dataclass
class Join:
    """Represents a JOIN clause."""
    table: Table
    join_type: JoinType
    on_conditions: List[str]


@dataclass
class WhereCondition:
    """Represents a WHERE condition."""
    condition: str
    connector: str = "AND"  # AND or OR


@dataclass
class OrderBy:
    """Represents an ORDER BY clause."""
    column: str
    direction: OrderDirection = OrderDirection.ASC
    nulls: Optional[str] = None  # FIRST or LAST


class SQLDialect(ABC):
    """
    Abstract base class for SQL dialects.

    Provides dialect-specific SQL generation for different database systems.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the dialect name."""
        pass

    @abstractmethod
    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier (table/column name)."""
        pass

    @abstractmethod
    def format_limit(self, limit: int, offset: Optional[int] = None) -> str:
        """Format LIMIT/OFFSET clause."""
        pass

    @abstractmethod
    def format_date_trunc(self, column: str, unit: str) -> str:
        """Format date truncation function."""
        pass

    @abstractmethod
    def format_current_timestamp(self) -> str:
        """Format current timestamp function."""
        pass

    @abstractmethod
    def format_coalesce(self, *args: str) -> str:
        """Format COALESCE function."""
        pass

    @abstractmethod
    def format_cast(self, expression: str, data_type: str) -> str:
        """Format CAST function."""
        pass

    @abstractmethod
    def format_concat(self, *args: str) -> str:
        """Format string concatenation."""
        pass

    @abstractmethod
    def format_isnull(self, column: str, default: str) -> str:
        """Format NULL replacement function."""
        pass

    def format_aggregate(self, func: AggregateFunction, column: str) -> str:
        """Format an aggregate function."""
        if func == AggregateFunction.COUNT_DISTINCT:
            return f"COUNT(DISTINCT {column})"
        return f"{func.value}({column})"

    def format_column(self, col: Column) -> str:
        """Format a column reference."""
        name = col.full_name()
        if col.aggregate:
            name = self.format_aggregate(col.aggregate, name)
        if col.alias:
            return f"{name} AS {self.quote_identifier(col.alias)}"
        return name

    def format_table(self, table: Table) -> str:
        """Format a table reference."""
        name = table.full_name()
        if table.alias:
            return f"{name} AS {table.alias}"
        return name

    def format_join(self, join: Join) -> str:
        """Format a JOIN clause."""
        table_ref = self.format_table(join.table)
        conditions = " AND ".join(join.on_conditions)
        return f"{join.join_type.value} {table_ref} ON {conditions}"

    def format_order_by(self, order: OrderBy) -> str:
        """Format an ORDER BY item."""
        result = f"{order.column} {order.direction.value}"
        if order.nulls:
            result += f" NULLS {order.nulls}"
        return result

    def format_window_function(
        self,
        func: str,
        column: str,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
    ) -> str:
        """Format a window function."""
        over_parts = []
        if partition_by:
            over_parts.append(f"PARTITION BY {', '.join(partition_by)}")
        if order_by:
            over_parts.append(f"ORDER BY {', '.join(order_by)}")
        over_clause = " ".join(over_parts)
        return f"{func}({column}) OVER ({over_clause})"


class SnowflakeDialect(SQLDialect):
    """Snowflake SQL dialect."""

    @property
    def name(self) -> str:
        return "snowflake"

    def quote_identifier(self, identifier: str) -> str:
        """Snowflake uses double quotes for identifiers."""
        return f'"{identifier}"'

    def format_limit(self, limit: int, offset: Optional[int] = None) -> str:
        """Snowflake uses LIMIT/OFFSET syntax."""
        if offset:
            return f"LIMIT {limit} OFFSET {offset}"
        return f"LIMIT {limit}"

    def format_date_trunc(self, column: str, unit: str) -> str:
        """Snowflake DATE_TRUNC function."""
        return f"DATE_TRUNC('{unit}', {column})"

    def format_current_timestamp(self) -> str:
        return "CURRENT_TIMESTAMP()"

    def format_coalesce(self, *args: str) -> str:
        return f"COALESCE({', '.join(args)})"

    def format_cast(self, expression: str, data_type: str) -> str:
        return f"CAST({expression} AS {data_type})"

    def format_concat(self, *args: str) -> str:
        return f"CONCAT({', '.join(args)})"

    def format_isnull(self, column: str, default: str) -> str:
        """Snowflake uses IFNULL or NVL."""
        return f"IFNULL({column}, {default})"


class TSQLDialect(SQLDialect):
    """SQL Server (T-SQL) dialect."""

    @property
    def name(self) -> str:
        return "tsql"

    def quote_identifier(self, identifier: str) -> str:
        """T-SQL uses square brackets for identifiers."""
        return f"[{identifier}]"

    def format_limit(self, limit: int, offset: Optional[int] = None) -> str:
        """T-SQL uses TOP or OFFSET/FETCH."""
        if offset:
            return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        return f"TOP {limit}"

    def format_date_trunc(self, column: str, unit: str) -> str:
        """T-SQL uses DATETRUNC (SQL Server 2022+) or DATEADD/DATEDIFF."""
        # For compatibility, use DATEADD/DATEDIFF pattern
        unit_map = {
            "year": "YEAR",
            "quarter": "QUARTER",
            "month": "MONTH",
            "week": "WEEK",
            "day": "DAY",
            "hour": "HOUR",
        }
        sql_unit = unit_map.get(unit.lower(), "DAY")
        return f"DATEADD({sql_unit}, DATEDIFF({sql_unit}, 0, {column}), 0)"

    def format_current_timestamp(self) -> str:
        return "GETDATE()"

    def format_coalesce(self, *args: str) -> str:
        return f"COALESCE({', '.join(args)})"

    def format_cast(self, expression: str, data_type: str) -> str:
        return f"CAST({expression} AS {data_type})"

    def format_concat(self, *args: str) -> str:
        """T-SQL can use CONCAT or + operator."""
        return f"CONCAT({', '.join(args)})"

    def format_isnull(self, column: str, default: str) -> str:
        """T-SQL uses ISNULL."""
        return f"ISNULL({column}, {default})"

    def format_limit(self, limit: int, offset: Optional[int] = None) -> str:
        """T-SQL requires ORDER BY for OFFSET/FETCH."""
        if offset:
            return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        # For TOP without ORDER BY
        return f"TOP ({limit})"


class SparkDialect(SQLDialect):
    """Spark SQL dialect."""

    @property
    def name(self) -> str:
        return "spark"

    def quote_identifier(self, identifier: str) -> str:
        """Spark uses backticks for identifiers."""
        return f"`{identifier}`"

    def format_limit(self, limit: int, offset: Optional[int] = None) -> str:
        """Spark uses LIMIT (no built-in OFFSET)."""
        if offset:
            # Spark SQL doesn't have native OFFSET, use window function workaround
            return f"LIMIT {limit}"  # Simplified - offset handled differently
        return f"LIMIT {limit}"

    def format_date_trunc(self, column: str, unit: str) -> str:
        """Spark uses DATE_TRUNC or TRUNC."""
        return f"DATE_TRUNC('{unit}', {column})"

    def format_current_timestamp(self) -> str:
        return "CURRENT_TIMESTAMP()"

    def format_coalesce(self, *args: str) -> str:
        return f"COALESCE({', '.join(args)})"

    def format_cast(self, expression: str, data_type: str) -> str:
        return f"CAST({expression} AS {data_type})"

    def format_concat(self, *args: str) -> str:
        return f"CONCAT({', '.join(args)})"

    def format_isnull(self, column: str, default: str) -> str:
        """Spark uses IFNULL or NVL."""
        return f"IFNULL({column}, {default})"


class PostgreSQLDialect(SQLDialect):
    """PostgreSQL dialect."""

    @property
    def name(self) -> str:
        return "postgresql"

    def quote_identifier(self, identifier: str) -> str:
        """PostgreSQL uses double quotes for identifiers."""
        return f'"{identifier}"'

    def format_limit(self, limit: int, offset: Optional[int] = None) -> str:
        """PostgreSQL uses LIMIT/OFFSET syntax."""
        if offset:
            return f"LIMIT {limit} OFFSET {offset}"
        return f"LIMIT {limit}"

    def format_date_trunc(self, column: str, unit: str) -> str:
        """PostgreSQL DATE_TRUNC function."""
        return f"DATE_TRUNC('{unit}', {column})"

    def format_current_timestamp(self) -> str:
        return "CURRENT_TIMESTAMP"

    def format_coalesce(self, *args: str) -> str:
        return f"COALESCE({', '.join(args)})"

    def format_cast(self, expression: str, data_type: str) -> str:
        # PostgreSQL also supports :: syntax
        return f"CAST({expression} AS {data_type})"

    def format_concat(self, *args: str) -> str:
        # PostgreSQL can use || or CONCAT
        return f"CONCAT({', '.join(args)})"

    def format_isnull(self, column: str, default: str) -> str:
        """PostgreSQL uses COALESCE."""
        return f"COALESCE({column}, {default})"


def get_dialect(name: str) -> SQLDialect:
    """Get a dialect instance by name."""
    dialects = {
        "snowflake": SnowflakeDialect,
        "tsql": TSQLDialect,
        "sqlserver": TSQLDialect,
        "spark": SparkDialect,
        "postgresql": PostgreSQLDialect,
        "postgres": PostgreSQLDialect,
    }
    dialect_class = dialects.get(name.lower())
    if not dialect_class:
        raise ValueError(f"Unknown dialect: {name}. Available: {list(dialects.keys())}")
    return dialect_class()
