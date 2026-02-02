"""
SQL Query Builder for DataBridge AI Researcher Analytics Engine.

Provides a fluent interface for building SQL queries with dialect support.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union, Literal
from copy import deepcopy

from .dialects import (
    SQLDialect,
    SnowflakeDialect,
    PostgreSQLDialect,
    Column,
    Table,
    Join,
    JoinType,
    WhereCondition,
    OrderBy,
    OrderDirection,
    AggregateFunction,
    get_dialect,
)


@dataclass
class Query:
    """Represents a complete SQL query."""

    sql: str
    dialect: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    columns: List[str] = field(default_factory=list)
    tables: List[str] = field(default_factory=list)
    estimated_rows: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sql": self.sql,
            "dialect": self.dialect,
            "parameters": self.parameters,
            "columns": self.columns,
            "tables": self.tables,
        }


class QueryBuilder:
    """
    Fluent SQL query builder with dialect support.

    Usage:
        query = (QueryBuilder()
            .select("id", "name", "amount")
            .from_table("sales")
            .where("amount > :min_amount")
            .order_by("amount", "DESC")
            .limit(100)
            .build())

    Supports:
        - SELECT with columns, aliases, and aggregates
        - FROM with schema/database qualification
        - JOIN (INNER, LEFT, RIGHT, FULL, CROSS)
        - WHERE with AND/OR conditions
        - GROUP BY with HAVING
        - ORDER BY with ASC/DESC and NULLS handling
        - LIMIT/OFFSET (dialect-specific)
        - Subqueries and CTEs
    """

    def __init__(self, dialect: Optional[Union[str, SQLDialect]] = None):
        """
        Initialize the query builder.

        Args:
            dialect: SQL dialect name or instance (default: PostgreSQL).
        """
        if dialect is None:
            self._dialect = PostgreSQLDialect()
        elif isinstance(dialect, str):
            self._dialect = get_dialect(dialect)
        else:
            self._dialect = dialect

        # Query components
        self._distinct: bool = False
        self._columns: List[Column] = []
        self._raw_columns: List[str] = []
        self._from_table: Optional[Table] = None
        self._joins: List[Join] = []
        self._where_conditions: List[WhereCondition] = []
        self._group_by: List[str] = []
        self._having: List[str] = []
        self._order_by: List[OrderBy] = []
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None
        self._ctes: List[Dict[str, str]] = []
        self._parameters: Dict[str, Any] = {}
        self._union_queries: List[tuple] = []  # (query_sql, union_type)

    def clone(self) -> "QueryBuilder":
        """Create a deep copy of the builder."""
        return deepcopy(self)

    def dialect(self, dialect: Union[str, SQLDialect]) -> "QueryBuilder":
        """
        Set the SQL dialect.

        Args:
            dialect: Dialect name or instance.

        Returns:
            Self for chaining.
        """
        if isinstance(dialect, str):
            self._dialect = get_dialect(dialect)
        else:
            self._dialect = dialect
        return self

    def distinct(self) -> "QueryBuilder":
        """Add DISTINCT to SELECT."""
        self._distinct = True
        return self

    def select(self, *columns: str) -> "QueryBuilder":
        """
        Add columns to SELECT.

        Args:
            *columns: Column names or expressions.

        Returns:
            Self for chaining.
        """
        for col in columns:
            self._raw_columns.append(col)
        return self

    def select_column(
        self,
        name: str,
        table: Optional[str] = None,
        alias: Optional[str] = None,
        aggregate: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add a structured column to SELECT.

        Args:
            name: Column name.
            table: Table alias/name.
            alias: Column alias.
            aggregate: Aggregate function (SUM, AVG, COUNT, etc.).

        Returns:
            Self for chaining.
        """
        agg_func = None
        if aggregate:
            agg_map = {
                "sum": AggregateFunction.SUM,
                "avg": AggregateFunction.AVG,
                "count": AggregateFunction.COUNT,
                "min": AggregateFunction.MIN,
                "max": AggregateFunction.MAX,
                "count_distinct": AggregateFunction.COUNT_DISTINCT,
            }
            agg_func = agg_map.get(aggregate.lower())

        self._columns.append(Column(name=name, table=table, alias=alias, aggregate=agg_func))
        return self

    def select_aggregate(
        self,
        func: str,
        column: str,
        alias: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add an aggregate function to SELECT.

        Args:
            func: Aggregate function name (SUM, AVG, COUNT, etc.).
            column: Column to aggregate.
            alias: Result alias.

        Returns:
            Self for chaining.
        """
        return self.select_column(name=column, aggregate=func, alias=alias)

    def from_table(
        self,
        table: str,
        schema: Optional[str] = None,
        database: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Set the FROM table.

        Args:
            table: Table name.
            schema: Schema name.
            database: Database name.
            alias: Table alias.

        Returns:
            Self for chaining.
        """
        self._from_table = Table(
            name=table,
            schema=schema,
            database=database,
            alias=alias,
        )
        return self

    def join(
        self,
        table: str,
        on: Union[str, List[str]],
        join_type: str = "INNER",
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add a JOIN clause.

        Args:
            table: Table to join.
            on: Join condition(s).
            join_type: Type of join (INNER, LEFT, RIGHT, FULL, CROSS).
            schema: Schema name.
            alias: Table alias.

        Returns:
            Self for chaining.
        """
        join_type_map = {
            "INNER": JoinType.INNER,
            "LEFT": JoinType.LEFT,
            "RIGHT": JoinType.RIGHT,
            "FULL": JoinType.FULL,
            "CROSS": JoinType.CROSS,
        }
        jt = join_type_map.get(join_type.upper(), JoinType.INNER)

        conditions = [on] if isinstance(on, str) else on

        self._joins.append(Join(
            table=Table(name=table, schema=schema, alias=alias),
            join_type=jt,
            on_conditions=conditions,
        ))
        return self

    def left_join(
        self,
        table: str,
        on: Union[str, List[str]],
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "QueryBuilder":
        """Add a LEFT JOIN."""
        return self.join(table, on, "LEFT", schema, alias)

    def right_join(
        self,
        table: str,
        on: Union[str, List[str]],
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "QueryBuilder":
        """Add a RIGHT JOIN."""
        return self.join(table, on, "RIGHT", schema, alias)

    def where(self, condition: str, connector: str = "AND") -> "QueryBuilder":
        """
        Add a WHERE condition.

        Args:
            condition: SQL condition expression.
            connector: Logical connector (AND/OR).

        Returns:
            Self for chaining.
        """
        self._where_conditions.append(WhereCondition(
            condition=condition,
            connector=connector.upper(),
        ))
        return self

    def where_or(self, condition: str) -> "QueryBuilder":
        """Add a WHERE condition with OR connector."""
        return self.where(condition, "OR")

    def where_in(self, column: str, values: List[Any], param_name: str) -> "QueryBuilder":
        """
        Add a WHERE IN condition.

        Args:
            column: Column name.
            values: List of values.
            param_name: Parameter name for the values.

        Returns:
            Self for chaining.
        """
        self._parameters[param_name] = values
        self._where_conditions.append(WhereCondition(
            condition=f"{column} IN (:{param_name})",
            connector="AND",
        ))
        return self

    def where_between(
        self,
        column: str,
        start: Any,
        end: Any,
        start_param: str,
        end_param: str,
    ) -> "QueryBuilder":
        """
        Add a WHERE BETWEEN condition.

        Args:
            column: Column name.
            start: Start value.
            end: End value.
            start_param: Parameter name for start.
            end_param: Parameter name for end.

        Returns:
            Self for chaining.
        """
        self._parameters[start_param] = start
        self._parameters[end_param] = end
        self._where_conditions.append(WhereCondition(
            condition=f"{column} BETWEEN :{start_param} AND :{end_param}",
            connector="AND",
        ))
        return self

    def where_equals(
        self,
        column: str,
        value: Any,
        param_name: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add a parameterized WHERE column = value condition.

        This is the safe way to add equality conditions - values are
        always parameterized to prevent SQL injection.

        Args:
            column: Column name.
            value: Value to compare (will be parameterized).
            param_name: Optional parameter name (auto-generated if not provided).

        Returns:
            Self for chaining.
        """
        if param_name is None:
            # Generate unique parameter name
            param_name = f"p_{column.replace('.', '_')}_{len(self._parameters)}"

        self._parameters[param_name] = value
        self._where_conditions.append(WhereCondition(
            condition=f"{column} = :{param_name}",
            connector="AND",
        ))
        return self

    def where_not_equals(
        self,
        column: str,
        value: Any,
        param_name: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add a parameterized WHERE column != value condition.

        Args:
            column: Column name.
            value: Value to compare (will be parameterized).
            param_name: Optional parameter name.

        Returns:
            Self for chaining.
        """
        if param_name is None:
            param_name = f"p_{column.replace('.', '_')}_{len(self._parameters)}"

        self._parameters[param_name] = value
        self._where_conditions.append(WhereCondition(
            condition=f"{column} != :{param_name}",
            connector="AND",
        ))
        return self

    def where_greater_than(
        self,
        column: str,
        value: Any,
        param_name: Optional[str] = None,
        or_equal: bool = False,
    ) -> "QueryBuilder":
        """
        Add a parameterized WHERE column > value condition.

        Args:
            column: Column name.
            value: Value to compare (will be parameterized).
            param_name: Optional parameter name.
            or_equal: If True, use >= instead of >.

        Returns:
            Self for chaining.
        """
        if param_name is None:
            param_name = f"p_{column.replace('.', '_')}_{len(self._parameters)}"

        operator = ">=" if or_equal else ">"
        self._parameters[param_name] = value
        self._where_conditions.append(WhereCondition(
            condition=f"{column} {operator} :{param_name}",
            connector="AND",
        ))
        return self

    def where_less_than(
        self,
        column: str,
        value: Any,
        param_name: Optional[str] = None,
        or_equal: bool = False,
    ) -> "QueryBuilder":
        """
        Add a parameterized WHERE column < value condition.

        Args:
            column: Column name.
            value: Value to compare (will be parameterized).
            param_name: Optional parameter name.
            or_equal: If True, use <= instead of <.

        Returns:
            Self for chaining.
        """
        if param_name is None:
            param_name = f"p_{column.replace('.', '_')}_{len(self._parameters)}"

        operator = "<=" if or_equal else "<"
        self._parameters[param_name] = value
        self._where_conditions.append(WhereCondition(
            condition=f"{column} {operator} :{param_name}",
            connector="AND",
        ))
        return self

    def where_like(
        self,
        column: str,
        pattern: str,
        param_name: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add a parameterized WHERE column LIKE pattern condition.

        Args:
            column: Column name.
            pattern: LIKE pattern (will be parameterized).
            param_name: Optional parameter name.

        Returns:
            Self for chaining.
        """
        if param_name is None:
            param_name = f"p_{column.replace('.', '_')}_{len(self._parameters)}"

        self._parameters[param_name] = pattern
        self._where_conditions.append(WhereCondition(
            condition=f"{column} LIKE :{param_name}",
            connector="AND",
        ))
        return self

    def where_is_null(self, column: str) -> "QueryBuilder":
        """
        Add a WHERE column IS NULL condition.

        Args:
            column: Column name.

        Returns:
            Self for chaining.
        """
        self._where_conditions.append(WhereCondition(
            condition=f"{column} IS NULL",
            connector="AND",
        ))
        return self

    def where_is_not_null(self, column: str) -> "QueryBuilder":
        """
        Add a WHERE column IS NOT NULL condition.

        Args:
            column: Column name.

        Returns:
            Self for chaining.
        """
        self._where_conditions.append(WhereCondition(
            condition=f"{column} IS NOT NULL",
            connector="AND",
        ))
        return self

    def group_by(self, *columns: str) -> "QueryBuilder":
        """
        Add GROUP BY columns.

        Args:
            *columns: Columns to group by.

        Returns:
            Self for chaining.
        """
        self._group_by.extend(columns)
        return self

    def having(self, condition: str) -> "QueryBuilder":
        """
        Add a HAVING condition.

        Args:
            condition: SQL condition expression.

        Returns:
            Self for chaining.
        """
        self._having.append(condition)
        return self

    def order_by(
        self,
        column: str,
        direction: str = "ASC",
        nulls: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        Add an ORDER BY clause.

        Args:
            column: Column to order by.
            direction: Sort direction (ASC/DESC).
            nulls: NULLS handling (FIRST/LAST).

        Returns:
            Self for chaining.
        """
        dir_enum = OrderDirection.DESC if direction.upper() == "DESC" else OrderDirection.ASC
        self._order_by.append(OrderBy(
            column=column,
            direction=dir_enum,
            nulls=nulls.upper() if nulls else None,
        ))
        return self

    def limit(self, limit: int) -> "QueryBuilder":
        """
        Set LIMIT.

        Args:
            limit: Maximum rows to return.

        Returns:
            Self for chaining.
        """
        self._limit_value = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        """
        Set OFFSET.

        Args:
            offset: Number of rows to skip.

        Returns:
            Self for chaining.
        """
        self._offset_value = offset
        return self

    def with_cte(self, name: str, query: Union[str, "QueryBuilder"]) -> "QueryBuilder":
        """
        Add a Common Table Expression (CTE).

        Args:
            name: CTE name.
            query: CTE query (SQL string or QueryBuilder).

        Returns:
            Self for chaining.
        """
        if isinstance(query, QueryBuilder):
            sql = query.build().sql
        else:
            sql = query
        self._ctes.append({"name": name, "query": sql})
        return self

    def param(self, name: str, value: Any) -> "QueryBuilder":
        """
        Add a query parameter.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            Self for chaining.
        """
        self._parameters[name] = value
        return self

    def union(self, query: Union[str, "QueryBuilder"], all: bool = False) -> "QueryBuilder":
        """
        Add a UNION query.

        Args:
            query: Query to union.
            all: Whether to use UNION ALL.

        Returns:
            Self for chaining.
        """
        if isinstance(query, QueryBuilder):
            sql = query.build().sql
        else:
            sql = query
        union_type = "UNION ALL" if all else "UNION"
        self._union_queries.append((sql, union_type))
        return self

    def build(self) -> Query:
        """
        Build the final SQL query.

        Returns:
            Query object with the generated SQL.
        """
        parts = []

        # CTEs
        if self._ctes:
            cte_parts = []
            for cte in self._ctes:
                cte_parts.append(f"{cte['name']} AS (\n{cte['query']}\n)")
            parts.append("WITH " + ",\n".join(cte_parts))

        # SELECT
        select_parts = []
        if self._distinct:
            select_parts.append("DISTINCT")

        # Build column list
        column_list = []
        for col in self._columns:
            column_list.append(self._dialect.format_column(col))
        column_list.extend(self._raw_columns)

        if not column_list:
            column_list = ["*"]

        select_parts.append(", ".join(column_list))
        parts.append("SELECT " + " ".join(select_parts))

        # FROM
        if self._from_table:
            parts.append("FROM " + self._dialect.format_table(self._from_table))

        # JOINs
        for join in self._joins:
            parts.append(self._dialect.format_join(join))

        # WHERE
        if self._where_conditions:
            where_parts = []
            for i, cond in enumerate(self._where_conditions):
                if i == 0:
                    where_parts.append(cond.condition)
                else:
                    where_parts.append(f"{cond.connector} {cond.condition}")
            parts.append("WHERE " + " ".join(where_parts))

        # GROUP BY
        if self._group_by:
            parts.append("GROUP BY " + ", ".join(self._group_by))

        # HAVING
        if self._having:
            parts.append("HAVING " + " AND ".join(self._having))

        # ORDER BY (required for T-SQL OFFSET)
        if self._order_by:
            order_parts = [self._dialect.format_order_by(o) for o in self._order_by]
            parts.append("ORDER BY " + ", ".join(order_parts))

        # LIMIT/OFFSET (dialect-specific)
        if self._limit_value is not None:
            # T-SQL puts TOP after SELECT
            if self._dialect.name == "tsql" and not self._order_by and not self._offset_value:
                # Insert TOP into SELECT clause
                select_idx = next(i for i, p in enumerate(parts) if p.startswith("SELECT"))
                parts[select_idx] = parts[select_idx].replace(
                    "SELECT ",
                    f"SELECT TOP ({self._limit_value}) ",
                    1
                )
            else:
                parts.append(self._dialect.format_limit(self._limit_value, self._offset_value))

        sql = "\n".join(parts)

        # UNION queries
        for union_sql, union_type in self._union_queries:
            sql = f"{sql}\n{union_type}\n{union_sql}"

        # Collect metadata
        tables = []
        if self._from_table:
            tables.append(self._from_table.name)
        tables.extend(j.table.name for j in self._joins)

        columns = [c.alias or c.name for c in self._columns]
        columns.extend(self._raw_columns)

        return Query(
            sql=sql,
            dialect=self._dialect.name,
            parameters=self._parameters,
            columns=columns,
            tables=tables,
        )

    def to_sql(self) -> str:
        """Convenience method to get just the SQL string."""
        return self.build().sql


# Convenience functions
def select(*columns: str) -> QueryBuilder:
    """Start a new SELECT query."""
    return QueryBuilder().select(*columns)


def from_table(table: str, **kwargs) -> QueryBuilder:
    """Start a new query from a table."""
    return QueryBuilder().from_table(table, **kwargs)
