"""
Dynamic Table Builder Service.

Builds DT_2 tier tables from VW_1 views with:
- Join management
- Filter handling with precedence
- Aggregation configuration
- SQL generation for multiple dialects
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .models import (
    DynamicTable,
    DynamicTableColumn,
    JoinDefinition,
    FilterDefinition,
    AggregationDefinition,
    JoinType,
    AggregationType,
    SQLDialect,
    TableStatus,
)


class DynamicTableBuilderService:
    """
    Service for building DT_2 dynamic tables.

    Provides:
    - Create and configure dynamic tables
    - Add joins, filters, and aggregations
    - Generate SQL for multiple dialects
    """

    def __init__(self):
        """Initialize the builder service."""
        # In-memory storage for stateless operation
        self._tables: Dict[str, DynamicTable] = {}

        # Set up Jinja2 template environment (use Librarian templates)
        librarian_template_dir = Path(__file__).parent.parent.parent.parent / "librarian" / "src" / "sql_generator" / "templates"
        if librarian_template_dir.exists():
            self._env = Environment(
                loader=FileSystemLoader(str(librarian_template_dir)),
                autoescape=select_autoescape(default=False),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self._env = None

    def create_dynamic_table(
        self,
        project_id: str,
        table_name: str,
        source_view_name: str,
        columns: List[Dict[str, Any]],
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        source_view_id: Optional[str] = None,
    ) -> DynamicTable:
        """
        Create a new dynamic table definition.

        Args:
            project_id: Project ID
            table_name: Name for the dynamic table
            source_view_name: Source VW_1 view name
            columns: List of column definitions
            source_database: Source database name
            source_schema: Source schema name
            target_database: Target database name
            target_schema: Target schema name
            dialect: SQL dialect
            display_name: Display name
            description: Description
            source_view_id: Optional ID of source GeneratedView

        Returns:
            Created DynamicTable
        """
        # Convert column dicts to DynamicTableColumn objects
        table_columns = []
        for col in columns:
            table_columns.append(DynamicTableColumn(
                name=col.get("name"),
                source_column=col.get("source_column"),
                alias=col.get("alias"),
                data_type=col.get("data_type"),
                expression=col.get("expression"),
                aggregation=AggregationType(col["aggregation"]) if col.get("aggregation") else None,
                is_dimension=col.get("is_dimension", False),
                is_measure=col.get("is_measure", False),
            ))

        dt = DynamicTable(
            project_id=project_id,
            table_name=table_name,
            display_name=display_name or table_name,
            description=description,
            source_view_id=source_view_id,
            source_view_name=source_view_name,
            source_database=source_database,
            source_schema=source_schema,
            target_database=target_database,
            target_schema=target_schema,
            columns=table_columns,
            dialect=dialect,
        )

        # Store in memory
        self._tables[dt.id] = dt
        return dt

    def get_dynamic_table(self, table_id: str) -> Optional[DynamicTable]:
        """Get a dynamic table by ID."""
        return self._tables.get(table_id)

    def list_dynamic_tables(self, project_id: Optional[str] = None) -> List[DynamicTable]:
        """List all dynamic tables, optionally filtered by project."""
        tables = list(self._tables.values())
        if project_id:
            tables = [t for t in tables if t.project_id == project_id]
        return tables

    def add_join(
        self,
        table_id: str,
        join_table: str,
        on_condition: str,
        join_type: JoinType = JoinType.LEFT,
        alias: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> DynamicTable:
        """
        Add a join to a dynamic table.

        Args:
            table_id: Dynamic table ID
            join_table: Table to join
            on_condition: Join condition (e.g., "f.account_id = d.id")
            join_type: Type of join
            alias: Table alias
            database: Database name
            schema_name: Schema name

        Returns:
            Updated DynamicTable
        """
        dt = self._tables.get(table_id)
        if not dt:
            raise ValueError(f"Dynamic table not found: {table_id}")

        join_def = JoinDefinition(
            table=join_table,
            alias=alias,
            database=database,
            schema_name=schema_name,
            join_type=join_type,
            on_condition=on_condition,
        )
        dt.joins.append(join_def)
        dt.updated_at = datetime.utcnow()

        return dt

    def add_filter(
        self,
        table_id: str,
        column: str,
        operator: str = "=",
        value: Any = None,
        values: Optional[List[Any]] = None,
        expression: Optional[str] = None,
        precedence_group: int = 1,
    ) -> DynamicTable:
        """
        Add a filter to a dynamic table.

        Args:
            table_id: Dynamic table ID
            column: Column to filter on
            operator: Comparison operator
            value: Single value for comparison
            values: List of values for IN clause
            expression: Complex filter expression
            precedence_group: Filter precedence (1 = highest)

        Returns:
            Updated DynamicTable
        """
        dt = self._tables.get(table_id)
        if not dt:
            raise ValueError(f"Dynamic table not found: {table_id}")

        filter_def = FilterDefinition(
            column=column,
            operator=operator,
            value=value,
            values=values,
            expression=expression,
            precedence_group=precedence_group,
        )
        dt.filters.append(filter_def)
        dt.updated_at = datetime.utcnow()

        return dt

    def add_aggregation(
        self,
        table_id: str,
        column: str,
        function: AggregationType = AggregationType.SUM,
        alias: Optional[str] = None,
        distinct: bool = False,
        filter_condition: Optional[str] = None,
    ) -> DynamicTable:
        """
        Add an aggregation to a dynamic table.

        Args:
            table_id: Dynamic table ID
            column: Column to aggregate
            function: Aggregation function
            alias: Output alias
            distinct: Use DISTINCT
            filter_condition: Optional FILTER clause

        Returns:
            Updated DynamicTable
        """
        dt = self._tables.get(table_id)
        if not dt:
            raise ValueError(f"Dynamic table not found: {table_id}")

        agg_def = AggregationDefinition(
            column=column,
            function=function,
            alias=alias or f"{function.value.lower()}_{column}",
            distinct=distinct,
            filter_condition=filter_condition,
        )
        dt.aggregations.append(agg_def)
        dt.updated_at = datetime.utcnow()

        return dt

    def set_group_by(self, table_id: str, columns: List[str]) -> DynamicTable:
        """Set the GROUP BY columns for a dynamic table."""
        dt = self._tables.get(table_id)
        if not dt:
            raise ValueError(f"Dynamic table not found: {table_id}")

        dt.group_by = columns
        dt.updated_at = datetime.utcnow()
        return dt

    def generate_sql(
        self,
        table_id: str,
        dialect: Optional[SQLDialect] = None,
    ) -> str:
        """
        Generate SQL for a dynamic table.

        Args:
            table_id: Dynamic table ID
            dialect: Optional dialect override

        Returns:
            Generated SQL string
        """
        dt = self._tables.get(table_id)
        if not dt:
            raise ValueError(f"Dynamic table not found: {table_id}")

        dialect = dialect or dt.dialect

        # Build SQL using template or manual generation
        if self._env and dialect == SQLDialect.SNOWFLAKE:
            sql = self._generate_with_template(dt, dialect)
        else:
            sql = self._generate_manual(dt, dialect)

        dt.generated_sql = sql
        dt.updated_at = datetime.utcnow()
        return sql

    def _generate_with_template(self, dt: DynamicTable, dialect: SQLDialect) -> str:
        """Generate SQL using Jinja2 template."""
        template = self._env.get_template(f"{dialect.value}/dynamic_table.j2")

        # Build column list
        columns = []
        for col in dt.columns:
            col_dict = {
                "name": col.name,
                "source_column": col.source_column or col.name,
                "alias": col.alias,
            }
            if col.expression:
                col_dict["expression"] = col.expression
            if col.aggregation:
                col_dict["aggregation"] = col.aggregation.value
            columns.append(col_dict)

        # Build joins
        joins = []
        for j in dt.joins:
            joins.append({
                "table": j.table,
                "alias": j.alias,
                "database": j.database,
                "schema": j.schema_name,
                "type": j.join_type.value,
                "on": j.on_condition,
            })

        # Build filters
        filters = []
        for f in dt.filters:
            if f.expression:
                filters.append({"expression": f.expression})
            elif f.values:
                filters.append({
                    "column": f.column,
                    "operator": "IN",
                    "value": f"({', '.join(repr(v) for v in f.values)})"
                })
            else:
                filters.append({
                    "column": f.column,
                    "operator": f.operator,
                    "value": f.value
                })

        sql = template.render(
            table_name=dt.table_name,
            target_database=dt.target_database,
            target_schema=dt.target_schema,
            source_view=dt.source_view_name,
            source_database=dt.source_database,
            source_schema=dt.source_schema,
            columns=columns,
            joins=joins,
            filters=filters,
            group_by=dt.group_by,
            aggregations=[
                {
                    "function": a.function.value,
                    "column": a.column,
                    "alias": a.alias,
                    "distinct": a.distinct,
                }
                for a in dt.aggregations
            ],
            target_lag=dt.target_lag,
            warehouse=dt.warehouse,
            comment=dt.description,
        )

        return sql.strip()

    def _generate_manual(self, dt: DynamicTable, dialect: SQLDialect) -> str:
        """Generate SQL manually (fallback)."""
        lines = []

        # CREATE statement
        target = self._qualify_name(dt.target_database, dt.target_schema, dt.table_name)

        if dialect == SQLDialect.SNOWFLAKE:
            lines.append(f"CREATE OR REPLACE DYNAMIC TABLE {target}")
            if dt.description:
                lines.append(f"COMMENT = '{dt.description}'")
            lines.append(f"TARGET_LAG = '{dt.target_lag}'")
            lines.append(f"WAREHOUSE = {dt.warehouse}")
            lines.append("AS")
        elif dialect == SQLDialect.POSTGRESQL:
            lines.append(f"CREATE MATERIALIZED VIEW {target} AS")
        else:
            lines.append(f"CREATE OR REPLACE VIEW {target} AS")

        # SELECT clause
        lines.append("SELECT")

        select_parts = []
        for col in dt.columns:
            if col.expression:
                part = f"    {col.expression}"
            elif col.aggregation:
                part = f"    {col.aggregation.value}({col.source_column or col.name})"
            else:
                part = f"    {col.source_column or col.name}"

            if col.alias:
                part += f" AS {col.alias}"
            select_parts.append(part)

        # Add aggregations
        for agg in dt.aggregations:
            distinct = "DISTINCT " if agg.distinct else ""
            part = f"    {agg.function.value}({distinct}{agg.column}) AS {agg.alias}"
            select_parts.append(part)

        lines.append(",\n".join(select_parts))

        # FROM clause
        source = self._qualify_name(dt.source_database, dt.source_schema, dt.source_view_name)
        lines.append(f"FROM {source}")

        # JOIN clauses
        for j in dt.joins:
            join_table = self._qualify_name(j.database, j.schema_name, j.table)
            alias_part = f" AS {j.alias}" if j.alias else ""
            lines.append(f"{j.join_type.value} JOIN {join_table}{alias_part}")
            lines.append(f"    ON {j.on_condition}")

        # WHERE clause
        if dt.filters:
            lines.append("WHERE")
            filter_parts = []
            for f in dt.filters:
                if f.expression:
                    filter_parts.append(f"    {f.expression}")
                elif f.values:
                    vals = ", ".join(repr(v) for v in f.values)
                    filter_parts.append(f"    {f.column} IN ({vals})")
                else:
                    val = f"'{f.value}'" if isinstance(f.value, str) else f.value
                    filter_parts.append(f"    {f.column} {f.operator} {val}")
            lines.append("\n    AND ".join(filter_parts))

        # GROUP BY clause
        if dt.group_by:
            lines.append("GROUP BY")
            lines.append("    " + ",\n    ".join(dt.group_by))

        lines.append(";")
        return "\n".join(lines)

    def _qualify_name(
        self,
        database: Optional[str],
        schema: Optional[str],
        name: str
    ) -> str:
        """Build a fully qualified name."""
        parts = [database, schema, name]
        return ".".join(p for p in parts if p)

    def update_status(self, table_id: str, status: TableStatus) -> DynamicTable:
        """Update the status of a dynamic table."""
        dt = self._tables.get(table_id)
        if not dt:
            raise ValueError(f"Dynamic table not found: {table_id}")

        dt.status = status
        dt.updated_at = datetime.utcnow()
        return dt

    def delete_dynamic_table(self, table_id: str) -> bool:
        """Delete a dynamic table."""
        if table_id in self._tables:
            del self._tables[table_id]
            return True
        return False


# Singleton instance
_builder_service = None


def get_dynamic_table_builder() -> DynamicTableBuilderService:
    """Get or create the dynamic table builder singleton."""
    global _builder_service
    if _builder_service is None:
        _builder_service = DynamicTableBuilderService()
    return _builder_service
