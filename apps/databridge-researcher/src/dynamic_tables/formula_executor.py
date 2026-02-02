"""
Formula Executor Service for DT_3 tier.

Applies formulas to create output tables:
- SUBTRACT: Gross Profit = Revenue - COGS
- ADD/SUM: Total OpEx = SG&A + R&D + Marketing
- MULTIPLY: Extended Amount = Qty * Price
- DIVIDE: Average = Total / Count
- PERCENT: Margin % = (Revenue - COGS) / Revenue * 100
- VARIANCE: Budget vs Actual variance
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .models import (
    OutputTable,
    DynamicTableColumn,
    FormulaColumn,
    FormulaType,
    SQLDialect,
    TableStatus,
)


class FormulaExecutorService:
    """
    Service for creating DT_3 output tables with formula calculations.

    Provides:
    - Create output tables from multiple sources
    - Add formula columns (SUBTRACT, ADD, MULTIPLY, DIVIDE, PERCENT)
    - Generate SQL with calculated columns
    - Deploy to target data warehouse
    """

    def __init__(self):
        """Initialize the formula executor service."""
        self._output_tables: Dict[str, OutputTable] = {}

        # Set up Jinja2 template environment
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

    def create_output_table(
        self,
        project_id: str,
        table_name: str,
        source_tables: List[Dict[str, Any]],
        dimensions: List[Dict[str, Any]],
        base_measures: List[Dict[str, Any]],
        formula_group_id: Optional[str] = None,
        formula_group_name: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> OutputTable:
        """
        Create an output table definition.

        Args:
            project_id: Project ID
            table_name: Output table name
            source_tables: List of source table definitions
            dimensions: List of dimension column definitions
            base_measures: List of base measure column definitions
            formula_group_id: Optional Librarian formula group ID
            formula_group_name: Optional formula group name
            target_database: Target database
            target_schema: Target schema
            dialect: SQL dialect
            display_name: Display name
            description: Description

        Returns:
            Created OutputTable
        """
        # Convert column dicts to DynamicTableColumn objects
        dim_cols = [DynamicTableColumn(**d) for d in dimensions]
        measure_cols = [DynamicTableColumn(**m) for m in base_measures]

        output = OutputTable(
            project_id=project_id,
            table_name=table_name,
            display_name=display_name or table_name,
            description=description,
            source_tables=source_tables,
            formula_group_id=formula_group_id,
            formula_group_name=formula_group_name,
            dimensions=dim_cols,
            base_measures=measure_cols,
            target_database=target_database,
            target_schema=target_schema,
            dialect=dialect,
        )

        self._output_tables[output.id] = output
        return output

    def get_output_table(self, table_id: str) -> Optional[OutputTable]:
        """Get an output table by ID."""
        return self._output_tables.get(table_id)

    def list_output_tables(self, project_id: Optional[str] = None) -> List[OutputTable]:
        """List output tables, optionally filtered by project."""
        tables = list(self._output_tables.values())
        if project_id:
            tables = [t for t in tables if t.project_id == project_id]
        return tables

    def add_formula_column(
        self,
        table_id: str,
        alias: str,
        formula_type: FormulaType,
        operands: List[str],
        expression: Optional[str] = None,
        round_decimals: Optional[int] = None,
    ) -> OutputTable:
        """
        Add a calculated column to an output table.

        Args:
            table_id: Output table ID
            alias: Column alias
            formula_type: Type of formula (ADD, SUBTRACT, etc.)
            operands: List of column names or values
            expression: Custom SQL expression (for EXPRESSION type)
            round_decimals: Optional decimal places to round to

        Returns:
            Updated OutputTable
        """
        output = self._output_tables.get(table_id)
        if not output:
            raise ValueError(f"Output table not found: {table_id}")

        formula = FormulaColumn(
            alias=alias,
            formula_type=formula_type,
            operands=operands,
            expression=expression,
            round_decimals=round_decimals,
        )
        output.calculated_columns.append(formula)
        output.updated_at = datetime.now(timezone.utc)

        return output

    def add_source_join(
        self,
        table_id: str,
        left_alias: str,
        right_alias: str,
        join_condition: str,
        join_type: str = "LEFT",
    ) -> OutputTable:
        """
        Add a join between source tables.

        Args:
            table_id: Output table ID
            left_alias: Left table alias
            right_alias: Right table alias
            join_condition: Join ON condition
            join_type: Join type (LEFT, INNER, etc.)

        Returns:
            Updated OutputTable
        """
        output = self._output_tables.get(table_id)
        if not output:
            raise ValueError(f"Output table not found: {table_id}")

        output.source_joins.append({
            "left": left_alias,
            "right": right_alias,
            "on": join_condition,
            "type": join_type,
        })
        output.updated_at = datetime.now(timezone.utc)

        return output

    def generate_sql(
        self,
        table_id: str,
        dialect: Optional[SQLDialect] = None,
    ) -> str:
        """
        Generate SQL for an output table.

        Args:
            table_id: Output table ID
            dialect: Optional dialect override

        Returns:
            Generated SQL string
        """
        output = self._output_tables.get(table_id)
        if not output:
            raise ValueError(f"Output table not found: {table_id}")

        dialect = dialect or output.dialect

        # Use template if available
        if self._env and dialect == SQLDialect.SNOWFLAKE:
            sql = self._generate_with_template(output, dialect)
        else:
            sql = self._generate_manual(output, dialect)

        output.generated_sql = sql
        output.updated_at = datetime.now(timezone.utc)
        return sql

    def _generate_with_template(self, output: OutputTable, dialect: SQLDialect) -> str:
        """Generate SQL using Jinja2 template."""
        template = self._env.get_template(f"{dialect.value}/output_table.j2")

        # Build calculated columns for template
        calculated = []
        for calc in output.calculated_columns:
            calculated.append({
                "alias": calc.alias,
                "formula_type": calc.formula_type.value,
                "operands": calc.operands,
                "expression": calc.expression,
            })

        sql = template.render(
            table_name=output.table_name,
            target_database=output.target_database,
            target_schema=output.target_schema,
            source_tables=output.source_tables,
            dimensions=[{"column": d.name, "alias": d.alias, "source_alias": d.source_column} for d in output.dimensions],
            base_measures=[{"column": m.name, "alias": m.alias, "source_alias": m.source_column} for m in output.base_measures],
            calculated_columns=calculated,
            filters=[],
            target_lag=output.target_lag,
            warehouse=output.warehouse,
            comment=output.description,
            use_cte=False,
        )

        return sql.strip()

    def _generate_manual(self, output: OutputTable, dialect: SQLDialect) -> str:
        """Generate SQL manually."""
        lines = []

        # CREATE statement
        target = self._qualify_name(output.target_database, output.target_schema, output.table_name)

        if dialect == SQLDialect.SNOWFLAKE:
            lines.append(f"CREATE OR REPLACE DYNAMIC TABLE {target}")
            if output.description:
                lines.append(f"COMMENT = '{output.description}'")
            lines.append(f"TARGET_LAG = '{output.target_lag}'")
            lines.append(f"WAREHOUSE = {output.warehouse}")
            lines.append("AS")
        elif dialect == SQLDialect.POSTGRESQL:
            lines.append(f"CREATE MATERIALIZED VIEW {target} AS")
        else:
            lines.append(f"CREATE OR REPLACE VIEW {target} AS")

        # SELECT clause
        lines.append("SELECT")
        select_parts = []

        # Dimension columns
        for dim in output.dimensions:
            source = f"{dim.source_column}." if dim.source_column else ""
            alias = f" AS {dim.alias}" if dim.alias else ""
            select_parts.append(f"    {source}{dim.name}{alias}")

        # Base measure columns
        for meas in output.base_measures:
            source = f"{meas.source_column}." if meas.source_column else ""
            alias = f" AS {meas.alias}" if meas.alias else ""
            select_parts.append(f"    {source}{meas.name}{alias}")

        # Calculated columns
        for calc in output.calculated_columns:
            expr = self._build_formula_expression(calc)
            select_parts.append(f"    {expr} AS {calc.alias}")

        lines.append(",\n".join(select_parts))

        # FROM clause
        if output.source_tables:
            first_source = output.source_tables[0]
            source_name = self._qualify_name(
                first_source.get("database"),
                first_source.get("schema"),
                first_source["name"]
            )
            alias = f" AS {first_source['alias']}" if first_source.get("alias") else ""
            lines.append(f"FROM {source_name}{alias}")

            # JOIN clauses for additional sources
            for source in output.source_tables[1:]:
                source_name = self._qualify_name(
                    source.get("database"),
                    source.get("schema"),
                    source["name"]
                )
                alias = f" AS {source['alias']}" if source.get("alias") else ""
                join_type = source.get("join_type", "LEFT")
                lines.append(f"{join_type} JOIN {source_name}{alias}")
                if source.get("join_on"):
                    lines.append(f"    ON {source['join_on']}")

        lines.append(";")
        return "\n".join(lines)

    def _build_formula_expression(self, calc: FormulaColumn) -> str:
        """Build SQL expression for a formula column."""
        if calc.formula_type == FormulaType.EXPRESSION and calc.expression:
            expr = calc.expression
        elif calc.formula_type == FormulaType.ADD:
            expr = f"({' + '.join(calc.operands)})"
        elif calc.formula_type == FormulaType.SUBTRACT:
            if len(calc.operands) >= 2:
                expr = f"({calc.operands[0]} - ({' + '.join(calc.operands[1:])}))"
            else:
                expr = calc.operands[0]
        elif calc.formula_type == FormulaType.MULTIPLY:
            expr = f"({' * '.join(calc.operands)})"
        elif calc.formula_type == FormulaType.DIVIDE:
            if len(calc.operands) >= 2:
                expr = f"CASE WHEN {calc.operands[1]} = 0 THEN NULL ELSE {calc.operands[0]} / {calc.operands[1]} END"
            else:
                expr = calc.operands[0]
        elif calc.formula_type == FormulaType.PERCENT:
            if len(calc.operands) >= 2:
                expr = f"CASE WHEN {calc.operands[1]} = 0 THEN NULL ELSE ({calc.operands[0]} / {calc.operands[1]}) * 100 END"
            else:
                expr = "NULL"
        elif calc.formula_type == FormulaType.VARIANCE:
            if len(calc.operands) >= 2:
                expr = f"({calc.operands[0]} - {calc.operands[1]})"
            else:
                expr = "NULL"
        else:
            expr = "NULL"

        # Apply rounding if specified
        if calc.round_decimals is not None:
            expr = f"ROUND({expr}, {calc.round_decimals})"

        return expr

    def _qualify_name(
        self,
        database: Optional[str],
        schema: Optional[str],
        name: str
    ) -> str:
        """Build a fully qualified name."""
        parts = [database, schema, name]
        return ".".join(p for p in parts if p)

    def update_status(self, table_id: str, status: TableStatus) -> OutputTable:
        """Update the status of an output table."""
        output = self._output_tables.get(table_id)
        if not output:
            raise ValueError(f"Output table not found: {table_id}")

        output.status = status
        output.updated_at = datetime.now(timezone.utc)
        return output

    def mark_deployed(
        self,
        table_id: str,
        connection_id: Optional[str] = None
    ) -> OutputTable:
        """Mark an output table as deployed."""
        output = self._output_tables.get(table_id)
        if not output:
            raise ValueError(f"Output table not found: {table_id}")

        output.is_deployed = True
        output.last_deployed_at = datetime.now(timezone.utc)
        output.status = TableStatus.DEPLOYED
        if connection_id:
            output.deployment_connection_id = connection_id
        output.updated_at = datetime.now(timezone.utc)

        return output

    def delete_output_table(self, table_id: str) -> bool:
        """Delete an output table."""
        if table_id in self._output_tables:
            del self._output_tables[table_id]
            return True
        return False


# Singleton instance
_formula_executor = None


def get_formula_executor() -> FormulaExecutorService:
    """Get or create the formula executor singleton."""
    global _formula_executor
    if _formula_executor is None:
        _formula_executor = FormulaExecutorService()
    return _formula_executor
