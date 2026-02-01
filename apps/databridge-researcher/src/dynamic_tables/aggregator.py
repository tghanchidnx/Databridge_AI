"""
Aggregation Service for DT_3A tier.

Handles intermediate aggregations with filter precedence:
- Precedence 1 filters take priority over precedence 2, etc.
- Generates COALESCE-based SQL for precedence fallback
- Supports hierarchy-based aggregations
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .models import (
    IntermediateAggregation,
    DynamicTable,
    FilterDefinition,
    AggregationDefinition,
    AggregationType,
    SQLDialect,
)


class AggregationService:
    """
    Service for creating DT_3A intermediate aggregations.

    Provides:
    - Create aggregations with precedence groups
    - Configure dimension filters per precedence
    - Generate SQL with COALESCE for precedence fallback
    """

    def __init__(self):
        """Initialize the aggregation service."""
        self._aggregations: Dict[str, IntermediateAggregation] = {}

        # Set up Jinja2 template environment
        v3_template_dir = Path(__file__).parent.parent.parent.parent / "v3" / "src" / "sql_generator" / "templates"
        if v3_template_dir.exists():
            self._env = Environment(
                loader=FileSystemLoader(str(v3_template_dir)),
                autoescape=select_autoescape(default=False),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self._env = None

    def create_intermediate_aggregation(
        self,
        dynamic_table_id: str,
        dimensions: List[str],
        measures: List[Dict[str, Any]],
        hierarchy_id: Optional[str] = None,
        hierarchy_name: Optional[str] = None,
        precedence_groups: Optional[List[int]] = None,
    ) -> IntermediateAggregation:
        """
        Create an intermediate aggregation definition.

        Args:
            dynamic_table_id: Source DT_2 table ID
            dimensions: List of dimension columns to group by
            measures: List of measure definitions (column, function, alias)
            hierarchy_id: Optional hierarchy ID from V3
            hierarchy_name: Optional hierarchy name
            precedence_groups: List of precedence groups (default [1])

        Returns:
            Created IntermediateAggregation
        """
        # Convert measure dicts to AggregationDefinition objects
        measure_defs = []
        for m in measures:
            measure_defs.append(AggregationDefinition(
                column=m["column"],
                function=AggregationType(m.get("function", "SUM")),
                alias=m.get("alias"),
                distinct=m.get("distinct", False),
            ))

        agg = IntermediateAggregation(
            dynamic_table_id=dynamic_table_id,
            hierarchy_id=hierarchy_id,
            hierarchy_name=hierarchy_name,
            precedence_groups=precedence_groups or [1],
            dimensions=dimensions,
            measures=measure_defs,
        )

        self._aggregations[agg.id] = agg
        return agg

    def get_aggregation(self, agg_id: str) -> Optional[IntermediateAggregation]:
        """Get an aggregation by ID."""
        return self._aggregations.get(agg_id)

    def list_aggregations(
        self,
        dynamic_table_id: Optional[str] = None
    ) -> List[IntermediateAggregation]:
        """List aggregations, optionally filtered by source table."""
        aggs = list(self._aggregations.values())
        if dynamic_table_id:
            aggs = [a for a in aggs if a.dynamic_table_id == dynamic_table_id]
        return aggs

    def set_filter_precedence(
        self,
        agg_id: str,
        precedence: int,
        column: str,
        values: List[Any],
        operator: str = "IN"
    ) -> IntermediateAggregation:
        """
        Set filter values for a precedence group.

        Args:
            agg_id: Aggregation ID
            precedence: Precedence level (1 = highest priority)
            column: Column to filter on
            values: Filter values
            operator: Filter operator

        Returns:
            Updated IntermediateAggregation
        """
        agg = self._aggregations.get(agg_id)
        if not agg:
            raise ValueError(f"Aggregation not found: {agg_id}")

        agg.add_precedence_filter(precedence, column, values, operator)
        agg.updated_at = datetime.utcnow()

        return agg

    def generate_sql(
        self,
        agg_id: str,
        source_table: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
    ) -> str:
        """
        Generate SQL for an intermediate aggregation.

        If multiple precedence groups exist, generates CTEs with COALESCE
        for fallback behavior.

        Args:
            agg_id: Aggregation ID
            source_table: Source table name
            source_database: Source database
            source_schema: Source schema
            dialect: SQL dialect

        Returns:
            Generated SQL string
        """
        agg = self._aggregations.get(agg_id)
        if not agg:
            raise ValueError(f"Aggregation not found: {agg_id}")

        if len(agg.precedence_groups) > 1 and agg.dimension_filters:
            sql = self._generate_precedence_sql(agg, source_table, source_database, source_schema, dialect)
        else:
            sql = self._generate_simple_sql(agg, source_table, source_database, source_schema, dialect)

        agg.generated_sql = sql
        agg.updated_at = datetime.utcnow()
        return sql

    def _generate_simple_sql(
        self,
        agg: IntermediateAggregation,
        source_table: str,
        source_database: Optional[str],
        source_schema: Optional[str],
        dialect: SQLDialect,
    ) -> str:
        """Generate simple aggregation SQL without precedence."""
        lines = []

        lines.append("SELECT")

        # Dimension columns
        select_parts = [f"    {dim}" for dim in agg.dimensions]

        # Measure columns
        for m in agg.measures:
            distinct = "DISTINCT " if m.distinct else ""
            alias = m.alias or f"{m.function.value.lower()}_{m.column}"
            select_parts.append(f"    {m.function.value}({distinct}{m.column}) AS {alias}")

        lines.append(",\n".join(select_parts))

        # FROM clause
        source = self._qualify_name(source_database, source_schema, source_table)
        lines.append(f"FROM {source}")

        # Apply filters from first precedence group if any
        if 1 in agg.dimension_filters:
            filters = agg.dimension_filters[1]
            if filters:
                lines.append("WHERE")
                filter_parts = []
                for f in filters:
                    if f.values:
                        vals = ", ".join(repr(v) for v in f.values)
                        filter_parts.append(f"    {f.column} IN ({vals})")
                    else:
                        val = f"'{f.value}'" if isinstance(f.value, str) else f.value
                        filter_parts.append(f"    {f.column} {f.operator} {val}")
                lines.append("\n    AND ".join(filter_parts))

        # GROUP BY
        if agg.dimensions:
            lines.append("GROUP BY")
            lines.append("    " + ",\n    ".join(agg.dimensions))

        lines.append(";")
        return "\n".join(lines)

    def _generate_precedence_sql(
        self,
        agg: IntermediateAggregation,
        source_table: str,
        source_database: Optional[str],
        source_schema: Optional[str],
        dialect: SQLDialect,
    ) -> str:
        """Generate SQL with CTEs for precedence handling."""
        lines = []
        source = self._qualify_name(source_database, source_schema, source_table)

        # Generate CTEs for each precedence group
        lines.append("WITH")
        cte_names = []

        for i, pg in enumerate(sorted(agg.precedence_groups)):
            cte_name = f"precedence_{pg}_data"
            cte_names.append(cte_name)

            if i > 0:
                lines.append(",")

            lines.append(f"{cte_name} AS (")
            lines.append("    SELECT")

            # Dimension columns
            select_parts = [f"        {dim}" for dim in agg.dimensions]

            # Measure columns
            for m in agg.measures:
                distinct = "DISTINCT " if m.distinct else ""
                alias = m.alias or f"{m.function.value.lower()}_{m.column}"
                select_parts.append(f"        {m.function.value}({distinct}{m.column}) AS {alias}")

            lines.append(",\n".join(select_parts))

            lines.append(f"    FROM {source}")

            # Apply filters for this precedence group
            if pg in agg.dimension_filters:
                filters = agg.dimension_filters[pg]
                if filters:
                    lines.append("    WHERE")
                    filter_parts = []
                    for f in filters:
                        if f.values:
                            vals = ", ".join(repr(v) for v in f.values)
                            filter_parts.append(f"        {f.column} IN ({vals})")
                        else:
                            val = f"'{f.value}'" if isinstance(f.value, str) else f.value
                            filter_parts.append(f"        {f.column} {f.operator} {val}")
                    lines.append("\n        AND ".join(filter_parts))

            # GROUP BY
            if agg.dimensions:
                lines.append("    GROUP BY")
                lines.append("        " + ",\n        ".join(agg.dimensions))

            lines.append(")")

        # Final SELECT with COALESCE
        lines.append("")
        lines.append("SELECT")

        # Dimensions with COALESCE
        select_parts = []
        for dim in agg.dimensions:
            coalesce_parts = [f"{cte}.{dim}" for cte in cte_names]
            select_parts.append(f"    COALESCE({', '.join(coalesce_parts)}) AS {dim}")

        # Measures with COALESCE
        for m in agg.measures:
            alias = m.alias or f"{m.function.value.lower()}_{m.column}"
            coalesce_parts = [f"{cte}.{alias}" for cte in cte_names]
            coalesce_parts.append("0")  # Default fallback
            select_parts.append(f"    COALESCE({', '.join(coalesce_parts)}) AS {alias}")

        lines.append(",\n".join(select_parts))

        # FROM first CTE with FULL OUTER JOINs
        lines.append(f"FROM {cte_names[0]}")

        for cte in cte_names[1:]:
            lines.append(f"FULL OUTER JOIN {cte}")
            join_conditions = [f"    {cte_names[0]}.{dim} = {cte}.{dim}" for dim in agg.dimensions]
            lines.append(f"    ON {' AND '.join(join_conditions)}")

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

    def preview_aggregation(
        self,
        agg_id: str,
        source_table: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
    ) -> Dict[str, Any]:
        """
        Preview aggregation configuration and SQL.

        Args:
            agg_id: Aggregation ID
            source_table: Source table name
            source_database: Source database
            source_schema: Source schema
            dialect: SQL dialect

        Returns:
            Preview information including SQL
        """
        agg = self._aggregations.get(agg_id)
        if not agg:
            raise ValueError(f"Aggregation not found: {agg_id}")

        sql = self.generate_sql(agg_id, source_table, source_database, source_schema, dialect)

        return {
            "id": agg.id,
            "dynamic_table_id": agg.dynamic_table_id,
            "hierarchy_id": agg.hierarchy_id,
            "dimensions": agg.dimensions,
            "measures": [m.model_dump() for m in agg.measures],
            "precedence_groups": agg.precedence_groups,
            "dimension_filters": {
                str(k): [f.model_dump() for f in v]
                for k, v in agg.dimension_filters.items()
            },
            "sql": sql,
        }

    def delete_aggregation(self, agg_id: str) -> bool:
        """Delete an aggregation."""
        if agg_id in self._aggregations:
            del self._aggregations[agg_id]
            return True
        return False


# Singleton instance
_aggregation_service = None


def get_aggregation_service() -> AggregationService:
    """Get or create the aggregation service singleton."""
    global _aggregation_service
    if _aggregation_service is None:
        _aggregation_service = AggregationService()
    return _aggregation_service
