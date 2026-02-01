"""
View Generator for creating VW_1 tier views.

This module provides specialized view generation for:
- Mapping views (VW_1)
- LATERAL FLATTEN for Snowflake
- UNNEST for BigQuery
- JSON functions for PostgreSQL
- Cross-dialect view generation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


class ViewDialect(str, Enum):
    """Supported SQL dialects for view generation."""

    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    BIGQUERY = "bigquery"
    TSQL = "tsql"
    MYSQL = "mysql"


class ViewType(str, Enum):
    """Types of views that can be generated."""

    VW_1_MAPPING = "vw_1_mapping"       # Standard mapping view
    VW_1_UNNEST = "vw_1_unnest"         # Unnested array view
    VW_1_FILTERED = "vw_1_filtered"     # Filtered mapping view
    VW_1_ROLLUP = "vw_1_rollup"         # Rollup hierarchy view
    VW_1_PRECEDENCE = "vw_1_precedence" # Precedence-based view


@dataclass
class ViewConfig:
    """Configuration for view generation."""

    target_schema: str = "HIERARCHIES"
    dialect: ViewDialect = ViewDialect.SNOWFLAKE
    include_all_levels: bool = True
    max_levels: int = 10
    include_audit_columns: bool = True
    include_precedence: bool = False
    filter_active_only: bool = True
    custom_columns: list[str] = field(default_factory=list)


@dataclass
class GeneratedView:
    """A generated view definition."""

    name: str
    view_type: ViewType
    dialect: ViewDialect
    ddl: str
    source_table: str
    target_schema: str
    columns: list[str]
    description: str = ""
    generated_at: datetime = field(default_factory=datetime.now)
    hierarchy_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_full_name(self) -> str:
        """Get fully qualified view name."""
        return f"{self.target_schema}.{self.name}"


class ViewGenerator:
    """
    Generates VW_1 tier views for hierarchy mappings.

    Supports multiple SQL dialects and view types:
    - Standard mapping views
    - Unnested array views for Snowflake/BigQuery
    - Filtered views with active-only records
    - Rollup views for aggregation
    - Precedence-based views for mapping priority

    Example:
        generator = ViewGenerator(dialect=ViewDialect.SNOWFLAKE)

        # Generate from a hierarchy
        view = generator.generate_mapping_view(
            hierarchy=converted_hierarchy,
            config=ViewConfig(target_schema="ANALYTICS")
        )

        # Get the DDL
        print(view.ddl)
    """

    def __init__(self, dialect: ViewDialect = ViewDialect.SNOWFLAKE):
        """
        Initialize the view generator.

        Args:
            dialect: Target SQL dialect
        """
        self.dialect = dialect

    def generate_mapping_view(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig | None = None,
    ) -> GeneratedView:
        """
        Generate a standard VW_1 mapping view.

        Args:
            hierarchy: Converted hierarchy to generate view for
            config: View configuration

        Returns:
            Generated view with DDL
        """
        config = config or ViewConfig(dialect=self.dialect)
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        columns = self._build_column_list(hierarchy, config)
        ddl = self._build_view_ddl(
            view_name=view_name,
            source_table=source_table,
            columns=columns,
            config=config,
            view_type=ViewType.VW_1_MAPPING,
        )

        return GeneratedView(
            name=view_name,
            view_type=ViewType.VW_1_MAPPING,
            dialect=config.dialect,
            ddl=ddl,
            source_table=source_table,
            target_schema=config.target_schema,
            columns=columns,
            description=f"Mapping view for {hierarchy.name} hierarchy",
            hierarchy_id=hierarchy.id,
        )

    def generate_unnest_view(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig | None = None,
        mapping_column: str = "SOURCE_MAPPINGS",
    ) -> GeneratedView:
        """
        Generate an unnest view for array-based mappings.

        Uses dialect-specific unnest operations:
        - Snowflake: LATERAL FLATTEN
        - BigQuery: UNNEST
        - PostgreSQL: json_array_elements

        Args:
            hierarchy: Converted hierarchy
            config: View configuration
            mapping_column: Column containing array data

        Returns:
            Generated unnest view
        """
        config = config or ViewConfig(dialect=self.dialect)
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}_UNNEST")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        columns = self._build_column_list(hierarchy, config)
        ddl = self._build_unnest_ddl(
            view_name=view_name,
            source_table=source_table,
            columns=columns,
            config=config,
            mapping_column=mapping_column,
        )

        return GeneratedView(
            name=view_name,
            view_type=ViewType.VW_1_UNNEST,
            dialect=config.dialect,
            ddl=ddl,
            source_table=source_table,
            target_schema=config.target_schema,
            columns=columns + ["SOURCE_UID", "MAPPING_INDEX"],
            description=f"Unnest view for {hierarchy.name} mappings",
            hierarchy_id=hierarchy.id,
        )

    def generate_filtered_view(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig | None = None,
        filter_conditions: list[str] | None = None,
    ) -> GeneratedView:
        """
        Generate a filtered mapping view.

        Args:
            hierarchy: Converted hierarchy
            config: View configuration
            filter_conditions: Additional WHERE conditions

        Returns:
            Generated filtered view
        """
        config = config or ViewConfig(dialect=self.dialect)
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}_FILTERED")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        columns = self._build_column_list(hierarchy, config)
        conditions = ["INCLUDE_FLAG = TRUE", "EXCLUDE_FLAG = FALSE"]

        if filter_conditions:
            conditions.extend(filter_conditions)

        ddl = self._build_view_ddl(
            view_name=view_name,
            source_table=source_table,
            columns=columns,
            config=config,
            view_type=ViewType.VW_1_FILTERED,
            where_conditions=conditions,
        )

        return GeneratedView(
            name=view_name,
            view_type=ViewType.VW_1_FILTERED,
            dialect=config.dialect,
            ddl=ddl,
            source_table=source_table,
            target_schema=config.target_schema,
            columns=columns,
            description=f"Filtered view for {hierarchy.name} (active only)",
            hierarchy_id=hierarchy.id,
            metadata={"filter_conditions": conditions},
        )

    def generate_rollup_view(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig | None = None,
        rollup_levels: list[int] | None = None,
    ) -> GeneratedView:
        """
        Generate a rollup view for hierarchy aggregation.

        Args:
            hierarchy: Converted hierarchy
            config: View configuration
            rollup_levels: Levels to include in rollup (default: all)

        Returns:
            Generated rollup view
        """
        config = config or ViewConfig(dialect=self.dialect)
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}_ROLLUP")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        if rollup_levels is None:
            rollup_levels = list(range(1, min(hierarchy.level_count + 1, config.max_levels + 1)))

        ddl = self._build_rollup_ddl(
            view_name=view_name,
            source_table=source_table,
            config=config,
            rollup_levels=rollup_levels,
        )

        return GeneratedView(
            name=view_name,
            view_type=ViewType.VW_1_ROLLUP,
            dialect=config.dialect,
            ddl=ddl,
            source_table=source_table,
            target_schema=config.target_schema,
            columns=[f"LEVEL_{i}" for i in rollup_levels] + ["MEMBER_COUNT", "MIN_SORT", "MAX_SORT"],
            description=f"Rollup view for {hierarchy.name}",
            hierarchy_id=hierarchy.id,
            metadata={"rollup_levels": rollup_levels},
        )

    def generate_precedence_view(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig | None = None,
    ) -> GeneratedView:
        """
        Generate a precedence-based mapping view.

        This view resolves mapping conflicts using PRECEDENCE_GROUP
        to determine which mapping takes priority.

        Args:
            hierarchy: Converted hierarchy
            config: View configuration

        Returns:
            Generated precedence view
        """
        config = config or ViewConfig(dialect=self.dialect)
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}_PRECEDENCE")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")
        mapping_table = self._sanitize_name(f"TBL_0_{hierarchy.name}_MAPPING")

        ddl = self._build_precedence_ddl(
            view_name=view_name,
            source_table=source_table,
            mapping_table=mapping_table,
            config=config,
        )

        return GeneratedView(
            name=view_name,
            view_type=ViewType.VW_1_PRECEDENCE,
            dialect=config.dialect,
            ddl=ddl,
            source_table=source_table,
            target_schema=config.target_schema,
            columns=self._build_column_list(hierarchy, config) + ["SOURCE_UID", "PRECEDENCE_GROUP", "PRECEDENCE_RANK"],
            description=f"Precedence view for {hierarchy.name} mappings",
            hierarchy_id=hierarchy.id,
        )

    def generate_all_views(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig | None = None,
        view_types: list[ViewType] | None = None,
    ) -> list[GeneratedView]:
        """
        Generate multiple view types for a hierarchy.

        Args:
            hierarchy: Converted hierarchy
            config: View configuration
            view_types: Types of views to generate (default: mapping only)

        Returns:
            List of generated views
        """
        config = config or ViewConfig(dialect=self.dialect)
        views = []

        if view_types is None:
            view_types = [ViewType.VW_1_MAPPING]

        for view_type in view_types:
            if view_type == ViewType.VW_1_MAPPING:
                views.append(self.generate_mapping_view(hierarchy, config))
            elif view_type == ViewType.VW_1_UNNEST:
                views.append(self.generate_unnest_view(hierarchy, config))
            elif view_type == ViewType.VW_1_FILTERED:
                views.append(self.generate_filtered_view(hierarchy, config))
            elif view_type == ViewType.VW_1_ROLLUP:
                views.append(self.generate_rollup_view(hierarchy, config))
            elif view_type == ViewType.VW_1_PRECEDENCE:
                views.append(self.generate_precedence_view(hierarchy, config))

        return views

    def _build_column_list(
        self,
        hierarchy: ConvertedHierarchy,
        config: ViewConfig,
    ) -> list[str]:
        """Build the list of columns for the view."""
        columns = [
            "HIERARCHY_ID",
            "HIERARCHY_NAME",
            "PARENT_ID",
        ]

        # Add level columns
        level_count = min(hierarchy.level_count, config.max_levels) if config.include_all_levels else 3
        for i in range(1, level_count + 2):  # Add one extra level
            columns.append(f"LEVEL_{i}")
            columns.append(f"LEVEL_{i}_SORT")

        # Add standard columns
        columns.extend([
            "INCLUDE_FLAG",
            "EXCLUDE_FLAG",
            "FORMULA_GROUP",
            "SORT_ORDER",
        ])

        # Add audit columns if requested
        if config.include_audit_columns:
            columns.extend(["CREATED_AT", "UPDATED_AT"])

        # Add custom columns
        if config.custom_columns:
            columns.extend(config.custom_columns)

        return columns

    def _build_view_ddl(
        self,
        view_name: str,
        source_table: str,
        columns: list[str],
        config: ViewConfig,
        view_type: ViewType,
        where_conditions: list[str] | None = None,
    ) -> str:
        """Build CREATE VIEW DDL."""
        column_str = ",\n    ".join(f"h.{col}" for col in columns)

        ddl = f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {column_str}
FROM {config.target_schema}.{source_table} h
"""

        if where_conditions:
            ddl += "WHERE " + "\n  AND ".join(where_conditions)
        elif config.filter_active_only:
            ddl += "WHERE h.INCLUDE_FLAG = TRUE"

        ddl += ";"
        return ddl

    def _build_unnest_ddl(
        self,
        view_name: str,
        source_table: str,
        columns: list[str],
        config: ViewConfig,
        mapping_column: str,
    ) -> str:
        """Build unnest view DDL based on dialect."""
        column_str = ",\n    ".join(f"h.{col}" for col in columns)

        if config.dialect == ViewDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {column_str},
    m.value::VARCHAR AS SOURCE_UID,
    m.index AS MAPPING_INDEX
FROM {config.target_schema}.{source_table} h,
LATERAL FLATTEN(input => PARSE_JSON(h.{mapping_column})) m
WHERE h.INCLUDE_FLAG = TRUE;
"""

        elif config.dialect == ViewDialect.BIGQUERY:
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {column_str},
    mapping AS SOURCE_UID,
    offset AS MAPPING_INDEX
FROM {config.target_schema}.{source_table} h,
UNNEST(JSON_EXTRACT_ARRAY(h.{mapping_column})) AS mapping WITH OFFSET
WHERE h.INCLUDE_FLAG = TRUE;
"""

        elif config.dialect == ViewDialect.POSTGRESQL:
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {column_str},
    m.value::TEXT AS SOURCE_UID,
    m.ordinality - 1 AS MAPPING_INDEX
FROM {config.target_schema}.{source_table} h
CROSS JOIN LATERAL json_array_elements(h.{mapping_column}::json) WITH ORDINALITY AS m(value, ordinality)
WHERE h.INCLUDE_FLAG = TRUE;
"""

        else:
            # Generic fallback
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {column_str}
FROM {config.target_schema}.{source_table} h
WHERE h.INCLUDE_FLAG = TRUE;
"""

    def _build_rollup_ddl(
        self,
        view_name: str,
        source_table: str,
        config: ViewConfig,
        rollup_levels: list[int],
    ) -> str:
        """Build rollup view DDL."""
        level_cols = ", ".join(f"LEVEL_{i}" for i in rollup_levels)

        if config.dialect == ViewDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {level_cols},
    COUNT(*) AS MEMBER_COUNT,
    MIN(SORT_ORDER) AS MIN_SORT,
    MAX(SORT_ORDER) AS MAX_SORT
FROM {config.target_schema}.{source_table}
WHERE INCLUDE_FLAG = TRUE
GROUP BY ROLLUP({level_cols});
"""

        elif config.dialect == ViewDialect.BIGQUERY:
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {level_cols},
    COUNT(*) AS MEMBER_COUNT,
    MIN(SORT_ORDER) AS MIN_SORT,
    MAX(SORT_ORDER) AS MAX_SORT
FROM {config.target_schema}.{source_table}
WHERE INCLUDE_FLAG = TRUE
GROUP BY ROLLUP({level_cols});
"""

        else:
            # Standard ROLLUP
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    {level_cols},
    COUNT(*) AS MEMBER_COUNT,
    MIN(SORT_ORDER) AS MIN_SORT,
    MAX(SORT_ORDER) AS MAX_SORT
FROM {config.target_schema}.{source_table}
WHERE INCLUDE_FLAG = TRUE
GROUP BY {level_cols};
"""

    def _build_precedence_ddl(
        self,
        view_name: str,
        source_table: str,
        mapping_table: str,
        config: ViewConfig,
    ) -> str:
        """Build precedence-based view DDL."""
        if config.dialect == ViewDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
WITH ranked_mappings AS (
    SELECT
        h.HIERARCHY_ID,
        h.HIERARCHY_NAME,
        h.PARENT_ID,
        h.LEVEL_1,
        h.LEVEL_2,
        h.LEVEL_3,
        h.LEVEL_4,
        h.LEVEL_5,
        m.SOURCE_UID,
        m.PRECEDENCE_GROUP,
        ROW_NUMBER() OVER (
            PARTITION BY m.SOURCE_UID
            ORDER BY m.PRECEDENCE_GROUP ASC, m.MAPPING_INDEX ASC
        ) AS PRECEDENCE_RANK
    FROM {config.target_schema}.{source_table} h
    JOIN {config.target_schema}.{mapping_table} m
        ON h.HIERARCHY_ID = m.HIERARCHY_ID
    WHERE h.INCLUDE_FLAG = TRUE
)
SELECT *
FROM ranked_mappings
WHERE PRECEDENCE_RANK = 1;
"""

        else:
            # Generic SQL
            return f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
WITH ranked_mappings AS (
    SELECT
        h.HIERARCHY_ID,
        h.HIERARCHY_NAME,
        h.PARENT_ID,
        h.LEVEL_1,
        h.LEVEL_2,
        h.LEVEL_3,
        h.LEVEL_4,
        h.LEVEL_5,
        m.SOURCE_UID,
        m.PRECEDENCE_GROUP,
        ROW_NUMBER() OVER (
            PARTITION BY m.SOURCE_UID
            ORDER BY m.PRECEDENCE_GROUP ASC, m.MAPPING_INDEX ASC
        ) AS PRECEDENCE_RANK
    FROM {config.target_schema}.{source_table} h
    JOIN {config.target_schema}.{mapping_table} m
        ON h.HIERARCHY_ID = m.HIERARCHY_ID
    WHERE h.INCLUDE_FLAG = TRUE
)
SELECT *
FROM ranked_mappings
WHERE PRECEDENCE_RANK = 1;
"""

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use in SQL identifiers."""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_')
        return sanitized.upper()

    def to_dict(self, view: GeneratedView) -> dict[str, Any]:
        """Convert GeneratedView to dictionary."""
        return {
            "name": view.name,
            "view_type": view.view_type.value,
            "dialect": view.dialect.value,
            "ddl": view.ddl,
            "source_table": view.source_table,
            "target_schema": view.target_schema,
            "columns": view.columns,
            "description": view.description,
            "generated_at": view.generated_at.isoformat(),
            "hierarchy_id": view.hierarchy_id,
            "metadata": view.metadata,
        }
