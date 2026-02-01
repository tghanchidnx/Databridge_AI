"""
SQL Generator for creating DDL scripts.

This module generates DDL scripts for multiple SQL dialects:
- Snowflake
- PostgreSQL
- BigQuery
- SQL Server (T-SQL)
- MySQL
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


class SQLDialect(str, Enum):
    """Supported SQL dialects."""

    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    BIGQUERY = "bigquery"
    TSQL = "tsql"
    MYSQL = "mysql"


class DDLType(str, Enum):
    """Types of DDL statements."""

    CREATE_TABLE = "create_table"
    CREATE_VIEW = "create_view"
    ALTER_TABLE = "alter_table"
    DROP_TABLE = "drop_table"
    INSERT = "insert"
    MERGE = "merge"
    CREATE_SCHEMA = "create_schema"


@dataclass
class ColumnDefinition:
    """Definition of a table column."""

    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None
    primary_key: bool = False
    unique: bool = False
    comment: str | None = None


@dataclass
class TableDefinition:
    """Definition of a table."""

    name: str
    schema: str
    columns: list[ColumnDefinition]
    primary_key: list[str] | None = None
    comment: str | None = None
    clustering_keys: list[str] | None = None


@dataclass
class GeneratedDDL:
    """Result of DDL generation."""

    ddl_type: DDLType
    dialect: SQLDialect
    sql: str
    object_name: str
    schema_name: str
    description: str = ""
    generated_at: datetime = field(default_factory=datetime.now)
    dependencies: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_full_name(self) -> str:
        """Get fully qualified object name."""
        return f"{self.schema_name}.{self.object_name}"


@dataclass
class SQLGeneratorConfig:
    """Configuration for SQL generation."""

    dialect: SQLDialect = SQLDialect.SNOWFLAKE
    target_database: str = ""
    target_schema: str = "HIERARCHIES"
    include_drop: bool = True
    include_comments: bool = True
    use_create_or_replace: bool = True
    generate_grants: bool = False
    grant_roles: list[str] = field(default_factory=list)


class SQLGenerator:
    """
    Generates DDL scripts for hierarchy tables and views.

    Supports multiple SQL dialects with dialect-specific syntax:
    - Data types
    - CREATE OR REPLACE vs DROP/CREATE
    - JSON handling
    - Clustering/partitioning

    Example:
        generator = SQLGenerator(dialect=SQLDialect.SNOWFLAKE)

        # Generate CREATE TABLE
        ddl = generator.generate_table_ddl(
            hierarchy=converted_hierarchy,
            config=SQLGeneratorConfig(target_schema="ANALYTICS")
        )

        print(ddl.sql)
    """

    # Type mappings per dialect
    TYPE_MAPPINGS: dict[SQLDialect, dict[str, str]] = {
        SQLDialect.SNOWFLAKE: {
            "string": "VARCHAR",
            "integer": "INTEGER",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP_NTZ",
            "json": "VARIANT",
            "text": "TEXT",
        },
        SQLDialect.POSTGRESQL: {
            "string": "VARCHAR",
            "integer": "INTEGER",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "json": "JSONB",
            "text": "TEXT",
        },
        SQLDialect.BIGQUERY: {
            "string": "STRING",
            "integer": "INT64",
            "boolean": "BOOL",
            "timestamp": "TIMESTAMP",
            "json": "JSON",
            "text": "STRING",
        },
        SQLDialect.TSQL: {
            "string": "NVARCHAR",
            "integer": "INT",
            "boolean": "BIT",
            "timestamp": "DATETIME2",
            "json": "NVARCHAR(MAX)",
            "text": "NVARCHAR(MAX)",
        },
        SQLDialect.MYSQL: {
            "string": "VARCHAR",
            "integer": "INT",
            "boolean": "TINYINT(1)",
            "timestamp": "DATETIME",
            "json": "JSON",
            "text": "TEXT",
        },
    }

    def __init__(self, dialect: SQLDialect = SQLDialect.SNOWFLAKE):
        """
        Initialize the SQL generator.

        Args:
            dialect: Target SQL dialect
        """
        self.dialect = dialect

    def generate_table_ddl(
        self,
        hierarchy: ConvertedHierarchy,
        config: SQLGeneratorConfig | None = None,
    ) -> GeneratedDDL:
        """
        Generate CREATE TABLE DDL for a hierarchy.

        Args:
            hierarchy: Converted hierarchy
            config: Generation configuration

        Returns:
            Generated DDL
        """
        config = config or SQLGeneratorConfig(dialect=self.dialect)
        table_name = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        # Build column definitions
        columns = self._build_hierarchy_columns(hierarchy, config)

        # Generate DDL
        sql = self._build_create_table(
            table_name=table_name,
            columns=columns,
            config=config,
            primary_key="HIERARCHY_ID",
            comment=f"Hierarchy table for {hierarchy.name}",
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_TABLE,
            dialect=config.dialect,
            sql=sql,
            object_name=table_name,
            schema_name=config.target_schema,
            description=f"Hierarchy table for {hierarchy.name}",
            metadata={
                "hierarchy_id": hierarchy.id,
                "level_count": hierarchy.level_count,
            },
        )

    def generate_view_ddl(
        self,
        hierarchy: ConvertedHierarchy,
        config: SQLGeneratorConfig | None = None,
        view_type: str = "mapping",
    ) -> GeneratedDDL:
        """
        Generate CREATE VIEW DDL.

        Args:
            hierarchy: Converted hierarchy
            config: Generation configuration
            view_type: Type of view to generate

        Returns:
            Generated DDL
        """
        config = config or SQLGeneratorConfig(dialect=self.dialect)
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        sql = self._build_create_view(
            view_name=view_name,
            source_table=source_table,
            config=config,
            hierarchy=hierarchy,
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_VIEW,
            dialect=config.dialect,
            sql=sql,
            object_name=view_name,
            schema_name=config.target_schema,
            description=f"Mapping view for {hierarchy.name}",
            dependencies=[f"{config.target_schema}.{source_table}"],
        )

    def generate_insert_ddl(
        self,
        hierarchy: ConvertedHierarchy,
        config: SQLGeneratorConfig | None = None,
    ) -> GeneratedDDL:
        """
        Generate INSERT statements for hierarchy data.

        Args:
            hierarchy: Converted hierarchy with nodes
            config: Generation configuration

        Returns:
            Generated DDL with INSERT statements
        """
        config = config or SQLGeneratorConfig(dialect=self.dialect)
        table_name = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        inserts = []
        for node_id, node in hierarchy.nodes.items():
            insert = self._build_insert(
                table_name=table_name,
                node_id=node_id,
                node=node,
                config=config,
            )
            inserts.append(insert)

        sql = "\n".join(inserts)

        return GeneratedDDL(
            ddl_type=DDLType.INSERT,
            dialect=config.dialect,
            sql=sql,
            object_name=table_name,
            schema_name=config.target_schema,
            description=f"INSERT statements for {hierarchy.name} ({len(hierarchy.nodes)} rows)",
            dependencies=[f"{config.target_schema}.{table_name}"],
        )

    def generate_merge_ddl(
        self,
        hierarchy: ConvertedHierarchy,
        config: SQLGeneratorConfig | None = None,
        staging_table: str | None = None,
    ) -> GeneratedDDL:
        """
        Generate MERGE statement for upsert operations.

        Args:
            hierarchy: Converted hierarchy
            config: Generation configuration
            staging_table: Name of staging table (default: _STG suffix)

        Returns:
            Generated MERGE DDL
        """
        config = config or SQLGeneratorConfig(dialect=self.dialect)
        target_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")
        staging = staging_table or f"{target_table}_STG"

        sql = self._build_merge(
            target_table=target_table,
            staging_table=staging,
            config=config,
            hierarchy=hierarchy,
        )

        return GeneratedDDL(
            ddl_type=DDLType.MERGE,
            dialect=config.dialect,
            sql=sql,
            object_name=target_table,
            schema_name=config.target_schema,
            description=f"MERGE statement for {hierarchy.name}",
            dependencies=[
                f"{config.target_schema}.{target_table}",
                f"{config.target_schema}.{staging}",
            ],
        )

    def generate_schema_ddl(
        self,
        config: SQLGeneratorConfig | None = None,
    ) -> GeneratedDDL:
        """
        Generate CREATE SCHEMA DDL.

        Args:
            config: Generation configuration

        Returns:
            Generated DDL
        """
        config = config or SQLGeneratorConfig(dialect=self.dialect)

        if config.dialect == SQLDialect.SNOWFLAKE:
            sql = f"CREATE SCHEMA IF NOT EXISTS {config.target_schema};"
        elif config.dialect == SQLDialect.POSTGRESQL:
            sql = f"CREATE SCHEMA IF NOT EXISTS {config.target_schema};"
        elif config.dialect == SQLDialect.BIGQUERY:
            sql = f"CREATE SCHEMA IF NOT EXISTS {config.target_database}.{config.target_schema};"
        elif config.dialect == SQLDialect.TSQL:
            sql = f"""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{config.target_schema}')
BEGIN
    EXEC('CREATE SCHEMA {config.target_schema}')
END;"""
        else:
            sql = f"CREATE SCHEMA IF NOT EXISTS {config.target_schema};"

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_SCHEMA,
            dialect=config.dialect,
            sql=sql,
            object_name=config.target_schema,
            schema_name="",
            description=f"Create schema {config.target_schema}",
        )

    def generate_deployment_script(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: SQLGeneratorConfig | None = None,
    ) -> str:
        """
        Generate a complete deployment script for multiple hierarchies.

        Args:
            hierarchies: List of hierarchies
            config: Generation configuration

        Returns:
            Complete deployment script
        """
        config = config or SQLGeneratorConfig(dialect=self.dialect)
        parts = []

        # Header
        parts.append(f"""-- ==============================================
-- Deployment Script
-- Generated: {datetime.now().isoformat()}
-- Dialect: {config.dialect.value}
-- Schema: {config.target_schema}
-- ==============================================
""")

        # Create schema
        schema_ddl = self.generate_schema_ddl(config)
        parts.append(f"-- Create Schema\n{schema_ddl.sql}\n")

        # Generate for each hierarchy
        for hier in hierarchies:
            parts.append(f"\n-- ==============================================")
            parts.append(f"-- Hierarchy: {hier.name}")
            parts.append(f"-- ==============================================\n")

            # Table
            table_ddl = self.generate_table_ddl(hier, config)
            parts.append(f"-- Create Table\n{table_ddl.sql}\n")

            # View
            view_ddl = self.generate_view_ddl(hier, config)
            parts.append(f"-- Create View\n{view_ddl.sql}\n")

            # Data
            insert_ddl = self.generate_insert_ddl(hier, config)
            if insert_ddl.sql:
                parts.append(f"-- Insert Data\n{insert_ddl.sql}\n")

        return "\n".join(parts)

    def _build_hierarchy_columns(
        self,
        hierarchy: ConvertedHierarchy,
        config: SQLGeneratorConfig,
    ) -> list[ColumnDefinition]:
        """Build column definitions for hierarchy table."""
        type_map = self.TYPE_MAPPINGS[config.dialect]

        columns = [
            ColumnDefinition(
                name="HIERARCHY_ID",
                data_type=f"{type_map['string']}(100)",
                nullable=False,
                primary_key=True,
                comment="Unique hierarchy node identifier",
            ),
            ColumnDefinition(
                name="HIERARCHY_NAME",
                data_type=f"{type_map['string']}(500)",
                nullable=True,
                comment="Display name",
            ),
            ColumnDefinition(
                name="PARENT_ID",
                data_type=f"{type_map['string']}(100)",
                nullable=True,
                comment="Parent node identifier",
            ),
            ColumnDefinition(
                name="DESCRIPTION",
                data_type=f"{type_map['string']}(2000)",
                nullable=True,
                comment="Node description",
            ),
        ]

        # Add level columns
        for i in range(1, min(hierarchy.level_count + 2, 11)):
            columns.append(ColumnDefinition(
                name=f"LEVEL_{i}",
                data_type=f"{type_map['string']}(500)",
                nullable=True,
                comment=f"Hierarchy level {i} value",
            ))
            columns.append(ColumnDefinition(
                name=f"LEVEL_{i}_SORT",
                data_type=type_map['integer'],
                nullable=True,
                comment=f"Sort order for level {i}",
            ))

        # Add standard columns
        columns.extend([
            ColumnDefinition(
                name="INCLUDE_FLAG",
                data_type=type_map['boolean'],
                nullable=True,
                default="TRUE",
                comment="Include in output",
            ),
            ColumnDefinition(
                name="EXCLUDE_FLAG",
                data_type=type_map['boolean'],
                nullable=True,
                default="FALSE",
                comment="Exclude from output",
            ),
            ColumnDefinition(
                name="FORMULA_GROUP",
                data_type=f"{type_map['string']}(100)",
                nullable=True,
                comment="Formula calculation group",
            ),
            ColumnDefinition(
                name="SORT_ORDER",
                data_type=type_map['integer'],
                nullable=True,
                comment="Overall sort order",
            ),
            ColumnDefinition(
                name="CREATED_AT",
                data_type=type_map['timestamp'],
                nullable=True,
                default="CURRENT_TIMESTAMP",
                comment="Record creation timestamp",
            ),
            ColumnDefinition(
                name="UPDATED_AT",
                data_type=type_map['timestamp'],
                nullable=True,
                default="CURRENT_TIMESTAMP",
                comment="Record update timestamp",
            ),
        ])

        return columns

    def _build_create_table(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        config: SQLGeneratorConfig,
        primary_key: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Build CREATE TABLE statement."""
        lines = []

        # Drop if requested
        if config.include_drop and not config.use_create_or_replace:
            lines.append(f"DROP TABLE IF EXISTS {config.target_schema}.{table_name};")
            lines.append("")

        # CREATE statement
        if config.use_create_or_replace and config.dialect == SQLDialect.SNOWFLAKE:
            lines.append(f"CREATE OR REPLACE TABLE {config.target_schema}.{table_name} (")
        else:
            lines.append(f"CREATE TABLE IF NOT EXISTS {config.target_schema}.{table_name} (")

        # Column definitions
        col_lines = []
        for col in columns:
            col_def = f"    {col.name} {col.data_type}"

            if not col.nullable:
                col_def += " NOT NULL"

            if col.default:
                default_val = col.default
                if config.dialect == SQLDialect.BIGQUERY and col.default == "CURRENT_TIMESTAMP":
                    default_val = "CURRENT_TIMESTAMP()"
                col_def += f" DEFAULT {default_val}"

            col_lines.append(col_def)

        # Add primary key constraint
        if primary_key:
            if config.dialect == SQLDialect.BIGQUERY:
                # BigQuery doesn't support PK in CREATE TABLE
                pass
            else:
                col_lines.append(f"    PRIMARY KEY ({primary_key})")

        lines.append(",\n".join(col_lines))
        lines.append(");")

        # Add comment if supported
        if comment and config.include_comments:
            if config.dialect == SQLDialect.SNOWFLAKE:
                lines.append(f"\nCOMMENT ON TABLE {config.target_schema}.{table_name} IS '{self._escape_sql(comment)}';")
            elif config.dialect == SQLDialect.POSTGRESQL:
                lines.append(f"\nCOMMENT ON TABLE {config.target_schema}.{table_name} IS '{self._escape_sql(comment)}';")

        return "\n".join(lines)

    def _build_create_view(
        self,
        view_name: str,
        source_table: str,
        config: SQLGeneratorConfig,
        hierarchy: ConvertedHierarchy,
    ) -> str:
        """Build CREATE VIEW statement."""
        lines = []

        # Select columns
        select_cols = [
            "h.HIERARCHY_ID",
            "h.HIERARCHY_NAME",
            "h.PARENT_ID",
        ]

        for i in range(1, min(hierarchy.level_count + 2, 6)):
            select_cols.append(f"h.LEVEL_{i}")
            select_cols.append(f"h.LEVEL_{i}_SORT")

        select_cols.extend([
            "h.INCLUDE_FLAG",
            "h.EXCLUDE_FLAG",
            "h.FORMULA_GROUP",
            "h.SORT_ORDER",
        ])

        col_str = ",\n    ".join(select_cols)

        if config.use_create_or_replace:
            lines.append(f"CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS")
        else:
            if config.include_drop:
                lines.append(f"DROP VIEW IF EXISTS {config.target_schema}.{view_name};")
            lines.append(f"CREATE VIEW {config.target_schema}.{view_name} AS")

        lines.append("SELECT")
        lines.append(f"    {col_str}")
        lines.append(f"FROM {config.target_schema}.{source_table} h")
        lines.append("WHERE h.INCLUDE_FLAG = TRUE;")

        return "\n".join(lines)

    def _build_insert(
        self,
        table_name: str,
        node_id: str,
        node: Any,
        config: SQLGeneratorConfig,
    ) -> str:
        """Build INSERT statement for a single node."""
        # Build level values
        level_values = ["NULL"] * 10
        level_sorts = ["NULL"] * 10
        if node.level > 0 and node.level <= 10:
            level_values[node.level - 1] = f"'{self._escape_sql(node.value)}'"
            level_sorts[node.level - 1] = str(node.sort_order)

        parent_val = f"'{node.parent_id}'" if node.parent_id else "NULL"

        return f"""INSERT INTO {config.target_schema}.{table_name} (
    HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID,
    LEVEL_1, LEVEL_1_SORT, LEVEL_2, LEVEL_2_SORT, LEVEL_3, LEVEL_3_SORT,
    INCLUDE_FLAG, EXCLUDE_FLAG, SORT_ORDER
) VALUES (
    '{node_id}', '{self._escape_sql(node.name)}', {parent_val},
    {level_values[0]}, {level_sorts[0]}, {level_values[1]}, {level_sorts[1]}, {level_values[2]}, {level_sorts[2]},
    TRUE, FALSE, {node.sort_order}
);"""

    def _build_merge(
        self,
        target_table: str,
        staging_table: str,
        config: SQLGeneratorConfig,
        hierarchy: ConvertedHierarchy,
    ) -> str:
        """Build MERGE statement."""
        if config.dialect == SQLDialect.SNOWFLAKE:
            return f"""MERGE INTO {config.target_schema}.{target_table} t
USING {config.target_schema}.{staging_table} s
ON t.HIERARCHY_ID = s.HIERARCHY_ID
WHEN MATCHED THEN UPDATE SET
    t.HIERARCHY_NAME = s.HIERARCHY_NAME,
    t.PARENT_ID = s.PARENT_ID,
    t.LEVEL_1 = s.LEVEL_1,
    t.LEVEL_2 = s.LEVEL_2,
    t.LEVEL_3 = s.LEVEL_3,
    t.INCLUDE_FLAG = s.INCLUDE_FLAG,
    t.EXCLUDE_FLAG = s.EXCLUDE_FLAG,
    t.SORT_ORDER = s.SORT_ORDER,
    t.UPDATED_AT = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID,
    LEVEL_1, LEVEL_2, LEVEL_3,
    INCLUDE_FLAG, EXCLUDE_FLAG, SORT_ORDER,
    CREATED_AT, UPDATED_AT
) VALUES (
    s.HIERARCHY_ID, s.HIERARCHY_NAME, s.PARENT_ID,
    s.LEVEL_1, s.LEVEL_2, s.LEVEL_3,
    s.INCLUDE_FLAG, s.EXCLUDE_FLAG, s.SORT_ORDER,
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);"""

        elif config.dialect == SQLDialect.POSTGRESQL:
            return f"""INSERT INTO {config.target_schema}.{target_table} (
    HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID,
    LEVEL_1, LEVEL_2, LEVEL_3,
    INCLUDE_FLAG, EXCLUDE_FLAG, SORT_ORDER
)
SELECT
    s.HIERARCHY_ID, s.HIERARCHY_NAME, s.PARENT_ID,
    s.LEVEL_1, s.LEVEL_2, s.LEVEL_3,
    s.INCLUDE_FLAG, s.EXCLUDE_FLAG, s.SORT_ORDER
FROM {config.target_schema}.{staging_table} s
ON CONFLICT (HIERARCHY_ID)
DO UPDATE SET
    HIERARCHY_NAME = EXCLUDED.HIERARCHY_NAME,
    PARENT_ID = EXCLUDED.PARENT_ID,
    LEVEL_1 = EXCLUDED.LEVEL_1,
    LEVEL_2 = EXCLUDED.LEVEL_2,
    LEVEL_3 = EXCLUDED.LEVEL_3,
    INCLUDE_FLAG = EXCLUDED.INCLUDE_FLAG,
    EXCLUDE_FLAG = EXCLUDED.EXCLUDE_FLAG,
    SORT_ORDER = EXCLUDED.SORT_ORDER,
    UPDATED_AT = CURRENT_TIMESTAMP;"""

        else:
            # Generic MERGE for other dialects
            return f"""MERGE INTO {config.target_schema}.{target_table} t
USING {config.target_schema}.{staging_table} s
ON t.HIERARCHY_ID = s.HIERARCHY_ID
WHEN MATCHED THEN UPDATE SET
    t.HIERARCHY_NAME = s.HIERARCHY_NAME,
    t.PARENT_ID = s.PARENT_ID,
    t.UPDATED_AT = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, CREATED_AT
) VALUES (
    s.HIERARCHY_ID, s.HIERARCHY_NAME, s.PARENT_ID, CURRENT_TIMESTAMP
);"""

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use in SQL identifiers."""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_')
        return sanitized.upper()

    def _escape_sql(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''")

    def to_dict(self, ddl: GeneratedDDL) -> dict[str, Any]:
        """Convert GeneratedDDL to dictionary."""
        return {
            "ddl_type": ddl.ddl_type.value,
            "dialect": ddl.dialect.value,
            "object_name": ddl.object_name,
            "schema_name": ddl.schema_name,
            "full_name": ddl.get_full_name(),
            "description": ddl.description,
            "generated_at": ddl.generated_at.isoformat(),
            "dependencies": ddl.dependencies,
            "sql_length": len(ddl.sql),
        }
