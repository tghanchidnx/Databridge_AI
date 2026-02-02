"""
DDL Generator for creating SQL scripts from hierarchy projects.

Supports multiple SQL dialects:
- Snowflake
- PostgreSQL
- BigQuery
- SQL Server (T-SQL)
- MySQL
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


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
    CREATE_DYNAMIC_TABLE = "create_dynamic_table"
    ALTER_TABLE = "alter_table"
    DROP = "drop"
    INSERT = "insert"
    MERGE = "merge"
    GRANT = "grant"


@dataclass
class GeneratedDDL:
    """A generated DDL statement."""

    ddl_type: DDLType
    object_name: str
    schema_name: str
    sql: str
    dialect: SQLDialect
    tier: Optional[str] = None  # TBL_0, VW_1, DT_2, etc.
    description: str = ""
    dependencies: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def full_name(self) -> str:
        """Get fully qualified object name."""
        return f"{self.schema_name}.{self.object_name}"


@dataclass
class DDLConfig:
    """Configuration for DDL generation."""

    dialect: SQLDialect = SQLDialect.SNOWFLAKE
    target_database: str = ""
    target_schema: str = "HIERARCHIES"

    # Generation options
    include_drop: bool = True
    use_create_or_replace: bool = True
    include_comments: bool = True
    include_grants: bool = False
    grant_roles: List[str] = field(default_factory=list)

    # Table options
    clustering_keys: List[str] = field(default_factory=list)
    include_audit_columns: bool = True

    # Tier options
    generate_tbl_0: bool = True  # Base hierarchy table
    generate_vw_1: bool = True   # Unnest view
    generate_dt_2: bool = False  # Dimension join dynamic table
    generate_dt_3a: bool = False  # Pre-aggregation table
    generate_dt_3: bool = False   # Final transactional union


class DDLGenerator:
    """
    Generates DDL scripts from hierarchy projects.

    Creates SQL scripts for:
    - TBL_0: Hierarchy data table (stores hierarchy structure)
    - VW_1: Mapping unnest view (flattens source mappings)
    - DT_2: Dimension join dynamic table
    - DT_3A: Pre-aggregation dynamic table
    - DT_3: Final transactional union table

    Example:
        from src.hierarchy import HierarchyService

        service = HierarchyService()
        project = service.get_project(project_id)

        generator = DDLGenerator()
        scripts = generator.generate(
            project=project,
            hierarchies=service.list_hierarchies(project_id),
            config=DDLConfig(
                dialect=SQLDialect.SNOWFLAKE,
                target_schema="ANALYTICS"
            )
        )

        for script in scripts:
            print(f"-- {script.object_name}")
            print(script.sql)
    """

    # Type mappings per dialect
    TYPE_MAPPINGS: Dict[SQLDialect, Dict[str, str]] = {
        SQLDialect.SNOWFLAKE: {
            "string": "VARCHAR",
            "integer": "INTEGER",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP_NTZ",
            "json": "VARIANT",
            "text": "TEXT",
            "decimal": "NUMBER(18,2)",
        },
        SQLDialect.POSTGRESQL: {
            "string": "VARCHAR(255)",
            "integer": "INTEGER",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "json": "JSONB",
            "text": "TEXT",
            "decimal": "NUMERIC(18,2)",
        },
        SQLDialect.BIGQUERY: {
            "string": "STRING",
            "integer": "INT64",
            "boolean": "BOOL",
            "timestamp": "TIMESTAMP",
            "json": "JSON",
            "text": "STRING",
            "decimal": "NUMERIC",
        },
        SQLDialect.TSQL: {
            "string": "NVARCHAR(255)",
            "integer": "INT",
            "boolean": "BIT",
            "timestamp": "DATETIME2",
            "json": "NVARCHAR(MAX)",
            "text": "NVARCHAR(MAX)",
            "decimal": "DECIMAL(18,2)",
        },
        SQLDialect.MYSQL: {
            "string": "VARCHAR(255)",
            "integer": "INT",
            "boolean": "TINYINT(1)",
            "timestamp": "DATETIME",
            "json": "JSON",
            "text": "TEXT",
            "decimal": "DECIMAL(18,2)",
        },
    }

    def __init__(self, config: Optional[DDLConfig] = None):
        """Initialize the DDL generator."""
        self.config = config or DDLConfig()

    def generate(
        self,
        project: Any,
        hierarchies: List[Any],
        config: Optional[DDLConfig] = None,
    ) -> List[GeneratedDDL]:
        """
        Generate DDL scripts for a hierarchy project.

        Args:
            project: Project object from HierarchyService.
            hierarchies: List of hierarchy objects.
            config: Optional DDL configuration (uses instance config if None).

        Returns:
            List of GeneratedDDL objects.
        """
        cfg = config or self.config
        scripts: List[GeneratedDDL] = []

        # TBL_0: Base hierarchy table
        if cfg.generate_tbl_0:
            scripts.append(self._generate_tbl_0(project, hierarchies, cfg))

        # VW_1: Unnest view
        if cfg.generate_vw_1:
            scripts.append(self._generate_vw_1(project, hierarchies, cfg))

        # DT_2: Dimension join table
        if cfg.generate_dt_2:
            scripts.append(self._generate_dt_2(project, hierarchies, cfg))

        # DT_3A: Pre-aggregation table
        if cfg.generate_dt_3a:
            scripts.append(self._generate_dt_3a(project, hierarchies, cfg))

        # DT_3: Final union table
        if cfg.generate_dt_3:
            scripts.append(self._generate_dt_3(project, hierarchies, cfg))

        # Insert statements
        if cfg.generate_tbl_0 and hierarchies:
            scripts.append(self._generate_inserts(project, hierarchies, cfg))

        return scripts

    def _get_type(self, base_type: str, dialect: SQLDialect) -> str:
        """Get dialect-specific data type."""
        return self.TYPE_MAPPINGS.get(dialect, {}).get(base_type, base_type.upper())

    def _generate_tbl_0(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DDLConfig,
    ) -> GeneratedDDL:
        """Generate TBL_0: Base hierarchy table."""
        table_name = f"TBL_0_{self._safe_name(project.name)}_HIERARCHY"

        # Build column definitions
        columns = self._get_hierarchy_columns(config.dialect)

        # Generate SQL
        sql = self._build_create_table(
            table_name=table_name,
            schema_name=config.target_schema,
            columns=columns,
            config=config,
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_TABLE,
            object_name=table_name,
            schema_name=config.target_schema,
            sql=sql,
            dialect=config.dialect,
            tier="TBL_0",
            description=f"Base hierarchy table for {project.name}",
        )

    def _generate_vw_1(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DDLConfig,
    ) -> GeneratedDDL:
        """Generate VW_1: Mapping unnest view."""
        view_name = f"VW_1_{self._safe_name(project.name)}_MAPPING"
        tbl_name = f"TBL_0_{self._safe_name(project.name)}_HIERARCHY"

        # Build view SQL
        sql = self._build_unnest_view(
            view_name=view_name,
            table_name=tbl_name,
            schema_name=config.target_schema,
            config=config,
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_VIEW,
            object_name=view_name,
            schema_name=config.target_schema,
            sql=sql,
            dialect=config.dialect,
            tier="VW_1",
            description=f"Mapping unnest view for {project.name}",
            dependencies=[f"{config.target_schema}.{tbl_name}"],
        )

    def _generate_dt_2(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DDLConfig,
    ) -> GeneratedDDL:
        """Generate DT_2: Dimension join dynamic table."""
        table_name = f"DT_2_{self._safe_name(project.name)}_DIMENSION"
        vw_name = f"VW_1_{self._safe_name(project.name)}_MAPPING"

        sql = self._build_dimension_table(
            table_name=table_name,
            view_name=vw_name,
            schema_name=config.target_schema,
            config=config,
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_DYNAMIC_TABLE,
            object_name=table_name,
            schema_name=config.target_schema,
            sql=sql,
            dialect=config.dialect,
            tier="DT_2",
            description=f"Dimension join table for {project.name}",
            dependencies=[f"{config.target_schema}.{vw_name}"],
        )

    def _generate_dt_3a(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DDLConfig,
    ) -> GeneratedDDL:
        """Generate DT_3A: Pre-aggregation dynamic table."""
        table_name = f"DT_3A_{self._safe_name(project.name)}_PREAGG"
        dt2_name = f"DT_2_{self._safe_name(project.name)}_DIMENSION"

        sql = self._build_preagg_table(
            table_name=table_name,
            source_name=dt2_name,
            schema_name=config.target_schema,
            config=config,
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_DYNAMIC_TABLE,
            object_name=table_name,
            schema_name=config.target_schema,
            sql=sql,
            dialect=config.dialect,
            tier="DT_3A",
            description=f"Pre-aggregation table for {project.name}",
            dependencies=[f"{config.target_schema}.{dt2_name}"],
        )

    def _generate_dt_3(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DDLConfig,
    ) -> GeneratedDDL:
        """Generate DT_3: Final transactional union."""
        table_name = f"DT_3_{self._safe_name(project.name)}_FINAL"
        dt3a_name = f"DT_3A_{self._safe_name(project.name)}_PREAGG"

        sql = self._build_final_table(
            table_name=table_name,
            source_name=dt3a_name,
            schema_name=config.target_schema,
            config=config,
        )

        return GeneratedDDL(
            ddl_type=DDLType.CREATE_DYNAMIC_TABLE,
            object_name=table_name,
            schema_name=config.target_schema,
            sql=sql,
            dialect=config.dialect,
            tier="DT_3",
            description=f"Final transactional table for {project.name}",
            dependencies=[f"{config.target_schema}.{dt3a_name}"],
        )

    def _generate_inserts(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DDLConfig,
    ) -> GeneratedDDL:
        """Generate INSERT statements for hierarchy data."""
        table_name = f"TBL_0_{self._safe_name(project.name)}_HIERARCHY"

        inserts = []
        for h in hierarchies:
            insert_sql = self._build_insert(h, table_name, config)
            inserts.append(insert_sql)

        sql = "\n".join(inserts)

        return GeneratedDDL(
            ddl_type=DDLType.INSERT,
            object_name=table_name,
            schema_name=config.target_schema,
            sql=sql,
            dialect=config.dialect,
            tier="TBL_0",
            description=f"Insert statements for {len(hierarchies)} hierarchies",
        )

    def _get_hierarchy_columns(self, dialect: SQLDialect) -> List[tuple]:
        """Get column definitions for hierarchy table."""
        return [
            ("HIERARCHY_ID", self._get_type("string", dialect), False),
            ("PROJECT_ID", self._get_type("string", dialect), False),
            ("HIERARCHY_NAME", self._get_type("string", dialect), False),
            ("DESCRIPTION", self._get_type("text", dialect), True),
            ("PARENT_ID", self._get_type("string", dialect), True),
            ("HIERARCHY_TYPE", self._get_type("string", dialect), True),
            ("AGGREGATION_METHOD", self._get_type("string", dialect), True),
            # Level columns
            ("LEVEL_1", self._get_type("string", dialect), True),
            ("LEVEL_2", self._get_type("string", dialect), True),
            ("LEVEL_3", self._get_type("string", dialect), True),
            ("LEVEL_4", self._get_type("string", dialect), True),
            ("LEVEL_5", self._get_type("string", dialect), True),
            ("LEVEL_6", self._get_type("string", dialect), True),
            ("LEVEL_7", self._get_type("string", dialect), True),
            ("LEVEL_8", self._get_type("string", dialect), True),
            ("LEVEL_9", self._get_type("string", dialect), True),
            ("LEVEL_10", self._get_type("string", dialect), True),
            # Sort columns
            ("LEVEL_1_SORT", self._get_type("integer", dialect), True),
            ("LEVEL_2_SORT", self._get_type("integer", dialect), True),
            ("LEVEL_3_SORT", self._get_type("integer", dialect), True),
            ("LEVEL_4_SORT", self._get_type("integer", dialect), True),
            ("LEVEL_5_SORT", self._get_type("integer", dialect), True),
            # Flags
            ("INCLUDE_FLAG", self._get_type("boolean", dialect), True),
            ("EXCLUDE_FLAG", self._get_type("boolean", dialect), True),
            ("TRANSFORM_FLAG", self._get_type("boolean", dialect), True),
            ("CALCULATION_FLAG", self._get_type("boolean", dialect), True),
            ("ACTIVE_FLAG", self._get_type("boolean", dialect), True),
            ("IS_LEAF_NODE", self._get_type("boolean", dialect), True),
            # Mappings
            ("SOURCE_MAPPINGS", self._get_type("json", dialect), True),
            ("FORMULA_CONFIG", self._get_type("json", dialect), True),
            # Sort order
            ("SORT_ORDER", self._get_type("integer", dialect), True),
            # Timestamps
            ("CREATED_AT", self._get_type("timestamp", dialect), True),
            ("UPDATED_AT", self._get_type("timestamp", dialect), True),
        ]

    def _build_create_table(
        self,
        table_name: str,
        schema_name: str,
        columns: List[tuple],
        config: DDLConfig,
    ) -> str:
        """Build CREATE TABLE statement."""
        lines = []

        # Add drop if requested
        if config.include_drop and not config.use_create_or_replace:
            lines.append(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
            lines.append("")

        # Start CREATE
        if config.use_create_or_replace and config.dialect == SQLDialect.SNOWFLAKE:
            lines.append(f"CREATE OR REPLACE TABLE {schema_name}.{table_name} (")
        else:
            lines.append(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (")

        # Add columns
        col_lines = []
        for col_name, col_type, nullable in columns:
            null_str = "" if nullable else " NOT NULL"
            col_lines.append(f"    {col_name} {col_type}{null_str}")

        lines.append(",\n".join(col_lines))
        lines.append(")")

        # Add Snowflake clustering
        if config.dialect == SQLDialect.SNOWFLAKE and config.clustering_keys:
            lines.append(f"CLUSTER BY ({', '.join(config.clustering_keys)})")

        lines.append(";")

        # Add comment
        if config.include_comments:
            lines.append("")
            lines.append(f"COMMENT ON TABLE {schema_name}.{table_name} IS 'Hierarchy master table';")

        return "\n".join(lines)

    def _build_unnest_view(
        self,
        view_name: str,
        table_name: str,
        schema_name: str,
        config: DDLConfig,
    ) -> str:
        """Build unnest view SQL."""
        if config.dialect == SQLDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE VIEW {schema_name}.{view_name} AS
SELECT
    h.HIERARCHY_ID,
    h.PROJECT_ID,
    h.HIERARCHY_NAME,
    h.PARENT_ID,
    h.HIERARCHY_TYPE,
    h.LEVEL_1,
    h.LEVEL_2,
    h.LEVEL_3,
    h.LEVEL_4,
    h.LEVEL_5,
    h.INCLUDE_FLAG,
    h.EXCLUDE_FLAG,
    h.IS_LEAF_NODE,
    m.value:source_database::VARCHAR AS SOURCE_DATABASE,
    m.value:source_schema::VARCHAR AS SOURCE_SCHEMA,
    m.value:source_table::VARCHAR AS SOURCE_TABLE,
    m.value:source_column::VARCHAR AS SOURCE_COLUMN,
    m.value:source_uid::VARCHAR AS SOURCE_UID,
    m.value:precedence_group::VARCHAR AS PRECEDENCE_GROUP
FROM {schema_name}.{table_name} h,
    LATERAL FLATTEN(input => h.SOURCE_MAPPINGS, outer => true) m
WHERE h.ACTIVE_FLAG = TRUE;"""
        else:
            # PostgreSQL/other version
            return f"""CREATE OR REPLACE VIEW {schema_name}.{view_name} AS
SELECT
    h.HIERARCHY_ID,
    h.PROJECT_ID,
    h.HIERARCHY_NAME,
    h.PARENT_ID,
    h.HIERARCHY_TYPE,
    h.LEVEL_1,
    h.LEVEL_2,
    h.LEVEL_3,
    h.LEVEL_4,
    h.LEVEL_5,
    h.INCLUDE_FLAG,
    h.EXCLUDE_FLAG,
    h.IS_LEAF_NODE,
    m->>'source_database' AS SOURCE_DATABASE,
    m->>'source_schema' AS SOURCE_SCHEMA,
    m->>'source_table' AS SOURCE_TABLE,
    m->>'source_column' AS SOURCE_COLUMN,
    m->>'source_uid' AS SOURCE_UID,
    m->>'precedence_group' AS PRECEDENCE_GROUP
FROM {schema_name}.{table_name} h
CROSS JOIN LATERAL jsonb_array_elements(h.SOURCE_MAPPINGS) m
WHERE h.ACTIVE_FLAG = TRUE;"""

    def _build_dimension_table(
        self,
        table_name: str,
        view_name: str,
        schema_name: str,
        config: DDLConfig,
    ) -> str:
        """Build dimension dynamic table SQL."""
        if config.dialect == SQLDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE DYNAMIC TABLE {schema_name}.{table_name}
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    HIERARCHY_ID,
    PROJECT_ID,
    HIERARCHY_NAME,
    HIERARCHY_TYPE,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    LEVEL_4,
    LEVEL_5,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    SOURCE_UID,
    PRECEDENCE_GROUP
FROM {schema_name}.{view_name}
WHERE IS_LEAF_NODE = TRUE
  AND INCLUDE_FLAG = TRUE
  AND (EXCLUDE_FLAG = FALSE OR EXCLUDE_FLAG IS NULL);"""
        else:
            # Standard view for non-Snowflake
            return f"""CREATE OR REPLACE VIEW {schema_name}.{table_name} AS
SELECT
    HIERARCHY_ID,
    PROJECT_ID,
    HIERARCHY_NAME,
    HIERARCHY_TYPE,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    LEVEL_4,
    LEVEL_5,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    SOURCE_UID,
    PRECEDENCE_GROUP
FROM {schema_name}.{view_name}
WHERE IS_LEAF_NODE = TRUE
  AND INCLUDE_FLAG = TRUE
  AND (EXCLUDE_FLAG = FALSE OR EXCLUDE_FLAG IS NULL);"""

    def _build_preagg_table(
        self,
        table_name: str,
        source_name: str,
        schema_name: str,
        config: DDLConfig,
    ) -> str:
        """Build pre-aggregation table SQL."""
        if config.dialect == SQLDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE DYNAMIC TABLE {schema_name}.{table_name}
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    HIERARCHY_ID,
    PROJECT_ID,
    HIERARCHY_NAME,
    HIERARCHY_TYPE,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    COUNT(*) AS MAPPING_COUNT,
    LISTAGG(DISTINCT SOURCE_UID, ', ') AS SOURCE_UIDS
FROM {schema_name}.{source_name}
GROUP BY ALL;"""
        else:
            return f"""CREATE OR REPLACE VIEW {schema_name}.{table_name} AS
SELECT
    HIERARCHY_ID,
    PROJECT_ID,
    HIERARCHY_NAME,
    HIERARCHY_TYPE,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    COUNT(*) AS MAPPING_COUNT,
    STRING_AGG(DISTINCT SOURCE_UID, ', ') AS SOURCE_UIDS
FROM {schema_name}.{source_name}
GROUP BY HIERARCHY_ID, PROJECT_ID, HIERARCHY_NAME, HIERARCHY_TYPE,
         LEVEL_1, LEVEL_2, LEVEL_3, SOURCE_DATABASE, SOURCE_SCHEMA,
         SOURCE_TABLE, SOURCE_COLUMN;"""

    def _build_final_table(
        self,
        table_name: str,
        source_name: str,
        schema_name: str,
        config: DDLConfig,
    ) -> str:
        """Build final transactional union table SQL."""
        if config.dialect == SQLDialect.SNOWFLAKE:
            return f"""CREATE OR REPLACE DYNAMIC TABLE {schema_name}.{table_name}
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    HIERARCHY_ID,
    PROJECT_ID,
    HIERARCHY_NAME,
    HIERARCHY_TYPE,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    MAPPING_COUNT,
    SOURCE_UIDS,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM {schema_name}.{source_name};"""
        else:
            return f"""CREATE OR REPLACE VIEW {schema_name}.{table_name} AS
SELECT
    HIERARCHY_ID,
    PROJECT_ID,
    HIERARCHY_NAME,
    HIERARCHY_TYPE,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    SOURCE_COLUMN,
    MAPPING_COUNT,
    SOURCE_UIDS,
    CURRENT_TIMESTAMP AS LOADED_AT
FROM {schema_name}.{source_name};"""

    def _build_insert(
        self,
        hierarchy: Any,
        table_name: str,
        config: DDLConfig,
    ) -> str:
        """Build INSERT statement for a hierarchy."""
        import json

        # Get source mappings as JSON
        mappings = getattr(hierarchy, "source_mappings", []) or []
        mappings_json = json.dumps(mappings) if mappings else "[]"

        # Get formula config
        formula = getattr(hierarchy, "formula_config", None)
        formula_json = json.dumps(formula) if formula else "NULL"

        return f"""INSERT INTO {config.target_schema}.{table_name} (
    HIERARCHY_ID, PROJECT_ID, HIERARCHY_NAME, DESCRIPTION, PARENT_ID,
    HIERARCHY_TYPE, AGGREGATION_METHOD,
    LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,
    LEVEL_1_SORT, LEVEL_2_SORT, LEVEL_3_SORT, LEVEL_4_SORT, LEVEL_5_SORT,
    INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG, ACTIVE_FLAG, IS_LEAF_NODE,
    SOURCE_MAPPINGS, FORMULA_CONFIG, SORT_ORDER
) VALUES (
    '{hierarchy.hierarchy_id}',
    '{hierarchy.project_id}',
    '{self._escape_string(hierarchy.hierarchy_name)}',
    {self._quote_or_null(hierarchy.description)},
    {self._quote_or_null(hierarchy.parent_id)},
    '{getattr(hierarchy, "hierarchy_type", "standard")}',
    '{getattr(hierarchy, "aggregation_method", "sum")}',
    {self._quote_or_null(hierarchy.level_1)},
    {self._quote_or_null(hierarchy.level_2)},
    {self._quote_or_null(hierarchy.level_3)},
    {self._quote_or_null(hierarchy.level_4)},
    {self._quote_or_null(hierarchy.level_5)},
    {hierarchy.level_1_sort or 1},
    {hierarchy.level_2_sort or 1},
    {hierarchy.level_3_sort or 1},
    {hierarchy.level_4_sort or 1},
    {hierarchy.level_5_sort or 1},
    {self._bool_to_sql(hierarchy.include_flag, config.dialect)},
    {self._bool_to_sql(hierarchy.exclude_flag, config.dialect)},
    {self._bool_to_sql(hierarchy.transform_flag, config.dialect)},
    {self._bool_to_sql(hierarchy.calculation_flag, config.dialect)},
    {self._bool_to_sql(hierarchy.active_flag, config.dialect)},
    {self._bool_to_sql(hierarchy.is_leaf_node, config.dialect)},
    PARSE_JSON('{mappings_json}'),
    {f"PARSE_JSON('{formula_json}')" if formula else "NULL"},
    {hierarchy.sort_order or 1}
);"""

    def _safe_name(self, name: str) -> str:
        """Convert name to safe SQL identifier."""
        import re
        return re.sub(r"[^a-zA-Z0-9_]", "_", name).upper()

    def _escape_string(self, value: Optional[str]) -> str:
        """Escape single quotes in strings."""
        if value is None:
            return ""
        return value.replace("'", "''")

    def _quote_or_null(self, value: Optional[str]) -> str:
        """Quote a string or return NULL."""
        if value is None:
            return "NULL"
        return f"'{self._escape_string(value)}'"

    def _bool_to_sql(self, value: Optional[bool], dialect: SQLDialect) -> str:
        """Convert boolean to SQL literal."""
        if value is None:
            return "NULL"
        if dialect in (SQLDialect.SNOWFLAKE, SQLDialect.POSTGRESQL):
            return "TRUE" if value else "FALSE"
        elif dialect == SQLDialect.MYSQL:
            return "1" if value else "0"
        elif dialect == SQLDialect.TSQL:
            return "1" if value else "0"
        return "TRUE" if value else "FALSE"

    def generate_preview(
        self,
        project: Any,
        hierarchies: List[Any],
        config: Optional[DDLConfig] = None,
    ) -> Dict[str, Any]:
        """
        Generate a preview of what will be created.

        Args:
            project: Project object.
            hierarchies: List of hierarchies.
            config: Optional DDL configuration.

        Returns:
            Dictionary with preview information.
        """
        cfg = config or self.config
        project_name = self._safe_name(project.name)

        objects = []

        if cfg.generate_tbl_0:
            objects.append({
                "tier": "TBL_0",
                "type": "TABLE",
                "name": f"TBL_0_{project_name}_HIERARCHY",
                "description": "Base hierarchy data table",
            })

        if cfg.generate_vw_1:
            objects.append({
                "tier": "VW_1",
                "type": "VIEW",
                "name": f"VW_1_{project_name}_MAPPING",
                "description": "Mapping unnest view",
            })

        if cfg.generate_dt_2:
            objects.append({
                "tier": "DT_2",
                "type": "DYNAMIC TABLE" if cfg.dialect == SQLDialect.SNOWFLAKE else "VIEW",
                "name": f"DT_2_{project_name}_DIMENSION",
                "description": "Dimension join table",
            })

        if cfg.generate_dt_3a:
            objects.append({
                "tier": "DT_3A",
                "type": "DYNAMIC TABLE" if cfg.dialect == SQLDialect.SNOWFLAKE else "VIEW",
                "name": f"DT_3A_{project_name}_PREAGG",
                "description": "Pre-aggregation table",
            })

        if cfg.generate_dt_3:
            objects.append({
                "tier": "DT_3",
                "type": "DYNAMIC TABLE" if cfg.dialect == SQLDialect.SNOWFLAKE else "VIEW",
                "name": f"DT_3_{project_name}_FINAL",
                "description": "Final transactional table",
            })

        return {
            "project_name": project.name,
            "target_schema": cfg.target_schema,
            "dialect": cfg.dialect.value,
            "hierarchy_count": len(hierarchies),
            "objects": objects,
            "estimated_rows": len(hierarchies),
        }
