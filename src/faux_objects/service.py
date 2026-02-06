"""Faux Objects Service - Generate standard Snowflake objects that wrap Semantic Views.

This service generates SQL scripts for:
- Standard Views wrapping SEMANTIC_VIEW() queries
- Snowpark Python Stored Procedures with RETURNS TABLE
- Dynamic Tables with auto-refresh from Semantic Views
- Snowflake Tasks for scheduled materialization

These "faux objects" let BI tools (Power BI, Tableau, Excel) consume Semantic Views
without understanding the SEMANTIC_VIEW() syntax.
"""
import json
import os
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from .types import (
    FauxProject,
    FauxObjectConfig,
    FauxObjectType,
    SemanticViewDefinition,
    SemanticColumn,
    SemanticColumnType,
    SemanticTable,
    SemanticRelationship,
    ProcedureParameter,
    GeneratedScript,
)


class FauxObjectsService:
    """Service for managing Faux Objects projects and generating SQL scripts."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.projects_dir = os.path.join(data_dir, "faux_objects")
        os.makedirs(self.projects_dir, exist_ok=True)

    # =========================================================================
    # Project CRUD
    # =========================================================================

    def create_project(self, name: str, description: str = "") -> FauxProject:
        """Create a new Faux Objects project."""
        project = FauxProject(
            id=str(uuid.uuid4())[:8],
            name=name,
            description=description,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        self._save_project(project)
        return project

    def get_project(self, project_id: str) -> Optional[FauxProject]:
        """Get a project by ID."""
        path = os.path.join(self.projects_dir, f"{project_id}.json")
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            data = json.load(f)
        return FauxProject(**data)

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all faux objects projects."""
        projects = []
        if not os.path.exists(self.projects_dir):
            return projects
        for filename in os.listdir(self.projects_dir):
            if filename.endswith(".json"):
                with open(os.path.join(self.projects_dir, filename), "r") as f:
                    data = json.load(f)
                projects.append({
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "description": data.get("description"),
                    "semantic_view": data.get("semantic_view", {}).get("name") if data.get("semantic_view") else None,
                    "faux_object_count": len(data.get("faux_objects", [])),
                    "created_at": data.get("created_at"),
                })
        return projects

    def delete_project(self, project_id: str) -> bool:
        """Delete a project."""
        path = os.path.join(self.projects_dir, f"{project_id}.json")
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    def _save_project(self, project: FauxProject):
        """Save a project to disk."""
        project.updated_at = datetime.now()
        path = os.path.join(self.projects_dir, f"{project.id}.json")
        with open(path, "w") as f:
            json.dump(project.model_dump(mode="json"), f, indent=2, default=str)

    # =========================================================================
    # Semantic View Definition
    # =========================================================================

    def define_semantic_view(
        self,
        project_id: str,
        name: str,
        database: str,
        schema_name: str,
        comment: Optional[str] = None,
        ai_sql_generation: Optional[str] = None,
    ) -> FauxProject:
        """Define the semantic view that faux objects will wrap."""
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id} not found")

        project.semantic_view = SemanticViewDefinition(
            name=name,
            database=database,
            schema_name=schema_name,
            comment=comment,
            ai_sql_generation=ai_sql_generation,
        )
        self._save_project(project)
        return project

    def add_semantic_table(
        self,
        project_id: str,
        alias: str,
        fully_qualified_name: str,
        primary_key: Optional[str] = None,
    ) -> FauxProject:
        """Add a table reference to the semantic view definition."""
        project = self.get_project(project_id)
        if not project or not project.semantic_view:
            raise ValueError(f"Project {project_id} not found or semantic view not defined")

        table = SemanticTable(
            alias=alias,
            fully_qualified_name=fully_qualified_name,
            primary_key=primary_key,
        )
        project.semantic_view.tables.append(table)
        self._save_project(project)
        return project

    def add_semantic_column(
        self,
        project_id: str,
        name: str,
        column_type: str,
        data_type: str = "VARCHAR",
        table_alias: Optional[str] = None,
        expression: Optional[str] = None,
        synonyms: Optional[List[str]] = None,
        comment: Optional[str] = None,
    ) -> FauxProject:
        """Add a dimension, metric, or fact column to the semantic view."""
        project = self.get_project(project_id)
        if not project or not project.semantic_view:
            raise ValueError(f"Project {project_id} not found or semantic view not defined")

        col = SemanticColumn(
            name=name,
            column_type=SemanticColumnType(column_type),
            data_type=data_type,
            table_alias=table_alias,
            expression=expression,
            synonyms=synonyms or [],
            comment=comment,
        )

        if col.column_type == SemanticColumnType.DIMENSION:
            project.semantic_view.dimensions.append(col)
        elif col.column_type == SemanticColumnType.METRIC:
            project.semantic_view.metrics.append(col)
        elif col.column_type == SemanticColumnType.FACT:
            project.semantic_view.facts.append(col)

        self._save_project(project)
        return project

    def add_semantic_relationship(
        self,
        project_id: str,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: Optional[str] = None,
    ) -> FauxProject:
        """Add a relationship between tables in the semantic view."""
        project = self.get_project(project_id)
        if not project or not project.semantic_view:
            raise ValueError(f"Project {project_id} not found or semantic view not defined")

        rel = SemanticRelationship(
            from_table=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
        )
        project.semantic_view.relationships.append(rel)
        self._save_project(project)
        return project

    # =========================================================================
    # Faux Object Configuration
    # =========================================================================

    def add_faux_object(
        self,
        project_id: str,
        name: str,
        faux_type: str,
        target_database: str,
        target_schema: str,
        selected_dimensions: Optional[List[str]] = None,
        selected_metrics: Optional[List[str]] = None,
        selected_facts: Optional[List[str]] = None,
        parameters: Optional[List[Dict[str, str]]] = None,
        warehouse: Optional[str] = None,
        target_lag: Optional[str] = None,
        schedule: Optional[str] = None,
        materialized_table: Optional[str] = None,
        where_clause: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> FauxProject:
        """Add a faux object configuration to the project."""
        project = self.get_project(project_id)
        if not project or not project.semantic_view:
            raise ValueError(f"Project {project_id} not found or semantic view not defined")

        sv = project.semantic_view

        # Default to all columns if none specified
        dims = selected_dimensions or [c.name for c in sv.dimensions]
        mets = selected_metrics or [c.name for c in sv.metrics]
        facts = selected_facts or [c.name for c in sv.facts]

        # Parse parameters for stored procedures
        proc_params = []
        if parameters:
            for p in parameters:
                proc_params.append(ProcedureParameter(**p))

        config = FauxObjectConfig(
            name=name,
            faux_type=FauxObjectType(faux_type),
            target_database=target_database,
            target_schema=target_schema,
            selected_dimensions=dims,
            selected_metrics=mets,
            selected_facts=facts,
            parameters=proc_params,
            warehouse=warehouse,
            target_lag=target_lag,
            schedule=schedule,
            materialized_table=materialized_table,
            where_clause=where_clause,
            comment=comment,
        )
        project.faux_objects.append(config)
        self._save_project(project)
        return project

    def remove_faux_object(self, project_id: str, object_name: str) -> FauxProject:
        """Remove a faux object from the project by name."""
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id} not found")

        project.faux_objects = [o for o in project.faux_objects if o.name != object_name]
        self._save_project(project)
        return project

    # =========================================================================
    # SQL Generation - Core
    # =========================================================================

    def _resolve_columns(
        self,
        sv: SemanticViewDefinition,
        selected_dims: List[str],
        selected_metrics: List[str],
        selected_facts: List[str],
    ) -> Dict[str, List[SemanticColumn]]:
        """Resolve selected column names to SemanticColumn objects."""
        dim_map = {c.name: c for c in sv.dimensions}
        met_map = {c.name: c for c in sv.metrics}
        fact_map = {c.name: c for c in sv.facts}

        return {
            "dimensions": [dim_map[n] for n in selected_dims if n in dim_map],
            "metrics": [met_map[n] for n in selected_metrics if n in met_map],
            "facts": [fact_map[n] for n in selected_facts if n in fact_map],
        }

    def _build_semantic_query(
        self,
        sv: SemanticViewDefinition,
        columns: Dict[str, List[SemanticColumn]],
        where_clause: Optional[str] = None,
        indent: int = 0,
    ) -> str:
        """Build a SELECT ... FROM SEMANTIC_VIEW() query string."""
        prefix = " " * indent
        parts = [f"{prefix}SELECT * FROM SEMANTIC_VIEW("]
        parts.append(f"{prefix}    {sv.fully_qualified_name}")

        # DIMENSIONS
        if columns["dimensions"]:
            dim_names = [c.qualified_name for c in columns["dimensions"]]
            parts.append(f"{prefix}    DIMENSIONS {','.join(dim_names)}")

        # FACTS
        if columns["facts"]:
            fact_names = [c.qualified_name for c in columns["facts"]]
            parts.append(f"{prefix}    FACTS {','.join(fact_names)}")

        # METRICS
        if columns["metrics"]:
            met_names = [c.qualified_name for c in columns["metrics"]]
            parts.append(f"{prefix}    METRICS {','.join(met_names)}")

        # WHERE
        if where_clause:
            parts.append(f"{prefix}    WHERE {where_clause}")

        parts.append(f"{prefix})")
        return "\n".join(parts)

    def _column_output_name(self, col: SemanticColumn) -> str:
        """Get the output column name (without table alias) for RETURNS TABLE."""
        return col.name.upper()

    def _column_return_type(self, col: SemanticColumn) -> str:
        """Get the Snowflake data type for RETURNS TABLE."""
        return col.data_type.upper()

    # =========================================================================
    # SQL Generation - View
    # =========================================================================

    def generate_view_sql(
        self, sv: SemanticViewDefinition, config: FauxObjectConfig
    ) -> str:
        """Generate CREATE OR REPLACE VIEW wrapping a SEMANTIC_VIEW() query."""
        columns = self._resolve_columns(
            sv, config.selected_dimensions, config.selected_metrics, config.selected_facts
        )

        lines = []
        lines.append(f"-- Faux Object: Standard View")
        lines.append(f"-- Wraps Semantic View: {sv.fully_qualified_name}")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append(f"-- BI tools can query this like a regular view")
        lines.append("")

        comment_clause = ""
        if config.comment:
            escaped = config.comment.replace("'", "''")
            comment_clause = f"\n    COMMENT = '{escaped}'"

        lines.append(f"CREATE OR REPLACE VIEW {config.fully_qualified_name}{comment_clause}")
        lines.append("AS")
        lines.append(self._build_semantic_query(sv, columns, config.where_clause))
        lines.append(";")

        return "\n".join(lines)

    # =========================================================================
    # SQL Generation - Stored Procedure
    # =========================================================================

    def generate_stored_procedure_sql(
        self, sv: SemanticViewDefinition, config: FauxObjectConfig
    ) -> str:
        """Generate CREATE OR REPLACE PROCEDURE with Snowpark Python handler.

        The procedure uses RETURNS TABLE(...) so BI tools can call it with:
            SELECT * FROM TABLE(procedure_name(args))
        """
        columns = self._resolve_columns(
            sv, config.selected_dimensions, config.selected_metrics, config.selected_facts
        )

        all_cols = columns["dimensions"] + columns["facts"] + columns["metrics"]

        # Build RETURNS TABLE columns
        return_cols = []
        for col in all_cols:
            return_cols.append(f"    {self._column_output_name(col)} {self._column_return_type(col)}")

        # Build procedure parameters
        proc_params = []
        handler_params = []
        handler_defaults = []

        for param in config.parameters:
            default_part = f" DEFAULT {param.default_value}" if param.default_value else ""
            proc_params.append(f"    {param.name} {param.data_type}{default_part}")
            handler_params.append(param.name.lower())
            if param.default_value:
                handler_defaults.append(f"{param.name.lower()}={param.default_value}")
            else:
                handler_defaults.append(param.name.lower())

        # Build dimension/metric qualified names for the SEMANTIC_VIEW query
        dim_names = ", ".join(c.qualified_name for c in columns["dimensions"])
        fact_names = ", ".join(c.qualified_name for c in columns["facts"])
        met_names = ", ".join(c.qualified_name for c in columns["metrics"])

        # Build the WHERE clause construction in Python
        where_parts = []
        for param in config.parameters:
            pname = param.name.lower()
            # Generate a Python condition that adds to WHERE if param is not None
            where_parts.append(
                f"    if {pname} is not None:\n"
                f"        conditions.append(f\"{param.name} = '{{{pname}}}'\")"
            )

        lines = []
        lines.append(f"-- Faux Object: Snowpark Python Stored Procedure")
        lines.append(f"-- Wraps Semantic View: {sv.fully_qualified_name}")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append(f"-- BI tools call: SELECT * FROM TABLE({config.fully_qualified_name}(args))")
        lines.append("")

        # CREATE PROCEDURE
        lines.append(f"CREATE OR REPLACE PROCEDURE {config.fully_qualified_name}(")
        if proc_params:
            lines.append(",\n".join(proc_params))
        lines.append(")")
        lines.append("RETURNS TABLE(")
        lines.append(",\n".join(return_cols))
        lines.append(")")
        lines.append("LANGUAGE PYTHON")
        lines.append("RUNTIME_VERSION = '3.11'")
        lines.append("PACKAGES = ('snowflake-snowpark-python')")
        lines.append("HANDLER = 'run'")
        if config.comment:
            escaped = config.comment.replace("'", "''")
            lines.append(f"COMMENT = '{escaped}'")
        lines.append("AS")
        lines.append("$$")

        # Python handler
        handler_sig = ", ".join(["session"] + handler_defaults)
        lines.append(f"def run({handler_sig}):")
        lines.append(f"    # Build WHERE clause from parameters")
        lines.append(f"    conditions = []")

        if config.where_clause:
            lines.append(f"    conditions.append(\"{config.where_clause}\")")

        for wp in where_parts:
            lines.append(wp)

        lines.append(f"    where = ''")
        lines.append(f"    if conditions:")
        lines.append(f"        where = 'WHERE ' + ' AND '.join(conditions)")
        lines.append(f"")
        lines.append(f"    query = f\"\"\"")
        lines.append(f"        SELECT * FROM SEMANTIC_VIEW(")
        lines.append(f"            {sv.fully_qualified_name}")

        if dim_names:
            lines.append(f"            DIMENSIONS {dim_names}")
        if fact_names:
            lines.append(f"            FACTS {fact_names}")
        if met_names:
            lines.append(f"            METRICS {met_names}")

        lines.append(f"            {{where}}")
        lines.append(f"        )")
        lines.append(f"    \"\"\"")
        lines.append(f"    return session.sql(query)")
        lines.append("$$;")

        return "\n".join(lines)

    # =========================================================================
    # SQL Generation - Dynamic Table
    # =========================================================================

    def generate_dynamic_table_sql(
        self, sv: SemanticViewDefinition, config: FauxObjectConfig
    ) -> str:
        """Generate CREATE OR REPLACE DYNAMIC TABLE wrapping a SEMANTIC_VIEW() query.

        Dynamic tables auto-refresh based on target_lag, providing BI tools
        with a regular table that stays current.
        """
        columns = self._resolve_columns(
            sv, config.selected_dimensions, config.selected_metrics, config.selected_facts
        )

        warehouse = config.warehouse or "COMPUTE_WH"
        lag = config.target_lag or "2 hours"

        lines = []
        lines.append(f"-- Faux Object: Dynamic Table (auto-refreshing)")
        lines.append(f"-- Wraps Semantic View: {sv.fully_qualified_name}")
        lines.append(f"-- Target Lag: {lag}")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append(f"-- BI tools see this as a regular table with auto-refresh")
        lines.append("")

        lines.append(f"CREATE OR REPLACE DYNAMIC TABLE {config.fully_qualified_name}")
        lines.append(f"    TARGET_LAG = '{lag}'")
        lines.append(f"    WAREHOUSE = {warehouse}")

        if config.comment:
            escaped = config.comment.replace("'", "''")
            lines.append(f"    COMMENT = '{escaped}'")

        lines.append("AS")
        lines.append(self._build_semantic_query(sv, columns, config.where_clause))
        lines.append(";")

        return "\n".join(lines)

    # =========================================================================
    # SQL Generation - Task (Scheduled Materialization)
    # =========================================================================

    def generate_task_sql(
        self, sv: SemanticViewDefinition, config: FauxObjectConfig
    ) -> str:
        """Generate a Snowflake Task + materializer procedure for scheduled refresh.

        Creates:
        1. A materializer stored procedure
        2. A Snowflake Task on a CRON schedule
        3. ALTER TASK ... RESUME to activate

        The materialized table is a regular table any BI tool can query.
        """
        columns = self._resolve_columns(
            sv, config.selected_dimensions, config.selected_metrics, config.selected_facts
        )

        warehouse = config.warehouse or "COMPUTE_WH"
        schedule = config.schedule or "USING CRON 0 */4 * * * America/Chicago"
        target_table = config.materialized_table or f"{config.fully_qualified_name}_MAT"
        proc_name = f"{config.fully_qualified_name}_MATERIALIZER"
        task_name = f"{config.fully_qualified_name}_REFRESH"

        # Build dimension/metric qualified names
        dim_names = ", ".join(c.qualified_name for c in columns["dimensions"])
        fact_names = ", ".join(c.qualified_name for c in columns["facts"])
        met_names = ", ".join(c.qualified_name for c in columns["metrics"])

        lines = []
        lines.append(f"-- Faux Object: Scheduled Materialization (Task + Procedure)")
        lines.append(f"-- Wraps Semantic View: {sv.fully_qualified_name}")
        lines.append(f"-- Schedule: {schedule}")
        lines.append(f"-- Target Table: {target_table}")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append(f"-- BI tools query the materialized table directly")
        lines.append("")

        # Step 1: Materializer procedure
        lines.append(f"-- Step 1: Create the materializer procedure")
        lines.append(f"CREATE OR REPLACE PROCEDURE {proc_name}()")
        lines.append(f"RETURNS TABLE(status VARCHAR, row_count INT, refreshed_at TIMESTAMP_NTZ)")
        lines.append(f"LANGUAGE PYTHON")
        lines.append(f"RUNTIME_VERSION = '3.11'")
        lines.append(f"PACKAGES = ('snowflake-snowpark-python')")
        lines.append(f"HANDLER = 'run'")
        lines.append(f"AS")
        lines.append(f"$$")
        lines.append(f"from datetime import datetime")
        lines.append(f"import pandas as pd")
        lines.append(f"")
        lines.append(f"def run(session):")
        lines.append(f"    query = \"\"\"")
        lines.append(f"        SELECT * FROM SEMANTIC_VIEW(")
        lines.append(f"            {sv.fully_qualified_name}")
        if dim_names:
            lines.append(f"            DIMENSIONS {dim_names}")
        if fact_names:
            lines.append(f"            FACTS {fact_names}")
        if met_names:
            lines.append(f"            METRICS {met_names}")
        if config.where_clause:
            lines.append(f"            WHERE {config.where_clause}")
        lines.append(f"        )")
        lines.append(f"    \"\"\"")
        lines.append(f"    df = session.sql(query)")
        lines.append(f"    row_count = df.count()")
        lines.append(f"    df.write.mode('overwrite').save_as_table('{target_table}')")
        lines.append(f"    return session.create_dataframe(pd.DataFrame([{{")
        lines.append(f"        'STATUS': 'SUCCESS',")
        lines.append(f"        'ROW_COUNT': row_count,")
        lines.append(f"        'REFRESHED_AT': datetime.utcnow()")
        lines.append(f"    }}]))")
        lines.append(f"$$;")
        lines.append(f"")

        # Step 2: Task
        lines.append(f"-- Step 2: Create the scheduled task")
        lines.append(f"CREATE OR REPLACE TASK {task_name}")
        lines.append(f"    WAREHOUSE = {warehouse}")
        lines.append(f"    SCHEDULE = '{schedule}'")
        if config.comment:
            escaped = config.comment.replace("'", "''")
            lines.append(f"    COMMENT = '{escaped}'")
        lines.append(f"AS")
        lines.append(f"CALL {proc_name}();")
        lines.append(f"")

        # Step 3: Resume
        lines.append(f"-- Step 3: Activate the task (tasks start suspended)")
        lines.append(f"ALTER TASK {task_name} RESUME;")

        return "\n".join(lines)

    # =========================================================================
    # SQL Generation - Semantic View DDL
    # =========================================================================

    def generate_semantic_view_ddl(
        self, sv: SemanticViewDefinition
    ) -> str:
        """Generate the CREATE SEMANTIC VIEW DDL from the definition."""
        lines = []
        lines.append(f"-- Semantic View DDL")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append("")

        # CREATE statement
        create_line = f"CREATE OR REPLACE SEMANTIC VIEW {sv.fully_qualified_name}"
        if sv.comment:
            escaped = sv.comment.replace("'", "''")
            create_line += f"\n    COMMENT = '{escaped}'"
        if sv.ai_sql_generation:
            escaped = sv.ai_sql_generation.replace("'", "''")
            create_line += f"\n    AI_SQL_GENERATION = '{escaped}'"
        lines.append(create_line)

        # TABLES
        if sv.tables:
            lines.append("")
            lines.append("    TABLES (")
            table_parts = []
            for t in sv.tables:
                part = f"        {t.alias} AS {t.fully_qualified_name}"
                if t.primary_key:
                    part += f"\n            PRIMARY KEY ({t.primary_key})"
                table_parts.append(part)
            lines.append(",\n".join(table_parts))
            lines.append("    )")

        # RELATIONSHIPS
        if sv.relationships:
            lines.append("")
            lines.append("    RELATIONSHIPS (")
            rel_parts = []
            for r in sv.relationships:
                if r.to_column:
                    rel_parts.append(
                        f"        {r.from_table} ({r.from_column}) REFERENCES {r.to_table} ({r.to_column})"
                    )
                else:
                    rel_parts.append(
                        f"        {r.from_table} ({r.from_column}) REFERENCES {r.to_table}"
                    )
            lines.append(",\n".join(rel_parts))
            lines.append("    )")

        # FACTS
        if sv.facts:
            lines.append("")
            lines.append("    FACTS (")
            fact_parts = []
            for f in sv.facts:
                part = f"        {f.qualified_name}"
                if f.expression:
                    part += f" AS {f.expression}"
                fact_parts.append(part)
            lines.append(",\n".join(fact_parts))
            lines.append("    )")

        # DIMENSIONS
        if sv.dimensions:
            lines.append("")
            lines.append("    DIMENSIONS (")
            dim_parts = []
            for d in sv.dimensions:
                part = f"        {d.qualified_name} AS {d.name}"
                if d.synonyms:
                    syns = ", ".join(f"'{s}'" for s in d.synonyms)
                    part += f"\n            WITH SYNONYMS = ({syns})"
                dim_parts.append(part)
            lines.append(",\n".join(dim_parts))
            lines.append("    )")

        # METRICS
        if sv.metrics:
            lines.append("")
            lines.append("    METRICS (")
            met_parts = []
            for m in sv.metrics:
                part = f"        {m.qualified_name}"
                if m.expression:
                    part += f" AS {m.expression}"
                if m.comment:
                    escaped = m.comment.replace("'", "''")
                    part += f"\n            COMMENT = '{escaped}'"
                met_parts.append(part)
            lines.append(",\n".join(met_parts))
            lines.append("    )")

        lines.append(";")
        return "\n".join(lines)

    # =========================================================================
    # Batch Generation
    # =========================================================================

    def generate_all_scripts(self, project_id: str) -> List[GeneratedScript]:
        """Generate SQL scripts for all faux objects in a project."""
        project = self.get_project(project_id)
        if not project or not project.semantic_view:
            raise ValueError(f"Project {project_id} not found or semantic view not defined")

        sv = project.semantic_view
        scripts = []

        # Generate semantic view DDL first
        scripts.append(GeneratedScript(
            object_name=sv.fully_qualified_name,
            object_type=FauxObjectType.VIEW,
            sql=self.generate_semantic_view_ddl(sv),
            dependencies=[],
        ))

        # Generate each faux object
        for config in project.faux_objects:
            if config.faux_type == FauxObjectType.VIEW:
                sql = self.generate_view_sql(sv, config)
                deps = [sv.fully_qualified_name]
            elif config.faux_type == FauxObjectType.STORED_PROCEDURE:
                sql = self.generate_stored_procedure_sql(sv, config)
                deps = [sv.fully_qualified_name]
            elif config.faux_type == FauxObjectType.DYNAMIC_TABLE:
                sql = self.generate_dynamic_table_sql(sv, config)
                deps = [sv.fully_qualified_name]
            elif config.faux_type == FauxObjectType.TASK:
                sql = self.generate_task_sql(sv, config)
                deps = [sv.fully_qualified_name]
            else:
                continue

            scripts.append(GeneratedScript(
                object_name=config.fully_qualified_name,
                object_type=config.faux_type,
                sql=sql,
                dependencies=deps,
            ))

        return scripts

    def generate_deployment_bundle(self, project_id: str) -> str:
        """Generate a complete deployment script with all faux objects."""
        scripts = self.generate_all_scripts(project_id)
        project = self.get_project(project_id)

        lines = []
        lines.append("-- =============================================================")
        lines.append(f"-- FAUX OBJECTS DEPLOYMENT BUNDLE")
        lines.append(f"-- Project: {project.name}")
        lines.append(f"-- Generated: {datetime.now().isoformat()}")
        lines.append(f"-- Objects: {len(scripts)}")
        lines.append("-- =============================================================")
        lines.append("")
        lines.append("-- Faux Objects make Semantic Views accessible to BI tools")
        lines.append("-- by wrapping them in standard Snowflake objects (views,")
        lines.append("-- stored procedures, dynamic tables, tasks).")
        lines.append("")

        for i, script in enumerate(scripts, 1):
            lines.append(f"-- [{i}/{len(scripts)}] {script.object_type.value.upper()}: {script.object_name}")
            lines.append("-- " + "-" * 60)
            lines.append(script.sql)
            lines.append("")
            lines.append("")

        lines.append("-- =============================================================")
        lines.append("-- END OF DEPLOYMENT BUNDLE")
        lines.append("-- =============================================================")

        return "\n".join(lines)

    def export_scripts(self, project_id: str, output_dir: str) -> Dict[str, str]:
        """Export all generated scripts to individual files."""
        scripts = self.generate_all_scripts(project_id)
        os.makedirs(output_dir, exist_ok=True)

        exported = {}
        for script in scripts:
            # Sanitize filename
            safe_name = script.object_name.replace(".", "_").lower()
            filename = f"{safe_name}_{script.object_type.value}.sql"
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w") as f:
                f.write(script.sql)

            exported[script.object_name] = filepath

        # Also write the bundle
        bundle_path = os.path.join(output_dir, "deployment_bundle.sql")
        with open(bundle_path, "w") as f:
            f.write(self.generate_deployment_bundle(project_id))
        exported["deployment_bundle"] = bundle_path

        return exported
