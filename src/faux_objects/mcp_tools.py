"""MCP tools for Faux Objects - Semantic View wrapper generation.

Faux Objects generate standard Snowflake objects (views, stored procedures,
dynamic tables, tasks) that wrap Semantic Views so BI tools can consume them
without understanding the SEMANTIC_VIEW() syntax.
"""
import json
from typing import Optional


def register_faux_objects_tools(mcp, data_dir: str = "data"):
    """Register all Faux Objects MCP tools with the server."""

    from .service import FauxObjectsService

    service = FauxObjectsService(data_dir)

    # =========================================================================
    # Project Management
    # =========================================================================

    @mcp.tool()
    def create_faux_project(name: str, description: str = "") -> str:
        """Create a new Faux Objects project.

        A Faux Objects project contains:
        1. A Semantic View definition (the source of truth)
        2. One or more Faux Object configurations (views, stored procedures,
           dynamic tables, tasks) that wrap the Semantic View

        Faux Objects make Semantic Views accessible to BI tools (Power BI,
        Tableau, Excel) by generating standard Snowflake objects that BI tools
        already know how to query.

        Args:
            name: Project name (e.g., "P&L Analysis Wrappers")
            description: Optional project description

        Returns:
            JSON with the created project details
        """
        try:
            project = service.create_project(name, description)
            return json.dumps({
                "status": "success",
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "description": project.description,
                },
                "next_step": "Use define_faux_semantic_view() to define the semantic view this project wraps",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_faux_projects() -> str:
        """List all Faux Objects projects.

        Returns a summary of each project including the semantic view name
        and number of configured faux objects.

        Returns:
            JSON array of project summaries
        """
        try:
            projects = service.list_projects()
            return json.dumps({
                "status": "success",
                "count": len(projects),
                "projects": projects,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_faux_project(project_id: str) -> str:
        """Get full details of a Faux Objects project.

        Returns the complete project including the semantic view definition,
        all faux object configurations, and column details.

        Args:
            project_id: The project ID

        Returns:
            JSON with full project details
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return json.dumps({"error": f"Project {project_id} not found"})

            result = {
                "status": "success",
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "description": project.description,
                    "created_at": str(project.created_at),
                },
            }

            if project.semantic_view:
                sv = project.semantic_view
                result["semantic_view"] = {
                    "name": sv.fully_qualified_name,
                    "tables": [{"alias": t.alias, "table": t.fully_qualified_name} for t in sv.tables],
                    "dimensions": [{"name": c.name, "type": c.data_type, "alias": c.table_alias} for c in sv.dimensions],
                    "metrics": [{"name": c.name, "type": c.data_type, "expression": c.expression} for c in sv.metrics],
                    "facts": [{"name": c.name, "type": c.data_type} for c in sv.facts],
                    "relationships": [{"from": f"{r.from_table}.{r.from_column}", "to": r.to_table} for r in sv.relationships],
                }

            result["faux_objects"] = [
                {
                    "name": o.fully_qualified_name,
                    "type": o.faux_type.value,
                    "dimensions": o.selected_dimensions,
                    "metrics": o.selected_metrics,
                }
                for o in project.faux_objects
            ]

            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def delete_faux_project(project_id: str) -> str:
        """Delete a Faux Objects project.

        Args:
            project_id: The project ID to delete

        Returns:
            JSON with deletion status
        """
        try:
            success = service.delete_project(project_id)
            if success:
                return json.dumps({"status": "success", "message": f"Project {project_id} deleted"})
            return json.dumps({"error": f"Project {project_id} not found"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Semantic View Definition
    # =========================================================================

    @mcp.tool()
    def define_faux_semantic_view(
        project_id: str,
        name: str,
        database: str,
        schema_name: str,
        comment: str = "",
        ai_sql_generation: str = "",
    ) -> str:
        """Define the Semantic View that this project's faux objects will wrap.

        This sets up the semantic view metadata. After defining the view,
        add tables, columns (dimensions/metrics/facts), and relationships.

        Args:
            project_id: The project ID
            name: Semantic view name (e.g., "pl_analysis")
            database: Database name (e.g., "FINANCE")
            schema_name: Schema name (e.g., "SEMANTIC")
            comment: Optional description of the semantic view
            ai_sql_generation: Optional AI context for Cortex Analyst

        Returns:
            JSON with the updated project

        Example:
            define_faux_semantic_view("abc123", "pl_analysis", "FINANCE", "SEMANTIC",
                "Profit & Loss analysis for all business units")
        """
        try:
            project = service.define_semantic_view(
                project_id, name, database, schema_name,
                comment=comment or None,
                ai_sql_generation=ai_sql_generation or None,
            )
            return json.dumps({
                "status": "success",
                "semantic_view": f"{database}.{schema_name}.{name}",
                "next_steps": [
                    "Add tables: add_faux_semantic_table()",
                    "Add columns: add_faux_semantic_column() with type='dimension'|'metric'|'fact'",
                    "Add relationships: add_faux_semantic_relationship()",
                ],
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def add_faux_semantic_table(
        project_id: str,
        alias: str,
        fully_qualified_name: str,
        primary_key: str = "",
    ) -> str:
        """Add a table reference to the semantic view definition.

        Tables are the physical data sources referenced by the semantic view.
        Each table gets an alias used to qualify dimension/metric/fact references.

        Args:
            project_id: The project ID
            alias: Short alias for the table (e.g., "gl_entries", "accounts")
            fully_qualified_name: Full table path (e.g., "FINANCE.GL.FACT_JOURNAL_ENTRIES")
            primary_key: Optional primary key column name

        Returns:
            JSON confirmation with current table count
        """
        try:
            project = service.add_semantic_table(
                project_id, alias, fully_qualified_name,
                primary_key=primary_key or None,
            )
            return json.dumps({
                "status": "success",
                "table_added": alias,
                "total_tables": len(project.semantic_view.tables),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def add_faux_semantic_column(
        project_id: str,
        name: str,
        column_type: str,
        data_type: str = "VARCHAR",
        table_alias: str = "",
        expression: str = "",
        synonyms: str = "",
        comment: str = "",
    ) -> str:
        """Add a dimension, metric, or fact column to the semantic view.

        Columns define the business concepts exposed by the semantic view:
        - **dimension**: Descriptive attributes (region, product_name, fiscal_year)
        - **metric**: Calculated aggregations (total_revenue, gross_profit)
        - **fact**: Raw measure values (debit_amount, credit_amount)

        Args:
            project_id: The project ID
            name: Column name (e.g., "total_revenue", "account_name")
            column_type: One of "dimension", "metric", or "fact"
            data_type: Snowflake data type (VARCHAR, FLOAT, INT, NUMBER, DATE, etc.)
            table_alias: Table alias prefix (e.g., "accounts" for accounts.account_name)
            expression: SQL expression for metrics (e.g., "SUM(net_amount)")
            synonyms: Comma-separated synonyms (e.g., "GL account,ledger account")
            comment: Column description

        Returns:
            JSON confirmation with column counts by type
        """
        try:
            syn_list = [s.strip() for s in synonyms.split(",") if s.strip()] if synonyms else []
            project = service.add_semantic_column(
                project_id, name, column_type,
                data_type=data_type,
                table_alias=table_alias or None,
                expression=expression or None,
                synonyms=syn_list,
                comment=comment or None,
            )
            sv = project.semantic_view
            return json.dumps({
                "status": "success",
                "column_added": name,
                "column_type": column_type,
                "counts": {
                    "dimensions": len(sv.dimensions),
                    "metrics": len(sv.metrics),
                    "facts": len(sv.facts),
                },
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def add_faux_semantic_relationship(
        project_id: str,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str = "",
    ) -> str:
        """Add a relationship between tables in the semantic view.

        Relationships define how tables join together. These map to the
        RELATIONSHIPS clause in the CREATE SEMANTIC VIEW DDL.

        Args:
            project_id: The project ID
            from_table: Source table alias (e.g., "gl_entries")
            from_column: Source column name (e.g., "account_code")
            to_table: Target table alias (e.g., "accounts")
            to_column: Target column (optional, defaults to primary key)

        Returns:
            JSON confirmation
        """
        try:
            project = service.add_semantic_relationship(
                project_id, from_table, from_column, to_table,
                to_column=to_column or None,
            )
            return json.dumps({
                "status": "success",
                "relationship_added": f"{from_table}.{from_column} -> {to_table}",
                "total_relationships": len(project.semantic_view.relationships),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Faux Object Configuration
    # =========================================================================

    @mcp.tool()
    def add_faux_object(
        project_id: str,
        name: str,
        faux_type: str,
        target_database: str,
        target_schema: str,
        selected_dimensions: str = "",
        selected_metrics: str = "",
        selected_facts: str = "",
        parameters: str = "",
        warehouse: str = "",
        target_lag: str = "",
        schedule: str = "",
        materialized_table: str = "",
        where_clause: str = "",
        comment: str = "",
    ) -> str:
        """Add a faux object configuration to the project.

        Faux objects are standard Snowflake objects that wrap the semantic view:

        - **view**: Standard VIEW using SEMANTIC_VIEW() in the AS clause.
          Universal BI tool support. No parameters.

        - **stored_procedure**: Snowpark Python procedure with RETURNS TABLE.
          BI tools call: SELECT * FROM TABLE(proc(args)).
          Supports parameters for dynamic filtering.

        - **dynamic_table**: Auto-refreshing table from semantic view query.
          Requires warehouse and target_lag. Universal BI support.

        - **task**: Scheduled materialization via Snowflake Task + procedure.
          Creates a regular table refreshed on a CRON schedule.

        Args:
            project_id: The project ID
            name: Object name (e.g., "V_PL_BY_REGION", "GET_PL_DATA")
            faux_type: One of "view", "stored_procedure", "dynamic_table", "task"
            target_database: Database for the faux object
            target_schema: Schema for the faux object
            selected_dimensions: Comma-separated dimension names (empty = all)
            selected_metrics: Comma-separated metric names (empty = all)
            selected_facts: Comma-separated fact names (empty = all)
            parameters: JSON array of procedure parameters (stored_procedure only).
                        Each: {"name": "FISCAL_YEAR", "data_type": "INT", "default_value": "2025"}
            warehouse: Warehouse name (for dynamic_table/task)
            target_lag: Refresh interval (for dynamic_table, e.g., "2 hours")
            schedule: CRON schedule (for task, e.g., "USING CRON 0 */4 * * * America/Chicago")
            materialized_table: Target table for materialization (task only)
            where_clause: Static WHERE filter (e.g., "fiscal_year = 2025")
            comment: Object description

        Returns:
            JSON confirmation with the faux object details
        """
        try:
            dims = [s.strip() for s in selected_dimensions.split(",") if s.strip()] if selected_dimensions else None
            mets = [s.strip() for s in selected_metrics.split(",") if s.strip()] if selected_metrics else None
            facts = [s.strip() for s in selected_facts.split(",") if s.strip()] if selected_facts else None

            params = None
            if parameters:
                import json as json_mod
                params = json_mod.loads(parameters)

            project = service.add_faux_object(
                project_id, name, faux_type, target_database, target_schema,
                selected_dimensions=dims,
                selected_metrics=mets,
                selected_facts=facts,
                parameters=params,
                warehouse=warehouse or None,
                target_lag=target_lag or None,
                schedule=schedule or None,
                materialized_table=materialized_table or None,
                where_clause=where_clause or None,
                comment=comment or None,
            )

            return json.dumps({
                "status": "success",
                "faux_object": {
                    "name": f"{target_database}.{target_schema}.{name}",
                    "type": faux_type,
                },
                "total_faux_objects": len(project.faux_objects),
                "next_step": "Use generate_faux_scripts() to generate SQL for all objects",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def remove_faux_object(project_id: str, object_name: str) -> str:
        """Remove a faux object from the project.

        Args:
            project_id: The project ID
            object_name: Name of the faux object to remove

        Returns:
            JSON confirmation
        """
        try:
            project = service.remove_faux_object(project_id, object_name)
            return json.dumps({
                "status": "success",
                "removed": object_name,
                "remaining": len(project.faux_objects),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # SQL Generation
    # =========================================================================

    @mcp.tool()
    def generate_faux_scripts(project_id: str) -> str:
        """Generate SQL scripts for all faux objects in a project.

        This generates the complete SQL for every configured faux object,
        plus the CREATE SEMANTIC VIEW DDL. Scripts are returned as a list
        with object name, type, and SQL content.

        Args:
            project_id: The project ID

        Returns:
            JSON with generated scripts for each object
        """
        try:
            scripts = service.generate_all_scripts(project_id)
            return json.dumps({
                "status": "success",
                "count": len(scripts),
                "scripts": [
                    {
                        "object_name": s.object_name,
                        "object_type": s.object_type.value,
                        "sql_preview": s.sql[:500] + ("..." if len(s.sql) > 500 else ""),
                        "sql_length": len(s.sql),
                        "dependencies": s.dependencies,
                    }
                    for s in scripts
                ],
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def generate_faux_deployment_bundle(project_id: str) -> str:
        """Generate a complete deployment bundle with all faux objects.

        Creates a single SQL script containing all objects in deployment order,
        with headers and comments. This can be executed directly in Snowflake
        to deploy all faux objects at once.

        Args:
            project_id: The project ID

        Returns:
            JSON with the complete deployment SQL bundle
        """
        try:
            bundle = service.generate_deployment_bundle(project_id)
            return json.dumps({
                "status": "success",
                "bundle_length": len(bundle),
                "bundle_sql": bundle,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def generate_semantic_view_ddl(project_id: str) -> str:
        """Generate the CREATE SEMANTIC VIEW DDL from the project definition.

        This generates just the semantic view DDL (not the faux object wrappers).
        Useful for reviewing or deploying the semantic view independently.

        Args:
            project_id: The project ID

        Returns:
            JSON with the semantic view DDL
        """
        try:
            project = service.get_project(project_id)
            if not project or not project.semantic_view:
                return json.dumps({"error": f"Project {project_id} not found or no semantic view defined"})

            ddl = service.generate_semantic_view_ddl(project.semantic_view)
            return json.dumps({
                "status": "success",
                "semantic_view": project.semantic_view.fully_qualified_name,
                "ddl": ddl,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def export_faux_scripts(project_id: str, output_dir: str = "") -> str:
        """Export all generated SQL scripts to individual files.

        Creates one .sql file per object plus a deployment_bundle.sql
        containing everything in deployment order.

        Args:
            project_id: The project ID
            output_dir: Directory for output files (default: data/faux_objects/exports/{project_id})

        Returns:
            JSON with file paths for each exported script
        """
        try:
            if not output_dir:
                import os
                output_dir = os.path.join(service.data_dir, "faux_objects", "exports", project_id)

            exported = service.export_scripts(project_id, output_dir)
            return json.dumps({
                "status": "success",
                "files_exported": len(exported),
                "output_dir": output_dir,
                "files": exported,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # SQL Translation Tools
    # =========================================================================

    @mcp.tool()
    def detect_sql_format(sql: str) -> str:
        """Detect the format of a SQL statement.

        Analyzes the SQL to determine if it's a CREATE VIEW, SELECT query,
        or CREATE SEMANTIC VIEW DDL statement.

        Args:
            sql: The SQL statement to analyze

        Returns:
            JSON with the detected format and description

        Example:
            detect_sql_format("SELECT * FROM orders GROUP BY region")
            # Returns: {"format": "select_query", "description": "SELECT query with aggregations"}
        """
        try:
            from .sql_translator import SQLTranslator, SQLInputFormat

            translator = SQLTranslator()
            format_type = translator.detect_format(sql)

            descriptions = {
                SQLInputFormat.CREATE_VIEW: "CREATE VIEW statement wrapping a query",
                SQLInputFormat.SELECT_QUERY: "SELECT query (may contain aggregations)",
                SQLInputFormat.CREATE_SEMANTIC_VIEW: "CREATE SEMANTIC VIEW DDL statement",
                SQLInputFormat.UNKNOWN: "Unknown SQL format",
            }

            return json.dumps({
                "status": "success",
                "format": format_type.value,
                "description": descriptions.get(format_type, "Unknown"),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def translate_sql_to_semantic_view(
        sql: str,
        name: str = "",
        database: str = "",
        schema_name: str = "",
    ) -> str:
        """Parse SQL into a SemanticViewDefinition.

        Accepts CREATE VIEW, SELECT query, or CREATE SEMANTIC VIEW DDL and
        reverse-engineers it into a structured semantic view definition with
        tables, dimensions, metrics, facts, and relationships.

        Column classification rules:
        - Columns in GROUP BY → DIMENSION
        - Columns with aggregations (SUM, COUNT, AVG, MIN, MAX) → METRIC
        - Raw columns not in GROUP BY → FACT

        Args:
            sql: SQL statement (CREATE VIEW, SELECT, or CREATE SEMANTIC VIEW)
            name: Override name for the semantic view (optional)
            database: Override database (optional)
            schema_name: Override schema (optional)

        Returns:
            JSON with tables, dimensions, metrics, facts, relationships, and warnings

        Example:
            translate_sql_to_semantic_view('''
                SELECT region, SUM(amount) as total_sales
                FROM orders
                GROUP BY region
            ''', name="sales_analysis")
        """
        try:
            from .sql_translator import SQLTranslator

            translator = SQLTranslator()
            result = translator.translate(
                sql,
                name=name or None,
                database=database or None,
                schema_name=schema_name or None,
            )

            sv = result.semantic_view

            return json.dumps({
                "status": "success",
                "input_format": result.input_format.value,
                "semantic_view": {
                    "name": sv.name,
                    "database": sv.database,
                    "schema": sv.schema_name,
                    "comment": sv.comment,
                    "ai_sql_generation": sv.ai_sql_generation,
                },
                "tables": [
                    {
                        "alias": t.alias,
                        "fully_qualified_name": t.fully_qualified_name,
                        "primary_key": t.primary_key,
                    }
                    for t in sv.tables
                ],
                "relationships": [
                    {
                        "from": f"{r.from_table}.{r.from_column}",
                        "to": f"{r.to_table}" + (f".{r.to_column}" if r.to_column else ""),
                    }
                    for r in sv.relationships
                ],
                "dimensions": [
                    {
                        "name": d.name,
                        "data_type": d.data_type,
                        "table_alias": d.table_alias,
                        "synonyms": d.synonyms,
                    }
                    for d in sv.dimensions
                ],
                "metrics": [
                    {
                        "name": m.name,
                        "data_type": m.data_type,
                        "expression": m.expression,
                        "table_alias": m.table_alias,
                    }
                    for m in sv.metrics
                ],
                "facts": [
                    {
                        "name": f.name,
                        "data_type": f.data_type,
                        "table_alias": f.table_alias,
                    }
                    for f in sv.facts
                ],
                "warnings": result.warnings,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def translate_sql_to_faux_project(
        sql: str,
        project_name: str,
        description: str = "",
        faux_type: str = "",
        target_database: str = "",
        target_schema: str = "",
    ) -> str:
        """Parse SQL and create a complete FauxProject in one step.

        This combines SQL parsing with project creation. The SQL is analyzed
        to extract the semantic view structure, then a project is created
        with the semantic view definition populated.

        Args:
            sql: SQL statement to parse
            project_name: Name for the new project
            description: Project description (optional)
            faux_type: Optional faux object type to create: "view", "stored_procedure",
                       "dynamic_table", or "task". If provided, a faux object is added.
            target_database: Target database for faux object (uses semantic view database if empty)
            target_schema: Target schema for faux object (uses semantic view schema if empty)

        Returns:
            JSON with created project details including ID and semantic view info

        Example:
            translate_sql_to_faux_project('''
                SELECT region, SUM(amount) as total_sales
                FROM WAREHOUSE.SALES.ORDERS o
                GROUP BY region
            ''', "Sales Analysis", faux_type="view")
        """
        try:
            from .sql_translator import SQLTranslator

            translator = SQLTranslator()
            project = translator.translate_to_project(
                sql=sql,
                project_name=project_name,
                service=service,
                description=description,
                faux_type=faux_type or None,
                target_database=target_database or None,
                target_schema=target_schema or None,
            )

            result = {
                "status": "success",
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "description": project.description,
                },
            }

            if project.semantic_view:
                sv = project.semantic_view
                result["semantic_view"] = {
                    "name": sv.fully_qualified_name,
                    "tables": len(sv.tables),
                    "dimensions": len(sv.dimensions),
                    "metrics": len(sv.metrics),
                    "facts": len(sv.facts),
                    "relationships": len(sv.relationships),
                }

            if project.faux_objects:
                result["faux_objects"] = [
                    {
                        "name": o.fully_qualified_name,
                        "type": o.faux_type.value,
                    }
                    for o in project.faux_objects
                ]

            result["next_steps"] = [
                "Use get_faux_project() to view full details",
                "Use generate_faux_scripts() to generate SQL",
                "Use add_faux_object() to add more wrapper objects",
            ]

            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def convert_sql_format(
        sql: str,
        target_format: str,
        name: str = "",
        database: str = "",
        schema_name: str = "",
        target_database: str = "",
        target_schema: str = "",
    ) -> str:
        """Convert SQL from one format to another.

        Supports conversions between:
        - SELECT query ↔ CREATE SEMANTIC VIEW DDL
        - CREATE VIEW ↔ CREATE SEMANTIC VIEW DDL
        - Any format → SELECT query

        Target formats:
        - "semantic_view_ddl": CREATE SEMANTIC VIEW statement
        - "create_view": CREATE VIEW wrapping SEMANTIC_VIEW() call
        - "select_query": Plain SELECT query

        Args:
            sql: Source SQL statement
            target_format: Target format ("semantic_view_ddl", "create_view", "select_query")
            name: Override semantic view name (optional)
            database: Override database (optional)
            schema_name: Override schema (optional)
            target_database: Target database for faux objects (optional)
            target_schema: Target schema for faux objects (optional)

        Returns:
            JSON with converted SQL

        Example:
            convert_sql_format('''
                SELECT region, SUM(amount) as total_sales
                FROM orders GROUP BY region
            ''', "semantic_view_ddl", name="sales_summary", database="ANALYTICS")
        """
        try:
            from .sql_translator import SQLTranslator

            translator = SQLTranslator()

            # Detect source format for info
            source_format = translator.detect_format(sql)

            converted = translator.convert(
                sql=sql,
                target_format=target_format,
                name=name or None,
                database=database or None,
                schema_name=schema_name or None,
                target_database=target_database or None,
                target_schema=target_schema or None,
            )

            return json.dumps({
                "status": "success",
                "source_format": source_format.value,
                "target_format": target_format,
                "sql": converted,
                "sql_length": len(converted),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return service
