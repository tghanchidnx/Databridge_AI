"""DataShield MCP Tools - Phase 33

Provides 12 tools for confidential data scrambling:

Project Management (4 tools):
- create_shield_project: Create a new DataShield project
- list_shield_projects: List all shield projects
- get_shield_project: Get project details and table configs
- delete_shield_project: Remove a shield project

Table Configuration (3 tools):
- auto_classify_table: Auto-detect column classifications
- add_table_shield: Add/update table shield config
- remove_table_shield: Remove a table from shield project

Data Operations (3 tools):
- preview_scrambled_data: Show before/after preview
- shield_local_file: Apply scrambling to CSV/JSON file
- generate_shield_ddl: Generate Snowflake UDFs + Views DDL

Status & Deployment (2 tools):
- deploy_shield_to_snowflake: Execute DDL on Snowflake
- get_shield_status: Project status and key health
"""

import json
import logging
from typing import Optional, List
from pathlib import Path

from .types import (
    ColumnRule,
    ColumnClassification,
    ScrambleStrategy,
    TableShieldConfig,
    ShieldProject,
    CLASSIFICATION_STRATEGY_MAP,
)
from .service import ShieldService
from .classifier import auto_classify_columns
from .snowflake_generator import generate_full_ddl
from .interceptor import DataShieldInterceptor

logger = logging.getLogger(__name__)

# Module-level state
_service: Optional[ShieldService] = None


def _ensure_initialized(settings) -> ShieldService:
    """Ensure ShieldService is initialized."""
    global _service
    if _service is None:
        data_dir = Path(settings.data_dir) / "datashield"
        _service = ShieldService(data_dir=str(data_dir))
    return _service


def register_datashield_tools(mcp, settings):
    """Register all DataShield MCP tools."""

    service = _ensure_initialized(settings)

    # =========================================================================
    # Project Management (4 tools)
    # =========================================================================

    @mcp.tool()
    def create_shield_project(
        name: str,
        passphrase: str,
        description: str = "",
    ) -> dict:
        """Create a new DataShield project with a dedicated encryption key.

        A shield project groups table configurations under a single encryption
        key. The passphrase protects the local keystore and is required to
        scramble or unscramble data.

        Args:
            name: Project name (e.g., "Client X Financial Data")
            passphrase: Passphrase to protect the encryption keystore
            description: Optional project description

        Returns:
            Project details including ID and key alias

        Example:
            create_shield_project(
                name="ACME Financial",
                passphrase="my-secure-passphrase",
                description="Shield for ACME Corp financial data"
            )
        """
        try:
            project = service.create_project(
                name=name,
                passphrase=passphrase,
                description=description or None,
            )
            return {
                "status": "success",
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "key_alias": project.key_alias,
                    "created_at": project.created_at,
                },
                "message": f"Shield project '{name}' created. Add tables with add_table_shield().",
            }
        except Exception as e:
            logger.error("Failed to create shield project: %s", e)
            return {"error": str(e)}

    @mcp.tool()
    def list_shield_projects() -> dict:
        """List all DataShield projects.

        Returns summary of each project including ID, name, table count,
        and active status.

        Returns:
            List of project summaries
        """
        projects = service.list_projects()
        return {
            "status": "success",
            "count": len(projects),
            "projects": projects,
        }

    @mcp.tool()
    def get_shield_project(project_id: str) -> dict:
        """Get full details of a DataShield project.

        Args:
            project_id: The project ID (e.g., "proj_a1b2c3")

        Returns:
            Full project details including all table configs and column rules
        """
        project = service.get_project(project_id)
        if not project:
            return {"error": f"Project not found: {project_id}"}

        return {
            "status": "success",
            "project": project.model_dump(),
        }

    @mcp.tool()
    def delete_shield_project(project_id: str) -> dict:
        """Delete a DataShield project and its encryption key.

        This permanently removes the project config and its key from the
        local keystore. Scrambled data cannot be reversed after deletion.

        Args:
            project_id: The project ID to delete

        Returns:
            Deletion confirmation
        """
        try:
            deleted = service.delete_project(project_id)
            if deleted:
                return {"status": "success", "message": f"Project {project_id} deleted"}
            return {"error": f"Project not found: {project_id}"}
        except Exception as e:
            return {"error": str(e)}

    # =========================================================================
    # Table Configuration (3 tools)
    # =========================================================================

    @mcp.tool()
    def auto_classify_table(
        columns: str,
        sample_data: str = "",
        row_count: int = 0,
    ) -> dict:
        """Auto-detect column classifications and suggest scrambling rules.

        Analyzes column names, data types, and optionally sample values to
        classify each column and recommend an appropriate scrambling strategy.

        Args:
            columns: JSON array of column objects with "name" and "data_type" keys.
                Example: [{"name": "AMOUNT", "data_type": "NUMBER(18,2)"},
                          {"name": "VENDOR_NAME", "data_type": "VARCHAR(200)"}]
            sample_data: Optional JSON object mapping column names to sample value arrays.
                Example: {"AMOUNT": [1234.56, 789.10], "VENDOR_NAME": ["Acme", "Beta"]}
            row_count: Total row count for cardinality analysis

        Returns:
            Suggested column rules with classifications and strategies

        Example:
            auto_classify_table(
                columns='[{"name":"AMOUNT","data_type":"NUMBER"},{"name":"SSN","data_type":"VARCHAR"}]'
            )
        """
        try:
            cols = json.loads(columns)
            samples = json.loads(sample_data) if sample_data else None

            rules = auto_classify_columns(cols, samples, row_count)

            return {
                "status": "success",
                "column_count": len(rules),
                "rules": [r.model_dump() for r in rules],
                "message": "Review and adjust classifications before adding to a shield project.",
            }
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON: {e}"}
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def add_table_shield(
        project_id: str,
        database: str,
        schema_name: str,
        table_name: str,
        column_rules: str,
        table_type: str = "unknown",
        key_columns: str = "[]",
        skip_columns: str = "[]",
    ) -> dict:
        """Add or update a table shield configuration in a project.

        Args:
            project_id: Target shield project ID
            database: Database name (e.g., "ANALYTICS")
            schema_name: Schema name (e.g., "FINANCE")
            table_name: Table name (e.g., "FACT_JOURNAL_ENTRIES")
            column_rules: JSON array of column rule objects. Each must have:
                "column_name", "classification", "strategy".
                Optional: "preserve_nulls", "preserve_format", "synthetic_pool"
            table_type: Table type - "fact", "dimension", or "unknown"
            key_columns: JSON array of PK/FK column names for referential integrity
            skip_columns: JSON array of columns to pass through unchanged

        Returns:
            Confirmation with table details

        Example:
            add_table_shield(
                project_id="proj_a1b2c3",
                database="ANALYTICS",
                schema_name="FINANCE",
                table_name="FACT_SALES",
                column_rules='[{"column_name":"AMOUNT","classification":"measure","strategy":"numeric_scaling"}]'
            )
        """
        try:
            rules = [ColumnRule(**r) for r in json.loads(column_rules)]
            keys = json.loads(key_columns)
            skips = json.loads(skip_columns)

            config = TableShieldConfig(
                database=database,
                schema_name=schema_name,
                table_name=table_name,
                table_type=table_type,
                column_rules=rules,
                key_columns=keys,
                skip_columns=skips,
            )

            service.add_table_shield(project_id, config)

            return {
                "status": "success",
                "table": f"{database}.{schema_name}.{table_name}",
                "rules_count": len(rules),
                "message": f"Table shield added with {len(rules)} column rules.",
            }
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON: {e}"}
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def remove_table_shield(
        project_id: str,
        database: str,
        schema_name: str,
        table_name: str,
    ) -> dict:
        """Remove a table from a shield project.

        Args:
            project_id: Shield project ID
            database: Database name
            schema_name: Schema name
            table_name: Table name

        Returns:
            Removal confirmation
        """
        try:
            removed = service.remove_table_shield(
                project_id, database, schema_name, table_name
            )
            if removed:
                return {
                    "status": "success",
                    "message": f"Removed {database}.{schema_name}.{table_name} from project",
                }
            return {"error": "Table not found in project"}
        except Exception as e:
            return {"error": str(e)}

    # =========================================================================
    # Data Operations (3 tools)
    # =========================================================================

    @mcp.tool()
    def preview_scrambled_data(
        project_id: str,
        passphrase: str,
        database: str,
        schema_name: str,
        table_name: str,
        sample_rows: str = "[]",
    ) -> dict:
        """Preview before/after scrambling for a shielded table.

        Shows original and scrambled values side by side for up to 5 rows
        so you can verify the scrambling looks correct before deployment.

        Args:
            project_id: Shield project ID
            passphrase: Passphrase to unlock the keystore
            database: Database name
            schema_name: Schema name
            table_name: Table name
            sample_rows: JSON array of row objects to preview.
                Example: [{"AMOUNT": 1234.56, "VENDOR": "Acme Corp"}]

        Returns:
            Before/after comparison for each column

        Example:
            preview_scrambled_data(
                project_id="proj_a1b2c3",
                passphrase="my-passphrase",
                database="ANALYTICS",
                schema_name="FINANCE",
                table_name="FACT_SALES",
                sample_rows='[{"AMOUNT":1234.56,"VENDOR":"Acme Corp","STATUS":"Active"}]'
            )
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return {"error": f"Project not found: {project_id}"}

            # Find table config
            fqn = f"{database}.{schema_name}.{table_name}"
            table_config = None
            for t in project.tables:
                if f"{t.database}.{t.schema_name}.{t.table_name}" == fqn:
                    table_config = t
                    break
            if not table_config:
                return {"error": f"Table {fqn} not found in project"}

            engine = service.get_engine(project_id, passphrase)

            rows = json.loads(sample_rows) if isinstance(sample_rows, str) else sample_rows
            if not rows:
                return {"error": "No sample rows provided"}

            # Limit to 5 rows
            rows = rows[:5]
            previews = []

            rules_by_col = {r.column_name: r for r in table_config.column_rules}

            for row in rows:
                preview_row = {}
                for col_name, original in row.items():
                    rule = rules_by_col.get(col_name)
                    if rule and col_name not in table_config.skip_columns:
                        scrambled = engine.scramble(original, rule)
                        preview_row[col_name] = {
                            "original": original,
                            "scrambled": scrambled,
                            "strategy": rule.strategy.value,
                        }
                    else:
                        preview_row[col_name] = {
                            "original": original,
                            "scrambled": original,
                            "strategy": "passthrough (no rule)",
                        }
                previews.append(preview_row)

            return {
                "status": "success",
                "table": fqn,
                "preview_rows": len(previews),
                "data": previews,
            }
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def shield_local_file(
        project_id: str,
        passphrase: str,
        input_path: str,
        output_path: str,
        database: str = "LOCAL",
        schema_name: str = "FILES",
        table_name: str = "",
    ) -> dict:
        """Apply scrambling to a CSV or JSON file and write a shielded copy.

        The output file has the same structure but with values scrambled
        according to the table's column rules.

        Args:
            project_id: Shield project ID
            passphrase: Passphrase to unlock the keystore
            input_path: Path to source file (CSV or JSON)
            output_path: Path to write the shielded output file
            database: Database reference for table lookup (default "LOCAL")
            schema_name: Schema reference for table lookup (default "FILES")
            table_name: Table name reference. If empty, uses input filename stem.

        Returns:
            Summary with row count and columns processed
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return {"error": f"Project not found: {project_id}"}

            if not table_name:
                table_name = Path(input_path).stem.upper()

            fqn = f"{database}.{schema_name}.{table_name}"
            table_config = None
            for t in project.tables:
                if f"{t.database}.{t.schema_name}.{t.table_name}" == fqn:
                    table_config = t
                    break
            if not table_config:
                return {"error": f"Table {fqn} not found in project. Add it with add_table_shield() first."}

            engine = service.get_engine(project_id, passphrase)
            interceptor = DataShieldInterceptor(engine)

            ext = Path(input_path).suffix.lower()
            if ext == ".csv":
                result = interceptor.shield_csv(input_path, output_path, table_config)
            elif ext == ".json":
                result = interceptor.shield_json(input_path, output_path, table_config)
            else:
                return {"error": f"Unsupported file type: {ext}. Use .csv or .json"}

            return {"status": "success", **result}
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def generate_shield_ddl(
        project_id: str,
        key_ref: str = "",
    ) -> dict:
        """Generate Snowflake UDFs and shielded views DDL for a project.

        Produces SQL that creates:
        1. DATASHIELD schema with scrambling UDFs
        2. VW_SHIELDED_* views for each table in the project

        The DDL can be reviewed, saved, or executed with deploy_shield_to_snowflake().

        Args:
            project_id: Shield project ID
            key_ref: Key reference to embed in UDF calls. If empty, uses project key_alias.

        Returns:
            Generated DDL string

        Example:
            generate_shield_ddl(project_id="proj_a1b2c3")
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return {"error": f"Project not found: {project_id}"}

            if not project.tables:
                return {"error": "Project has no tables configured. Add tables with add_table_shield() first."}

            ddl = generate_full_ddl(project, key_ref or None)

            return {
                "status": "success",
                "project": project.name,
                "tables": len(project.tables),
                "ddl": ddl,
                "message": "Review the DDL before deploying. Use deploy_shield_to_snowflake() to execute.",
            }
        except Exception as e:
            return {"error": str(e)}

    # =========================================================================
    # Status & Deployment (2 tools)
    # =========================================================================

    @mcp.tool()
    def deploy_shield_to_snowflake(
        project_id: str,
        connection_id: str,
        key_ref: str = "",
    ) -> dict:
        """Execute DataShield DDL on a target Snowflake connection.

        Creates the DATASHIELD schema, UDFs, and shielded views on the
        specified Snowflake connection.

        Args:
            project_id: Shield project ID
            connection_id: Backend connection ID for Snowflake
            key_ref: Key reference for UDF calls

        Returns:
            Deployment result with statement count
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return {"error": f"Project not found: {project_id}"}

            ddl = generate_full_ddl(project, key_ref or None)

            # Try to get the query_database function from server context
            try:
                try:
                    from src.server import _connections
                except ImportError:
                    from server import _connections

                if connection_id not in _connections:
                    return {"error": f"Connection not found: {connection_id}. Use get_console_connections() to list available connections."}

                conn = _connections[connection_id]
                engine = conn.get("engine")
                if not engine:
                    return {"error": "Connection has no active engine"}

                # Execute DDL statements
                from sqlalchemy import text
                statements = [s.strip() for s in ddl.split(";") if s.strip()]
                executed = 0
                with engine.connect() as connection:
                    for stmt in statements:
                        if stmt.startswith("--"):
                            continue
                        connection.execute(text(stmt))
                        executed += 1
                    connection.commit()

                return {
                    "status": "success",
                    "statements_executed": executed,
                    "message": f"Deployed {executed} DDL statements to Snowflake via {connection_id}",
                }
            except ImportError:
                return {
                    "error": "Database connections not available. Use generate_shield_ddl() to get the DDL and execute manually.",
                }
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def get_shield_status(
        project_id: str = "",
    ) -> dict:
        """Get DataShield status overview.

        Shows keystore health, project count, and optionally detailed
        status for a specific project.

        Args:
            project_id: Optional project ID for detailed status

        Returns:
            Status overview including keystore state and project details
        """
        try:
            status = service.get_status(project_id or None)
            return {"status": "success", **status}
        except Exception as e:
            return {"error": str(e)}

    return service
