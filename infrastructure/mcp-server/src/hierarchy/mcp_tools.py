"""MCP Tools for Hierarchy Builder.

AUTO-SYNC FEATURE:
When enabled (default), all write operations (create, update, delete)
automatically sync to the NestJS backend. This ensures the MCP server
and Web UI always reflect the same data without manual sync calls.
"""
import json
import logging
from datetime import datetime
from typing import Optional

from .service import HierarchyService
from .api_sync import HierarchyApiSync
from ..config import settings

# Configure logging
logger = logging.getLogger("hierarchy_mcp_tools")


def register_hierarchy_tools(mcp, data_dir: str = "data"):
    """Register all hierarchy MCP tools with the server."""

    service = HierarchyService(data_dir)

    # Initialize sync service if enabled
    sync_service = None
    auto_sync_manager = None
    if settings.nestjs_sync_enabled:
        sync_service = HierarchyApiSync(
            base_url=settings.nestjs_backend_url,
            api_key=settings.nestjs_api_key,
            auto_sync=True,  # Enable auto-sync by default
        )
        # Connect local service for auto-sync
        sync_service.set_local_service(service)
        auto_sync_manager = sync_service.auto_sync_manager

    def _auto_sync_operation(
        operation: str,
        project_id: str,
        hierarchy_id: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> dict:
        """Helper to perform auto-sync after local operations."""
        if auto_sync_manager and auto_sync_manager.is_enabled:
            try:
                result = auto_sync_manager.on_local_change(
                    operation=operation,
                    project_id=project_id,
                    hierarchy_id=hierarchy_id,
                    data=data,
                )
                return result
            except Exception as e:
                logger.warning(f"Auto-sync failed: {e}")
                return {"auto_sync": "failed", "error": str(e)}
        return {"auto_sync": "disabled"}

    # =========================================================================
    # Project Management Tools
    # =========================================================================

    @mcp.tool()
    def create_hierarchy_project(name: str, description: str = "") -> str:
        """
        Create a new hierarchy project.

        AUTO-SYNC: When enabled, automatically creates the project in the backend.

        Args:
            name: Project name (e.g., "Financial Reporting 2024")
            description: Optional project description

        Returns:
            JSON with project ID and details (includes auto_sync status)
        """
        try:
            project = service.create_project(name, description)

            # Auto-sync to backend
            sync_result = _auto_sync_operation(
                operation="create_project",
                project_id=project.id,
                data={"name": name, "description": description},
            )

            return json.dumps({
                "status": "success",
                "project": project.model_dump(),
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_hierarchy_projects() -> str:
        """
        List all hierarchy projects with summary statistics.

        Returns:
            JSON array of projects with hierarchy counts
        """
        try:
            projects = service.list_projects()
            return json.dumps({
                "total": len(projects),
                "projects": projects,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_hierarchy_project(project_id: str) -> str:
        """
        Get detailed information about a specific project.

        Args:
            project_id: Project UUID

        Returns:
            JSON with project details
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return json.dumps({"error": f"Project '{project_id}' not found"})
            return json.dumps(project, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def delete_hierarchy_project(project_id: str) -> str:
        """
        Delete a project and all its hierarchies.

        AUTO-SYNC: When enabled, automatically deletes the project in the backend.

        Args:
            project_id: Project UUID to delete

        Returns:
            JSON with deletion status (includes auto_sync status)
        """
        try:
            success = service.delete_project(project_id)
            if success:
                # Auto-sync to backend
                sync_result = _auto_sync_operation(
                    operation="delete_project",
                    project_id=project_id,
                )
                return json.dumps({
                    "status": "success",
                    "message": "Project deleted",
                    "sync": sync_result,
                })
            return json.dumps({"error": "Project not found"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Hierarchy CRUD Tools
    # =========================================================================

    @mcp.tool()
    def create_hierarchy(
        project_id: str,
        hierarchy_name: str,
        parent_id: str = "",
        description: str = "",
        flags: str = "{}"
    ) -> str:
        """
        Create a new hierarchy node in a project.

        AUTO-SYNC: When enabled, automatically creates the hierarchy in the backend.

        Args:
            project_id: Target project UUID
            hierarchy_name: Display name for the hierarchy
            parent_id: Optional parent hierarchy ID for nesting
            description: Optional description
            flags: JSON string of hierarchy flags (include_flag, calculation_flag, etc.)

        Returns:
            JSON with created hierarchy details (includes auto_sync status)

        Example flags:
            {"calculation_flag": true, "active_flag": true, "is_leaf_node": false}
        """
        try:
            flags_dict = json.loads(flags) if flags else {}
            hierarchy = service.create_hierarchy(
                project_id=project_id,
                hierarchy_name=hierarchy_name,
                parent_id=parent_id if parent_id else None,
                description=description,
                flags=flags_dict,
            )

            # Auto-sync to backend
            sync_result = _auto_sync_operation(
                operation="create_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy.hierarchy_id,
                data={
                    "hierarchy_name": hierarchy_name,
                    "parent_id": parent_id if parent_id else None,
                    "description": description,
                    "flags": flags_dict,
                },
            )

            return json.dumps({
                "status": "success",
                "hierarchy": hierarchy.model_dump(),
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_hierarchy(project_id: str, hierarchy_id: str) -> str:
        """
        Get a specific hierarchy by ID.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID (slug)

        Returns:
            JSON with hierarchy details
        """
        try:
            hierarchy = service.get_hierarchy(project_id, hierarchy_id)
            if not hierarchy:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})
            return json.dumps(hierarchy, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def update_hierarchy(project_id: str, hierarchy_id: str, updates: str) -> str:
        """
        Update an existing hierarchy.

        AUTO-SYNC: When enabled, automatically updates the hierarchy in the backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy to update
            updates: JSON string of fields to update

        Returns:
            JSON with updated hierarchy (includes auto_sync status)
        """
        try:
            updates_dict = json.loads(updates)
            hierarchy = service.update_hierarchy(project_id, hierarchy_id, updates_dict)
            if not hierarchy:
                return json.dumps({"error": "Hierarchy not found"})

            # Auto-sync to backend
            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data=hierarchy,
            )

            return json.dumps({
                "status": "success",
                "hierarchy": hierarchy,
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def delete_hierarchy(project_id: str, hierarchy_id: str) -> str:
        """
        Delete a hierarchy.

        AUTO-SYNC: When enabled, automatically deletes the hierarchy in the backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy to delete

        Returns:
            JSON with deletion status (includes auto_sync status)
        """
        try:
            success = service.delete_hierarchy(project_id, hierarchy_id)
            if success:
                # Auto-sync to backend
                sync_result = _auto_sync_operation(
                    operation="delete_hierarchy",
                    project_id=project_id,
                    hierarchy_id=hierarchy_id,
                )
                return json.dumps({
                    "status": "success",
                    "message": "Hierarchy deleted",
                    "sync": sync_result,
                })
            return json.dumps({"error": "Hierarchy not found"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_hierarchies(project_id: str) -> str:
        """
        List all hierarchies in a project.

        Args:
            project_id: Project UUID

        Returns:
            JSON array of hierarchies
        """
        try:
            hierarchies = service.list_hierarchies(project_id)
            return json.dumps({
                "total": len(hierarchies),
                "hierarchies": hierarchies,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_hierarchy_tree(project_id: str) -> str:
        """
        Get the complete hierarchy tree for a project.

        Args:
            project_id: Project UUID

        Returns:
            JSON tree structure with all hierarchies and their children
        """
        try:
            tree = service.get_hierarchy_tree(project_id)
            return json.dumps({
                "root_count": len(tree),
                "tree": tree,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Source Mapping Tools
    # =========================================================================

    @mcp.tool()
    def add_source_mapping(
        project_id: str,
        hierarchy_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_column: str,
        source_uid: str = "",
        precedence_group: str = "1"
    ) -> str:
        """
        Add a source mapping to a hierarchy.

        Maps a database column/value to a hierarchy node for data aggregation.

        AUTO-SYNC: When enabled, automatically adds the mapping in the backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Target hierarchy
            source_database: Database name (e.g., "WAREHOUSE")
            source_schema: Schema name (e.g., "FINANCE")
            source_table: Table name (e.g., "DIM_ACCOUNT")
            source_column: Column name (e.g., "ACCOUNT_CODE")
            source_uid: Specific value to match (e.g., "4100-500")
            precedence_group: Grouping for precedence logic (default "1")

        Returns:
            JSON with updated hierarchy (includes auto_sync status)
        """
        try:
            hierarchy = service.add_source_mapping(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                source_column=source_column,
                source_uid=source_uid,
                precedence_group=precedence_group,
            )
            if not hierarchy:
                return json.dumps({"error": "Hierarchy not found"})

            # Auto-sync to backend
            sync_result = _auto_sync_operation(
                operation="add_mapping",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={
                    "source_database": source_database,
                    "source_schema": source_schema,
                    "source_table": source_table,
                    "source_column": source_column,
                    "source_uid": source_uid,
                    "precedence_group": precedence_group,
                },
            )

            return json.dumps({
                "status": "success",
                "hierarchy": hierarchy,
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def remove_source_mapping(
        project_id: str,
        hierarchy_id: str,
        mapping_index: int
    ) -> str:
        """
        Remove a source mapping by index.

        Args:
            project_id: Project UUID
            hierarchy_id: Target hierarchy
            mapping_index: Index of mapping to remove

        Returns:
            JSON with updated hierarchy
        """
        try:
            hierarchy = service.remove_source_mapping(project_id, hierarchy_id, mapping_index)
            if not hierarchy:
                return json.dumps({"error": "Hierarchy not found"})
            return json.dumps({
                "status": "success",
                "hierarchy": hierarchy,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_inherited_mappings(project_id: str, hierarchy_uuid: str) -> str:
        """
        Get all mappings for a hierarchy including those inherited from children.

        Child mappings propagate UP to parent levels (not the other way).
        This allows parent nodes to aggregate all mappings from their descendants.

        Args:
            project_id: Project UUID
            hierarchy_uuid: The UUID (not hierarchy_id) of the hierarchy

        Returns:
            JSON with:
            - own_mappings: Mappings directly on this hierarchy
            - inherited_mappings: Mappings from all child hierarchies
            - by_precedence: All mappings grouped by precedence_group
            - child_counts: Mapping counts per immediate child
            - total_count: Total number of mappings
        """
        try:
            result = service.get_inherited_mappings(project_id, hierarchy_uuid)
            return json.dumps(result, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_mapping_summary(project_id: str) -> str:
        """
        Get mapping summary for entire project with inheritance info.

        Shows each hierarchy's own mappings and total mappings (including
        those inherited from children). Use this to understand the complete
        mapping coverage across your hierarchy tree.

        Args:
            project_id: Project UUID

        Returns:
            JSON with mapping summary for all hierarchies
        """
        try:
            result = service.get_mapping_summary(project_id)
            return json.dumps(result, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_mappings_by_precedence(project_id: str, hierarchy_uuid: str, precedence_group: str = None) -> str:
        """
        Get mappings filtered by precedence group.

        Precedence groups segregate mappings into separate logical groupings.
        Each unique precedence value represents a separate mapping context.

        Args:
            project_id: Project UUID
            hierarchy_uuid: The UUID of the hierarchy
            precedence_group: Optional - filter to specific precedence group

        Returns:
            JSON with mappings organized by precedence group
        """
        try:
            result = service.get_inherited_mappings(project_id, hierarchy_uuid)
            if "error" in result:
                return json.dumps(result)

            by_precedence = result.get("by_precedence", {})

            if precedence_group:
                filtered = {precedence_group: by_precedence.get(precedence_group, [])}
                return json.dumps({
                    "hierarchy_id": result.get("hierarchy_id"),
                    "precedence_group": precedence_group,
                    "mappings": filtered.get(precedence_group, []),
                    "count": len(filtered.get(precedence_group, [])),
                }, default=str, indent=2)

            return json.dumps({
                "hierarchy_id": result.get("hierarchy_id"),
                "precedence_groups": result.get("precedence_groups", []),
                "by_precedence": by_precedence,
                "counts_by_group": {k: len(v) for k, v in by_precedence.items()},
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Formula Tools
    # =========================================================================

    @mcp.tool()
    def create_formula_group(
        project_id: str,
        main_hierarchy_id: str,
        group_name: str,
        rules: str
    ) -> str:
        """
        Create a formula group for calculated hierarchies.

        Args:
            project_id: Project UUID
            main_hierarchy_id: Hierarchy that stores the calculation result
            group_name: Name for the formula group
            rules: JSON array of formula rules

        Rules format example:
            [
                {"operation": "SUM", "hierarchy_id": "REVENUE_1", "precedence": 1},
                {"operation": "SUBTRACT", "hierarchy_id": "EXPENSES_1", "precedence": 2}
            ]

        Returns:
            JSON with updated hierarchy
        """
        try:
            rules_list = json.loads(rules)
            hierarchy = service.create_formula_group(
                project_id=project_id,
                main_hierarchy_id=main_hierarchy_id,
                group_name=group_name,
                rules=rules_list,
            )
            if not hierarchy:
                return json.dumps({"error": "Hierarchy not found"})
            return json.dumps({
                "status": "success",
                "hierarchy": hierarchy,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def add_formula_rule(
        project_id: str,
        main_hierarchy_id: str,
        operation: str,
        source_hierarchy_id: str,
        precedence: int = 1,
        constant_number: str = ""
    ) -> str:
        """
        Add a rule to an existing formula group.

        Args:
            project_id: Project UUID
            main_hierarchy_id: Hierarchy with the formula
            operation: SUM, SUBTRACT, MULTIPLY, DIVIDE, AVERAGE
            source_hierarchy_id: Hierarchy to include in calculation
            precedence: Order of operations (1, 2, 3...)
            constant_number: Optional constant for multiply/divide

        Returns:
            JSON with updated hierarchy
        """
        try:
            const = float(constant_number) if constant_number else None
            hierarchy = service.add_formula_rule(
                project_id=project_id,
                main_hierarchy_id=main_hierarchy_id,
                operation=operation,
                source_hierarchy_id=source_hierarchy_id,
                precedence=precedence,
                constant_number=const,
            )
            if not hierarchy:
                return json.dumps({"error": "Hierarchy not found"})
            return json.dumps({
                "status": "success",
                "hierarchy": hierarchy,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_formula_groups(project_id: str) -> str:
        """
        List all hierarchies with formula groups in a project.

        Args:
            project_id: Project UUID

        Returns:
            JSON array of hierarchies with formulas
        """
        try:
            groups = service.list_formula_groups(project_id)
            return json.dumps({
                "total": len(groups),
                "formula_groups": groups,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Import/Export Tools
    # =========================================================================

    @mcp.tool()
    def export_hierarchy_csv(project_id: str) -> str:
        """
        Export all hierarchies to CSV format - exports HIERARCHY structure only.

        NOTE: For a complete export, you need TWO files:
        1. This tool exports: {PROJECT_NAME}_HIERARCHY.CSV (structure + sort orders)
        2. Also use export_mapping_csv for: {PROJECT_NAME}_HIERARCHY_MAPPING.CSV (mappings)

        CSV columns include:
        - HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
        - LEVEL_1 through LEVEL_10 (hierarchy level values)
        - LEVEL_1_SORT through LEVEL_10_SORT (sort order for each level)
        - INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG, ACTIVE_FLAG, IS_LEAF_NODE
        - FORMULA_GROUP, SORT_ORDER

        Args:
            project_id: Project UUID

        Returns:
            JSON with CSV content and suggested filename
        """
        try:
            csv_content = service.export_hierarchy_csv(project_id)
            project = service.get_project(project_id)
            project_name = project.get("name", "export").upper().replace(" ", "_")
            filename = f"{project_name}_HIERARCHY.csv"

            return json.dumps({
                "filename": filename,
                "content": csv_content,
                "row_count": len(csv_content.split("\n")) - 1,
                "note": "Also export mappings using export_mapping_csv for complete backup",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def export_mapping_csv(project_id: str) -> str:
        """
        Export all source mappings to CSV format - exports MAPPING data only.

        NOTE: For a complete export, you need TWO files:
        1. Use export_hierarchy_csv for: {PROJECT_NAME}_HIERARCHY.CSV (structure)
        2. This tool exports: {PROJECT_NAME}_HIERARCHY_MAPPING.CSV (mappings)

        Args:
            project_id: Project UUID

        Returns:
            JSON with CSV content and suggested filename
        """
        try:
            csv_content = service.export_mapping_csv(project_id)
            project = service.get_project(project_id)
            project_name = project.get("name", "export").upper().replace(" ", "_")
            filename = f"{project_name}_HIERARCHY_MAPPING.csv"

            return json.dumps({
                "filename": filename,
                "content": csv_content,
                "row_count": len(csv_content.split("\n")) - 1,
                "note": "Also export hierarchy structure using export_hierarchy_csv for complete backup",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def import_hierarchy_csv(project_id: str, csv_content: str, is_legacy_format: bool = False) -> str:
        """
        Import hierarchies from CSV - Step 1 of 2 for full hierarchy import.

        IMPORTANT - BEFORE CALLING THIS TOOL:
        1. Always ask the user: "Is this an older/legacy version CSV format?"
        2. Hierarchy imports require TWO CSV files:
           - Hierarchy structure CSV (filename usually ends with _HIERARCHY.CSV)
           - Mapping CSV (filename usually ends with HIERARCHY_MAPPING.CSV)
        3. Import the hierarchy CSV FIRST, then import the mapping CSV.
        4. Sort orders come from LEVEL_X_SORT columns in the HIERARCHY CSV (not mapping CSV)

        Args:
            project_id: Target project UUID
            csv_content: CSV content as string (the _HIERARCHY.CSV file)
            is_legacy_format: Set to True if user confirms this is an older version CSV

        Expected CSV columns:
            - HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
            - LEVEL_1 through LEVEL_10 (hierarchy level values)
            - LEVEL_1_SORT through LEVEL_10_SORT (sort order for each level)
            - INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG, ACTIVE_FLAG, IS_LEAF_NODE
            - FORMULA_GROUP, SORT_ORDER

        Returns:
            JSON with import statistics (imported, skipped, errors)
        """
        try:
            result = service.import_hierarchy_csv(project_id, csv_content)
            result["next_step"] = "Now import the mapping CSV using import_mapping_csv tool"
            result["expected_file"] = "File ending with HIERARCHY_MAPPING.CSV"
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def import_mapping_csv(project_id: str, csv_content: str) -> str:
        """
        Import source mappings from CSV - Step 2 of 2 for full hierarchy import.

        IMPORTANT - BEFORE CALLING THIS TOOL:
        1. Ensure hierarchies have been imported first using import_hierarchy_csv
        2. The HIERARCHY_ID values in this CSV must match existing hierarchies
        3. Mapping CSV filename usually ends with HIERARCHY_MAPPING.CSV
        4. NOTE: Sort orders come from HIERARCHY CSV (LEVEL_X_SORT columns), NOT this mapping CSV

        Args:
            project_id: Target project UUID
            csv_content: CSV content as string (the HIERARCHY_MAPPING.CSV file)

        Expected CSV columns:
            HIERARCHY_ID, MAPPING_INDEX, SOURCE_DATABASE, SOURCE_SCHEMA,
            SOURCE_TABLE, SOURCE_COLUMN, SOURCE_UID, PRECEDENCE_GROUP,
            INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, ACTIVE_FLAG

        Returns:
            JSON with import statistics (imported, skipped, errors)
        """
        try:
            result = service.import_mapping_csv(project_id, csv_content)
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def export_project_json(project_id: str) -> str:
        """
        Export complete project as JSON backup.

        Args:
            project_id: Project UUID

        Returns:
            JSON with full project backup
        """
        try:
            backup = service.export_project_json(project_id)
            return json.dumps(backup, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Script Generation Tools
    # =========================================================================

    @mcp.tool()
    def generate_hierarchy_scripts(
        project_id: str,
        script_type: str = "all",
        table_name: str = "HIERARCHY_MASTER",
        view_name: str = "V_HIERARCHY_MASTER"
    ) -> str:
        """
        Generate SQL scripts for hierarchy deployment.

        Args:
            project_id: Project UUID
            script_type: "insert", "view", or "all"
            table_name: Target table name
            view_name: Target view name

        Returns:
            JSON with generated SQL scripts
        """
        try:
            scripts = {}

            if script_type in ["insert", "all"]:
                scripts["insert"] = service.generate_insert_script(project_id, table_name)

            if script_type in ["view", "all"]:
                scripts["view"] = service.generate_view_script(project_id, view_name)

            return json.dumps({
                "script_type": script_type,
                "scripts": scripts,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Validation Tools
    # =========================================================================

    @mcp.tool()
    def validate_hierarchy_project(project_id: str) -> str:
        """
        Validate a hierarchy project for issues.

        Checks for:
        - Orphaned hierarchies (invalid parent references)
        - Leaf nodes without source mappings
        - Invalid formula references
        - Circular dependencies

        Args:
            project_id: Project UUID

        Returns:
            JSON with validation results and recommendations
        """
        try:
            result = service.validate_project(project_id)
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Backend Sync Tools
    # =========================================================================

    @mcp.tool()
    def sync_backend_health() -> str:
        """
        Check if the NestJS backend is reachable and auto-sync status.

        Returns:
            JSON with connection status, backend URL, and auto-sync configuration
        """
        if not sync_service:
            return json.dumps({
                "error": "Backend sync not enabled",
                "hint": "Set NESTJS_SYNC_ENABLED=true in config",
            })

        try:
            result = sync_service.health_check()
            sync_status = sync_service.get_sync_status()
            result.update(sync_status)
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def configure_auto_sync(enabled: bool = True) -> str:
        """
        Enable or disable automatic synchronization between MCP and backend.

        When enabled, all write operations (create, update, delete) on projects
        and hierarchies automatically sync to the NestJS backend. This keeps
        the MCP server and Web UI in sync without manual sync_to_backend calls.

        Args:
            enabled: True to enable auto-sync, False to disable

        Returns:
            JSON with new sync configuration status
        """
        if not sync_service:
            return json.dumps({
                "error": "Backend sync not enabled",
                "hint": "Set NESTJS_SYNC_ENABLED=true in config",
            })

        try:
            if enabled:
                sync_service.enable_auto_sync()
            else:
                sync_service.disable_auto_sync()

            status = sync_service.get_sync_status()
            return json.dumps({
                "status": "success",
                "message": f"Auto-sync {'enabled' if enabled else 'disabled'}",
                "sync_config": status,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def sync_to_backend(local_project_id: str, backend_project_id: str = "") -> str:
        """
        Push a local project and its hierarchies to the NestJS backend.

        This syncs MCP local storage -> Web App (MySQL database).

        Args:
            local_project_id: The local MCP project ID
            backend_project_id: Optional backend project ID (creates new if empty)

        Returns:
            JSON with sync statistics
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            result = sync_service.sync_project_to_backend(
                local_service=service,
                local_project_id=local_project_id,
                backend_project_id=backend_project_id if backend_project_id else None,
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def sync_from_backend(backend_project_id: str, local_project_id: str = "") -> str:
        """
        Pull a project and its hierarchies from the NestJS backend to local MCP storage.

        This syncs Web App (MySQL database) -> MCP local storage.

        Args:
            backend_project_id: The backend project ID to sync from
            local_project_id: Optional local project ID (creates new if empty)

        Returns:
            JSON with sync statistics
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            result = sync_service.sync_project_from_backend(
                local_service=service,
                backend_project_id=backend_project_id,
                local_project_id=local_project_id if local_project_id else None,
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_backend_projects() -> str:
        """
        List all projects from the NestJS backend (Web App).

        Returns:
            JSON array of projects from the backend database
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            projects = sync_service.list_projects()
            return json.dumps({
                "source": "backend",
                "total": len(projects),
                "projects": projects,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_backend_hierarchy_tree(backend_project_id: str) -> str:
        """
        Get hierarchy tree from the NestJS backend.

        Args:
            backend_project_id: Backend project ID

        Returns:
            JSON tree structure from the backend
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            tree = sync_service.get_hierarchy_tree(backend_project_id)
            return json.dumps({
                "source": "backend",
                "project_id": backend_project_id,
                "root_count": len(tree),
                "tree": tree,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Enhanced Backend Tools
    # =========================================================================

    @mcp.tool()
    def get_dashboard_stats() -> str:
        """
        Get dashboard statistics from the NestJS backend.

        Returns:
            JSON with statistics including project count, hierarchy count,
            deployment stats, and activity summaries.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            stats = sync_service.get_dashboard_stats()
            return json.dumps({
                "source": "backend",
                "stats": stats,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_recent_activities(limit: int = 10) -> str:
        """
        Get recent activities from the backend dashboard.

        Args:
            limit: Maximum number of activities to return (default: 10)

        Returns:
            JSON array of recent activity entries with timestamps and details.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            activities = sync_service.get_dashboard_activities(limit=limit)
            return json.dumps({
                "source": "backend",
                "total": len(activities),
                "activities": activities,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def search_hierarchies_backend(project_id: str, query: str) -> str:
        """
        Search hierarchies within a project via the backend.

        Args:
            project_id: Project UUID
            query: Search query string

        Returns:
            JSON array of matching hierarchies with relevance scores.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            results = sync_service.search_hierarchies(project_id, query)
            return json.dumps({
                "source": "backend",
                "project_id": project_id,
                "query": query,
                "total": len(results),
                "results": results,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def generate_deployment_scripts(
        project_id: str,
        table_name: str = "HIERARCHY_MASTER",
        view_name: str = "V_HIERARCHY_MASTER",
        include_insert: bool = True,
        include_view: bool = True
    ) -> str:
        """
        Generate SQL deployment scripts for a hierarchy project via the backend.

        Args:
            project_id: Project UUID
            table_name: Target table name for INSERT statements
            view_name: Target view name for VIEW creation
            include_insert: Whether to include INSERT script
            include_view: Whether to include VIEW script

        Returns:
            JSON with generated SQL scripts ready for deployment.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            config = {
                "tableName": table_name,
                "viewName": view_name,
                "includeInsert": include_insert,
                "includeView": include_view,
            }
            result = sync_service.generate_deployment_scripts(project_id, config)
            return json.dumps({
                "source": "backend",
                "project_id": project_id,
                "config": config,
                "scripts": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def push_hierarchy_to_snowflake(
        project_id: str,
        connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str
    ) -> str:
        """
        Deploy a hierarchy project to Snowflake.

        Args:
            project_id: Project UUID to deploy
            connection_id: Snowflake connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name

        Returns:
            JSON with deployment result including row counts and any errors.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            dto = {
                "projectId": project_id,
                "connectionId": connection_id,
                "target": {
                    "database": target_database,
                    "schema": target_schema,
                    "table": target_table,
                }
            }
            result = sync_service.push_to_snowflake(dto)
            return json.dumps({
                "source": "backend",
                "operation": "snowflake_deployment",
                "project_id": project_id,
                "target": f"{target_database}.{target_schema}.{target_table}",
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_deployment_history(project_id: str, limit: int = 50) -> str:
        """
        Get deployment history for a project.

        Args:
            project_id: Project UUID
            limit: Maximum number of entries to return (default: 50)

        Returns:
            JSON array of deployment history entries with timestamps and status.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            history = sync_service.get_deployment_history(project_id, limit=limit)
            return json.dumps({
                "source": "backend",
                "project_id": project_id,
                "total": len(history),
                "history": history,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def export_hierarchy_csv_backend(project_id: str) -> str:
        """
        Export hierarchy to CSV via the NestJS backend.

        This uses the backend's export functionality which may have
        different formatting than the local export.

        Args:
            project_id: Project UUID

        Returns:
            JSON with CSV content and suggested filename.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            csv_content = sync_service.export_hierarchy_csv_backend(project_id)
            filename = f"HIERARCHY_{project_id}_{datetime.now().strftime('%Y%m%d')}.csv"
            return json.dumps({
                "source": "backend",
                "project_id": project_id,
                "filename": filename,
                "content": csv_content,
                "row_count": len(csv_content.split("\n")) - 1 if csv_content else 0,
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def import_hierarchy_csv_backend(project_id: str, csv_content: str) -> str:
        """
        Import hierarchy from CSV via the NestJS backend.

        This uses the backend's import functionality which handles
        validation and database insertion.

        Args:
            project_id: Target project UUID
            csv_content: CSV content as string

        Returns:
            JSON with import statistics (imported, skipped, errors).
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            result = sync_service.import_hierarchy_csv_backend(project_id, csv_content)
            return json.dumps({
                "source": "backend",
                "project_id": project_id,
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def create_filter_group_backend(
        project_id: str,
        group_name: str,
        filters: str
    ) -> str:
        """
        Create a filter group via the NestJS backend.

        Filter groups allow you to define reusable filter criteria
        for hierarchy views and reports.

        Args:
            project_id: Project UUID
            group_name: Name for the filter group
            filters: JSON string of filter definitions

        Returns:
            JSON with created filter group details.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            filters_list = json.loads(filters)
            body = {
                "projectId": project_id,
                "groupName": group_name,
                "filters": filters_list,
            }
            result = sync_service.create_filter_group(body)
            return json.dumps({
                "source": "backend",
                "operation": "create_filter_group",
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_filter_groups_backend(project_id: str) -> str:
        """
        List all filter groups for a project via the backend.

        Args:
            project_id: Project UUID

        Returns:
            JSON array of filter groups with their configurations.
        """
        if not sync_service:
            return json.dumps({"error": "Backend sync not enabled"})

        try:
            groups = sync_service.list_filter_groups(project_id)
            return json.dumps({
                "source": "backend",
                "project_id": project_id,
                "total": len(groups),
                "filter_groups": groups,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return service  # Return service for potential direct use
