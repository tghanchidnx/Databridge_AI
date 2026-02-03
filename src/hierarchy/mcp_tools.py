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

    # =========================================================================
    # Flexible Import Tools (Tiered Hierarchy Creation)
    # =========================================================================

    # Import the flexible import service
    try:
        from .flexible_import import FlexibleImportService, FormatDetector, FormatTier
        flexible_import_service = FlexibleImportService(service)
        flexible_import_enabled = True
    except ImportError as e:
        logger.warning(f"Flexible import not available: {e}")
        flexible_import_enabled = False

    @mcp.tool()
    def detect_hierarchy_format(content: str, filename: str = "") -> str:
        """
        Detect the format and tier of hierarchy input data.

        Analyzes input content to determine:
        - Input format (CSV, Excel, JSON, text)
        - Complexity tier (tier_1 to tier_4)
        - Parent relationship strategy
        - Recommendations for import

        Tiers:
        - Tier 1: Ultra-simple (2-3 columns: source_value, group_name)
        - Tier 2: Basic (5-7 columns with parent names)
        - Tier 3: Standard (10-12 columns with explicit IDs)
        - Tier 4: Enterprise (28+ columns with LEVEL_X)

        Args:
            content: Input data content (CSV, JSON, or plain text)
            filename: Optional filename to help detect format from extension

        Returns:
            JSON with detected format, tier, columns, parent strategy,
            sample data, and recommendations.

        Example:
            detect_hierarchy_format("source_value,group_name\\n4100,Revenue\\n5100,COGS")
            -> {"format": "csv", "tier": "tier_1", "columns_found": ["source_value", "group_name"], ...}
        """
        if not flexible_import_enabled:
            return json.dumps({"error": "Flexible import module not available"})

        try:
            analysis = FormatDetector.analyze(content, filename)
            return json.dumps({
                "status": "success",
                **analysis,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def configure_project_defaults(
        project_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_column: str
    ) -> str:
        """
        Configure default source information for a project.

        These defaults are used during flexible import when source columns
        are not specified in the input data. Essential for Tier 1 and Tier 2
        imports where source info is not included.

        Args:
            project_id: Project UUID
            source_database: Default database name (e.g., "WAREHOUSE")
            source_schema: Default schema name (e.g., "FINANCE")
            source_table: Default table name (e.g., "DIM_ACCOUNT")
            source_column: Default column name (e.g., "ACCOUNT_CODE")

        Returns:
            JSON with configured defaults and completeness status.

        Example:
            configure_project_defaults(
                project_id="abc-123",
                source_database="WAREHOUSE",
                source_schema="FINANCE",
                source_table="DIM_ACCOUNT",
                source_column="ACCOUNT_CODE"
            )
        """
        if not flexible_import_enabled:
            return json.dumps({"error": "Flexible import module not available"})

        try:
            # Verify project exists
            project = service.get_project(project_id)
            if not project:
                return json.dumps({"error": f"Project '{project_id}' not found"})

            defaults = flexible_import_service.configure_defaults(
                project_id=project_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                source_column=source_column,
            )

            return json.dumps({
                "status": "success",
                "project_id": project_id,
                "project_name": project.get("name"),
                "defaults": defaults.to_dict(),
                "is_complete": defaults.is_complete(),
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def preview_import(
        content: str,
        format_type: str = "auto",
        source_defaults: str = "{}",
        limit: int = 10
    ) -> str:
        """
        Preview hierarchy import without creating anything.

        Shows what hierarchies and mappings would be created from the input
        without actually persisting them. Use this to verify data before
        committing to import.

        Args:
            content: Input data (CSV, JSON, or text)
            format_type: Format hint ("auto", "csv", "json", "excel", "text")
            source_defaults: JSON string of source defaults:
                {"database": "X", "schema": "Y", "table": "Z", "column": "W"}
            limit: Maximum rows to preview (default 10)

        Returns:
            JSON with detected format/tier, preview of hierarchies,
            inferred fields, and source defaults status.

        Example:
            preview_import(
                content="source_value,group_name\\n4100,Revenue",
                source_defaults='{"database":"WAREHOUSE","schema":"FINANCE"}'
            )
        """
        if not flexible_import_enabled:
            return json.dumps({"error": "Flexible import module not available"})

        try:
            defaults_dict = json.loads(source_defaults) if source_defaults else {}

            # Normalize keys
            normalized_defaults = {
                "source_database": defaults_dict.get("source_database") or defaults_dict.get("database", ""),
                "source_schema": defaults_dict.get("source_schema") or defaults_dict.get("schema", ""),
                "source_table": defaults_dict.get("source_table") or defaults_dict.get("table", ""),
                "source_column": defaults_dict.get("source_column") or defaults_dict.get("column", ""),
            }

            preview = flexible_import_service.preview_import(
                content=content,
                format_type=format_type,
                source_defaults=normalized_defaults,
                limit=limit,
            )

            return json.dumps({
                "status": "success",
                **preview,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def import_flexible_hierarchy(
        project_id: str,
        content: str,
        format_type: str = "auto",
        source_defaults: str = "{}",
        tier_hint: str = "auto"
    ) -> str:
        """
        Import hierarchies from flexible format with auto-detection.

        Supports four tiers of input complexity:
        - Tier 1: Ultra-simple (source_value, group_name)
        - Tier 2: Basic (hierarchy_name, parent_name, source_value, sort_order)
        - Tier 3: Standard (explicit IDs, full source info)
        - Tier 4: Enterprise (LEVEL_1-10, all flags, formulas)

        Auto-infers missing fields based on tier and project defaults.

        AUTO-SYNC: When enabled, created hierarchies sync to backend.

        Args:
            project_id: Target project UUID
            content: Input data (CSV, JSON, or text)
            format_type: Format hint ("auto", "csv", "json", "excel", "text")
            source_defaults: JSON string of source defaults (overrides project defaults)
            tier_hint: Tier hint ("auto", "tier_1", "tier_2", "tier_3", "tier_4")

        Returns:
            JSON with import results including:
            - detected_format, detected_tier
            - hierarchies_created, mappings_created
            - created_hierarchies (list with IDs and names)
            - inferred_fields (what was auto-generated)
            - errors (if any)

        Example:
            # Tier 1 import
            import_flexible_hierarchy(
                project_id="abc-123",
                content="source_value,group_name\\n4100,Revenue\\n4200,Revenue\\n5100,COGS",
                source_defaults='{"database":"WAREHOUSE","schema":"FINANCE","table":"DIM_ACCOUNT","column":"ACCOUNT_CODE"}'
            )

            # Tier 2 import
            import_flexible_hierarchy(
                project_id="abc-123",
                content="hierarchy_name,parent_name,source_value\\nRevenue,,4%\\nProduct Rev,Revenue,41%"
            )
        """
        if not flexible_import_enabled:
            return json.dumps({"error": "Flexible import module not available"})

        try:
            defaults_dict = json.loads(source_defaults) if source_defaults else {}

            # Normalize keys
            normalized_defaults = None
            if defaults_dict:
                normalized_defaults = {
                    "source_database": defaults_dict.get("source_database") or defaults_dict.get("database", ""),
                    "source_schema": defaults_dict.get("source_schema") or defaults_dict.get("schema", ""),
                    "source_table": defaults_dict.get("source_table") or defaults_dict.get("table", ""),
                    "source_column": defaults_dict.get("source_column") or defaults_dict.get("column", ""),
                }

            result = flexible_import_service.import_flexible(
                project_id=project_id,
                content=content,
                format_type=format_type,
                source_defaults=normalized_defaults,
                tier_hint=tier_hint,
            )

            if "error" in result:
                return json.dumps({"error": result["error"]})

            # Auto-sync created hierarchies
            if result.get("created_hierarchies"):
                for h in result["created_hierarchies"]:
                    _auto_sync_operation(
                        operation="create_hierarchy",
                        project_id=project_id,
                        hierarchy_id=h.get("hierarchy_id"),
                        data=h,
                    )

            return json.dumps({
                "status": result.get("status", "success"),
                **result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def export_hierarchy_simplified(
        project_id: str,
        target_tier: str = "tier_2"
    ) -> str:
        """
        Export project hierarchies in simplified format.

        Converts hierarchies to a simpler tier format for:
        - Sharing with non-technical users (Tier 1)
        - Easy editing and re-import (Tier 2)
        - Standard format with explicit IDs (Tier 3)

        Args:
            project_id: Project UUID
            target_tier: Target format ("tier_1", "tier_2", "tier_3")
                - tier_1: source_value, group_name (mappings only)
                - tier_2: hierarchy_name, parent_name, source_value, sort_order
                - tier_3: Standard with hierarchy_id, parent_id, flags

        Returns:
            JSON with:
            - format: Target tier
            - csv_content: Exported CSV data
            - row_count: Number of data rows
            - note: Usage guidance

        Example:
            export_hierarchy_simplified(project_id="abc-123", target_tier="tier_2")
            -> CSV with hierarchy_name, parent_name, source_value, sort_order
        """
        if not flexible_import_enabled:
            return json.dumps({"error": "Flexible import module not available"})

        try:
            # Verify project exists
            project = service.get_project(project_id)
            if not project:
                return json.dumps({"error": f"Project '{project_id}' not found"})

            result = flexible_import_service.export_simplified(
                project_id=project_id,
                target_tier=target_tier,
            )

            if "error" in result:
                return json.dumps({"error": result["error"]})

            return json.dumps({
                "status": "success",
                "project_id": project_id,
                "project_name": project.get("name"),
                **result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_project_defaults(project_id: str) -> str:
        """
        Get configured source defaults for a project.

        Returns the default source information that will be used
        for Tier 1 and Tier 2 imports when source columns are not
        specified in the input data.

        Args:
            project_id: Project UUID

        Returns:
            JSON with defaults and completeness status.
        """
        if not flexible_import_enabled:
            return json.dumps({"error": "Flexible import module not available"})

        try:
            project = service.get_project(project_id)
            if not project:
                return json.dumps({"error": f"Project '{project_id}' not found"})

            defaults = flexible_import_service.get_defaults(project_id)

            if defaults:
                return json.dumps({
                    "status": "success",
                    "project_id": project_id,
                    "project_name": project.get("name"),
                    "defaults": defaults.to_dict(),
                    "is_complete": defaults.is_complete(),
                }, default=str, indent=2)
            else:
                return json.dumps({
                    "status": "success",
                    "project_id": project_id,
                    "project_name": project.get("name"),
                    "defaults": None,
                    "is_complete": False,
                    "message": "No defaults configured. Use configure_project_defaults to set them.",
                }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Property Management Tools
    # =========================================================================

    @mcp.tool()
    def add_hierarchy_property(
        project_id: str,
        hierarchy_id: str,
        name: str,
        value: str,
        category: str = "custom",
        level: str = "",
        inherit: str = "true",
        override_allowed: str = "true",
        description: str = ""
    ) -> str:
        """
        Add a property to a hierarchy node.

        Properties control how dimensions are built, facts are designed,
        and filters are configured. Properties can be inherited by children.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID (slug)
            name: Property name (e.g., 'aggregation_type', 'measure_type', 'color')
            value: Property value (JSON string for complex values)
            category: Property category:
                - dimension: Controls dimension building
                - fact: Controls fact/measure design
                - filter: Controls filter behavior
                - display: Controls UI display
                - validation: Data validation rules
                - security: Row-level security
                - custom: User-defined
            level: Specific level this applies to (empty = hierarchy level, number = specific LEVEL_X)
            inherit: Whether children inherit this property ("true"/"false")
            override_allowed: Whether children can override ("true"/"false")
            description: Property description

        Returns:
            JSON with updated hierarchy and property details.

        Examples:
            # Add aggregation type
            add_hierarchy_property(project_id, "REVENUE_1", "aggregation_type", "SUM", "dimension")

            # Add measure type
            add_hierarchy_property(project_id, "NET_INCOME", "measure_type", "derived", "fact")

            # Add display color
            add_hierarchy_property(project_id, "REVENUE_1", "color", "#22c55e", "display")

            # Add custom property
            add_hierarchy_property(project_id, "WELL_1", "regulatory_reporting", "true", "custom")
        """
        try:
            # Parse value - try JSON first
            try:
                parsed_value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # Keep as string if not valid JSON
                parsed_value = value

            # Parse level
            parsed_level = int(level) if level and level.isdigit() else None

            result = service.add_property(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                name=name,
                value=parsed_value,
                category=category,
                level=parsed_level,
                inherit=inherit.lower() == "true",
                override_allowed=override_allowed.lower() == "true",
                description=description,
            )

            if not result:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            # Auto-sync
            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"property_added": name},
            )

            return json.dumps({
                "status": "success",
                "property": {
                    "name": name,
                    "value": parsed_value,
                    "category": category,
                    "level": parsed_level,
                    "inherit": inherit.lower() == "true",
                },
                "hierarchy_id": hierarchy_id,
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def remove_hierarchy_property(
        project_id: str,
        hierarchy_id: str,
        name: str,
        level: str = ""
    ) -> str:
        """
        Remove a property from a hierarchy.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            name: Property name to remove
            level: Level of the property (empty = hierarchy level)

        Returns:
            JSON with status.
        """
        try:
            parsed_level = int(level) if level and level.isdigit() else None

            result = service.remove_property(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                name=name,
                level=parsed_level,
            )

            if not result:
                return json.dumps({"error": f"Property '{name}' not found on '{hierarchy_id}'"})

            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"property_removed": name},
            )

            return json.dumps({
                "status": "success",
                "message": f"Property '{name}' removed",
                "hierarchy_id": hierarchy_id,
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_hierarchy_properties(
        project_id: str,
        hierarchy_id: str,
        category: str = "",
        include_inherited: str = "true"
    ) -> str:
        """
        Get properties for a hierarchy, optionally including inherited properties.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID (slug) or UUID
            category: Filter by category (empty = all categories)
            include_inherited: Include properties inherited from ancestors ("true"/"false")

        Returns:
            JSON with:
            - own_properties: Properties defined on this hierarchy
            - inherited_properties: Properties from ancestors (if include_inherited)
            - effective_properties: Final resolved properties
            - dimension_props, fact_props, filter_props, display_props: Type-specific props

        Property Categories:
            - dimension: aggregation_type, drill_enabled, sort_behavior, etc.
            - fact: measure_type, time_balance, format_string, etc.
            - filter: filter_behavior, default_value, cascading_parent_id, etc.
            - display: color, icon, tooltip, visible, etc.
            - custom: user-defined properties
        """
        try:
            # Try to get by hierarchy_id first, then by UUID
            hierarchy = service.get_hierarchy(project_id, hierarchy_id)
            if not hierarchy:
                hierarchy = service.get_hierarchy_by_id(hierarchy_id)

            if not hierarchy:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            hierarchy_uuid = hierarchy.get("id")

            if include_inherited.lower() == "true":
                result = service.get_inherited_properties(project_id, hierarchy_uuid)
                if "error" in result:
                    return json.dumps({"error": result["error"]})
            else:
                own_props = service.get_properties(
                    project_id,
                    hierarchy.get("hierarchy_id"),
                    category=category if category else None,
                )
                result = {
                    "hierarchy_id": hierarchy.get("hierarchy_id"),
                    "hierarchy_name": hierarchy.get("hierarchy_name"),
                    "own_properties": own_props,
                    "effective_properties": own_props,
                    "inherited_properties": [],
                }

            # Filter by category if specified
            if category:
                result["effective_properties"] = [
                    p for p in result.get("effective_properties", [])
                    if p.get("category") == category
                ]

            # Add type-specific props
            result["dimension_props"] = hierarchy.get("dimension_props")
            result["fact_props"] = hierarchy.get("fact_props")
            result["filter_props"] = hierarchy.get("filter_props")
            result["display_props"] = hierarchy.get("display_props")
            result["property_template_id"] = hierarchy.get("property_template_id")

            return json.dumps({
                "status": "success",
                **result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def set_dimension_properties(
        project_id: str,
        hierarchy_id: str,
        props: str
    ) -> str:
        """
        Set dimension properties for a hierarchy.

        Dimension properties control how the hierarchy behaves as a dimension
        in reports and analytics.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of dimension properties:
                {
                    "aggregation_type": "SUM",      // SUM, AVG, COUNT, MIN, MAX, NONE
                    "display_format": null,         // Format string
                    "sort_behavior": "alpha",       // alpha, numeric, custom, natural
                    "drill_enabled": true,          // Allow drill-down
                    "drill_path": null,             // Custom drill path hierarchy IDs
                    "grouping_enabled": true,       // Allow grouping in reports
                    "totals_enabled": true,         // Show totals
                    "hierarchy_type": "standard",   // standard, ragged, parent-child, time
                    "all_member_name": "All",       // Name for 'All' member
                    "default_member": null          // Default member ID
                }

        Returns:
            JSON with updated hierarchy.

        Example:
            set_dimension_properties(project_id, "ACCOUNT", '{"aggregation_type": "SUM", "drill_enabled": true}')
        """
        try:
            props_dict = json.loads(props)

            result = service.set_dimension_props(project_id, hierarchy_id, props_dict)

            if not result:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"dimension_props_updated": True},
            )

            return json.dumps({
                "status": "success",
                "hierarchy_id": hierarchy_id,
                "dimension_props": props_dict,
                "sync": sync_result,
            }, default=str, indent=2)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def set_fact_properties(
        project_id: str,
        hierarchy_id: str,
        props: str
    ) -> str:
        """
        Set fact/measure properties for a hierarchy.

        Fact properties control how the hierarchy behaves as a measure
        in reports and analytics.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of fact properties:
                {
                    "measure_type": "additive",     // additive, semi_additive, non_additive, derived
                    "aggregation_type": "SUM",      // SUM, AVG, COUNT, etc.
                    "time_balance": null,           // flow, first, last, average (for semi-additive)
                    "format_string": "#,##0.00",    // Number format
                    "decimal_places": 2,            // Decimal places
                    "currency_code": "USD",         // Currency code
                    "unit_of_measure": null,        // Unit (bbl, mcf, units)
                    "null_handling": "zero",        // zero, null, exclude
                    "negative_format": "minus",     // minus, parens, red
                    "calculation_formula": null,    // Formula for derived measures
                    "base_measure_ids": null        // IDs of measures used in calculation
                }

        Returns:
            JSON with updated hierarchy.

        Examples:
            # Additive measure (revenue, expenses)
            set_fact_properties(project_id, "REVENUE", '{"measure_type": "additive", "aggregation_type": "SUM"}')

            # Semi-additive balance (uses last value for time)
            set_fact_properties(project_id, "BALANCE", '{"measure_type": "semi_additive", "time_balance": "last"}')

            # Derived ratio
            set_fact_properties(project_id, "MARGIN_PCT", '{"measure_type": "non_additive", "format_string": "0.00%"}')
        """
        try:
            props_dict = json.loads(props)

            result = service.set_fact_props(project_id, hierarchy_id, props_dict)

            if not result:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"fact_props_updated": True},
            )

            return json.dumps({
                "status": "success",
                "hierarchy_id": hierarchy_id,
                "fact_props": props_dict,
                "sync": sync_result,
            }, default=str, indent=2)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def set_filter_properties(
        project_id: str,
        hierarchy_id: str,
        props: str
    ) -> str:
        """
        Set filter properties for a hierarchy.

        Filter properties control how the hierarchy behaves as a filter
        in reports and dashboards.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of filter properties:
                {
                    "filter_behavior": "multi",     // single, multi, range, cascading, search, hierarchy
                    "default_value": null,          // Default filter value
                    "default_to_all": true,         // Default to all values
                    "allowed_values": null,         // Restrict to these values
                    "excluded_values": null,        // Exclude these values
                    "cascading_parent_id": null,    // Parent filter hierarchy ID
                    "required": false,              // Selection required
                    "visible": true,                // Show in filter panel
                    "search_enabled": true,         // Enable search
                    "show_all_option": true,        // Show 'All' option
                    "max_selections": null          // Max selections for multi-select
                }

        Returns:
            JSON with updated hierarchy.

        Examples:
            # Multi-select with search
            set_filter_properties(project_id, "ACCOUNT", '{"filter_behavior": "multi", "search_enabled": true}')

            # Cascading filter (depends on parent)
            set_filter_properties(project_id, "WELL", '{"filter_behavior": "cascading", "cascading_parent_id": "FIELD_1"}')

            # Required single-select
            set_filter_properties(project_id, "PERIOD", '{"filter_behavior": "single", "required": true}')
        """
        try:
            props_dict = json.loads(props)

            result = service.set_filter_props(project_id, hierarchy_id, props_dict)

            if not result:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"filter_props_updated": True},
            )

            return json.dumps({
                "status": "success",
                "hierarchy_id": hierarchy_id,
                "filter_props": props_dict,
                "sync": sync_result,
            }, default=str, indent=2)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def set_display_properties(
        project_id: str,
        hierarchy_id: str,
        props: str
    ) -> str:
        """
        Set display properties for a hierarchy.

        Display properties control how the hierarchy appears in the UI.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of display properties:
                {
                    "color": "#22c55e",             // Display color
                    "background_color": null,       // Background color
                    "icon": null,                   // Icon name or emoji
                    "tooltip": null,                // Hover tooltip
                    "visible": true,                // Visible in UI
                    "collapsed_by_default": false,  // Start collapsed
                    "highlight_condition": null,    // Condition for highlighting
                    "custom_css_class": null,       // Custom CSS class
                    "display_order": null           // Override display order
                }

        Returns:
            JSON with updated hierarchy.

        Examples:
            set_display_properties(project_id, "REVENUE", '{"color": "#22c55e", "icon": "dollar"}')
            set_display_properties(project_id, "EXPENSES", '{"color": "#ef4444", "collapsed_by_default": true}')
        """
        try:
            props_dict = json.loads(props)

            result = service.set_display_props(project_id, hierarchy_id, props_dict)

            if not result:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"display_props_updated": True},
            )

            return json.dumps({
                "status": "success",
                "hierarchy_id": hierarchy_id,
                "display_props": props_dict,
                "sync": sync_result,
            }, default=str, indent=2)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_property_templates() -> str:
        """
        List available property templates.

        Property templates provide pre-configured property sets for common
        use cases like financial dimensions, time dimensions, measures, etc.

        Returns:
            JSON array of templates with:
            - id: Template ID
            - name: Template name
            - description: Description
            - category: Primary category (dimension, fact, filter)
            - tags: Tags for searching

        Available Templates:
            - financial_dimension: Standard financial reporting dimensions
            - time_dimension: Time/date dimensions with period handling
            - additive_measure: Standard summable measures
            - balance_measure: Semi-additive balance measures
            - ratio_measure: Non-additive ratios/percentages
            - currency_measure: Monetary measures
            - cascading_filter: Dependent filters
            - required_filter: Required single-select filters
            - oil_gas_dimension: Oil & gas operational hierarchies
            - volume_measure: Volume measures with units
        """
        try:
            templates = service.get_property_templates()

            return json.dumps({
                "status": "success",
                "total": len(templates),
                "templates": [
                    {
                        "id": t["id"],
                        "name": t["name"],
                        "description": t.get("description"),
                        "category": t.get("category"),
                        "tags": t.get("tags", []),
                    }
                    for t in templates
                ],
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_property_template(template_id: str) -> str:
        """
        Get detailed information about a property template.

        Args:
            template_id: Template ID (e.g., "financial_dimension", "additive_measure")

        Returns:
            JSON with full template details including all properties.
        """
        try:
            templates = service.get_property_templates()
            template = next((t for t in templates if t["id"] == template_id), None)

            if not template:
                return json.dumps({"error": f"Template '{template_id}' not found"})

            return json.dumps({
                "status": "success",
                "template": template,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def apply_property_template(
        project_id: str,
        hierarchy_id: str,
        template_id: str,
        merge: str = "true"
    ) -> str:
        """
        Apply a property template to a hierarchy.

        Templates provide pre-configured property sets for common use cases.
        Use list_property_templates to see available templates.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            template_id: Template ID to apply
            merge: If "true", merge with existing properties. If "false", replace.

        Returns:
            JSON with updated hierarchy and applied template info.

        Examples:
            # Apply financial dimension template
            apply_property_template(project_id, "ACCOUNT", "financial_dimension")

            # Apply measure template
            apply_property_template(project_id, "REVENUE", "additive_measure")

            # Apply time dimension (replace existing)
            apply_property_template(project_id, "PERIOD", "time_dimension", "false")
        """
        try:
            result = service.apply_property_template(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                template_id=template_id,
                merge=merge.lower() == "true",
            )

            if not result:
                return json.dumps({"error": f"Hierarchy '{hierarchy_id}' not found"})

            if "error" in result:
                return json.dumps({"error": result["error"]})

            sync_result = _auto_sync_operation(
                operation="update_hierarchy",
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                data={"template_applied": template_id},
            )

            return json.dumps({
                "status": "success",
                "hierarchy_id": hierarchy_id,
                "template_id": template_id,
                "merge_mode": merge.lower() == "true",
                "sync": sync_result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def bulk_set_property(
        project_id: str,
        hierarchy_ids: str,
        name: str,
        value: str,
        category: str = "custom",
        inherit: str = "true"
    ) -> str:
        """
        Set a property on multiple hierarchies at once.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_ids: JSON array of hierarchy IDs (e.g., '["REVENUE_1", "COGS_1"]')
            name: Property name
            value: Property value
            category: Property category
            inherit: Whether children inherit

        Returns:
            JSON with success count and any errors.

        Example:
            bulk_set_property(
                project_id,
                '["REVENUE_1", "COGS_1", "EXPENSES_1"]',
                "aggregation_type",
                "SUM",
                "dimension"
            )
        """
        try:
            ids_list = json.loads(hierarchy_ids)

            # Parse value
            try:
                parsed_value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                parsed_value = value

            result = service.bulk_set_property(
                project_id=project_id,
                hierarchy_ids=ids_list,
                name=name,
                value=parsed_value,
                category=category,
                inherit=inherit.lower() == "true",
            )

            return json.dumps({
                "status": "success" if result["error_count"] == 0 else "partial",
                **result,
            }, default=str, indent=2)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON for hierarchy_ids: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_properties_summary(project_id: str) -> str:
        """
        Get a summary of all properties used across a project.

        Returns:
            JSON with:
            - total_hierarchies: Total hierarchy count
            - hierarchies_with_properties: Count with properties
            - total_properties: Total property count
            - by_category: Property counts by category
            - by_name: Property usage by name with unique values
        """
        try:
            project = service.get_project(project_id)
            if not project:
                return json.dumps({"error": f"Project '{project_id}' not found"})

            result = service.get_properties_summary(project_id)

            return json.dumps({
                "status": "success",
                "project_name": project.get("name"),
                **result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return service  # Return service for potential direct use
