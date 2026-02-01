"""
MCP tools for hierarchy management.

Provides 15 tools for hierarchy CRUD and navigation:
- create_hierarchy
- get_hierarchy
- update_hierarchy
- delete_hierarchy
- get_hierarchy_tree
- move_hierarchy_node
- get_hierarchy_children
- add_source_mapping
- remove_source_mapping
- get_mappings_by_precedence
- create_formula_group
- add_formula_rule
- list_formula_groups
- export_hierarchy_csv
- import_hierarchy_csv
"""

from typing import Optional, List, Dict, Any
from fastmcp import FastMCP

from ...hierarchy.service import (
    HierarchyService,
    DuplicateError,
    ProjectNotFoundError,
    HierarchyNotFoundError,
    HierarchyServiceError,
)
from ...hierarchy.tree import TreeBuilder, TreeNavigator
from ...hierarchy.csv_handler import CSVHandler


def register_hierarchy_tools(mcp: FastMCP) -> None:
    """Register all hierarchy tools with the MCP server."""

    @mcp.tool()
    def create_hierarchy(
        project_id: str,
        hierarchy_id: str,
        hierarchy_name: str,
        parent_id: Optional[str] = None,
        description: Optional[str] = None,
        levels: Optional[Dict[str, str]] = None,
        level_sorts: Optional[Dict[str, int]] = None,
        sort_order: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a new hierarchy node.

        Args:
            project_id: Parent project ID.
            hierarchy_id: Unique identifier for the hierarchy (e.g., "REV-001").
            hierarchy_name: Display name for the hierarchy.
            parent_id: Parent hierarchy ID (optional, None for root nodes).
            description: Optional description.
            levels: Dictionary of level values (e.g., {"level_1": "Revenue", "level_2": "Product"}).
            level_sorts: Dictionary of level sort orders (e.g., {"level_1": 1, "level_2": 2}).
            sort_order: Overall sort order within siblings.

        Returns:
            Dictionary with created hierarchy details.
        """
        service = HierarchyService()
        try:
            hierarchy = service.create_hierarchy(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                hierarchy_name=hierarchy_name,
                parent_id=parent_id,
                description=description,
                levels=levels,
                level_sorts=level_sorts,
                sort_order=sort_order,
            )
            return {
                "success": True,
                "hierarchy": {
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "parent_id": hierarchy.parent_id,
                    "description": hierarchy.description,
                    "level_1": hierarchy.level_1,
                    "level_2": hierarchy.level_2,
                    "sort_order": hierarchy.sort_order,
                },
            }
        except (ProjectNotFoundError, DuplicateError) as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_hierarchy(hierarchy_id: str) -> Dict[str, Any]:
        """
        Get a hierarchy by ID.

        Args:
            hierarchy_id: Hierarchy ID to retrieve.

        Returns:
            Dictionary with full hierarchy details.
        """
        service = HierarchyService()
        try:
            hierarchy = service.get_hierarchy(hierarchy_id)
            return {
                "success": True,
                "hierarchy": {
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "parent_id": hierarchy.parent_id,
                    "description": hierarchy.description,
                    "project_id": hierarchy.project_id,
                    "levels": {
                        f"level_{i}": getattr(hierarchy, f"level_{i}")
                        for i in range(1, 16)
                        if getattr(hierarchy, f"level_{i}")
                    },
                    "sort_order": hierarchy.sort_order,
                    "include_flag": hierarchy.include_flag,
                    "is_current": hierarchy.is_current,
                    "version_number": hierarchy.version_number,
                },
            }
        except HierarchyNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def update_hierarchy(
        hierarchy_id: str,
        hierarchy_name: Optional[str] = None,
        description: Optional[str] = None,
        levels: Optional[Dict[str, str]] = None,
        level_sorts: Optional[Dict[str, int]] = None,
        sort_order: Optional[int] = None,
        create_version: bool = False,
    ) -> Dict[str, Any]:
        """
        Update a hierarchy.

        Args:
            hierarchy_id: Hierarchy ID to update.
            hierarchy_name: New display name (optional).
            description: New description (optional).
            levels: Dictionary of level values to update.
            level_sorts: Dictionary of level sort orders to update.
            sort_order: New sort order (optional).
            create_version: If True, creates a new version (SCD Type 2) instead of updating in place.

        Returns:
            Dictionary with updated hierarchy details.
        """
        service = HierarchyService()
        try:
            hierarchy = service.update_hierarchy(
                hierarchy_id=hierarchy_id,
                hierarchy_name=hierarchy_name,
                description=description,
                levels=levels,
                level_sorts=level_sorts,
                sort_order=sort_order,
                create_version=create_version,
            )
            return {
                "success": True,
                "hierarchy": {
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "description": hierarchy.description,
                    "version_number": hierarchy.version_number,
                },
            }
        except HierarchyNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def delete_hierarchy(
        hierarchy_id: str,
        cascade: bool = False,
        soft_delete: bool = True,
    ) -> Dict[str, Any]:
        """
        Delete a hierarchy.

        Args:
            hierarchy_id: Hierarchy ID to delete.
            cascade: If True, delete all children and mappings.
            soft_delete: If True (default), mark as inactive. If False, hard delete.

        Returns:
            Dictionary with deletion result.
        """
        service = HierarchyService()
        try:
            service.delete_hierarchy(
                hierarchy_id=hierarchy_id,
                cascade=cascade,
                soft_delete=soft_delete,
            )
            return {"success": True, "message": f"Hierarchy {hierarchy_id} deleted"}
        except (HierarchyNotFoundError, HierarchyServiceError) as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_hierarchy_tree(
        project_id: str,
        root_id: Optional[str] = None,
        max_depth: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get the hierarchy tree structure.

        Args:
            project_id: Project ID.
            root_id: Optional root hierarchy ID to start from.
            max_depth: Maximum depth to traverse (None for unlimited).

        Returns:
            Dictionary with nested tree structure.
        """
        service = HierarchyService()
        try:
            hierarchies = service.list_hierarchies(project_id=project_id, limit=10000)

            items = [
                {
                    "hierarchy_id": h.hierarchy_id,
                    "hierarchy_name": h.hierarchy_name,
                    "parent_id": h.parent_id,
                    "sort_order": h.sort_order,
                }
                for h in hierarchies
            ]

            builder = TreeBuilder()
            roots = builder.build(items)

            if root_id:
                nav = TreeNavigator(roots)
                root_node = nav.get_node(root_id)
                if root_node:
                    roots = [root_node]
                else:
                    return {"success": False, "error": f"Root not found: {root_id}"}

            nav = TreeNavigator(roots)
            nested = nav.to_nested_dict()

            return {
                "success": True,
                "tree": nested,
                "stats": {
                    "total_nodes": sum(1 for _ in nav.traverse_breadth_first()),
                    "max_depth": nav.get_max_depth(),
                    "root_count": len(roots),
                    "leaf_count": len(nav.get_leaves()),
                },
            }
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def move_hierarchy_node(
        hierarchy_id: str,
        new_parent_id: Optional[str],
        new_sort_order: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Move a hierarchy node to a new parent.

        Args:
            hierarchy_id: Hierarchy ID to move.
            new_parent_id: New parent hierarchy ID (None to make root).
            new_sort_order: New sort order among siblings (optional).

        Returns:
            Dictionary with moved hierarchy details.
        """
        service = HierarchyService()
        try:
            hierarchy = service.move_hierarchy(
                hierarchy_id=hierarchy_id,
                new_parent_id=new_parent_id,
                new_sort_order=new_sort_order,
            )
            return {
                "success": True,
                "hierarchy": {
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "parent_id": hierarchy.parent_id,
                    "sort_order": hierarchy.sort_order,
                },
            }
        except (HierarchyNotFoundError, HierarchyServiceError) as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_hierarchy_children(
        hierarchy_id: str,
        recursive: bool = False,
    ) -> Dict[str, Any]:
        """
        Get children of a hierarchy node.

        Args:
            hierarchy_id: Parent hierarchy ID.
            recursive: If True, get all descendants. If False, only direct children.

        Returns:
            Dictionary with list of child hierarchies.
        """
        service = HierarchyService()
        try:
            hierarchy = service.get_hierarchy(hierarchy_id)
            hierarchies = service.list_hierarchies(
                project_id=hierarchy.project_id,
                limit=10000,
            )

            items = [
                {
                    "hierarchy_id": h.hierarchy_id,
                    "hierarchy_name": h.hierarchy_name,
                    "parent_id": h.parent_id,
                    "sort_order": h.sort_order,
                }
                for h in hierarchies
            ]

            builder = TreeBuilder()
            roots = builder.build(items)
            nav = TreeNavigator(roots)

            if recursive:
                children = nav.get_descendants(hierarchy_id)
            else:
                children = nav.get_children(hierarchy_id)

            return {
                "success": True,
                "children": [
                    {
                        "hierarchy_id": c.hierarchy_id,
                        "hierarchy_name": c.hierarchy_name,
                        "parent_id": c.parent_id,
                        "depth": c.depth,
                    }
                    for c in children
                ],
                "count": len(children),
            }
        except HierarchyNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def add_source_mapping(
        hierarchy_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_column: str,
        source_uid: Optional[str] = None,
        mapping_index: int = 0,
        precedence_group: str = "DEFAULT",
    ) -> Dict[str, Any]:
        """
        Add a source mapping to a hierarchy.

        Maps a database column to a hierarchy node for data extraction.

        Args:
            hierarchy_id: Target hierarchy ID.
            source_database: Source database name (e.g., "ANALYTICS").
            source_schema: Source schema name (e.g., "PUBLIC").
            source_table: Source table name (e.g., "FACT_SALES").
            source_column: Source column name (e.g., "AMOUNT").
            source_uid: Optional filter value (e.g., "HW%" for hardware products).
            mapping_index: Order within hierarchy (lower = higher priority).
            precedence_group: Grouping for precedence rules.

        Returns:
            Dictionary with created mapping details.
        """
        service = HierarchyService()
        try:
            mapping = service.add_source_mapping(
                hierarchy_id=hierarchy_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                source_column=source_column,
                source_uid=source_uid,
                mapping_index=mapping_index,
                precedence_group=precedence_group,
            )
            return {
                "success": True,
                "mapping": {
                    "id": mapping.id,
                    "hierarchy_id": mapping.hierarchy_id,
                    "source_path": mapping.full_source_path,
                    "source_uid": mapping.source_uid,
                    "mapping_index": mapping.mapping_index,
                    "precedence_group": mapping.precedence_group,
                },
            }
        except HierarchyNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def remove_source_mapping(mapping_id: int) -> Dict[str, Any]:
        """
        Remove a source mapping.

        Args:
            mapping_id: Mapping ID to remove.

        Returns:
            Dictionary with deletion result.
        """
        service = HierarchyService()
        result = service.remove_source_mapping(mapping_id)
        if result:
            return {"success": True, "message": f"Mapping {mapping_id} removed"}
        return {"success": False, "error": f"Mapping {mapping_id} not found"}

    @mcp.tool()
    def get_mappings_by_precedence(
        hierarchy_id: str,
        precedence_group: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get source mappings for a hierarchy ordered by precedence.

        Args:
            hierarchy_id: Hierarchy ID.
            precedence_group: Optional filter by precedence group.

        Returns:
            Dictionary with ordered list of mappings.
        """
        service = HierarchyService()
        try:
            mappings = service.get_mappings(
                hierarchy_id=hierarchy_id,
                precedence_group=precedence_group,
            )
            return {
                "success": True,
                "mappings": [
                    {
                        "id": m.id,
                        "hierarchy_id": m.hierarchy_id,
                        "source_path": m.full_source_path,
                        "source_uid": m.source_uid,
                        "mapping_index": m.mapping_index,
                        "precedence_group": m.precedence_group,
                        "include_flag": m.include_flag,
                    }
                    for m in mappings
                ],
                "count": len(mappings),
            }
        except HierarchyNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def create_formula_group(
        project_id: str,
        name: str,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a formula group for calculated hierarchies.

        Formula groups contain rules for calculations like SUM, SUBTRACT, etc.

        Args:
            project_id: Project ID.
            name: Formula group name (e.g., "Revenue Calculations").
            description: Optional description of the formula group.

        Returns:
            Dictionary with created formula group details.
        """
        service = HierarchyService()
        try:
            group = service.create_formula_group(
                project_id=project_id,
                name=name,
                description=description,
            )
            return {
                "success": True,
                "formula_group": {
                    "id": group.id,
                    "name": group.name,
                    "description": group.description,
                    "project_id": group.project_id,
                },
            }
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def add_formula_rule(
        group_id: int,
        target_hierarchy_id: str,
        source_hierarchy_ids: List[str],
        operation: str,
        rule_order: int = 0,
    ) -> Dict[str, Any]:
        """
        Add a formula rule to a formula group.

        Args:
            group_id: Formula group ID.
            target_hierarchy_id: Target hierarchy for the calculation result.
            source_hierarchy_ids: List of source hierarchy IDs for calculation.
            operation: Operation to perform (SUM, SUBTRACT, MULTIPLY, DIVIDE, PERCENT, AVERAGE).
            rule_order: Order of rule execution (lower = earlier).

        Returns:
            Dictionary with created rule details.

        Example:
            # TOTAL_REVENUE = SUM(PRODUCT_REVENUE, SERVICE_REVENUE)
            add_formula_rule(
                group_id=1,
                target_hierarchy_id="TOTAL_REVENUE",
                source_hierarchy_ids=["PRODUCT_REVENUE", "SERVICE_REVENUE"],
                operation="SUM"
            )
        """
        service = HierarchyService()
        try:
            rule = service.add_formula_rule(
                group_id=group_id,
                target_hierarchy_id=target_hierarchy_id,
                source_hierarchy_ids=source_hierarchy_ids,
                operation=operation,
                rule_order=rule_order,
            )
            return {
                "success": True,
                "rule": {
                    "id": rule.id,
                    "group_id": rule.group_id,
                    "target_hierarchy_id": rule.target_hierarchy_id,
                    "source_hierarchy_ids": rule.get_source_ids(),
                    "operation": rule.operation,
                    "rule_order": rule.rule_order,
                },
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def list_formula_groups(project_id: str) -> Dict[str, Any]:
        """
        List formula groups in a project.

        Args:
            project_id: Project ID.

        Returns:
            Dictionary with list of formula groups and their rules.
        """
        service = HierarchyService()
        try:
            groups = service.list_formula_groups(project_id)
            return {
                "success": True,
                "formula_groups": [
                    {
                        "id": g.id,
                        "name": g.name,
                        "description": g.description,
                        "rules": [
                            {
                                "id": r.id,
                                "target": r.target_hierarchy_id,
                                "sources": r.get_source_ids(),
                                "operation": r.operation,
                            }
                            for r in g.rules
                        ],
                    }
                    for g in groups
                ],
                "count": len(groups),
            }
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def export_hierarchy_csv(
        project_id: str,
        file_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Export hierarchies to CSV format.

        Args:
            project_id: Project ID to export.
            file_path: Optional file path to save CSV. If None, returns content.

        Returns:
            Dictionary with CSV content or file path.
        """
        service = HierarchyService()
        handler = CSVHandler()
        try:
            hierarchies = service.list_hierarchies(project_id=project_id, limit=10000)

            data = []
            for h in hierarchies:
                row = {
                    "hierarchy_id": h.hierarchy_id,
                    "hierarchy_name": h.hierarchy_name,
                    "parent_id": h.parent_id,
                    "description": h.description,
                    "include_flag": h.include_flag,
                    "sort_order": h.sort_order,
                }
                # Add levels
                for i in range(1, 16):
                    level_val = getattr(h, f"level_{i}")
                    sort_val = getattr(h, f"level_{i}_sort")
                    if level_val:
                        row[f"level_{i}"] = level_val
                        row[f"level_{i}_sort"] = sort_val
                data.append(row)

            if file_path:
                from pathlib import Path
                handler.export_hierarchy_csv(data, Path(file_path))
                return {
                    "success": True,
                    "file_path": file_path,
                    "rows_exported": len(data),
                }
            else:
                content = handler.export_hierarchy_csv(data)
                return {
                    "success": True,
                    "csv_content": content[:5000] if len(content) > 5000 else content,
                    "rows_exported": len(data),
                    "truncated": len(content) > 5000,
                }
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def import_hierarchy_csv(
        project_id: str,
        file_path: Optional[str] = None,
        csv_content: Optional[str] = None,
        skip_errors: bool = True,
    ) -> Dict[str, Any]:
        """
        Import hierarchies from CSV format.

        Args:
            project_id: Project ID to import into.
            file_path: Path to CSV file (use either file_path or csv_content).
            csv_content: CSV content as string (use either file_path or csv_content).
            skip_errors: If True, skip rows with errors. If False, fail on first error.

        Returns:
            Dictionary with import results including success count and errors.
        """
        handler = CSVHandler()
        service = HierarchyService()

        try:
            # Verify project exists
            project = service.get_project(project_id)

            if file_path:
                result = handler.import_hierarchy_csv(file_path, skip_errors=skip_errors)
            elif csv_content:
                result = handler.import_from_string(csv_content, "hierarchy")
            else:
                return {"success": False, "error": "Provide either file_path or csv_content"}

            if not result.success and not skip_errors:
                return {
                    "success": False,
                    "error": f"Import failed: {[str(e) for e in result.errors]}",
                }

            # Create hierarchies from imported data
            created = 0
            errors = []
            for row in result.data:
                try:
                    service.create_hierarchy(
                        project_id=project.id,
                        hierarchy_id=row["hierarchy_id"],
                        hierarchy_name=row.get("hierarchy_name", row["hierarchy_id"]),
                        parent_id=row.get("parent_id"),
                        description=row.get("description"),
                        levels={
                            f"level_{i}": row.get(f"level_{i}")
                            for i in range(1, 16)
                            if row.get(f"level_{i}")
                        },
                        sort_order=row.get("sort_order", 0),
                    )
                    created += 1
                except DuplicateError:
                    errors.append(f"Duplicate: {row['hierarchy_id']}")
                except Exception as e:
                    errors.append(f"Error on {row['hierarchy_id']}: {str(e)}")

            return {
                "success": True,
                "rows_processed": result.rows_processed,
                "rows_imported": created,
                "errors": errors[:20] if errors else [],
                "warnings": [str(w) for w in result.warnings][:10] if result.warnings else [],
            }
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}
