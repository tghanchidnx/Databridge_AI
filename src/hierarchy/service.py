"""Hierarchy Builder Service - Core business logic."""
import json
import uuid
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import hashlib

from .types import (
    SmartHierarchy,
    HierarchyProject,
    HierarchyLevel,
    HierarchyFlags,
    SourceMapping,
    SourceMappingFlags,
    FormulaGroup,
    FormulaRule,
    FormulaConfig,
    FilterConfig,
    FilterCondition,
    DeploymentConfig,
)


class HierarchyService:
    """Service for managing hierarchy projects and hierarchies."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.projects_file = self.data_dir / "hierarchy_projects.json"
        self.hierarchies_file = self.data_dir / "hierarchies.json"
        self.deployments_file = self.data_dir / "deployment_history.json"
        self._init_storage()

    def _init_storage(self):
        """Initialize JSON storage files."""
        if not self.projects_file.exists():
            self._save_json(self.projects_file, {"projects": {}})
        if not self.hierarchies_file.exists():
            self._save_json(self.hierarchies_file, {"hierarchies": {}})
        if not self.deployments_file.exists():
            self._save_json(self.deployments_file, {"deployments": []})

    def _load_json(self, path: Path) -> dict:
        """Load JSON from file."""
        with open(path, "r") as f:
            return json.load(f)

    def _save_json(self, path: Path, data: dict):
        """Save JSON to file."""
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _generate_id(self) -> str:
        """Generate a UUID."""
        return str(uuid.uuid4())

    def _generate_hierarchy_id(self, name: str, project_id: str) -> str:
        """Generate a unique hierarchy ID from name."""
        slug = re.sub(r"[^A-Z0-9]+", "_", name.upper())
        slug = re.sub(r"^_+|_+$", "", slug)[:50]

        data = self._load_json(self.hierarchies_file)
        existing = [h for h in data["hierarchies"].values()
                   if h.get("project_id") == project_id and h.get("hierarchy_id", "").startswith(slug)]

        counter = len(existing) + 1
        return f"{slug}_{counter}"

    # =========================================================================
    # Project Management
    # =========================================================================

    def create_project(self, name: str, description: str = "") -> HierarchyProject:
        """Create a new hierarchy project."""
        data = self._load_json(self.projects_file)

        project = HierarchyProject(
            id=self._generate_id(),
            name=name,
            description=description,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        data["projects"][project.id] = project.model_dump()
        self._save_json(self.projects_file, data)

        return project

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects with hierarchy counts."""
        data = self._load_json(self.projects_file)
        hier_data = self._load_json(self.hierarchies_file)

        projects = []
        for proj_id, proj in data["projects"].items():
            count = len([h for h in hier_data["hierarchies"].values()
                        if h.get("project_id") == proj_id])
            proj["hierarchy_count"] = count
            projects.append(proj)

        return projects

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific project."""
        data = self._load_json(self.projects_file)
        return data["projects"].get(project_id)

    def delete_project(self, project_id: str) -> bool:
        """Delete a project and all its hierarchies."""
        data = self._load_json(self.projects_file)
        if project_id not in data["projects"]:
            return False

        del data["projects"][project_id]
        self._save_json(self.projects_file, data)

        # Delete associated hierarchies
        hier_data = self._load_json(self.hierarchies_file)
        hier_data["hierarchies"] = {
            k: v for k, v in hier_data["hierarchies"].items()
            if v.get("project_id") != project_id
        }
        self._save_json(self.hierarchies_file, hier_data)

        return True

    # =========================================================================
    # Hierarchy CRUD
    # =========================================================================

    def create_hierarchy(
        self,
        project_id: str,
        hierarchy_name: str,
        parent_id: Optional[str] = None,
        description: str = "",
        flags: Optional[Dict] = None,
        hierarchy_level: Optional[Dict] = None,
        sort_order: Optional[int] = None,
    ) -> SmartHierarchy:
        """
        Create a new hierarchy node.

        Args:
            project_id: Parent project ID
            hierarchy_name: Display name for the hierarchy
            parent_id: Parent hierarchy ID (optional)
            description: Hierarchy description
            flags: Hierarchy flags (include_flag, exclude_flag, etc.)
            hierarchy_level: Optional level data including LEVEL_X and LEVEL_X_SORT values.
                             If not provided, levels are auto-calculated from parent.
            sort_order: Optional sort order. If not provided, auto-calculated.
        """
        data = self._load_json(self.hierarchies_file)

        hierarchy_id = self._generate_hierarchy_id(hierarchy_name, project_id)

        # Use provided hierarchy_level or calculate from parent
        if hierarchy_level:
            level_obj = HierarchyLevel(**hierarchy_level)
        else:
            level_obj = self._calculate_levels(project_id, hierarchy_name, parent_id)

        # Use provided sort_order or calculate next
        final_sort_order = sort_order if sort_order is not None else self._get_next_sort_order(project_id, parent_id)

        hierarchy = SmartHierarchy(
            id=self._generate_id(),
            project_id=project_id,
            hierarchy_id=hierarchy_id,
            hierarchy_name=hierarchy_name,
            description=description,
            parent_id=parent_id,
            is_root=parent_id is None,
            sort_order=final_sort_order,
            hierarchy_level=level_obj,
            flags=HierarchyFlags(**(flags or {})),
            mapping=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        data["hierarchies"][hierarchy.id] = hierarchy.model_dump()
        self._save_json(self.hierarchies_file, data)

        return hierarchy

    def _calculate_levels(
        self, project_id: str, hierarchy_name: str, parent_id: Optional[str]
    ) -> HierarchyLevel:
        """Calculate hierarchy levels based on parent."""
        levels = HierarchyLevel()

        if not parent_id:
            levels.level_1 = hierarchy_name
            return levels

        # Get parent hierarchy
        parent = self.get_hierarchy_by_id(parent_id)
        if not parent:
            levels.level_1 = hierarchy_name
            return levels

        parent_levels = parent.get("hierarchy_level", {})

        # Copy parent levels
        for i in range(1, 16):
            key = f"level_{i}"
            if parent_levels.get(key):
                setattr(levels, key, parent_levels[key])
            else:
                setattr(levels, key, hierarchy_name)
                break

        return levels

    def _get_next_sort_order(self, project_id: str, parent_id: Optional[str]) -> int:
        """Get the next sort order for siblings."""
        data = self._load_json(self.hierarchies_file)
        siblings = [
            h for h in data["hierarchies"].values()
            if h.get("project_id") == project_id and h.get("parent_id") == parent_id
        ]
        if not siblings:
            return 1
        return max(h.get("sort_order", 0) for h in siblings) + 1

    def get_hierarchy(self, project_id: str, hierarchy_id: str) -> Optional[Dict]:
        """Get a hierarchy by project and hierarchy ID."""
        data = self._load_json(self.hierarchies_file)
        for h in data["hierarchies"].values():
            if h.get("project_id") == project_id and h.get("hierarchy_id") == hierarchy_id:
                return h
        return None

    def get_hierarchy_by_id(self, id: str) -> Optional[Dict]:
        """Get a hierarchy by UUID."""
        data = self._load_json(self.hierarchies_file)
        return data["hierarchies"].get(id)

    def update_hierarchy(
        self, project_id: str, hierarchy_id: str, updates: Dict[str, Any]
    ) -> Optional[Dict]:
        """Update a hierarchy."""
        data = self._load_json(self.hierarchies_file)

        for id, h in data["hierarchies"].items():
            if h.get("project_id") == project_id and h.get("hierarchy_id") == hierarchy_id:
                h.update(updates)
                h["updated_at"] = datetime.now().isoformat()
                data["hierarchies"][id] = h
                self._save_json(self.hierarchies_file, data)
                return h

        return None

    def delete_hierarchy(self, project_id: str, hierarchy_id: str) -> bool:
        """Delete a hierarchy."""
        data = self._load_json(self.hierarchies_file)

        to_delete = None
        for id, h in data["hierarchies"].items():
            if h.get("project_id") == project_id and h.get("hierarchy_id") == hierarchy_id:
                to_delete = id
                break

        if to_delete:
            del data["hierarchies"][to_delete]
            self._save_json(self.hierarchies_file, data)
            return True

        return False

    def list_hierarchies(self, project_id: str) -> List[Dict]:
        """List all hierarchies for a project."""
        data = self._load_json(self.hierarchies_file)
        hierarchies = [
            h for h in data["hierarchies"].values()
            if h.get("project_id") == project_id
        ]
        return sorted(hierarchies, key=lambda x: (x.get("sort_order", 0), x.get("hierarchy_name", "")))

    def get_hierarchy_tree(self, project_id: str) -> List[Dict]:
        """Get hierarchies as a tree structure."""
        hierarchies = self.list_hierarchies(project_id)

        # Build tree
        by_parent = {}
        roots = []

        for h in hierarchies:
            parent_id = h.get("parent_id")
            if parent_id:
                if parent_id not in by_parent:
                    by_parent[parent_id] = []
                by_parent[parent_id].append(h)
            else:
                roots.append(h)

        def build_tree(node):
            node_id = node.get("id")
            children = by_parent.get(node_id, [])
            node["children"] = [build_tree(c) for c in children]
            return node

        return [build_tree(r) for r in roots]

    # =========================================================================
    # Source Mapping
    # =========================================================================

    def add_source_mapping(
        self,
        project_id: str,
        hierarchy_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_column: str,
        source_uid: str = "",
        precedence_group: str = "1",
    ) -> Optional[Dict]:
        """Add a source mapping to a hierarchy."""
        hierarchy = self.get_hierarchy(project_id, hierarchy_id)
        if not hierarchy:
            return None

        mappings = hierarchy.get("mapping", [])
        next_index = max([m.get("mapping_index", 0) for m in mappings], default=0) + 1

        new_mapping = {
            "mapping_index": next_index,
            "source_database": source_database,
            "source_schema": source_schema,
            "source_table": source_table,
            "source_column": source_column,
            "source_uid": source_uid,
            "precedence_group": precedence_group,
            "flags": {
                "include_flag": True,
                "exclude_flag": False,
                "transform_flag": False,
                "active_flag": True,
            }
        }

        mappings.append(new_mapping)
        return self.update_hierarchy(project_id, hierarchy_id, {"mapping": mappings})

    def remove_source_mapping(
        self, project_id: str, hierarchy_id: str, mapping_index: int
    ) -> Optional[Dict]:
        """Remove a source mapping by index."""
        hierarchy = self.get_hierarchy(project_id, hierarchy_id)
        if not hierarchy:
            return None

        mappings = [m for m in hierarchy.get("mapping", [])
                   if m.get("mapping_index") != mapping_index]

        return self.update_hierarchy(project_id, hierarchy_id, {"mapping": mappings})

    def get_child_hierarchies(self, project_id: str, parent_ref: str) -> List[Dict]:
        """
        Get all immediate children of a hierarchy.

        Args:
            project_id: Project UUID
            parent_ref: Can be UUID (id) or hierarchy_id - checks both for compatibility
        """
        data = self._load_json(self.hierarchies_file)

        # Find the parent to get both its id and hierarchy_id
        parent = self.get_hierarchy_by_id(parent_ref)
        if not parent:
            # Maybe parent_ref is a hierarchy_id
            parent = self.get_hierarchy(project_id, parent_ref)

        # Collect all possible parent references to match
        parent_refs = {parent_ref}
        if parent:
            parent_refs.add(parent.get("id", ""))
            parent_refs.add(parent.get("hierarchy_id", ""))

        children = [
            h for h in data["hierarchies"].values()
            if h.get("project_id") == project_id and h.get("parent_id") in parent_refs
        ]
        return sorted(children, key=lambda x: x.get("sort_order", 0))

    def get_all_descendants(self, project_id: str, parent_uuid: str) -> List[Dict]:
        """Get all descendants (children, grandchildren, etc.) recursively."""
        descendants = []
        children = self.get_child_hierarchies(project_id, parent_uuid)

        for child in children:
            descendants.append(child)
            child_descendants = self.get_all_descendants(project_id, child.get("id"))
            descendants.extend(child_descendants)

        return descendants

    def get_inherited_mappings(self, project_id: str, hierarchy_uuid: str) -> Dict[str, Any]:
        """
        Get all mappings inherited from child hierarchies.

        Returns a dictionary with:
        - own_mappings: Mappings directly on this hierarchy
        - inherited_mappings: Mappings from all descendants, grouped by child
        - by_precedence: All mappings grouped by precedence_group
        - total_count: Total number of mappings (own + inherited)
        - child_counts: Mapping counts per immediate child
        """
        hierarchy = self.get_hierarchy_by_id(hierarchy_uuid)
        if not hierarchy:
            return {"error": "Hierarchy not found"}

        own_mappings = hierarchy.get("mapping", [])

        # Get all descendants
        descendants = self.get_all_descendants(project_id, hierarchy_uuid)

        # Collect mappings from each descendant
        inherited_mappings = []
        child_mapping_details = []

        for desc in descendants:
            desc_mappings = desc.get("mapping", [])
            if desc_mappings:
                for m in desc_mappings:
                    mapping_with_source = {
                        **m,
                        "inherited_from_id": desc.get("id"),
                        "inherited_from_name": desc.get("hierarchy_name"),
                        "inherited_from_hierarchy_id": desc.get("hierarchy_id"),
                    }
                    inherited_mappings.append(mapping_with_source)

                child_mapping_details.append({
                    "hierarchy_id": desc.get("hierarchy_id"),
                    "hierarchy_name": desc.get("hierarchy_name"),
                    "uuid": desc.get("id"),
                    "mapping_count": len(desc_mappings),
                    "mappings": desc_mappings,
                })

        # Group all mappings by precedence
        all_mappings = own_mappings + inherited_mappings
        by_precedence = {}
        for m in all_mappings:
            prec = m.get("precedence_group", "1")
            if prec not in by_precedence:
                by_precedence[prec] = []
            by_precedence[prec].append(m)

        # Get immediate child counts
        immediate_children = self.get_child_hierarchies(project_id, hierarchy_uuid)
        child_counts = []
        for child in immediate_children:
            child_desc = self.get_all_descendants(project_id, child.get("id"))
            child_own = child.get("mapping", [])
            child_inherited = sum(len(d.get("mapping", [])) for d in child_desc)
            child_counts.append({
                "hierarchy_id": child.get("hierarchy_id"),
                "hierarchy_name": child.get("hierarchy_name"),
                "uuid": child.get("id"),
                "own_count": len(child_own),
                "inherited_count": child_inherited,
                "total_count": len(child_own) + child_inherited,
            })

        return {
            "hierarchy_id": hierarchy.get("hierarchy_id"),
            "hierarchy_name": hierarchy.get("hierarchy_name"),
            "own_mappings": own_mappings,
            "own_count": len(own_mappings),
            "inherited_mappings": inherited_mappings,
            "inherited_count": len(inherited_mappings),
            "total_count": len(own_mappings) + len(inherited_mappings),
            "by_precedence": by_precedence,
            "precedence_groups": sorted(by_precedence.keys()),
            "child_counts": child_counts,
            "child_mapping_details": child_mapping_details,
        }

    def get_mapping_summary(self, project_id: str) -> Dict[str, Any]:
        """
        Get mapping summary for entire project with inheritance info.
        Shows each hierarchy's own mappings and inherited mappings from children.
        """
        hierarchies = self.list_hierarchies(project_id)

        summary = []
        for h in hierarchies:
            inherited_info = self.get_inherited_mappings(project_id, h.get("id"))
            summary.append({
                "hierarchy_id": h.get("hierarchy_id"),
                "hierarchy_name": h.get("hierarchy_name"),
                "uuid": h.get("id"),
                "parent_id": h.get("parent_id"),
                "own_mappings": inherited_info.get("own_count", 0),
                "inherited_mappings": inherited_info.get("inherited_count", 0),
                "total_mappings": inherited_info.get("total_count", 0),
                "precedence_groups": inherited_info.get("precedence_groups", []),
                "child_counts": inherited_info.get("child_counts", []),
            })

        return {
            "project_id": project_id,
            "hierarchies": summary,
            "total_hierarchies": len(hierarchies),
        }

    # =========================================================================
    # Formula Management
    # =========================================================================

    def create_formula_group(
        self,
        project_id: str,
        main_hierarchy_id: str,
        group_name: str,
        rules: List[Dict],
    ) -> Optional[Dict]:
        """Create a formula group for a hierarchy."""
        hierarchy = self.get_hierarchy(project_id, main_hierarchy_id)
        if not hierarchy:
            return None

        formula_group = {
            "group_name": group_name,
            "main_hierarchy_id": main_hierarchy_id,
            "main_hierarchy_name": hierarchy.get("hierarchy_name"),
            "rules": rules,
        }

        formula_config = {
            "formula_type": "EXPRESSION",
            "formula_group": formula_group,
        }

        # Mark as calculation
        flags = hierarchy.get("flags", {})
        flags["calculation_flag"] = True

        return self.update_hierarchy(project_id, main_hierarchy_id, {
            "formula_config": formula_config,
            "flags": flags,
        })

    def add_formula_rule(
        self,
        project_id: str,
        main_hierarchy_id: str,
        operation: str,
        source_hierarchy_id: str,
        precedence: int = 1,
        constant_number: Optional[float] = None,
    ) -> Optional[Dict]:
        """Add a rule to an existing formula group."""
        hierarchy = self.get_hierarchy(project_id, main_hierarchy_id)
        if not hierarchy:
            return None

        source_hierarchy = self.get_hierarchy(project_id, source_hierarchy_id)
        if not source_hierarchy:
            return None

        formula_config = hierarchy.get("formula_config", {})
        formula_group = formula_config.get("formula_group", {
            "group_name": f"{hierarchy.get('hierarchy_name')} Formula",
            "main_hierarchy_id": main_hierarchy_id,
            "main_hierarchy_name": hierarchy.get("hierarchy_name"),
            "rules": [],
        })

        new_rule = {
            "operation": operation,
            "hierarchy_id": source_hierarchy_id,
            "hierarchy_name": source_hierarchy.get("hierarchy_name"),
            "precedence": precedence,
        }
        if constant_number is not None:
            new_rule["constant_number"] = constant_number

        formula_group["rules"].append(new_rule)
        formula_config["formula_group"] = formula_group
        formula_config["formula_type"] = "EXPRESSION"

        flags = hierarchy.get("flags", {})
        flags["calculation_flag"] = True

        return self.update_hierarchy(project_id, main_hierarchy_id, {
            "formula_config": formula_config,
            "flags": flags,
        })

    def list_formula_groups(self, project_id: str) -> List[Dict]:
        """List all hierarchies with formula groups."""
        hierarchies = self.list_hierarchies(project_id)
        return [
            {
                "hierarchy_id": h.get("hierarchy_id"),
                "hierarchy_name": h.get("hierarchy_name"),
                "formula_group": h.get("formula_config", {}).get("formula_group"),
            }
            for h in hierarchies
            if h.get("formula_config", {}).get("formula_group")
        ]

    # =========================================================================
    # Import/Export
    # =========================================================================

    def export_hierarchy_csv(self, project_id: str) -> str:
        """
        Export hierarchies to CSV format.

        Includes LEVEL_X columns for hierarchy values and LEVEL_X_SORT columns
        for controlling display order within each level.
        """
        hierarchies = self.list_hierarchies(project_id)

        headers = [
            "HIERARCHY_ID", "HIERARCHY_NAME", "PARENT_ID", "DESCRIPTION",
            "LEVEL_1", "LEVEL_2", "LEVEL_3", "LEVEL_4", "LEVEL_5",
            "LEVEL_6", "LEVEL_7", "LEVEL_8", "LEVEL_9", "LEVEL_10",
            "LEVEL_1_SORT", "LEVEL_2_SORT", "LEVEL_3_SORT", "LEVEL_4_SORT", "LEVEL_5_SORT",
            "LEVEL_6_SORT", "LEVEL_7_SORT", "LEVEL_8_SORT", "LEVEL_9_SORT", "LEVEL_10_SORT",
            "INCLUDE_FLAG", "EXCLUDE_FLAG", "TRANSFORM_FLAG",
            "CALCULATION_FLAG", "ACTIVE_FLAG", "IS_LEAF_NODE",
            "FORMULA_GROUP", "SORT_ORDER"
        ]

        rows = [",".join(headers)]

        for h in hierarchies:
            levels = h.get("hierarchy_level", {}) or {}
            flags = h.get("flags", {}) or {}
            formula_config = h.get("formula_config") or {}
            formula_group = formula_config.get("formula_group") or {}

            row = [
                h.get("hierarchy_id", ""),
                f'"{h.get("hierarchy_name", "")}"',
                h.get("parent_id") or "",
                f'"{h.get("description", "") or ""}"',
                # Level values
                levels.get("level_1", "") or "",
                levels.get("level_2", "") or "",
                levels.get("level_3", "") or "",
                levels.get("level_4", "") or "",
                levels.get("level_5", "") or "",
                levels.get("level_6", "") or "",
                levels.get("level_7", "") or "",
                levels.get("level_8", "") or "",
                levels.get("level_9", "") or "",
                levels.get("level_10", "") or "",
                # Level sort values
                str(levels.get("level_1_sort", "") or ""),
                str(levels.get("level_2_sort", "") or ""),
                str(levels.get("level_3_sort", "") or ""),
                str(levels.get("level_4_sort", "") or ""),
                str(levels.get("level_5_sort", "") or ""),
                str(levels.get("level_6_sort", "") or ""),
                str(levels.get("level_7_sort", "") or ""),
                str(levels.get("level_8_sort", "") or ""),
                str(levels.get("level_9_sort", "") or ""),
                str(levels.get("level_10_sort", "") or ""),
                # Flags
                str(flags.get("include_flag", True)).lower(),
                str(flags.get("exclude_flag", False)).lower(),
                str(flags.get("transform_flag", False)).lower(),
                str(flags.get("calculation_flag", False)).lower(),
                str(flags.get("active_flag", True)).lower(),
                str(flags.get("is_leaf_node", False)).lower(),
                formula_group.get("group_name", ""),
                str(h.get("sort_order", 0)),
            ]
            rows.append(",".join(row))

        return "\n".join(rows)

    def export_mapping_csv(self, project_id: str) -> str:
        """Export source mappings to CSV format."""
        hierarchies = self.list_hierarchies(project_id)

        headers = [
            "HIERARCHY_ID", "MAPPING_INDEX", "SOURCE_DATABASE", "SOURCE_SCHEMA",
            "SOURCE_TABLE", "SOURCE_COLUMN", "SOURCE_UID", "PRECEDENCE_GROUP",
            "INCLUDE_FLAG", "EXCLUDE_FLAG", "TRANSFORM_FLAG", "ACTIVE_FLAG"
        ]

        rows = [",".join(headers)]

        for h in hierarchies:
            for m in h.get("mapping", []):
                flags = m.get("flags", {})
                row = [
                    h.get("hierarchy_id", ""),
                    str(m.get("mapping_index", 0)),
                    m.get("source_database", ""),
                    m.get("source_schema", ""),
                    m.get("source_table", ""),
                    m.get("source_column", ""),
                    m.get("source_uid", ""),
                    m.get("precedence_group", "1"),
                    str(flags.get("include_flag", True)).lower(),
                    str(flags.get("exclude_flag", False)).lower(),
                    str(flags.get("transform_flag", False)).lower(),
                    str(flags.get("active_flag", True)).lower(),
                ]
                rows.append(",".join(row))

        return "\n".join(rows)

    def import_mapping_csv(self, project_id: str, csv_content: str) -> Dict[str, Any]:
        """
        Import source mappings from CSV.

        Expected CSV format (columns):
        HIERARCHY_ID, MAPPING_INDEX, SOURCE_DATABASE, SOURCE_SCHEMA,
        SOURCE_TABLE, SOURCE_COLUMN, SOURCE_UID, PRECEDENCE_GROUP,
        INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, ACTIVE_FLAG

        Note: The HIERARCHY_ID must match existing hierarchies in the project.
        """
        lines = csv_content.strip().split("\n")
        if not lines:
            return {"imported": 0, "skipped": 0, "errors": ["Empty CSV"]}

        headers = [h.strip().upper() for h in lines[0].split(",")]
        imported = 0
        skipped = 0
        errors = []

        # Get all hierarchies for lookup
        all_hierarchies = self.list_hierarchies(project_id)
        hierarchy_map = {h.get("hierarchy_id"): h for h in all_hierarchies}

        for i, line in enumerate(lines[1:], start=2):
            try:
                values = self._parse_csv_line(line)
                if len(values) < len(headers):
                    values.extend([""] * (len(headers) - len(values)))

                row = dict(zip(headers, values))

                hierarchy_id = row.get("HIERARCHY_ID", "").strip()
                if not hierarchy_id:
                    skipped += 1
                    continue

                # Find the hierarchy
                if hierarchy_id not in hierarchy_map:
                    errors.append(f"Row {i}: Hierarchy '{hierarchy_id}' not found")
                    skipped += 1
                    continue

                # Add the mapping
                result = self.add_source_mapping(
                    project_id=project_id,
                    hierarchy_id=hierarchy_id,
                    source_database=row.get("SOURCE_DATABASE", ""),
                    source_schema=row.get("SOURCE_SCHEMA", ""),
                    source_table=row.get("SOURCE_TABLE", ""),
                    source_column=row.get("SOURCE_COLUMN", ""),
                    source_uid=row.get("SOURCE_UID", ""),
                    precedence_group=row.get("PRECEDENCE_GROUP", "1"),
                )

                if result:
                    imported += 1
                else:
                    errors.append(f"Row {i}: Failed to add mapping to '{hierarchy_id}'")
                    skipped += 1

            except Exception as e:
                errors.append(f"Row {i}: {str(e)}")
                skipped += 1

        return {"imported": imported, "skipped": skipped, "errors": errors}

    def import_hierarchy_csv(self, project_id: str, csv_content: str) -> Dict[str, Any]:
        """
        Import hierarchies from CSV.

        Expected columns include:
        - HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
        - LEVEL_1 through LEVEL_10 (level values)
        - LEVEL_1_SORT through LEVEL_10_SORT (level sort orders)
        - INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG, ACTIVE_FLAG, IS_LEAF_NODE
        - FORMULA_GROUP, SORT_ORDER
        """
        lines = csv_content.strip().split("\n")
        if not lines:
            return {"imported": 0, "skipped": 0, "errors": ["Empty CSV"]}

        headers = [h.strip().upper() for h in lines[0].split(",")]
        imported = 0
        skipped = 0
        errors = []

        for i, line in enumerate(lines[1:], start=2):
            try:
                values = self._parse_csv_line(line)
                if len(values) < len(headers):
                    values.extend([""] * (len(headers) - len(values)))

                row = dict(zip(headers, values))

                # Create hierarchy from row
                hierarchy_name = row.get("HIERARCHY_NAME", "").strip('"')
                if not hierarchy_name:
                    skipped += 1
                    continue

                flags = {
                    "include_flag": row.get("INCLUDE_FLAG", "true").lower() == "true",
                    "exclude_flag": row.get("EXCLUDE_FLAG", "false").lower() == "true",
                    "transform_flag": row.get("TRANSFORM_FLAG", "false").lower() == "true",
                    "calculation_flag": row.get("CALCULATION_FLAG", "false").lower() == "true",
                    "active_flag": row.get("ACTIVE_FLAG", "true").lower() == "true",
                    "is_leaf_node": row.get("IS_LEAF_NODE", "false").lower() == "true",
                }

                # Build hierarchy_level with LEVEL_X and LEVEL_X_SORT values
                hierarchy_level = {}
                for level_num in range(1, 16):
                    level_key = f"LEVEL_{level_num}"
                    sort_key = f"LEVEL_{level_num}_SORT"

                    level_val = row.get(level_key, "").strip()
                    if level_val:
                        hierarchy_level[f"level_{level_num}"] = level_val

                    sort_val = row.get(sort_key, "").strip()
                    if sort_val:
                        try:
                            hierarchy_level[f"level_{level_num}_sort"] = int(sort_val)
                        except ValueError:
                            pass  # Skip invalid sort values

                # Get sort_order from CSV if provided
                sort_order = None
                sort_order_val = row.get("SORT_ORDER", "").strip()
                if sort_order_val:
                    try:
                        sort_order = int(sort_order_val)
                    except ValueError:
                        pass

                self.create_hierarchy(
                    project_id=project_id,
                    hierarchy_name=hierarchy_name,
                    parent_id=row.get("PARENT_ID") or None,
                    description=row.get("DESCRIPTION", "").strip('"'),
                    flags=flags,
                    hierarchy_level=hierarchy_level if hierarchy_level else None,
                    sort_order=sort_order,
                )
                imported += 1

            except Exception as e:
                errors.append(f"Row {i}: {str(e)}")
                skipped += 1

        return {"imported": imported, "skipped": skipped, "errors": errors}

    def _parse_csv_line(self, line: str) -> List[str]:
        """Parse a CSV line handling quoted values."""
        values = []
        current = ""
        in_quotes = False

        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == "," and not in_quotes:
                values.append(current.strip())
                current = ""
            else:
                current += char

        values.append(current.strip())
        return values

    def export_project_json(self, project_id: str) -> Dict[str, Any]:
        """Export complete project as JSON."""
        project = self.get_project(project_id)
        hierarchies = self.list_hierarchies(project_id)

        return {
            "version": "1.0",
            "exported_at": datetime.now().isoformat(),
            "project": project,
            "hierarchies": hierarchies,
        }

    # =========================================================================
    # Script Generation
    # =========================================================================

    def generate_insert_script(
        self, project_id: str, table_name: str = "HIERARCHY_MASTER"
    ) -> str:
        """Generate INSERT statements for hierarchies."""
        hierarchies = self.list_hierarchies(project_id)

        script = f"-- Hierarchy INSERT Script\n"
        script += f"-- Generated: {datetime.now().isoformat()}\n\n"

        for h in hierarchies:
            levels = h.get("hierarchy_level", {})
            flags = h.get("flags", {})

            script += f"INSERT INTO {table_name} (\n"
            script += "  HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID,\n"
            script += "  LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,\n"
            script += "  INCLUDE_FLAG, EXCLUDE_FLAG, CALCULATION_FLAG, ACTIVE_FLAG\n"
            script += ") VALUES (\n"
            script += f"  '{h.get('hierarchy_id')}',\n"
            script += f"  '{h.get('hierarchy_name')}',\n"
            script += f"  {self._sql_value(h.get('parent_id'))},\n"
            script += f"  {self._sql_value(levels.get('level_1'))},\n"
            script += f"  {self._sql_value(levels.get('level_2'))},\n"
            script += f"  {self._sql_value(levels.get('level_3'))},\n"
            script += f"  {self._sql_value(levels.get('level_4'))},\n"
            script += f"  {self._sql_value(levels.get('level_5'))},\n"
            script += f"  {str(flags.get('include_flag', True)).upper()},\n"
            script += f"  {str(flags.get('exclude_flag', False)).upper()},\n"
            script += f"  {str(flags.get('calculation_flag', False)).upper()},\n"
            script += f"  {str(flags.get('active_flag', True)).upper()}\n"
            script += ");\n\n"

        return script

    def generate_view_script(
        self, project_id: str, view_name: str = "V_HIERARCHY_MASTER"
    ) -> str:
        """Generate VIEW script for hierarchies."""
        project = self.get_project(project_id)

        script = f"-- Hierarchy VIEW Script\n"
        script += f"-- Project: {project.get('name') if project else 'Unknown'}\n"
        script += f"-- Generated: {datetime.now().isoformat()}\n\n"

        script += f"CREATE OR REPLACE VIEW {view_name} AS\n"
        script += "SELECT\n"
        script += "  HIERARCHY_ID,\n"
        script += "  HIERARCHY_NAME,\n"
        script += "  PARENT_ID,\n"
        script += "  LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,\n"
        script += "  INCLUDE_FLAG,\n"
        script += "  EXCLUDE_FLAG,\n"
        script += "  CALCULATION_FLAG,\n"
        script += "  ACTIVE_FLAG\n"
        script += "FROM HIERARCHY_MASTER\n"
        script += "WHERE ACTIVE_FLAG = TRUE;\n"

        return script

    def _sql_value(self, value: Any) -> str:
        """Convert value to SQL string."""
        if value is None or value == "":
            return "NULL"
        return f"'{value}'"

    # =========================================================================
    # Validation
    # =========================================================================

    def validate_project(self, project_id: str) -> Dict[str, Any]:
        """Validate a hierarchy project for issues."""
        hierarchies = self.list_hierarchies(project_id)
        issues = []
        warnings = []

        hierarchy_ids = {h.get("hierarchy_id") for h in hierarchies}

        for h in hierarchies:
            hier_id = h.get("hierarchy_id")

            # Check for orphaned hierarchies
            parent_id = h.get("parent_id")
            if parent_id and parent_id not in hierarchy_ids:
                issues.append(f"{hier_id}: Parent '{parent_id}' not found")

            # Check leaf nodes have mappings
            if h.get("flags", {}).get("is_leaf_node") and not h.get("mapping"):
                warnings.append(f"{hier_id}: Leaf node has no source mappings")

            # Check formula references
            formula_config = h.get("formula_config", {})
            formula_group = formula_config.get("formula_group", {})
            for rule in formula_group.get("rules", []):
                ref_id = rule.get("hierarchy_id")
                if ref_id and ref_id not in hierarchy_ids:
                    issues.append(f"{hier_id}: Formula references unknown hierarchy '{ref_id}'")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "hierarchy_count": len(hierarchies),
        }
