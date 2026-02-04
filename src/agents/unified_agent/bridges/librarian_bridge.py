"""
Librarian Bridge - Converts between Book and Librarian data models.

This bridge provides bidirectional conversion between:
- Book (Python in-memory hierarchy) - from Book/book/models.py
- Librarian (NestJS SmartHierarchyMaster) - via REST API

Key operations:
- book_to_librarian_hierarchies: Convert Book nodes to Librarian format
- librarian_hierarchies_to_book: Convert Librarian data to Book
- diff_book_project: Compare Book with Librarian project
- sync operations: Push/pull between systems
"""

import json
import logging
import requests
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("librarian_bridge")


class DiffResult:
    """Result of comparing a Book with a Librarian project."""

    def __init__(self):
        self.book_only: List[str] = []  # Nodes only in Book
        self.librarian_only: List[str] = []  # Hierarchies only in Librarian
        self.modified: List[Dict[str, Any]] = []  # Changed items
        self.identical: List[str] = []  # Unchanged items

    def to_dict(self) -> Dict[str, Any]:
        return {
            "book_only": self.book_only,
            "librarian_only": self.librarian_only,
            "modified": self.modified,
            "identical": self.identical,
            "summary": {
                "book_only_count": len(self.book_only),
                "librarian_only_count": len(self.librarian_only),
                "modified_count": len(self.modified),
                "identical_count": len(self.identical),
                "total_differences": len(self.book_only) + len(self.librarian_only) + len(self.modified),
            },
        }


class LibrarianBridge:
    """
    Bridge for converting and syncing between Book and Librarian.

    Usage:
        bridge = LibrarianBridge(base_url="http://localhost:8001/api", api_key="v2-dev-key-1")

        # Convert Book to Librarian hierarchies
        hierarchies = bridge.book_to_librarian_hierarchies(book, project_id="my-project")

        # Convert Librarian hierarchies to Book
        book = bridge.librarian_hierarchies_to_book(hierarchies, "My Book")

        # Compare differences
        diff = bridge.diff_book_project(book, project_id)
    """

    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        """
        Initialize the Librarian bridge.

        Args:
            base_url: Librarian API base URL (e.g., 'http://localhost:8001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json',
        }

    # =========================================================================
    # Book → Librarian Conversion
    # =========================================================================

    def book_to_librarian_hierarchies(
        self,
        book: Any,
        project_id: str,
        include_metadata: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Convert a Book instance to a list of Librarian SmartHierarchyMaster records.

        Args:
            book: Book instance (from Book.book.models)
            project_id: Target Librarian project ID
            include_metadata: Include Book properties in metadata field

        Returns:
            List of SmartHierarchyMaster-compatible dictionaries
        """
        hierarchies = []

        def process_node(
            node: Any,
            parent_id: Optional[str],
            depth: int,
            sort_order: int,
            level_path: List[str],
        ) -> int:
            """Process a single node and its children recursively."""
            nonlocal hierarchies

            # Generate hierarchy ID from node
            hierarchy_id = self._generate_hierarchy_id(node)

            # Build hierarchy level structure
            hierarchy_level = self._build_hierarchy_level(level_path + [node.name])

            # Convert flags
            flags = self._convert_book_flags_to_librarian(node.flags)

            # Determine if leaf node
            is_leaf = not node.children or len(node.children) == 0
            flags["is_leaf_node"] = is_leaf

            # Extract source mappings from properties
            mappings = self._extract_source_mappings(node.properties)

            # Build formula config from node formulas
            formula_config = self._convert_book_formulas_to_librarian(node.formulas)

            # Build the hierarchy record
            hierarchy = {
                "projectId": project_id,
                "hierarchyId": hierarchy_id,
                "hierarchyName": node.name,
                "description": node.properties.get("description", ""),
                "parentId": parent_id,
                "isRoot": parent_id is None,
                "sortOrder": sort_order,
                "hierarchyLevel": hierarchy_level,
                "flags": flags,
                "mapping": mappings,
            }

            if formula_config:
                hierarchy["formulaConfig"] = formula_config

            if include_metadata:
                # Store original Book properties in metadata
                metadata = {
                    "book_node_id": node.id,
                    "book_schema_version": node.schema_version,
                    "python_function": node.python_function,
                    "llm_prompt": node.llm_prompt,
                }
                # Add other properties (excluding internal ones)
                for key, value in node.properties.items():
                    if key not in ("source_mappings", "description") and not key.startswith("_"):
                        metadata[f"prop_{key}"] = value
                hierarchy["metadata"] = metadata

            hierarchies.append(hierarchy)

            # Process children
            child_sort = 0
            for child in node.children:
                child_sort = process_node(
                    child,
                    parent_id=hierarchy_id,
                    depth=depth + 1,
                    sort_order=child_sort,
                    level_path=level_path + [node.name],
                )
                child_sort += 1

            return sort_order

        # Process all root nodes
        sort_order = 0
        for root_node in book.root_nodes:
            process_node(root_node, parent_id=None, depth=0, sort_order=sort_order, level_path=[])
            sort_order += 1

        logger.info(f"Converted Book '{book.name}' to {len(hierarchies)} Librarian hierarchies")
        return hierarchies

    def _generate_hierarchy_id(self, node: Any) -> str:
        """Generate a unique hierarchy ID from a node."""
        # Use node ID if it looks like a valid hierarchy ID
        if node.id and not node.id.startswith("-") and len(node.id) <= 50:
            # Clean the ID
            clean_id = node.id.upper().replace(" ", "_")
            clean_id = "".join(c if c.isalnum() or c == "_" else "_" for c in clean_id)
            return clean_id[:50]

        # Generate from name
        slug = node.name.upper().replace(" ", "_")
        slug = "".join(c if c.isalnum() or c == "_" else "_" for c in slug)
        slug = slug.strip("_")[:40]

        # Add short unique suffix
        suffix = str(uuid.uuid4())[:6].upper()
        return f"{slug}_{suffix}"

    def _build_hierarchy_level(self, path: List[str]) -> Dict[str, str]:
        """Build the hierarchyLevel structure from a path."""
        level = {}
        for i, name in enumerate(path[:15], start=1):  # Max 15 levels
            level[f"level_{i}"] = name
        return level

    def _convert_book_flags_to_librarian(self, book_flags: Dict[str, bool]) -> Dict[str, Any]:
        """Convert Book flags to Librarian HierarchyFlags format."""
        return {
            "include_flag": book_flags.get("include", True),
            "exclude_flag": book_flags.get("exclude", False),
            "transform_flag": book_flags.get("transform", False),
            "calculation_flag": book_flags.get("calculation", False),
            "active_flag": book_flags.get("active", True),
            "is_leaf_node": book_flags.get("is_leaf", False),
        }

    def _extract_source_mappings(self, properties: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract source mappings from node properties."""
        mappings = []
        source_mappings = properties.get("source_mappings", [])

        for i, sm in enumerate(source_mappings):
            mapping = {
                "mapping_index": i + 1,
                "source_database": sm.get("database", ""),
                "source_schema": sm.get("schema", ""),
                "source_table": sm.get("table", ""),
                "source_column": sm.get("column", ""),
                "source_uid": sm.get("uid", ""),
                "precedence_group": sm.get("precedence_group", ""),
                "flags": {
                    "include_flag": sm.get("include", True),
                    "exclude_flag": sm.get("exclude", False),
                    "transform_flag": sm.get("transform", False),
                    "active_flag": sm.get("active", True),
                },
            }
            mappings.append(mapping)

        return mappings

    def _convert_book_formulas_to_librarian(self, formulas: List[Any]) -> Optional[Dict[str, Any]]:
        """Convert Book formulas to Librarian formulaConfig format."""
        if not formulas:
            return None

        # Take the first formula group
        formula = formulas[0] if formulas else None
        if not formula:
            return None

        return {
            "formula_type": "EXPRESSION",
            "formula_text": getattr(formula, "expression", str(formula)),
            "variables": getattr(formula, "variables", {}),
        }

    # =========================================================================
    # Librarian → Book Conversion
    # =========================================================================

    def librarian_hierarchies_to_book(
        self,
        hierarchies: List[Dict[str, Any]],
        book_name: str,
    ) -> Any:
        """
        Convert Librarian SmartHierarchyMaster records to a Book instance.

        Args:
            hierarchies: List of SmartHierarchyMaster records
            book_name: Name for the new Book

        Returns:
            Book instance
        """
        # Lazy import to avoid circular dependencies
        try:
            from Book.book.models import Book, Node
        except ImportError:
            # Try alternative import path
            import sys
            sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "Book"))
            from book.models import Book, Node

        # Build parent → children map
        children_map: Dict[str, List[Dict]] = defaultdict(list)
        hierarchy_map: Dict[str, Dict] = {}

        for h in hierarchies:
            hier_id = h.get("hierarchyId", h.get("hierarchy_id"))
            hierarchy_map[hier_id] = h

            parent_id = h.get("parentId", h.get("parent_id"))
            if parent_id:
                children_map[parent_id].append(h)

        # Sort children by sortOrder
        for parent_id in children_map:
            children_map[parent_id].sort(key=lambda x: x.get("sortOrder", x.get("sort_order", 0)))

        def create_node(h: Dict) -> Node:
            """Create a Node from a hierarchy record."""
            hier_id = h.get("hierarchyId", h.get("hierarchy_id"))

            # Convert flags
            flags = self._convert_librarian_flags_to_book(h.get("flags", {}))

            # Build properties
            properties = self._extract_book_properties(h)

            # Convert formulas
            formulas = self._convert_librarian_formulas_to_book(h.get("formulaConfig"))

            # Create children recursively
            child_hierarchies = children_map.get(hier_id, [])
            children = [create_node(ch) for ch in child_hierarchies]

            # Get original node ID from metadata if available
            metadata = h.get("metadata", {})
            node_id = metadata.get("book_node_id", hier_id)

            return Node(
                id=node_id,
                name=h.get("hierarchyName", h.get("hierarchy_name", "Unknown")),
                children=children,
                properties=properties,
                python_function=metadata.get("python_function"),
                llm_prompt=metadata.get("llm_prompt"),
                flags=flags,
                formulas=formulas,
            )

        # Find root nodes (no parent)
        root_hierarchies = [h for h in hierarchies if not h.get("parentId") and not h.get("parent_id")]
        root_hierarchies.sort(key=lambda x: x.get("sortOrder", x.get("sort_order", 0)))

        root_nodes = [create_node(h) for h in root_hierarchies]

        # Create Book
        book = Book(
            name=book_name,
            root_nodes=root_nodes,
            metadata={
                "source": "librarian",
                "converted_at": datetime.now(timezone.utc).isoformat(),
                "hierarchy_count": len(hierarchies),
            },
        )

        logger.info(f"Created Book '{book_name}' with {len(root_nodes)} root nodes from {len(hierarchies)} hierarchies")
        return book

    def _convert_librarian_flags_to_book(self, librarian_flags: Dict[str, Any]) -> Dict[str, bool]:
        """Convert Librarian flags to Book flags format."""
        return {
            "include": librarian_flags.get("include_flag", True),
            "exclude": librarian_flags.get("exclude_flag", False),
            "transform": librarian_flags.get("transform_flag", False),
            "calculation": librarian_flags.get("calculation_flag", False),
            "active": librarian_flags.get("active_flag", True),
            "is_leaf": librarian_flags.get("is_leaf_node", False),
        }

    def _extract_book_properties(self, hierarchy: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Book properties from a hierarchy record."""
        properties = {
            "description": hierarchy.get("description", ""),
        }

        # Convert mappings to source_mappings format
        mappings = hierarchy.get("mapping", [])
        if mappings:
            source_mappings = []
            for m in mappings:
                sm = {
                    "database": m.get("source_database", ""),
                    "schema": m.get("source_schema", ""),
                    "table": m.get("source_table", ""),
                    "column": m.get("source_column", ""),
                    "uid": m.get("source_uid", ""),
                    "precedence_group": m.get("precedence_group", ""),
                }
                if m.get("flags"):
                    sm["include"] = m["flags"].get("include_flag", True)
                    sm["exclude"] = m["flags"].get("exclude_flag", False)
                source_mappings.append(sm)
            properties["source_mappings"] = source_mappings

        # Extract custom properties from metadata
        metadata = hierarchy.get("metadata", {})
        for key, value in metadata.items():
            if key.startswith("prop_"):
                properties[key[5:]] = value

        return properties

    def _convert_librarian_formulas_to_book(self, formula_config: Optional[Dict]) -> List:
        """Convert Librarian formulaConfig to Book formulas."""
        if not formula_config:
            return []

        # Lazy import
        try:
            from Book.book.formulas import Formula
        except ImportError:
            # Return empty if Formula not available
            return []

        formula_text = formula_config.get("formula_text", "")
        if not formula_text:
            return []

        try:
            return [Formula(expression=formula_text, variables=formula_config.get("variables", {}))]
        except Exception:
            return []

    # =========================================================================
    # Diff Operations
    # =========================================================================

    def diff_book_project(self, book: Any, project_id: str) -> DiffResult:
        """
        Compare a Book with a Librarian project.

        Args:
            book: Book instance
            project_id: Librarian project ID

        Returns:
            DiffResult with differences
        """
        result = DiffResult()

        # Get Librarian hierarchies
        librarian_hierarchies = self.list_hierarchies(project_id)
        librarian_map = {
            h.get("hierarchyId", h.get("hierarchy_id")): h
            for h in librarian_hierarchies
        }

        # Convert Book to Librarian format for comparison
        book_hierarchies = self.book_to_librarian_hierarchies(book, project_id)
        book_map = {h["hierarchyId"]: h for h in book_hierarchies}

        # Find Book-only and modified
        for hier_id, book_hier in book_map.items():
            if hier_id not in librarian_map:
                result.book_only.append(hier_id)
            else:
                librarian_hier = librarian_map[hier_id]
                changes = self._compare_hierarchies(book_hier, librarian_hier)
                if changes:
                    result.modified.append({
                        "hierarchy_id": hier_id,
                        "changes": changes,
                    })
                else:
                    result.identical.append(hier_id)

        # Find Librarian-only
        for hier_id in librarian_map:
            if hier_id not in book_map:
                result.librarian_only.append(hier_id)

        return result

    def _compare_hierarchies(
        self,
        book_hier: Dict[str, Any],
        librarian_hier: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Compare two hierarchy records and return differences."""
        changes = []

        # Compare key fields
        compare_fields = [
            ("hierarchyName", "hierarchy_name"),
            ("description", "description"),
            ("parentId", "parent_id"),
            ("sortOrder", "sort_order"),
        ]

        for book_field, librarian_field in compare_fields:
            book_val = book_hier.get(book_field)
            lib_val = librarian_hier.get(book_field, librarian_hier.get(librarian_field))
            if book_val != lib_val:
                changes.append({
                    "field": book_field,
                    "book_value": book_val,
                    "librarian_value": lib_val,
                })

        # Compare flags
        book_flags = book_hier.get("flags", {})
        lib_flags = librarian_hier.get("flags", {})
        for flag_key in set(book_flags.keys()) | set(lib_flags.keys()):
            if book_flags.get(flag_key) != lib_flags.get(flag_key):
                changes.append({
                    "field": f"flags.{flag_key}",
                    "book_value": book_flags.get(flag_key),
                    "librarian_value": lib_flags.get(flag_key),
                })

        # Compare mapping count
        book_mappings = len(book_hier.get("mapping", []))
        lib_mappings = len(librarian_hier.get("mapping", []))
        if book_mappings != lib_mappings:
            changes.append({
                "field": "mapping_count",
                "book_value": book_mappings,
                "librarian_value": lib_mappings,
            })

        return changes

    # =========================================================================
    # HTTP Operations
    # =========================================================================

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request to the Librarian API."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=self.timeout,
            )

            if response.status_code >= 400:
                return {
                    "error": True,
                    "status_code": response.status_code,
                    "message": response.text,
                }

            return response.json() if response.text else {"success": True}

        except requests.exceptions.ConnectionError:
            return {"error": True, "message": "Librarian backend not reachable"}
        except requests.exceptions.Timeout:
            return {"error": True, "message": "Request timed out"}
        except Exception as e:
            return {"error": True, "message": str(e)}

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all Librarian projects."""
        result = self._request("GET", "/smart-hierarchy/projects")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get a Librarian project by ID."""
        result = self._request("GET", f"/smart-hierarchy/projects/{project_id}")
        if result.get("error"):
            return None
        return result.get("data", result)

    def list_hierarchies(self, project_id: str) -> List[Dict[str, Any]]:
        """List all hierarchies in a Librarian project."""
        result = self._request("GET", f"/smart-hierarchy/project/{project_id}")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def create_project(self, name: str, description: str = "") -> Dict[str, Any]:
        """Create a new Librarian project."""
        return self._request("POST", "/smart-hierarchy/projects", {
            "name": name,
            "description": description,
        })

    def create_hierarchy(self, hierarchy: Dict[str, Any]) -> Dict[str, Any]:
        """Create a hierarchy in Librarian."""
        return self._request("POST", "/smart-hierarchy", hierarchy)

    def update_hierarchy(
        self,
        project_id: str,
        hierarchy_id: str,
        updates: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update a hierarchy in Librarian."""
        return self._request(
            "PUT",
            f"/smart-hierarchy/project/{project_id}/{hierarchy_id}",
            updates,
        )

    def delete_hierarchy(self, project_id: str, hierarchy_id: str) -> bool:
        """Delete a hierarchy from Librarian."""
        result = self._request(
            "DELETE",
            f"/smart-hierarchy/project/{project_id}/{hierarchy_id}",
        )
        return not result.get("error", False)

    # =========================================================================
    # Sync Operations
    # =========================================================================

    def promote_book_to_librarian(
        self,
        book: Any,
        project_name: str,
        project_description: str = "",
        existing_project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Promote a Book to Librarian by creating/updating a project.

        Args:
            book: Book instance to promote
            project_name: Name for the Librarian project
            project_description: Description for the project
            existing_project_id: If provided, update existing project

        Returns:
            Result with project_id and statistics
        """
        result = {
            "success": False,
            "project_id": None,
            "created_hierarchies": 0,
            "updated_hierarchies": 0,
            "errors": [],
        }

        # Create or get project
        if existing_project_id:
            project = self.get_project(existing_project_id)
            if not project:
                result["errors"].append(f"Project {existing_project_id} not found")
                return result
            project_id = existing_project_id
        else:
            project_result = self.create_project(project_name, project_description)
            if project_result.get("error"):
                result["errors"].append(f"Failed to create project: {project_result.get('message')}")
                return result
            project_data = project_result.get("data", project_result)
            project_id = project_data.get("id")

        result["project_id"] = project_id

        # Convert Book to hierarchies
        hierarchies = self.book_to_librarian_hierarchies(book, project_id)

        # Get existing hierarchies for update detection
        existing = {h["hierarchyId"]: h for h in self.list_hierarchies(project_id)}

        # Create/update hierarchies
        for hier in hierarchies:
            hier_id = hier["hierarchyId"]
            try:
                if hier_id in existing:
                    # Update
                    update_result = self.update_hierarchy(project_id, hier_id, hier)
                    if update_result.get("error"):
                        result["errors"].append(f"Failed to update {hier_id}: {update_result.get('message')}")
                    else:
                        result["updated_hierarchies"] += 1
                else:
                    # Create
                    create_result = self.create_hierarchy(hier)
                    if create_result.get("error"):
                        result["errors"].append(f"Failed to create {hier_id}: {create_result.get('message')}")
                    else:
                        result["created_hierarchies"] += 1
            except Exception as e:
                result["errors"].append(f"Error processing {hier_id}: {str(e)}")

        result["success"] = len(result["errors"]) == 0
        return result

    def checkout_librarian_to_book(
        self,
        project_id: str,
        book_name: Optional[str] = None,
    ) -> Tuple[Any, Dict[str, Any]]:
        """
        Checkout a Librarian project to a Book for manipulation.

        Args:
            project_id: Librarian project ID
            book_name: Name for the Book (defaults to project name)

        Returns:
            Tuple of (Book instance, result dict)
        """
        result = {
            "success": False,
            "project_id": project_id,
            "hierarchy_count": 0,
            "errors": [],
        }

        # Get project info
        project = self.get_project(project_id)
        if not project:
            result["errors"].append(f"Project {project_id} not found")
            return None, result

        # Get hierarchies
        hierarchies = self.list_hierarchies(project_id)
        result["hierarchy_count"] = len(hierarchies)

        # Use project name if book_name not provided
        name = book_name or project.get("name", f"Project_{project_id}")

        # Convert to Book
        try:
            book = self.librarian_hierarchies_to_book(hierarchies, name)
            result["success"] = True
            return book, result
        except Exception as e:
            result["errors"].append(f"Conversion error: {str(e)}")
            return None, result

    def sync_book_and_librarian(
        self,
        book: Any,
        project_id: str,
        direction: str = "bidirectional",
        conflict_resolution: str = "book_wins",
    ) -> Dict[str, Any]:
        """
        Synchronize Book and Librarian project.

        Args:
            book: Book instance
            project_id: Librarian project ID
            direction: "to_librarian", "from_librarian", or "bidirectional"
            conflict_resolution: "book_wins" or "librarian_wins"

        Returns:
            Sync result with statistics
        """
        result = {
            "success": False,
            "direction": direction,
            "conflict_resolution": conflict_resolution,
            "pushed": 0,
            "pulled": 0,
            "conflicts_resolved": 0,
            "errors": [],
        }

        # Get current diff
        diff = self.diff_book_project(book, project_id)

        if direction in ("to_librarian", "bidirectional"):
            # Push Book-only items to Librarian
            book_hierarchies = self.book_to_librarian_hierarchies(book, project_id)
            book_map = {h["hierarchyId"]: h for h in book_hierarchies}

            for hier_id in diff.book_only:
                if hier_id in book_map:
                    create_result = self.create_hierarchy(book_map[hier_id])
                    if create_result.get("error"):
                        result["errors"].append(f"Failed to push {hier_id}")
                    else:
                        result["pushed"] += 1

            # Handle modified items
            for mod in diff.modified:
                hier_id = mod["hierarchy_id"]
                if hier_id in book_map:
                    if conflict_resolution == "book_wins" or direction == "to_librarian":
                        update_result = self.update_hierarchy(project_id, hier_id, book_map[hier_id])
                        if not update_result.get("error"):
                            result["conflicts_resolved"] += 1
                            result["pushed"] += 1

        if direction in ("from_librarian", "bidirectional"):
            # For bidirectional with librarian_wins, pull Librarian-only items
            # Note: This would require modifying the Book in-place, which
            # is complex. For now, we just report what would be pulled.
            result["would_pull"] = len(diff.librarian_only)

            if conflict_resolution == "librarian_wins" and diff.modified:
                result["would_update_book"] = len(diff.modified)

        result["success"] = len(result["errors"]) == 0
        return result


# Import Path for lazy imports
from pathlib import Path
