"""
Tree operations for DataBridge AI V3.

Provides tree building and navigation utilities for hierarchies.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Iterator, Callable
from collections import deque


@dataclass
class HierarchyNode:
    """
    Represents a node in the hierarchy tree.

    Attributes:
        hierarchy_id: Unique identifier.
        hierarchy_name: Display name.
        parent_id: Parent node ID (None for root).
        children: List of child nodes.
        data: Additional data from the hierarchy record.
        depth: Depth in the tree (0 for root).
        path: List of ancestor IDs from root to this node.
    """

    hierarchy_id: str
    hierarchy_name: str
    parent_id: Optional[str] = None
    children: List["HierarchyNode"] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)
    depth: int = 0
    path: List[str] = field(default_factory=list)

    def is_root(self) -> bool:
        """Check if this is a root node."""
        return self.parent_id is None

    def is_leaf(self) -> bool:
        """Check if this is a leaf node (no children)."""
        return len(self.children) == 0

    def child_count(self) -> int:
        """Get number of direct children."""
        return len(self.children)

    def descendant_count(self) -> int:
        """Get total number of descendants."""
        count = len(self.children)
        for child in self.children:
            count += child.descendant_count()
        return count

    def get_level_path(self) -> List[str]:
        """Get the level values as a path."""
        levels = []
        for i in range(1, 16):
            level_key = f"level_{i}"
            value = self.data.get(level_key)
            if value:
                levels.append(value)
            else:
                break
        return levels

    def to_dict(self, include_children: bool = True) -> Dict[str, Any]:
        """
        Convert node to dictionary.

        Args:
            include_children: Include children recursively.

        Returns:
            Dictionary representation.
        """
        result = {
            "hierarchy_id": self.hierarchy_id,
            "hierarchy_name": self.hierarchy_name,
            "parent_id": self.parent_id,
            "depth": self.depth,
            "path": self.path,
            "is_leaf": self.is_leaf(),
            "child_count": self.child_count(),
            **self.data,
        }

        if include_children and self.children:
            result["children"] = [
                child.to_dict(include_children=True) for child in self.children
            ]

        return result


class TreeBuilder:
    """
    Builds a tree structure from flat hierarchy data.

    Can work with:
    - List of dictionaries
    - List of Hierarchy model objects
    - Query results
    """

    def __init__(
        self,
        id_field: str = "hierarchy_id",
        parent_field: str = "parent_id",
        name_field: str = "hierarchy_name",
    ):
        """
        Initialize the tree builder.

        Args:
            id_field: Field name for hierarchy ID.
            parent_field: Field name for parent ID.
            name_field: Field name for display name.
        """
        self.id_field = id_field
        self.parent_field = parent_field
        self.name_field = name_field

    def build(self, items: List[Any]) -> List[HierarchyNode]:
        """
        Build a tree from flat items.

        Args:
            items: List of hierarchy items (dicts or objects).

        Returns:
            List of root nodes.
        """
        # Convert items to dictionaries
        item_dicts = []
        for item in items:
            if hasattr(item, "__dict__"):
                # SQLAlchemy model or similar
                item_dict = {}
                for key in dir(item):
                    if not key.startswith("_") and not callable(getattr(item, key)):
                        try:
                            item_dict[key] = getattr(item, key)
                        except Exception:
                            pass
                item_dicts.append(item_dict)
            elif isinstance(item, dict):
                item_dicts.append(item)
            else:
                raise ValueError(f"Unsupported item type: {type(item)}")

        # Build node lookup
        nodes: Dict[str, HierarchyNode] = {}
        for item_dict in item_dicts:
            hierarchy_id = item_dict.get(self.id_field)
            parent_id = item_dict.get(self.parent_field)
            name = item_dict.get(self.name_field, hierarchy_id)

            # Extract data (exclude id, parent, and name fields)
            data = {
                k: v
                for k, v in item_dict.items()
                if k not in (self.id_field, self.parent_field, self.name_field)
            }

            node = HierarchyNode(
                hierarchy_id=hierarchy_id,
                hierarchy_name=name,
                parent_id=parent_id,
                data=data,
            )
            nodes[hierarchy_id] = node

        # Build parent-child relationships
        roots: List[HierarchyNode] = []
        for node in nodes.values():
            if node.parent_id and node.parent_id in nodes:
                parent = nodes[node.parent_id]
                parent.children.append(node)
            else:
                roots.append(node)

        # Sort children by sort_order if available
        for node in nodes.values():
            node.children.sort(
                key=lambda n: (n.data.get("sort_order", 0), n.hierarchy_name)
            )

        # Calculate depth and path
        self._calculate_depth_and_path(roots)

        # Sort roots
        roots.sort(key=lambda n: (n.data.get("sort_order", 0), n.hierarchy_name))

        return roots

    def _calculate_depth_and_path(
        self,
        nodes: List[HierarchyNode],
        depth: int = 0,
        path: List[str] = None,
    ) -> None:
        """Recursively calculate depth and path for nodes."""
        if path is None:
            path = []

        for node in nodes:
            node.depth = depth
            node.path = path.copy()
            if node.children:
                child_path = path + [node.hierarchy_id]
                self._calculate_depth_and_path(node.children, depth + 1, child_path)

    def build_single(self, items: List[Any], root_id: str) -> Optional[HierarchyNode]:
        """
        Build a subtree from a specific root.

        Args:
            items: List of hierarchy items.
            root_id: ID of the root node for the subtree.

        Returns:
            Root node of the subtree, or None if not found.
        """
        all_roots = self.build(items)

        # Find the specific root (might be nested)
        def find_node(nodes: List[HierarchyNode], target_id: str) -> Optional[HierarchyNode]:
            for node in nodes:
                if node.hierarchy_id == target_id:
                    return node
                found = find_node(node.children, target_id)
                if found:
                    return found
            return None

        return find_node(all_roots, root_id)


class TreeNavigator:
    """
    Provides navigation and traversal utilities for hierarchy trees.
    """

    def __init__(self, roots: List[HierarchyNode]):
        """
        Initialize the navigator with root nodes.

        Args:
            roots: List of root nodes.
        """
        self.roots = roots
        self._node_index: Dict[str, HierarchyNode] = {}
        self._build_index(roots)

    def _build_index(self, nodes: List[HierarchyNode]) -> None:
        """Build an index of all nodes by ID."""
        for node in nodes:
            self._node_index[node.hierarchy_id] = node
            if node.children:
                self._build_index(node.children)

    def get_node(self, hierarchy_id: str) -> Optional[HierarchyNode]:
        """
        Get a node by ID.

        Args:
            hierarchy_id: Node ID.

        Returns:
            Node if found, None otherwise.
        """
        return self._node_index.get(hierarchy_id)

    def get_parent(self, hierarchy_id: str) -> Optional[HierarchyNode]:
        """
        Get the parent of a node.

        Args:
            hierarchy_id: Node ID.

        Returns:
            Parent node if exists, None otherwise.
        """
        node = self.get_node(hierarchy_id)
        if node and node.parent_id:
            return self.get_node(node.parent_id)
        return None

    def get_children(self, hierarchy_id: str) -> List[HierarchyNode]:
        """
        Get direct children of a node.

        Args:
            hierarchy_id: Node ID.

        Returns:
            List of child nodes.
        """
        node = self.get_node(hierarchy_id)
        return node.children if node else []

    def get_siblings(self, hierarchy_id: str) -> List[HierarchyNode]:
        """
        Get siblings of a node (excluding itself).

        Args:
            hierarchy_id: Node ID.

        Returns:
            List of sibling nodes.
        """
        node = self.get_node(hierarchy_id)
        if not node:
            return []

        if node.parent_id:
            parent = self.get_node(node.parent_id)
            if parent:
                return [c for c in parent.children if c.hierarchy_id != hierarchy_id]
        else:
            # Root level siblings
            return [r for r in self.roots if r.hierarchy_id != hierarchy_id]

        return []

    def get_ancestors(self, hierarchy_id: str) -> List[HierarchyNode]:
        """
        Get all ancestors of a node (from parent to root).

        Args:
            hierarchy_id: Node ID.

        Returns:
            List of ancestor nodes.
        """
        ancestors = []
        node = self.get_node(hierarchy_id)

        while node and node.parent_id:
            parent = self.get_node(node.parent_id)
            if parent:
                ancestors.append(parent)
                node = parent
            else:
                break

        return ancestors

    def get_path_to_root(self, hierarchy_id: str) -> List[HierarchyNode]:
        """
        Get path from node to root (including the node itself).

        Args:
            hierarchy_id: Node ID.

        Returns:
            List of nodes from the specified node to root.
        """
        node = self.get_node(hierarchy_id)
        if not node:
            return []

        path = [node]
        path.extend(self.get_ancestors(hierarchy_id))
        return path

    def get_descendants(self, hierarchy_id: str) -> List[HierarchyNode]:
        """
        Get all descendants of a node.

        Args:
            hierarchy_id: Node ID.

        Returns:
            List of all descendant nodes.
        """
        node = self.get_node(hierarchy_id)
        if not node:
            return []

        descendants = []
        queue = deque(node.children)

        while queue:
            current = queue.popleft()
            descendants.append(current)
            queue.extend(current.children)

        return descendants

    def get_leaves(self, hierarchy_id: Optional[str] = None) -> List[HierarchyNode]:
        """
        Get all leaf nodes under a node (or all leaves if no ID specified).

        Args:
            hierarchy_id: Optional root node ID.

        Returns:
            List of leaf nodes.
        """
        if hierarchy_id:
            node = self.get_node(hierarchy_id)
            if not node:
                return []
            if node.is_leaf():
                return [node]
            descendants = self.get_descendants(hierarchy_id)
            return [d for d in descendants if d.is_leaf()]
        else:
            # All leaves in the tree
            return [n for n in self._node_index.values() if n.is_leaf()]

    def get_depth(self, hierarchy_id: str) -> int:
        """
        Get the depth of a node in the tree.

        Args:
            hierarchy_id: Node ID.

        Returns:
            Depth (0 for root), -1 if not found.
        """
        node = self.get_node(hierarchy_id)
        return node.depth if node else -1

    def get_max_depth(self) -> int:
        """
        Get the maximum depth of the tree.

        Returns:
            Maximum depth.
        """
        if not self._node_index:
            return 0
        return max(n.depth for n in self._node_index.values())

    def traverse_breadth_first(
        self,
        start_id: Optional[str] = None,
    ) -> Iterator[HierarchyNode]:
        """
        Traverse the tree breadth-first.

        Args:
            start_id: Optional starting node ID.

        Yields:
            Nodes in breadth-first order.
        """
        if start_id:
            start_node = self.get_node(start_id)
            if not start_node:
                return
            queue = deque([start_node])
        else:
            queue = deque(self.roots)

        while queue:
            node = queue.popleft()
            yield node
            queue.extend(node.children)

    def traverse_depth_first(
        self,
        start_id: Optional[str] = None,
        pre_order: bool = True,
    ) -> Iterator[HierarchyNode]:
        """
        Traverse the tree depth-first.

        Args:
            start_id: Optional starting node ID.
            pre_order: If True, visit parent before children.

        Yields:
            Nodes in depth-first order.
        """
        if start_id:
            start_node = self.get_node(start_id)
            if not start_node:
                return
            nodes = [start_node]
        else:
            nodes = self.roots

        def visit(node: HierarchyNode) -> Iterator[HierarchyNode]:
            if pre_order:
                yield node
            for child in node.children:
                yield from visit(child)
            if not pre_order:
                yield node

        for node in nodes:
            yield from visit(node)

    def find_nodes(
        self,
        predicate: Callable[[HierarchyNode], bool],
    ) -> List[HierarchyNode]:
        """
        Find nodes matching a predicate.

        Args:
            predicate: Function that returns True for matching nodes.

        Returns:
            List of matching nodes.
        """
        return [n for n in self._node_index.values() if predicate(n)]

    def search(
        self,
        query: str,
        fields: List[str] = None,
    ) -> List[HierarchyNode]:
        """
        Search for nodes by name or other fields.

        Args:
            query: Search query (case-insensitive).
            fields: Fields to search (default: hierarchy_name, hierarchy_id).

        Returns:
            List of matching nodes.
        """
        if fields is None:
            fields = ["hierarchy_name", "hierarchy_id"]

        query_lower = query.lower()
        results = []

        for node in self._node_index.values():
            for field in fields:
                if field == "hierarchy_name":
                    value = node.hierarchy_name
                elif field == "hierarchy_id":
                    value = node.hierarchy_id
                else:
                    value = node.data.get(field, "")

                if value and query_lower in str(value).lower():
                    results.append(node)
                    break

        return results

    def to_flat_list(
        self,
        include_data: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Convert tree to a flat list of dictionaries.

        Args:
            include_data: Include additional data fields.

        Returns:
            List of node dictionaries.
        """
        result = []
        for node in self.traverse_depth_first():
            item = {
                "hierarchy_id": node.hierarchy_id,
                "hierarchy_name": node.hierarchy_name,
                "parent_id": node.parent_id,
                "depth": node.depth,
                "is_leaf": node.is_leaf(),
            }
            if include_data:
                item.update(node.data)
            result.append(item)
        return result

    def to_nested_dict(self) -> List[Dict[str, Any]]:
        """
        Convert tree to nested dictionary structure.

        Returns:
            List of root node dictionaries with nested children.
        """
        return [root.to_dict(include_children=True) for root in self.roots]

    def validate_tree(self) -> List[str]:
        """
        Validate tree integrity.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors = []

        # Check for orphaned nodes
        all_ids = set(self._node_index.keys())
        for node in self._node_index.values():
            if node.parent_id and node.parent_id not in all_ids:
                errors.append(
                    f"Orphaned node: {node.hierarchy_id} references missing parent {node.parent_id}"
                )

        # Check for duplicate IDs (shouldn't happen if index built correctly)
        # Already handled by dict

        # Check for circular references (depth should be finite)
        for node in self._node_index.values():
            if node.depth > 100:  # Arbitrary max depth
                errors.append(f"Possible circular reference at: {node.hierarchy_id}")

        return errors
