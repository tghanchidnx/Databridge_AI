"""
Hierarchy Merger for combining discovered hierarchies with existing Librarian hierarchies.

This module provides tools to merge, compare, and reconcile hierarchies
from different sources.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy, HierarchyNode


class MergeStrategy(str, Enum):
    """Strategy for handling merge conflicts."""

    KEEP_EXISTING = "keep_existing"
    PREFER_NEW = "prefer_new"
    MERGE_BOTH = "merge_both"
    INTERACTIVE = "interactive"


class ConflictType(str, Enum):
    """Types of conflicts during merge."""

    DUPLICATE_VALUE = "duplicate_value"
    DIFFERENT_PARENT = "different_parent"
    DIFFERENT_LEVEL = "different_level"
    MISSING_IN_SOURCE = "missing_in_source"
    MISSING_IN_TARGET = "missing_in_target"


@dataclass
class MergeConflict:
    """A conflict detected during merge."""

    conflict_type: ConflictType
    value: str
    source_info: dict[str, Any]
    target_info: dict[str, Any]
    resolution: str | None = None
    resolved: bool = False


@dataclass
class MergeResult:
    """Result of a hierarchy merge operation."""

    success: bool
    merged_hierarchy: ConvertedHierarchy | None
    conflicts: list[MergeConflict]
    additions: list[str]  # Values added
    updates: list[str]  # Values updated
    deletions: list[str]  # Values removed (if applicable)
    notes: list[str] = field(default_factory=list)


@dataclass
class ComparisonResult:
    """Result of comparing two hierarchies."""

    are_equal: bool
    differences: list[dict[str, Any]]
    source_only: list[str]  # Values only in source
    target_only: list[str]  # Values only in target
    common_values: list[str]  # Values in both
    structural_differences: list[dict[str, Any]]
    notes: list[str] = field(default_factory=list)


class HierarchyMerger:
    """
    Merges discovered hierarchies with existing Librarian hierarchies.

    Provides comparison, conflict detection, and merge capabilities
    for combining hierarchies from different sources.

    Example:
        merger = HierarchyMerger()

        # Compare two hierarchies
        comparison = merger.compare(source_hier, target_hier)

        # Merge hierarchies
        result = merger.merge(source_hier, target_hier)

        # Merge with specific strategy
        result = merger.merge(
            source_hier,
            target_hier,
            strategy=MergeStrategy.PREFER_NEW
        )
    """

    def __init__(
        self,
        strategy: MergeStrategy = MergeStrategy.MERGE_BOTH,
        ignore_case: bool = True,
    ):
        """
        Initialize the merger.

        Args:
            strategy: Default merge strategy
            ignore_case: Ignore case when comparing values
        """
        self.strategy = strategy
        self.ignore_case = ignore_case

    def compare(
        self,
        source: ConvertedHierarchy,
        target: ConvertedHierarchy,
    ) -> ComparisonResult:
        """
        Compare two hierarchies.

        Args:
            source: Source hierarchy (e.g., discovered)
            target: Target hierarchy (e.g., existing Librarian)

        Returns:
            ComparisonResult
        """
        differences: list[dict[str, Any]] = []
        structural_diffs: list[dict[str, Any]] = []
        notes: list[str] = []

        # Get all values from both hierarchies
        source_values = self._get_all_values(source)
        target_values = self._get_all_values(target)

        # Normalize for comparison if ignoring case
        if self.ignore_case:
            source_normalized = {v.lower(): v for v in source_values}
            target_normalized = {v.lower(): v for v in target_values}
            source_keys = set(source_normalized.keys())
            target_keys = set(target_normalized.keys())
        else:
            source_keys = set(source_values)
            target_keys = set(target_values)
            source_normalized = {v: v for v in source_values}
            target_normalized = {v: v for v in target_values}

        # Find differences
        source_only = source_keys - target_keys
        target_only = target_keys - source_keys
        common = source_keys & target_keys

        # Check structural differences for common values
        for key in common:
            source_val = source_normalized[key]
            target_val = target_normalized[key]

            source_node = self._find_node_by_value(source, source_val)
            target_node = self._find_node_by_value(target, target_val)

            if source_node and target_node:
                # Check level
                if source_node.level != target_node.level:
                    structural_diffs.append({
                        "value": source_val,
                        "type": "level_mismatch",
                        "source_level": source_node.level,
                        "target_level": target_node.level,
                    })

                # Check parent
                source_parent = self._get_parent_value(source, source_node)
                target_parent = self._get_parent_value(target, target_node)
                if source_parent != target_parent:
                    structural_diffs.append({
                        "value": source_val,
                        "type": "parent_mismatch",
                        "source_parent": source_parent,
                        "target_parent": target_parent,
                    })

                # Check sort order
                if source_node.sort_order != target_node.sort_order:
                    differences.append({
                        "value": source_val,
                        "type": "sort_order",
                        "source_order": source_node.sort_order,
                        "target_order": target_node.sort_order,
                    })

        # Summary
        are_equal = (
            len(source_only) == 0
            and len(target_only) == 0
            and len(structural_diffs) == 0
        )

        if source_only:
            notes.append(f"{len(source_only)} values only in source")
        if target_only:
            notes.append(f"{len(target_only)} values only in target")
        if structural_diffs:
            notes.append(f"{len(structural_diffs)} structural differences")

        return ComparisonResult(
            are_equal=are_equal,
            differences=differences,
            source_only=[source_normalized[k] for k in source_only],
            target_only=[target_normalized[k] for k in target_only],
            common_values=[source_normalized[k] for k in common],
            structural_differences=structural_diffs,
            notes=notes,
        )

    def merge(
        self,
        source: ConvertedHierarchy,
        target: ConvertedHierarchy,
        strategy: MergeStrategy | None = None,
        resolve_conflicts: dict[str, str] | None = None,
    ) -> MergeResult:
        """
        Merge source hierarchy into target.

        Args:
            source: Source hierarchy to merge from
            target: Target hierarchy to merge into
            strategy: Merge strategy (uses default if None)
            resolve_conflicts: Pre-resolved conflicts (value -> resolution)

        Returns:
            MergeResult
        """
        strategy = strategy or self.strategy
        resolve_conflicts = resolve_conflicts or {}
        conflicts: list[MergeConflict] = []
        additions: list[str] = []
        updates: list[str] = []
        notes: list[str] = []

        # Compare first
        comparison = self.compare(source, target)

        # Create merged hierarchy based on target
        merged_nodes = {nid: self._copy_node(n) for nid, n in target.nodes.items()}
        merged_mapping = dict(target.mapping)

        # Handle values only in source (additions)
        for value in comparison.source_only:
            source_node = self._find_node_by_value(source, value)
            if source_node:
                new_node_id = f"merged_{len(merged_nodes)}"
                new_node = self._copy_node(source_node)
                new_node.id = new_node_id

                # Try to find parent in merged hierarchy
                source_parent = self._get_parent_value(source, source_node)
                if source_parent:
                    parent_node = self._find_node_by_value_in_dict(merged_nodes, source_parent)
                    if parent_node:
                        new_node.parent_id = parent_node.id
                        parent_node.children.append(new_node_id)

                merged_nodes[new_node_id] = new_node
                additions.append(value)

                # Update mapping
                for src_val in source_node.source_values:
                    merged_mapping[src_val] = new_node_id

        # Handle values only in target (keep them)
        for value in comparison.target_only:
            if strategy == MergeStrategy.PREFER_NEW:
                # Could mark for deletion, but typically keep
                pass
            # Otherwise keep as-is

        # Handle structural differences
        for diff in comparison.structural_differences:
            value = diff["value"]
            conflict = MergeConflict(
                conflict_type=ConflictType.DIFFERENT_LEVEL if diff["type"] == "level_mismatch" else ConflictType.DIFFERENT_PARENT,
                value=value,
                source_info=diff,
                target_info=diff,
            )

            # Check for pre-resolution
            if value in resolve_conflicts:
                conflict.resolution = resolve_conflicts[value]
                conflict.resolved = True

                # Apply resolution
                if conflict.resolution == "use_source":
                    source_node = self._find_node_by_value(source, value)
                    target_node = self._find_node_by_value_in_dict(merged_nodes, value)
                    if source_node and target_node:
                        target_node.level = source_node.level
                        target_node.parent_id = source_node.parent_id
                        updates.append(value)

            else:
                # Apply strategy
                if strategy == MergeStrategy.PREFER_NEW:
                    source_node = self._find_node_by_value(source, value)
                    target_node = self._find_node_by_value_in_dict(merged_nodes, value)
                    if source_node and target_node:
                        target_node.level = source_node.level
                        updates.append(value)
                        conflict.resolved = True
                        conflict.resolution = "used_source"
                elif strategy == MergeStrategy.KEEP_EXISTING:
                    conflict.resolved = True
                    conflict.resolution = "kept_target"
                else:
                    conflicts.append(conflict)

        # Create merged hierarchy
        root_nodes = [nid for nid, n in merged_nodes.items() if n.parent_id is None]

        merged = ConvertedHierarchy(
            id=f"merged_{target.id}",
            name=target.name,
            entity_type=target.entity_type,
            nodes=merged_nodes,
            root_nodes=root_nodes,
            level_count=max((n.level for n in merged_nodes.values()), default=0),
            total_nodes=len(merged_nodes),
            source_case_id=source.source_case_id,
            source_column=target.source_column,
            source_table=target.source_table,
            mapping=merged_mapping,
            confidence=min(source.confidence, target.confidence),
            notes=notes,
        )

        success = len([c for c in conflicts if not c.resolved]) == 0

        return MergeResult(
            success=success,
            merged_hierarchy=merged,
            conflicts=conflicts,
            additions=additions,
            updates=updates,
            deletions=[],
            notes=notes,
        )

    def merge_from_librarian_csv(
        self,
        source: ConvertedHierarchy,
        hierarchy_rows: list[dict[str, Any]],
        mapping_rows: list[dict[str, Any]] | None = None,
    ) -> MergeResult:
        """
        Merge discovered hierarchy with Librarian CSV data.

        Args:
            source: Discovered hierarchy
            hierarchy_rows: Rows from Librarian HIERARCHY.CSV
            mapping_rows: Rows from Librarian MAPPING.CSV (optional)

        Returns:
            MergeResult
        """
        # Convert Librarian CSV to ConvertedHierarchy
        target = self._from_librarian_csv(hierarchy_rows, mapping_rows)

        if target is None:
            return MergeResult(
                success=False,
                merged_hierarchy=None,
                conflicts=[],
                additions=[],
                updates=[],
                deletions=[],
                notes=["Failed to parse Librarian CSV data"],
            )

        return self.merge(source, target)

    def _from_librarian_csv(
        self,
        hierarchy_rows: list[dict[str, Any]],
        mapping_rows: list[dict[str, Any]] | None = None,
    ) -> ConvertedHierarchy | None:
        """Convert Librarian CSV data to ConvertedHierarchy."""
        if not hierarchy_rows:
            return None

        nodes: dict[str, HierarchyNode] = {}
        mapping: dict[str, str] = {}

        for row in hierarchy_rows:
            node_id = str(row.get("HIERARCHY_ID", ""))
            if not node_id:
                continue

            # Find value from level columns
            value = None
            level = 0
            for i in range(1, 11):
                level_val = row.get(f"LEVEL_{i}")
                if level_val:
                    value = str(level_val)
                    level = i
                    break

            if not value:
                continue

            parent_id = row.get("PARENT_ID")
            sort_order = row.get("SORT_ORDER", 0) or row.get(f"LEVEL_{level}_SORT", 0)

            nodes[node_id] = HierarchyNode(
                id=node_id,
                name=value,
                value=value,
                level=level,
                parent_id=str(parent_id) if parent_id else None,
                sort_order=int(sort_order) if sort_order else 0,
            )

        # Build children lists
        for node in nodes.values():
            if node.parent_id and node.parent_id in nodes:
                nodes[node.parent_id].children.append(node.id)

        # Process mapping rows
        if mapping_rows:
            for row in mapping_rows:
                hier_id = str(row.get("HIERARCHY_ID", ""))
                source_uid = row.get("SOURCE_UID")
                if hier_id and source_uid:
                    mapping[str(source_uid)] = hier_id

        root_nodes = [nid for nid, n in nodes.items() if n.parent_id is None]

        return ConvertedHierarchy(
            id="librarian_imported",
            name=hierarchy_rows[0].get("HIERARCHY_NAME", "Imported"),
            entity_type=hierarchy_rows[0].get("entity_type", "unknown"),
            nodes=nodes,
            root_nodes=root_nodes,
            level_count=max((n.level for n in nodes.values()), default=0),
            total_nodes=len(nodes),
            source_case_id="",
            source_column="",
            source_table=None,
            mapping=mapping,
            confidence=1.0,
        )

    def _get_all_values(self, hierarchy: ConvertedHierarchy) -> list[str]:
        """Get all values from a hierarchy."""
        return [n.value for n in hierarchy.nodes.values()]

    def _find_node_by_value(
        self,
        hierarchy: ConvertedHierarchy,
        value: str,
    ) -> HierarchyNode | None:
        """Find a node by its value."""
        return self._find_node_by_value_in_dict(hierarchy.nodes, value)

    def _find_node_by_value_in_dict(
        self,
        nodes: dict[str, HierarchyNode],
        value: str,
    ) -> HierarchyNode | None:
        """Find a node by value in a dict."""
        for node in nodes.values():
            if self.ignore_case:
                if node.value.lower() == value.lower():
                    return node
            else:
                if node.value == value:
                    return node
        return None

    def _get_parent_value(
        self,
        hierarchy: ConvertedHierarchy,
        node: HierarchyNode,
    ) -> str | None:
        """Get the parent's value for a node."""
        if not node.parent_id:
            return None
        parent = hierarchy.nodes.get(node.parent_id)
        return parent.value if parent else None

    def _copy_node(self, node: HierarchyNode) -> HierarchyNode:
        """Create a copy of a node."""
        return HierarchyNode(
            id=node.id,
            name=node.name,
            value=node.value,
            level=node.level,
            parent_id=node.parent_id,
            children=list(node.children),
            sort_order=node.sort_order,
            source_values=list(node.source_values),
            metadata=dict(node.metadata),
        )

    def suggest_mappings(
        self,
        source: ConvertedHierarchy,
        target: ConvertedHierarchy,
        threshold: float = 0.8,
    ) -> list[tuple[str, str, float]]:
        """
        Suggest value mappings between hierarchies.

        Uses fuzzy matching to find potential matches.

        Args:
            source: Source hierarchy
            target: Target hierarchy
            threshold: Minimum similarity threshold

        Returns:
            List of (source_value, target_value, similarity) tuples
        """
        try:
            from rapidfuzz import fuzz
        except ImportError:
            return []

        suggestions: list[tuple[str, str, float]] = []
        source_values = self._get_all_values(source)
        target_values = self._get_all_values(target)

        for src_val in source_values:
            best_match = None
            best_score = 0.0

            for tgt_val in target_values:
                score = fuzz.ratio(src_val.lower(), tgt_val.lower()) / 100.0
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = tgt_val

            if best_match:
                suggestions.append((src_val, best_match, best_score))

        suggestions.sort(key=lambda x: x[2], reverse=True)
        return suggestions
