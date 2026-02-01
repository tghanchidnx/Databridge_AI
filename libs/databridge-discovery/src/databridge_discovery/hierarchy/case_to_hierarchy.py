"""
CASE Statement to Hierarchy Converter.

This module converts extracted CASE statements into structured hierarchies,
detecting parent-child relationships and building Librarian-compatible output.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from databridge_discovery.models.case_statement import (
    CaseStatement,
    CaseWhen,
    EntityType,
    ExtractedHierarchy,
    HierarchyLevel,
)


@dataclass
class HierarchyNode:
    """A node in the extracted hierarchy."""

    id: str
    name: str
    value: str
    level: int
    parent_id: str | None = None
    children: list[str] = field(default_factory=list)
    sort_order: int = 0
    source_values: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConvertedHierarchy:
    """Result of converting a CASE statement to hierarchy."""

    id: str
    name: str
    entity_type: EntityType
    nodes: dict[str, HierarchyNode]
    root_nodes: list[str]
    level_count: int
    total_nodes: int
    source_case_id: str
    source_column: str
    source_table: str | None
    mapping: dict[str, str]  # source_value -> node_id
    confidence: float
    notes: list[str] = field(default_factory=list)


class CaseToHierarchyConverter:
    """
    Converts CASE statements into structured hierarchies.

    This converter analyzes the CASE statement structure to:
    1. Identify unique result values as hierarchy nodes
    2. Detect parent-child relationships from nested CASE or patterns
    3. Infer sort orders from position and patterns
    4. Build mappings from source values to hierarchy nodes

    Example:
        converter = CaseToHierarchyConverter()

        # Convert a single CASE statement
        hierarchy = converter.convert(case_statement)

        # Convert with parent detection
        hierarchy = converter.convert_with_parent_detection(
            child_case, parent_case
        )

        # Export to Librarian format
        rows = converter.to_librarian_csv(hierarchy)
    """

    def __init__(
        self,
        infer_parents: bool = True,
        infer_sort_order: bool = True,
    ):
        """
        Initialize the converter.

        Args:
            infer_parents: Whether to try to detect parent-child relationships
            infer_sort_order: Whether to infer sort orders from patterns
        """
        self.infer_parents = infer_parents
        self.infer_sort_order = infer_sort_order

    def convert(self, case_stmt: CaseStatement) -> ConvertedHierarchy:
        """
        Convert a CASE statement to a hierarchy.

        Args:
            case_stmt: CaseStatement to convert

        Returns:
            ConvertedHierarchy
        """
        hierarchy_id = f"hier_{case_stmt.id}"
        nodes: dict[str, HierarchyNode] = {}
        mapping: dict[str, str] = {}  # source_value -> node_id
        notes: list[str] = []

        # Group WHEN clauses by result value
        result_groups = self._group_by_result(case_stmt.when_clauses)

        # Create nodes for each unique result
        sort_order = 0
        for result_value, when_clauses in result_groups.items():
            node_id = f"{hierarchy_id}_{len(nodes)}"

            # Collect source values that map to this result
            source_values = []
            for when in when_clauses:
                source_values.extend(when.condition.values)

            node = HierarchyNode(
                id=node_id,
                name=result_value,
                value=result_value,
                level=1,
                sort_order=sort_order,
                source_values=source_values,
                metadata={
                    "condition_count": len(when_clauses),
                    "pattern": case_stmt.detected_pattern,
                },
            )
            nodes[node_id] = node

            # Build mapping
            for source_val in source_values:
                mapping[source_val] = node_id

            sort_order += 1

        # Add ELSE as a node if present
        if case_stmt.else_value:
            else_node_id = f"{hierarchy_id}_else"
            nodes[else_node_id] = HierarchyNode(
                id=else_node_id,
                name=case_stmt.else_value,
                value=case_stmt.else_value,
                level=1,
                sort_order=sort_order,
                metadata={"is_else": True},
            )

        # Try to detect parent-child relationships
        if self.infer_parents:
            self._detect_parents_from_patterns(nodes, case_stmt, notes)

        # Infer sort orders if enabled
        if self.infer_sort_order:
            self._infer_sort_orders(nodes, notes)

        # Calculate level count
        levels = set(n.level for n in nodes.values())
        level_count = len(levels)

        # Root nodes are those without parents
        root_nodes = [nid for nid, n in nodes.items() if n.parent_id is None]

        # Calculate confidence
        confidence = self._calculate_confidence(case_stmt, nodes, notes)

        return ConvertedHierarchy(
            id=hierarchy_id,
            name=case_stmt.source_column,
            entity_type=case_stmt.detected_entity_type,
            nodes=nodes,
            root_nodes=root_nodes,
            level_count=level_count,
            total_nodes=len(nodes),
            source_case_id=case_stmt.id,
            source_column=case_stmt.input_column,
            source_table=case_stmt.input_table,
            mapping=mapping,
            confidence=confidence,
            notes=notes,
        )

    def convert_nested(
        self,
        parent_case: CaseStatement,
        child_case: CaseStatement,
    ) -> ConvertedHierarchy:
        """
        Convert two related CASE statements into a nested hierarchy.

        Used when one CASE's output feeds into another CASE's input.

        Args:
            parent_case: The higher-level CASE statement
            child_case: The lower-level CASE statement

        Returns:
            ConvertedHierarchy with two levels
        """
        hierarchy_id = f"hier_{parent_case.id}_{child_case.id}"
        nodes: dict[str, HierarchyNode] = {}
        mapping: dict[str, str] = {}
        notes: list[str] = ["Created from nested CASE statements"]

        # Create parent level nodes
        parent_result_groups = self._group_by_result(parent_case.when_clauses)
        parent_node_map: dict[str, str] = {}  # result_value -> node_id

        for sort_order, (result_value, when_clauses) in enumerate(parent_result_groups.items()):
            node_id = f"{hierarchy_id}_L1_{len(parent_node_map)}"
            source_values = []
            for when in when_clauses:
                source_values.extend(when.condition.values)

            node = HierarchyNode(
                id=node_id,
                name=result_value,
                value=result_value,
                level=1,
                sort_order=sort_order,
                source_values=source_values,
            )
            nodes[node_id] = node
            parent_node_map[result_value] = node_id

        # Create child level nodes with parent relationships
        child_result_groups = self._group_by_result(child_case.when_clauses)

        for sort_order, (result_value, when_clauses) in enumerate(child_result_groups.items()):
            node_id = f"{hierarchy_id}_L2_{sort_order}"
            source_values = []
            for when in when_clauses:
                source_values.extend(when.condition.values)

            # Find parent by matching source values
            parent_id = None
            for source_val in source_values:
                if source_val in parent_node_map:
                    parent_id = parent_node_map[source_val]
                    break

            node = HierarchyNode(
                id=node_id,
                name=result_value,
                value=result_value,
                level=2,
                parent_id=parent_id,
                sort_order=sort_order,
                source_values=source_values,
            )
            nodes[node_id] = node

            # Update parent's children list
            if parent_id and parent_id in nodes:
                nodes[parent_id].children.append(node_id)

            # Build mapping
            for source_val in source_values:
                mapping[source_val] = node_id

        root_nodes = [nid for nid, n in nodes.items() if n.level == 1]
        confidence = 0.8  # Higher confidence for explicit nesting

        return ConvertedHierarchy(
            id=hierarchy_id,
            name=f"{parent_case.source_column}_{child_case.source_column}",
            entity_type=parent_case.detected_entity_type,
            nodes=nodes,
            root_nodes=root_nodes,
            level_count=2,
            total_nodes=len(nodes),
            source_case_id=parent_case.id,
            source_column=child_case.input_column,
            source_table=child_case.input_table,
            mapping=mapping,
            confidence=confidence,
            notes=notes,
        )

    def _group_by_result(self, when_clauses: list[CaseWhen]) -> dict[str, list[CaseWhen]]:
        """Group WHEN clauses by their result value."""
        groups: dict[str, list[CaseWhen]] = defaultdict(list)
        for when in when_clauses:
            groups[when.result_value].append(when)
        return dict(groups)

    def _detect_parents_from_patterns(
        self,
        nodes: dict[str, HierarchyNode],
        case_stmt: CaseStatement,
        notes: list[str],
    ) -> None:
        """Try to detect parent-child relationships from value patterns."""
        if case_stmt.detected_pattern != "prefix":
            return

        # Sort nodes by their source value prefixes
        node_list = list(nodes.values())

        # Find potential parent-child based on prefix containment
        for node in node_list:
            if not node.source_values:
                continue

            # Get the shortest prefix for this node
            shortest_prefix = min(
                (v.rstrip('%').rstrip('_') for v in node.source_values if v),
                key=len,
                default="",
            )

            if not shortest_prefix:
                continue

            # Find potential parent (a node whose prefix contains this one)
            for other_node in node_list:
                if other_node.id == node.id:
                    continue
                if node.parent_id:
                    break

                other_prefixes = [
                    v.rstrip('%').rstrip('_')
                    for v in other_node.source_values
                    if v
                ]

                for other_prefix in other_prefixes:
                    if (
                        shortest_prefix.startswith(other_prefix)
                        and len(shortest_prefix) > len(other_prefix)
                    ):
                        node.parent_id = other_node.id
                        node.level = other_node.level + 1
                        other_node.children.append(node.id)
                        notes.append(f"Detected parent-child: {other_node.name} -> {node.name}")
                        break

    def _infer_sort_orders(
        self,
        nodes: dict[str, HierarchyNode],
        notes: list[str],
    ) -> None:
        """Infer sort orders from patterns in values."""
        # Group nodes by level
        by_level: dict[int, list[HierarchyNode]] = defaultdict(list)
        for node in nodes.values():
            by_level[node.level].append(node)

        for level, level_nodes in by_level.items():
            # Try numeric sort
            numeric_nodes = []
            for node in level_nodes:
                prefix = self._extract_numeric_prefix(node.value)
                if prefix is not None:
                    numeric_nodes.append((prefix, node))

            if len(numeric_nodes) == len(level_nodes):
                # All nodes have numeric prefixes - sort by them
                numeric_nodes.sort(key=lambda x: x[0])
                for idx, (_, node) in enumerate(numeric_nodes):
                    node.sort_order = idx
                notes.append(f"Applied numeric sort to level {level}")
            else:
                # Fall back to alphabetical
                level_nodes.sort(key=lambda n: n.value.lower())
                for idx, node in enumerate(level_nodes):
                    node.sort_order = idx

    def _extract_numeric_prefix(self, value: str) -> int | None:
        """Extract leading numeric prefix from a value."""
        import re
        match = re.match(r'^(\d+)', value)
        if match:
            return int(match.group(1))
        return None

    def _calculate_confidence(
        self,
        case_stmt: CaseStatement,
        nodes: dict[str, HierarchyNode],
        notes: list[str],
    ) -> float:
        """Calculate confidence score for the conversion."""
        confidence = 0.5  # Base

        # More conditions = higher confidence
        if case_stmt.condition_count >= 10:
            confidence += 0.15
        elif case_stmt.condition_count >= 5:
            confidence += 0.1

        # Known entity type
        if case_stmt.detected_entity_type != EntityType.UNKNOWN:
            confidence += 0.15

        # Consistent pattern
        if case_stmt.detected_pattern:
            confidence += 0.1

        # Parent-child relationships found
        parent_count = sum(1 for n in nodes.values() if n.parent_id)
        if parent_count > 0:
            confidence += 0.1

        return min(confidence, 1.0)

    def to_extracted_hierarchy(self, converted: ConvertedHierarchy) -> ExtractedHierarchy:
        """
        Convert to ExtractedHierarchy model.

        Args:
            converted: ConvertedHierarchy to convert

        Returns:
            ExtractedHierarchy
        """
        # Group nodes by level
        by_level: dict[int, list[HierarchyNode]] = defaultdict(list)
        for node in converted.nodes.values():
            by_level[node.level].append(node)

        levels: list[HierarchyLevel] = []
        for level_num in sorted(by_level.keys()):
            level_nodes = by_level[level_num]
            level_nodes.sort(key=lambda n: n.sort_order)

            levels.append(HierarchyLevel(
                level_number=level_num,
                level_name=f"Level {level_num}",
                values=[n.value for n in level_nodes],
                parent_level=level_num - 1 if level_num > 1 else None,
                sort_order_map={n.value: n.sort_order for n in level_nodes},
            ))

        # Build value_to_node mapping
        value_to_node: dict[str, dict[str, Any]] = {}
        for source_val, node_id in converted.mapping.items():
            node = converted.nodes[node_id]
            value_to_node[source_val] = {
                "result": node.value,
                "level": node.level,
                "node_id": node_id,
            }

        return ExtractedHierarchy(
            id=converted.id,
            name=converted.name,
            source_case_id=converted.source_case_id,
            entity_type=converted.entity_type,
            levels=levels,
            total_levels=converted.level_count,
            total_nodes=converted.total_nodes,
            value_to_node=value_to_node,
            source_column=converted.source_column,
            source_table=converted.source_table,
            confidence_score=converted.confidence,
            confidence_notes=converted.notes,
        )

    def to_librarian_hierarchy_csv(
        self,
        converted: ConvertedHierarchy,
        hierarchy_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Convert to Librarian HIERARCHY.CSV format.

        Args:
            converted: ConvertedHierarchy to export
            hierarchy_name: Optional override for hierarchy name

        Returns:
            List of row dictionaries for CSV export
        """
        rows: list[dict[str, Any]] = []
        hier_name = hierarchy_name or converted.name

        # Sort nodes by level then sort_order
        sorted_nodes = sorted(
            converted.nodes.values(),
            key=lambda n: (n.level, n.sort_order),
        )

        for node in sorted_nodes:
            row: dict[str, Any] = {
                "HIERARCHY_ID": node.id,
                "HIERARCHY_NAME": hier_name,
                "PARENT_ID": node.parent_id,
                "DESCRIPTION": "",
                "INCLUDE_FLAG": True,
                "EXCLUDE_FLAG": False,
                "FORMULA_GROUP": None,
                "SORT_ORDER": node.sort_order,
            }

            # Add level columns (up to 10 levels)
            for i in range(1, 11):
                if i == node.level:
                    row[f"LEVEL_{i}"] = node.value
                    row[f"LEVEL_{i}_SORT"] = node.sort_order
                else:
                    row[f"LEVEL_{i}"] = None
                    row[f"LEVEL_{i}_SORT"] = None

            rows.append(row)

        return rows

    def to_librarian_mapping_csv(
        self,
        converted: ConvertedHierarchy,
        source_database: str = "",
        source_schema: str = "",
        source_table: str = "",
        source_column: str = "",
    ) -> list[dict[str, Any]]:
        """
        Convert to Librarian MAPPING.CSV format.

        Args:
            converted: ConvertedHierarchy to export
            source_database: Database name for mapping
            source_schema: Schema name for mapping
            source_table: Table name for mapping
            source_column: Column name for mapping

        Returns:
            List of row dictionaries for CSV export
        """
        rows: list[dict[str, Any]] = []

        # Use provided values or fall back to converted values
        table = source_table or converted.source_table or ""
        column = source_column or converted.source_column or ""

        mapping_index = 0
        for source_value, node_id in converted.mapping.items():
            row = {
                "HIERARCHY_ID": node_id,
                "MAPPING_INDEX": mapping_index,
                "SOURCE_DATABASE": source_database,
                "SOURCE_SCHEMA": source_schema,
                "SOURCE_TABLE": table,
                "SOURCE_COLUMN": column,
                "SOURCE_UID": source_value,
                "PRECEDENCE_GROUP": 1,
                "INCLUDE_FLAG": True,
                "EXCLUDE_FLAG": False,
            }
            rows.append(row)
            mapping_index += 1

        return rows

    def convert_multiple(
        self,
        case_statements: list[CaseStatement],
    ) -> list[ConvertedHierarchy]:
        """
        Convert multiple CASE statements to hierarchies.

        Also detects and handles nested relationships.

        Args:
            case_statements: List of CaseStatement to convert

        Returns:
            List of ConvertedHierarchy
        """
        results: list[ConvertedHierarchy] = []

        # First, detect nested relationships
        from databridge_discovery.parser.case_extractor import CaseExtractor
        extractor = CaseExtractor()
        nested_pairs = extractor.find_nested_hierarchies(case_statements)

        # Track which cases are part of nested pairs
        nested_case_ids = set()
        for parent, child in nested_pairs:
            nested_case_ids.add(parent.id)
            nested_case_ids.add(child.id)

            # Convert as nested
            converted = self.convert_nested(parent, child)
            results.append(converted)

        # Convert remaining cases individually
        for case_stmt in case_statements:
            if case_stmt.id not in nested_case_ids:
                converted = self.convert(case_stmt)
                results.append(converted)

        return results
