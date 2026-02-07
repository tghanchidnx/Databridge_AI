"""
Impact Analyzer.

Analyzes the impact of changes to hierarchy and data objects:
- Column removal/rename impact
- Hierarchy node changes
- Mapping modifications
- Formula changes
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from .types import (
    LineageGraph,
    LineageNode,
    ImpactResult,
    ImpactedObject,
    ImpactSeverity,
    ChangeType,
    NodeType,
    DependencyGraph,
    DependencyNode,
    LineageValidationResult,
)
from .lineage_tracker import LineageTracker

logger = logging.getLogger(__name__)


class ImpactAnalyzer:
    """Analyzes impact of changes on data lineage."""

    # Severity mapping for different scenarios
    SEVERITY_RULES = {
        # Column removal impacts
        (ChangeType.REMOVE_COLUMN, NodeType.DATA_MART): ImpactSeverity.CRITICAL,
        (ChangeType.REMOVE_COLUMN, NodeType.DYNAMIC_TABLE): ImpactSeverity.HIGH,
        (ChangeType.REMOVE_COLUMN, NodeType.VIEW): ImpactSeverity.HIGH,
        (ChangeType.REMOVE_COLUMN, NodeType.HIERARCHY): ImpactSeverity.MEDIUM,

        # Column rename impacts
        (ChangeType.RENAME_COLUMN, NodeType.DATA_MART): ImpactSeverity.HIGH,
        (ChangeType.RENAME_COLUMN, NodeType.DYNAMIC_TABLE): ImpactSeverity.MEDIUM,
        (ChangeType.RENAME_COLUMN, NodeType.VIEW): ImpactSeverity.MEDIUM,

        # Type modification impacts
        (ChangeType.MODIFY_TYPE, NodeType.DATA_MART): ImpactSeverity.HIGH,
        (ChangeType.MODIFY_TYPE, NodeType.DYNAMIC_TABLE): ImpactSeverity.MEDIUM,

        # Node removal impacts
        (ChangeType.REMOVE_NODE, NodeType.DATA_MART): ImpactSeverity.CRITICAL,
        (ChangeType.REMOVE_NODE, NodeType.HIERARCHY): ImpactSeverity.HIGH,

        # Mapping changes
        (ChangeType.MODIFY_MAPPING, NodeType.HIERARCHY): ImpactSeverity.MEDIUM,
        (ChangeType.MODIFY_MAPPING, NodeType.DATA_MART): ImpactSeverity.HIGH,

        # Formula changes
        (ChangeType.MODIFY_FORMULA, NodeType.DATA_MART): ImpactSeverity.HIGH,
        (ChangeType.MODIFY_FORMULA, NodeType.FORMULA_GROUP): ImpactSeverity.MEDIUM,
    }

    def __init__(self, lineage_tracker: LineageTracker):
        """
        Initialize the impact analyzer.

        Args:
            lineage_tracker: LineageTracker instance
        """
        self.tracker = lineage_tracker

    def analyze_column_removal(
        self,
        graph_name: str,
        node: str,
        column: str,
    ) -> ImpactResult:
        """
        Analyze impact of removing a column.

        Args:
            graph_name: Name of the lineage graph
            node: Node ID or name
            column: Column to remove

        Returns:
            ImpactResult with affected objects
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return ImpactResult(
                change_type=ChangeType.REMOVE_COLUMN,
                source_node_id="",
                source_node_name=node,
                change_description=f"Remove column '{column}'",
            )

        node_id = self.tracker._resolve_node_id(graph, node)
        source_node = graph.get_node(node_id) if node_id else None

        result = ImpactResult(
            change_type=ChangeType.REMOVE_COLUMN,
            source_node_id=node_id or "",
            source_node_name=source_node.name if source_node else node,
            change_description=f"Remove column '{column}' from {node}",
        )

        if not node_id:
            return result

        # Find all downstream columns that depend on this column
        visited = set()
        self._analyze_column_impact_downstream(
            graph, node_id, column, result, visited, 0
        )

        return result

    def analyze_column_rename(
        self,
        graph_name: str,
        node: str,
        old_column: str,
        new_column: str,
    ) -> ImpactResult:
        """
        Analyze impact of renaming a column.

        Args:
            graph_name: Name of the lineage graph
            node: Node ID or name
            old_column: Current column name
            new_column: New column name

        Returns:
            ImpactResult with affected objects
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return ImpactResult(
                change_type=ChangeType.RENAME_COLUMN,
                source_node_id="",
                source_node_name=node,
                change_description=f"Rename column '{old_column}' to '{new_column}'",
            )

        node_id = self.tracker._resolve_node_id(graph, node)
        source_node = graph.get_node(node_id) if node_id else None

        result = ImpactResult(
            change_type=ChangeType.RENAME_COLUMN,
            source_node_id=node_id or "",
            source_node_name=source_node.name if source_node else node,
            change_description=f"Rename column '{old_column}' to '{new_column}' in {node}",
        )

        if not node_id:
            return result

        # Find all downstream columns that depend on this column
        visited = set()
        self._analyze_column_impact_downstream(
            graph, node_id, old_column, result, visited, 0,
            severity_override=ImpactSeverity.MEDIUM  # Renames are less severe than removals
        )

        return result

    def analyze_hierarchy_change(
        self,
        graph_name: str,
        hierarchy_node: str,
        change_type: ChangeType,
        change_details: Optional[Dict[str, Any]] = None,
    ) -> ImpactResult:
        """
        Analyze impact of a hierarchy change.

        Args:
            graph_name: Name of the lineage graph
            hierarchy_node: Hierarchy node ID or name
            change_type: Type of change
            change_details: Additional change details

        Returns:
            ImpactResult with affected objects
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return ImpactResult(
                change_type=change_type,
                source_node_id="",
                source_node_name=hierarchy_node,
                change_description=f"Hierarchy change: {change_type.value}",
            )

        node_id = self.tracker._resolve_node_id(graph, hierarchy_node)
        source_node = graph.get_node(node_id) if node_id else None

        result = ImpactResult(
            change_type=change_type,
            source_node_id=node_id or "",
            source_node_name=source_node.name if source_node else hierarchy_node,
            change_description=f"Hierarchy change: {change_type.value}",
        )

        if not node_id:
            return result

        # Get all downstream objects
        downstream_ids = graph.get_all_downstream(node_id)

        for did in downstream_ids:
            if did == node_id:
                continue

            dnode = graph.get_node(did)
            if not dnode:
                continue

            # Calculate distance
            distance = self._calculate_distance(graph, node_id, did)

            # Determine severity
            severity = self._get_severity(change_type, dnode.node_type, distance)

            impact = ImpactedObject(
                node_id=did,
                node_name=dnode.name,
                node_type=dnode.node_type,
                impact_severity=severity,
                impact_description=f"Depends on hierarchy '{source_node.name if source_node else hierarchy_node}'",
                distance_from_source=distance,
            )
            result.add_impact(impact)

        return result

    def get_downstream_impact(
        self,
        graph_name: str,
        node: str,
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Get all objects that would be impacted by changes to a node.

        Args:
            graph_name: Name of the lineage graph
            node: Node ID or name
            max_depth: Maximum depth to traverse

        Returns:
            Impact summary
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return {"error": f"Graph '{graph_name}' not found"}

        node_id = self.tracker._resolve_node_id(graph, node)
        if not node_id:
            return {"error": f"Node '{node}' not found"}

        source_node = graph.get_node(node_id)

        # Get all downstream nodes
        downstream_ids = graph.get_all_downstream(node_id)
        downstream_ids.discard(node_id)  # Remove the source node

        impacted = []
        for did in downstream_ids:
            dnode = graph.get_node(did)
            if dnode:
                distance = self._calculate_distance(graph, node_id, did)
                impacted.append({
                    "node_id": did,
                    "node_name": dnode.name,
                    "node_type": dnode.node_type.value,
                    "distance": distance,
                })

        # Sort by distance
        impacted.sort(key=lambda x: x["distance"])

        return {
            "source_node": source_node.name if source_node else node,
            "source_node_id": node_id,
            "downstream_count": len(impacted),
            "impacted_objects": impacted,
            "by_type": self._group_by_type(impacted),
        }

    def get_upstream_dependencies(
        self,
        graph_name: str,
        node: str,
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Get all upstream dependencies of a node.

        Args:
            graph_name: Name of the lineage graph
            node: Node ID or name
            max_depth: Maximum depth to traverse

        Returns:
            Dependencies summary
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return {"error": f"Graph '{graph_name}' not found"}

        node_id = self.tracker._resolve_node_id(graph, node)
        if not node_id:
            return {"error": f"Node '{node}' not found"}

        source_node = graph.get_node(node_id)

        # Get all upstream nodes
        upstream_ids = graph.get_all_upstream(node_id)
        upstream_ids.discard(node_id)  # Remove the target node

        dependencies = []
        for uid in upstream_ids:
            unode = graph.get_node(uid)
            if unode:
                distance = self._calculate_distance(graph, uid, node_id)
                dependencies.append({
                    "node_id": uid,
                    "node_name": unode.name,
                    "node_type": unode.node_type.value,
                    "distance": distance,
                })

        # Sort by distance
        dependencies.sort(key=lambda x: x["distance"])

        return {
            "target_node": source_node.name if source_node else node,
            "target_node_id": node_id,
            "dependency_count": len(dependencies),
            "dependencies": dependencies,
            "by_type": self._group_by_type(dependencies),
        }

    def build_dependency_graph(
        self,
        graph_name: str,
        node: str,
        direction: str = "downstream",
        max_depth: int = 10,
    ) -> DependencyGraph:
        """
        Build a dependency graph for visualization.

        Args:
            graph_name: Name of the lineage graph
            node: Root node ID or name
            direction: "upstream" or "downstream"
            max_depth: Maximum depth to traverse

        Returns:
            DependencyGraph for visualization
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return DependencyGraph(direction=direction)

        node_id = self.tracker._resolve_node_id(graph, node)
        if not node_id:
            return DependencyGraph(direction=direction)

        dep_graph = DependencyGraph(
            root_node_id=node_id,
            direction=direction,
        )

        # Get related nodes
        if direction == "downstream":
            related_ids = graph.get_all_downstream(node_id)
        else:
            related_ids = graph.get_all_upstream(node_id)

        # Build nodes with levels
        levels: Dict[str, int] = {}
        self._calculate_levels(graph, node_id, direction, levels, 0)

        for nid in related_ids:
            lnode = graph.get_node(nid)
            if lnode:
                level = levels.get(nid, 0)
                dep_graph.nodes.append(DependencyNode(
                    id=nid,
                    name=lnode.name,
                    node_type=lnode.node_type,
                    level=level,
                ))
                if level > dep_graph.max_depth:
                    dep_graph.max_depth = level

        # Build edges
        for edge in graph.edges:
            if direction == "downstream":
                if edge.source_node_id in related_ids and edge.target_node_id in related_ids:
                    dep_graph.edges.append({
                        "source": edge.source_node_id,
                        "target": edge.target_node_id,
                    })
            else:
                if edge.source_node_id in related_ids and edge.target_node_id in related_ids:
                    dep_graph.edges.append({
                        "source": edge.source_node_id,
                        "target": edge.target_node_id,
                    })

        dep_graph.total_nodes = len(dep_graph.nodes)

        return dep_graph

    def validate_lineage(
        self,
        graph_name: str,
    ) -> LineageValidationResult:
        """
        Validate lineage graph completeness.

        Args:
            graph_name: Name of the lineage graph

        Returns:
            Validation result
        """
        graph = self.tracker.get_graph(graph_name)
        if not graph:
            return LineageValidationResult(is_valid=False)

        result = LineageValidationResult()
        result.node_count = len(graph.nodes)
        result.edge_count = len(graph.edges)

        # Count column lineage
        for edge in graph.edges:
            result.column_lineage_count += len(edge.column_lineage)

        # Find orphan nodes (no edges)
        node_ids_with_edges = set()
        for edge in graph.edges:
            node_ids_with_edges.add(edge.source_node_id)
            node_ids_with_edges.add(edge.target_node_id)

        for node_id in graph.nodes:
            if node_id not in node_ids_with_edges:
                result.orphan_nodes.append(node_id)

        # Check for circular dependencies
        for node_id in graph.nodes:
            if self._has_cycle(graph, node_id, set(), set()):
                result.circular_dependencies.append([node_id])

        # Calculate completeness score
        if result.node_count > 0:
            connected_ratio = len(node_ids_with_edges) / result.node_count
            has_column_lineage_ratio = (
                result.column_lineage_count / (result.edge_count + 1)
                if result.edge_count > 0 else 0
            )
            result.completeness_score = (connected_ratio + min(has_column_lineage_ratio, 1)) / 2

        # Add warnings
        if result.orphan_nodes:
            result.warnings.append(
                f"{len(result.orphan_nodes)} orphan node(s) with no lineage connections"
            )

        if result.circular_dependencies:
            result.warnings.append(
                f"{len(result.circular_dependencies)} circular dependency(ies) detected"
            )
            result.is_valid = False

        return result

    def _analyze_column_impact_downstream(
        self,
        graph: LineageGraph,
        node_id: str,
        column: str,
        result: ImpactResult,
        visited: Set[str],
        depth: int,
        severity_override: Optional[ImpactSeverity] = None,
    ) -> None:
        """Recursively analyze downstream column impact."""
        if depth > 50:  # Prevent infinite loops
            return

        key = f"{node_id}:{column}"
        if key in visited:
            return
        visited.add(key)

        # Find edges where this column is a source
        for edge in graph.edges:
            if edge.source_node_id != node_id:
                continue

            for col_lin in edge.column_lineage:
                if column.upper() not in [c.upper() for c in col_lin.source_columns]:
                    continue

                target_node = graph.get_node(col_lin.target_node_id)
                if not target_node:
                    continue

                # Calculate severity
                if severity_override:
                    severity = severity_override
                else:
                    severity = self._get_severity(
                        result.change_type, target_node.node_type, depth
                    )

                impact = ImpactedObject(
                    node_id=col_lin.target_node_id,
                    node_name=target_node.name,
                    node_type=target_node.node_type,
                    impact_severity=severity,
                    impact_description=f"Column '{col_lin.target_column}' depends on removed column",
                    affected_columns=[col_lin.target_column],
                    distance_from_source=depth + 1,
                )
                result.add_impact(impact)

                # Recurse for the target column
                self._analyze_column_impact_downstream(
                    graph, col_lin.target_node_id, col_lin.target_column,
                    result, visited, depth + 1, severity_override
                )

    def _calculate_distance(
        self,
        graph: LineageGraph,
        from_node: str,
        to_node: str,
    ) -> int:
        """Calculate minimum distance between nodes."""
        if from_node == to_node:
            return 0

        visited = set()
        queue = [(from_node, 0)]

        while queue:
            current, dist = queue.pop(0)
            if current == to_node:
                return dist

            if current in visited:
                continue
            visited.add(current)

            for edge in graph.edges:
                if edge.source_node_id == current:
                    if edge.target_node_id not in visited:
                        queue.append((edge.target_node_id, dist + 1))

        return 999  # Not reachable

    def _calculate_levels(
        self,
        graph: LineageGraph,
        node_id: str,
        direction: str,
        levels: Dict[str, int],
        current_level: int,
    ) -> None:
        """Calculate node levels for visualization."""
        if node_id in levels:
            return

        levels[node_id] = current_level

        for edge in graph.edges:
            if direction == "downstream":
                if edge.source_node_id == node_id:
                    self._calculate_levels(
                        graph, edge.target_node_id, direction, levels, current_level + 1
                    )
            else:
                if edge.target_node_id == node_id:
                    self._calculate_levels(
                        graph, edge.source_node_id, direction, levels, current_level + 1
                    )

    def _get_severity(
        self,
        change_type: ChangeType,
        node_type: NodeType,
        distance: int,
    ) -> ImpactSeverity:
        """Determine impact severity."""
        base_severity = self.SEVERITY_RULES.get(
            (change_type, node_type),
            ImpactSeverity.MEDIUM
        )

        # Reduce severity with distance
        if distance > 3:
            if base_severity == ImpactSeverity.CRITICAL:
                return ImpactSeverity.HIGH
            elif base_severity == ImpactSeverity.HIGH:
                return ImpactSeverity.MEDIUM
            elif base_severity == ImpactSeverity.MEDIUM:
                return ImpactSeverity.LOW

        return base_severity

    def _group_by_type(self, objects: List[Dict[str, Any]]) -> Dict[str, int]:
        """Group objects by node type."""
        by_type: Dict[str, int] = {}
        for obj in objects:
            node_type = obj.get("node_type", "UNKNOWN")
            by_type[node_type] = by_type.get(node_type, 0) + 1
        return by_type

    def _has_cycle(
        self,
        graph: LineageGraph,
        node_id: str,
        visiting: Set[str],
        visited: Set[str],
    ) -> bool:
        """Check for cycles using DFS."""
        if node_id in visiting:
            return True
        if node_id in visited:
            return False

        visiting.add(node_id)

        for edge in graph.edges:
            if edge.source_node_id == node_id:
                if self._has_cycle(graph, edge.target_node_id, visiting, visited):
                    return True

        visiting.remove(node_id)
        visited.add(node_id)

        return False
