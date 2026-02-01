"""
Semantic Graph using NetworkX for modeling data relationships.

This module provides the main SemanticGraph class that builds and manages
a graph-based representation of database schemas, columns, and hierarchies.
"""

from __future__ import annotations

import json
from typing import Any, Iterator

import networkx as nx

from databridge_discovery.graph.node_types import (
    ColumnNode,
    ConceptNode,
    EdgeType,
    GraphEdge,
    GraphNode,
    GraphStats,
    HierarchyNode,
    NodeType,
    SchemaNode,
    TableNode,
)
from databridge_discovery.models.parsed_query import ParsedQuery


class SemanticGraph:
    """
    A semantic graph for modeling database schema relationships.

    Uses NetworkX to build a graph where nodes represent tables, columns,
    and hierarchies, and edges represent relationships like joins, foreign
    keys, and semantic similarities.

    Example:
        graph = SemanticGraph()

        # Add from parsed query
        graph.add_from_parsed_query(parsed_query)

        # Find join paths
        paths = graph.find_join_paths("table_a", "table_b")

        # Get related nodes
        related = graph.get_related_nodes(node_id)
    """

    def __init__(self, name: str = "semantic_graph"):
        """
        Initialize the semantic graph.

        Args:
            name: Name for this graph
        """
        self.name = name
        self._graph = nx.DiGraph()  # Directed graph for relationships

        # Index for fast lookups
        self._nodes_by_type: dict[NodeType, dict[str, GraphNode]] = {
            nt: {} for nt in NodeType
        }
        self._nodes_by_name: dict[str, str] = {}  # name -> id
        self._edges_by_type: dict[EdgeType, list[GraphEdge]] = {
            et: [] for et in EdgeType
        }

    @property
    def node_count(self) -> int:
        """Get total number of nodes."""
        return self._graph.number_of_nodes()

    @property
    def edge_count(self) -> int:
        """Get total number of edges."""
        return self._graph.number_of_edges()

    def add_node(self, node: GraphNode) -> str:
        """
        Add a node to the graph.

        Args:
            node: GraphNode to add

        Returns:
            Node ID
        """
        # Add to NetworkX graph
        self._graph.add_node(
            node.id,
            node_type=node.node_type.value,
            name=node.name,
            data=node.model_dump(),
        )

        # Update indexes
        self._nodes_by_type[node.node_type][node.id] = node
        self._nodes_by_name[node.name.lower()] = node.id

        return node.id

    def add_edge(self, edge: GraphEdge) -> str:
        """
        Add an edge to the graph.

        Args:
            edge: GraphEdge to add

        Returns:
            Edge ID
        """
        # Add to NetworkX graph
        self._graph.add_edge(
            edge.source_id,
            edge.target_id,
            edge_type=edge.edge_type.value,
            weight=edge.weight,
            data=edge.model_dump(),
        )

        # Update index
        self._edges_by_type[edge.edge_type].append(edge)

        return edge.id

    def get_node(self, node_id: str) -> GraphNode | None:
        """
        Get a node by ID.

        Args:
            node_id: Node ID

        Returns:
            GraphNode or None
        """
        if node_id not in self._graph:
            return None

        node_data = self._graph.nodes[node_id].get("data", {})
        node_type_str = node_data.get("node_type", "table")

        # Reconstruct appropriate node type
        try:
            node_type = NodeType(node_type_str)
            if node_type == NodeType.TABLE:
                return TableNode(**node_data)
            elif node_type == NodeType.COLUMN:
                return ColumnNode(**node_data)
            elif node_type == NodeType.HIERARCHY:
                return HierarchyNode(**node_data)
            elif node_type == NodeType.CONCEPT:
                return ConceptNode(**node_data)
            else:
                return GraphNode(**node_data)
        except Exception:
            return GraphNode(**node_data) if node_data else None

    def get_node_by_name(self, name: str) -> GraphNode | None:
        """
        Get a node by name.

        Args:
            name: Node name (case-insensitive)

        Returns:
            GraphNode or None
        """
        node_id = self._nodes_by_name.get(name.lower())
        if node_id:
            return self.get_node(node_id)
        return None

    def get_nodes_by_type(self, node_type: NodeType | str) -> list[GraphNode]:
        """
        Get all nodes of a specific type.

        Args:
            node_type: Type of nodes to get (NodeType enum or string)

        Returns:
            List of GraphNodes
        """
        if isinstance(node_type, str):
            try:
                node_type = NodeType(node_type)
            except ValueError:
                return []
        return list(self._nodes_by_type.get(node_type, {}).values())

    def find_nodes_by_name(self, name: str, fuzzy: bool = False) -> list[GraphNode]:
        """
        Find nodes by name.

        Args:
            name: Name to search for (case-insensitive)
            fuzzy: Whether to do partial matching

        Returns:
            List of matching GraphNodes
        """
        name_lower = name.lower()
        results = []

        if fuzzy:
            # Partial match
            for node_name, node_id in self._nodes_by_name.items():
                if name_lower in node_name:
                    node = self.get_node(node_id)
                    if node:
                        results.append(node)
        else:
            # Exact match
            node_id = self._nodes_by_name.get(name_lower)
            if node_id:
                node = self.get_node(node_id)
                if node:
                    results.append(node)

        return results

    def get_all_nodes(self) -> list[GraphNode]:
        """
        Get all nodes in the graph.

        Returns:
            List of all GraphNodes
        """
        nodes = []
        for type_nodes in self._nodes_by_type.values():
            nodes.extend(type_nodes.values())
        return nodes

    def get_edges_by_type(self, edge_type: EdgeType) -> list[GraphEdge]:
        """
        Get all edges of a specific type.

        Args:
            edge_type: Type of edges to get

        Returns:
            List of GraphEdges
        """
        return self._edges_by_type.get(edge_type, [])

    def remove_node(self, node_id: str) -> bool:
        """
        Remove a node and its edges from the graph.

        Args:
            node_id: Node ID to remove

        Returns:
            True if removed
        """
        if node_id not in self._graph:
            return False

        # Remove from indexes
        node = self.get_node(node_id)
        if node:
            self._nodes_by_type[node.node_type].pop(node_id, None)
            self._nodes_by_name.pop(node.name.lower(), None)

        # Remove from graph (also removes edges)
        self._graph.remove_node(node_id)
        return True

    def add_from_parsed_query(self, parsed_query: ParsedQuery) -> dict[str, int]:
        """
        Add nodes and edges from a parsed SQL query.

        Args:
            parsed_query: ParsedQuery to add

        Returns:
            Dictionary with counts of added nodes and edges
        """
        added = {"tables": 0, "columns": 0, "joins": 0}

        # Add table nodes
        table_id_map: dict[str, str] = {}  # alias/name -> node_id

        for table in parsed_query.tables:
            table_node = TableNode.from_parsed_table(table)
            self.add_node(table_node)
            table_id_map[table.reference_name] = table_node.id
            added["tables"] += 1

        # Add column nodes
        for column in parsed_query.columns:
            col_node = ColumnNode.from_parsed_column(
                column,
                table_id=table_id_map.get(column.table_ref) if column.table_ref else None,
            )
            self.add_node(col_node)
            added["columns"] += 1

            # Add COLUMN_OF edge if table reference exists
            if column.table_ref and column.table_ref in table_id_map:
                edge = GraphEdge(
                    edge_type=EdgeType.COLUMN_OF,
                    source_id=col_node.id,
                    target_id=table_id_map[column.table_ref],
                )
                self.add_edge(edge)

        # Add join edges
        for join in parsed_query.joins:
            left_id = table_id_map.get(join.left_table)
            right_id = table_id_map.get(join.right_table)

            if left_id and right_id:
                edge = GraphEdge(
                    edge_type=EdgeType.JOIN,
                    source_id=left_id,
                    target_id=right_id,
                    join_condition=join.condition,
                    join_type=join.join_type.value,
                    metadata={
                        "left_column": join.left_column,
                        "right_column": join.right_column,
                    },
                )
                self.add_edge(edge)
                added["joins"] += 1

        return added

    def add_from_case_statement(self, case_statement: Any) -> dict[str, int]:
        """
        Add hierarchy nodes from a CASE statement.

        Args:
            case_statement: CaseStatement object

        Returns:
            Dictionary with counts of added nodes
        """
        added = {"hierarchies": 0, "levels": 0}

        # Create hierarchy node
        hier_node = HierarchyNode(
            name=case_statement.source_column,
            hierarchy_id=case_statement.id,
            source_column=case_statement.input_column,
            metadata={
                "entity_type": case_statement.detected_entity_type.value,
                "pattern": case_statement.detected_pattern,
                "condition_count": case_statement.condition_count,
            },
        )
        self.add_node(hier_node)
        added["hierarchies"] += 1

        # Create nodes for each unique result value
        for idx, result in enumerate(case_statement.unique_result_values):
            level_node = HierarchyNode(
                node_type=NodeType.HIERARCHY_NODE,
                name=result,
                value=result,
                parent_id=hier_node.id,
                sort_order=idx,
            )
            self.add_node(level_node)
            added["levels"] += 1

            # Add parent relationship
            edge = GraphEdge(
                edge_type=EdgeType.MEMBER_OF,
                source_id=level_node.id,
                target_id=hier_node.id,
            )
            self.add_edge(edge)

        return added

    def get_neighbors(self, node_id: str, edge_type: EdgeType | None = None) -> list[str]:
        """
        Get neighboring node IDs.

        Args:
            node_id: Node ID
            edge_type: Filter by edge type (optional)

        Returns:
            List of neighbor node IDs
        """
        if node_id not in self._graph:
            return []

        neighbors = []

        # Outgoing edges
        for _, target, data in self._graph.out_edges(node_id, data=True):
            if edge_type is None or data.get("edge_type") == edge_type.value:
                neighbors.append(target)

        # Incoming edges
        for source, _, data in self._graph.in_edges(node_id, data=True):
            if edge_type is None or data.get("edge_type") == edge_type.value:
                neighbors.append(source)

        return list(set(neighbors))

    def get_related_nodes(
        self,
        node_id: str,
        max_depth: int = 2,
        edge_types: list[EdgeType] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Get all nodes related to a given node up to max_depth.

        Args:
            node_id: Starting node ID
            max_depth: Maximum traversal depth
            edge_types: Filter by edge types (optional)

        Returns:
            Dictionary of node_id -> {node, depth, path}
        """
        if node_id not in self._graph:
            return {}

        related: dict[str, dict[str, Any]] = {}
        visited = {node_id}
        queue = [(node_id, 0, [node_id])]

        while queue:
            current_id, depth, path = queue.pop(0)

            if depth >= max_depth:
                continue

            neighbors = self.get_neighbors(current_id)

            for neighbor_id in neighbors:
                if neighbor_id in visited:
                    continue

                # Check edge type filter
                if edge_types:
                    # Get edge between current and neighbor
                    edge_data = self._graph.get_edge_data(current_id, neighbor_id) or \
                                self._graph.get_edge_data(neighbor_id, current_id)
                    if edge_data:
                        edge_type_str = edge_data.get("edge_type")
                        if not any(et.value == edge_type_str for et in edge_types):
                            continue

                visited.add(neighbor_id)
                new_path = path + [neighbor_id]

                node = self.get_node(neighbor_id)
                related[neighbor_id] = {
                    "node": node,
                    "depth": depth + 1,
                    "path": new_path,
                }

                queue.append((neighbor_id, depth + 1, new_path))

        return related

    def find_join_paths(
        self,
        source_name: str,
        target_name: str,
        max_length: int = 5,
    ) -> list[list[str]]:
        """
        Find all paths between two tables through joins.

        Args:
            source_name: Source table name
            target_name: Target table name
            max_length: Maximum path length

        Returns:
            List of paths (each path is a list of node IDs)
        """
        source_id = self._nodes_by_name.get(source_name.lower())
        target_id = self._nodes_by_name.get(target_name.lower())

        if not source_id or not target_id:
            return []

        # Create undirected view for path finding
        undirected = self._graph.to_undirected()

        try:
            # Find all simple paths
            paths = list(nx.all_simple_paths(
                undirected,
                source_id,
                target_id,
                cutoff=max_length,
            ))
            return paths
        except nx.NetworkXNoPath:
            return []

    def find_shortest_path(self, source_id: str, target_id: str) -> list[str] | None:
        """
        Find shortest path between two nodes.

        Args:
            source_id: Source node ID
            target_id: Target node ID

        Returns:
            Path as list of node IDs, or None
        """
        try:
            undirected = self._graph.to_undirected()
            return nx.shortest_path(undirected, source_id, target_id)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def get_connected_components(self) -> list[set[str]]:
        """
        Get connected components of the graph.

        Returns:
            List of sets of node IDs
        """
        undirected = self._graph.to_undirected()
        return [set(c) for c in nx.connected_components(undirected)]

    def get_stats(self) -> GraphStats:
        """
        Get graph statistics.

        Returns:
            GraphStats object
        """
        undirected = self._graph.to_undirected()
        components = list(nx.connected_components(undirected))

        # Calculate density
        n = self._graph.number_of_nodes()
        e = self._graph.number_of_edges()
        density = (2 * e) / (n * (n - 1)) if n > 1 else 0

        # Calculate average degree
        avg_degree = sum(dict(self._graph.degree()).values()) / n if n > 0 else 0

        return GraphStats(
            node_count=n,
            edge_count=e,
            table_count=len(self._nodes_by_type.get(NodeType.TABLE, {})),
            column_count=len(self._nodes_by_type.get(NodeType.COLUMN, {})),
            hierarchy_count=len(self._nodes_by_type.get(NodeType.HIERARCHY, {})),
            concept_count=len(self._nodes_by_type.get(NodeType.CONCEPT, {})),
            join_count=len(self._edges_by_type.get(EdgeType.JOIN, [])),
            foreign_key_count=len(self._edges_by_type.get(EdgeType.FOREIGN_KEY, [])),
            similarity_count=len(self._edges_by_type.get(EdgeType.SIMILAR_TO, [])),
            density=density,
            avg_degree=avg_degree,
            connected_components=len(components),
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Convert graph to dictionary format.

        Returns:
            Dictionary representation
        """
        nodes = []
        for node_id in self._graph.nodes():
            node_data = self._graph.nodes[node_id].get("data", {})
            nodes.append({
                "id": node_id,
                **node_data,
            })

        edges = []
        for source, target, data in self._graph.edges(data=True):
            edge_data = data.get("data", {})
            edges.append({
                "source": source,
                "target": target,
                **edge_data,
            })

        return {
            "name": self.name,
            "nodes": nodes,
            "edges": edges,
            "stats": self.get_stats().model_dump(),
        }

    def to_graphml(self) -> str:
        """
        Export graph to GraphML format.

        Returns:
            GraphML string
        """
        # Create a copy with serializable attributes
        export_graph = nx.DiGraph()

        for node_id in self._graph.nodes():
            attrs = dict(self._graph.nodes[node_id])
            # Convert dict data to JSON string
            if "data" in attrs:
                attrs["data"] = json.dumps(attrs["data"])
            export_graph.add_node(node_id, **attrs)

        for source, target, data in self._graph.edges(data=True):
            attrs = dict(data)
            if "data" in attrs:
                attrs["data"] = json.dumps(attrs["data"])
            export_graph.add_edge(source, target, **attrs)

        from io import BytesIO
        buffer = BytesIO()
        nx.write_graphml(export_graph, buffer)
        return buffer.getvalue().decode("utf-8")

    def to_json(self, include_embeddings: bool = True) -> dict[str, Any]:
        """
        Export graph to JSON-serializable dictionary.

        Args:
            include_embeddings: Whether to include vector embeddings

        Returns:
            Dictionary representation (JSON-serializable)
        """
        data = self.to_dict()

        # Optionally strip embeddings
        if not include_embeddings:
            for node in data.get("nodes", []):
                node.pop("embedding", None)
                node.pop("embedding_model", None)

        return data

    def to_json_string(self, include_embeddings: bool = True) -> str:
        """
        Export graph to JSON string format.

        Args:
            include_embeddings: Whether to include vector embeddings

        Returns:
            JSON string
        """
        return json.dumps(self.to_json(include_embeddings), indent=2, default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SemanticGraph":
        """
        Create graph from dictionary.

        Args:
            data: Dictionary representation

        Returns:
            SemanticGraph instance
        """
        graph = cls(name=data.get("name", "imported"))

        # Add nodes
        for node_data in data.get("nodes", []):
            node_type_str = node_data.get("node_type", "table")
            try:
                node_type = NodeType(node_type_str)
                if node_type == NodeType.TABLE:
                    node = TableNode(**node_data)
                elif node_type == NodeType.COLUMN:
                    node = ColumnNode(**node_data)
                elif node_type == NodeType.HIERARCHY:
                    node = HierarchyNode(**node_data)
                else:
                    node = GraphNode(**node_data)
                graph.add_node(node)
            except Exception:
                pass

        # Add edges
        for edge_data in data.get("edges", []):
            try:
                edge = GraphEdge(**edge_data)
                graph.add_edge(edge)
            except Exception:
                pass

        return graph

    def merge_graph(self, other: "SemanticGraph") -> int:
        """
        Merge another graph into this one.

        Args:
            other: Graph to merge

        Returns:
            Number of new nodes added
        """
        added = 0

        # Add nodes from other graph
        for node_type in NodeType:
            for node_id, node in other._nodes_by_type.get(node_type, {}).items():
                if node_id not in self._graph:
                    self.add_node(node)
                    added += 1

        # Add edges from other graph
        for edge_type in EdgeType:
            for edge in other._edges_by_type.get(edge_type, []):
                if edge.source_id in self._graph and edge.target_id in self._graph:
                    if not self._graph.has_edge(edge.source_id, edge.target_id):
                        self.add_edge(edge)

        return added

    def iter_nodes(self, node_type: NodeType | None = None) -> Iterator[GraphNode]:
        """
        Iterate over nodes.

        Args:
            node_type: Filter by node type (optional)

        Yields:
            GraphNode objects
        """
        if node_type:
            yield from self._nodes_by_type.get(node_type, {}).values()
        else:
            for nodes in self._nodes_by_type.values():
                yield from nodes.values()

    def iter_edges(self, edge_type: EdgeType | None = None) -> Iterator[GraphEdge]:
        """
        Iterate over edges.

        Args:
            edge_type: Filter by edge type (optional)

        Yields:
            GraphEdge objects
        """
        if edge_type:
            yield from self._edges_by_type.get(edge_type, [])
        else:
            for edges in self._edges_by_type.values():
                yield from edges
