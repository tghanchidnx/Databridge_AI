"""
Graph Analyzer for centrality analysis, community detection, and pattern finding.

This module provides algorithms for analyzing the semantic graph to find
important nodes, clusters, and structural patterns.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import networkx as nx

from databridge_discovery.graph.node_types import EdgeType, NodeType
from databridge_discovery.graph.semantic_graph import SemanticGraph


@dataclass
class CentralityResult:
    """Result from centrality analysis."""

    node_id: str
    node_name: str
    node_type: str
    score: float
    rank: int


@dataclass
class CommunityResult:
    """Result from community detection."""

    community_id: int
    node_ids: list[str]
    size: int
    dominant_type: str | None = None
    label: str | None = None


@dataclass
class JoinPathResult:
    """Result from join path analysis."""

    source: str
    target: str
    path: list[str]
    length: int
    join_conditions: list[str] = field(default_factory=list)
    confidence: float = 1.0


@dataclass
class PatternMatch:
    """Result from pattern matching."""

    pattern_type: str
    nodes: list[str]
    confidence: float
    description: str


class GraphAnalyzer:
    """
    Analyzer for semantic graphs.

    Provides centrality analysis, community detection, join path finding,
    and pattern detection algorithms.

    Example:
        analyzer = GraphAnalyzer(semantic_graph)

        # Find central tables
        central = analyzer.get_central_nodes(NodeType.TABLE, top_k=10)

        # Detect communities
        communities = analyzer.detect_communities()

        # Find star schema patterns
        patterns = analyzer.find_star_schema_patterns()
    """

    def __init__(self, graph: SemanticGraph):
        """
        Initialize the analyzer.

        Args:
            graph: SemanticGraph to analyze
        """
        self.graph = graph
        self._nx_graph = graph._graph
        self._undirected = self._nx_graph.to_undirected()

    def get_degree_centrality(
        self,
        node_type: NodeType | None = None,
        top_k: int = 10,
    ) -> list[CentralityResult]:
        """
        Calculate degree centrality for nodes.

        Degree centrality measures how connected a node is.

        Args:
            node_type: Filter by node type (optional)
            top_k: Return top K results

        Returns:
            List of CentralityResult ordered by score
        """
        centrality = nx.degree_centrality(self._undirected)
        return self._filter_and_rank_centrality(centrality, node_type, top_k)

    def get_betweenness_centrality(
        self,
        node_type: NodeType | None = None,
        top_k: int = 10,
    ) -> list[CentralityResult]:
        """
        Calculate betweenness centrality for nodes.

        Betweenness centrality measures how often a node appears
        on shortest paths between other nodes.

        Args:
            node_type: Filter by node type (optional)
            top_k: Return top K results

        Returns:
            List of CentralityResult ordered by score
        """
        centrality = nx.betweenness_centrality(self._undirected)
        return self._filter_and_rank_centrality(centrality, node_type, top_k)

    def get_pagerank(
        self,
        node_type: NodeType | None = None,
        top_k: int = 10,
        alpha: float = 0.85,
    ) -> list[CentralityResult]:
        """
        Calculate PageRank for nodes.

        PageRank measures importance based on incoming connections
        and the importance of connecting nodes.

        Args:
            node_type: Filter by node type (optional)
            top_k: Return top K results
            alpha: Damping factor

        Returns:
            List of CentralityResult ordered by score
        """
        try:
            centrality = nx.pagerank(self._nx_graph, alpha=alpha)
        except nx.PowerIterationFailedConvergence:
            centrality = nx.pagerank(self._nx_graph, alpha=alpha, max_iter=500)

        return self._filter_and_rank_centrality(centrality, node_type, top_k)

    def get_eigenvector_centrality(
        self,
        node_type: NodeType | None = None,
        top_k: int = 10,
    ) -> list[CentralityResult]:
        """
        Calculate eigenvector centrality for nodes.

        Eigenvector centrality measures influence based on
        connections to other influential nodes.

        Args:
            node_type: Filter by node type (optional)
            top_k: Return top K results

        Returns:
            List of CentralityResult ordered by score
        """
        try:
            centrality = nx.eigenvector_centrality(self._undirected, max_iter=500)
        except nx.PowerIterationFailedConvergence:
            # Fall back to degree centrality
            centrality = nx.degree_centrality(self._undirected)

        return self._filter_and_rank_centrality(centrality, node_type, top_k)

    def _filter_and_rank_centrality(
        self,
        centrality: dict[str, float],
        node_type: NodeType | None,
        top_k: int,
    ) -> list[CentralityResult]:
        """Filter and rank centrality results."""
        results = []

        for node_id, score in centrality.items():
            node_data = self._nx_graph.nodes.get(node_id, {})
            current_type = node_data.get("node_type", "unknown")

            if node_type and current_type != node_type.value:
                continue

            results.append(CentralityResult(
                node_id=node_id,
                node_name=node_data.get("name", node_id),
                node_type=current_type,
                score=score,
                rank=0,
            ))

        # Sort by score descending
        results.sort(key=lambda x: x.score, reverse=True)

        # Assign ranks
        for i, result in enumerate(results[:top_k]):
            result.rank = i + 1

        return results[:top_k]

    def detect_communities(
        self,
        algorithm: str = "louvain",
        resolution: float = 1.0,
    ) -> list[CommunityResult]:
        """
        Detect communities in the graph.

        Args:
            algorithm: Algorithm to use: "louvain", "label_propagation", or "greedy"
            resolution: Resolution parameter for Louvain

        Returns:
            List of CommunityResult
        """
        if self._undirected.number_of_nodes() == 0:
            return []

        # Get communities based on algorithm
        if algorithm == "louvain":
            try:
                import community.community_louvain as community_louvain
                partition = community_louvain.best_partition(
                    self._undirected,
                    resolution=resolution,
                )
            except ImportError:
                # Fall back to greedy modularity
                communities = nx.community.greedy_modularity_communities(self._undirected)
                partition = {}
                for i, comm in enumerate(communities):
                    for node in comm:
                        partition[node] = i

        elif algorithm == "label_propagation":
            communities = nx.community.label_propagation_communities(self._undirected)
            partition = {}
            for i, comm in enumerate(communities):
                for node in comm:
                    partition[node] = i

        else:  # greedy
            communities = nx.community.greedy_modularity_communities(self._undirected)
            partition = {}
            for i, comm in enumerate(communities):
                for node in comm:
                    partition[node] = i

        # Group nodes by community
        community_nodes: dict[int, list[str]] = defaultdict(list)
        for node_id, comm_id in partition.items():
            community_nodes[comm_id].append(node_id)

        # Build results
        results = []
        for comm_id, node_ids in community_nodes.items():
            # Find dominant node type
            type_counts: dict[str, int] = defaultdict(int)
            for node_id in node_ids:
                node_data = self._nx_graph.nodes.get(node_id, {})
                node_type = node_data.get("node_type", "unknown")
                type_counts[node_type] += 1

            dominant_type = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else None

            results.append(CommunityResult(
                community_id=comm_id,
                node_ids=node_ids,
                size=len(node_ids),
                dominant_type=dominant_type,
                label=f"Community {comm_id} ({dominant_type})",
            ))

        # Sort by size descending
        results.sort(key=lambda x: x.size, reverse=True)
        return results

    def find_join_paths(
        self,
        source: str,
        target: str,
        max_length: int = 5,
    ) -> list[JoinPathResult]:
        """
        Find all join paths between two nodes.

        Args:
            source: Source node name
            target: Target node name
            max_length: Maximum path length

        Returns:
            List of JoinPathResult
        """
        paths = self.graph.find_join_paths(source, target, max_length)

        results = []
        for path in paths:
            # Extract join conditions along the path
            conditions = []
            for i in range(len(path) - 1):
                edge_data = self._nx_graph.get_edge_data(path[i], path[i + 1])
                if edge_data is None:
                    edge_data = self._nx_graph.get_edge_data(path[i + 1], path[i])
                if edge_data:
                    data = edge_data.get("data", {})
                    if "join_condition" in data:
                        conditions.append(data["join_condition"])

            # Calculate confidence based on path length
            confidence = 1.0 / len(path)

            results.append(JoinPathResult(
                source=source,
                target=target,
                path=path,
                length=len(path) - 1,
                join_conditions=conditions,
                confidence=confidence,
            ))

        # Sort by length (shorter is better)
        results.sort(key=lambda x: x.length)
        return results

    def find_star_schema_patterns(self) -> list[PatternMatch]:
        """
        Find star schema patterns (fact table surrounded by dimensions).

        Returns:
            List of PatternMatch for star schemas
        """
        patterns = []

        # Look for nodes with high degree that might be fact tables
        for node_id in self._nx_graph.nodes():
            node_data = self._nx_graph.nodes[node_id]
            if node_data.get("node_type") != NodeType.TABLE.value:
                continue

            # Get degree
            degree = self._undirected.degree(node_id)

            if degree >= 3:  # At least 3 connections for a star
                neighbors = list(self._undirected.neighbors(node_id))

                # Check if neighbors are tables (potential dimensions)
                dim_neighbors = [
                    n for n in neighbors
                    if self._nx_graph.nodes.get(n, {}).get("node_type") == NodeType.TABLE.value
                ]

                if len(dim_neighbors) >= 3:
                    patterns.append(PatternMatch(
                        pattern_type="star_schema",
                        nodes=[node_id] + dim_neighbors,
                        confidence=min(1.0, len(dim_neighbors) / 5),
                        description=f"Potential fact table '{node_data.get('name')}' with {len(dim_neighbors)} dimensions",
                    ))

        return patterns

    def find_hub_nodes(
        self,
        min_degree: int = 5,
        node_type: NodeType | None = None,
    ) -> list[dict[str, Any]]:
        """
        Find hub nodes with many connections.

        Args:
            min_degree: Minimum degree to be considered a hub
            node_type: Filter by node type (optional)

        Returns:
            List of hub node info
        """
        hubs = []

        for node_id in self._nx_graph.nodes():
            node_data = self._nx_graph.nodes[node_id]

            if node_type and node_data.get("node_type") != node_type.value:
                continue

            degree = self._undirected.degree(node_id)

            if degree >= min_degree:
                hubs.append({
                    "node_id": node_id,
                    "name": node_data.get("name", node_id),
                    "node_type": node_data.get("node_type"),
                    "degree": degree,
                    "in_degree": self._nx_graph.in_degree(node_id),
                    "out_degree": self._nx_graph.out_degree(node_id),
                })

        hubs.sort(key=lambda x: x["degree"], reverse=True)
        return hubs

    def find_bridges(self) -> list[tuple[str, str]]:
        """
        Find bridge edges that connect different components.

        Returns:
            List of (source, target) edge tuples
        """
        return list(nx.bridges(self._undirected))

    def find_orphan_nodes(self) -> list[str]:
        """
        Find nodes with no connections.

        Returns:
            List of orphan node IDs
        """
        return [
            node_id
            for node_id in self._nx_graph.nodes()
            if self._undirected.degree(node_id) == 0
        ]

    def get_clustering_coefficient(self, node_id: str | None = None) -> float | dict[str, float]:
        """
        Get clustering coefficient.

        Args:
            node_id: Specific node (optional, returns all if None)

        Returns:
            Clustering coefficient(s)
        """
        if node_id:
            return nx.clustering(self._undirected, node_id)
        return nx.average_clustering(self._undirected)

    def find_cliques(self, min_size: int = 3) -> list[list[str]]:
        """
        Find cliques (fully connected subgraphs).

        Args:
            min_size: Minimum clique size

        Returns:
            List of cliques (each is a list of node IDs)
        """
        cliques = [
            list(c)
            for c in nx.find_cliques(self._undirected)
            if len(c) >= min_size
        ]
        cliques.sort(key=len, reverse=True)
        return cliques

    def suggest_missing_edges(
        self,
        similarity_threshold: float = 0.5,
        max_suggestions: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Suggest potentially missing edges based on graph structure.

        Uses common neighbors and Jaccard coefficient to find
        nodes that might be related but aren't connected.

        Args:
            similarity_threshold: Minimum similarity score
            max_suggestions: Maximum suggestions to return

        Returns:
            List of suggested edges with confidence
        """
        suggestions = []

        # Get pairs with common neighbors but no direct edge
        for u in self._nx_graph.nodes():
            for v in self._nx_graph.nodes():
                if u >= v:  # Avoid duplicates
                    continue

                if self._undirected.has_edge(u, v):
                    continue

                # Calculate Jaccard coefficient
                neighbors_u = set(self._undirected.neighbors(u))
                neighbors_v = set(self._undirected.neighbors(v))

                if not neighbors_u or not neighbors_v:
                    continue

                intersection = len(neighbors_u & neighbors_v)
                union = len(neighbors_u | neighbors_v)

                if union == 0:
                    continue

                jaccard = intersection / union

                if jaccard >= similarity_threshold:
                    u_data = self._nx_graph.nodes[u]
                    v_data = self._nx_graph.nodes[v]

                    suggestions.append({
                        "source": u,
                        "source_name": u_data.get("name", u),
                        "target": v,
                        "target_name": v_data.get("name", v),
                        "confidence": jaccard,
                        "common_neighbors": list(neighbors_u & neighbors_v),
                    })

        suggestions.sort(key=lambda x: x["confidence"], reverse=True)
        return suggestions[:max_suggestions]

    def get_summary(self) -> dict[str, Any]:
        """
        Get analysis summary for the graph.

        Returns:
            Dictionary with analysis results
        """
        stats = self.graph.get_stats()

        return {
            "stats": stats.model_dump(),
            "top_central_tables": [
                {"name": r.node_name, "score": r.score}
                for r in self.get_degree_centrality(NodeType.TABLE, top_k=5)
            ],
            "community_count": len(self.detect_communities()),
            "hub_count": len(self.find_hub_nodes(min_degree=5)),
            "orphan_count": len(self.find_orphan_nodes()),
            "star_patterns": len(self.find_star_schema_patterns()),
        }
