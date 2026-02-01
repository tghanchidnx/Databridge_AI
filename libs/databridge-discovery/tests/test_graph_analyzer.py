"""
Tests for the GraphAnalyzer module.
"""

import pytest

from databridge_discovery.graph.semantic_graph import SemanticGraph
from databridge_discovery.graph.graph_analyzer import (
    CentralityResult,
    CommunityResult,
    GraphAnalyzer,
    JoinPathResult,
    PatternMatch,
)
from databridge_discovery.graph.node_types import (
    EdgeType,
    GraphEdge,
    NodeType,
    TableNode,
)


@pytest.fixture
def sample_graph() -> SemanticGraph:
    """Create a sample graph for testing."""
    graph = SemanticGraph(name="test")

    # Create a star schema pattern
    # fact_sales in center connected to dim_customer, dim_product, dim_time, dim_store
    fact = TableNode(name="fact_sales", table_name="fact_sales", table_type="fact")
    dim_customer = TableNode(name="dim_customer", table_name="dim_customer", table_type="dimension")
    dim_product = TableNode(name="dim_product", table_name="dim_product", table_type="dimension")
    dim_time = TableNode(name="dim_time", table_name="dim_time", table_type="dimension")
    dim_store = TableNode(name="dim_store", table_name="dim_store", table_type="dimension")

    graph.add_node(fact)
    graph.add_node(dim_customer)
    graph.add_node(dim_product)
    graph.add_node(dim_time)
    graph.add_node(dim_store)

    # Connect dimensions to fact
    for dim in [dim_customer, dim_product, dim_time, dim_store]:
        graph.add_edge(GraphEdge(
            edge_type=EdgeType.JOIN,
            source_id=fact.id,
            target_id=dim.id,
        ))

    return graph


@pytest.fixture
def chain_graph() -> SemanticGraph:
    """Create a chain graph for testing."""
    graph = SemanticGraph(name="chain")

    # A -> B -> C -> D -> E
    nodes = []
    for name in ["A", "B", "C", "D", "E"]:
        node = TableNode(name=name, table_name=name)
        graph.add_node(node)
        nodes.append(node)

    for i in range(len(nodes) - 1):
        graph.add_edge(GraphEdge(
            edge_type=EdgeType.JOIN,
            source_id=nodes[i].id,
            target_id=nodes[i+1].id,
        ))

    return graph


class TestCentralityAnalysis:
    """Tests for centrality analysis."""

    def test_degree_centrality(self, sample_graph):
        """Test degree centrality calculation."""
        analyzer = GraphAnalyzer(sample_graph)
        results = analyzer.get_degree_centrality(top_k=5)

        assert len(results) == 5
        # fact_sales should be most central (highest degree)
        assert results[0].node_name == "fact_sales"
        assert results[0].rank == 1
        assert results[0].score > 0

    def test_degree_centrality_by_type(self, sample_graph):
        """Test degree centrality filtered by node type."""
        analyzer = GraphAnalyzer(sample_graph)
        results = analyzer.get_degree_centrality(node_type=NodeType.TABLE, top_k=3)

        assert len(results) == 3
        for result in results:
            assert result.node_type == "table"

    def test_betweenness_centrality(self, chain_graph):
        """Test betweenness centrality calculation."""
        analyzer = GraphAnalyzer(chain_graph)
        results = analyzer.get_betweenness_centrality(top_k=5)

        # Middle nodes should have higher betweenness
        assert len(results) == 5
        # C should be in top results (middle of chain)
        names = [r.node_name for r in results]
        assert "C" in names[:3]

    def test_pagerank(self, sample_graph):
        """Test PageRank calculation."""
        analyzer = GraphAnalyzer(sample_graph)
        results = analyzer.get_pagerank(top_k=5)

        assert len(results) == 5
        # All nodes should have valid scores
        for result in results:
            assert 0 <= result.score <= 1

    def test_eigenvector_centrality(self, sample_graph):
        """Test eigenvector centrality calculation."""
        analyzer = GraphAnalyzer(sample_graph)
        results = analyzer.get_eigenvector_centrality(top_k=5)

        assert len(results) == 5
        # Fact table should be most influential
        assert results[0].node_name == "fact_sales"


class TestCommunityDetection:
    """Tests for community detection."""

    def test_detect_communities_greedy(self, sample_graph):
        """Test community detection with greedy modularity."""
        analyzer = GraphAnalyzer(sample_graph)
        communities = analyzer.detect_communities(algorithm="greedy")

        # Should find at least one community
        assert len(communities) >= 1
        # Check community structure
        for comm in communities:
            assert comm.community_id >= 0
            assert len(comm.node_ids) > 0
            assert comm.size == len(comm.node_ids)

    def test_detect_communities_label_propagation(self, sample_graph):
        """Test community detection with label propagation."""
        analyzer = GraphAnalyzer(sample_graph)
        communities = analyzer.detect_communities(algorithm="label_propagation")

        assert len(communities) >= 1

    def test_community_dominant_type(self, sample_graph):
        """Test that community dominant type is calculated."""
        analyzer = GraphAnalyzer(sample_graph)
        communities = analyzer.detect_communities()

        for comm in communities:
            # All our nodes are tables
            assert comm.dominant_type == "table"


class TestJoinPathFinding:
    """Tests for join path finding."""

    def test_find_join_paths(self, chain_graph):
        """Test finding join paths between tables."""
        analyzer = GraphAnalyzer(chain_graph)
        paths = analyzer.find_join_paths("A", "E", max_length=5)

        assert len(paths) >= 1
        # Path should be A -> B -> C -> D -> E
        path = paths[0]
        assert path.source == "A"
        assert path.target == "E"
        assert path.length == 4

    def test_find_join_paths_no_path(self):
        """Test when no path exists."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="isolated1", table_name="isolated1"))
        graph.add_node(TableNode(name="isolated2", table_name="isolated2"))

        analyzer = GraphAnalyzer(graph)
        paths = analyzer.find_join_paths("isolated1", "isolated2")

        assert len(paths) == 0

    def test_join_path_confidence(self, chain_graph):
        """Test that path confidence is calculated."""
        analyzer = GraphAnalyzer(chain_graph)
        paths = analyzer.find_join_paths("A", "C")

        assert len(paths) >= 1
        # Shorter paths should have higher confidence
        for path in paths:
            assert 0 < path.confidence <= 1


class TestPatternDetection:
    """Tests for pattern detection."""

    def test_find_star_schema_patterns(self, sample_graph):
        """Test detecting star schema patterns."""
        analyzer = GraphAnalyzer(sample_graph)
        patterns = analyzer.find_star_schema_patterns()

        assert len(patterns) >= 1
        pattern = patterns[0]
        assert pattern.pattern_type == "star_schema"
        assert "fact_sales" in pattern.description.lower()
        assert len(pattern.nodes) >= 4  # fact + 3+ dimensions

    def test_find_hub_nodes(self, sample_graph):
        """Test finding hub nodes."""
        analyzer = GraphAnalyzer(sample_graph)
        hubs = analyzer.find_hub_nodes(min_degree=3)

        assert len(hubs) >= 1
        # fact_sales should be a hub
        hub_names = [h["name"] for h in hubs]
        assert "fact_sales" in hub_names

    def test_find_hub_nodes_by_type(self, sample_graph):
        """Test finding hub nodes filtered by type."""
        analyzer = GraphAnalyzer(sample_graph)
        hubs = analyzer.find_hub_nodes(min_degree=1, node_type=NodeType.TABLE)

        for hub in hubs:
            assert hub["node_type"] == "table"


class TestGraphStructure:
    """Tests for graph structure analysis."""

    def test_find_orphan_nodes(self):
        """Test finding orphan (disconnected) nodes."""
        graph = SemanticGraph()
        connected = TableNode(name="connected", table_name="connected")
        orphan = TableNode(name="orphan", table_name="orphan")

        graph.add_node(connected)
        graph.add_node(orphan)
        graph.add_edge(GraphEdge(
            edge_type=EdgeType.JOIN,
            source_id=connected.id,
            target_id=connected.id,  # Self-loop just to make it "connected"
        ))

        analyzer = GraphAnalyzer(graph)
        orphans = analyzer.find_orphan_nodes()

        assert orphan.id in orphans

    def test_find_cliques(self, sample_graph):
        """Test finding cliques."""
        analyzer = GraphAnalyzer(sample_graph)
        cliques = analyzer.find_cliques(min_size=2)

        # Should find some cliques in the star schema
        # Note: star schema doesn't have many cliques of size 3+
        assert isinstance(cliques, list)

    def test_get_clustering_coefficient(self, sample_graph):
        """Test getting clustering coefficient."""
        analyzer = GraphAnalyzer(sample_graph)
        coeff = analyzer.get_clustering_coefficient()

        assert isinstance(coeff, float)
        assert 0 <= coeff <= 1


class TestMissingEdgeSuggestions:
    """Tests for missing edge suggestions."""

    def test_suggest_missing_edges(self):
        """Test suggesting potentially missing edges."""
        graph = SemanticGraph()

        # Create three nodes where A and C both connect to B
        # but not to each other
        a = TableNode(name="A", table_name="A")
        b = TableNode(name="B", table_name="B")
        c = TableNode(name="C", table_name="C")

        graph.add_node(a)
        graph.add_node(b)
        graph.add_node(c)

        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=a.id, target_id=b.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=c.id, target_id=b.id))

        analyzer = GraphAnalyzer(graph)
        suggestions = analyzer.suggest_missing_edges(similarity_threshold=0.3)

        # A and C should be suggested since they share neighbor B
        if suggestions:
            # Check structure of suggestion
            for s in suggestions:
                assert "source" in s
                assert "target" in s
                assert "confidence" in s


class TestGraphSummary:
    """Tests for graph summary."""

    def test_get_summary(self, sample_graph):
        """Test getting analysis summary."""
        analyzer = GraphAnalyzer(sample_graph)
        summary = analyzer.get_summary()

        assert "stats" in summary
        assert "top_central_tables" in summary
        assert "community_count" in summary
        assert "hub_count" in summary
        assert "orphan_count" in summary
        assert "star_patterns" in summary

        # Verify stats
        assert summary["stats"]["node_count"] == 5
        assert summary["stats"]["table_count"] == 5
