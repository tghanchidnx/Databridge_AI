"""
Tests for the SemanticGraph module.
"""

import pytest

from databridge_discovery.graph.semantic_graph import SemanticGraph
from databridge_discovery.graph.node_types import (
    ColumnNode,
    EdgeType,
    GraphEdge,
    HierarchyNode,
    NodeType,
    TableNode,
)


class TestSemanticGraph:
    """Tests for SemanticGraph class."""

    def test_create_empty_graph(self):
        """Test creating an empty graph."""
        graph = SemanticGraph(name="test_graph")
        assert graph.name == "test_graph"
        assert graph.node_count == 0
        assert graph.edge_count == 0

    def test_add_table_node(self):
        """Test adding a table node."""
        graph = SemanticGraph()
        table = TableNode(
            name="customers",
            table_name="customers",
            schema_name="public",
            database="main",
        )
        node_id = graph.add_node(table)

        assert node_id == table.id
        assert graph.node_count == 1

        retrieved = graph.get_node(node_id)
        assert retrieved is not None
        assert retrieved.name == "customers"

    def test_add_column_node(self):
        """Test adding a column node."""
        graph = SemanticGraph()
        column = ColumnNode(
            name="customer_id",
            column_name="customer_id",
            data_type="INTEGER",
            is_primary_key=True,
        )
        node_id = graph.add_node(column)

        assert node_id == column.id
        assert graph.node_count == 1

        retrieved = graph.get_node(node_id)
        assert retrieved is not None
        assert retrieved.name == "customer_id"

    def test_add_edge(self):
        """Test adding an edge between nodes."""
        graph = SemanticGraph()

        # Add two tables
        orders = TableNode(name="orders", table_name="orders")
        customers = TableNode(name="customers", table_name="customers")
        graph.add_node(orders)
        graph.add_node(customers)

        # Add join edge
        edge = GraphEdge(
            edge_type=EdgeType.JOIN,
            source_id=orders.id,
            target_id=customers.id,
            join_condition="orders.customer_id = customers.id",
            join_type="INNER",
        )
        edge_id = graph.add_edge(edge)

        assert edge_id == edge.id
        assert graph.edge_count == 1

    def test_get_node_by_name(self):
        """Test getting a node by name."""
        graph = SemanticGraph()
        table = TableNode(name="products", table_name="products")
        graph.add_node(table)

        # Case-insensitive lookup
        retrieved = graph.get_node_by_name("PRODUCTS")
        assert retrieved is not None
        assert retrieved.id == table.id

        # Non-existent
        assert graph.get_node_by_name("nonexistent") is None

    def test_find_nodes_by_name_exact(self):
        """Test finding nodes by name with exact match."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="customers", table_name="customers"))
        graph.add_node(TableNode(name="customer_orders", table_name="customer_orders"))

        results = graph.find_nodes_by_name("customers", fuzzy=False)
        assert len(results) == 1
        assert results[0].name == "customers"

    def test_find_nodes_by_name_fuzzy(self):
        """Test finding nodes by name with fuzzy match."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="customers", table_name="customers"))
        graph.add_node(TableNode(name="customer_orders", table_name="customer_orders"))
        graph.add_node(TableNode(name="products", table_name="products"))

        results = graph.find_nodes_by_name("customer", fuzzy=True)
        assert len(results) == 2
        names = {r.name for r in results}
        assert "customers" in names
        assert "customer_orders" in names

    def test_get_nodes_by_type(self):
        """Test getting nodes by type."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="table1", table_name="table1"))
        graph.add_node(TableNode(name="table2", table_name="table2"))
        graph.add_node(ColumnNode(name="col1", column_name="col1"))

        tables = graph.get_nodes_by_type(NodeType.TABLE)
        assert len(tables) == 2

        columns = graph.get_nodes_by_type(NodeType.COLUMN)
        assert len(columns) == 1

        # Test with string type
        tables_str = graph.get_nodes_by_type("table")
        assert len(tables_str) == 2

    def test_get_all_nodes(self):
        """Test getting all nodes."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="table1", table_name="table1"))
        graph.add_node(ColumnNode(name="col1", column_name="col1"))
        graph.add_node(HierarchyNode(name="hier1"))

        all_nodes = graph.get_all_nodes()
        assert len(all_nodes) == 3

    def test_remove_node(self):
        """Test removing a node."""
        graph = SemanticGraph()
        table = TableNode(name="to_remove", table_name="to_remove")
        graph.add_node(table)

        assert graph.node_count == 1
        result = graph.remove_node(table.id)
        assert result is True
        assert graph.node_count == 0
        assert graph.get_node(table.id) is None

    def test_get_neighbors(self):
        """Test getting node neighbors."""
        graph = SemanticGraph()

        # Create a simple graph: A -> B -> C
        a = TableNode(name="A", table_name="A")
        b = TableNode(name="B", table_name="B")
        c = TableNode(name="C", table_name="C")
        graph.add_node(a)
        graph.add_node(b)
        graph.add_node(c)

        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=a.id, target_id=b.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=b.id, target_id=c.id))

        # B has neighbors A and C
        neighbors = graph.get_neighbors(b.id)
        assert len(neighbors) == 2
        assert a.id in neighbors
        assert c.id in neighbors

    def test_get_related_nodes(self):
        """Test getting related nodes up to a depth."""
        graph = SemanticGraph()

        # Create chain: A -> B -> C -> D
        nodes = [TableNode(name=n, table_name=n) for n in ["A", "B", "C", "D"]]
        for node in nodes:
            graph.add_node(node)

        for i in range(len(nodes) - 1):
            graph.add_edge(GraphEdge(
                edge_type=EdgeType.JOIN,
                source_id=nodes[i].id,
                target_id=nodes[i+1].id,
            ))

        # From A, depth 2 should get B and C
        related = graph.get_related_nodes(nodes[0].id, max_depth=2)
        assert len(related) == 2
        assert nodes[1].id in related
        assert nodes[2].id in related

    def test_find_join_paths(self):
        """Test finding join paths between tables."""
        graph = SemanticGraph()

        # Create: orders -> customers, orders -> products
        orders = TableNode(name="orders", table_name="orders")
        customers = TableNode(name="customers", table_name="customers")
        products = TableNode(name="products", table_name="products")

        graph.add_node(orders)
        graph.add_node(customers)
        graph.add_node(products)

        graph.add_edge(GraphEdge(
            edge_type=EdgeType.JOIN,
            source_id=orders.id,
            target_id=customers.id,
        ))
        graph.add_edge(GraphEdge(
            edge_type=EdgeType.JOIN,
            source_id=orders.id,
            target_id=products.id,
        ))

        # Find path from customers to products
        paths = graph.find_join_paths("customers", "products")
        assert len(paths) >= 1
        # Path should go through orders
        assert len(paths[0]) == 3

    def test_find_shortest_path(self):
        """Test finding shortest path."""
        graph = SemanticGraph()

        # Create triangle: A - B - C - A
        a = TableNode(name="A", table_name="A")
        b = TableNode(name="B", table_name="B")
        c = TableNode(name="C", table_name="C")
        graph.add_node(a)
        graph.add_node(b)
        graph.add_node(c)

        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=a.id, target_id=b.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=b.id, target_id=c.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=a.id, target_id=c.id))

        # Shortest from A to C should be direct
        path = graph.find_shortest_path(a.id, c.id)
        assert path is not None
        assert len(path) == 2

    def test_get_connected_components(self):
        """Test getting connected components."""
        graph = SemanticGraph()

        # Create two disconnected components
        a = TableNode(name="A", table_name="A")
        b = TableNode(name="B", table_name="B")
        c = TableNode(name="C", table_name="C")
        d = TableNode(name="D", table_name="D")

        graph.add_node(a)
        graph.add_node(b)
        graph.add_node(c)
        graph.add_node(d)

        # Connect A-B and C-D
        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=a.id, target_id=b.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=c.id, target_id=d.id))

        components = graph.get_connected_components()
        assert len(components) == 2

    def test_get_stats(self):
        """Test getting graph statistics."""
        graph = SemanticGraph()

        # Add some nodes and edges
        t1 = TableNode(name="table1", table_name="table1")
        t2 = TableNode(name="table2", table_name="table2")
        c1 = ColumnNode(name="col1", column_name="col1")

        graph.add_node(t1)
        graph.add_node(t2)
        graph.add_node(c1)

        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=t1.id, target_id=t2.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.COLUMN_OF, source_id=c1.id, target_id=t1.id))

        stats = graph.get_stats()
        assert stats.node_count == 3
        assert stats.edge_count == 2
        assert stats.table_count == 2
        assert stats.column_count == 1
        assert stats.join_count == 1

    def test_to_dict_and_from_dict(self):
        """Test serialization and deserialization."""
        graph = SemanticGraph(name="test")
        t1 = TableNode(name="table1", table_name="table1")
        graph.add_node(t1)

        # Serialize
        data = graph.to_dict()
        assert data["name"] == "test"
        assert len(data["nodes"]) == 1

        # Deserialize
        restored = SemanticGraph.from_dict(data)
        assert restored.name == "test"
        assert restored.node_count == 1

    def test_to_json(self):
        """Test JSON export."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="test", table_name="test"))

        # With embeddings
        json_data = graph.to_json(include_embeddings=True)
        assert isinstance(json_data, dict)

        # Without embeddings
        json_data = graph.to_json(include_embeddings=False)
        assert isinstance(json_data, dict)

    def test_to_graphml(self):
        """Test GraphML export."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="test", table_name="test"))

        graphml = graph.to_graphml()
        assert isinstance(graphml, str)
        assert "graphml" in graphml.lower()

    def test_merge_graph(self):
        """Test merging two graphs."""
        graph1 = SemanticGraph()
        graph1.add_node(TableNode(name="A", table_name="A"))

        graph2 = SemanticGraph()
        graph2.add_node(TableNode(name="B", table_name="B"))
        graph2.add_node(TableNode(name="C", table_name="C"))

        added = graph1.merge_graph(graph2)
        assert added == 2
        assert graph1.node_count == 3

    def test_iter_nodes(self):
        """Test iterating over nodes."""
        graph = SemanticGraph()
        graph.add_node(TableNode(name="t1", table_name="t1"))
        graph.add_node(TableNode(name="t2", table_name="t2"))
        graph.add_node(ColumnNode(name="c1", column_name="c1"))

        # All nodes
        all_nodes = list(graph.iter_nodes())
        assert len(all_nodes) == 3

        # Only tables
        tables = list(graph.iter_nodes(NodeType.TABLE))
        assert len(tables) == 2

    def test_iter_edges(self):
        """Test iterating over edges."""
        graph = SemanticGraph()
        t1 = TableNode(name="t1", table_name="t1")
        t2 = TableNode(name="t2", table_name="t2")
        graph.add_node(t1)
        graph.add_node(t2)

        graph.add_edge(GraphEdge(edge_type=EdgeType.JOIN, source_id=t1.id, target_id=t2.id))
        graph.add_edge(GraphEdge(edge_type=EdgeType.FOREIGN_KEY, source_id=t1.id, target_id=t2.id))

        # All edges
        all_edges = list(graph.iter_edges())
        assert len(all_edges) == 2

        # Only joins
        joins = list(graph.iter_edges(EdgeType.JOIN))
        assert len(joins) == 1


class TestSemanticGraphWithParsedQuery:
    """Tests for adding nodes from parsed queries."""

    def test_add_from_parsed_query(self):
        """Test adding nodes from a parsed query."""
        from databridge_discovery.parser.sql_parser import SQLParser

        sql = """
        SELECT o.id, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        """

        parser = SQLParser()
        parsed = parser.parse(sql)

        graph = SemanticGraph()
        added = graph.add_from_parsed_query(parsed)

        assert added["tables"] == 2
        assert added["columns"] >= 2
        assert added["joins"] == 1

        # Check tables exist
        assert graph.get_node_by_name("orders") is not None or graph.get_node_by_name("o") is not None
        assert graph.get_node_by_name("customers") is not None or graph.get_node_by_name("c") is not None
