"""
Tests for Lineage & Impact Analysis module.

Tests cover:
- LineageTracker: Graph creation, node/edge management, column lineage
- ImpactAnalyzer: Impact analysis, dependency graphs, validation
- MCP Tools: All 11 registered tools
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path

from src.lineage import (
    # Enums
    NodeType,
    TransformationType,
    ImpactSeverity,
    ChangeType,
    # Models
    LineageColumn,
    LineageNode,
    ColumnLineage,
    LineageEdge,
    LineageGraph,
    ImpactedObject,
    ImpactResult,
    DependencyNode,
    DependencyGraph,
    LineageValidationResult,
    # Classes
    LineageTracker,
    ImpactAnalyzer,
)


# ========================================
# Fixtures
# ========================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def tracker(temp_dir):
    """Create a LineageTracker with temp directory."""
    return LineageTracker(output_dir=temp_dir)


@pytest.fixture
def analyzer(tracker):
    """Create an ImpactAnalyzer."""
    return ImpactAnalyzer(tracker)


@pytest.fixture
def sample_graph(tracker):
    """Create a sample lineage graph with nodes and edges."""
    graph_name = "test_graph"

    # Add source table
    source = tracker.add_node(
        graph_name=graph_name,
        name="DIM_ACCOUNT",
        node_type=NodeType.TABLE,
        database="ANALYTICS",
        schema_name="PUBLIC",
        columns=[
            {"name": "ACCOUNT_ID", "data_type": "NUMBER", "is_primary_key": True},
            {"name": "ACCOUNT_CODE", "data_type": "VARCHAR"},
            {"name": "ACCOUNT_NAME", "data_type": "VARCHAR"},
        ],
        description="Account dimension table",
        tags=["source", "dim"],
    )

    # Add view
    view = tracker.add_node(
        graph_name=graph_name,
        name="VW_1_TRANSLATED",
        node_type=NodeType.VIEW,
        columns=[
            {"name": "RESOLVED_VALUE", "data_type": "VARCHAR"},
        ],
        tags=["mart_factory", "VW_1"],
    )

    # Add dynamic table
    dt = tracker.add_node(
        graph_name=graph_name,
        name="DT_3_GROSS",
        node_type=NodeType.DYNAMIC_TABLE,
        columns=[
            {"name": "GROSS_AMOUNT", "data_type": "NUMBER", "is_derived": True},
        ],
        tags=["mart_factory", "DT_3"],
    )

    # Add edges with column lineage
    tracker.add_column_lineage(
        graph_name=graph_name,
        source_node=source.id,
        source_columns=["ACCOUNT_CODE", "ACCOUNT_NAME"],
        target_node=view.id,
        target_column="RESOLVED_VALUE",
        transformation_type=TransformationType.CASE,
        transformation_expression="CASE WHEN ... THEN ACCOUNT_CODE ...",
    )

    tracker.add_column_lineage(
        graph_name=graph_name,
        source_node=view.id,
        source_columns=["RESOLVED_VALUE"],
        target_node=dt.id,
        target_column="GROSS_AMOUNT",
        transformation_type=TransformationType.AGGREGATION,
        transformation_expression="SUM(AMOUNT)",
    )

    return {
        "graph_name": graph_name,
        "source_id": source.id,
        "view_id": view.id,
        "dt_id": dt.id,
    }


# ========================================
# Types Tests
# ========================================

class TestEnums:
    """Test enum values."""

    def test_node_type_values(self):
        """Test NodeType enum values."""
        assert NodeType.TABLE.value == "TABLE"
        assert NodeType.VIEW.value == "VIEW"
        assert NodeType.DYNAMIC_TABLE.value == "DYNAMIC_TABLE"
        assert NodeType.HIERARCHY.value == "HIERARCHY"
        assert NodeType.DATA_MART.value == "DATA_MART"

    def test_transformation_type_values(self):
        """Test TransformationType enum values."""
        assert TransformationType.DIRECT.value == "DIRECT"
        assert TransformationType.AGGREGATION.value == "AGGREGATION"
        assert TransformationType.CALCULATION.value == "CALCULATION"
        assert TransformationType.CASE.value == "CASE"

    def test_impact_severity_values(self):
        """Test ImpactSeverity enum values."""
        assert ImpactSeverity.CRITICAL.value == "CRITICAL"
        assert ImpactSeverity.HIGH.value == "HIGH"
        assert ImpactSeverity.MEDIUM.value == "MEDIUM"
        assert ImpactSeverity.LOW.value == "LOW"

    def test_change_type_values(self):
        """Test ChangeType enum values."""
        assert ChangeType.ADD_COLUMN.value == "ADD_COLUMN"
        assert ChangeType.REMOVE_COLUMN.value == "REMOVE_COLUMN"
        assert ChangeType.RENAME_COLUMN.value == "RENAME_COLUMN"


class TestLineageColumn:
    """Test LineageColumn model."""

    def test_create_basic_column(self):
        """Test creating a basic column."""
        col = LineageColumn(
            name="ACCOUNT_ID",
            data_type="NUMBER",
        )
        assert col.name == "ACCOUNT_ID"
        assert col.data_type == "NUMBER"
        assert col.is_primary_key is False
        assert col.is_derived is False

    def test_create_primary_key_column(self):
        """Test creating a primary key column."""
        col = LineageColumn(
            name="ACCOUNT_ID",
            data_type="NUMBER",
            is_primary_key=True,
        )
        assert col.is_primary_key is True

    def test_create_derived_column(self):
        """Test creating a derived column."""
        col = LineageColumn(
            name="TOTAL_AMOUNT",
            data_type="NUMBER",
            is_derived=True,
            expression="SUM(amount)",
        )
        assert col.is_derived is True
        assert col.expression == "SUM(amount)"

    def test_to_dict(self):
        """Test column to_dict conversion."""
        col = LineageColumn(
            name="TEST",
            data_type="VARCHAR",
            description="Test column",
        )
        d = col.to_dict()
        assert d["name"] == "TEST"
        assert d["data_type"] == "VARCHAR"
        assert d["description"] == "Test column"


class TestLineageNode:
    """Test LineageNode model."""

    def test_create_node(self):
        """Test creating a lineage node."""
        node = LineageNode(
            name="DIM_ACCOUNT",
            node_type=NodeType.TABLE,
            database="ANALYTICS",
            schema_name="PUBLIC",
        )
        assert node.name == "DIM_ACCOUNT"
        assert node.node_type == NodeType.TABLE
        assert node.database == "ANALYTICS"
        assert len(node.id) == 8  # UUID prefix

    def test_fully_qualified_name(self):
        """Test fully qualified name property."""
        node = LineageNode(
            name="DIM_ACCOUNT",
            node_type=NodeType.TABLE,
            database="ANALYTICS",
            schema_name="PUBLIC",
        )
        assert node.fully_qualified_name == "ANALYTICS.PUBLIC.DIM_ACCOUNT"

    def test_add_column(self):
        """Test adding columns to node."""
        node = LineageNode(name="TEST", node_type=NodeType.TABLE)
        col = LineageColumn(name="COL1", data_type="VARCHAR")
        node.add_column(col)
        assert len(node.columns) == 1
        assert node.columns[0].name == "COL1"

    def test_get_column(self):
        """Test getting column by name."""
        node = LineageNode(name="TEST", node_type=NodeType.TABLE)
        node.add_column(LineageColumn(name="COL1", data_type="VARCHAR"))
        node.add_column(LineageColumn(name="COL2", data_type="NUMBER"))

        col = node.get_column("COL1")
        assert col is not None
        assert col.name == "COL1"

        # Case insensitive
        col2 = node.get_column("col2")
        assert col2 is not None
        assert col2.name == "COL2"

        # Not found
        assert node.get_column("COL3") is None

    def test_to_dict(self):
        """Test node to_dict conversion."""
        node = LineageNode(
            name="TEST",
            node_type=NodeType.VIEW,
            tags=["test", "sample"],
        )
        node.add_column(LineageColumn(name="COL1"))

        d = node.to_dict()
        assert d["name"] == "TEST"
        assert d["node_type"] == "VIEW"
        assert d["column_count"] == 1
        assert d["tags"] == ["test", "sample"]


class TestLineageGraph:
    """Test LineageGraph model."""

    def test_create_graph(self):
        """Test creating a lineage graph."""
        graph = LineageGraph(
            name="test_graph",
            description="Test lineage graph",
        )
        assert graph.name == "test_graph"
        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0

    def test_add_and_get_node(self):
        """Test adding and getting nodes."""
        graph = LineageGraph(name="test")
        node = LineageNode(name="TABLE1", node_type=NodeType.TABLE)
        graph.add_node(node)

        assert len(graph.nodes) == 1
        assert graph.get_node(node.id) == node

    def test_get_node_by_name(self):
        """Test getting node by name."""
        graph = LineageGraph(name="test")
        node = LineageNode(name="TABLE1", node_type=NodeType.TABLE)
        graph.add_node(node)

        found = graph.get_node_by_name("TABLE1")
        assert found == node

        # Case insensitive
        found2 = graph.get_node_by_name("table1")
        assert found2 == node

    def test_add_edge(self):
        """Test adding edges."""
        graph = LineageGraph(name="test")
        node1 = LineageNode(name="SOURCE", node_type=NodeType.TABLE)
        node2 = LineageNode(name="TARGET", node_type=NodeType.VIEW)
        graph.add_node(node1)
        graph.add_node(node2)

        edge = LineageEdge(
            source_node_id=node1.id,
            target_node_id=node2.id,
        )
        graph.add_edge(edge)

        assert len(graph.edges) == 1

    def test_get_upstream_nodes(self):
        """Test getting upstream nodes."""
        graph = LineageGraph(name="test")
        source = LineageNode(name="SOURCE", node_type=NodeType.TABLE)
        target = LineageNode(name="TARGET", node_type=NodeType.VIEW)
        graph.add_node(source)
        graph.add_node(target)

        edge = LineageEdge(source_node_id=source.id, target_node_id=target.id)
        graph.add_edge(edge)

        upstream = graph.get_upstream_nodes(target.id)
        assert len(upstream) == 1
        assert upstream[0].name == "SOURCE"

    def test_get_downstream_nodes(self):
        """Test getting downstream nodes."""
        graph = LineageGraph(name="test")
        source = LineageNode(name="SOURCE", node_type=NodeType.TABLE)
        target = LineageNode(name="TARGET", node_type=NodeType.VIEW)
        graph.add_node(source)
        graph.add_node(target)

        edge = LineageEdge(source_node_id=source.id, target_node_id=target.id)
        graph.add_edge(edge)

        downstream = graph.get_downstream_nodes(source.id)
        assert len(downstream) == 1
        assert downstream[0].name == "TARGET"

    def test_get_all_upstream(self):
        """Test recursive upstream traversal."""
        graph = LineageGraph(name="test")
        n1 = LineageNode(name="N1", node_type=NodeType.TABLE)
        n2 = LineageNode(name="N2", node_type=NodeType.VIEW)
        n3 = LineageNode(name="N3", node_type=NodeType.DYNAMIC_TABLE)
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_node(n3)

        graph.add_edge(LineageEdge(source_node_id=n1.id, target_node_id=n2.id))
        graph.add_edge(LineageEdge(source_node_id=n2.id, target_node_id=n3.id))

        all_upstream = graph.get_all_upstream(n3.id)
        assert n1.id in all_upstream
        assert n2.id in all_upstream
        assert n3.id in all_upstream

    def test_get_all_downstream(self):
        """Test recursive downstream traversal."""
        graph = LineageGraph(name="test")
        n1 = LineageNode(name="N1", node_type=NodeType.TABLE)
        n2 = LineageNode(name="N2", node_type=NodeType.VIEW)
        n3 = LineageNode(name="N3", node_type=NodeType.DYNAMIC_TABLE)
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_node(n3)

        graph.add_edge(LineageEdge(source_node_id=n1.id, target_node_id=n2.id))
        graph.add_edge(LineageEdge(source_node_id=n2.id, target_node_id=n3.id))

        all_downstream = graph.get_all_downstream(n1.id)
        assert n1.id in all_downstream
        assert n2.id in all_downstream
        assert n3.id in all_downstream


class TestImpactResult:
    """Test ImpactResult model."""

    def test_create_impact_result(self):
        """Test creating an impact result."""
        result = ImpactResult(
            change_type=ChangeType.REMOVE_COLUMN,
            source_node_id="abc123",
            source_node_name="DIM_ACCOUNT",
            change_description="Remove column ACCOUNT_CODE",
        )
        assert result.change_type == ChangeType.REMOVE_COLUMN
        assert result.total_impacted == 0

    def test_add_impact(self):
        """Test adding impacted objects."""
        result = ImpactResult(
            change_type=ChangeType.REMOVE_COLUMN,
            source_node_id="abc",
            source_node_name="SOURCE",
            change_description="Test",
        )

        impact = ImpactedObject(
            node_id="xyz",
            node_name="TARGET",
            node_type=NodeType.VIEW,
            impact_severity=ImpactSeverity.HIGH,
            impact_description="Depends on removed column",
        )
        result.add_impact(impact)

        assert result.total_impacted == 1
        assert result.high_count == 1
        assert result.max_severity == ImpactSeverity.HIGH

    def test_max_severity(self):
        """Test max severity calculation."""
        result = ImpactResult(
            change_type=ChangeType.REMOVE_COLUMN,
            source_node_id="abc",
            source_node_name="SOURCE",
            change_description="Test",
        )

        # Add medium impact
        result.add_impact(ImpactedObject(
            node_id="1", node_name="T1", node_type=NodeType.VIEW,
            impact_severity=ImpactSeverity.MEDIUM, impact_description="Test",
        ))
        assert result.max_severity == ImpactSeverity.MEDIUM

        # Add critical impact
        result.add_impact(ImpactedObject(
            node_id="2", node_name="T2", node_type=NodeType.DATA_MART,
            impact_severity=ImpactSeverity.CRITICAL, impact_description="Test",
        ))
        assert result.max_severity == ImpactSeverity.CRITICAL


class TestDependencyGraph:
    """Test DependencyGraph model."""

    def test_create_dependency_graph(self):
        """Test creating a dependency graph."""
        dg = DependencyGraph(
            root_node_id="abc",
            direction="downstream",
        )
        assert dg.root_node_id == "abc"
        assert dg.direction == "downstream"

    def test_to_mermaid(self):
        """Test Mermaid export."""
        dg = DependencyGraph()
        dg.nodes.append(DependencyNode(
            id="n1", name="Source", node_type=NodeType.TABLE, level=0
        ))
        dg.nodes.append(DependencyNode(
            id="n2", name="Target", node_type=NodeType.VIEW, level=1
        ))
        dg.edges.append({"source": "n1", "target": "n2"})

        mermaid = dg.to_mermaid()
        assert "graph TD" in mermaid
        assert "n1[Source]" in mermaid
        assert "n2[Target]" in mermaid
        assert "n1 --> n2" in mermaid

    def test_to_dot(self):
        """Test DOT export."""
        dg = DependencyGraph()
        dg.nodes.append(DependencyNode(
            id="n1", name="Source", node_type=NodeType.TABLE, level=0
        ))
        dg.nodes.append(DependencyNode(
            id="n2", name="Target", node_type=NodeType.VIEW, level=1
        ))
        dg.edges.append({"source": "n1", "target": "n2"})

        dot = dg.to_dot()
        assert "digraph G" in dot
        assert '"n1"' in dot
        assert '"n2"' in dot
        assert '"n1" -> "n2"' in dot


# ========================================
# LineageTracker Tests
# ========================================

class TestLineageTracker:
    """Test LineageTracker class."""

    def test_create_graph(self, tracker):
        """Test creating a new graph."""
        graph = tracker.create_graph("test_graph", "Test description")
        assert graph.name == "test_graph"
        assert graph.description == "Test description"

    def test_create_duplicate_graph_fails(self, tracker):
        """Test that creating duplicate graph raises error."""
        tracker.create_graph("test_graph")
        with pytest.raises(ValueError, match="already exists"):
            tracker.create_graph("test_graph")

    def test_get_graph(self, tracker):
        """Test getting a graph."""
        tracker.create_graph("test_graph")
        graph = tracker.get_graph("test_graph")
        assert graph is not None
        assert graph.name == "test_graph"

    def test_get_or_create_graph(self, tracker):
        """Test get_or_create_graph method."""
        # First call creates
        graph1 = tracker.get_or_create_graph("new_graph")
        assert graph1 is not None

        # Second call returns existing
        graph2 = tracker.get_or_create_graph("new_graph")
        assert graph1.id == graph2.id

    def test_list_graphs(self, tracker):
        """Test listing graphs."""
        tracker.create_graph("graph1")
        tracker.create_graph("graph2")

        graphs = tracker.list_graphs()
        assert len(graphs) == 2

    def test_add_node(self, tracker):
        """Test adding a node."""
        node = tracker.add_node(
            graph_name="test",
            name="DIM_ACCOUNT",
            node_type=NodeType.TABLE,
            database="ANALYTICS",
            schema_name="PUBLIC",
        )

        assert node.name == "DIM_ACCOUNT"
        assert node.node_type == NodeType.TABLE

    def test_add_node_with_columns(self, tracker):
        """Test adding a node with columns."""
        node = tracker.add_node(
            graph_name="test",
            name="DIM_ACCOUNT",
            node_type=NodeType.TABLE,
            columns=[
                {"name": "ID", "data_type": "NUMBER", "is_primary_key": True},
                {"name": "NAME", "data_type": "VARCHAR"},
            ],
        )

        assert len(node.columns) == 2
        assert node.columns[0].is_primary_key is True

    def test_add_edge(self, tracker):
        """Test adding an edge."""
        source = tracker.add_node("test", "SOURCE", NodeType.TABLE)
        target = tracker.add_node("test", "TARGET", NodeType.VIEW)

        edge = tracker.add_edge(
            graph_name="test",
            source_node=source.id,
            target_node=target.id,
            transformation_type=TransformationType.JOIN,
        )

        assert edge.source_node_id == source.id
        assert edge.target_node_id == target.id

    def test_add_edge_by_name(self, tracker):
        """Test adding an edge using node names."""
        tracker.add_node("test", "SOURCE", NodeType.TABLE)
        tracker.add_node("test", "TARGET", NodeType.VIEW)

        edge = tracker.add_edge(
            graph_name="test",
            source_node="SOURCE",
            target_node="TARGET",
        )

        assert edge is not None

    def test_add_column_lineage(self, tracker):
        """Test adding column lineage."""
        source = tracker.add_node("test", "SOURCE", NodeType.TABLE)
        target = tracker.add_node("test", "TARGET", NodeType.VIEW)

        lineage = tracker.add_column_lineage(
            graph_name="test",
            source_node=source.id,
            source_columns=["COL1", "COL2"],
            target_node=target.id,
            target_column="TARGET_COL",
            transformation_type=TransformationType.CALCULATION,
            transformation_expression="COL1 + COL2",
        )

        assert lineage.source_columns == ["COL1", "COL2"]
        assert lineage.target_column == "TARGET_COL"

    def test_get_column_lineage_upstream(self, tracker, sample_graph):
        """Test getting upstream column lineage."""
        lineage = tracker.get_column_lineage(
            graph_name=sample_graph["graph_name"],
            node=sample_graph["view_id"],
            column="RESOLVED_VALUE",
            direction="upstream",
        )

        assert len(lineage) == 1
        assert "ACCOUNT_CODE" in lineage[0]["source_columns"]

    def test_get_column_lineage_downstream(self, tracker, sample_graph):
        """Test getting downstream column lineage."""
        lineage = tracker.get_column_lineage(
            graph_name=sample_graph["graph_name"],
            node=sample_graph["view_id"],
            column="RESOLVED_VALUE",
            direction="downstream",
        )

        assert len(lineage) == 1
        assert lineage[0]["target_column"] == "GROSS_AMOUNT"

    def test_get_table_lineage(self, tracker, sample_graph):
        """Test getting table lineage."""
        result = tracker.get_table_lineage(
            graph_name=sample_graph["graph_name"],
            node=sample_graph["view_id"],
            direction="both",
        )

        assert len(result["upstream"]) == 1
        assert len(result["downstream"]) == 1

    def test_persistence(self, temp_dir):
        """Test that lineage data persists to disk."""
        # Create and populate tracker
        tracker1 = LineageTracker(output_dir=temp_dir)
        tracker1.add_node("test", "NODE1", NodeType.TABLE)
        tracker1.add_node("test", "NODE2", NodeType.VIEW)
        tracker1.add_edge("test", "NODE1", "NODE2")

        # Create new tracker and load
        tracker2 = LineageTracker(output_dir=temp_dir)
        graph = tracker2.get_graph("test")

        assert graph is not None
        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1


# ========================================
# ImpactAnalyzer Tests
# ========================================

class TestImpactAnalyzer:
    """Test ImpactAnalyzer class."""

    def test_analyze_column_removal(self, analyzer, tracker, sample_graph):
        """Test analyzing column removal impact."""
        result = analyzer.analyze_column_removal(
            graph_name=sample_graph["graph_name"],
            node="DIM_ACCOUNT",
            column="ACCOUNT_CODE",
        )

        assert result.change_type == ChangeType.REMOVE_COLUMN
        assert result.total_impacted > 0

    def test_analyze_column_rename(self, analyzer, tracker, sample_graph):
        """Test analyzing column rename impact."""
        result = analyzer.analyze_column_rename(
            graph_name=sample_graph["graph_name"],
            node="DIM_ACCOUNT",
            old_column="ACCOUNT_CODE",
            new_column="ACCOUNT_NUMBER",
        )

        assert result.change_type == ChangeType.RENAME_COLUMN
        assert "ACCOUNT_CODE" in result.change_description
        assert "ACCOUNT_NUMBER" in result.change_description

    def test_analyze_hierarchy_change(self, analyzer, tracker, sample_graph):
        """Test analyzing hierarchy change impact."""
        result = analyzer.analyze_hierarchy_change(
            graph_name=sample_graph["graph_name"],
            hierarchy_node="DIM_ACCOUNT",
            change_type=ChangeType.REMOVE_NODE,
        )

        assert result.change_type == ChangeType.REMOVE_NODE

    def test_get_downstream_impact(self, analyzer, tracker, sample_graph):
        """Test getting downstream impact."""
        result = analyzer.get_downstream_impact(
            graph_name=sample_graph["graph_name"],
            node="DIM_ACCOUNT",
        )

        assert "downstream_count" in result
        assert result["downstream_count"] == 2  # VW_1 and DT_3

    def test_get_upstream_dependencies(self, analyzer, tracker, sample_graph):
        """Test getting upstream dependencies."""
        result = analyzer.get_upstream_dependencies(
            graph_name=sample_graph["graph_name"],
            node="DT_3_GROSS",
        )

        assert "dependency_count" in result
        assert result["dependency_count"] == 2  # VW_1 and DIM_ACCOUNT

    def test_build_dependency_graph_downstream(self, analyzer, tracker, sample_graph):
        """Test building downstream dependency graph."""
        dep_graph = analyzer.build_dependency_graph(
            graph_name=sample_graph["graph_name"],
            node="DIM_ACCOUNT",
            direction="downstream",
        )

        assert dep_graph.total_nodes == 3  # DIM_ACCOUNT, VW_1, DT_3
        assert dep_graph.direction == "downstream"

    def test_build_dependency_graph_upstream(self, analyzer, tracker, sample_graph):
        """Test building upstream dependency graph."""
        dep_graph = analyzer.build_dependency_graph(
            graph_name=sample_graph["graph_name"],
            node="DT_3_GROSS",
            direction="upstream",
        )

        assert dep_graph.total_nodes == 3
        assert dep_graph.direction == "upstream"

    def test_validate_lineage(self, analyzer, tracker, sample_graph):
        """Test lineage validation."""
        result = analyzer.validate_lineage(sample_graph["graph_name"])

        assert result.is_valid is True
        assert result.node_count == 3
        assert result.edge_count == 2
        assert result.completeness_score > 0

    def test_validate_lineage_with_orphan(self, analyzer, tracker):
        """Test validation detects orphan nodes."""
        tracker.add_node("test", "ORPHAN", NodeType.TABLE)

        result = analyzer.validate_lineage("test")

        assert len(result.orphan_nodes) == 1
        assert len(result.warnings) > 0

    def test_severity_rules(self, analyzer):
        """Test severity rules are applied correctly."""
        # DATA_MART removal is CRITICAL
        severity = analyzer._get_severity(
            ChangeType.REMOVE_COLUMN,
            NodeType.DATA_MART,
            distance=0,
        )
        assert severity == ImpactSeverity.CRITICAL

        # VIEW removal is HIGH
        severity = analyzer._get_severity(
            ChangeType.REMOVE_COLUMN,
            NodeType.VIEW,
            distance=0,
        )
        assert severity == ImpactSeverity.HIGH

        # Distance reduces severity
        severity = analyzer._get_severity(
            ChangeType.REMOVE_COLUMN,
            NodeType.DATA_MART,
            distance=5,
        )
        assert severity == ImpactSeverity.HIGH  # Reduced from CRITICAL


# ========================================
# MCP Tools Tests
# ========================================

class TestMCPTools:
    """Test MCP tool registration and functionality."""

    def test_register_lineage_tools(self, temp_dir):
        """Test that tools are registered correctly."""
        from src.lineage.mcp_tools import register_lineage_tools

        # Mock MCP
        class MockMCP:
            def __init__(self):
                self.tools = []

            def tool(self):
                def decorator(func):
                    self.tools.append(func.__name__)
                    return func
                return decorator

        mcp = MockMCP()
        result = register_lineage_tools(mcp)

        assert result["tools_registered"] == 11
        assert len(result["tools"]) == 11

        # Check specific tools
        assert "add_lineage_node" in result["tools"]
        assert "track_column_lineage" in result["tools"]
        assert "analyze_change_impact" in result["tools"]
        assert "build_dependency_graph" in result["tools"]
        assert "export_lineage_diagram" in result["tools"]
        assert "list_lineage_graphs" in result["tools"]

    def test_add_lineage_node_tool(self, temp_dir):
        """Test add_lineage_node MCP tool."""
        from src.lineage.mcp_tools import register_lineage_tools

        class MockMCP:
            def __init__(self):
                self.registered = {}

            def tool(self):
                def decorator(func):
                    self.registered[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_lineage_tools(mcp)

        # Call the tool
        result = mcp.registered["add_lineage_node"](
            graph_name="test",
            node_name="TEST_TABLE",
            node_type="TABLE",
            database="DB",
            schema_name="SCHEMA",
        )

        assert result["success"] is True
        assert result["node_name"] == "TEST_TABLE"

    def test_validate_lineage_tool(self, temp_dir):
        """Test validate_lineage MCP tool."""
        from src.lineage.mcp_tools import register_lineage_tools

        class MockMCP:
            def __init__(self):
                self.registered = {}

            def tool(self):
                def decorator(func):
                    self.registered[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_lineage_tools(mcp)

        # Add a node first
        mcp.registered["add_lineage_node"](
            graph_name="test",
            node_name="TABLE1",
            node_type="TABLE",
        )

        # Validate
        result = mcp.registered["validate_lineage"](graph_name="test")

        assert result["success"] is True
        assert "completeness_score" in result


# ========================================
# Integration Tests
# ========================================

class TestIntegration:
    """Integration tests for complete workflows."""

    def test_complete_lineage_workflow(self, tracker, analyzer):
        """Test a complete lineage tracking workflow."""
        graph_name = "integration_test"

        # 1. Create source tables
        gl = tracker.add_node(
            graph_name=graph_name,
            name="GL_TRANSACTIONS",
            node_type=NodeType.TABLE,
            database="ANALYTICS",
            schema_name="RAW",
            columns=[
                {"name": "TRANSACTION_ID", "data_type": "NUMBER", "is_primary_key": True},
                {"name": "ACCOUNT_CODE", "data_type": "VARCHAR"},
                {"name": "AMOUNT", "data_type": "NUMBER"},
            ],
        )

        account = tracker.add_node(
            graph_name=graph_name,
            name="DIM_ACCOUNT",
            node_type=NodeType.TABLE,
            database="ANALYTICS",
            schema_name="DIM",
            columns=[
                {"name": "ACCOUNT_CODE", "data_type": "VARCHAR", "is_primary_key": True},
                {"name": "ACCOUNT_NAME", "data_type": "VARCHAR"},
            ],
        )

        # 2. Create hierarchy
        hierarchy = tracker.add_node(
            graph_name=graph_name,
            name="PL_HIERARCHY",
            node_type=NodeType.HIERARCHY,
        )

        # 3. Create mart pipeline
        vw1 = tracker.add_node(graph_name, "VW_1_PL", NodeType.VIEW)
        dt2 = tracker.add_node(graph_name, "DT_2_PL", NodeType.DYNAMIC_TABLE)
        dt3 = tracker.add_node(graph_name, "DT_3_PL", NodeType.DATA_MART)

        # 4. Add lineage
        tracker.add_column_lineage(graph_name, gl.id, ["ACCOUNT_CODE"], vw1.id, "RESOLVED_KEY", TransformationType.JOIN)
        tracker.add_column_lineage(graph_name, gl.id, ["AMOUNT"], vw1.id, "AMOUNT", TransformationType.DIRECT)
        tracker.add_column_lineage(graph_name, account.id, ["ACCOUNT_NAME"], vw1.id, "ACCOUNT_LABEL", TransformationType.JOIN)
        tracker.add_edge(graph_name, hierarchy.id, vw1.id, TransformationType.JOIN)
        tracker.add_edge(graph_name, vw1.id, dt2.id, TransformationType.UNPIVOT)
        tracker.add_edge(graph_name, dt2.id, dt3.id, TransformationType.AGGREGATION)

        # 5. Analyze impact of removing a source column
        impact = analyzer.analyze_column_removal(graph_name, "GL_TRANSACTIONS", "ACCOUNT_CODE")

        assert impact.total_impacted > 0

        # 6. Get dependency graph
        dep_graph = analyzer.build_dependency_graph(graph_name, "GL_TRANSACTIONS", "downstream")

        assert dep_graph.total_nodes >= 4  # GL -> VW1 -> DT2 -> DT3

        # 7. Validate lineage
        validation = analyzer.validate_lineage(graph_name)

        assert validation.is_valid is True
        assert validation.node_count == 6

    def test_mermaid_diagram_generation(self, tracker, analyzer):
        """Test generating Mermaid diagrams."""
        graph_name = "diagram_test"

        # Create simple pipeline
        source = tracker.add_node(graph_name, "SOURCE_TABLE", NodeType.TABLE)
        view = tracker.add_node(graph_name, "STAGING_VIEW", NodeType.VIEW)
        mart = tracker.add_node(graph_name, "DATA_MART", NodeType.DATA_MART)

        tracker.add_edge(graph_name, source.id, view.id)
        tracker.add_edge(graph_name, view.id, mart.id)

        # Generate diagram
        dep_graph = analyzer.build_dependency_graph(graph_name, "SOURCE_TABLE", "downstream")
        mermaid = dep_graph.to_mermaid()

        assert "graph TD" in mermaid
        assert "SOURCE_TABLE" in mermaid
        assert "DATA_MART" in mermaid
        assert "-->" in mermaid

    def test_dot_diagram_generation(self, tracker, analyzer):
        """Test generating DOT diagrams."""
        graph_name = "dot_test"

        # Create simple pipeline
        source = tracker.add_node(graph_name, "SOURCE", NodeType.TABLE)
        target = tracker.add_node(graph_name, "TARGET", NodeType.VIEW)

        tracker.add_edge(graph_name, source.id, target.id)

        # Generate diagram
        dep_graph = analyzer.build_dependency_graph(graph_name, "SOURCE", "downstream")
        dot = dep_graph.to_dot()

        assert "digraph G" in dot
        assert "rankdir=TB" in dot
        assert "fillcolor" in dot
