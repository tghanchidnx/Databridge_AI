"""
Lineage Tracker.

Tracks data lineage across DataBridge objects:
- Column-level lineage tracking
- Auto-discovery from hierarchy mappings
- Integration with Mart Factory pipeline
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .types import (
    LineageGraph,
    LineageNode,
    LineageEdge,
    LineageColumn,
    ColumnLineage,
    NodeType,
    TransformationType,
)

logger = logging.getLogger(__name__)


class LineageTracker:
    """Tracks data lineage across DataBridge objects."""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize the lineage tracker.

        Args:
            output_dir: Directory for storing lineage data
        """
        self.output_dir = Path(output_dir) if output_dir else Path("data/lineage")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._graphs: Dict[str, LineageGraph] = {}
        self._load()

    def create_graph(
        self,
        name: str = "default",
        description: Optional[str] = None,
    ) -> LineageGraph:
        """
        Create a new lineage graph.

        Args:
            name: Graph name
            description: Graph description

        Returns:
            Created LineageGraph
        """
        if name in self._graphs:
            raise ValueError(f"Graph '{name}' already exists")

        graph = LineageGraph(
            name=name,
            description=description,
        )
        self._graphs[name] = graph
        self._save()

        logger.info(f"Created lineage graph: {name}")
        return graph

    def get_graph(self, name: str = "default") -> Optional[LineageGraph]:
        """Get a lineage graph by name."""
        return self._graphs.get(name)

    def get_or_create_graph(self, name: str = "default") -> LineageGraph:
        """Get or create a lineage graph."""
        if name not in self._graphs:
            return self.create_graph(name)
        return self._graphs[name]

    def list_graphs(self) -> List[Dict[str, Any]]:
        """List all lineage graphs."""
        return [g.to_dict() for g in self._graphs.values()]

    def add_node(
        self,
        graph_name: str,
        name: str,
        node_type: NodeType,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        columns: Optional[List[Dict[str, Any]]] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> LineageNode:
        """
        Add a node to the lineage graph.

        Args:
            graph_name: Name of the graph
            name: Node name (object name)
            node_type: Type of node
            database: Database name
            schema_name: Schema name
            columns: List of column definitions
            description: Node description
            tags: Node tags

        Returns:
            Created LineageNode
        """
        graph = self._graphs.get(graph_name)
        if not graph:
            graph = self.create_graph(graph_name)

        # Create node
        node = LineageNode(
            name=name,
            node_type=node_type,
            database=database,
            schema_name=schema_name,
            description=description,
            tags=tags or [],
        )

        # Add columns
        if columns:
            for col_data in columns:
                column = LineageColumn(
                    name=col_data.get("name", ""),
                    data_type=col_data.get("data_type"),
                    description=col_data.get("description"),
                    is_primary_key=col_data.get("is_primary_key", False),
                    is_derived=col_data.get("is_derived", False),
                    expression=col_data.get("expression"),
                )
                node.add_column(column)

        graph.add_node(node)
        self._save()

        logger.info(f"Added node '{name}' to graph '{graph_name}'")
        return node

    def add_edge(
        self,
        graph_name: str,
        source_node: str,  # Node ID or name
        target_node: str,  # Node ID or name
        transformation_type: TransformationType = TransformationType.DIRECT,
        description: Optional[str] = None,
    ) -> LineageEdge:
        """
        Add an edge between nodes.

        Args:
            graph_name: Name of the graph
            source_node: Source node ID or name
            target_node: Target node ID or name
            transformation_type: Type of transformation
            description: Edge description

        Returns:
            Created LineageEdge
        """
        graph = self._graphs.get(graph_name)
        if not graph:
            raise ValueError(f"Graph '{graph_name}' not found")

        # Resolve node IDs
        source_id = self._resolve_node_id(graph, source_node)
        target_id = self._resolve_node_id(graph, target_node)

        if not source_id:
            raise ValueError(f"Source node '{source_node}' not found")
        if not target_id:
            raise ValueError(f"Target node '{target_node}' not found")

        edge = LineageEdge(
            source_node_id=source_id,
            target_node_id=target_id,
            transformation_type=transformation_type,
            description=description,
        )

        graph.add_edge(edge)
        self._save()

        logger.info(f"Added edge from '{source_node}' to '{target_node}'")
        return edge

    def add_column_lineage(
        self,
        graph_name: str,
        source_node: str,
        source_columns: List[str],
        target_node: str,
        target_column: str,
        transformation_type: TransformationType = TransformationType.DIRECT,
        transformation_expression: Optional[str] = None,
        confidence: float = 1.0,
    ) -> ColumnLineage:
        """
        Add column-level lineage.

        Args:
            graph_name: Name of the graph
            source_node: Source node ID or name
            source_columns: Source column names
            target_node: Target node ID or name
            target_column: Target column name
            transformation_type: Type of transformation
            transformation_expression: Expression used
            confidence: Lineage confidence (0-1)

        Returns:
            Created ColumnLineage
        """
        graph = self._graphs.get(graph_name)
        if not graph:
            raise ValueError(f"Graph '{graph_name}' not found")

        # Resolve node IDs
        source_id = self._resolve_node_id(graph, source_node)
        target_id = self._resolve_node_id(graph, target_node)

        if not source_id:
            raise ValueError(f"Source node '{source_node}' not found")
        if not target_id:
            raise ValueError(f"Target node '{target_node}' not found")

        # Find or create edge
        edge = None
        for e in graph.edges:
            if e.source_node_id == source_id and e.target_node_id == target_id:
                edge = e
                break

        if not edge:
            edge = self.add_edge(
                graph_name, source_node, target_node, transformation_type
            )

        # Create column lineage
        col_lineage = ColumnLineage(
            source_node_id=source_id,
            source_columns=source_columns,
            target_node_id=target_id,
            target_column=target_column,
            transformation_type=transformation_type,
            transformation_expression=transformation_expression,
            confidence=confidence,
        )

        edge.add_column_lineage(col_lineage)
        self._save()

        logger.info(f"Added column lineage: {source_columns} -> {target_column}")
        return col_lineage

    def get_column_lineage(
        self,
        graph_name: str,
        node: str,
        column: str,
        direction: str = "upstream",
    ) -> List[Dict[str, Any]]:
        """
        Get lineage for a specific column.

        Args:
            graph_name: Name of the graph
            node: Node ID or name
            column: Column name
            direction: "upstream" or "downstream"

        Returns:
            List of lineage relationships
        """
        graph = self._graphs.get(graph_name)
        if not graph:
            return []

        node_id = self._resolve_node_id(graph, node)
        if not node_id:
            return []

        result = []

        if direction == "upstream":
            # Find what feeds into this column
            for edge in graph.edges:
                for col_lin in edge.column_lineage:
                    if (col_lin.target_node_id == node_id and
                        col_lin.target_column.upper() == column.upper()):
                        source_node = graph.get_node(col_lin.source_node_id)
                        result.append({
                            "source_node": source_node.name if source_node else col_lin.source_node_id,
                            "source_columns": col_lin.source_columns,
                            "target_column": col_lin.target_column,
                            "transformation_type": col_lin.transformation_type.value,
                            "transformation_expression": col_lin.transformation_expression,
                        })
        else:
            # Find what this column feeds into
            for edge in graph.edges:
                for col_lin in edge.column_lineage:
                    if (col_lin.source_node_id == node_id and
                        column.upper() in [c.upper() for c in col_lin.source_columns]):
                        target_node = graph.get_node(col_lin.target_node_id)
                        result.append({
                            "target_node": target_node.name if target_node else col_lin.target_node_id,
                            "target_column": col_lin.target_column,
                            "source_columns": col_lin.source_columns,
                            "transformation_type": col_lin.transformation_type.value,
                        })

        return result

    def get_table_lineage(
        self,
        graph_name: str,
        node: str,
        direction: str = "both",
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Get lineage for a table/object.

        Args:
            graph_name: Name of the graph
            node: Node ID or name
            direction: "upstream", "downstream", or "both"
            max_depth: Maximum depth to traverse

        Returns:
            Lineage information
        """
        graph = self._graphs.get(graph_name)
        if not graph:
            return {"error": f"Graph '{graph_name}' not found"}

        node_id = self._resolve_node_id(graph, node)
        if not node_id:
            return {"error": f"Node '{node}' not found"}

        result = {
            "node_id": node_id,
            "node_name": graph.get_node(node_id).name,
            "upstream": [],
            "downstream": [],
        }

        if direction in ("upstream", "both"):
            upstream_ids = self._traverse_lineage(graph, node_id, "upstream", max_depth)
            for uid in upstream_ids:
                if uid != node_id:
                    unode = graph.get_node(uid)
                    if unode:
                        result["upstream"].append(unode.to_dict())

        if direction in ("downstream", "both"):
            downstream_ids = self._traverse_lineage(graph, node_id, "downstream", max_depth)
            for did in downstream_ids:
                if did != node_id:
                    dnode = graph.get_node(did)
                    if dnode:
                        result["downstream"].append(dnode.to_dict())

        return result

    def from_hierarchy_project(
        self,
        graph_name: str,
        project_id: str,
        hierarchy_service,  # HierarchyKnowledgeBase
    ) -> Dict[str, Any]:
        """
        Build lineage from a hierarchy project.

        Analyzes hierarchy nodes and mappings to create lineage
        from source tables to hierarchy nodes.

        Args:
            graph_name: Name of the graph
            project_id: Hierarchy project ID
            hierarchy_service: HierarchyKnowledgeBase instance

        Returns:
            Created lineage summary
        """
        graph = self.get_or_create_graph(graph_name)

        project = hierarchy_service.get_project(project_id)
        if not project:
            raise ValueError(f"Project '{project_id}' not found")

        # Add hierarchy as a node
        hier_node = self.add_node(
            graph_name=graph_name,
            name=f"{project_id}_HIERARCHY",
            node_type=NodeType.HIERARCHY,
            description=f"Hierarchy from project {project_id}",
            tags=["hierarchy", project_id],
        )

        # Get all mappings
        mappings = hierarchy_service.get_all_mappings(project_id)

        # Track unique source tables
        source_tables: Dict[str, LineageNode] = {}

        for mapping in mappings:
            source_db = mapping.get("source_database", "")
            source_schema = mapping.get("source_schema", "")
            source_table = mapping.get("source_table", "")
            source_column = mapping.get("source_column", "")

            if not source_table:
                continue

            # Create source table node if needed
            table_key = f"{source_db}.{source_schema}.{source_table}"
            if table_key not in source_tables:
                source_node = self.add_node(
                    graph_name=graph_name,
                    name=source_table,
                    node_type=NodeType.TABLE,
                    database=source_db,
                    schema_name=source_schema,
                    tags=["source"],
                )
                source_tables[table_key] = source_node

            # Add column lineage
            if source_column:
                self.add_column_lineage(
                    graph_name=graph_name,
                    source_node=source_tables[table_key].id,
                    source_columns=[source_column],
                    target_node=hier_node.id,
                    target_column=f"MAPPING_{mapping.get('hierarchy_id', '')}",
                    transformation_type=TransformationType.FILTER,
                )

        return {
            "graph_name": graph_name,
            "hierarchy_node_id": hier_node.id,
            "source_table_count": len(source_tables),
            "mapping_count": len(mappings),
        }

    def from_mart_pipeline(
        self,
        graph_name: str,
        config_name: str,
        mart_config_gen,  # MartConfigGenerator
    ) -> Dict[str, Any]:
        """
        Build lineage from a mart pipeline configuration.

        Creates lineage for the 4-object pipeline:
        VW_1 -> DT_2 -> DT_3A -> DT_3

        Args:
            graph_name: Name of the graph
            config_name: Mart configuration name
            mart_config_gen: MartConfigGenerator instance

        Returns:
            Created lineage summary
        """
        graph = self.get_or_create_graph(graph_name)

        config = mart_config_gen.get_config(config_name)
        if not config:
            raise ValueError(f"Mart config '{config_name}' not found")

        # Add source nodes
        hier_node = self.add_node(
            graph_name=graph_name,
            name=config.hierarchy_table.split(".")[-1],
            node_type=NodeType.HIERARCHY,
            database=config.target_database,
            schema_name=config.target_schema,
        )

        map_node = self.add_node(
            graph_name=graph_name,
            name=config.mapping_table.split(".")[-1],
            node_type=NodeType.HIERARCHY_MAPPING,
            database=config.target_database,
            schema_name=config.target_schema,
        )

        # Add pipeline nodes
        vw1_node = self.add_node(
            graph_name=graph_name,
            name=f"VW_1_{config.project_name.upper()}_TRANSLATED",
            node_type=NodeType.VIEW,
            tags=["mart_factory", "VW_1"],
        )

        dt2_node = self.add_node(
            graph_name=graph_name,
            name=f"DT_2_{config.project_name.upper()}_GRANULARITY",
            node_type=NodeType.DYNAMIC_TABLE,
            tags=["mart_factory", "DT_2"],
        )

        dt3a_node = self.add_node(
            graph_name=graph_name,
            name=f"DT_3A_{config.project_name.upper()}",
            node_type=NodeType.DYNAMIC_TABLE,
            tags=["mart_factory", "DT_3A"],
        )

        dt3_node = self.add_node(
            graph_name=graph_name,
            name=f"DT_3_{config.project_name.upper()}",
            node_type=NodeType.DATA_MART,
            tags=["mart_factory", "DT_3"],
        )

        # Add edges
        self.add_edge(graph_name, hier_node.id, vw1_node.id, TransformationType.JOIN)
        self.add_edge(graph_name, map_node.id, vw1_node.id, TransformationType.JOIN)
        self.add_edge(graph_name, vw1_node.id, dt2_node.id, TransformationType.UNPIVOT)
        self.add_edge(graph_name, dt2_node.id, dt3a_node.id, TransformationType.AGGREGATION)
        self.add_edge(graph_name, dt3a_node.id, dt3_node.id, TransformationType.CALCULATION)

        return {
            "graph_name": graph_name,
            "config_name": config_name,
            "node_count": 6,
            "edge_count": 5,
            "pipeline": ["VW_1", "DT_2", "DT_3A", "DT_3"],
        }

    def _resolve_node_id(self, graph: LineageGraph, node: str) -> Optional[str]:
        """Resolve node name or ID to ID."""
        # Try as ID first
        if node in graph.nodes:
            return node

        # Try as name
        for nid, n in graph.nodes.items():
            if n.name.upper() == node.upper():
                return nid
            if n.fully_qualified_name.upper() == node.upper():
                return nid

        return None

    def _traverse_lineage(
        self,
        graph: LineageGraph,
        node_id: str,
        direction: str,
        max_depth: int,
    ) -> Set[str]:
        """Traverse lineage in a direction."""
        if direction == "upstream":
            return graph.get_all_upstream(node_id)
        else:
            return graph.get_all_downstream(node_id)

    def _save(self) -> None:
        """Persist lineage data to disk."""
        data = {}
        for name, graph in self._graphs.items():
            data[name] = {
                "id": graph.id,
                "name": graph.name,
                "description": graph.description,
                "nodes": {
                    nid: {
                        "id": n.id,
                        "name": n.name,
                        "node_type": n.node_type.value,
                        "database": n.database,
                        "schema_name": n.schema_name,
                        "description": n.description,
                        "columns": [c.to_dict() for c in n.columns],
                        "tags": n.tags,
                        "properties": n.properties,
                    }
                    for nid, n in graph.nodes.items()
                },
                "edges": [
                    {
                        "id": e.id,
                        "source_node_id": e.source_node_id,
                        "target_node_id": e.target_node_id,
                        "transformation_type": e.transformation_type.value,
                        "description": e.description,
                        "column_lineage": [cl.to_dict() for cl in e.column_lineage],
                    }
                    for e in graph.edges
                ],
                "created_at": graph.created_at.isoformat(),
                "updated_at": graph.updated_at.isoformat(),
            }

        lineage_file = self.output_dir / "lineage.json"
        lineage_file.write_text(json.dumps(data, indent=2))

    def _load(self) -> None:
        """Load lineage data from disk."""
        lineage_file = self.output_dir / "lineage.json"
        if not lineage_file.exists():
            return

        try:
            data = json.loads(lineage_file.read_text())
            for name, graph_data in data.items():
                graph = LineageGraph(
                    id=graph_data.get("id"),
                    name=graph_data["name"],
                    description=graph_data.get("description"),
                    created_at=datetime.fromisoformat(graph_data.get("created_at", datetime.now().isoformat())),
                    updated_at=datetime.fromisoformat(graph_data.get("updated_at", datetime.now().isoformat())),
                )

                # Load nodes
                for nid, node_data in graph_data.get("nodes", {}).items():
                    node = LineageNode(
                        id=node_data["id"],
                        name=node_data["name"],
                        node_type=NodeType(node_data["node_type"]),
                        database=node_data.get("database"),
                        schema_name=node_data.get("schema_name"),
                        description=node_data.get("description"),
                        tags=node_data.get("tags", []),
                        properties=node_data.get("properties", {}),
                    )
                    for col_data in node_data.get("columns", []):
                        node.columns.append(LineageColumn(**col_data))
                    graph.nodes[nid] = node

                # Load edges
                for edge_data in graph_data.get("edges", []):
                    edge = LineageEdge(
                        id=edge_data["id"],
                        source_node_id=edge_data["source_node_id"],
                        target_node_id=edge_data["target_node_id"],
                        transformation_type=TransformationType(edge_data["transformation_type"]),
                        description=edge_data.get("description"),
                    )
                    for cl_data in edge_data.get("column_lineage", []):
                        edge.column_lineage.append(ColumnLineage(
                            id=cl_data.get("id"),
                            source_node_id=cl_data["source_node_id"],
                            source_columns=cl_data["source_columns"],
                            target_node_id=cl_data["target_node_id"],
                            target_column=cl_data["target_column"],
                            transformation_type=TransformationType(cl_data["transformation_type"]),
                            transformation_expression=cl_data.get("transformation_expression"),
                            confidence=cl_data.get("confidence", 1.0),
                        ))
                    graph.edges.append(edge)

                self._graphs[name] = graph

        except Exception as e:
            logger.error(f"Failed to load lineage data: {e}")
