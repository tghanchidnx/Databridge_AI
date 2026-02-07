"""
MCP Tools for Lineage & Impact Analysis.

Provides 10 tools for data lineage tracking and impact analysis:

Lineage Tracking (4):
- track_column_lineage
- get_column_lineage
- get_table_lineage
- add_lineage_node

Impact Analysis (3):
- analyze_change_impact
- get_downstream_impact
- get_upstream_dependencies

Visualization & Validation (3):
- build_dependency_graph
- export_lineage_diagram
- validate_lineage
"""

import json
import logging
from typing import Any, Dict, List, Optional

from .types import (
    NodeType,
    TransformationType,
    ChangeType,
)
from .lineage_tracker import LineageTracker
from .impact_analyzer import ImpactAnalyzer

logger = logging.getLogger(__name__)


def register_lineage_tools(mcp, settings=None) -> Dict[str, Any]:
    """
    Register Lineage & Impact Analysis MCP tools.

    Args:
        mcp: The FastMCP instance
        settings: Optional settings

    Returns:
        Dict with registration info
    """

    # Initialize components
    tracker = LineageTracker()
    analyzer = ImpactAnalyzer(tracker)

    # ========================================
    # Lineage Tracking (4 tools)
    # ========================================

    @mcp.tool()
    def add_lineage_node(
        graph_name: str,
        node_name: str,
        node_type: str,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        columns: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add a node to the lineage graph.

        Nodes represent data objects like tables, views, hierarchies,
        and data marts that participate in data lineage.

        Args:
            graph_name: Name of the lineage graph (creates if not exists)
            node_name: Node name (object name)
            node_type: Type of node (TABLE, VIEW, DYNAMIC_TABLE, HIERARCHY, DATA_MART, DBT_MODEL)
            database: Database name
            schema_name: Schema name
            columns: JSON array of column definitions [{"name": "col1", "data_type": "VARCHAR"}]
            description: Node description
            tags: Comma-separated tags

        Returns:
            Created node details

        Example:
            add_lineage_node(
                graph_name="finance_lineage",
                node_name="DIM_ACCOUNT",
                node_type="TABLE",
                database="ANALYTICS",
                schema_name="PUBLIC",
                columns='[{"name": "ACCOUNT_ID", "data_type": "NUMBER", "is_primary_key": true}]'
            )
        """
        try:
            # Parse node type
            try:
                nt = NodeType(node_type.upper())
            except ValueError:
                return {"success": False, "error": f"Invalid node type: {node_type}"}

            # Parse columns
            col_list = None
            if columns:
                col_list = json.loads(columns)

            # Parse tags
            tag_list = None
            if tags:
                tag_list = [t.strip() for t in tags.split(",")]

            node = tracker.add_node(
                graph_name=graph_name,
                name=node_name,
                node_type=nt,
                database=database,
                schema_name=schema_name,
                columns=col_list,
                description=description,
                tags=tag_list,
            )

            return {
                "success": True,
                "node_id": node.id,
                "node_name": node.name,
                "node_type": node.node_type.value,
                "fully_qualified_name": node.fully_qualified_name,
                "column_count": len(node.columns),
                "message": f"Added node '{node_name}' to graph '{graph_name}'",
            }

        except Exception as e:
            logger.error(f"Failed to add lineage node: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def track_column_lineage(
        graph_name: str,
        source_node: str,
        source_columns: str,
        target_node: str,
        target_column: str,
        transformation_type: str = "DIRECT",
        transformation_expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add column-level lineage relationship.

        Tracks how source column(s) transform into a target column.

        Args:
            graph_name: Name of the lineage graph
            source_node: Source node name or ID
            source_columns: Comma-separated source column names
            target_node: Target node name or ID
            target_column: Target column name
            transformation_type: Type of transformation (DIRECT, AGGREGATION, CALCULATION, FILTER, JOIN, CASE)
            transformation_expression: Optional expression used

        Returns:
            Created lineage details

        Example:
            track_column_lineage(
                graph_name="finance_lineage",
                source_node="DIM_ACCOUNT",
                source_columns="ACCOUNT_CODE,ACCOUNT_NAME",
                target_node="VW_1_GROSS_TRANSLATED",
                target_column="RESOLVED_VALUE",
                transformation_type="CASE",
                transformation_expression="CASE WHEN ID_SOURCE = 'ACCOUNT_CODE' THEN ..."
            )
        """
        try:
            # Parse transformation type
            try:
                tt = TransformationType(transformation_type.upper())
            except ValueError:
                tt = TransformationType.DIRECT

            # Parse source columns
            source_col_list = [c.strip() for c in source_columns.split(",")]

            lineage = tracker.add_column_lineage(
                graph_name=graph_name,
                source_node=source_node,
                source_columns=source_col_list,
                target_node=target_node,
                target_column=target_column,
                transformation_type=tt,
                transformation_expression=transformation_expression,
            )

            return {
                "success": True,
                "lineage_id": lineage.id,
                "source_node_id": lineage.source_node_id,
                "source_columns": lineage.source_columns,
                "target_node_id": lineage.target_node_id,
                "target_column": lineage.target_column,
                "transformation_type": lineage.transformation_type.value,
                "message": f"Added column lineage: {source_columns} -> {target_column}",
            }

        except Exception as e:
            logger.error(f"Failed to track column lineage: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_column_lineage(
        graph_name: str,
        node: str,
        column: str,
        direction: str = "upstream",
    ) -> Dict[str, Any]:
        """
        Get lineage for a specific column.

        Shows what feeds into a column (upstream) or what a column feeds (downstream).

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            column: Column name
            direction: "upstream" (what feeds this column) or "downstream" (what this column feeds)

        Returns:
            Column lineage relationships

        Example:
            get_column_lineage(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                column="GROSS_AMOUNT",
                direction="upstream"
            )
        """
        try:
            lineage = tracker.get_column_lineage(
                graph_name=graph_name,
                node=node,
                column=column,
                direction=direction,
            )

            return {
                "success": True,
                "node": node,
                "column": column,
                "direction": direction,
                "lineage_count": len(lineage),
                "lineage": lineage,
            }

        except Exception as e:
            logger.error(f"Failed to get column lineage: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_table_lineage(
        graph_name: str,
        node: str,
        direction: str = "both",
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Get lineage for a table/object.

        Shows all upstream sources and downstream consumers of a data object.

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            direction: "upstream", "downstream", or "both"
            max_depth: Maximum depth to traverse

        Returns:
            Table lineage with upstream and downstream objects

        Example:
            get_table_lineage(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                direction="both"
            )
        """
        try:
            result = tracker.get_table_lineage(
                graph_name=graph_name,
                node=node,
                direction=direction,
                max_depth=max_depth,
            )

            if "error" in result:
                return {"success": False, "error": result["error"]}

            return {
                "success": True,
                "node": result.get("node_name"),
                "node_id": result.get("node_id"),
                "upstream_count": len(result.get("upstream", [])),
                "downstream_count": len(result.get("downstream", [])),
                "upstream": result.get("upstream", []),
                "downstream": result.get("downstream", []),
            }

        except Exception as e:
            logger.error(f"Failed to get table lineage: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Impact Analysis (3 tools)
    # ========================================

    @mcp.tool()
    def analyze_change_impact(
        graph_name: str,
        node: str,
        change_type: str,
        column: Optional[str] = None,
        new_column_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyze impact of a proposed change.

        Identifies all objects affected by changes like column removal,
        column rename, hierarchy modifications, etc.

        Args:
            graph_name: Name of the lineage graph
            node: Node to change
            change_type: Type of change (REMOVE_COLUMN, RENAME_COLUMN, REMOVE_NODE, MODIFY_MAPPING, MODIFY_FORMULA)
            column: Column name (for column changes)
            new_column_name: New column name (for renames)

        Returns:
            Impact analysis with affected objects and severity

        Example:
            analyze_change_impact(
                graph_name="finance_lineage",
                node="DIM_ACCOUNT",
                change_type="REMOVE_COLUMN",
                column="ACCOUNT_CODE"
            )
        """
        try:
            # Parse change type
            try:
                ct = ChangeType(change_type.upper())
            except ValueError:
                return {"success": False, "error": f"Invalid change type: {change_type}"}

            if ct == ChangeType.REMOVE_COLUMN and column:
                result = analyzer.analyze_column_removal(graph_name, node, column)
            elif ct == ChangeType.RENAME_COLUMN and column and new_column_name:
                result = analyzer.analyze_column_rename(
                    graph_name, node, column, new_column_name
                )
            else:
                result = analyzer.analyze_hierarchy_change(graph_name, node, ct)

            return {
                "success": True,
                "change_type": result.change_type.value,
                "source_node": result.source_node_name,
                "change_description": result.change_description,
                "total_impacted": result.total_impacted,
                "max_severity": result.max_severity.value,
                "critical_count": result.critical_count,
                "high_count": result.high_count,
                "medium_count": result.medium_count,
                "low_count": result.low_count,
                "impacted_objects": [i.to_dict() for i in result.impacted_objects],
            }

        except Exception as e:
            logger.error(f"Failed to analyze impact: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_downstream_impact(
        graph_name: str,
        node: str,
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Get all objects that would be impacted by changes to a node.

        Shows the "blast radius" of potential changes to a data object.

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            max_depth: Maximum depth to traverse

        Returns:
            All downstream objects that depend on this node

        Example:
            get_downstream_impact(
                graph_name="finance_lineage",
                node="TBL_0_GROSS_LOS_REPORT_HIERARCHY_"
            )
        """
        try:
            result = analyzer.get_downstream_impact(graph_name, node, max_depth)

            if "error" in result:
                return {"success": False, "error": result["error"]}

            return {
                "success": True,
                **result,
            }

        except Exception as e:
            logger.error(f"Failed to get downstream impact: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_upstream_dependencies(
        graph_name: str,
        node: str,
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Get all upstream dependencies of a node.

        Shows all source tables and objects that feed into a data object.

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            max_depth: Maximum depth to traverse

        Returns:
            All upstream objects that this node depends on

        Example:
            get_upstream_dependencies(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS"
            )
        """
        try:
            result = analyzer.get_upstream_dependencies(graph_name, node, max_depth)

            if "error" in result:
                return {"success": False, "error": result["error"]}

            return {
                "success": True,
                **result,
            }

        except Exception as e:
            logger.error(f"Failed to get upstream dependencies: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Visualization & Validation (3 tools)
    # ========================================

    @mcp.tool()
    def build_dependency_graph(
        graph_name: str,
        node: str,
        direction: str = "downstream",
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Build a dependency graph for visualization.

        Creates a hierarchical view of object dependencies that can be
        exported as Mermaid or DOT diagrams.

        Args:
            graph_name: Name of the lineage graph
            node: Root node name or ID
            direction: "upstream" or "downstream"
            max_depth: Maximum depth to traverse

        Returns:
            Dependency graph structure

        Example:
            build_dependency_graph(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                direction="upstream"
            )
        """
        try:
            dep_graph = analyzer.build_dependency_graph(
                graph_name, node, direction, max_depth
            )

            return {
                "success": True,
                "root_node_id": dep_graph.root_node_id,
                "direction": dep_graph.direction,
                "total_nodes": dep_graph.total_nodes,
                "max_depth": dep_graph.max_depth,
                "nodes": [n.to_dict() for n in dep_graph.nodes],
                "edges": dep_graph.edges,
            }

        except Exception as e:
            logger.error(f"Failed to build dependency graph: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def export_lineage_diagram(
        graph_name: str,
        node: str,
        direction: str = "downstream",
        format: str = "mermaid",
        max_depth: int = 10,
    ) -> Dict[str, Any]:
        """
        Export lineage as a diagram.

        Generates Mermaid or DOT (Graphviz) diagram code for visualization.

        Args:
            graph_name: Name of the lineage graph
            node: Root node name or ID
            direction: "upstream" or "downstream"
            format: Output format - "mermaid" or "dot"
            max_depth: Maximum depth to traverse

        Returns:
            Diagram code string

        Example:
            export_lineage_diagram(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                direction="upstream",
                format="mermaid"
            )
        """
        try:
            dep_graph = analyzer.build_dependency_graph(
                graph_name, node, direction, max_depth
            )

            if format.lower() == "mermaid":
                diagram = dep_graph.to_mermaid()
            elif format.lower() == "dot":
                diagram = dep_graph.to_dot()
            else:
                return {"success": False, "error": f"Unknown format: {format}"}

            return {
                "success": True,
                "format": format,
                "node_count": dep_graph.total_nodes,
                "edge_count": len(dep_graph.edges),
                "diagram": diagram,
            }

        except Exception as e:
            logger.error(f"Failed to export diagram: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def validate_lineage(
        graph_name: str,
    ) -> Dict[str, Any]:
        """
        Validate lineage graph completeness.

        Checks for:
        - Orphan nodes (nodes with no connections)
        - Missing source lineage
        - Circular dependencies
        - Overall completeness score

        Args:
            graph_name: Name of the lineage graph

        Returns:
            Validation result with issues and completeness score

        Example:
            validate_lineage(graph_name="finance_lineage")
        """
        try:
            result = analyzer.validate_lineage(graph_name)

            return {
                "success": True,
                "is_valid": result.is_valid,
                "completeness_score": round(result.completeness_score * 100, 1),
                "node_count": result.node_count,
                "edge_count": result.edge_count,
                "column_lineage_count": result.column_lineage_count,
                "orphan_nodes": result.orphan_nodes,
                "missing_sources": result.missing_sources,
                "circular_dependencies": result.circular_dependencies,
                "warnings": result.warnings,
            }

        except Exception as e:
            logger.error(f"Failed to validate lineage: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Additional utility tools
    # ========================================

    @mcp.tool()
    def list_lineage_graphs() -> Dict[str, Any]:
        """
        List all lineage graphs.

        Returns:
            List of graph summaries

        Example:
            list_lineage_graphs()
        """
        try:
            graphs = tracker.list_graphs()

            return {
                "success": True,
                "graph_count": len(graphs),
                "graphs": graphs,
            }

        except Exception as e:
            logger.error(f"Failed to list graphs: {e}")
            return {"success": False, "error": str(e)}

    # Return registration info
    return {
        "tools_registered": 11,
        "tools": [
            # Lineage Tracking
            "add_lineage_node",
            "track_column_lineage",
            "get_column_lineage",
            "get_table_lineage",
            # Impact Analysis
            "analyze_change_impact",
            "get_downstream_impact",
            "get_upstream_dependencies",
            # Visualization & Validation
            "build_dependency_graph",
            "export_lineage_diagram",
            "validate_lineage",
            # Utilities
            "list_lineage_graphs",
        ],
    }
