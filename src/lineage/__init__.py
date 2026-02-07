"""
Lineage & Impact Analysis Module.

Provides column-level lineage tracking, impact analysis for hierarchy changes,
and dependency graph visualization.

Components:
- LineageTracker: Tracks data lineage across DataBridge objects
- ImpactAnalyzer: Analyzes impact of changes on data objects
- Types: Pydantic models for lineage graphs and impact results

MCP Tools (11):
- Lineage Tracking: add_lineage_node, track_column_lineage, get_column_lineage, get_table_lineage
- Impact Analysis: analyze_change_impact, get_downstream_impact, get_upstream_dependencies
- Visualization: build_dependency_graph, export_lineage_diagram, validate_lineage
- Utilities: list_lineage_graphs
"""

from .types import (
    # Enums
    NodeType,
    TransformationType,
    ImpactSeverity,
    ChangeType,
    # Lineage models
    LineageColumn,
    LineageNode,
    ColumnLineage,
    LineageEdge,
    LineageGraph,
    # Impact models
    ImpactedObject,
    ImpactResult,
    # Dependency models
    DependencyNode,
    DependencyGraph,
    # Validation
    LineageValidationResult,
)

from .lineage_tracker import LineageTracker
from .impact_analyzer import ImpactAnalyzer
from .mcp_tools import register_lineage_tools

__all__ = [
    # Enums
    "NodeType",
    "TransformationType",
    "ImpactSeverity",
    "ChangeType",
    # Lineage models
    "LineageColumn",
    "LineageNode",
    "ColumnLineage",
    "LineageEdge",
    "LineageGraph",
    # Impact models
    "ImpactedObject",
    "ImpactResult",
    # Dependency models
    "DependencyNode",
    "DependencyGraph",
    # Validation
    "LineageValidationResult",
    # Classes
    "LineageTracker",
    "ImpactAnalyzer",
    # Registration
    "register_lineage_tools",
]
