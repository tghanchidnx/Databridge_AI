"""
Lineage & Impact Analysis Types.

Pydantic models for data lineage tracking and impact analysis:
- LineageNode: Source/target objects with column information
- LineageEdge: Transformations between nodes
- LineageGraph: Complete lineage graph
- ImpactResult: Impact analysis results
- DependencyNode/Edge: Dependency graph structures
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field
import uuid


class NodeType(str, Enum):
    """Type of lineage node."""
    TABLE = "TABLE"
    VIEW = "VIEW"
    DYNAMIC_TABLE = "DYNAMIC_TABLE"
    HIERARCHY = "HIERARCHY"
    HIERARCHY_MAPPING = "HIERARCHY_MAPPING"
    FORMULA_GROUP = "FORMULA_GROUP"
    DATA_MART = "DATA_MART"
    DBT_MODEL = "DBT_MODEL"
    COLUMN = "COLUMN"
    EXTERNAL = "EXTERNAL"


class TransformationType(str, Enum):
    """Type of transformation between nodes."""
    DIRECT = "DIRECT"  # Direct column reference
    AGGREGATION = "AGGREGATION"  # SUM, COUNT, AVG, etc.
    CALCULATION = "CALCULATION"  # Formula/expression
    FILTER = "FILTER"  # WHERE clause
    JOIN = "JOIN"  # JOIN operation
    UNION = "UNION"  # UNION operation
    CASE = "CASE"  # CASE statement
    UNPIVOT = "UNPIVOT"  # UNPIVOT operation
    PIVOT = "PIVOT"  # PIVOT operation
    DERIVED = "DERIVED"  # Derived column


class ImpactSeverity(str, Enum):
    """Severity of impact from a change."""
    CRITICAL = "CRITICAL"  # Breaking change
    HIGH = "HIGH"  # Significant impact
    MEDIUM = "MEDIUM"  # Moderate impact
    LOW = "LOW"  # Minor impact
    INFO = "INFO"  # Informational only


class ChangeType(str, Enum):
    """Type of change being analyzed."""
    ADD_COLUMN = "ADD_COLUMN"
    REMOVE_COLUMN = "REMOVE_COLUMN"
    RENAME_COLUMN = "RENAME_COLUMN"
    MODIFY_TYPE = "MODIFY_TYPE"
    ADD_NODE = "ADD_NODE"
    REMOVE_NODE = "REMOVE_NODE"
    MODIFY_MAPPING = "MODIFY_MAPPING"
    MODIFY_FORMULA = "MODIFY_FORMULA"


class LineageColumn(BaseModel):
    """A column within a lineage node."""
    name: str
    data_type: Optional[str] = None
    description: Optional[str] = None
    is_primary_key: bool = False
    is_derived: bool = False
    expression: Optional[str] = None  # For derived columns

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "description": self.description,
            "is_primary_key": self.is_primary_key,
            "is_derived": self.is_derived,
            "expression": self.expression,
        }


class LineageNode(BaseModel):
    """
    A node in the lineage graph.

    Represents a data object (table, view, hierarchy, etc.)
    that participates in data lineage.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str  # Object name
    node_type: NodeType
    database: Optional[str] = None
    schema_name: Optional[str] = None
    description: Optional[str] = None

    # Column information
    columns: List[LineageColumn] = Field(default_factory=list)

    # Metadata
    owner: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    properties: Dict[str, Any] = Field(default_factory=dict)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    @property
    def fully_qualified_name(self) -> str:
        """Get fully qualified name."""
        parts = [p for p in [self.database, self.schema_name, self.name] if p]
        return ".".join(parts)

    def get_column(self, name: str) -> Optional[LineageColumn]:
        """Get column by name."""
        for col in self.columns:
            if col.name.upper() == name.upper():
                return col
        return None

    def add_column(self, column: LineageColumn) -> None:
        """Add a column."""
        self.columns.append(column)
        self.updated_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "node_type": self.node_type.value,
            "fully_qualified_name": self.fully_qualified_name,
            "database": self.database,
            "schema_name": self.schema_name,
            "column_count": len(self.columns),
            "tags": self.tags,
        }


class ColumnLineage(BaseModel):
    """
    Column-level lineage relationship.

    Tracks the transformation from source column(s) to target column.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])

    # Source columns (can be multiple for derived columns)
    source_node_id: str
    source_columns: List[str]  # Column names

    # Target column
    target_node_id: str
    target_column: str

    # Transformation info
    transformation_type: TransformationType = TransformationType.DIRECT
    transformation_expression: Optional[str] = None
    transformation_description: Optional[str] = None

    # Confidence
    confidence: float = 1.0  # 0-1, auto-detected lineage may have lower confidence

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "source_node_id": self.source_node_id,
            "source_columns": self.source_columns,
            "target_node_id": self.target_node_id,
            "target_column": self.target_column,
            "transformation_type": self.transformation_type.value,
            "transformation_expression": self.transformation_expression,
            "confidence": self.confidence,
        }


class LineageEdge(BaseModel):
    """
    An edge in the lineage graph.

    Represents a transformation relationship between two nodes.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    source_node_id: str
    target_node_id: str

    # Edge type
    transformation_type: TransformationType = TransformationType.DIRECT
    description: Optional[str] = None

    # Column-level lineage within this edge
    column_lineage: List[ColumnLineage] = Field(default_factory=list)

    # Metadata
    properties: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)

    def add_column_lineage(self, lineage: ColumnLineage) -> None:
        """Add column lineage."""
        self.column_lineage.append(lineage)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "transformation_type": self.transformation_type.value,
            "description": self.description,
            "column_lineage_count": len(self.column_lineage),
        }


class LineageGraph(BaseModel):
    """
    Complete lineage graph.

    Contains all nodes and edges representing data lineage.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = "default"
    description: Optional[str] = None

    # Graph structure
    nodes: Dict[str, LineageNode] = Field(default_factory=dict)
    edges: List[LineageEdge] = Field(default_factory=list)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    def add_node(self, node: LineageNode) -> None:
        """Add a node to the graph."""
        self.nodes[node.id] = node
        self.updated_at = datetime.now()

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        """Get a node by ID."""
        return self.nodes.get(node_id)

    def get_node_by_name(self, name: str) -> Optional[LineageNode]:
        """Get a node by name."""
        for node in self.nodes.values():
            if node.name.upper() == name.upper():
                return node
            if node.fully_qualified_name.upper() == name.upper():
                return node
        return None

    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge to the graph."""
        self.edges.append(edge)
        self.updated_at = datetime.now()

    def get_upstream_nodes(self, node_id: str) -> List[LineageNode]:
        """Get all nodes that feed into the given node."""
        upstream_ids = set()
        for edge in self.edges:
            if edge.target_node_id == node_id:
                upstream_ids.add(edge.source_node_id)
        return [self.nodes[nid] for nid in upstream_ids if nid in self.nodes]

    def get_downstream_nodes(self, node_id: str) -> List[LineageNode]:
        """Get all nodes that are fed by the given node."""
        downstream_ids = set()
        for edge in self.edges:
            if edge.source_node_id == node_id:
                downstream_ids.add(edge.target_node_id)
        return [self.nodes[nid] for nid in downstream_ids if nid in self.nodes]

    def get_all_upstream(self, node_id: str, visited: Optional[Set[str]] = None) -> Set[str]:
        """Recursively get all upstream node IDs."""
        if visited is None:
            visited = set()

        if node_id in visited:
            return visited

        visited.add(node_id)

        for edge in self.edges:
            if edge.target_node_id == node_id:
                self.get_all_upstream(edge.source_node_id, visited)

        return visited

    def get_all_downstream(self, node_id: str, visited: Optional[Set[str]] = None) -> Set[str]:
        """Recursively get all downstream node IDs."""
        if visited is None:
            visited = set()

        if node_id in visited:
            return visited

        visited.add(node_id)

        for edge in self.edges:
            if edge.source_node_id == node_id:
                self.get_all_downstream(edge.target_node_id, visited)

        return visited

    def get_column_lineage(self, node_id: str, column_name: str) -> List[ColumnLineage]:
        """Get all column lineage for a specific column."""
        result = []
        for edge in self.edges:
            for col_lineage in edge.column_lineage:
                if (col_lineage.target_node_id == node_id and
                    col_lineage.target_column.upper() == column_name.upper()):
                    result.append(col_lineage)
        return result

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class ImpactedObject(BaseModel):
    """An object impacted by a change."""
    node_id: str
    node_name: str
    node_type: NodeType
    impact_severity: ImpactSeverity
    impact_description: str
    affected_columns: List[str] = Field(default_factory=list)
    distance_from_source: int = 0  # Hops from the changed object

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "node_id": self.node_id,
            "node_name": self.node_name,
            "node_type": self.node_type.value,
            "impact_severity": self.impact_severity.value,
            "impact_description": self.impact_description,
            "affected_columns": self.affected_columns,
            "distance_from_source": self.distance_from_source,
        }


class ImpactResult(BaseModel):
    """
    Result of impact analysis.

    Contains all objects affected by a proposed change.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])

    # Change being analyzed
    change_type: ChangeType
    source_node_id: str
    source_node_name: str
    change_description: str

    # Impacted objects
    impacted_objects: List[ImpactedObject] = Field(default_factory=list)

    # Summary statistics
    total_impacted: int = 0
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0

    # Analysis metadata
    analyzed_at: datetime = Field(default_factory=datetime.now)

    def add_impact(self, impact: ImpactedObject) -> None:
        """Add an impacted object."""
        self.impacted_objects.append(impact)
        self.total_impacted = len(self.impacted_objects)

        # Update counts
        if impact.impact_severity == ImpactSeverity.CRITICAL:
            self.critical_count += 1
        elif impact.impact_severity == ImpactSeverity.HIGH:
            self.high_count += 1
        elif impact.impact_severity == ImpactSeverity.MEDIUM:
            self.medium_count += 1
        elif impact.impact_severity == ImpactSeverity.LOW:
            self.low_count += 1

    @property
    def max_severity(self) -> ImpactSeverity:
        """Get the maximum severity across all impacts."""
        if self.critical_count > 0:
            return ImpactSeverity.CRITICAL
        if self.high_count > 0:
            return ImpactSeverity.HIGH
        if self.medium_count > 0:
            return ImpactSeverity.MEDIUM
        if self.low_count > 0:
            return ImpactSeverity.LOW
        return ImpactSeverity.INFO

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "change_type": self.change_type.value,
            "source_node_id": self.source_node_id,
            "source_node_name": self.source_node_name,
            "change_description": self.change_description,
            "total_impacted": self.total_impacted,
            "max_severity": self.max_severity.value,
            "critical_count": self.critical_count,
            "high_count": self.high_count,
            "medium_count": self.medium_count,
            "low_count": self.low_count,
            "analyzed_at": self.analyzed_at.isoformat(),
        }


class DependencyNode(BaseModel):
    """A node in the dependency graph (simplified view)."""
    id: str
    name: str
    node_type: NodeType
    level: int = 0  # Distance from root

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "node_type": self.node_type.value,
            "level": self.level,
        }


class DependencyGraph(BaseModel):
    """
    Simplified dependency graph for visualization.

    Provides a hierarchical view of object dependencies.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    root_node_id: Optional[str] = None
    direction: str = "downstream"  # "upstream" or "downstream"

    # Graph structure
    nodes: List[DependencyNode] = Field(default_factory=list)
    edges: List[Dict[str, str]] = Field(default_factory=list)  # [{source, target}]

    # Statistics
    max_depth: int = 0
    total_nodes: int = 0

    def to_mermaid(self) -> str:
        """Export as Mermaid diagram."""
        lines = ["graph TD"]

        # Add nodes with styling based on type
        for node in self.nodes:
            style = self._get_node_style(node.node_type)
            lines.append(f"    {node.id}[{node.name}]{style}")

        # Add edges
        for edge in self.edges:
            lines.append(f"    {edge['source']} --> {edge['target']}")

        return "\n".join(lines)

    def to_dot(self) -> str:
        """Export as DOT (Graphviz) diagram."""
        lines = ["digraph G {"]
        lines.append("    rankdir=TB;")
        lines.append("    node [shape=box];")

        # Add nodes
        for node in self.nodes:
            color = self._get_node_color(node.node_type)
            lines.append(f'    "{node.id}" [label="{node.name}" fillcolor="{color}" style="filled"];')

        # Add edges
        for edge in self.edges:
            lines.append(f'    "{edge["source"]}" -> "{edge["target"]}";')

        lines.append("}")
        return "\n".join(lines)

    def _get_node_style(self, node_type: NodeType) -> str:
        """Get Mermaid node style based on type."""
        styles = {
            NodeType.TABLE: ":::table",
            NodeType.VIEW: ":::view",
            NodeType.DYNAMIC_TABLE: ":::dynamic",
            NodeType.HIERARCHY: ":::hierarchy",
            NodeType.DATA_MART: ":::mart",
            NodeType.DBT_MODEL: ":::dbt",
        }
        return styles.get(node_type, "")

    def _get_node_color(self, node_type: NodeType) -> str:
        """Get DOT node color based on type."""
        colors = {
            NodeType.TABLE: "#E3F2FD",
            NodeType.VIEW: "#E8F5E9",
            NodeType.DYNAMIC_TABLE: "#FFF3E0",
            NodeType.HIERARCHY: "#FCE4EC",
            NodeType.DATA_MART: "#F3E5F5",
            NodeType.DBT_MODEL: "#E0F7FA",
        }
        return colors.get(node_type, "#FFFFFF")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "root_node_id": self.root_node_id,
            "direction": self.direction,
            "total_nodes": self.total_nodes,
            "max_depth": self.max_depth,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": self.edges,
        }


class LineageValidationResult(BaseModel):
    """Result of lineage validation."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])

    # Validation status
    is_valid: bool = True
    completeness_score: float = 0.0  # 0-1

    # Issues found
    orphan_nodes: List[str] = Field(default_factory=list)  # Nodes with no edges
    missing_sources: List[str] = Field(default_factory=list)  # Columns with no lineage
    circular_dependencies: List[List[str]] = Field(default_factory=list)  # Cycles

    # Warnings
    warnings: List[str] = Field(default_factory=list)

    # Statistics
    node_count: int = 0
    edge_count: int = 0
    column_lineage_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "is_valid": self.is_valid,
            "completeness_score": self.completeness_score,
            "orphan_node_count": len(self.orphan_nodes),
            "missing_source_count": len(self.missing_sources),
            "circular_dependency_count": len(self.circular_dependencies),
            "warning_count": len(self.warnings),
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "column_lineage_count": self.column_lineage_count,
        }
