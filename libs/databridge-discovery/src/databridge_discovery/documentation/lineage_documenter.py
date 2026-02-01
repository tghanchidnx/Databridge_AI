"""
Lineage Documenter for generating lineage documentation and diagrams.

This module provides:
- Data lineage documentation
- Mermaid diagram generation
- D2 diagram generation
- Interactive HTML lineage views
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


class NodeType(str, Enum):
    """Types of lineage nodes."""

    SOURCE_TABLE = "source_table"
    SOURCE_COLUMN = "source_column"
    TRANSFORMATION = "transformation"
    HIERARCHY = "hierarchy"
    OUTPUT_TABLE = "output_table"
    OUTPUT_COLUMN = "output_column"


class EdgeType(str, Enum):
    """Types of lineage edges."""

    DERIVES_FROM = "derives_from"
    TRANSFORMS_TO = "transforms_to"
    MAPS_TO = "maps_to"
    AGGREGATES = "aggregates"
    JOINS = "joins"


class DiagramFormat(str, Enum):
    """Output diagram formats."""

    MERMAID = "mermaid"
    D2 = "d2"
    GRAPHVIZ = "graphviz"
    HTML = "html"


@dataclass
class LineageNode:
    """A node in the lineage graph."""

    id: str
    name: str
    node_type: NodeType
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    properties: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "node_type": self.node_type.value,
            "description": self.description,
            "metadata": self.metadata,
            "properties": self.properties,
        }


@dataclass
class LineageEdge:
    """An edge in the lineage graph."""

    source_id: str
    target_id: str
    edge_type: EdgeType
    label: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type.value,
            "label": self.label,
            "metadata": self.metadata,
        }


@dataclass
class LineageDiagram:
    """A complete lineage diagram."""

    name: str
    nodes: list[LineageNode]
    edges: list[LineageEdge]
    description: str = ""
    format: DiagramFormat = DiagramFormat.MERMAID
    generated_at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    def get_node(self, node_id: str) -> LineageNode | None:
        """Get node by ID."""
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def get_edges_from(self, node_id: str) -> list[LineageEdge]:
        """Get all edges from a node."""
        return [e for e in self.edges if e.source_id == node_id]

    def get_edges_to(self, node_id: str) -> list[LineageEdge]:
        """Get all edges to a node."""
        return [e for e in self.edges if e.target_id == node_id]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "format": self.format.value,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "generated_at": self.generated_at.isoformat(),
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "metadata": self.metadata,
        }


class LineageDocumenter:
    """
    Generates lineage documentation and diagrams.

    Supports multiple output formats:
    - Mermaid (for GitHub/GitLab markdown)
    - D2 (for higher-quality diagrams)
    - GraphViz DOT format
    - Interactive HTML

    Example:
        documenter = LineageDocumenter()

        # Build lineage from hierarchies
        diagram = documenter.build_lineage(
            hierarchies=[hierarchy1],
            name="GL Lineage"
        )

        # Export to Mermaid
        mermaid = documenter.to_mermaid(diagram)

        # Export to D2
        d2 = documenter.to_d2(diagram)
    """

    def __init__(self):
        """Initialize the documenter."""
        pass

    def build_lineage(
        self,
        hierarchies: list[ConvertedHierarchy],
        name: str,
        include_mappings: bool = True,
    ) -> LineageDiagram:
        """
        Build lineage diagram from hierarchies.

        Args:
            hierarchies: List of hierarchies
            name: Diagram name
            include_mappings: Include source mappings

        Returns:
            LineageDiagram
        """
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        for hier in hierarchies:
            # Add hierarchy root node
            hier_node = LineageNode(
                id=f"hier_{hier.id}",
                name=hier.name,
                node_type=NodeType.HIERARCHY,
                description=f"Hierarchy: {hier.name}",
                metadata={
                    "entity_type": hier.entity_type,
                    "level_count": hier.level_count,
                },
            )
            nodes.append(hier_node)

            # Add output table node
            output_node = LineageNode(
                id=f"tbl_{hier.id}",
                name=f"TBL_0_{self._sanitize_name(hier.name)}",
                node_type=NodeType.OUTPUT_TABLE,
                description=f"Output table for {hier.name}",
            )
            nodes.append(output_node)

            # Edge from hierarchy to output
            edges.append(LineageEdge(
                source_id=hier_node.id,
                target_id=output_node.id,
                edge_type=EdgeType.TRANSFORMS_TO,
                label="generates",
            ))

            # Add source column if exists
            if hier.source_column:
                source_col_node = LineageNode(
                    id=f"src_{hier.id}",
                    name=hier.source_column,
                    node_type=NodeType.SOURCE_COLUMN,
                    description=f"Source column: {hier.source_column}",
                )
                nodes.append(source_col_node)

                edges.append(LineageEdge(
                    source_id=source_col_node.id,
                    target_id=hier_node.id,
                    edge_type=EdgeType.DERIVES_FROM,
                    label="extracted from",
                ))

            # Add nodes for hierarchy levels
            for level in range(1, hier.level_count + 1):
                level_node = LineageNode(
                    id=f"level_{hier.id}_{level}",
                    name=f"Level {level}",
                    node_type=NodeType.OUTPUT_COLUMN,
                    description=f"Hierarchy level {level}",
                )
                nodes.append(level_node)

                edges.append(LineageEdge(
                    source_id=hier_node.id,
                    target_id=level_node.id,
                    edge_type=EdgeType.MAPS_TO,
                    label=f"level {level}",
                ))

        return LineageDiagram(
            name=name,
            nodes=nodes,
            edges=edges,
            description=f"Lineage for {len(hierarchies)} hierarchy(s)",
            metadata={
                "hierarchy_count": len(hierarchies),
                "include_mappings": include_mappings,
            },
        )

    def build_lineage_from_sql(
        self,
        tables: list[dict[str, Any]],
        columns: list[dict[str, Any]],
        transformations: list[dict[str, Any]],
        name: str,
    ) -> LineageDiagram:
        """
        Build lineage from SQL analysis results.

        Args:
            tables: List of table metadata
            columns: List of column metadata
            transformations: List of transformations
            name: Diagram name

        Returns:
            LineageDiagram
        """
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        # Add table nodes
        for table in tables:
            node = LineageNode(
                id=f"tbl_{table['name']}",
                name=table["name"],
                node_type=NodeType.SOURCE_TABLE if table.get("is_source") else NodeType.OUTPUT_TABLE,
                description=table.get("description", ""),
                metadata=table.get("metadata", {}),
            )
            nodes.append(node)

        # Add column nodes
        for col in columns:
            node = LineageNode(
                id=f"col_{col['table']}_{col['name']}",
                name=f"{col['table']}.{col['name']}",
                node_type=NodeType.SOURCE_COLUMN if col.get("is_source") else NodeType.OUTPUT_COLUMN,
                description=col.get("description", ""),
            )
            nodes.append(node)

        # Add transformation edges
        for transform in transformations:
            edge = LineageEdge(
                source_id=f"col_{transform['source_table']}_{transform['source_column']}",
                target_id=f"col_{transform['target_table']}_{transform['target_column']}",
                edge_type=EdgeType.TRANSFORMS_TO,
                label=transform.get("operation", ""),
            )
            edges.append(edge)

        return LineageDiagram(
            name=name,
            nodes=nodes,
            edges=edges,
            description=f"SQL lineage with {len(tables)} tables",
        )

    def to_mermaid(self, diagram: LineageDiagram) -> str:
        """
        Export lineage to Mermaid format.

        Args:
            diagram: Lineage diagram

        Returns:
            Mermaid diagram string
        """
        lines = [
            "```mermaid",
            "flowchart LR",
            "",
        ]

        # Define subgraphs by node type
        source_nodes = [n for n in diagram.nodes if n.node_type in [NodeType.SOURCE_TABLE, NodeType.SOURCE_COLUMN]]
        transform_nodes = [n for n in diagram.nodes if n.node_type in [NodeType.TRANSFORMATION, NodeType.HIERARCHY]]
        output_nodes = [n for n in diagram.nodes if n.node_type in [NodeType.OUTPUT_TABLE, NodeType.OUTPUT_COLUMN]]

        # Source subgraph
        if source_nodes:
            lines.append("    subgraph Sources")
            for node in source_nodes:
                shape = self._mermaid_shape(node.node_type)
                lines.append(f"        {self._safe_id(node.id)}{shape[0]}\"{node.name}\"{shape[1]}")
            lines.append("    end")
            lines.append("")

        # Transformation subgraph
        if transform_nodes:
            lines.append("    subgraph Transformations")
            for node in transform_nodes:
                shape = self._mermaid_shape(node.node_type)
                lines.append(f"        {self._safe_id(node.id)}{shape[0]}\"{node.name}\"{shape[1]}")
            lines.append("    end")
            lines.append("")

        # Output subgraph
        if output_nodes:
            lines.append("    subgraph Outputs")
            for node in output_nodes:
                shape = self._mermaid_shape(node.node_type)
                lines.append(f"        {self._safe_id(node.id)}{shape[0]}\"{node.name}\"{shape[1]}")
            lines.append("    end")
            lines.append("")

        # Edges
        for edge in diagram.edges:
            arrow = self._mermaid_arrow(edge.edge_type)
            label = f"|{edge.label}|" if edge.label else ""
            lines.append(f"    {self._safe_id(edge.source_id)} {arrow}{label} {self._safe_id(edge.target_id)}")

        lines.append("```")

        return "\n".join(lines)

    def to_d2(self, diagram: LineageDiagram) -> str:
        """
        Export lineage to D2 format.

        Args:
            diagram: Lineage diagram

        Returns:
            D2 diagram string
        """
        lines = [
            f"# {diagram.name}",
            "",
        ]

        # Define nodes with shapes
        for node in diagram.nodes:
            shape = self._d2_shape(node.node_type)
            style = self._d2_style(node.node_type)
            lines.append(f"{self._safe_id(node.id)}: {node.name} {{")
            lines.append(f"  shape: {shape}")
            if style:
                lines.append(f"  style: {{")
                for k, v in style.items():
                    lines.append(f"    {k}: {v}")
                lines.append(f"  }}")
            lines.append("}")
            lines.append("")

        # Define edges
        for edge in diagram.edges:
            arrow = self._d2_arrow(edge.edge_type)
            if edge.label:
                lines.append(f"{self._safe_id(edge.source_id)} {arrow} {self._safe_id(edge.target_id)}: {edge.label}")
            else:
                lines.append(f"{self._safe_id(edge.source_id)} {arrow} {self._safe_id(edge.target_id)}")

        return "\n".join(lines)

    def to_graphviz(self, diagram: LineageDiagram) -> str:
        """
        Export lineage to GraphViz DOT format.

        Args:
            diagram: Lineage diagram

        Returns:
            DOT format string
        """
        lines = [
            "digraph lineage {",
            "    rankdir=LR;",
            '    node [fontname="Arial"];',
            "",
        ]

        # Define nodes
        for node in diagram.nodes:
            shape = self._graphviz_shape(node.node_type)
            color = self._graphviz_color(node.node_type)
            lines.append(f'    {self._safe_id(node.id)} [label="{node.name}" shape={shape} fillcolor="{color}" style=filled];')

        lines.append("")

        # Define edges
        for edge in diagram.edges:
            style = self._graphviz_edge_style(edge.edge_type)
            label = f' label="{edge.label}"' if edge.label else ""
            lines.append(f'    {self._safe_id(edge.source_id)} -> {self._safe_id(edge.target_id)} [{style}{label}];')

        lines.append("}")

        return "\n".join(lines)

    def to_html(self, diagram: LineageDiagram) -> str:
        """
        Export lineage to interactive HTML.

        Args:
            diagram: Lineage diagram

        Returns:
            HTML string
        """
        mermaid_code = self.to_mermaid(diagram).replace("```mermaid\n", "").replace("\n```", "")

        return f"""<!DOCTYPE html>
<html>
<head>
    <title>{diagram.name} - Lineage</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
        }}
        .mermaid {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metadata {{
            margin-top: 20px;
            padding: 15px;
            background-color: white;
            border-radius: 8px;
        }}
    </style>
</head>
<body>
    <h1>{diagram.name}</h1>
    <p>{diagram.description}</p>

    <div class="mermaid">
{mermaid_code}
    </div>

    <div class="metadata">
        <h3>Diagram Statistics</h3>
        <ul>
            <li>Nodes: {diagram.node_count}</li>
            <li>Edges: {diagram.edge_count}</li>
            <li>Generated: {diagram.generated_at.strftime('%Y-%m-%d %H:%M')}</li>
        </ul>
    </div>

    <script>
        mermaid.initialize({{ startOnLoad: true, theme: 'default' }});
    </script>
</body>
</html>"""

    def to_markdown(self, diagram: LineageDiagram) -> str:
        """
        Export lineage to Markdown with embedded Mermaid.

        Args:
            diagram: Lineage diagram

        Returns:
            Markdown string
        """
        lines = [
            f"# {diagram.name}",
            "",
            diagram.description,
            "",
            "## Lineage Diagram",
            "",
            self.to_mermaid(diagram),
            "",
            "## Node Details",
            "",
            "| Node | Type | Description |",
            "|------|------|-------------|",
        ]

        for node in diagram.nodes:
            lines.append(f"| {node.name} | {node.node_type.value} | {node.description} |")

        lines.extend([
            "",
            "## Edge Details",
            "",
            "| Source | Target | Type | Label |",
            "|--------|--------|------|-------|",
        ])

        for edge in diagram.edges:
            source_name = diagram.get_node(edge.source_id)
            target_name = diagram.get_node(edge.target_id)
            src = source_name.name if source_name else edge.source_id
            tgt = target_name.name if target_name else edge.target_id
            lines.append(f"| {src} | {tgt} | {edge.edge_type.value} | {edge.label} |")

        return "\n".join(lines)

    def _mermaid_shape(self, node_type: NodeType) -> tuple[str, str]:
        """Get Mermaid shape for node type."""
        shapes = {
            NodeType.SOURCE_TABLE: ("[", "]"),
            NodeType.SOURCE_COLUMN: ("(", ")"),
            NodeType.TRANSFORMATION: ("[[", "]]"),
            NodeType.HIERARCHY: ("{", "}"),
            NodeType.OUTPUT_TABLE: ("[", "]"),
            NodeType.OUTPUT_COLUMN: ("(", ")"),
        }
        return shapes.get(node_type, ("[", "]"))

    def _mermaid_arrow(self, edge_type: EdgeType) -> str:
        """Get Mermaid arrow for edge type."""
        arrows = {
            EdgeType.DERIVES_FROM: "-->",
            EdgeType.TRANSFORMS_TO: "==>",
            EdgeType.MAPS_TO: "-.->",
            EdgeType.AGGREGATES: "-->",
            EdgeType.JOINS: "<-->",
        }
        return arrows.get(edge_type, "-->")

    def _d2_shape(self, node_type: NodeType) -> str:
        """Get D2 shape for node type."""
        shapes = {
            NodeType.SOURCE_TABLE: "cylinder",
            NodeType.SOURCE_COLUMN: "oval",
            NodeType.TRANSFORMATION: "hexagon",
            NodeType.HIERARCHY: "diamond",
            NodeType.OUTPUT_TABLE: "cylinder",
            NodeType.OUTPUT_COLUMN: "oval",
        }
        return shapes.get(node_type, "rectangle")

    def _d2_style(self, node_type: NodeType) -> dict[str, str]:
        """Get D2 style for node type."""
        styles = {
            NodeType.SOURCE_TABLE: {"fill": '"#e3f2fd"'},
            NodeType.SOURCE_COLUMN: {"fill": '"#e8f5e9"'},
            NodeType.TRANSFORMATION: {"fill": '"#fff3e0"'},
            NodeType.HIERARCHY: {"fill": '"#fce4ec"'},
            NodeType.OUTPUT_TABLE: {"fill": '"#f3e5f5"'},
            NodeType.OUTPUT_COLUMN: {"fill": '"#e1f5fe"'},
        }
        return styles.get(node_type, {})

    def _d2_arrow(self, edge_type: EdgeType) -> str:
        """Get D2 arrow for edge type."""
        arrows = {
            EdgeType.DERIVES_FROM: "->",
            EdgeType.TRANSFORMS_TO: "->",
            EdgeType.MAPS_TO: "->",
            EdgeType.AGGREGATES: "->",
            EdgeType.JOINS: "<->",
        }
        return arrows.get(edge_type, "->")

    def _graphviz_shape(self, node_type: NodeType) -> str:
        """Get GraphViz shape for node type."""
        shapes = {
            NodeType.SOURCE_TABLE: "cylinder",
            NodeType.SOURCE_COLUMN: "ellipse",
            NodeType.TRANSFORMATION: "hexagon",
            NodeType.HIERARCHY: "diamond",
            NodeType.OUTPUT_TABLE: "box3d",
            NodeType.OUTPUT_COLUMN: "ellipse",
        }
        return shapes.get(node_type, "box")

    def _graphviz_color(self, node_type: NodeType) -> str:
        """Get GraphViz color for node type."""
        colors = {
            NodeType.SOURCE_TABLE: "#e3f2fd",
            NodeType.SOURCE_COLUMN: "#e8f5e9",
            NodeType.TRANSFORMATION: "#fff3e0",
            NodeType.HIERARCHY: "#fce4ec",
            NodeType.OUTPUT_TABLE: "#f3e5f5",
            NodeType.OUTPUT_COLUMN: "#e1f5fe",
        }
        return colors.get(node_type, "#ffffff")

    def _graphviz_edge_style(self, edge_type: EdgeType) -> str:
        """Get GraphViz edge style for edge type."""
        styles = {
            EdgeType.DERIVES_FROM: "style=solid",
            EdgeType.TRANSFORMS_TO: "style=bold",
            EdgeType.MAPS_TO: "style=dashed",
            EdgeType.AGGREGATES: "style=dotted",
            EdgeType.JOINS: "style=solid dir=both",
        }
        return styles.get(edge_type, "style=solid")

    def _safe_id(self, id_str: str) -> str:
        """Make ID safe for diagram formats."""
        import re
        # Replace special characters with underscores
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', id_str)
        # Ensure starts with letter
        if safe and not safe[0].isalpha():
            safe = "n_" + safe
        return safe

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for identifiers."""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        return sanitized.strip('_').upper()
