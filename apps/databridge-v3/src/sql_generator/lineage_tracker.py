"""
Data Lineage Tracker Service for SQL Generator.

Uses NetworkX for graph-based lineage tracking:
- Track data flow between objects
- Upstream/downstream traversal
- Column-level lineage (optional)
- Lineage visualization
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime
import json

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    nx = None

from sqlalchemy.orm import Session

from .models import DataLineageEdge, TransformationType, ObjectType


class LineageTrackerService:
    """
    Service for tracking data lineage using NetworkX graphs.

    Provides:
    - Add/remove lineage edges
    - Upstream traversal (where does data come from?)
    - Downstream traversal (where does data go?)
    - Impact analysis
    - Lineage visualization
    """

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize the lineage tracker service.

        Args:
            session: Optional SQLAlchemy session
        """
        self._session = session
        self._graphs: Dict[str, "nx.DiGraph"] = {}  # Cache graphs per project

        if not HAS_NETWORKX:
            raise ImportError(
                "NetworkX is required for lineage tracking. "
                "Install with: pip install networkx"
            )

    def _get_session(self, session: Optional[Session] = None) -> Session:
        """Get session from parameter or instance."""
        if session:
            return session
        if self._session:
            return self._session
        raise ValueError("No database session available")

    def _get_graph(self, project_id: str, session: Optional[Session] = None) -> "nx.DiGraph":
        """
        Get or build the lineage graph for a project.

        Args:
            project_id: Project ID
            session: Optional database session

        Returns:
            NetworkX DiGraph for the project
        """
        if project_id not in self._graphs:
            self._graphs[project_id] = self._build_graph(project_id, session)
        return self._graphs[project_id]

    def _build_graph(self, project_id: str, session: Optional[Session] = None) -> "nx.DiGraph":
        """
        Build a NetworkX graph from stored lineage edges.

        Args:
            project_id: Project ID
            session: Optional database session

        Returns:
            NetworkX DiGraph with all lineage edges
        """
        db = self._get_session(session)
        G = nx.DiGraph()

        edges = db.query(DataLineageEdge).filter(
            DataLineageEdge.project_id == project_id,
            DataLineageEdge.is_active == True,
        ).all()

        for edge in edges:
            source, target, attrs = edge.to_edge_tuple()
            G.add_edge(source, target, **attrs)

        return G

    def _invalidate_cache(self, project_id: str) -> None:
        """Invalidate the cached graph for a project."""
        if project_id in self._graphs:
            del self._graphs[project_id]

    def track_lineage(
        self,
        project_id: str,
        source_object: str,
        target_object: str,
        transformation_type: TransformationType,
        source_object_type: Optional[ObjectType] = None,
        target_object_type: Optional[ObjectType] = None,
        source_column: Optional[str] = None,
        target_column: Optional[str] = None,
        transformation_logic: Optional[str] = None,
        description: Optional[str] = None,
        confidence_score: int = 100,
        is_inferred: bool = False,
        source_object_id: Optional[str] = None,
        target_object_id: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> DataLineageEdge:
        """
        Record a lineage edge between source and target objects.

        Args:
            project_id: Project ID
            source_object: Fully qualified source object name
            target_object: Fully qualified target object name
            transformation_type: Type of transformation
            source_object_type: Type of source object
            target_object_type: Type of target object
            source_column: Optional source column for column-level lineage
            target_column: Optional target column for column-level lineage
            transformation_logic: SQL or formula for the transformation
            description: Human-readable description
            confidence_score: Confidence 0-100 (for inferred lineage)
            is_inferred: Whether this was auto-detected
            source_object_id: Optional ID of source GeneratedView
            target_object_id: Optional ID of target GeneratedView
            session: Optional database session

        Returns:
            Created DataLineageEdge
        """
        db = self._get_session(session)

        # Check for existing edge
        existing = db.query(DataLineageEdge).filter(
            DataLineageEdge.project_id == project_id,
            DataLineageEdge.source_object_name == source_object,
            DataLineageEdge.target_object_name == target_object,
            DataLineageEdge.source_column == source_column,
            DataLineageEdge.target_column == target_column,
            DataLineageEdge.is_active == True,
        ).first()

        if existing:
            # Update existing edge
            existing.transformation_type = transformation_type
            existing.transformation_logic = transformation_logic
            existing.description = description
            existing.confidence_score = confidence_score
            existing.updated_at = datetime.utcnow()
            db.commit()
            self._invalidate_cache(project_id)
            return existing

        # Create new edge
        edge = DataLineageEdge(
            project_id=project_id,
            source_object_id=source_object_id,
            source_object_type=source_object_type,
            source_object_name=source_object,
            source_column=source_column,
            target_object_id=target_object_id,
            target_object_type=target_object_type,
            target_object_name=target_object,
            target_column=target_column,
            transformation_type=transformation_type,
            transformation_logic=transformation_logic,
            description=description,
            confidence_score=confidence_score,
            is_inferred=is_inferred,
        )
        db.add(edge)
        db.commit()
        db.refresh(edge)

        self._invalidate_cache(project_id)
        return edge

    def remove_lineage(
        self,
        project_id: str,
        source_object: str,
        target_object: str,
        source_column: Optional[str] = None,
        target_column: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> bool:
        """
        Remove a lineage edge (soft delete).

        Args:
            project_id: Project ID
            source_object: Source object name
            target_object: Target object name
            source_column: Optional source column
            target_column: Optional target column
            session: Optional database session

        Returns:
            True if edge was found and removed
        """
        db = self._get_session(session)

        edge = db.query(DataLineageEdge).filter(
            DataLineageEdge.project_id == project_id,
            DataLineageEdge.source_object_name == source_object,
            DataLineageEdge.target_object_name == target_object,
            DataLineageEdge.source_column == source_column,
            DataLineageEdge.target_column == target_column,
            DataLineageEdge.is_active == True,
        ).first()

        if edge:
            edge.is_active = False
            edge.updated_at = datetime.utcnow()
            db.commit()
            self._invalidate_cache(project_id)
            return True

        return False

    def get_upstream(
        self,
        project_id: str,
        object_name: str,
        max_depth: Optional[int] = None,
        include_columns: bool = False,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all upstream objects (data sources) for a given object.

        Args:
            project_id: Project ID
            object_name: Object to trace upstream from
            max_depth: Maximum traversal depth (None for unlimited)
            include_columns: Include column-level details
            session: Optional database session

        Returns:
            List of upstream objects with metadata
        """
        G = self._get_graph(project_id, session)

        if object_name not in G:
            return []

        # Get all ancestors (upstream) using BFS
        upstream = []
        visited = set()
        queue = [(object_name, 0)]

        while queue:
            current, depth = queue.pop(0)
            if current in visited:
                continue
            if max_depth is not None and depth > max_depth:
                continue

            visited.add(current)

            # Get predecessors (sources)
            for pred in G.predecessors(current):
                edge_data = G.get_edge_data(pred, current) or {}

                if not include_columns and edge_data.get("source_column"):
                    continue

                upstream.append({
                    "object_name": pred,
                    "target_object": current,
                    "depth": depth,
                    "transformation_type": edge_data.get("transformation_type"),
                    "transformation_logic": edge_data.get("transformation_logic"),
                    "source_column": edge_data.get("source_column"),
                    "target_column": edge_data.get("target_column"),
                })

                if pred not in visited:
                    queue.append((pred, depth + 1))

        return upstream

    def get_downstream(
        self,
        project_id: str,
        object_name: str,
        max_depth: Optional[int] = None,
        include_columns: bool = False,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all downstream objects (data targets) for a given object.

        Args:
            project_id: Project ID
            object_name: Object to trace downstream from
            max_depth: Maximum traversal depth (None for unlimited)
            include_columns: Include column-level details
            session: Optional database session

        Returns:
            List of downstream objects with metadata
        """
        G = self._get_graph(project_id, session)

        if object_name not in G:
            return []

        downstream = []
        visited = set()
        queue = [(object_name, 0)]

        while queue:
            current, depth = queue.pop(0)
            if current in visited:
                continue
            if max_depth is not None and depth > max_depth:
                continue

            visited.add(current)

            # Get successors (targets)
            for succ in G.successors(current):
                edge_data = G.get_edge_data(current, succ) or {}

                if not include_columns and edge_data.get("source_column"):
                    continue

                downstream.append({
                    "object_name": succ,
                    "source_object": current,
                    "depth": depth,
                    "transformation_type": edge_data.get("transformation_type"),
                    "transformation_logic": edge_data.get("transformation_logic"),
                    "source_column": edge_data.get("source_column"),
                    "target_column": edge_data.get("target_column"),
                })

                if succ not in visited:
                    queue.append((succ, depth + 1))

        return downstream

    def get_full_lineage(
        self,
        project_id: str,
        object_name: str,
        max_depth: Optional[int] = None,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Get complete lineage (both upstream and downstream) for an object.

        Args:
            project_id: Project ID
            object_name: Object to analyze
            max_depth: Maximum traversal depth
            session: Optional database session

        Returns:
            Dictionary with upstream, downstream, and summary
        """
        upstream = self.get_upstream(project_id, object_name, max_depth, session=session)
        downstream = self.get_downstream(project_id, object_name, max_depth, session=session)

        # Get unique objects
        upstream_objects = list(set(u["object_name"] for u in upstream))
        downstream_objects = list(set(d["object_name"] for d in downstream))

        return {
            "object": object_name,
            "upstream": upstream,
            "downstream": downstream,
            "summary": {
                "upstream_count": len(upstream_objects),
                "downstream_count": len(downstream_objects),
                "total_edges": len(upstream) + len(downstream),
                "upstream_objects": upstream_objects,
                "downstream_objects": downstream_objects,
            }
        }

    def analyze_impact(
        self,
        project_id: str,
        object_name: str,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Analyze the impact of changes to an object.

        Shows all objects that would be affected by changes.

        Args:
            project_id: Project ID
            object_name: Object being changed
            session: Optional database session

        Returns:
            Impact analysis with affected objects
        """
        downstream = self.get_downstream(project_id, object_name, session=session)

        # Group by depth
        by_depth: Dict[int, List[str]] = {}
        for item in downstream:
            depth = item["depth"]
            if depth not in by_depth:
                by_depth[depth] = []
            if item["object_name"] not in by_depth[depth]:
                by_depth[depth].append(item["object_name"])

        # Count impact
        direct_impact = by_depth.get(0, [])
        indirect_impact = []
        for depth, objects in by_depth.items():
            if depth > 0:
                indirect_impact.extend(objects)

        return {
            "source_object": object_name,
            "direct_impact": direct_impact,
            "indirect_impact": list(set(indirect_impact)),
            "total_affected": len(set(direct_impact + indirect_impact)),
            "by_depth": {str(k): v for k, v in sorted(by_depth.items())},
        }

    def visualize_lineage(
        self,
        project_id: str,
        object_name: Optional[str] = None,
        format: str = "mermaid",
        max_depth: Optional[int] = 3,
        session: Optional[Session] = None,
    ) -> str:
        """
        Generate a visualization of the lineage graph.

        Args:
            project_id: Project ID
            object_name: Optional object to center on
            format: Output format ('mermaid', 'dot', 'json')
            max_depth: Max depth for subgraph
            session: Optional database session

        Returns:
            Visualization string in requested format
        """
        G = self._get_graph(project_id, session)

        # If object specified, get subgraph
        if object_name and object_name in G:
            # Get nodes within max_depth
            upstream = self.get_upstream(project_id, object_name, max_depth, session=session)
            downstream = self.get_downstream(project_id, object_name, max_depth, session=session)

            nodes = {object_name}
            nodes.update(u["object_name"] for u in upstream)
            nodes.update(d["object_name"] for d in downstream)

            G = G.subgraph(nodes).copy()

        if format == "mermaid":
            return self._to_mermaid(G, object_name)
        elif format == "dot":
            return self._to_dot(G, object_name)
        elif format == "json":
            return self._to_json(G)
        else:
            raise ValueError(f"Unknown format: {format}")

    def _to_mermaid(self, G: "nx.DiGraph", highlight: Optional[str] = None) -> str:
        """Convert graph to Mermaid diagram format."""
        lines = ["graph LR"]

        # Add node styling
        if highlight:
            lines.append(f"    style {self._sanitize_node(highlight)} fill:#f9f,stroke:#333,stroke-width:4px")

        # Add edges
        for source, target, data in G.edges(data=True):
            src = self._sanitize_node(source)
            tgt = self._sanitize_node(target)
            transform = data.get("transformation_type", "")
            if transform:
                lines.append(f"    {src} -->|{transform}| {tgt}")
            else:
                lines.append(f"    {src} --> {tgt}")

        return "\n".join(lines)

    def _to_dot(self, G: "nx.DiGraph", highlight: Optional[str] = None) -> str:
        """Convert graph to DOT format for Graphviz."""
        lines = ["digraph lineage {"]
        lines.append("    rankdir=LR;")
        lines.append("    node [shape=box];")

        if highlight:
            lines.append(f'    "{highlight}" [style=filled,fillcolor=yellow];')

        for source, target, data in G.edges(data=True):
            transform = data.get("transformation_type", "")
            if transform:
                lines.append(f'    "{source}" -> "{target}" [label="{transform}"];')
            else:
                lines.append(f'    "{source}" -> "{target}";')

        lines.append("}")
        return "\n".join(lines)

    def _to_json(self, G: "nx.DiGraph") -> str:
        """Convert graph to JSON format."""
        data = {
            "nodes": list(G.nodes()),
            "edges": [
                {
                    "source": s,
                    "target": t,
                    **d,
                }
                for s, t, d in G.edges(data=True)
            ],
        }
        return json.dumps(data, indent=2, default=str)

    def _sanitize_node(self, name: str) -> str:
        """Sanitize node name for Mermaid (no dots or special chars)."""
        return name.replace(".", "_").replace("-", "_").replace(" ", "_")

    def list_edges(
        self,
        project_id: str,
        source_object: Optional[str] = None,
        target_object: Optional[str] = None,
        transformation_type: Optional[TransformationType] = None,
        session: Optional[Session] = None,
    ) -> List[DataLineageEdge]:
        """
        List lineage edges with optional filters.

        Args:
            project_id: Project ID
            source_object: Filter by source object
            target_object: Filter by target object
            transformation_type: Filter by transformation type
            session: Optional database session

        Returns:
            List of matching DataLineageEdge objects
        """
        db = self._get_session(session)

        query = db.query(DataLineageEdge).filter(
            DataLineageEdge.project_id == project_id,
            DataLineageEdge.is_active == True,
        )

        if source_object:
            query = query.filter(DataLineageEdge.source_object_name == source_object)
        if target_object:
            query = query.filter(DataLineageEdge.target_object_name == target_object)
        if transformation_type:
            query = query.filter(DataLineageEdge.transformation_type == transformation_type)

        return query.all()


# Singleton instance
_lineage_tracker = None


def get_lineage_tracker(session: Optional[Session] = None) -> LineageTrackerService:
    """Get or create the lineage tracker singleton."""
    global _lineage_tracker
    if _lineage_tracker is None:
        _lineage_tracker = LineageTrackerService(session)
    return _lineage_tracker
