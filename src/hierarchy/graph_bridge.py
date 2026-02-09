"""Hierarchy-Graph Bridge — auto-populates vector store and lineage graph on hierarchy events.

Subscribes to AutoSyncManager callbacks so that every hierarchy create/update/delete
automatically keeps the GraphRAG index and lineage graph in sync.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hierarchy_graph_bridge")


class HierarchyGraphBridge:
    """Bridge between HierarchyService events and GraphRAG / Lineage systems."""

    def __init__(
        self,
        hierarchy_service: Any,
        vector_store: Any = None,
        embedding_provider: Any = None,
        lineage_tracker: Any = None,
    ):
        """
        Args:
            hierarchy_service: HierarchyService instance
            vector_store: Optional VectorStore for semantic indexing
            embedding_provider: Optional EmbeddingProvider for generating embeddings
            lineage_tracker: Optional LineageTracker for lineage graph population
        """
        self.hierarchy_service = hierarchy_service
        self._vector_store = vector_store
        self._embedder = embedding_provider
        self._lineage_tracker = lineage_tracker

    # ------------------------------------------------------------------
    # Event callback (registered with AutoSyncManager.add_callback)
    # ------------------------------------------------------------------

    def on_hierarchy_change(self, event_data: Dict[str, Any]) -> None:
        """
        Callback for AutoSyncManager — receives rich event data dict.

        Expected keys:
            operation, project_id, hierarchy_id, data, timestamp
        """
        if not isinstance(event_data, dict):
            return

        operation = event_data.get("operation", "")
        project_id = event_data.get("project_id", "")
        hierarchy_id = event_data.get("hierarchy_id")

        try:
            if operation in ("create_hierarchy", "update_hierarchy", "add_mapping"):
                if hierarchy_id:
                    hier = self.hierarchy_service.get_hierarchy(project_id, hierarchy_id)
                    if hier:
                        self.index_hierarchy_rich(project_id, hier)
                        self.sync_lineage(project_id, hier)
            elif operation == "delete_hierarchy":
                if hierarchy_id:
                    self.remove_from_index(project_id, hierarchy_id)
            elif operation in ("create_project", "delete_project"):
                # Re-index entire project on project-level events
                if operation == "create_project":
                    self._index_project(project_id)
                else:
                    self._remove_project_from_index(project_id)
        except Exception as e:
            logger.warning(f"HierarchyGraphBridge event handler error: {e}")

    # ------------------------------------------------------------------
    # Rich content generation
    # ------------------------------------------------------------------

    def build_hierarchy_content(self, hierarchy: Dict, project_name: str = "") -> str:
        """Build rich text content for embedding — much richer than just name + levels."""
        name = hierarchy.get("hierarchy_name", "")
        hier_id = hierarchy.get("hierarchy_id", "")
        description = hierarchy.get("description", "")
        parent_id = hierarchy.get("parent_id", "")
        is_root = hierarchy.get("is_root", False)

        # Levels
        levels = hierarchy.get("hierarchy_level", {}) or {}
        level_parts = []
        for i in range(1, 16):
            val = levels.get(f"level_{i}")
            if val:
                level_parts.append(f"L{i}: {val}")
        level_str = " > ".join(level_parts) if level_parts else "none"

        # Source mappings
        mappings = hierarchy.get("mapping", [])
        mapping_strs = []
        for m in mappings:
            db = m.get("source_database", "")
            schema = m.get("source_schema", "")
            table = m.get("source_table", "")
            col = m.get("source_column", "")
            mapping_strs.append(f"{db}.{schema}.{table}.{col}")
        mapping_str = ", ".join(mapping_strs) if mapping_strs else "none"

        # Properties
        properties = hierarchy.get("properties", [])
        prop_strs = [f"{p.get('name')}={p.get('value')}" for p in properties]
        prop_str = ", ".join(prop_strs) if prop_strs else "none"

        # Formulas
        formula_config = hierarchy.get("formula_config", {}) or {}
        formula_group = formula_config.get("formula_group", {}) or {}
        rules = formula_group.get("rules", [])
        formula_strs = []
        for r in rules:
            op = r.get("operation", "")
            ref = r.get("hierarchy_name", r.get("hierarchy_id", ""))
            formula_strs.append(f"{op} [{ref}]")
        formula_str = " ".join(formula_strs) if formula_strs else "none"

        # Flags
        flags = hierarchy.get("flags", {}) or {}
        flag_parts = []
        if flags.get("calculation_flag"):
            flag_parts.append("calculation")
        if flags.get("exclude_flag"):
            flag_parts.append("excluded")
        if flags.get("is_leaf_node"):
            flag_parts.append("leaf")
        flag_str = ", ".join(flag_parts) if flag_parts else "active"

        # Dimension/Fact props
        dim_props = hierarchy.get("dimension_props", {}) or {}
        fact_props = hierarchy.get("fact_props", {}) or {}
        hier_type = dim_props.get("hierarchy_type", fact_props.get("measure_type", ""))

        lines = [
            f"Hierarchy: {name} (ID: {hier_id})",
        ]
        if project_name:
            lines.append(f"Project: {project_name}")
        if description:
            lines.append(f"Description: {description}")
        if hier_type:
            lines.append(f"Type: {hier_type}")
        lines.append(f"Role: {'root' if is_root else 'child'} | Flags: {flag_str}")
        lines.append(f"Levels: {level_str}")
        lines.append(f"Source Mappings: {mapping_str}")
        if prop_str != "none":
            lines.append(f"Properties: {prop_str}")
        if formula_str != "none":
            lines.append(f"Formula: {formula_str}")
        if parent_id:
            lines.append(f"Parent: {parent_id}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------

    def index_hierarchy_rich(self, project_id: str, hierarchy: Dict) -> bool:
        """Upsert a single hierarchy into the vector store with rich content."""
        if not self._vector_store or not self._embedder:
            return False

        hier_id = hierarchy.get("hierarchy_id", hierarchy.get("id", ""))
        doc_id = f"hierarchy:{project_id}:{hier_id}"

        # Get project name for richer context
        project = self.hierarchy_service.get_project(project_id)
        project_name = project.get("name", "") if project else ""

        content = self.build_hierarchy_content(hierarchy, project_name)

        try:
            embedding = self._embedder.embed(content)
            mappings = hierarchy.get("mapping", [])
            properties = hierarchy.get("properties", [])
            levels = hierarchy.get("hierarchy_level", {}) or {}
            level_depth = sum(1 for i in range(1, 16) if levels.get(f"level_{i}"))

            self._vector_store.upsert(
                id=doc_id,
                embedding=embedding,
                content=content,
                metadata={
                    "source_type": "hierarchy",
                    "project_id": project_id,
                    "hierarchy_id": hier_id,
                    "name": hierarchy.get("hierarchy_name", ""),
                    "parent_id": hierarchy.get("parent_id"),
                    "is_root": hierarchy.get("is_root", False),
                    "has_mappings": len(mappings) > 0,
                    "has_formula": bool(hierarchy.get("formula_config", {}).get("formula_group")),
                    "property_count": len(properties),
                    "level_depth": level_depth,
                    "mapping_count": len(mappings),
                },
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to index hierarchy {hier_id}: {e}")
            return False

    def remove_from_index(self, project_id: str, hierarchy_id: str) -> bool:
        """Remove a hierarchy from the vector store."""
        if not self._vector_store:
            return False
        try:
            doc_id = f"hierarchy:{project_id}:{hierarchy_id}"
            self._vector_store.delete(doc_id)
            return True
        except Exception as e:
            logger.warning(f"Failed to remove hierarchy {hierarchy_id} from index: {e}")
            return False

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------

    def sync_lineage(self, project_id: str, hierarchy: Dict) -> bool:
        """Create lineage nodes and edges from hierarchy source mappings."""
        if not self._lineage_tracker:
            return False

        hier_id = hierarchy.get("hierarchy_id", "")
        hier_name = hierarchy.get("hierarchy_name", "")

        try:
            # Register hierarchy as a lineage node
            self._lineage_tracker.register_node(
                node_id=f"hierarchy:{project_id}:{hier_id}",
                node_type="hierarchy",
                name=hier_name,
                metadata={"project_id": project_id, "hierarchy_id": hier_id},
            )

            # Create edges from source mappings
            for m in hierarchy.get("mapping", []):
                table = m.get("source_table", "")
                col = m.get("source_column", "")
                if table:
                    source_node = f"table:{m.get('source_database', '')}.{m.get('source_schema', '')}.{table}"
                    self._lineage_tracker.add_edge(
                        source=source_node,
                        target=f"hierarchy:{project_id}:{hier_id}",
                        edge_type="maps_to",
                        metadata={"column": col},
                    )

            # Create parent-child edge
            parent_id = hierarchy.get("parent_id")
            if parent_id:
                self._lineage_tracker.add_edge(
                    source=f"hierarchy:{project_id}:{parent_id}",
                    target=f"hierarchy:{project_id}:{hier_id}",
                    edge_type="parent_of",
                )

            # Create formula reference edges
            formula_config = hierarchy.get("formula_config", {}) or {}
            formula_group = formula_config.get("formula_group", {}) or {}
            for rule in formula_group.get("rules", []):
                ref_id = rule.get("hierarchy_id", "")
                if ref_id:
                    self._lineage_tracker.add_edge(
                        source=f"hierarchy:{project_id}:{ref_id}",
                        target=f"hierarchy:{project_id}:{hier_id}",
                        edge_type="formula_ref",
                        metadata={"operation": rule.get("operation", "")},
                    )

            return True
        except Exception as e:
            logger.warning(f"Failed to sync lineage for {hier_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def reindex_project(self, project_id: str) -> Dict[str, Any]:
        """Full reindex of all hierarchies in a project."""
        indexed = 0
        errors = 0

        self._index_project(project_id)

        hierarchies = self.hierarchy_service.list_hierarchies(project_id)
        for hier in hierarchies:
            if self.index_hierarchy_rich(project_id, hier):
                self.sync_lineage(project_id, hier)
                indexed += 1
            else:
                errors += 1

        return {"indexed": indexed, "errors": errors, "project_id": project_id}

    def _index_project(self, project_id: str) -> bool:
        """Index the project itself as a vector store document."""
        if not self._vector_store or not self._embedder:
            return False

        project = self.hierarchy_service.get_project(project_id)
        if not project:
            return False

        doc_id = f"hierarchy_project:{project_id}"
        hierarchies = self.hierarchy_service.list_hierarchies(project_id)
        mapping_count = sum(len(h.get("mapping", [])) for h in hierarchies)

        content = (
            f"Hierarchy Project: {project.get('name', '')}\n"
            f"Description: {project.get('description', '')}\n"
            f"Hierarchies: {len(hierarchies)}, Mappings: {mapping_count}"
        )

        try:
            embedding = self._embedder.embed(content)
            self._vector_store.upsert(
                id=doc_id,
                embedding=embedding,
                content=content,
                metadata={
                    "source_type": "hierarchy_project",
                    "project_id": project_id,
                    "name": project.get("name", ""),
                    "hierarchy_count": len(hierarchies),
                    "mapping_count": mapping_count,
                },
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to index project {project_id}: {e}")
            return False

    def _remove_project_from_index(self, project_id: str) -> None:
        """Remove all hierarchy documents for a project from the vector store."""
        if not self._vector_store:
            return
        try:
            self._vector_store.delete(f"hierarchy_project:{project_id}")
            # Note: individual hierarchy docs would also need cleanup;
            # vector stores that support prefix-delete can do this efficiently.
        except Exception as e:
            logger.warning(f"Failed to remove project {project_id} from index: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Return bridge status for diagnostics."""
        return {
            "vector_store_available": self._vector_store is not None,
            "embedder_available": self._embedder is not None,
            "lineage_tracker_available": self._lineage_tracker is not None,
        }
