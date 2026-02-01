"""
Concept Merger for consolidating similar entities into unified concepts.

This module provides functionality to merge similar schema elements into
canonical concepts, reducing duplication and improving semantic consistency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from databridge_discovery.consolidation.entity_matcher import EntityMatcher, MatchResult
from databridge_discovery.graph.node_types import (
    ConceptNode,
    GraphNode,
    NodeType,
)


@dataclass
class MergeCandidate:
    """Candidate group for merging."""

    nodes: list[GraphNode]
    canonical_name: str
    confidence: float
    merge_reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MergeResult:
    """Result from a merge operation."""

    concept: ConceptNode
    merged_nodes: list[str]  # IDs of merged nodes
    confidence: float
    merge_type: str  # "automatic", "suggested", "manual"


class ConceptMerger:
    """
    Merges similar entities into unified concepts.

    Analyzes schema elements and identifies opportunities to consolidate
    duplicates, similar names, and semantically equivalent entities.

    Example:
        merger = ConceptMerger()

        # Find merge candidates
        candidates = merger.find_merge_candidates(nodes)

        # Preview merges
        for candidate in candidates:
            print(f"Merge {len(candidate.nodes)} nodes into '{candidate.canonical_name}'")

        # Execute merges
        concepts = merger.merge_all(candidates)
    """

    def __init__(
        self,
        matcher: EntityMatcher | None = None,
        similarity_threshold: float = 0.85,
        min_group_size: int = 2,
        auto_merge_threshold: float = 0.95,
    ):
        """
        Initialize concept merger.

        Args:
            matcher: EntityMatcher instance (creates one if not provided)
            similarity_threshold: Threshold for considering entities as candidates
            min_group_size: Minimum group size to consider for merging
            auto_merge_threshold: Threshold for automatic (no-confirm) merging
        """
        self.matcher = matcher or EntityMatcher(similarity_threshold=similarity_threshold)
        self.similarity_threshold = similarity_threshold
        self.min_group_size = min_group_size
        self.auto_merge_threshold = auto_merge_threshold

        # Track merged concepts
        self._concepts: dict[str, ConceptNode] = {}
        self._node_to_concept: dict[str, str] = {}  # node_id -> concept_id

    def find_merge_candidates(
        self,
        nodes: list[GraphNode],
        by_type: bool = True,
    ) -> list[MergeCandidate]:
        """
        Find groups of nodes that could be merged.

        Args:
            nodes: Nodes to analyze
            by_type: Whether to group by node type first

        Returns:
            List of MergeCandidate
        """
        candidates = []

        if by_type:
            # Group nodes by type first
            type_groups: dict[str, list[GraphNode]] = {}
            for node in nodes:
                node_type = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)
                if node_type not in type_groups:
                    type_groups[node_type] = []
                type_groups[node_type].append(node)

            # Find candidates within each type
            for node_type, type_nodes in type_groups.items():
                type_candidates = self._find_candidates_in_group(type_nodes)
                candidates.extend(type_candidates)
        else:
            # Find candidates across all nodes
            candidates = self._find_candidates_in_group(nodes)

        # Sort by confidence and group size
        candidates.sort(key=lambda c: (c.confidence, len(c.nodes)), reverse=True)
        return candidates

    def _find_candidates_in_group(
        self,
        nodes: list[GraphNode],
    ) -> list[MergeCandidate]:
        """Find merge candidates within a group of nodes."""
        candidates = []

        # Group similar nodes
        groups = self.matcher.group_similar(nodes, threshold=self.similarity_threshold)

        for group in groups:
            if len(group) < self.min_group_size:
                continue

            # Calculate group confidence
            similarities = []
            for i, node1 in enumerate(group):
                for node2 in group[i + 1:]:
                    sim = self.matcher.calculate_similarity(node1.name, node2.name)
                    similarities.append(sim)

            avg_similarity = sum(similarities) / len(similarities) if similarities else 0

            # Suggest canonical name
            names = [n.name for n in group]
            canonical_name = self.matcher.suggest_canonical_name(names)

            # Determine merge reason
            if avg_similarity >= self.auto_merge_threshold:
                merge_reason = "High similarity - exact or near-exact matches"
            elif avg_similarity >= 0.9:
                merge_reason = "Very similar names with minor variations"
            else:
                merge_reason = "Similar names - may represent same concept"

            candidates.append(MergeCandidate(
                nodes=group,
                canonical_name=canonical_name,
                confidence=avg_similarity,
                merge_reason=merge_reason,
                metadata={
                    "node_count": len(group),
                    "original_names": names,
                    "avg_similarity": avg_similarity,
                },
            ))

        return candidates

    def merge_candidate(
        self,
        candidate: MergeCandidate,
        concept_type: str = "generic",
        description: str | None = None,
    ) -> MergeResult:
        """
        Merge a single candidate into a concept.

        Args:
            candidate: MergeCandidate to merge
            concept_type: Type of concept to create
            description: Optional description

        Returns:
            MergeResult
        """
        # Create concept node
        concept = ConceptNode(
            id=str(uuid4()),
            name=candidate.canonical_name,
            node_type=NodeType.CONCEPT,
            display_name=candidate.canonical_name.replace("_", " ").title(),
            description=description or f"Merged from {len(candidate.nodes)} similar entities",
            concept_type=concept_type,
            member_ids=[n.id for n in candidate.nodes],
            confidence=candidate.confidence,
            canonical_name=candidate.canonical_name,
            aliases=[n.name for n in candidate.nodes if n.name != candidate.canonical_name],
            metadata={
                "merge_reason": candidate.merge_reason,
                "original_count": len(candidate.nodes),
                **candidate.metadata,
            },
            tags=self._collect_tags(candidate.nodes),
        )

        # Track the concept
        self._concepts[concept.id] = concept
        for node in candidate.nodes:
            self._node_to_concept[node.id] = concept.id

        # Determine merge type
        if candidate.confidence >= self.auto_merge_threshold:
            merge_type = "automatic"
        elif candidate.confidence >= self.similarity_threshold:
            merge_type = "suggested"
        else:
            merge_type = "manual"

        return MergeResult(
            concept=concept,
            merged_nodes=[n.id for n in candidate.nodes],
            confidence=candidate.confidence,
            merge_type=merge_type,
        )

    def merge_all(
        self,
        candidates: list[MergeCandidate],
        auto_only: bool = False,
    ) -> list[MergeResult]:
        """
        Merge all candidates.

        Args:
            candidates: List of MergeCandidate
            auto_only: Only merge high-confidence candidates

        Returns:
            List of MergeResult
        """
        results = []

        for candidate in candidates:
            # Skip low confidence if auto_only
            if auto_only and candidate.confidence < self.auto_merge_threshold:
                continue

            # Skip if any node already merged
            already_merged = any(
                n.id in self._node_to_concept for n in candidate.nodes
            )
            if already_merged:
                continue

            result = self.merge_candidate(candidate)
            results.append(result)

        return results

    def merge_nodes(
        self,
        nodes: list[GraphNode],
        canonical_name: str | None = None,
        concept_type: str = "generic",
    ) -> MergeResult:
        """
        Manually merge a list of nodes.

        Args:
            nodes: Nodes to merge
            canonical_name: Name for the concept (auto-suggested if None)
            concept_type: Type of concept

        Returns:
            MergeResult
        """
        if not nodes:
            raise ValueError("Cannot merge empty node list")

        # Calculate similarity for confidence
        if len(nodes) > 1:
            similarities = []
            for i, n1 in enumerate(nodes):
                for n2 in nodes[i + 1:]:
                    sim = self.matcher.calculate_similarity(n1.name, n2.name)
                    similarities.append(sim)
            confidence = sum(similarities) / len(similarities)
        else:
            confidence = 1.0

        # Suggest name if not provided
        if not canonical_name:
            canonical_name = self.matcher.suggest_canonical_name([n.name for n in nodes])

        candidate = MergeCandidate(
            nodes=nodes,
            canonical_name=canonical_name,
            confidence=confidence,
            merge_reason="Manual merge",
        )

        return self.merge_candidate(candidate, concept_type=concept_type)

    def get_concept_for_node(self, node_id: str) -> ConceptNode | None:
        """
        Get the concept that a node was merged into.

        Args:
            node_id: Node ID

        Returns:
            ConceptNode or None if not merged
        """
        concept_id = self._node_to_concept.get(node_id)
        if concept_id:
            return self._concepts.get(concept_id)
        return None

    def get_all_concepts(self) -> list[ConceptNode]:
        """
        Get all merged concepts.

        Returns:
            List of ConceptNode
        """
        return list(self._concepts.values())

    def unmerge_concept(self, concept_id: str) -> list[str]:
        """
        Unmerge a concept back to individual nodes.

        Args:
            concept_id: Concept ID to unmerge

        Returns:
            List of node IDs that were unmerged
        """
        concept = self._concepts.get(concept_id)
        if not concept:
            return []

        # Remove node-to-concept mappings
        unmerged_nodes = []
        for node_id, cid in list(self._node_to_concept.items()):
            if cid == concept_id:
                del self._node_to_concept[node_id]
                unmerged_nodes.append(node_id)

        # Remove concept
        del self._concepts[concept_id]

        return unmerged_nodes

    def suggest_cross_type_merges(
        self,
        nodes: list[GraphNode],
        threshold: float = 0.9,
    ) -> list[MergeCandidate]:
        """
        Suggest merges across different node types.

        This finds cases where a table, column, and hierarchy
        might all represent the same underlying concept.

        Args:
            nodes: All nodes to analyze
            threshold: Higher threshold for cross-type matches

        Returns:
            List of cross-type merge candidates
        """
        candidates = []

        # Group by normalized name
        name_groups: dict[str, list[GraphNode]] = {}
        for node in nodes:
            normalized = self.matcher.normalize_name(node.name)
            if normalized not in name_groups:
                name_groups[normalized] = []
            name_groups[normalized].append(node)

        # Find groups with multiple types
        for normalized_name, group in name_groups.items():
            if len(group) < 2:
                continue

            # Check if multiple types
            types = set()
            for node in group:
                node_type = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)
                types.add(node_type)

            if len(types) > 1:
                # Calculate confidence
                similarities = []
                for i, n1 in enumerate(group):
                    for n2 in group[i + 1:]:
                        sim = self.matcher.calculate_similarity(n1.name, n2.name)
                        similarities.append(sim)

                avg_similarity = sum(similarities) / len(similarities) if similarities else 0

                if avg_similarity >= threshold:
                    canonical = self.matcher.suggest_canonical_name([n.name for n in group])
                    candidates.append(MergeCandidate(
                        nodes=group,
                        canonical_name=canonical,
                        confidence=avg_similarity,
                        merge_reason=f"Cross-type match: {', '.join(sorted(types))}",
                        metadata={
                            "node_types": list(types),
                            "cross_type": True,
                        },
                    ))

        return candidates

    def create_concept_hierarchy(
        self,
        concepts: list[ConceptNode],
    ) -> dict[str, list[str]]:
        """
        Create a hierarchy of concepts based on naming patterns.

        Finds parent-child relationships like:
        - "customer" -> "customer_address", "customer_phone"

        Args:
            concepts: Concepts to organize

        Returns:
            Dictionary mapping concept ID to child concept IDs
        """
        hierarchy: dict[str, list[str]] = {}

        # Sort by name length (shorter = more likely parent)
        sorted_concepts = sorted(concepts, key=lambda c: len(c.name))

        for concept in sorted_concepts:
            hierarchy[concept.id] = []

        # Find parent-child relationships
        for i, potential_parent in enumerate(sorted_concepts):
            parent_normalized = self.matcher.normalize_name(potential_parent.name)

            for potential_child in sorted_concepts[i + 1:]:
                child_normalized = self.matcher.normalize_name(potential_child.name)

                # Check if child name starts with parent name
                if child_normalized.startswith(parent_normalized + " "):
                    hierarchy[potential_parent.id].append(potential_child.id)

        return hierarchy

    def get_merge_summary(self) -> dict[str, Any]:
        """
        Get summary of all merges.

        Returns:
            Summary dictionary
        """
        total_nodes_merged = len(self._node_to_concept)
        total_concepts = len(self._concepts)

        # Count by type
        type_counts: dict[str, int] = {}
        for concept in self._concepts.values():
            type_counts[concept.concept_type] = type_counts.get(concept.concept_type, 0) + 1

        # Calculate compression ratio
        if total_nodes_merged > 0:
            compression_ratio = total_concepts / total_nodes_merged
        else:
            compression_ratio = 1.0

        return {
            "total_concepts": total_concepts,
            "total_nodes_merged": total_nodes_merged,
            "compression_ratio": compression_ratio,
            "reduction_percent": (1 - compression_ratio) * 100,
            "concept_types": type_counts,
            "avg_members_per_concept": total_nodes_merged / max(total_concepts, 1),
        }

    def _collect_tags(self, nodes: list[GraphNode]) -> list[str]:
        """Collect and deduplicate tags from nodes."""
        tags = set()
        for node in nodes:
            if node.tags:
                tags.update(node.tags)
        return sorted(tags)

    def export_concepts(self) -> list[dict[str, Any]]:
        """
        Export concepts as dictionaries.

        Returns:
            List of concept dictionaries
        """
        return [
            concept.model_dump()
            for concept in self._concepts.values()
        ]

    def clear(self) -> None:
        """Clear all concepts and mappings."""
        self._concepts.clear()
        self._node_to_concept.clear()
