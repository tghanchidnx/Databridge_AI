"""
Entity Matcher for fuzzy matching and deduplication of schema elements.

This module provides fuzzy string matching using RapidFuzz for finding
similar entities across databases, schemas, and tables.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from rapidfuzz import fuzz, process

from databridge_discovery.graph.node_types import (
    ColumnNode,
    GraphNode,
    HierarchyNode,
    TableNode,
)


@dataclass
class MatchResult:
    """Result from entity matching."""

    source_id: str
    source_name: str
    target_id: str
    target_name: str
    score: float
    match_type: str  # "exact", "fuzzy", "semantic", "structural"
    confidence: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchCandidate:
    """Candidate for matching."""

    id: str
    name: str
    normalized_name: str
    node: GraphNode | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class EntityMatcher:
    """
    Entity matcher for finding similar schema elements.

    Uses fuzzy string matching (RapidFuzz), pattern normalization,
    and optional semantic similarity for comprehensive matching.

    Example:
        matcher = EntityMatcher()

        # Find matches between two sets of columns
        matches = matcher.match_columns(source_columns, target_columns)

        # Find duplicates within a single set
        duplicates = matcher.find_duplicates(nodes, threshold=0.85)

        # Match by name patterns
        matches = matcher.match_by_pattern(nodes, pattern="customer_*")
    """

    # Common prefixes/suffixes to normalize
    COMMON_PREFIXES = [
        "tbl_", "t_", "dim_", "fact_", "stg_", "raw_", "src_",
        "vw_", "v_", "view_", "mv_", "mat_",
        "fk_", "pk_", "idx_", "ix_",
        "col_", "c_", "fld_", "f_",
    ]

    COMMON_SUFFIXES = [
        "_id", "_key", "_code", "_num", "_no", "_number",
        "_date", "_dt", "_ts", "_timestamp",
        "_name", "_nm", "_desc", "_description",
        "_amt", "_amount", "_qty", "_quantity",
        "_flg", "_flag", "_ind", "_indicator",
        "_tbl", "_table", "_view", "_vw",
        "_dim", "_fact", "_stg",
    ]

    # Abbreviation mappings
    ABBREVIATIONS = {
        "acct": "account",
        "addr": "address",
        "amt": "amount",
        "avg": "average",
        "bal": "balance",
        "calc": "calculated",
        "cat": "category",
        "cd": "code",
        "cnt": "count",
        "cust": "customer",
        "dept": "department",
        "desc": "description",
        "dt": "date",
        "emp": "employee",
        "flg": "flag",
        "id": "identifier",
        "ind": "indicator",
        "inv": "invoice",
        "loc": "location",
        "mgr": "manager",
        "nm": "name",
        "no": "number",
        "num": "number",
        "org": "organization",
        "pct": "percent",
        "prod": "product",
        "qty": "quantity",
        "ref": "reference",
        "seq": "sequence",
        "src": "source",
        "stat": "status",
        "tbl": "table",
        "ts": "timestamp",
        "typ": "type",
        "val": "value",
        "vw": "view",
    }

    def __init__(
        self,
        similarity_threshold: float = 0.8,
        use_semantic: bool = False,
        embedder: Any | None = None,
    ):
        """
        Initialize entity matcher.

        Args:
            similarity_threshold: Default threshold for fuzzy matching
            use_semantic: Whether to use semantic similarity
            embedder: SchemaEmbedder instance for semantic matching
        """
        self.similarity_threshold = similarity_threshold
        self.use_semantic = use_semantic
        self.embedder = embedder

    def normalize_name(self, name: str) -> str:
        """
        Normalize a name for matching.

        Removes common prefixes/suffixes, expands abbreviations,
        and normalizes casing and separators.

        Args:
            name: Original name

        Returns:
            Normalized name
        """
        if not name:
            return ""

        # Lowercase
        normalized = name.lower().strip()

        # Remove common prefixes
        for prefix in self.COMMON_PREFIXES:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
                break

        # Remove common suffixes
        for suffix in self.COMMON_SUFFIXES:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
                break

        # Replace separators with spaces
        normalized = re.sub(r'[_\-\s]+', ' ', normalized)

        # Expand abbreviations
        words = normalized.split()
        expanded_words = []
        for word in words:
            if word in self.ABBREVIATIONS:
                expanded_words.append(self.ABBREVIATIONS[word])
            else:
                expanded_words.append(word)

        normalized = ' '.join(expanded_words)

        # Remove extra spaces
        normalized = ' '.join(normalized.split())

        return normalized

    def calculate_similarity(
        self,
        name1: str,
        name2: str,
        use_normalized: bool = True,
    ) -> float:
        """
        Calculate similarity between two names.

        Args:
            name1: First name
            name2: Second name
            use_normalized: Whether to normalize names first

        Returns:
            Similarity score (0-1)
        """
        if use_normalized:
            name1 = self.normalize_name(name1)
            name2 = self.normalize_name(name2)

        if not name1 or not name2:
            return 0.0

        # Exact match
        if name1 == name2:
            return 1.0

        # Use multiple fuzzy metrics and combine
        ratio = fuzz.ratio(name1, name2) / 100.0
        partial_ratio = fuzz.partial_ratio(name1, name2) / 100.0
        token_sort = fuzz.token_sort_ratio(name1, name2) / 100.0
        token_set = fuzz.token_set_ratio(name1, name2) / 100.0

        # Weighted combination
        combined = (
            ratio * 0.3 +
            partial_ratio * 0.2 +
            token_sort * 0.25 +
            token_set * 0.25
        )

        return combined

    def match_entities(
        self,
        sources: list[GraphNode],
        targets: list[GraphNode],
        threshold: float | None = None,
    ) -> list[MatchResult]:
        """
        Match entities between source and target lists.

        Args:
            sources: Source nodes to match from
            targets: Target nodes to match to
            threshold: Similarity threshold (uses default if None)

        Returns:
            List of MatchResult for matches above threshold
        """
        threshold = threshold or self.similarity_threshold
        results = []

        # Build candidate lists
        source_candidates = [
            MatchCandidate(
                id=n.id,
                name=n.name,
                normalized_name=self.normalize_name(n.name),
                node=n,
            )
            for n in sources
        ]

        target_candidates = [
            MatchCandidate(
                id=n.id,
                name=n.name,
                normalized_name=self.normalize_name(n.name),
                node=n,
            )
            for n in targets
        ]

        # Match each source against all targets
        for source in source_candidates:
            target_names = [t.normalized_name for t in target_candidates]

            if not target_names:
                continue

            # Use RapidFuzz process.extract for efficient matching
            matches = process.extract(
                source.normalized_name,
                target_names,
                scorer=fuzz.token_set_ratio,
                limit=5,
            )

            for match_name, score, idx in matches:
                if score >= threshold * 100:
                    target = target_candidates[idx]

                    # Determine match type
                    if source.normalized_name == target.normalized_name:
                        match_type = "exact"
                        confidence = 1.0
                    else:
                        match_type = "fuzzy"
                        confidence = score / 100.0

                    results.append(MatchResult(
                        source_id=source.id,
                        source_name=source.name,
                        target_id=target.id,
                        target_name=target.name,
                        score=score / 100.0,
                        match_type=match_type,
                        confidence=confidence,
                        metadata={
                            "normalized_source": source.normalized_name,
                            "normalized_target": target.normalized_name,
                        },
                    ))

        # Sort by score descending
        results.sort(key=lambda x: x.score, reverse=True)
        return results

    def match_columns(
        self,
        source_columns: list[ColumnNode],
        target_columns: list[ColumnNode],
        threshold: float | None = None,
        match_types: bool = True,
    ) -> list[MatchResult]:
        """
        Match columns with optional data type consideration.

        Args:
            source_columns: Source columns
            target_columns: Target columns
            threshold: Similarity threshold
            match_types: Whether to consider data types in matching

        Returns:
            List of MatchResult
        """
        threshold = threshold or self.similarity_threshold
        results = []

        for source in source_columns:
            source_normalized = self.normalize_name(source.column_name)

            for target in target_columns:
                target_normalized = self.normalize_name(target.column_name)

                # Calculate name similarity
                name_score = self.calculate_similarity(
                    source.column_name,
                    target.column_name,
                    use_normalized=True,
                )

                # Adjust for data type match
                if match_types and source.data_type != "unknown" and target.data_type != "unknown":
                    if self._types_compatible(source.data_type, target.data_type):
                        type_boost = 0.1
                    else:
                        type_boost = -0.1
                    name_score = min(1.0, max(0.0, name_score + type_boost))

                if name_score >= threshold:
                    results.append(MatchResult(
                        source_id=source.id,
                        source_name=source.column_name,
                        target_id=target.id,
                        target_name=target.column_name,
                        score=name_score,
                        match_type="exact" if name_score == 1.0 else "fuzzy",
                        confidence=name_score,
                        metadata={
                            "source_type": source.data_type,
                            "target_type": target.data_type,
                            "types_compatible": self._types_compatible(source.data_type, target.data_type),
                        },
                    ))

        results.sort(key=lambda x: x.score, reverse=True)
        return results

    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible."""
        type1 = type1.lower()
        type2 = type2.lower()

        if type1 == type2:
            return True

        # Type families
        numeric_types = {"int", "integer", "bigint", "smallint", "tinyint", "decimal", "numeric", "float", "double", "real", "number"}
        string_types = {"varchar", "char", "text", "string", "nvarchar", "nchar", "clob"}
        date_types = {"date", "datetime", "timestamp", "time", "datetime2"}
        bool_types = {"boolean", "bool", "bit"}

        def get_family(t: str) -> str | None:
            t_base = t.split("(")[0].strip()
            if t_base in numeric_types:
                return "numeric"
            if t_base in string_types:
                return "string"
            if t_base in date_types:
                return "date"
            if t_base in bool_types:
                return "bool"
            return None

        return get_family(type1) == get_family(type2)

    def find_duplicates(
        self,
        nodes: list[GraphNode],
        threshold: float | None = None,
    ) -> list[tuple[GraphNode, GraphNode, float]]:
        """
        Find duplicate nodes within a list.

        Args:
            nodes: List of nodes to check
            threshold: Similarity threshold

        Returns:
            List of (node1, node2, similarity) tuples
        """
        threshold = threshold or self.similarity_threshold
        duplicates = []
        seen_pairs = set()

        for i, node1 in enumerate(nodes):
            name1_normalized = self.normalize_name(node1.name)

            for node2 in nodes[i + 1:]:
                pair_key = tuple(sorted([node1.id, node2.id]))
                if pair_key in seen_pairs:
                    continue

                name2_normalized = self.normalize_name(node2.name)

                similarity = self.calculate_similarity(
                    name1_normalized,
                    name2_normalized,
                    use_normalized=False,  # Already normalized
                )

                if similarity >= threshold:
                    duplicates.append((node1, node2, similarity))
                    seen_pairs.add(pair_key)

        duplicates.sort(key=lambda x: x[2], reverse=True)
        return duplicates

    def match_by_pattern(
        self,
        nodes: list[GraphNode],
        pattern: str,
    ) -> list[tuple[GraphNode, float]]:
        """
        Match nodes against a glob-like pattern.

        Args:
            nodes: Nodes to match
            pattern: Pattern with * wildcards (e.g., "customer_*")

        Returns:
            List of (node, score) tuples
        """
        # Convert glob pattern to regex
        regex_pattern = pattern.replace("*", ".*").replace("?", ".")
        regex_pattern = f"^{regex_pattern}$"
        regex = re.compile(regex_pattern, re.IGNORECASE)

        matches = []

        for node in nodes:
            if regex.match(node.name):
                matches.append((node, 1.0))
            else:
                # Try fuzzy match against pattern base
                pattern_base = pattern.replace("*", "").replace("?", "").strip("_- ")
                if pattern_base:
                    similarity = self.calculate_similarity(node.name, pattern_base)
                    if similarity >= 0.5:
                        matches.append((node, similarity))

        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    def suggest_canonical_name(self, names: list[str]) -> str:
        """
        Suggest a canonical name from a list of similar names.

        Picks the most descriptive/common form.

        Args:
            names: List of similar names

        Returns:
            Suggested canonical name
        """
        if not names:
            return ""

        if len(names) == 1:
            return names[0]

        # Score each name
        scores = []
        for name in names:
            score = 0

            # Prefer longer names (more descriptive)
            score += len(name) * 0.1

            # Prefer names without abbreviations
            normalized = self.normalize_name(name)
            if normalized == name.lower().replace("_", " "):
                score += 5

            # Prefer snake_case
            if "_" in name and name == name.lower():
                score += 3

            # Prefer names without prefixes/suffixes
            has_prefix = any(name.lower().startswith(p) for p in self.COMMON_PREFIXES)
            has_suffix = any(name.lower().endswith(s) for s in self.COMMON_SUFFIXES)
            if not has_prefix and not has_suffix:
                score += 2

            scores.append((name, score))

        # Return highest scored name
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[0][0]

    def group_similar(
        self,
        nodes: list[GraphNode],
        threshold: float | None = None,
    ) -> list[list[GraphNode]]:
        """
        Group similar nodes together.

        Args:
            nodes: Nodes to group
            threshold: Similarity threshold

        Returns:
            List of groups (each group is a list of similar nodes)
        """
        threshold = threshold or self.similarity_threshold
        groups: list[list[GraphNode]] = []
        assigned = set()

        # Sort by name for consistent grouping
        sorted_nodes = sorted(nodes, key=lambda n: n.name.lower())

        for node in sorted_nodes:
            if node.id in assigned:
                continue

            # Start a new group
            group = [node]
            assigned.add(node.id)
            node_normalized = self.normalize_name(node.name)

            # Find similar nodes
            for other in sorted_nodes:
                if other.id in assigned:
                    continue

                other_normalized = self.normalize_name(other.name)
                similarity = self.calculate_similarity(
                    node_normalized,
                    other_normalized,
                    use_normalized=False,
                )

                if similarity >= threshold:
                    group.append(other)
                    assigned.add(other.id)

            groups.append(group)

        # Sort groups by size
        groups.sort(key=len, reverse=True)
        return groups

    def get_match_report(
        self,
        matches: list[MatchResult],
    ) -> dict[str, Any]:
        """
        Generate a summary report from match results.

        Args:
            matches: List of MatchResult

        Returns:
            Report dictionary
        """
        if not matches:
            return {
                "total_matches": 0,
                "exact_matches": 0,
                "fuzzy_matches": 0,
                "avg_score": 0.0,
                "high_confidence": 0,
                "low_confidence": 0,
            }

        exact_matches = [m for m in matches if m.match_type == "exact"]
        fuzzy_matches = [m for m in matches if m.match_type == "fuzzy"]
        high_conf = [m for m in matches if m.confidence >= 0.9]
        low_conf = [m for m in matches if m.confidence < 0.7]

        return {
            "total_matches": len(matches),
            "exact_matches": len(exact_matches),
            "fuzzy_matches": len(fuzzy_matches),
            "avg_score": sum(m.score for m in matches) / len(matches),
            "high_confidence": len(high_conf),
            "low_confidence": len(low_conf),
            "match_types": {
                m.match_type: len([x for x in matches if x.match_type == m.match_type])
                for m in matches
            },
        }
