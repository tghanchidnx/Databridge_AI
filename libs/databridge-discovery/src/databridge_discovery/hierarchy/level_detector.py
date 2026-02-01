"""
Hierarchy Level Detector.

This module detects hierarchy levels from data patterns, analyzing
column values, code structures, and relationships to determine
the natural hierarchy structure.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class DetectedLevel:
    """A detected hierarchy level."""

    level_number: int
    level_name: str
    distinct_values: list[str]
    value_count: int
    pattern: str | None = None
    parent_level: int | None = None
    confidence: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LevelDetectionResult:
    """Result of level detection."""

    levels: list[DetectedLevel]
    total_levels: int
    detection_method: str
    relationships: list[tuple[int, int]]  # (parent_level, child_level)
    confidence: float
    notes: list[str] = field(default_factory=list)


class LevelDetector:
    """
    Detects hierarchy levels from data patterns.

    Analyzes column values, code patterns, and cardinality to determine
    the natural hierarchical structure in data.

    Example:
        detector = LevelDetector()

        # Detect from a DataFrame
        result = detector.detect_from_dataframe(df, column="account_code")

        # Detect from CASE statement patterns
        result = detector.detect_from_patterns(patterns)

        # Detect from code prefixes (e.g., account codes)
        result = detector.detect_from_code_prefixes(codes)
    """

    # Common level name patterns
    LEVEL_PATTERNS = {
        "geographic": [
            "Region", "Country", "State", "City", "Location",
        ],
        "organizational": [
            "Division", "Department", "Team", "Group", "Unit",
        ],
        "financial": [
            "Category", "Subcategory", "Account Group", "Account", "Detail",
        ],
        "product": [
            "Category", "Subcategory", "Product Line", "Product", "SKU",
        ],
        "time": [
            "Year", "Quarter", "Month", "Week", "Day",
        ],
    }

    def __init__(
        self,
        max_levels: int = 10,
        min_cardinality_ratio: float = 0.1,
    ):
        """
        Initialize the level detector.

        Args:
            max_levels: Maximum number of levels to detect
            min_cardinality_ratio: Minimum ratio between level cardinalities
        """
        self.max_levels = max_levels
        self.min_cardinality_ratio = min_cardinality_ratio

    def detect_from_dataframe(
        self,
        df: pd.DataFrame,
        column: str,
        delimiter: str | None = None,
    ) -> LevelDetectionResult:
        """
        Detect hierarchy levels from a DataFrame column.

        Args:
            df: DataFrame containing the data
            column: Column name to analyze
            delimiter: Optional delimiter for parsing hierarchical values

        Returns:
            LevelDetectionResult
        """
        if column not in df.columns:
            return LevelDetectionResult(
                levels=[],
                total_levels=0,
                detection_method="dataframe",
                relationships=[],
                confidence=0.0,
                notes=[f"Column '{column}' not found"],
            )

        values = df[column].dropna().astype(str).unique().tolist()

        if delimiter:
            return self._detect_from_delimited_values(values, delimiter)
        else:
            return self.detect_from_code_prefixes(values)

    def detect_from_code_prefixes(
        self,
        codes: list[str],
        max_prefix_lengths: list[int] | None = None,
    ) -> LevelDetectionResult:
        """
        Detect levels from code prefixes.

        Analyzes codes like account numbers to find natural level breaks.
        E.g., "5010100" -> ["5", "501", "5010", "5010100"]

        Args:
            codes: List of codes to analyze
            max_prefix_lengths: Specific prefix lengths to try

        Returns:
            LevelDetectionResult
        """
        notes: list[str] = []

        if not codes:
            return LevelDetectionResult(
                levels=[],
                total_levels=0,
                detection_method="code_prefix",
                relationships=[],
                confidence=0.0,
                notes=["No codes provided"],
            )

        # Clean codes
        clean_codes = [c.strip() for c in codes if c.strip()]

        # Determine if codes are numeric or alphanumeric
        all_numeric = all(c.isdigit() for c in clean_codes)

        # Find common code lengths
        code_lengths = [len(c) for c in clean_codes]
        max_length = max(code_lengths) if code_lengths else 0

        # Determine prefix lengths to test
        if max_prefix_lengths:
            prefix_lengths = max_prefix_lengths
        elif all_numeric:
            # For numeric codes, try powers of 10 or common patterns
            prefix_lengths = self._detect_numeric_breakpoints(clean_codes)
        else:
            # For alphanumeric, try incremental lengths
            prefix_lengths = list(range(1, min(max_length + 1, self.max_levels + 1)))

        # Analyze cardinality at each prefix length
        level_candidates: list[DetectedLevel] = []

        for length in sorted(prefix_lengths):
            if length > max_length:
                continue

            prefixes = set()
            for code in clean_codes:
                if len(code) >= length:
                    prefixes.add(code[:length])

            cardinality = len(prefixes)

            if cardinality > 0:
                level_candidates.append(DetectedLevel(
                    level_number=len(level_candidates) + 1,
                    level_name=f"Level {len(level_candidates) + 1}",
                    distinct_values=sorted(prefixes),
                    value_count=cardinality,
                    pattern=f"prefix_{length}",
                    metadata={"prefix_length": length},
                ))

        # Filter out redundant levels (same cardinality as previous)
        levels = self._filter_redundant_levels(level_candidates)

        # Assign parent-child relationships
        relationships = []
        for i in range(len(levels) - 1):
            relationships.append((levels[i].level_number, levels[i + 1].level_number))
            levels[i + 1].parent_level = levels[i].level_number

        # Calculate confidence
        confidence = self._calculate_level_confidence(levels, clean_codes)

        if levels:
            notes.append(f"Detected {len(levels)} levels from code prefixes")
            notes.append(f"Cardinalities: {[l.value_count for l in levels]}")

        return LevelDetectionResult(
            levels=levels,
            total_levels=len(levels),
            detection_method="code_prefix",
            relationships=relationships,
            confidence=confidence,
            notes=notes,
        )

    def detect_from_patterns(
        self,
        patterns: list[str],
        values: list[str] | None = None,
    ) -> LevelDetectionResult:
        """
        Detect levels from LIKE patterns.

        Analyzes patterns like '5%', '50%', '501%' to find level structure.

        Args:
            patterns: List of LIKE patterns (e.g., ['5%', '50%', '501%'])
            values: Optional actual values that match patterns

        Returns:
            LevelDetectionResult
        """
        notes: list[str] = []

        if not patterns:
            return LevelDetectionResult(
                levels=[],
                total_levels=0,
                detection_method="pattern",
                relationships=[],
                confidence=0.0,
                notes=["No patterns provided"],
            )

        # Extract prefixes from patterns
        prefixes: list[str] = []
        for pattern in patterns:
            # Remove wildcards to get prefix
            prefix = pattern.rstrip('%').rstrip('_')
            if prefix:
                prefixes.append(prefix)

        # Group by prefix length
        by_length: dict[int, set[str]] = defaultdict(set)
        for prefix in prefixes:
            by_length[len(prefix)].add(prefix)

        # Create levels from each length group
        levels: list[DetectedLevel] = []
        for length in sorted(by_length.keys()):
            prefix_values = sorted(by_length[length])
            levels.append(DetectedLevel(
                level_number=len(levels) + 1,
                level_name=f"Level {len(levels) + 1}",
                distinct_values=prefix_values,
                value_count=len(prefix_values),
                pattern=f"like_prefix_{length}",
                metadata={"prefix_length": length},
            ))

        # Set up relationships
        relationships = []
        for i in range(len(levels) - 1):
            relationships.append((levels[i].level_number, levels[i + 1].level_number))
            levels[i + 1].parent_level = levels[i].level_number

        confidence = 0.7 if levels else 0.0
        notes.append(f"Detected {len(levels)} levels from {len(patterns)} patterns")

        return LevelDetectionResult(
            levels=levels,
            total_levels=len(levels),
            detection_method="pattern",
            relationships=relationships,
            confidence=confidence,
            notes=notes,
        )

    def _detect_from_delimited_values(
        self,
        values: list[str],
        delimiter: str,
    ) -> LevelDetectionResult:
        """Detect levels from delimited values like 'A|B|C'."""
        notes: list[str] = []

        # Split values and find max depth
        split_values = [v.split(delimiter) for v in values]
        max_depth = max(len(parts) for parts in split_values)

        levels: list[DetectedLevel] = []

        for depth in range(max_depth):
            # Get all values at this depth
            depth_values = set()
            for parts in split_values:
                if depth < len(parts):
                    depth_values.add(parts[depth])

            levels.append(DetectedLevel(
                level_number=depth + 1,
                level_name=f"Level {depth + 1}",
                distinct_values=sorted(depth_values),
                value_count=len(depth_values),
                pattern="delimited",
                parent_level=depth if depth > 0 else None,
                metadata={"delimiter": delimiter, "depth": depth},
            ))

        relationships = [(i, i + 1) for i in range(1, len(levels))]
        confidence = 0.9 if levels else 0.0
        notes.append(f"Detected {len(levels)} levels from delimited values")

        return LevelDetectionResult(
            levels=levels,
            total_levels=len(levels),
            detection_method="delimited",
            relationships=relationships,
            confidence=confidence,
            notes=notes,
        )

    def _detect_numeric_breakpoints(self, codes: list[str]) -> list[int]:
        """Detect natural breakpoints in numeric codes."""
        if not codes:
            return []

        # Get all code lengths
        lengths = [len(c) for c in codes]
        max_len = max(lengths)

        # Try different prefix lengths and check cardinality changes
        breakpoints: list[int] = []
        prev_cardinality = 0

        for length in range(1, max_len + 1):
            prefixes = set()
            for code in codes:
                if len(code) >= length:
                    prefixes.add(code[:length])

            cardinality = len(prefixes)

            # Significant cardinality change indicates a level
            if prev_cardinality > 0:
                ratio = cardinality / prev_cardinality
                if ratio > 1.5 or (length == max_len):
                    breakpoints.append(length)
            else:
                breakpoints.append(length)

            prev_cardinality = cardinality

        return breakpoints[:self.max_levels]

    def _filter_redundant_levels(
        self,
        candidates: list[DetectedLevel],
    ) -> list[DetectedLevel]:
        """Filter out levels with same cardinality as previous level."""
        if not candidates:
            return []

        result: list[DetectedLevel] = [candidates[0]]
        result[0].level_number = 1

        for candidate in candidates[1:]:
            if candidate.value_count != result[-1].value_count:
                candidate.level_number = len(result) + 1
                result.append(candidate)

        return result

    def _calculate_level_confidence(
        self,
        levels: list[DetectedLevel],
        codes: list[str],
    ) -> float:
        """Calculate confidence in level detection."""
        if not levels:
            return 0.0

        confidence = 0.5

        # More levels with increasing cardinality = higher confidence
        cardinalities = [l.value_count for l in levels]
        if cardinalities == sorted(cardinalities):
            confidence += 0.2

        # Good ratio between levels
        if len(levels) >= 2:
            ratios = []
            for i in range(len(levels) - 1):
                if levels[i].value_count > 0:
                    ratio = levels[i + 1].value_count / levels[i].value_count
                    ratios.append(ratio)

            if all(r > 1.5 for r in ratios):
                confidence += 0.2

        # Reasonable number of levels
        if 2 <= len(levels) <= 6:
            confidence += 0.1

        return min(confidence, 1.0)

    def suggest_level_names(
        self,
        levels: list[DetectedLevel],
        entity_type: str | None = None,
    ) -> list[str]:
        """
        Suggest names for detected levels.

        Args:
            levels: Detected levels
            entity_type: Optional entity type hint

        Returns:
            List of suggested level names
        """
        if not levels:
            return []

        # Get appropriate pattern
        if entity_type and entity_type.lower() in self.LEVEL_PATTERNS:
            pattern = self.LEVEL_PATTERNS[entity_type.lower()]
        else:
            # Default to financial hierarchy names
            pattern = self.LEVEL_PATTERNS["financial"]

        # Assign names based on level count
        names: list[str] = []
        for i, level in enumerate(levels):
            if i < len(pattern):
                names.append(pattern[i])
            else:
                names.append(f"Level {i + 1}")

        return names

    def detect_parent_child_from_values(
        self,
        parent_values: list[str],
        child_values: list[str],
    ) -> dict[str, list[str]]:
        """
        Detect parent-child relationships based on value patterns.

        Args:
            parent_values: List of parent level values
            child_values: List of child level values

        Returns:
            Dict mapping parent values to their children
        """
        relationships: dict[str, list[str]] = defaultdict(list)

        for parent in parent_values:
            parent_clean = parent.lower().strip()

            for child in child_values:
                child_clean = child.lower().strip()

                # Check if child contains parent or starts with parent
                if (
                    child_clean.startswith(parent_clean)
                    or parent_clean in child_clean
                ):
                    relationships[parent].append(child)

        return dict(relationships)
