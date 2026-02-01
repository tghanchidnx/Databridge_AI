"""
CSV Result Analyzer for hierarchy and pattern detection.

This module analyzes CSV data to detect potential hierarchies,
entity types, and data patterns suitable for hierarchy building.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from databridge_discovery.models.case_statement import EntityType


@dataclass
class ColumnProfile:
    """Profile of a single column."""

    column_name: str
    data_type: str
    distinct_count: int
    null_count: int
    sample_values: list[str]
    is_hierarchical: bool = False
    detected_entity_type: EntityType = EntityType.UNKNOWN
    hierarchy_confidence: float = 0.0
    patterns: list[str] = field(default_factory=list)
    statistics: dict[str, Any] = field(default_factory=dict)


@dataclass
class HierarchyCandidate:
    """A potential hierarchy detected in CSV data."""

    columns: list[str]
    confidence: float
    entity_type: EntityType
    level_count: int
    cardinality_chain: list[int]  # Distinct counts per level
    sample_path: list[str]  # Sample hierarchy path
    notes: list[str] = field(default_factory=list)


@dataclass
class CSVAnalysisResult:
    """Result of analyzing a CSV file."""

    row_count: int
    column_count: int
    column_profiles: dict[str, ColumnProfile]
    hierarchy_candidates: list[HierarchyCandidate]
    entity_columns: dict[EntityType, list[str]]
    relationship_pairs: list[tuple[str, str, float]]  # (parent, child, confidence)
    notes: list[str] = field(default_factory=list)


class CSVResultAnalyzer:
    """
    Analyzes CSV data for hierarchy detection.

    Examines column cardinalities, value patterns, and relationships
    to identify potential hierarchies and entity types.

    Example:
        analyzer = CSVResultAnalyzer()

        # Analyze a DataFrame
        result = analyzer.analyze(df)

        # Find hierarchy candidates
        hierarchies = result.hierarchy_candidates

        # Get column profiles
        for col, profile in result.column_profiles.items():
            print(f"{col}: {profile.distinct_count} distinct values")
    """

    # Column name patterns for entity detection
    ENTITY_COLUMN_PATTERNS = {
        EntityType.ACCOUNT: [
            "account", "acct", "gl_account", "account_code", "account_num",
            "chart_of_accounts", "coa",
        ],
        EntityType.COST_CENTER: [
            "cost_center", "cc", "costcenter", "profit_center",
        ],
        EntityType.DEPARTMENT: [
            "department", "dept", "division", "business_unit", "bu",
        ],
        EntityType.ENTITY: [
            "entity", "company", "legal_entity", "subsidiary", "org",
        ],
        EntityType.PROJECT: [
            "project", "wbs", "work_order", "job",
        ],
        EntityType.PRODUCT: [
            "product", "sku", "item", "material", "upc",
        ],
        EntityType.CUSTOMER: [
            "customer", "client", "buyer", "cust",
        ],
        EntityType.VENDOR: [
            "vendor", "supplier", "provider",
        ],
        EntityType.EMPLOYEE: [
            "employee", "emp", "worker", "staff",
        ],
        EntityType.LOCATION: [
            "location", "site", "facility", "warehouse", "plant",
        ],
        EntityType.TIME_PERIOD: [
            "period", "fiscal_period", "month", "quarter", "year",
        ],
        EntityType.CURRENCY: [
            "currency", "curr", "fx",
        ],
    }

    # Hierarchical column patterns (parent-child naming conventions)
    HIERARCHY_PATTERNS = [
        ("category", "subcategory"),
        ("group", "subgroup"),
        ("parent", "child"),
        ("level1", "level2"),
        ("l1", "l2"),
        ("major", "minor"),
        ("region", "country"),
        ("country", "state"),
        ("state", "city"),
        ("division", "department"),
        ("department", "team"),
    ]

    def __init__(
        self,
        max_sample_values: int = 10,
        hierarchy_threshold: float = 0.7,
    ):
        """
        Initialize the analyzer.

        Args:
            max_sample_values: Maximum sample values to collect per column
            hierarchy_threshold: Confidence threshold for hierarchy detection
        """
        self.max_sample_values = max_sample_values
        self.hierarchy_threshold = hierarchy_threshold

    def analyze(
        self,
        df: pd.DataFrame,
        target_columns: list[str] | None = None,
    ) -> CSVAnalysisResult:
        """
        Analyze a DataFrame for hierarchy patterns.

        Args:
            df: DataFrame to analyze
            target_columns: Optional list of columns to focus on

        Returns:
            CSVAnalysisResult
        """
        notes: list[str] = []

        columns = target_columns if target_columns else df.columns.tolist()

        # Profile each column
        column_profiles: dict[str, ColumnProfile] = {}
        for col in columns:
            if col in df.columns:
                profile = self._profile_column(df, col)
                column_profiles[col] = profile

        # Detect entity types
        entity_columns: dict[EntityType, list[str]] = defaultdict(list)
        for col, profile in column_profiles.items():
            if profile.detected_entity_type != EntityType.UNKNOWN:
                entity_columns[profile.detected_entity_type].append(col)

        # Find relationship pairs
        relationship_pairs = self._find_relationship_pairs(df, column_profiles)

        # Detect hierarchy candidates
        hierarchy_candidates = self._find_hierarchy_candidates(
            df, column_profiles, relationship_pairs
        )

        return CSVAnalysisResult(
            row_count=len(df),
            column_count=len(columns),
            column_profiles=column_profiles,
            hierarchy_candidates=hierarchy_candidates,
            entity_columns=dict(entity_columns),
            relationship_pairs=relationship_pairs,
            notes=notes,
        )

    def analyze_csv_file(
        self,
        file_path: str,
        encoding: str = "utf-8",
        **read_csv_kwargs,
    ) -> CSVAnalysisResult:
        """
        Analyze a CSV file.

        Args:
            file_path: Path to CSV file
            encoding: File encoding
            **read_csv_kwargs: Additional arguments for pd.read_csv

        Returns:
            CSVAnalysisResult
        """
        df = pd.read_csv(file_path, encoding=encoding, **read_csv_kwargs)
        return self.analyze(df)

    def _profile_column(self, df: pd.DataFrame, column: str) -> ColumnProfile:
        """Profile a single column."""
        series = df[column]

        # Basic stats
        distinct_count = series.nunique()
        null_count = series.isnull().sum()

        # Get sample values
        sample_values = (
            series.dropna()
            .astype(str)
            .unique()[:self.max_sample_values]
            .tolist()
        )

        # Detect data type
        if pd.api.types.is_numeric_dtype(series):
            data_type = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(series):
            data_type = "datetime"
        else:
            data_type = "string"

        # Detect entity type from column name
        entity_type = self._detect_entity_type_from_name(column)

        # Check if hierarchical based on cardinality
        is_hierarchical = self._is_hierarchical_column(df, column)

        # Calculate hierarchy confidence
        hierarchy_confidence = self._calculate_hierarchy_confidence(
            df, column, entity_type
        )

        # Detect patterns in values
        patterns = self._detect_value_patterns(sample_values)

        # Statistics
        statistics: dict[str, Any] = {
            "cardinality_ratio": distinct_count / max(len(df), 1),
        }

        if data_type == "numeric" and not series.isnull().all():
            statistics["min"] = series.min()
            statistics["max"] = series.max()
            statistics["mean"] = series.mean()

        return ColumnProfile(
            column_name=column,
            data_type=data_type,
            distinct_count=distinct_count,
            null_count=null_count,
            sample_values=sample_values,
            is_hierarchical=is_hierarchical,
            detected_entity_type=entity_type,
            hierarchy_confidence=hierarchy_confidence,
            patterns=patterns,
            statistics=statistics,
        )

    def _detect_entity_type_from_name(self, column: str) -> EntityType:
        """Detect entity type from column name."""
        col_lower = column.lower().replace(" ", "_")

        for entity_type, patterns in self.ENTITY_COLUMN_PATTERNS.items():
            for pattern in patterns:
                if pattern in col_lower:
                    return entity_type

        return EntityType.UNKNOWN

    def _is_hierarchical_column(self, df: pd.DataFrame, column: str) -> bool:
        """Check if a column appears to be hierarchical."""
        distinct_count = df[column].nunique()
        row_count = len(df)

        # Hierarchical columns typically have fewer distinct values than rows
        cardinality_ratio = distinct_count / max(row_count, 1)
        return cardinality_ratio < 0.5

    def _calculate_hierarchy_confidence(
        self,
        df: pd.DataFrame,
        column: str,
        entity_type: EntityType,
    ) -> float:
        """Calculate confidence that column is hierarchical."""
        confidence = 0.0

        # Entity type detected
        if entity_type != EntityType.UNKNOWN:
            confidence += 0.3

        # Good cardinality ratio
        distinct_count = df[column].nunique()
        row_count = len(df)
        cardinality_ratio = distinct_count / max(row_count, 1)

        if cardinality_ratio < 0.1:
            confidence += 0.3
        elif cardinality_ratio < 0.3:
            confidence += 0.2
        elif cardinality_ratio < 0.5:
            confidence += 0.1

        # Check for hierarchical naming patterns
        col_lower = column.lower()
        if any(p in col_lower for p in ["level", "category", "group", "type"]):
            confidence += 0.2

        return min(confidence, 1.0)

    def _detect_value_patterns(self, values: list[str]) -> list[str]:
        """Detect patterns in sample values."""
        patterns: list[str] = []

        if not values:
            return patterns

        # Check for numeric prefix
        import re
        numeric_prefix_count = sum(
            1 for v in values
            if re.match(r'^\d+', v)
        )
        if numeric_prefix_count / len(values) > 0.8:
            patterns.append("numeric_prefix")

        # Check for code-like values
        code_like_count = sum(
            1 for v in values
            if re.match(r'^[A-Z0-9_-]+$', v.upper()) and len(v) <= 20
        )
        if code_like_count / len(values) > 0.8:
            patterns.append("code_like")

        # Check for delimited values
        for delimiter in ["|", "/", "\\", "::", "->"]:
            delimited_count = sum(1 for v in values if delimiter in v)
            if delimited_count / len(values) > 0.5:
                patterns.append(f"delimited_{delimiter}")
                break

        return patterns

    def _find_relationship_pairs(
        self,
        df: pd.DataFrame,
        profiles: dict[str, ColumnProfile],
    ) -> list[tuple[str, str, float]]:
        """Find potential parent-child column pairs."""
        pairs: list[tuple[str, str, float]] = []

        columns = list(profiles.keys())

        # Check naming patterns
        for parent_pattern, child_pattern in self.HIERARCHY_PATTERNS:
            for col1 in columns:
                col1_lower = col1.lower()
                for col2 in columns:
                    if col1 == col2:
                        continue
                    col2_lower = col2.lower()

                    if parent_pattern in col1_lower and child_pattern in col2_lower:
                        pairs.append((col1, col2, 0.9))
                    elif (
                        col1_lower.replace(parent_pattern, "") ==
                        col2_lower.replace(child_pattern, "")
                    ):
                        pairs.append((col1, col2, 0.8))

        # Check cardinality relationships
        for i, col1 in enumerate(columns):
            for col2 in columns[i + 1:]:
                profile1 = profiles[col1]
                profile2 = profiles[col2]

                # Skip non-hierarchical columns
                if not profile1.is_hierarchical or not profile2.is_hierarchical:
                    continue

                # Parent should have fewer distinct values
                if profile1.distinct_count < profile2.distinct_count:
                    # Check if there's a functional dependency
                    fd_confidence = self._check_functional_dependency(
                        df, col1, col2
                    )
                    if fd_confidence > 0.7:
                        pairs.append((col1, col2, fd_confidence))
                elif profile2.distinct_count < profile1.distinct_count:
                    fd_confidence = self._check_functional_dependency(
                        df, col2, col1
                    )
                    if fd_confidence > 0.7:
                        pairs.append((col2, col1, fd_confidence))

        # Remove duplicates, keeping highest confidence
        pair_dict: dict[tuple[str, str], float] = {}
        for parent, child, conf in pairs:
            key = (parent, child)
            if key not in pair_dict or pair_dict[key] < conf:
                pair_dict[key] = conf

        return [(p, c, conf) for (p, c), conf in pair_dict.items()]

    def _check_functional_dependency(
        self,
        df: pd.DataFrame,
        parent_col: str,
        child_col: str,
    ) -> float:
        """Check if child functionally depends on parent."""
        # For each child value, check if it maps to a single parent
        grouped = df.groupby(child_col)[parent_col].nunique()
        single_parent_count = (grouped == 1).sum()
        total_children = len(grouped)

        if total_children == 0:
            return 0.0

        return single_parent_count / total_children

    def _find_hierarchy_candidates(
        self,
        df: pd.DataFrame,
        profiles: dict[str, ColumnProfile],
        relationship_pairs: list[tuple[str, str, float]],
    ) -> list[HierarchyCandidate]:
        """Find hierarchy candidates from column relationships."""
        candidates: list[HierarchyCandidate] = []

        # Build hierarchy chains from relationship pairs
        chains = self._build_hierarchy_chains(relationship_pairs)

        for chain in chains:
            if len(chain) < 2:
                continue

            # Calculate cardinality chain
            cardinalities = [
                profiles[col].distinct_count
                for col in chain
                if col in profiles
            ]

            # Check if cardinalities increase (good hierarchy indicator)
            if cardinalities != sorted(cardinalities):
                continue

            # Get entity type from first column
            entity_type = profiles[chain[0]].detected_entity_type

            # Get sample path
            sample_path = self._get_sample_path(df, chain)

            # Calculate confidence
            confidence = sum(
                profiles[col].hierarchy_confidence
                for col in chain
                if col in profiles
            ) / len(chain)

            candidates.append(HierarchyCandidate(
                columns=chain,
                confidence=confidence,
                entity_type=entity_type,
                level_count=len(chain),
                cardinality_chain=cardinalities,
                sample_path=sample_path,
            ))

        # Sort by confidence
        candidates.sort(key=lambda c: c.confidence, reverse=True)
        return candidates

    def _build_hierarchy_chains(
        self,
        pairs: list[tuple[str, str, float]],
    ) -> list[list[str]]:
        """Build hierarchy chains from parent-child pairs."""
        # Build graph
        children: dict[str, list[str]] = defaultdict(list)
        parents: dict[str, str] = {}

        for parent, child, _ in pairs:
            children[parent].append(child)
            parents[child] = parent

        # Find root nodes (no parents)
        all_nodes = set(children.keys()) | set(parents.keys())
        root_nodes = [n for n in all_nodes if n not in parents]

        # Build chains from each root
        chains: list[list[str]] = []

        def build_chain(node: str, current_chain: list[str]) -> None:
            current_chain.append(node)
            if not children[node]:
                chains.append(list(current_chain))
            else:
                for child in children[node]:
                    build_chain(child, current_chain)
            current_chain.pop()

        for root in root_nodes:
            build_chain(root, [])

        return chains

    def _get_sample_path(
        self,
        df: pd.DataFrame,
        columns: list[str],
    ) -> list[str]:
        """Get a sample path through the hierarchy."""
        if not columns or df.empty:
            return []

        # Get first non-null row
        sample_row = df[columns].dropna().iloc[0] if not df[columns].dropna().empty else None
        if sample_row is None:
            return []

        return [str(sample_row[col]) for col in columns]
