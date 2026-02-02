"""
Fuzzy Matcher for DataBridge AI Librarian.

Provides fuzzy string matching, deduplication, and record linkage using RapidFuzz.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple, Callable, Union

import pandas as pd
import numpy as np

try:
    from rapidfuzz import fuzz, process
    from rapidfuzz.distance import Levenshtein
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


class MatchMethod(str, Enum):
    """Fuzzy matching method."""
    RATIO = "ratio"  # Simple ratio
    PARTIAL_RATIO = "partial_ratio"  # Partial string matching
    TOKEN_SORT = "token_sort_ratio"  # Token-based with sorting
    TOKEN_SET = "token_set_ratio"  # Token-based with set operations
    WEIGHTED = "weighted_ratio"  # Weighted combination
    LEVENSHTEIN = "levenshtein"  # Edit distance


@dataclass
class MatchCandidate:
    """A candidate match between two records."""

    source_index: int
    target_index: int
    source_value: str
    target_value: str
    score: float
    method: MatchMethod
    additional_scores: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source_index": self.source_index,
            "target_index": self.target_index,
            "source_value": self.source_value,
            "target_value": self.target_value,
            "score": round(self.score, 2),
            "method": self.method.value,
            "additional_scores": {k: round(v, 2) for k, v in self.additional_scores.items()},
        }


@dataclass
class MatchResult:
    """Result of a fuzzy matching operation."""

    success: bool
    total_source: int = 0
    total_target: int = 0
    matched: int = 0
    unmatched_source: int = 0
    unmatched_target: int = 0
    match_time_ms: int = 0
    threshold: float = 80.0
    method: str = ""
    matches: List[MatchCandidate] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "summary": {
                "total_source": self.total_source,
                "total_target": self.total_target,
                "matched": self.matched,
                "unmatched_source": self.unmatched_source,
                "unmatched_target": self.unmatched_target,
            },
            "match_rate": round(self.matched / self.total_source * 100, 2) if self.total_source > 0 else 0,
            "match_time_ms": self.match_time_ms,
            "threshold": self.threshold,
            "method": self.method,
            "errors": self.errors,
        }

    def get_matched_pairs(self) -> List[Tuple[int, int]]:
        """Get list of matched index pairs (source_idx, target_idx)."""
        return [(m.source_index, m.target_index) for m in self.matches]

    def get_unmatched_source_indices(self, source_size: int) -> List[int]:
        """Get indices of unmatched source records."""
        matched_source = {m.source_index for m in self.matches}
        return [i for i in range(source_size) if i not in matched_source]

    def get_unmatched_target_indices(self, target_size: int) -> List[int]:
        """Get indices of unmatched target records."""
        matched_target = {m.target_index for m in self.matches}
        return [i for i in range(target_size) if i not in matched_target]


@dataclass
class DedupeResult:
    """Result of a deduplication operation."""

    success: bool
    total_records: int = 0
    unique_records: int = 0
    duplicate_groups: int = 0
    duplicate_records: int = 0
    dedupe_time_ms: int = 0
    threshold: float = 80.0
    groups: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "summary": {
                "total_records": self.total_records,
                "unique_records": self.unique_records,
                "duplicate_groups": self.duplicate_groups,
                "duplicate_records": self.duplicate_records,
            },
            "dedupe_rate": round((self.total_records - self.unique_records) / self.total_records * 100, 2) if self.total_records > 0 else 0,
            "dedupe_time_ms": self.dedupe_time_ms,
            "threshold": self.threshold,
            "groups": self.groups[:100],  # Limit to first 100 groups
            "errors": self.errors,
        }


class FuzzyMatcher:
    """
    Fuzzy string matcher for reconciliation.

    Uses RapidFuzz for high-performance fuzzy matching with multiple algorithms:
    - Simple ratio (character-level similarity)
    - Partial ratio (substring matching)
    - Token sort ratio (word-level with sorting)
    - Token set ratio (word-level with set operations)
    - Weighted combination of multiple methods
    - Levenshtein edit distance
    """

    def __init__(
        self,
        threshold: float = 80.0,
        method: MatchMethod = MatchMethod.WEIGHTED,
        case_sensitive: bool = False,
        trim_whitespace: bool = True,
        remove_punctuation: bool = False,
        limit: int = 5,
    ):
        """
        Initialize the fuzzy matcher.

        Args:
            threshold: Minimum score (0-100) to consider a match.
            method: Matching method to use.
            case_sensitive: Whether matching is case-sensitive.
            trim_whitespace: Whether to trim whitespace from values.
            remove_punctuation: Whether to remove punctuation.
            limit: Maximum number of candidates to return per source.
        """
        if not RAPIDFUZZ_AVAILABLE:
            raise ImportError(
                "RapidFuzz is required for fuzzy matching. "
                "Install it with: pip install rapidfuzz"
            )

        self.threshold = threshold
        self.method = method
        self.case_sensitive = case_sensitive
        self.trim_whitespace = trim_whitespace
        self.remove_punctuation = remove_punctuation
        self.limit = limit

    def match(
        self,
        source: Union[List[str], pd.Series],
        target: Union[List[str], pd.Series],
        source_indices: Optional[List[int]] = None,
        target_indices: Optional[List[int]] = None,
    ) -> MatchResult:
        """
        Match source values to target values using fuzzy matching.

        Args:
            source: List or Series of source values to match.
            target: List or Series of target values to match against.
            source_indices: Optional custom indices for source records.
            target_indices: Optional custom indices for target records.

        Returns:
            MatchResult with matching information.
        """
        start_time = datetime.now()

        # Convert to lists
        source_list = list(source) if isinstance(source, pd.Series) else list(source)
        target_list = list(target) if isinstance(target, pd.Series) else list(target)

        # Set up indices
        if source_indices is None:
            source_indices = list(range(len(source_list)))
        if target_indices is None:
            target_indices = list(range(len(target_list)))

        try:
            # Normalize values
            source_normalized = [self._normalize(str(v)) for v in source_list]
            target_normalized = [self._normalize(str(v)) for v in target_list]

            # Get scorer function
            scorer = self._get_scorer()

            matches = []
            matched_target_indices = set()

            # Find best match for each source
            for i, (src_idx, src_val, src_norm) in enumerate(zip(source_indices, source_list, source_normalized)):
                if not src_norm:
                    continue

                # Find candidates
                candidates = process.extract(
                    src_norm,
                    target_normalized,
                    scorer=scorer,
                    limit=self.limit,
                )

                # Filter by threshold and find best unmatched
                for tgt_norm, score, tgt_list_idx in candidates:
                    if score >= self.threshold:
                        tgt_idx = target_indices[tgt_list_idx]

                        # Skip if already matched (one-to-one matching)
                        if tgt_idx in matched_target_indices:
                            continue

                        # Calculate additional scores for context
                        additional_scores = self._get_additional_scores(src_norm, tgt_norm)

                        match = MatchCandidate(
                            source_index=src_idx,
                            target_index=tgt_idx,
                            source_value=str(src_val),
                            target_value=str(target_list[tgt_list_idx]),
                            score=score,
                            method=self.method,
                            additional_scores=additional_scores,
                        )
                        matches.append(match)
                        matched_target_indices.add(tgt_idx)
                        break

            match_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return MatchResult(
                success=True,
                total_source=len(source_list),
                total_target=len(target_list),
                matched=len(matches),
                unmatched_source=len(source_list) - len(matches),
                unmatched_target=len(target_list) - len(matched_target_indices),
                match_time_ms=match_time,
                threshold=self.threshold,
                method=self.method.value,
                matches=matches,
            )

        except Exception as e:
            return MatchResult(
                success=False,
                total_source=len(source_list),
                total_target=len(target_list),
                errors=[str(e)],
            )

    def match_dataframes(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        source_column: str,
        target_column: str,
        include_all_source: bool = True,
    ) -> pd.DataFrame:
        """
        Match records between two DataFrames using fuzzy matching.

        Args:
            source_df: Source DataFrame.
            target_df: Target DataFrame.
            source_column: Column name in source to match on.
            target_column: Column name in target to match on.
            include_all_source: Whether to include unmatched source records.

        Returns:
            DataFrame with matched records.
        """
        result = self.match(
            source_df[source_column],
            target_df[target_column],
            source_indices=list(source_df.index),
            target_indices=list(target_df.index),
        )

        if not result.success:
            raise ValueError(f"Matching failed: {result.errors}")

        # Build result DataFrame
        records = []
        matched_source = set()

        for match in result.matches:
            source_row = source_df.loc[match.source_index].to_dict()
            target_row = target_df.loc[match.target_index].to_dict()

            record = {
                "match_score": match.score,
                "match_method": match.method.value,
            }

            # Add source columns with prefix
            for col, val in source_row.items():
                record[f"source_{col}"] = val

            # Add target columns with prefix
            for col, val in target_row.items():
                record[f"target_{col}"] = val

            records.append(record)
            matched_source.add(match.source_index)

        # Add unmatched source records if requested
        if include_all_source:
            for idx in source_df.index:
                if idx not in matched_source:
                    source_row = source_df.loc[idx].to_dict()
                    record = {
                        "match_score": None,
                        "match_method": None,
                    }
                    for col, val in source_row.items():
                        record[f"source_{col}"] = val
                    records.append(record)

        return pd.DataFrame(records)

    def deduplicate(
        self,
        values: Union[List[str], pd.Series],
        indices: Optional[List[int]] = None,
    ) -> DedupeResult:
        """
        Find duplicate records using fuzzy matching.

        Args:
            values: List or Series of values to deduplicate.
            indices: Optional custom indices for records.

        Returns:
            DedupeResult with duplicate groups.
        """
        start_time = datetime.now()

        # Convert to list
        value_list = list(values) if isinstance(values, pd.Series) else list(values)

        if indices is None:
            indices = list(range(len(value_list)))

        try:
            # Normalize values
            normalized = [self._normalize(str(v)) for v in value_list]

            # Get scorer function
            scorer = self._get_scorer()

            # Track which records have been assigned to groups
            assigned = set()
            groups = []

            for i, (idx, val, norm) in enumerate(zip(indices, value_list, normalized)):
                if idx in assigned or not norm:
                    continue

                # Find similar records
                similar_indices = [idx]
                similar_values = [str(val)]
                similar_scores = [100.0]

                for j, (other_idx, other_val, other_norm) in enumerate(zip(indices, value_list, normalized)):
                    if i >= j or other_idx in assigned or not other_norm:
                        continue

                    score = scorer(norm, other_norm)
                    if score >= self.threshold:
                        similar_indices.append(other_idx)
                        similar_values.append(str(other_val))
                        similar_scores.append(score)

                # Only create group if duplicates found
                if len(similar_indices) > 1:
                    groups.append({
                        "group_id": len(groups) + 1,
                        "master_index": similar_indices[0],
                        "master_value": similar_values[0],
                        "duplicate_count": len(similar_indices) - 1,
                        "records": [
                            {"index": idx, "value": val, "score": score}
                            for idx, val, score in zip(similar_indices, similar_values, similar_scores)
                        ],
                    })
                    assigned.update(similar_indices)

            # Count unique records (not in any duplicate group)
            duplicate_records = len(assigned)
            unique_records = len(value_list) - duplicate_records + len(groups)  # One representative per group

            dedupe_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return DedupeResult(
                success=True,
                total_records=len(value_list),
                unique_records=unique_records,
                duplicate_groups=len(groups),
                duplicate_records=duplicate_records - len(groups),  # Exclude group masters
                dedupe_time_ms=dedupe_time,
                threshold=self.threshold,
                groups=groups,
            )

        except Exception as e:
            return DedupeResult(
                success=False,
                total_records=len(value_list),
                errors=[str(e)],
            )

    def deduplicate_dataframe(
        self,
        df: pd.DataFrame,
        column: str,
        keep: str = "first",
    ) -> Tuple[pd.DataFrame, DedupeResult]:
        """
        Deduplicate a DataFrame using fuzzy matching.

        Args:
            df: DataFrame to deduplicate.
            column: Column name to check for duplicates.
            keep: Which record to keep ('first', 'last', or 'best').

        Returns:
            Tuple of (deduplicated DataFrame, DedupeResult).
        """
        result = self.deduplicate(
            df[column],
            indices=list(df.index),
        )

        if not result.success:
            raise ValueError(f"Deduplication failed: {result.errors}")

        # Identify records to remove
        indices_to_remove = set()
        for group in result.groups:
            records = group["records"]
            if keep == "first":
                # Keep first, remove rest
                indices_to_remove.update(r["index"] for r in records[1:])
            elif keep == "last":
                # Keep last, remove rest
                indices_to_remove.update(r["index"] for r in records[:-1])
            elif keep == "best":
                # Keep highest score (usually the master), remove rest
                best_idx = max(records, key=lambda r: r["score"])["index"]
                indices_to_remove.update(r["index"] for r in records if r["index"] != best_idx)

        # Create deduplicated DataFrame
        deduped_df = df.drop(index=list(indices_to_remove))

        return deduped_df, result

    def find_best_match(
        self,
        value: str,
        candidates: Union[List[str], pd.Series],
    ) -> Optional[MatchCandidate]:
        """
        Find the best matching candidate for a single value.

        Args:
            value: Value to match.
            candidates: List of candidate values.

        Returns:
            Best MatchCandidate or None if no match above threshold.
        """
        candidate_list = list(candidates) if isinstance(candidates, pd.Series) else list(candidates)

        value_normalized = self._normalize(str(value))
        candidates_normalized = [self._normalize(str(c)) for c in candidate_list]

        scorer = self._get_scorer()

        result = process.extractOne(
            value_normalized,
            candidates_normalized,
            scorer=scorer,
            score_cutoff=self.threshold,
        )

        if result is None:
            return None

        matched_norm, score, idx = result

        additional_scores = self._get_additional_scores(value_normalized, matched_norm)

        return MatchCandidate(
            source_index=0,
            target_index=idx,
            source_value=str(value),
            target_value=str(candidate_list[idx]),
            score=score,
            method=self.method,
            additional_scores=additional_scores,
        )

    def score(self, value1: str, value2: str) -> float:
        """
        Calculate the fuzzy match score between two values.

        Args:
            value1: First value.
            value2: Second value.

        Returns:
            Match score (0-100).
        """
        norm1 = self._normalize(str(value1))
        norm2 = self._normalize(str(value2))

        scorer = self._get_scorer()
        return scorer(norm1, norm2)

    def _normalize(self, value: str) -> str:
        """Normalize a value for matching."""
        if not value or value.lower() in ("none", "nan", "null"):
            return ""

        s = str(value)

        if self.trim_whitespace:
            s = " ".join(s.split())

        if not self.case_sensitive:
            s = s.lower()

        if self.remove_punctuation:
            import string
            s = s.translate(str.maketrans("", "", string.punctuation))

        return s

    def _get_scorer(self) -> Callable:
        """Get the scoring function for the configured method."""
        if self.method == MatchMethod.RATIO:
            return fuzz.ratio
        elif self.method == MatchMethod.PARTIAL_RATIO:
            return fuzz.partial_ratio
        elif self.method == MatchMethod.TOKEN_SORT:
            return fuzz.token_sort_ratio
        elif self.method == MatchMethod.TOKEN_SET:
            return fuzz.token_set_ratio
        elif self.method == MatchMethod.WEIGHTED:
            return fuzz.WRatio
        elif self.method == MatchMethod.LEVENSHTEIN:
            return lambda s1, s2: 100 - Levenshtein.normalized_distance(s1, s2) * 100
        else:
            return fuzz.WRatio

    def _get_additional_scores(self, value1: str, value2: str) -> Dict[str, float]:
        """Calculate additional scores for context."""
        return {
            "ratio": fuzz.ratio(value1, value2),
            "partial_ratio": fuzz.partial_ratio(value1, value2),
            "token_sort_ratio": fuzz.token_sort_ratio(value1, value2),
            "token_set_ratio": fuzz.token_set_ratio(value1, value2),
        }


def match_columns(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    threshold: float = 80.0,
) -> Dict[str, str]:
    """
    Match column names between two DataFrames using fuzzy matching.

    Args:
        source_df: Source DataFrame.
        target_df: Target DataFrame.
        threshold: Minimum score to consider a match.

    Returns:
        Dictionary mapping source column names to target column names.
    """
    matcher = FuzzyMatcher(threshold=threshold, method=MatchMethod.TOKEN_SET)

    result = matcher.match(
        list(source_df.columns),
        list(target_df.columns),
    )

    return {
        m.source_value: m.target_value
        for m in result.matches
    }
