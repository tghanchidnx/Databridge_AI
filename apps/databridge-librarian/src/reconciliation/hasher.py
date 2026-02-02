"""
Hash Comparer for DataBridge AI Librarian.

Provides hash-based data comparison for reconciliation.
"""

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Set, Tuple

import pandas as pd
import numpy as np


class MatchStatus(str, Enum):
    """Status of a record match."""
    MATCH = "match"
    ORPHAN_SOURCE = "orphan_source"  # In source but not target
    ORPHAN_TARGET = "orphan_target"  # In target but not source
    CONFLICT = "conflict"  # Same key, different values


@dataclass
class RecordMatch:
    """Result of comparing a single record."""

    status: MatchStatus
    key_values: Dict[str, Any]
    source_hash: Optional[str] = None
    target_hash: Optional[str] = None
    source_values: Optional[Dict[str, Any]] = None
    target_values: Optional[Dict[str, Any]] = None
    diff_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status.value,
            "key_values": self.key_values,
            "source_hash": self.source_hash,
            "target_hash": self.target_hash,
            "diff_columns": self.diff_columns,
        }


@dataclass
class CompareResult:
    """Result of a data comparison operation."""

    success: bool
    total_source: int = 0
    total_target: int = 0
    matched: int = 0
    orphan_source: int = 0
    orphan_target: int = 0
    conflicts: int = 0
    compare_time_ms: int = 0
    key_columns: List[str] = field(default_factory=list)
    value_columns: List[str] = field(default_factory=list)
    matches: List[RecordMatch] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "summary": {
                "total_source": self.total_source,
                "total_target": self.total_target,
                "matched": self.matched,
                "orphan_source": self.orphan_source,
                "orphan_target": self.orphan_target,
                "conflicts": self.conflicts,
            },
            "match_rate": round(self.matched / self.total_source * 100, 2) if self.total_source > 0 else 0,
            "compare_time_ms": self.compare_time_ms,
            "key_columns": self.key_columns,
            "value_columns": self.value_columns,
            "errors": self.errors,
        }

    def get_orphans_source(self) -> List[RecordMatch]:
        """Get records only in source."""
        return [m for m in self.matches if m.status == MatchStatus.ORPHAN_SOURCE]

    def get_orphans_target(self) -> List[RecordMatch]:
        """Get records only in target."""
        return [m for m in self.matches if m.status == MatchStatus.ORPHAN_TARGET]

    def get_conflicts(self) -> List[RecordMatch]:
        """Get conflict records."""
        return [m for m in self.matches if m.status == MatchStatus.CONFLICT]

    def get_matches(self) -> List[RecordMatch]:
        """Get matched records."""
        return [m for m in self.matches if m.status == MatchStatus.MATCH]


class HashComparer:
    """
    Hash-based data comparer for reconciliation.

    Uses row hashing to efficiently compare datasets and identify:
    - Matching records
    - Orphan records (in one dataset but not the other)
    - Conflicts (same key, different values)
    """

    def __init__(
        self,
        hash_algorithm: str = "md5",
        case_sensitive: bool = True,
        trim_whitespace: bool = True,
        null_placeholder: str = "__NULL__",
    ):
        """
        Initialize the comparer.

        Args:
            hash_algorithm: Hash algorithm ('md5', 'sha1', 'sha256').
            case_sensitive: Whether comparisons are case-sensitive.
            trim_whitespace: Whether to trim whitespace from values.
            null_placeholder: Placeholder for null values in hashing.
        """
        self.hash_algorithm = hash_algorithm
        self.case_sensitive = case_sensitive
        self.trim_whitespace = trim_whitespace
        self.null_placeholder = null_placeholder

    def compare(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: List[str],
        value_columns: Optional[List[str]] = None,
        include_details: bool = True,
        max_details: int = 1000,
    ) -> CompareResult:
        """
        Compare two DataFrames using hash-based comparison.

        Args:
            source_df: Source DataFrame.
            target_df: Target DataFrame.
            key_columns: Columns that uniquely identify a record.
            value_columns: Columns to compare for conflicts (None = all non-key columns).
            include_details: Whether to include detailed match records.
            max_details: Maximum number of detail records to include.

        Returns:
            CompareResult with comparison results.
        """
        start_time = datetime.now()
        errors = []

        # Validate columns exist
        for col in key_columns:
            if col not in source_df.columns:
                errors.append(f"Key column '{col}' not found in source")
            if col not in target_df.columns:
                errors.append(f"Key column '{col}' not found in target")

        if errors:
            return CompareResult(success=False, errors=errors)

        # Determine value columns
        if value_columns is None:
            source_value_cols = [c for c in source_df.columns if c not in key_columns]
            target_value_cols = [c for c in target_df.columns if c not in key_columns]
            value_columns = list(set(source_value_cols) & set(target_value_cols))

        try:
            # Compute hashes for source
            source_key_hashes, source_value_hashes = self._compute_hashes(
                source_df, key_columns, value_columns
            )

            # Compute hashes for target
            target_key_hashes, target_value_hashes = self._compute_hashes(
                target_df, key_columns, value_columns
            )

            # Build key sets
            source_keys = set(source_key_hashes.values())
            target_keys = set(target_key_hashes.values())

            # Find orphans
            orphan_source_keys = source_keys - target_keys
            orphan_target_keys = target_keys - source_keys
            common_keys = source_keys & target_keys

            # Build reverse lookup: key_hash -> row index
            source_key_to_idx = {h: i for i, h in source_key_hashes.items()}
            target_key_to_idx = {h: i for i, h in target_key_hashes.items()}

            # Count results
            matched = 0
            conflicts = 0
            matches = []

            # Process common keys
            for key_hash in common_keys:
                source_idx = source_key_to_idx[key_hash]
                target_idx = target_key_to_idx[key_hash]

                source_value_hash = source_value_hashes.get(source_idx)
                target_value_hash = target_value_hashes.get(target_idx)

                key_values = self._get_key_values(source_df, source_idx, key_columns)

                if source_value_hash == target_value_hash:
                    matched += 1
                    if include_details and len(matches) < max_details:
                        matches.append(RecordMatch(
                            status=MatchStatus.MATCH,
                            key_values=key_values,
                            source_hash=source_value_hash,
                            target_hash=target_value_hash,
                        ))
                else:
                    conflicts += 1
                    if include_details and len(matches) < max_details:
                        diff_cols = self._find_diff_columns(
                            source_df.iloc[source_idx],
                            target_df.iloc[target_idx],
                            value_columns,
                        )
                        matches.append(RecordMatch(
                            status=MatchStatus.CONFLICT,
                            key_values=key_values,
                            source_hash=source_value_hash,
                            target_hash=target_value_hash,
                            source_values=self._row_to_dict(source_df.iloc[source_idx], value_columns),
                            target_values=self._row_to_dict(target_df.iloc[target_idx], value_columns),
                            diff_columns=diff_cols,
                        ))

            # Process source orphans
            if include_details:
                for key_hash in list(orphan_source_keys)[:max_details - len(matches)]:
                    source_idx = source_key_to_idx[key_hash]
                    matches.append(RecordMatch(
                        status=MatchStatus.ORPHAN_SOURCE,
                        key_values=self._get_key_values(source_df, source_idx, key_columns),
                        source_hash=source_value_hashes.get(source_idx),
                        source_values=self._row_to_dict(source_df.iloc[source_idx], value_columns),
                    ))

            # Process target orphans
            if include_details:
                for key_hash in list(orphan_target_keys)[:max_details - len(matches)]:
                    target_idx = target_key_to_idx[key_hash]
                    matches.append(RecordMatch(
                        status=MatchStatus.ORPHAN_TARGET,
                        key_values=self._get_key_values(target_df, target_idx, key_columns),
                        target_hash=target_value_hashes.get(target_idx),
                        target_values=self._row_to_dict(target_df.iloc[target_idx], value_columns),
                    ))

            compare_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return CompareResult(
                success=True,
                total_source=len(source_df),
                total_target=len(target_df),
                matched=matched,
                orphan_source=len(orphan_source_keys),
                orphan_target=len(orphan_target_keys),
                conflicts=conflicts,
                compare_time_ms=compare_time,
                key_columns=key_columns,
                value_columns=value_columns,
                matches=matches,
            )

        except Exception as e:
            return CompareResult(
                success=False,
                errors=[str(e)],
            )

    def _compute_hashes(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        value_columns: List[str],
    ) -> Tuple[Dict[int, str], Dict[int, str]]:
        """Compute key and value hashes for all rows."""
        key_hashes = {}
        value_hashes = {}

        for idx in range(len(df)):
            row = df.iloc[idx]

            # Key hash
            key_values = [self._normalize_value(row[col]) for col in key_columns]
            key_str = "|".join(key_values)
            key_hashes[idx] = self._hash_string(key_str)

            # Value hash
            value_values = [self._normalize_value(row[col]) for col in value_columns]
            value_str = "|".join(value_values)
            value_hashes[idx] = self._hash_string(value_str)

        return key_hashes, value_hashes

    def _normalize_value(self, value: Any) -> str:
        """Normalize a value for hashing."""
        if pd.isna(value):
            return self.null_placeholder

        s = str(value)

        if self.trim_whitespace:
            s = s.strip()

        if not self.case_sensitive:
            s = s.lower()

        return s

    def _hash_string(self, s: str) -> str:
        """Hash a string using the configured algorithm."""
        if self.hash_algorithm == "md5":
            return hashlib.md5(s.encode()).hexdigest()
        elif self.hash_algorithm == "sha1":
            return hashlib.sha1(s.encode()).hexdigest()
        elif self.hash_algorithm == "sha256":
            return hashlib.sha256(s.encode()).hexdigest()
        else:
            return hashlib.md5(s.encode()).hexdigest()

    def _get_key_values(
        self,
        df: pd.DataFrame,
        idx: int,
        key_columns: List[str],
    ) -> Dict[str, Any]:
        """Get key values for a row."""
        row = df.iloc[idx]
        return {col: self._safe_value(row[col]) for col in key_columns}

    def _row_to_dict(
        self,
        row: pd.Series,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Convert a row to a dictionary."""
        return {col: self._safe_value(row[col]) for col in columns if col in row.index}

    def _safe_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable type."""
        if pd.isna(value):
            return None
        if isinstance(value, (np.integer, np.floating)):
            return float(value)
        if isinstance(value, np.bool_):
            return bool(value)
        return str(value)

    def _find_diff_columns(
        self,
        source_row: pd.Series,
        target_row: pd.Series,
        columns: List[str],
    ) -> List[str]:
        """Find columns with different values."""
        diff_cols = []
        for col in columns:
            if col in source_row.index and col in target_row.index:
                source_val = self._normalize_value(source_row[col])
                target_val = self._normalize_value(target_row[col])
                if source_val != target_val:
                    diff_cols.append(col)
        return diff_cols

    def reconcile(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: List[str],
        value_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Create a reconciliation report DataFrame.

        Args:
            source_df: Source DataFrame.
            target_df: Target DataFrame.
            key_columns: Key columns.
            value_columns: Value columns to compare.

        Returns:
            DataFrame with reconciliation report.
        """
        result = self.compare(
            source_df,
            target_df,
            key_columns,
            value_columns,
            include_details=True,
            max_details=10000,
        )

        records = []
        for match in result.matches:
            record = {
                "status": match.status.value,
                **{f"key_{k}": v for k, v in match.key_values.items()},
            }
            if match.diff_columns:
                record["diff_columns"] = ",".join(match.diff_columns)
            records.append(record)

        if not records:
            return pd.DataFrame(columns=["status"] + [f"key_{k}" for k in key_columns])

        return pd.DataFrame(records)
