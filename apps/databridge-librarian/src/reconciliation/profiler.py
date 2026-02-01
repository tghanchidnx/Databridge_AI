"""
Data Profiler for DataBridge AI V3.

Provides comprehensive data profiling and quality analysis.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

import pandas as pd
import numpy as np


@dataclass
class ColumnProfile:
    """Profile for a single column."""

    name: str
    dtype: str
    total_count: int
    null_count: int
    null_percentage: float
    unique_count: int
    cardinality: float  # unique_count / total_count

    # Numeric stats (for numeric columns)
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_value: Optional[float] = None

    # String stats (for string columns)
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None

    # Pattern detection
    patterns: List[str] = field(default_factory=list)
    top_values: List[Dict[str, Any]] = field(default_factory=list)

    # Quality indicators
    has_duplicates: bool = False
    is_unique: bool = False
    is_constant: bool = False
    data_quality_score: float = 100.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "dtype": self.dtype,
            "total_count": self.total_count,
            "null_count": self.null_count,
            "null_percentage": round(self.null_percentage, 2),
            "unique_count": self.unique_count,
            "cardinality": round(self.cardinality, 4),
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": round(self.mean_value, 4) if self.mean_value else None,
            "median_value": round(self.median_value, 4) if self.median_value else None,
            "std_value": round(self.std_value, 4) if self.std_value else None,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "avg_length": round(self.avg_length, 2) if self.avg_length else None,
            "patterns": self.patterns,
            "top_values": self.top_values[:5],
            "is_unique": self.is_unique,
            "is_constant": self.is_constant,
            "data_quality_score": round(self.data_quality_score, 2),
        }


@dataclass
class ProfileResult:
    """Result of a data profiling operation."""

    success: bool
    row_count: int = 0
    column_count: int = 0
    columns: List[ColumnProfile] = field(default_factory=list)
    duplicate_rows: int = 0
    memory_usage_bytes: int = 0
    profile_time_ms: int = 0
    overall_quality_score: float = 100.0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [c.to_dict() for c in self.columns],
            "duplicate_rows": self.duplicate_rows,
            "memory_usage_mb": round(self.memory_usage_bytes / (1024 * 1024), 2),
            "profile_time_ms": self.profile_time_ms,
            "overall_quality_score": round(self.overall_quality_score, 2),
            "errors": self.errors,
        }

    def get_column(self, name: str) -> Optional[ColumnProfile]:
        """Get profile for a specific column."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


class DataProfiler:
    """
    Comprehensive data profiler for quality analysis.

    Provides:
    - Column-level statistics
    - Pattern detection
    - Quality scoring
    - Data type analysis
    """

    # Common patterns to detect
    PATTERNS = {
        "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        "phone": r"^[\d\-\(\)\s\+\.]{7,20}$",
        "date_iso": r"^\d{4}-\d{2}-\d{2}$",
        "datetime_iso": r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}",
        "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        "url": r"^https?://[^\s]+$",
        "ip_address": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
        "us_zip": r"^\d{5}(-\d{4})?$",
        "currency": r"^[\$\€\£]?\s*[\d,]+\.?\d*$",
        "percentage": r"^[\d.]+\s*%$",
    }

    def __init__(
        self,
        top_n: int = 10,
        sample_size: int = 10000,
        detect_patterns: bool = True,
    ):
        """
        Initialize the profiler.

        Args:
            top_n: Number of top values to include.
            sample_size: Maximum rows to sample for pattern detection.
            detect_patterns: Whether to detect common patterns.
        """
        self.top_n = top_n
        self.sample_size = sample_size
        self.detect_patterns = detect_patterns

    def profile(self, df: pd.DataFrame) -> ProfileResult:
        """
        Profile a DataFrame.

        Args:
            df: DataFrame to profile.

        Returns:
            ProfileResult with comprehensive statistics.
        """
        start_time = datetime.now()
        errors = []

        try:
            row_count = len(df)
            column_count = len(df.columns)

            # Profile each column
            column_profiles = []
            quality_scores = []

            for col in df.columns:
                try:
                    profile = self._profile_column(df[col], col)
                    column_profiles.append(profile)
                    quality_scores.append(profile.data_quality_score)
                except Exception as e:
                    errors.append(f"Error profiling column {col}: {str(e)}")

            # Calculate duplicate rows
            duplicate_rows = df.duplicated().sum()

            # Memory usage
            memory_usage = df.memory_usage(deep=True).sum()

            # Overall quality score
            overall_quality = np.mean(quality_scores) if quality_scores else 0.0

            # Adjust for duplicate rows
            if row_count > 0:
                duplicate_penalty = (duplicate_rows / row_count) * 10
                overall_quality = max(0, overall_quality - duplicate_penalty)

            profile_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return ProfileResult(
                success=True,
                row_count=row_count,
                column_count=column_count,
                columns=column_profiles,
                duplicate_rows=int(duplicate_rows),
                memory_usage_bytes=int(memory_usage),
                profile_time_ms=profile_time,
                overall_quality_score=overall_quality,
                errors=errors,
            )

        except Exception as e:
            return ProfileResult(
                success=False,
                errors=[str(e)],
            )

    def _profile_column(self, series: pd.Series, name: str) -> ColumnProfile:
        """Profile a single column."""
        total_count = len(series)
        null_count = series.isna().sum()
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        non_null = series.dropna()
        unique_count = non_null.nunique()
        cardinality = (unique_count / total_count) if total_count > 0 else 0

        # Determine if numeric or string
        dtype = str(series.dtype)
        is_numeric = pd.api.types.is_numeric_dtype(series)
        is_string = pd.api.types.is_string_dtype(series) or series.dtype == "object"

        # Initialize profile
        profile = ColumnProfile(
            name=name,
            dtype=dtype,
            total_count=total_count,
            null_count=int(null_count),
            null_percentage=null_percentage,
            unique_count=unique_count,
            cardinality=cardinality,
            is_unique=(unique_count == total_count - null_count),
            is_constant=(unique_count <= 1),
            has_duplicates=(unique_count < total_count - null_count),
        )

        if is_numeric and len(non_null) > 0:
            # Numeric statistics
            profile.min_value = float(non_null.min())
            profile.max_value = float(non_null.max())
            profile.mean_value = float(non_null.mean())
            profile.median_value = float(non_null.median())
            profile.std_value = float(non_null.std()) if len(non_null) > 1 else 0.0

        if is_string and len(non_null) > 0:
            # String statistics
            str_lengths = non_null.astype(str).str.len()
            profile.min_length = int(str_lengths.min())
            profile.max_length = int(str_lengths.max())
            profile.avg_length = float(str_lengths.mean())

            # Pattern detection
            if self.detect_patterns:
                profile.patterns = self._detect_patterns(non_null)

        # Top values
        if len(non_null) > 0:
            value_counts = non_null.value_counts().head(self.top_n)
            profile.top_values = [
                {"value": str(v), "count": int(c), "percentage": round(c / total_count * 100, 2)}
                for v, c in value_counts.items()
            ]

        # Calculate quality score
        profile.data_quality_score = self._calculate_quality_score(profile)

        return profile

    def _detect_patterns(self, series: pd.Series) -> List[str]:
        """Detect common patterns in a string column."""
        detected = []

        # Sample for performance
        if len(series) > self.sample_size:
            sample = series.sample(self.sample_size, random_state=42)
        else:
            sample = series

        sample_str = sample.astype(str)

        for pattern_name, pattern_regex in self.PATTERNS.items():
            try:
                matches = sample_str.str.match(pattern_regex, na=False)
                match_pct = matches.sum() / len(sample) * 100
                if match_pct > 80:
                    detected.append(pattern_name)
            except Exception:
                pass

        return detected

    def _calculate_quality_score(self, profile: ColumnProfile) -> float:
        """Calculate a quality score for a column (0-100)."""
        score = 100.0

        # Penalize for nulls
        score -= min(profile.null_percentage, 30)

        # Penalize for constant values (might indicate data issues)
        if profile.is_constant and profile.total_count > 1:
            score -= 10

        # Bonus for unique identifier columns
        if profile.is_unique and profile.null_count == 0:
            score = min(100, score + 5)

        return max(0, min(100, score))

    def compare_schemas(
        self,
        profile1: ProfileResult,
        profile2: ProfileResult,
    ) -> Dict[str, Any]:
        """
        Compare schemas of two profiles.

        Args:
            profile1: First profile.
            profile2: Second profile.

        Returns:
            Dictionary with schema differences.
        """
        cols1 = {c.name: c for c in profile1.columns}
        cols2 = {c.name: c for c in profile2.columns}

        only_in_1 = set(cols1.keys()) - set(cols2.keys())
        only_in_2 = set(cols2.keys()) - set(cols1.keys())
        common = set(cols1.keys()) & set(cols2.keys())

        type_mismatches = []
        for col in common:
            if cols1[col].dtype != cols2[col].dtype:
                type_mismatches.append({
                    "column": col,
                    "type1": cols1[col].dtype,
                    "type2": cols2[col].dtype,
                })

        return {
            "schemas_match": len(only_in_1) == 0 and len(only_in_2) == 0 and len(type_mismatches) == 0,
            "only_in_first": list(only_in_1),
            "only_in_second": list(only_in_2),
            "common_columns": list(common),
            "type_mismatches": type_mismatches,
        }

    def detect_schema_drift(
        self,
        baseline: ProfileResult,
        current: ProfileResult,
        thresholds: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Detect schema drift between baseline and current profiles.

        Args:
            baseline: Baseline profile.
            current: Current profile.
            thresholds: Thresholds for drift detection.

        Returns:
            Dictionary with drift information.
        """
        if thresholds is None:
            thresholds = {
                "null_increase": 10.0,  # Percentage point increase
                "cardinality_change": 0.2,  # Relative change
                "type_change": True,
            }

        schema_diff = self.compare_schemas(baseline, current)
        drifts = []

        # Check common columns for drift
        for col_name in schema_diff["common_columns"]:
            baseline_col = baseline.get_column(col_name)
            current_col = current.get_column(col_name)

            if baseline_col and current_col:
                col_drifts = []

                # Check null percentage increase
                null_diff = current_col.null_percentage - baseline_col.null_percentage
                if null_diff > thresholds["null_increase"]:
                    col_drifts.append({
                        "type": "null_increase",
                        "baseline": baseline_col.null_percentage,
                        "current": current_col.null_percentage,
                        "change": null_diff,
                    })

                # Check cardinality change
                if baseline_col.cardinality > 0:
                    card_change = abs(current_col.cardinality - baseline_col.cardinality) / baseline_col.cardinality
                    if card_change > thresholds["cardinality_change"]:
                        col_drifts.append({
                            "type": "cardinality_change",
                            "baseline": baseline_col.cardinality,
                            "current": current_col.cardinality,
                            "change": card_change,
                        })

                # Type change
                if thresholds.get("type_change") and baseline_col.dtype != current_col.dtype:
                    col_drifts.append({
                        "type": "type_change",
                        "baseline": baseline_col.dtype,
                        "current": current_col.dtype,
                    })

                if col_drifts:
                    drifts.append({
                        "column": col_name,
                        "drifts": col_drifts,
                    })

        return {
            "has_drift": len(drifts) > 0 or not schema_diff["schemas_match"],
            "schema_changes": schema_diff,
            "column_drifts": drifts,
        }
