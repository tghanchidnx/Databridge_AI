"""
Pattern Detection Service for SQL Generator.

Analyzes table schemas and data to classify:
- Tables as fact, dimension, bridge, aggregate, or staging
- Columns as measures, dimensions, keys, dates, or metadata
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import re
from datetime import datetime

from .models import (
    PatternType,
    ColumnType,
    DetectedPattern,
    ColumnClassification,
)


@dataclass
class TableAnalysis:
    """Results of table pattern analysis."""
    table_name: str
    row_count: Optional[int]
    column_count: int
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]]
    foreign_keys: List[Dict[str, Any]]
    numeric_columns: List[str]
    date_columns: List[str]
    string_columns: List[str]


class PatternDetectorService:
    """
    Service for detecting fact/dimension patterns from table schemas.

    Uses heuristics based on:
    - Column naming conventions
    - Data types
    - Cardinality analysis
    - Foreign key relationships
    """

    # Common naming patterns
    FACT_PREFIXES = ("fact_", "fct_", "f_", "transaction_", "txn_")
    DIMENSION_PREFIXES = ("dim_", "d_", "dimension_", "lookup_", "lkp_")
    BRIDGE_PREFIXES = ("bridge_", "brg_", "xref_", "map_")
    STAGING_PREFIXES = ("stg_", "stage_", "staging_", "raw_", "src_")
    AGGREGATE_PREFIXES = ("agg_", "aggregate_", "summary_", "mart_")

    # Measure column patterns
    MEASURE_PATTERNS = [
        r".*_amt$",
        r".*_amount$",
        r".*_qty$",
        r".*_quantity$",
        r".*_count$",
        r".*_total$",
        r".*_sum$",
        r".*_avg$",
        r".*_price$",
        r".*_cost$",
        r".*_value$",
        r".*_revenue$",
        r".*_sales$",
        r".*_balance$",
        r".*_rate$",
        r".*_percent$",
        r".*_pct$",
        r"^gross_.*",
        r"^net_.*",
        r"^total_.*",
    ]

    # Key column patterns
    KEY_PATTERNS = [
        r".*_id$",
        r".*_key$",
        r".*_sk$",
        r".*_nk$",
        r".*_pk$",
        r".*_fk$",
        r"^id$",
        r"^key$",
    ]

    # Date column patterns
    DATE_PATTERNS = [
        r".*_date$",
        r".*_dt$",
        r".*_datetime$",
        r".*_timestamp$",
        r".*_time$",
        r".*_at$",
        r"^date$",
        r"^created.*",
        r"^updated.*",
        r"^modified.*",
        r"^effective.*",
        r"^expir.*",
    ]

    # Metadata column patterns
    METADATA_PATTERNS = [
        r"^created_.*",
        r"^updated_.*",
        r"^modified_.*",
        r"^deleted_.*",
        r"^is_deleted$",
        r"^is_active$",
        r"^etl_.*",
        r"^dw_.*",
        r"^load_.*",
        r"^insert_.*",
        r"^update_.*",
        r"^batch_.*",
        r"^row_.*",
        r"^audit_.*",
        r"^source_.*",
    ]

    # Numeric SQL types
    NUMERIC_TYPES = {
        "int", "integer", "bigint", "smallint", "tinyint",
        "decimal", "numeric", "number", "float", "real", "double",
        "money", "smallmoney",
    }

    # Date SQL types
    DATE_TYPES = {
        "date", "datetime", "datetime2", "timestamp", "time",
        "timestamp_ntz", "timestamp_ltz", "timestamp_tz",
    }

    # String SQL types
    STRING_TYPES = {
        "varchar", "char", "nvarchar", "nchar", "text", "string",
        "clob", "nclob",
    }

    def __init__(self):
        """Initialize the pattern detector."""
        # Compile regex patterns for performance
        self._measure_patterns = [re.compile(p, re.IGNORECASE) for p in self.MEASURE_PATTERNS]
        self._key_patterns = [re.compile(p, re.IGNORECASE) for p in self.KEY_PATTERNS]
        self._date_patterns = [re.compile(p, re.IGNORECASE) for p in self.DATE_PATTERNS]
        self._metadata_patterns = [re.compile(p, re.IGNORECASE) for p in self.METADATA_PATTERNS]

    def detect_table_pattern(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        primary_key: Optional[List[str]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
        row_count: Optional[int] = None,
        sample_data: Optional[List[Dict[str, Any]]] = None,
    ) -> DetectedPattern:
        """
        Detect the pattern type for a table.

        Args:
            table_name: Name of the table
            columns: List of column definitions with 'name' and 'data_type'
            primary_key: List of primary key column names
            foreign_keys: List of foreign key definitions
            row_count: Optional row count for cardinality analysis
            sample_data: Optional sample rows for data analysis

        Returns:
            DetectedPattern with classification and confidence
        """
        table_lower = table_name.lower()
        foreign_keys = foreign_keys or []

        # Classify all columns first
        classified_columns = [
            self.classify_column(col["name"], col.get("data_type", ""), sample_data)
            for col in columns
        ]

        # Count column types
        measure_count = sum(1 for c in classified_columns if c.column_type == ColumnType.MEASURE)
        key_count = sum(1 for c in classified_columns if c.column_type in (ColumnType.PRIMARY_KEY, ColumnType.FOREIGN_KEY))
        dimension_count = sum(1 for c in classified_columns if c.column_type == ColumnType.DIMENSION)
        date_count = sum(1 for c in classified_columns if c.column_type == ColumnType.DATE_KEY)

        # Check naming conventions first (highest priority)
        pattern_type = PatternType.UNKNOWN
        confidence = 0.0
        reasoning_parts = []

        # Check prefixes
        if any(table_lower.startswith(p) for p in self.FACT_PREFIXES):
            pattern_type = PatternType.FACT
            confidence = 0.85
            reasoning_parts.append("Table name has fact prefix")

        elif any(table_lower.startswith(p) for p in self.DIMENSION_PREFIXES):
            pattern_type = PatternType.DIMENSION
            confidence = 0.85
            reasoning_parts.append("Table name has dimension prefix")

        elif any(table_lower.startswith(p) for p in self.BRIDGE_PREFIXES):
            pattern_type = PatternType.BRIDGE
            confidence = 0.85
            reasoning_parts.append("Table name has bridge prefix")

        elif any(table_lower.startswith(p) for p in self.STAGING_PREFIXES):
            pattern_type = PatternType.STAGING
            confidence = 0.80
            reasoning_parts.append("Table name has staging prefix")

        elif any(table_lower.startswith(p) for p in self.AGGREGATE_PREFIXES):
            pattern_type = PatternType.AGGREGATE
            confidence = 0.80
            reasoning_parts.append("Table name has aggregate prefix")

        else:
            # Use heuristics based on column analysis
            total_cols = len(columns)

            # Fact table heuristics
            if measure_count >= 2 and key_count >= 2:
                pattern_type = PatternType.FACT
                confidence = 0.70
                reasoning_parts.append(f"Has {measure_count} measure columns and {key_count} key columns")

            # Dimension table heuristics
            elif dimension_count > measure_count and key_count <= 2:
                pattern_type = PatternType.DIMENSION
                confidence = 0.65
                reasoning_parts.append(f"Has {dimension_count} dimension columns with few keys")

            # Bridge table heuristics (mostly foreign keys)
            elif key_count >= 2 and measure_count == 0 and dimension_count <= 2:
                pattern_type = PatternType.BRIDGE
                confidence = 0.60
                reasoning_parts.append(f"Has {key_count} key columns with no measures")

            # Default based on measures
            elif measure_count > 0:
                pattern_type = PatternType.FACT
                confidence = 0.50
                reasoning_parts.append(f"Has {measure_count} measure columns")
            else:
                pattern_type = PatternType.DIMENSION
                confidence = 0.40
                reasoning_parts.append("Default classification - no clear measures")

        # Adjust confidence based on foreign key analysis
        if foreign_keys:
            fk_count = len(foreign_keys)
            if pattern_type == PatternType.FACT and fk_count >= 2:
                confidence = min(confidence + 0.1, 0.95)
                reasoning_parts.append(f"Has {fk_count} foreign keys (common for facts)")
            elif pattern_type == PatternType.DIMENSION and fk_count == 0:
                confidence = min(confidence + 0.1, 0.95)
                reasoning_parts.append("No foreign keys (common for dimensions)")

        # Build column list for result
        result_columns = [c.to_dict() for c in classified_columns]

        return DetectedPattern(
            table_name=table_name,
            pattern_type=pattern_type,
            confidence=confidence,
            columns=result_columns,
            reasoning="; ".join(reasoning_parts),
        )

    def classify_column(
        self,
        column_name: str,
        data_type: str,
        sample_data: Optional[List[Dict[str, Any]]] = None,
    ) -> ColumnClassification:
        """
        Classify a single column by its type.

        Args:
            column_name: Name of the column
            data_type: SQL data type of the column
            sample_data: Optional sample rows for data analysis

        Returns:
            ColumnClassification with type and confidence
        """
        col_lower = column_name.lower()
        type_lower = data_type.lower().split("(")[0].strip()  # Remove size spec

        confidence = 0.5  # Default confidence
        column_type = ColumnType.UNKNOWN
        sample_values = []
        statistics = {}

        # Extract sample values if available
        if sample_data:
            sample_values = [
                row.get(column_name) for row in sample_data[:10]
                if column_name in row and row.get(column_name) is not None
            ]

        # Check for metadata columns first (highest priority)
        if any(p.match(col_lower) for p in self._metadata_patterns):
            column_type = ColumnType.METADATA
            confidence = 0.90
            return ColumnClassification(column_name, column_type, data_type, confidence, sample_values, statistics)

        # Check for primary key pattern
        if col_lower in ("id", "pk") or col_lower.endswith("_pk"):
            column_type = ColumnType.PRIMARY_KEY
            confidence = 0.85

        # Check for foreign key pattern
        elif col_lower.endswith("_fk") or (col_lower.endswith("_id") and col_lower != "id"):
            column_type = ColumnType.FOREIGN_KEY
            confidence = 0.80

        # Check for key patterns (could be PK or FK)
        elif any(p.match(col_lower) for p in self._key_patterns):
            if type_lower in self.NUMERIC_TYPES:
                column_type = ColumnType.FOREIGN_KEY
                confidence = 0.70
            else:
                column_type = ColumnType.DIMENSION
                confidence = 0.60

        # Check for date patterns
        elif any(p.match(col_lower) for p in self._date_patterns) or type_lower in self.DATE_TYPES:
            column_type = ColumnType.DATE_KEY
            confidence = 0.85

        # Check for measure patterns
        elif any(p.match(col_lower) for p in self._measure_patterns):
            column_type = ColumnType.MEASURE
            confidence = 0.85

        # Check by data type
        elif type_lower in self.NUMERIC_TYPES:
            # Numeric could be measure or FK
            if any(p.match(col_lower) for p in self._key_patterns):
                column_type = ColumnType.FOREIGN_KEY
                confidence = 0.65
            else:
                column_type = ColumnType.MEASURE
                confidence = 0.70

        # String columns are typically dimensions
        elif type_lower in self.STRING_TYPES:
            column_type = ColumnType.DIMENSION
            confidence = 0.75

        else:
            # Default to dimension
            column_type = ColumnType.DIMENSION
            confidence = 0.40

        return ColumnClassification(column_name, column_type, data_type, confidence, sample_values, statistics)

    def suggest_hierarchy_mappings(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        hierarchy_levels: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Suggest mappings between table columns and hierarchy levels.

        Args:
            table_name: Name of the source table
            columns: List of column definitions
            hierarchy_levels: List of hierarchy level names to map

        Returns:
            List of suggested mappings with confidence scores
        """
        suggestions = []

        for level in hierarchy_levels:
            level_lower = level.lower().replace(" ", "_")
            best_match = None
            best_score = 0.0

            for col in columns:
                col_name = col["name"].lower()
                score = self._calculate_match_score(level_lower, col_name)

                if score > best_score:
                    best_score = score
                    best_match = col

            if best_match and best_score > 0.3:
                suggestions.append({
                    "hierarchy_level": level,
                    "source_column": best_match["name"],
                    "source_table": table_name,
                    "confidence": best_score,
                    "data_type": best_match.get("data_type", ""),
                })

        return suggestions

    def _calculate_match_score(self, level_name: str, column_name: str) -> float:
        """
        Calculate similarity score between hierarchy level and column name.

        Args:
            level_name: Normalized hierarchy level name
            column_name: Normalized column name

        Returns:
            Similarity score between 0 and 1
        """
        # Exact match
        if level_name == column_name:
            return 1.0

        # Contains match
        if level_name in column_name or column_name in level_name:
            return 0.8

        # Word overlap
        level_words = set(level_name.split("_"))
        col_words = set(column_name.split("_"))
        common = level_words & col_words

        if common:
            return len(common) / max(len(level_words), len(col_words)) * 0.7

        return 0.0

    def analyze_table_schema(
        self,
        columns: List[Dict[str, Any]],
        primary_key: Optional[List[str]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Analyze table schema and return summary statistics.

        Args:
            columns: List of column definitions
            primary_key: Primary key columns
            foreign_keys: Foreign key definitions

        Returns:
            Dictionary with analysis summary
        """
        classified = [
            self.classify_column(c["name"], c.get("data_type", ""))
            for c in columns
        ]

        type_counts = {}
        for c in classified:
            type_name = c.column_type.value
            type_counts[type_name] = type_counts.get(type_name, 0) + 1

        return {
            "total_columns": len(columns),
            "column_type_counts": type_counts,
            "has_primary_key": bool(primary_key),
            "foreign_key_count": len(foreign_keys) if foreign_keys else 0,
            "measure_columns": [c.column_name for c in classified if c.column_type == ColumnType.MEASURE],
            "dimension_columns": [c.column_name for c in classified if c.column_type == ColumnType.DIMENSION],
            "key_columns": [c.column_name for c in classified if c.column_type in (ColumnType.PRIMARY_KEY, ColumnType.FOREIGN_KEY)],
            "date_columns": [c.column_name for c in classified if c.column_type == ColumnType.DATE_KEY],
        }


# Singleton instance for easy access
_pattern_detector = None


def get_pattern_detector() -> PatternDetectorService:
    """Get or create the pattern detector singleton."""
    global _pattern_detector
    if _pattern_detector is None:
        _pattern_detector = PatternDetectorService()
    return _pattern_detector
