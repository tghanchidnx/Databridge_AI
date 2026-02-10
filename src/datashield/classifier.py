"""DataShield Classifier - Auto-classify columns by sensitivity and type.

Reuses PII patterns from data_catalog and adds DataShield-specific heuristics
based on column names, data types, value patterns, and cardinality.
"""

import re
import logging
from typing import List, Optional, Dict, Any

from .types import (
    ColumnRule,
    ColumnClassification,
    ScrambleStrategy,
    CLASSIFICATION_STRATEGY_MAP,
)
from .constants import COLUMN_NAME_PATTERNS, VALUE_PII_PATTERNS

logger = logging.getLogger(__name__)


def _classify_by_name(column_name: str) -> Optional[ColumnClassification]:
    """Classify a column by its name using regex patterns.

    Args:
        column_name: The column name to classify

    Returns:
        Classification or None if no match
    """
    col_lower = column_name.lower()

    # Check in priority order (PII first, then specific, then generic)
    priority_order = [
        ("sensitive_pii", ColumnClassification.SENSITIVE_PII),
        ("identifier", ColumnClassification.IDENTIFIER),
        ("temporal", ColumnClassification.TEMPORAL),
        ("measure", ColumnClassification.MEASURE),
        ("code", ColumnClassification.CODE),
        ("geographic", ColumnClassification.GEOGRAPHIC),
        ("fact_dimension", ColumnClassification.FACT_DIMENSION),
        ("descriptive", ColumnClassification.DESCRIPTIVE),
        ("safe", ColumnClassification.SAFE),
    ]

    for pattern_key, classification in priority_order:
        patterns = COLUMN_NAME_PATTERNS.get(pattern_key, [])
        for pattern in patterns:
            if re.search(pattern, col_lower):
                return classification

    return None


def _classify_by_data_type(data_type: str) -> Optional[ColumnClassification]:
    """Classify by SQL data type.

    Args:
        data_type: SQL data type string (e.g., "NUMBER(18,2)", "VARCHAR(100)")

    Returns:
        Classification or None
    """
    dt = data_type.upper()

    if any(t in dt for t in ("NUMBER", "NUMERIC", "DECIMAL", "FLOAT",
                              "DOUBLE", "REAL", "INT", "BIGINT", "SMALLINT")):
        return ColumnClassification.MEASURE

    if any(t in dt for t in ("DATE", "TIME", "TIMESTAMP")):
        return ColumnClassification.TEMPORAL

    if any(t in dt for t in ("BOOLEAN", "BOOL")):
        return ColumnClassification.SAFE

    return None


def _classify_by_values(sample_values: List[Any]) -> Optional[ColumnClassification]:
    """Classify by inspecting sample values for PII patterns.

    Args:
        sample_values: List of distinct sample values

    Returns:
        Classification or None
    """
    if not sample_values:
        return None

    # Check for PII patterns in values
    str_values = [str(v) for v in sample_values if v is not None]
    if not str_values:
        return None

    for _pii_type, pattern in VALUE_PII_PATTERNS.items():
        matches = sum(1 for v in str_values if re.match(pattern, v))
        if matches > len(str_values) * 0.5:
            return ColumnClassification.SENSITIVE_PII

    return None


def _classify_by_cardinality(distinct_count: int, total_count: int,
                              data_type: str) -> Optional[ColumnClassification]:
    """Use cardinality heuristics.

    High cardinality VARCHAR → likely IDENTIFIER
    Low cardinality → likely SAFE (enum/status)
    """
    if total_count == 0:
        return None

    ratio = distinct_count / total_count
    dt = data_type.upper()

    # Low cardinality string columns → safe (flags, statuses)
    if "CHAR" in dt or "TEXT" in dt:
        if distinct_count <= 10 and ratio < 0.01:
            return ColumnClassification.SAFE
        # Very high cardinality VARCHAR → likely identifier
        if ratio > 0.95 and distinct_count > 100:
            return ColumnClassification.IDENTIFIER

    return None


def auto_classify_columns(
    columns: List[Dict[str, Any]],
    sample_data: Optional[Dict[str, List[Any]]] = None,
    row_count: int = 0,
) -> List[ColumnRule]:
    """Auto-classify columns and suggest scrambling rules.

    Args:
        columns: List of column metadata dicts with keys:
            - name: column name
            - data_type: SQL data type
            - nullable: bool (optional)
        sample_data: Optional dict of column_name -> list of sample values
        row_count: Total row count for cardinality analysis

    Returns:
        List of suggested ColumnRules
    """
    rules = []

    for col in columns:
        col_name = col.get("name", "")
        data_type = col.get("data_type", "VARCHAR")

        # Try classification in priority order
        classification = None

        # 1. Check sample values for PII
        if sample_data and col_name in sample_data:
            classification = _classify_by_values(sample_data[col_name])

        # 2. Check column name patterns
        if classification is None:
            classification = _classify_by_name(col_name)

        # 3. Check data type
        if classification is None:
            classification = _classify_by_data_type(data_type)

        # 4. Check cardinality
        if classification is None and sample_data and col_name in sample_data:
            distinct_count = len(set(sample_data[col_name]))
            classification = _classify_by_cardinality(
                distinct_count, row_count or len(sample_data[col_name]), data_type
            )

        # 5. Default to SAFE if nothing matched
        if classification is None:
            classification = ColumnClassification.SAFE

        # Get default strategy
        strategy = CLASSIFICATION_STRATEGY_MAP.get(
            classification, ScrambleStrategy.PASSTHROUGH
        )

        # Determine synthetic pool for substitution strategy
        synthetic_pool = None
        if strategy == ScrambleStrategy.SYNTHETIC_SUBSTITUTION:
            if classification == ColumnClassification.GEOGRAPHIC:
                col_lower = col_name.lower()
                if "country" in col_lower:
                    synthetic_pool = "country_names"
                elif "city" in col_lower:
                    synthetic_pool = "cities"
                elif "region" in col_lower or "state" in col_lower:
                    synthetic_pool = "regions"
                else:
                    synthetic_pool = "cities"
            elif classification == ColumnClassification.DESCRIPTIVE:
                col_lower = col_name.lower()
                if any(kw in col_lower for kw in ("company", "vendor", "supplier", "org")):
                    synthetic_pool = "company_names"
                elif any(kw in col_lower for kw in ("person", "employee", "customer", "name")):
                    synthetic_pool = "person_names"
                elif any(kw in col_lower for kw in ("dept", "department", "division")):
                    synthetic_pool = "department_names"
                elif any(kw in col_lower for kw in ("product", "item", "sku")):
                    synthetic_pool = "product_names"
                else:
                    synthetic_pool = "company_names"

        rule = ColumnRule(
            column_name=col_name,
            classification=classification,
            strategy=strategy,
            preserve_nulls=True,
            preserve_format=True,
            synthetic_pool=synthetic_pool,
        )
        rules.append(rule)

    return rules
