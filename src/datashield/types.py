"""DataShield Types - Pydantic models and enums for data scrambling.

Defines the configuration schema for shield projects, table configs,
column rules, and scrambling strategies.
"""

from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import uuid


# =============================================================================
# Enumerations
# =============================================================================

class ScrambleStrategy(str, Enum):
    """Scrambling strategy for a column."""
    FORMAT_PRESERVING_HASH = "format_preserving_hash"
    NUMERIC_SCALING = "numeric_scaling"
    SYNTHETIC_SUBSTITUTION = "synthetic_substitution"
    DATE_SHIFT = "date_shift"
    PATTERN_PRESERVING = "pattern_preserving"
    PASSTHROUGH = "passthrough"


class ColumnClassification(str, Enum):
    """Classification of a column's data type and sensitivity."""
    MEASURE = "measure"
    FACT_DIMENSION = "fact_dimension"
    DESCRIPTIVE = "descriptive"
    GEOGRAPHIC = "geographic"
    TEMPORAL = "temporal"
    IDENTIFIER = "identifier"
    CODE = "code"
    SENSITIVE_PII = "sensitive_pii"
    SAFE = "safe"


class ShieldScope(str, Enum):
    """Scope of shield application."""
    TABLE = "table"
    COLUMN = "column"
    SCHEMA = "schema"


# =============================================================================
# Default strategy mapping per classification
# =============================================================================

CLASSIFICATION_STRATEGY_MAP = {
    ColumnClassification.MEASURE: ScrambleStrategy.NUMERIC_SCALING,
    ColumnClassification.FACT_DIMENSION: ScrambleStrategy.FORMAT_PRESERVING_HASH,
    ColumnClassification.DESCRIPTIVE: ScrambleStrategy.SYNTHETIC_SUBSTITUTION,
    ColumnClassification.GEOGRAPHIC: ScrambleStrategy.SYNTHETIC_SUBSTITUTION,
    ColumnClassification.TEMPORAL: ScrambleStrategy.DATE_SHIFT,
    ColumnClassification.IDENTIFIER: ScrambleStrategy.FORMAT_PRESERVING_HASH,
    ColumnClassification.CODE: ScrambleStrategy.FORMAT_PRESERVING_HASH,
    ColumnClassification.SENSITIVE_PII: ScrambleStrategy.PATTERN_PRESERVING,
    ColumnClassification.SAFE: ScrambleStrategy.PASSTHROUGH,
}


# =============================================================================
# Core Models
# =============================================================================

class ColumnRule(BaseModel):
    """Per-column scrambling configuration."""
    column_name: str
    classification: ColumnClassification
    strategy: ScrambleStrategy
    preserve_nulls: bool = True
    preserve_format: bool = True
    synthetic_pool: Optional[str] = None
    custom_regex: Optional[str] = None


class TableShieldConfig(BaseModel):
    """Per-table shield configuration with column rules."""
    database: str
    schema_name: str
    table_name: str
    table_type: str = "unknown"
    column_rules: List[ColumnRule] = []
    key_columns: List[str] = []
    skip_columns: List[str] = []


class ShieldProject(BaseModel):
    """Top-level DataShield project."""
    id: str = Field(default_factory=lambda: f"proj_{uuid.uuid4().hex[:6]}")
    name: str
    description: Optional[str] = None
    key_alias: str
    tables: List[TableShieldConfig] = []
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_at: Optional[str] = None
    active: bool = True
