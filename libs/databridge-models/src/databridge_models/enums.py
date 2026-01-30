"""
Shared enumerations for DataBridge AI platform.

These enums are used across V3 and V4 applications for consistent
type definitions and validation.
"""

from enum import Enum


class SQLDialect(str, Enum):
    """Supported SQL dialects for code generation."""
    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    TSQL = "tsql"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    DATABRICKS = "databricks"
    REDSHIFT = "redshift"


class TableStatus(str, Enum):
    """Status of generated tables/views."""
    DRAFT = "draft"
    PENDING = "pending"
    VALIDATED = "validated"
    DEPLOYED = "deployed"
    FAILED = "failed"
    ARCHIVED = "archived"


class AggregationType(str, Enum):
    """Supported aggregation functions."""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    FIRST = "FIRST"
    LAST = "LAST"
    MEDIAN = "MEDIAN"
    STDDEV = "STDDEV"
    VARIANCE = "VARIANCE"


class JoinType(str, Enum):
    """SQL join types."""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    LEFT_OUTER = "LEFT OUTER"
    RIGHT_OUTER = "RIGHT OUTER"
    FULL_OUTER = "FULL OUTER"


class FormulaType(str, Enum):
    """Types of formula calculations."""
    ADD = "ADD"
    SUBTRACT = "SUBTRACT"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    PERCENT = "PERCENT"
    VARIANCE = "VARIANCE"
    EXPRESSION = "EXPRESSION"
    RATIO = "RATIO"
    GROWTH = "GROWTH"
    YOY = "YOY"
    MOM = "MOM"


class PatternType(str, Enum):
    """Detected table patterns."""
    FACT = "fact"
    DIMENSION = "dimension"
    BRIDGE = "bridge"
    LOOKUP = "lookup"
    AGGREGATE = "aggregate"
    STAGING = "staging"
    UNKNOWN = "unknown"


class ColumnClassification(str, Enum):
    """Column classification types."""
    MEASURE = "measure"
    DIMENSION = "dimension"
    KEY = "key"
    FOREIGN_KEY = "foreign_key"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TEXT = "text"
    FLAG = "flag"
    AUDIT = "audit"
    UNKNOWN = "unknown"


class TransformationType(str, Enum):
    """Types of data transformations for lineage tracking."""
    SELECT = "select"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    UNION = "union"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    FORMULA = "formula"
    CUSTOM = "custom"
