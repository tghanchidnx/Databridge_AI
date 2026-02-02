"""Query module for DataBridge AI Researcher Analytics Engine."""

from .builder import QueryBuilder, Query
from .dialects import SQLDialect, SnowflakeDialect, TSQLDialect, SparkDialect, PostgreSQLDialect
from .safety import (
    SQLSanitizer,
    QueryValidator,
    QueryAuditor,
    SQLRiskLevel,
    ValidationResult,
    ValidationError,
    get_sanitizer,
    get_validator,
    get_auditor,
)

__all__ = [
    # Builder
    "QueryBuilder",
    "Query",
    # Dialects
    "SQLDialect",
    "SnowflakeDialect",
    "TSQLDialect",
    "SparkDialect",
    "PostgreSQLDialect",
    # Safety
    "SQLSanitizer",
    "QueryValidator",
    "QueryAuditor",
    "SQLRiskLevel",
    "ValidationResult",
    "ValidationError",
    "get_sanitizer",
    "get_validator",
    "get_auditor",
]
