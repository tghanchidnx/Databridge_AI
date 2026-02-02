"""
SQL Safety Module for DataBridge AI Researcher.

Provides SQL injection prevention, query validation, and audit logging.
"""

import logging
import re
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


class SQLRiskLevel(str, Enum):
    """Risk levels for SQL operations."""

    SAFE = "safe"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationError(Exception):
    """Exception raised for SQL validation failures."""

    def __init__(self, message: str, risk_level: SQLRiskLevel = SQLRiskLevel.HIGH):
        super().__init__(message)
        self.risk_level = risk_level


@dataclass
class ValidationResult:
    """Result of SQL validation."""

    is_valid: bool
    risk_level: SQLRiskLevel
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    sanitized_query: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "risk_level": self.risk_level.value,
            "issues": self.issues,
            "warnings": self.warnings,
            "sanitized_query": self.sanitized_query,
        }


@dataclass
class AuditEntry:
    """Audit log entry for SQL operations."""

    timestamp: datetime
    query_hash: str
    query_preview: str
    parameters: Dict[str, Any]
    risk_level: SQLRiskLevel
    user_context: Optional[str] = None
    source: str = "nl_to_sql"
    validated: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "query_hash": self.query_hash,
            "query_preview": self.query_preview,
            "parameters": {k: str(v)[:100] for k, v in self.parameters.items()},
            "risk_level": self.risk_level.value,
            "user_context": self.user_context,
            "source": self.source,
            "validated": self.validated,
        }


class SQLSanitizer:
    """
    SQL input sanitizer for preventing injection attacks.

    Validates and sanitizes user inputs before they're used in SQL queries.
    """

    # Dangerous SQL patterns that should never appear in user input
    DANGEROUS_PATTERNS = [
        r";\s*--",  # Comment after semicolon
        r";\s*DROP\s+",  # DROP after semicolon
        r";\s*DELETE\s+",  # DELETE after semicolon
        r";\s*INSERT\s+",  # INSERT after semicolon
        r";\s*UPDATE\s+",  # UPDATE after semicolon
        r";\s*TRUNCATE\s+",  # TRUNCATE after semicolon
        r";\s*ALTER\s+",  # ALTER after semicolon
        r";\s*CREATE\s+",  # CREATE after semicolon
        r";\s*EXEC\s*\(",  # EXEC function
        r"UNION\s+SELECT",  # UNION injection
        r"OR\s+1\s*=\s*1",  # Always true condition
        r"AND\s+1\s*=\s*1",  # Always true condition
        r"OR\s+'[^']*'\s*=\s*'[^']*'",  # String comparison always true
        r"--\s*$",  # Trailing comment
        r"/\*.*\*/",  # Block comment
        r"xp_cmdshell",  # SQL Server command execution
        r"sp_executesql",  # SQL Server dynamic execution
        r"WAITFOR\s+DELAY",  # Time-based injection
        r"BENCHMARK\s*\(",  # MySQL benchmark
        r"SLEEP\s*\(",  # MySQL/PostgreSQL sleep
        r"PG_SLEEP\s*\(",  # PostgreSQL sleep
    ]

    # Allowed characters for identifiers (table/column names)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    # Allowed characters for qualified identifiers (schema.table)
    QUALIFIED_IDENTIFIER_PATTERN = re.compile(
        r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$'
    )

    def __init__(self, strict_mode: bool = True):
        """
        Initialize sanitizer.

        Args:
            strict_mode: If True, reject any suspicious input.
        """
        self.strict_mode = strict_mode
        self._compiled_patterns = [
            re.compile(p, re.IGNORECASE | re.DOTALL)
            for p in self.DANGEROUS_PATTERNS
        ]

    def sanitize_identifier(self, value: str) -> str:
        """
        Sanitize a SQL identifier (table/column name).

        Args:
            value: Identifier to sanitize.

        Returns:
            Sanitized identifier.

        Raises:
            ValidationError: If identifier is invalid.
        """
        if not value:
            raise ValidationError("Empty identifier")

        # Strip whitespace
        value = value.strip()

        # Check for valid identifier pattern
        if not self.QUALIFIED_IDENTIFIER_PATTERN.match(value):
            raise ValidationError(
                f"Invalid identifier: {value[:50]}",
                SQLRiskLevel.HIGH,
            )

        # Check against reserved words (basic set)
        reserved = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
            "ALTER", "TRUNCATE", "EXEC", "EXECUTE", "UNION", "INTO",
        }
        if value.upper() in reserved:
            raise ValidationError(
                f"Reserved word used as identifier: {value}",
                SQLRiskLevel.MEDIUM,
            )

        return value

    def sanitize_value(self, value: Any) -> Tuple[Any, str]:
        """
        Sanitize a value for use in a parameterized query.

        Args:
            value: Value to sanitize.

        Returns:
            Tuple of (sanitized_value, value_type).

        Raises:
            ValidationError: If value is dangerous.
        """
        if value is None:
            return None, "null"

        if isinstance(value, bool):
            return value, "boolean"

        if isinstance(value, (int, float)):
            return value, "numeric"

        if isinstance(value, str):
            # Check for dangerous patterns
            for pattern in self._compiled_patterns:
                if pattern.search(value):
                    raise ValidationError(
                        f"Potentially dangerous SQL pattern detected",
                        SQLRiskLevel.CRITICAL,
                    )

            # Check for excessive length
            if len(value) > 10000:
                raise ValidationError(
                    "String value too long",
                    SQLRiskLevel.MEDIUM,
                )

            return value, "string"

        if isinstance(value, (list, tuple)):
            sanitized = []
            for item in value:
                san_item, _ = self.sanitize_value(item)
                sanitized.append(san_item)
            return sanitized, "array"

        if isinstance(value, datetime):
            return value, "datetime"

        # Unknown type - convert to string and check
        str_value = str(value)
        return self.sanitize_value(str_value)

    def validate_year(self, year: Any) -> int:
        """
        Validate and sanitize a year value.

        Args:
            year: Year value to validate.

        Returns:
            Validated year as integer.

        Raises:
            ValidationError: If year is invalid.
        """
        try:
            year_int = int(year)
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid year value: {year}",
                SQLRiskLevel.MEDIUM,
            )

        # Reasonable year range
        if year_int < 1900 or year_int > 2100:
            raise ValidationError(
                f"Year out of range: {year_int}",
                SQLRiskLevel.LOW,
            )

        return year_int

    def validate_month(self, month: Any) -> int:
        """
        Validate and sanitize a month value.

        Args:
            month: Month value to validate.

        Returns:
            Validated month as integer.

        Raises:
            ValidationError: If month is invalid.
        """
        try:
            month_int = int(month)
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid month value: {month}",
                SQLRiskLevel.MEDIUM,
            )

        if month_int < 1 or month_int > 12:
            raise ValidationError(
                f"Month out of range: {month_int}",
                SQLRiskLevel.LOW,
            )

        return month_int

    def validate_limit(self, limit: Any, max_limit: int = 10000) -> int:
        """
        Validate and sanitize a LIMIT value.

        Args:
            limit: Limit value to validate.
            max_limit: Maximum allowed limit.

        Returns:
            Validated limit as integer.

        Raises:
            ValidationError: If limit is invalid.
        """
        try:
            limit_int = int(limit)
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid limit value: {limit}",
                SQLRiskLevel.MEDIUM,
            )

        if limit_int < 0:
            raise ValidationError(
                "Limit cannot be negative",
                SQLRiskLevel.LOW,
            )

        if limit_int > max_limit:
            raise ValidationError(
                f"Limit exceeds maximum: {limit_int} > {max_limit}",
                SQLRiskLevel.LOW,
            )

        return limit_int


class QueryValidator:
    """
    Validates SQL queries for safety and complexity.
    """

    # Maximum query complexity thresholds
    DEFAULT_MAX_JOINS = 10
    DEFAULT_MAX_SUBQUERIES = 5
    DEFAULT_MAX_UNIONS = 5
    DEFAULT_MAX_QUERY_LENGTH = 50000

    def __init__(
        self,
        max_joins: int = DEFAULT_MAX_JOINS,
        max_subqueries: int = DEFAULT_MAX_SUBQUERIES,
        max_unions: int = DEFAULT_MAX_UNIONS,
        max_query_length: int = DEFAULT_MAX_QUERY_LENGTH,
    ):
        """
        Initialize validator.

        Args:
            max_joins: Maximum allowed JOINs.
            max_subqueries: Maximum allowed subqueries.
            max_unions: Maximum allowed UNIONs.
            max_query_length: Maximum query string length.
        """
        self.max_joins = max_joins
        self.max_subqueries = max_subqueries
        self.max_unions = max_unions
        self.max_query_length = max_query_length
        self.sanitizer = SQLSanitizer()

    def validate(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """
        Validate a SQL query.

        Args:
            sql: SQL query string.
            parameters: Query parameters.

        Returns:
            ValidationResult with findings.
        """
        issues = []
        warnings = []
        risk_level = SQLRiskLevel.SAFE

        parameters = parameters or {}

        # Check query length
        if len(sql) > self.max_query_length:
            issues.append(f"Query exceeds maximum length ({len(sql)} > {self.max_query_length})")
            risk_level = SQLRiskLevel.MEDIUM

        # Count JOINs
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        if join_count > self.max_joins:
            issues.append(f"Too many JOINs ({join_count} > {self.max_joins})")
            risk_level = max(risk_level, SQLRiskLevel.MEDIUM, key=lambda x: list(SQLRiskLevel).index(x))

        # Count subqueries
        subquery_count = sql.count('(SELECT')
        if subquery_count > self.max_subqueries:
            issues.append(f"Too many subqueries ({subquery_count} > {self.max_subqueries})")
            risk_level = max(risk_level, SQLRiskLevel.MEDIUM, key=lambda x: list(SQLRiskLevel).index(x))

        # Count UNIONs
        union_count = len(re.findall(r'\bUNION\b', sql, re.IGNORECASE))
        if union_count > self.max_unions:
            issues.append(f"Too many UNIONs ({union_count} > {self.max_unions})")
            risk_level = max(risk_level, SQLRiskLevel.MEDIUM, key=lambda x: list(SQLRiskLevel).index(x))

        # Check for dangerous patterns in the SQL itself
        for pattern in self.sanitizer._compiled_patterns:
            if pattern.search(sql):
                issues.append("Potentially dangerous SQL pattern detected")
                risk_level = SQLRiskLevel.CRITICAL
                break

        # Check for non-parameterized values that look like injection attempts
        # Look for string literals with suspicious content
        string_literals = re.findall(r"'([^']*)'", sql)
        for literal in string_literals:
            if any(p.search(literal) for p in self.sanitizer._compiled_patterns):
                issues.append("Suspicious string literal in query")
                risk_level = SQLRiskLevel.HIGH

        # Validate parameters
        for name, value in parameters.items():
            try:
                self.sanitizer.sanitize_value(value)
            except ValidationError as e:
                issues.append(f"Parameter '{name}': {str(e)}")
                risk_level = max(risk_level, e.risk_level, key=lambda x: list(SQLRiskLevel).index(x))

        # Warnings for best practices
        if 'SELECT *' in sql.upper():
            warnings.append("SELECT * may be inefficient")

        if not re.search(r'\bLIMIT\b|\bTOP\b|\bFETCH\s+FIRST\b', sql, re.IGNORECASE):
            warnings.append("Query has no LIMIT clause")

        is_valid = len(issues) == 0

        return ValidationResult(
            is_valid=is_valid,
            risk_level=risk_level,
            issues=issues,
            warnings=warnings,
            sanitized_query=sql if is_valid else None,
        )


class QueryAuditor:
    """
    Audit logging for SQL queries.
    """

    def __init__(self, max_entries: int = 10000):
        """
        Initialize auditor.

        Args:
            max_entries: Maximum audit entries to keep in memory.
        """
        self._entries: List[AuditEntry] = []
        self._max_entries = max_entries

    def log_query(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
        risk_level: SQLRiskLevel = SQLRiskLevel.SAFE,
        user_context: Optional[str] = None,
        source: str = "nl_to_sql",
        validated: bool = True,
    ) -> AuditEntry:
        """
        Log a SQL query.

        Args:
            sql: SQL query string.
            parameters: Query parameters.
            risk_level: Risk level of the query.
            user_context: User/session context.
            source: Source of the query.
            validated: Whether query was validated.

        Returns:
            AuditEntry for the logged query.
        """
        parameters = parameters or {}

        # Hash the query for deduplication/tracking
        query_hash = hashlib.sha256(sql.encode()).hexdigest()[:16]

        # Truncate query for preview
        query_preview = sql[:200] + "..." if len(sql) > 200 else sql

        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            query_hash=query_hash,
            query_preview=query_preview,
            parameters=parameters,
            risk_level=risk_level,
            user_context=user_context,
            source=source,
            validated=validated,
        )

        self._entries.append(entry)

        # Trim if over limit
        if len(self._entries) > self._max_entries:
            self._entries = self._entries[-self._max_entries:]

        # Log high-risk queries
        if risk_level in (SQLRiskLevel.HIGH, SQLRiskLevel.CRITICAL):
            logger.warning(
                f"High-risk query logged: {query_hash} - {risk_level.value}"
            )

        return entry

    def get_entries(
        self,
        limit: int = 100,
        risk_level: Optional[SQLRiskLevel] = None,
        source: Optional[str] = None,
    ) -> List[AuditEntry]:
        """
        Get audit entries with optional filtering.

        Args:
            limit: Maximum entries to return.
            risk_level: Filter by risk level.
            source: Filter by source.

        Returns:
            List of matching audit entries.
        """
        entries = self._entries

        if risk_level:
            entries = [e for e in entries if e.risk_level == risk_level]

        if source:
            entries = [e for e in entries if e.source == source]

        return entries[-limit:]

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get audit statistics.

        Returns:
            Dictionary with audit statistics.
        """
        if not self._entries:
            return {
                "total_queries": 0,
                "by_risk_level": {},
                "by_source": {},
                "validated_count": 0,
            }

        by_risk = {}
        by_source = {}
        validated_count = 0

        for entry in self._entries:
            by_risk[entry.risk_level.value] = by_risk.get(entry.risk_level.value, 0) + 1
            by_source[entry.source] = by_source.get(entry.source, 0) + 1
            if entry.validated:
                validated_count += 1

        return {
            "total_queries": len(self._entries),
            "by_risk_level": by_risk,
            "by_source": by_source,
            "validated_count": validated_count,
        }

    def clear(self) -> None:
        """Clear all audit entries."""
        self._entries.clear()


# Global instances
_sanitizer: Optional[SQLSanitizer] = None
_validator: Optional[QueryValidator] = None
_auditor: Optional[QueryAuditor] = None


def get_sanitizer() -> SQLSanitizer:
    """Get the global SQL sanitizer instance."""
    global _sanitizer
    if _sanitizer is None:
        _sanitizer = SQLSanitizer()
    return _sanitizer


def get_validator() -> QueryValidator:
    """Get the global query validator instance."""
    global _validator
    if _validator is None:
        _validator = QueryValidator()
    return _validator


def get_auditor() -> QueryAuditor:
    """Get the global query auditor instance."""
    global _auditor
    if _auditor is None:
        _auditor = QueryAuditor()
    return _auditor


def reset_safety_instances() -> None:
    """Reset all global instances (for testing)."""
    global _sanitizer, _validator, _auditor
    _sanitizer = None
    _validator = None
    _auditor = None
