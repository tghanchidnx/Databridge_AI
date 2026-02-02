"""Unit tests for SQL safety module."""

import pytest
from datetime import datetime, timezone

from src.query.safety import (
    SQLSanitizer,
    QueryValidator,
    QueryAuditor,
    SQLRiskLevel,
    ValidationResult,
    ValidationError,
    AuditEntry,
    get_sanitizer,
    get_validator,
    get_auditor,
    reset_safety_instances,
)


class TestSQLRiskLevel:
    """Tests for SQLRiskLevel enum."""

    def test_risk_levels_exist(self):
        """Test all risk levels exist."""
        assert SQLRiskLevel.SAFE.value == "safe"
        assert SQLRiskLevel.LOW.value == "low"
        assert SQLRiskLevel.MEDIUM.value == "medium"
        assert SQLRiskLevel.HIGH.value == "high"
        assert SQLRiskLevel.CRITICAL.value == "critical"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """Test validation result creation."""
        result = ValidationResult(
            is_valid=True,
            risk_level=SQLRiskLevel.SAFE,
        )

        assert result.is_valid is True
        assert result.risk_level == SQLRiskLevel.SAFE
        assert result.issues == []
        assert result.warnings == []

    def test_validation_result_with_issues(self):
        """Test validation result with issues."""
        result = ValidationResult(
            is_valid=False,
            risk_level=SQLRiskLevel.HIGH,
            issues=["SQL injection detected"],
            warnings=["Query has no LIMIT"],
        )

        assert result.is_valid is False
        assert len(result.issues) == 1
        assert len(result.warnings) == 1

    def test_validation_result_to_dict(self):
        """Test validation result serialization."""
        result = ValidationResult(
            is_valid=True,
            risk_level=SQLRiskLevel.SAFE,
            warnings=["Consider adding index"],
        )
        data = result.to_dict()

        assert data["is_valid"] is True
        assert data["risk_level"] == "safe"
        assert len(data["warnings"]) == 1


class TestSQLSanitizer:
    """Tests for SQLSanitizer class."""

    def setup_method(self):
        """Reset instances before each test."""
        reset_safety_instances()

    def test_sanitizer_initialization(self):
        """Test sanitizer initializes correctly."""
        sanitizer = SQLSanitizer()
        assert sanitizer.strict_mode is True

    def test_sanitize_identifier_valid(self):
        """Test valid identifier sanitization."""
        sanitizer = SQLSanitizer()

        assert sanitizer.sanitize_identifier("column_name") == "column_name"
        assert sanitizer.sanitize_identifier("TableName") == "TableName"
        assert sanitizer.sanitize_identifier("schema.table") == "schema.table"
        assert sanitizer.sanitize_identifier("db.schema.table") == "db.schema.table"

    def test_sanitize_identifier_invalid(self):
        """Test invalid identifier rejection."""
        sanitizer = SQLSanitizer()

        with pytest.raises(ValidationError):
            sanitizer.sanitize_identifier("")

        with pytest.raises(ValidationError):
            sanitizer.sanitize_identifier("1column")  # Starts with number

        with pytest.raises(ValidationError):
            sanitizer.sanitize_identifier("column-name")  # Contains hyphen

        with pytest.raises(ValidationError):
            sanitizer.sanitize_identifier("column name")  # Contains space

    def test_sanitize_identifier_reserved_word(self):
        """Test reserved word rejection."""
        sanitizer = SQLSanitizer()

        with pytest.raises(ValidationError):
            sanitizer.sanitize_identifier("SELECT")

        with pytest.raises(ValidationError):
            sanitizer.sanitize_identifier("DROP")

    def test_sanitize_value_null(self):
        """Test null value sanitization."""
        sanitizer = SQLSanitizer()

        value, value_type = sanitizer.sanitize_value(None)
        assert value is None
        assert value_type == "null"

    def test_sanitize_value_boolean(self):
        """Test boolean value sanitization."""
        sanitizer = SQLSanitizer()

        value, value_type = sanitizer.sanitize_value(True)
        assert value is True
        assert value_type == "boolean"

    def test_sanitize_value_numeric(self):
        """Test numeric value sanitization."""
        sanitizer = SQLSanitizer()

        value, value_type = sanitizer.sanitize_value(42)
        assert value == 42
        assert value_type == "numeric"

        value, value_type = sanitizer.sanitize_value(3.14)
        assert value == 3.14
        assert value_type == "numeric"

    def test_sanitize_value_string(self):
        """Test string value sanitization."""
        sanitizer = SQLSanitizer()

        value, value_type = sanitizer.sanitize_value("hello")
        assert value == "hello"
        assert value_type == "string"

    def test_sanitize_value_dangerous_string(self):
        """Test dangerous string value rejection."""
        sanitizer = SQLSanitizer()

        with pytest.raises(ValidationError):
            sanitizer.sanitize_value("; DROP TABLE users")

        with pytest.raises(ValidationError):
            sanitizer.sanitize_value("' OR 1=1 --")

        with pytest.raises(ValidationError):
            sanitizer.sanitize_value("UNION SELECT * FROM passwords")

    def test_sanitize_value_array(self):
        """Test array value sanitization."""
        sanitizer = SQLSanitizer()

        value, value_type = sanitizer.sanitize_value([1, 2, 3])
        assert value == [1, 2, 3]
        assert value_type == "array"

    def test_validate_year_valid(self):
        """Test valid year validation."""
        sanitizer = SQLSanitizer()

        assert sanitizer.validate_year(2024) == 2024
        assert sanitizer.validate_year("2024") == 2024
        assert sanitizer.validate_year(1950) == 1950

    def test_validate_year_invalid(self):
        """Test invalid year rejection."""
        sanitizer = SQLSanitizer()

        with pytest.raises(ValidationError):
            sanitizer.validate_year("not a year")

        with pytest.raises(ValidationError):
            sanitizer.validate_year(1800)  # Too old

        with pytest.raises(ValidationError):
            sanitizer.validate_year(2200)  # Too far future

    def test_validate_month_valid(self):
        """Test valid month validation."""
        sanitizer = SQLSanitizer()

        assert sanitizer.validate_month(1) == 1
        assert sanitizer.validate_month(12) == 12
        assert sanitizer.validate_month("6") == 6

    def test_validate_month_invalid(self):
        """Test invalid month rejection."""
        sanitizer = SQLSanitizer()

        with pytest.raises(ValidationError):
            sanitizer.validate_month(0)

        with pytest.raises(ValidationError):
            sanitizer.validate_month(13)

        with pytest.raises(ValidationError):
            sanitizer.validate_month("not a month")

    def test_validate_limit_valid(self):
        """Test valid limit validation."""
        sanitizer = SQLSanitizer()

        assert sanitizer.validate_limit(100) == 100
        assert sanitizer.validate_limit("50") == 50
        assert sanitizer.validate_limit(0) == 0

    def test_validate_limit_invalid(self):
        """Test invalid limit rejection."""
        sanitizer = SQLSanitizer()

        with pytest.raises(ValidationError):
            sanitizer.validate_limit(-1)

        with pytest.raises(ValidationError):
            sanitizer.validate_limit(20000)  # Exceeds default max

        with pytest.raises(ValidationError):
            sanitizer.validate_limit("not a number")


class TestQueryValidator:
    """Tests for QueryValidator class."""

    def setup_method(self):
        """Reset instances before each test."""
        reset_safety_instances()

    def test_validator_initialization(self):
        """Test validator initializes correctly."""
        validator = QueryValidator()

        assert validator.max_joins == 10
        assert validator.max_subqueries == 5
        assert validator.max_unions == 5

    def test_validate_simple_query(self):
        """Test validating a simple safe query."""
        validator = QueryValidator()

        result = validator.validate(
            "SELECT id, name FROM users WHERE active = :active LIMIT 100"
        )

        assert result.is_valid is True
        assert result.risk_level == SQLRiskLevel.SAFE

    def test_validate_query_with_injection(self):
        """Test detecting SQL injection patterns."""
        validator = QueryValidator()

        result = validator.validate(
            "SELECT * FROM users; DROP TABLE users"
        )

        assert result.is_valid is False
        assert result.risk_level == SQLRiskLevel.CRITICAL

    def test_validate_query_too_many_joins(self):
        """Test detecting too many JOINs."""
        validator = QueryValidator(max_joins=2)

        sql = """
        SELECT * FROM a
        JOIN b ON a.id = b.a_id
        JOIN c ON b.id = c.b_id
        JOIN d ON c.id = d.c_id
        """

        result = validator.validate(sql)

        assert result.is_valid is False
        assert "Too many JOINs" in result.issues[0]

    def test_validate_query_too_many_subqueries(self):
        """Test detecting too many subqueries."""
        validator = QueryValidator(max_subqueries=1)

        sql = """
        SELECT * FROM (SELECT * FROM a) x
        WHERE id IN (SELECT id FROM b)
        AND name IN (SELECT name FROM c)
        """

        result = validator.validate(sql)

        assert result.is_valid is False
        assert "Too many subqueries" in result.issues[0]

    def test_validate_query_too_long(self):
        """Test detecting query that's too long."""
        validator = QueryValidator(max_query_length=100)

        sql = "SELECT " + ", ".join([f"col{i}" for i in range(100)]) + " FROM table"

        result = validator.validate(sql)

        assert result.is_valid is False
        assert "exceeds maximum length" in result.issues[0]

    def test_validate_query_with_warnings(self):
        """Test query validation warnings."""
        validator = QueryValidator()

        result = validator.validate("SELECT * FROM users")

        assert result.is_valid is True
        assert "SELECT * may be inefficient" in result.warnings
        assert "no LIMIT clause" in result.warnings[1]

    def test_validate_parameters(self):
        """Test parameter validation."""
        validator = QueryValidator()

        result = validator.validate(
            "SELECT * FROM users WHERE id = :id LIMIT 10",
            parameters={"id": "; DROP TABLE users"}
        )

        assert result.is_valid is False
        assert result.risk_level == SQLRiskLevel.CRITICAL


class TestQueryAuditor:
    """Tests for QueryAuditor class."""

    def setup_method(self):
        """Reset instances before each test."""
        reset_safety_instances()

    def test_auditor_initialization(self):
        """Test auditor initializes correctly."""
        auditor = QueryAuditor()
        assert len(auditor._entries) == 0

    def test_log_query(self):
        """Test logging a query."""
        auditor = QueryAuditor()

        entry = auditor.log_query(
            sql="SELECT * FROM users",
            parameters={"id": 123},
            risk_level=SQLRiskLevel.SAFE,
        )

        assert entry.query_hash is not None
        assert entry.risk_level == SQLRiskLevel.SAFE
        assert len(auditor._entries) == 1

    def test_log_query_truncates_preview(self):
        """Test query preview truncation."""
        auditor = QueryAuditor()

        long_sql = "SELECT " + "x" * 300
        entry = auditor.log_query(sql=long_sql)

        assert len(entry.query_preview) <= 203  # 200 + "..."
        assert entry.query_preview.endswith("...")

    def test_get_entries(self):
        """Test getting audit entries."""
        auditor = QueryAuditor()

        auditor.log_query("SELECT 1", risk_level=SQLRiskLevel.SAFE)
        auditor.log_query("SELECT 2", risk_level=SQLRiskLevel.HIGH)
        auditor.log_query("SELECT 3", risk_level=SQLRiskLevel.SAFE)

        entries = auditor.get_entries()
        assert len(entries) == 3

        # Filter by risk level
        high_risk = auditor.get_entries(risk_level=SQLRiskLevel.HIGH)
        assert len(high_risk) == 1

    def test_get_entries_limited(self):
        """Test getting limited audit entries."""
        auditor = QueryAuditor()

        for i in range(10):
            auditor.log_query(f"SELECT {i}")

        entries = auditor.get_entries(limit=5)
        assert len(entries) == 5

    def test_get_statistics(self):
        """Test getting audit statistics."""
        auditor = QueryAuditor()

        auditor.log_query("SELECT 1", risk_level=SQLRiskLevel.SAFE, source="api")
        auditor.log_query("SELECT 2", risk_level=SQLRiskLevel.HIGH, source="nl_to_sql")
        auditor.log_query("SELECT 3", risk_level=SQLRiskLevel.SAFE, source="api")

        stats = auditor.get_statistics()

        assert stats["total_queries"] == 3
        assert stats["by_risk_level"]["safe"] == 2
        assert stats["by_risk_level"]["high"] == 1
        assert stats["by_source"]["api"] == 2
        assert stats["by_source"]["nl_to_sql"] == 1

    def test_clear_entries(self):
        """Test clearing audit entries."""
        auditor = QueryAuditor()

        auditor.log_query("SELECT 1")
        auditor.log_query("SELECT 2")

        auditor.clear()

        assert len(auditor._entries) == 0

    def test_max_entries_limit(self):
        """Test that entries are trimmed at max."""
        auditor = QueryAuditor(max_entries=5)

        for i in range(10):
            auditor.log_query(f"SELECT {i}")

        assert len(auditor._entries) == 5


class TestAuditEntry:
    """Tests for AuditEntry dataclass."""

    def test_audit_entry_creation(self):
        """Test audit entry creation."""
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            query_hash="abc123",
            query_preview="SELECT *",
            parameters={"id": 1},
            risk_level=SQLRiskLevel.SAFE,
        )

        assert entry.query_hash == "abc123"
        assert entry.validated is True  # Default

    def test_audit_entry_to_dict(self):
        """Test audit entry serialization."""
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            query_hash="abc123",
            query_preview="SELECT *",
            parameters={"id": 1},
            risk_level=SQLRiskLevel.SAFE,
            source="test",
        )
        data = entry.to_dict()

        assert data["query_hash"] == "abc123"
        assert data["risk_level"] == "safe"
        assert data["source"] == "test"
        assert "timestamp" in data


class TestGlobalInstances:
    """Tests for global instance functions."""

    def setup_method(self):
        """Reset instances before each test."""
        reset_safety_instances()

    def teardown_method(self):
        """Clean up after each test."""
        reset_safety_instances()

    def test_get_sanitizer_creates_instance(self):
        """Test get_sanitizer creates singleton."""
        sanitizer = get_sanitizer()
        assert sanitizer is not None
        assert isinstance(sanitizer, SQLSanitizer)

    def test_get_sanitizer_returns_same_instance(self):
        """Test get_sanitizer returns same instance."""
        s1 = get_sanitizer()
        s2 = get_sanitizer()
        assert s1 is s2

    def test_get_validator_creates_instance(self):
        """Test get_validator creates singleton."""
        validator = get_validator()
        assert validator is not None
        assert isinstance(validator, QueryValidator)

    def test_get_validator_returns_same_instance(self):
        """Test get_validator returns same instance."""
        v1 = get_validator()
        v2 = get_validator()
        assert v1 is v2

    def test_get_auditor_creates_instance(self):
        """Test get_auditor creates singleton."""
        auditor = get_auditor()
        assert auditor is not None
        assert isinstance(auditor, QueryAuditor)

    def test_get_auditor_returns_same_instance(self):
        """Test get_auditor returns same instance."""
        a1 = get_auditor()
        a2 = get_auditor()
        assert a1 is a2

    def test_reset_safety_instances(self):
        """Test reset_safety_instances clears all."""
        s1 = get_sanitizer()
        v1 = get_validator()
        a1 = get_auditor()

        reset_safety_instances()

        s2 = get_sanitizer()
        v2 = get_validator()
        a2 = get_auditor()

        assert s1 is not s2
        assert v1 is not v2
        assert a1 is not a2


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_validation_error_creation(self):
        """Test validation error creation."""
        error = ValidationError("Test error")
        assert str(error) == "Test error"
        assert error.risk_level == SQLRiskLevel.HIGH  # Default

    def test_validation_error_with_risk_level(self):
        """Test validation error with custom risk level."""
        error = ValidationError("Test error", SQLRiskLevel.CRITICAL)
        assert error.risk_level == SQLRiskLevel.CRITICAL
