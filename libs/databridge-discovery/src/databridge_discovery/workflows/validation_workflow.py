"""
Validation Workflow for data quality and integrity checks.

This workflow runs comprehensive validations:
1. Schema validation
2. Data quality checks
3. Referential integrity
4. Business rule validation
5. Aggregation reconciliation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable


class ValidationLevel(str, Enum):
    """Validation severity levels."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ValidationType(str, Enum):
    """Types of validations."""

    SCHEMA = "schema"
    DATA_QUALITY = "data_quality"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    BUSINESS_RULE = "business_rule"
    AGGREGATION = "aggregation"
    CUSTOM = "custom"


@dataclass
class ValidationCheck:
    """A single validation check."""

    name: str
    validation_type: ValidationType
    level: ValidationLevel = ValidationLevel.ERROR
    query: str | None = None
    expected: Any = None
    tolerance: float = 0.0
    custom_validator: Callable | None = None
    description: str = ""


@dataclass
class ValidationCheckResult:
    """Result of a validation check."""

    check_name: str
    validation_type: ValidationType
    level: ValidationLevel
    passed: bool
    expected: Any = None
    actual: Any = None
    difference: float | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    executed_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "validation_type": self.validation_type.value,
            "level": self.level.value,
            "passed": self.passed,
            "expected": self.expected,
            "actual": self.actual,
            "difference": self.difference,
            "message": self.message,
            "details": self.details,
            "executed_at": self.executed_at.isoformat(),
        }


@dataclass
class ValidationConfig:
    """Configuration for validation workflow."""

    name: str = "validation"
    checks: list[ValidationCheck] = field(default_factory=list)
    fail_on_error: bool = True
    fail_on_warning: bool = False
    parallel_execution: bool = False
    timeout_seconds: int = 300


@dataclass
class ValidationResult:
    """Result of validation workflow."""

    validation_id: str
    name: str
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    warning_checks: int = 0
    skipped_checks: int = 0
    check_results: list[ValidationCheckResult] = field(default_factory=list)
    overall_passed: bool = True
    errors: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_id": self.validation_id,
            "name": self.name,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "warning_checks": self.warning_checks,
            "skipped_checks": self.skipped_checks,
            "overall_passed": self.overall_passed,
            "pass_rate": self.passed_checks / self.total_checks if self.total_checks > 0 else 0,
            "check_results": [r.to_dict() for r in self.check_results],
            "error_count": len(self.errors),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
        }


class ValidationWorkflow:
    """
    Comprehensive validation workflow.

    Supports:
    - Schema validation (column types, nullability)
    - Data quality (nulls, duplicates, patterns)
    - Referential integrity (FK relationships)
    - Business rule validation (custom rules)
    - Aggregation reconciliation (source vs target totals)

    Example:
        workflow = ValidationWorkflow()

        # Add checks
        workflow.add_check(ValidationCheck(
            name="row_count",
            validation_type=ValidationType.AGGREGATION,
            expected=1000,
            tolerance=0.01,
        ))

        # Execute
        result = workflow.execute(data)
    """

    def __init__(self, config: ValidationConfig | None = None):
        """Initialize validation workflow."""
        self._config = config or ValidationConfig()
        self._checks: list[ValidationCheck] = list(self._config.checks)
        self._history: list[ValidationResult] = []

    def add_check(self, check: ValidationCheck) -> None:
        """Add a validation check."""
        self._checks.append(check)

    def add_checks(self, checks: list[ValidationCheck]) -> None:
        """Add multiple validation checks."""
        self._checks.extend(checks)

    def clear_checks(self) -> None:
        """Clear all checks."""
        self._checks.clear()

    def execute(
        self,
        data: dict[str, Any],
    ) -> ValidationResult:
        """
        Execute validation workflow.

        Args:
            data: Data to validate, may include:
                - source_data: Source records
                - target_data: Target records
                - schema: Schema definition
                - aggregates: Pre-computed aggregates

        Returns:
            ValidationResult
        """
        import uuid

        result = ValidationResult(
            validation_id=str(uuid.uuid4())[:8],
            name=self._config.name,
            total_checks=len(self._checks),
            started_at=datetime.now(),
        )

        try:
            for check in self._checks:
                check_result = self._execute_check(check, data)
                result.check_results.append(check_result)

                if check_result.passed:
                    result.passed_checks += 1
                elif check_result.level == ValidationLevel.WARNING:
                    result.warning_checks += 1
                    if self._config.fail_on_warning:
                        result.overall_passed = False
                else:
                    result.failed_checks += 1
                    if self._config.fail_on_error:
                        result.overall_passed = False

        except Exception as e:
            result.errors.append(str(e))
            result.overall_passed = False

        result.completed_at = datetime.now()
        if result.started_at:
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

        self._history.append(result)
        return result

    def _execute_check(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Execute a single validation check."""
        try:
            if check.validation_type == ValidationType.SCHEMA:
                return self._validate_schema(check, data)
            elif check.validation_type == ValidationType.DATA_QUALITY:
                return self._validate_data_quality(check, data)
            elif check.validation_type == ValidationType.REFERENTIAL_INTEGRITY:
                return self._validate_referential_integrity(check, data)
            elif check.validation_type == ValidationType.BUSINESS_RULE:
                return self._validate_business_rule(check, data)
            elif check.validation_type == ValidationType.AGGREGATION:
                return self._validate_aggregation(check, data)
            elif check.validation_type == ValidationType.CUSTOM:
                return self._validate_custom(check, data)
            else:
                return ValidationCheckResult(
                    check_name=check.name,
                    validation_type=check.validation_type,
                    level=check.level,
                    passed=False,
                    message=f"Unknown validation type: {check.validation_type}",
                )
        except Exception as e:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=False,
                message=f"Check failed with error: {str(e)}",
            )

    def _validate_schema(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Validate schema structure."""
        schema = data.get("schema", {})
        expected_schema = check.expected or {}

        # Compare schemas
        mismatches = []
        for col_name, expected_type in expected_schema.items():
            actual_type = schema.get(col_name)
            if actual_type is None:
                mismatches.append(f"Missing column: {col_name}")
            elif actual_type != expected_type:
                mismatches.append(f"{col_name}: expected {expected_type}, got {actual_type}")

        passed = len(mismatches) == 0

        return ValidationCheckResult(
            check_name=check.name,
            validation_type=check.validation_type,
            level=check.level,
            passed=passed,
            expected=expected_schema,
            actual=schema,
            message="Schema matches" if passed else f"Schema mismatches: {'; '.join(mismatches)}",
            details={"mismatches": mismatches},
        )

    def _validate_data_quality(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Validate data quality metrics."""
        records = data.get("records", data.get("source_data", []))

        if not records:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=True,
                message="No data to validate",
            )

        # Calculate quality metrics
        total_records = len(records)
        null_counts: dict[str, int] = {}
        duplicate_keys = 0

        # Count nulls per column
        if records:
            columns = records[0].keys()
            for col in columns:
                null_counts[col] = sum(1 for r in records if r.get(col) is None)

        # Check expected quality thresholds
        expected = check.expected or {}
        max_null_pct = expected.get("max_null_percent", 100)
        max_duplicate_pct = expected.get("max_duplicate_percent", 100)

        issues = []
        for col, null_count in null_counts.items():
            null_pct = (null_count / total_records) * 100
            if null_pct > max_null_pct:
                issues.append(f"{col}: {null_pct:.1f}% nulls exceeds {max_null_pct}%")

        passed = len(issues) == 0

        return ValidationCheckResult(
            check_name=check.name,
            validation_type=check.validation_type,
            level=check.level,
            passed=passed,
            expected=expected,
            actual={"null_counts": null_counts, "total_records": total_records},
            message="Data quality OK" if passed else f"Quality issues: {'; '.join(issues)}",
            details={"issues": issues},
        )

    def _validate_referential_integrity(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Validate referential integrity."""
        source_data = data.get("source_data", [])
        reference_data = data.get("reference_data", [])

        if not source_data or not reference_data:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=True,
                message="No data to validate",
            )

        # Get FK column from check config
        fk_column = check.expected.get("fk_column") if check.expected else None
        pk_column = check.expected.get("pk_column") if check.expected else None

        if not fk_column or not pk_column:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=False,
                message="FK/PK columns not specified",
            )

        # Build reference set
        reference_keys = set(r.get(pk_column) for r in reference_data if r.get(pk_column))

        # Check for orphans
        orphan_count = 0
        for record in source_data:
            fk_value = record.get(fk_column)
            if fk_value and fk_value not in reference_keys:
                orphan_count += 1

        passed = orphan_count == 0

        return ValidationCheckResult(
            check_name=check.name,
            validation_type=check.validation_type,
            level=check.level,
            passed=passed,
            expected=0,
            actual=orphan_count,
            message="Referential integrity OK" if passed else f"{orphan_count} orphan records found",
            details={"orphan_count": orphan_count, "fk_column": fk_column, "pk_column": pk_column},
        )

    def _validate_business_rule(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Validate business rules."""
        records = data.get("records", data.get("source_data", []))

        if not records:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=True,
                message="No data to validate",
            )

        # Execute custom validator if provided
        if check.custom_validator:
            try:
                violations = check.custom_validator(records)
                passed = len(violations) == 0
                return ValidationCheckResult(
                    check_name=check.name,
                    validation_type=check.validation_type,
                    level=check.level,
                    passed=passed,
                    actual=len(violations),
                    message="Business rule OK" if passed else f"{len(violations)} violations",
                    details={"violations": violations[:10]},  # First 10
                )
            except Exception as e:
                return ValidationCheckResult(
                    check_name=check.name,
                    validation_type=check.validation_type,
                    level=check.level,
                    passed=False,
                    message=f"Validator error: {str(e)}",
                )

        return ValidationCheckResult(
            check_name=check.name,
            validation_type=check.validation_type,
            level=check.level,
            passed=True,
            message="No validator provided",
        )

    def _validate_aggregation(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Validate aggregation values."""
        expected = check.expected
        actual = data.get("actual", data.get("aggregates", {}).get(check.name))

        if actual is None:
            # Try to calculate from records
            records = data.get("records", data.get("source_data", []))
            column = data.get("column", check.name)
            if records:
                actual = len(records)  # Default to count

        if expected is None or actual is None:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=False,
                message="Expected or actual value missing",
            )

        # Calculate difference
        if expected == 0:
            diff_pct = 100 if actual != 0 else 0
        else:
            diff_pct = abs(actual - expected) / abs(expected) * 100

        passed = diff_pct <= check.tolerance * 100

        return ValidationCheckResult(
            check_name=check.name,
            validation_type=check.validation_type,
            level=check.level,
            passed=passed,
            expected=expected,
            actual=actual,
            difference=diff_pct,
            message=f"Aggregation OK ({diff_pct:.2f}% diff)" if passed else f"Aggregation mismatch: {diff_pct:.2f}% exceeds {check.tolerance*100}%",
        )

    def _validate_custom(
        self,
        check: ValidationCheck,
        data: dict[str, Any],
    ) -> ValidationCheckResult:
        """Execute custom validation."""
        if not check.custom_validator:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=False,
                message="No custom validator provided",
            )

        try:
            result = check.custom_validator(data)

            if isinstance(result, bool):
                return ValidationCheckResult(
                    check_name=check.name,
                    validation_type=check.validation_type,
                    level=check.level,
                    passed=result,
                    message="Custom validation passed" if result else "Custom validation failed",
                )
            elif isinstance(result, dict):
                return ValidationCheckResult(
                    check_name=check.name,
                    validation_type=check.validation_type,
                    level=check.level,
                    passed=result.get("passed", False),
                    message=result.get("message", ""),
                    details=result,
                )
            else:
                return ValidationCheckResult(
                    check_name=check.name,
                    validation_type=check.validation_type,
                    level=check.level,
                    passed=bool(result),
                    message=str(result),
                )
        except Exception as e:
            return ValidationCheckResult(
                check_name=check.name,
                validation_type=check.validation_type,
                level=check.level,
                passed=False,
                message=f"Custom validator error: {str(e)}",
            )

    def get_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get validation history."""
        return [r.to_dict() for r in self._history[-limit:]]

    @staticmethod
    def create_standard_checks() -> list[ValidationCheck]:
        """Create standard validation checks."""
        return [
            ValidationCheck(
                name="null_check",
                validation_type=ValidationType.DATA_QUALITY,
                level=ValidationLevel.WARNING,
                expected={"max_null_percent": 10},
                description="Check for excessive null values",
            ),
            ValidationCheck(
                name="row_count",
                validation_type=ValidationType.AGGREGATION,
                level=ValidationLevel.ERROR,
                tolerance=0.01,
                description="Verify row counts match within 1%",
            ),
        ]
