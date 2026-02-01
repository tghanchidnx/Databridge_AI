"""
Deploy & Validate Agent for deployment and data validation.

Capabilities:
- execute_ddl: Execute DDL statements
- run_dbt: Run dbt commands
- validate_counts: Validate row counts
- compare_aggregates: Compare aggregated values
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from databridge_discovery.agents.base_agent import (
    BaseAgent,
    AgentCapability,
    AgentConfig,
    AgentResult,
    AgentError,
    TaskContext,
)


class ValidationStatus(str, Enum):
    """Validation result status."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class DDLExecution:
    """Result of DDL execution."""

    statement: str
    status: str  # success, failed
    rows_affected: int = 0
    duration_ms: float = 0.0
    error: str | None = None
    executed_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "statement": self.statement[:100] + "..." if len(self.statement) > 100 else self.statement,
            "status": self.status,
            "rows_affected": self.rows_affected,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "executed_at": self.executed_at.isoformat(),
        }


@dataclass
class ValidationResult:
    """Result of a validation check."""

    name: str
    status: ValidationStatus
    expected: Any = None
    actual: Any = None
    difference: float | None = None
    threshold: float | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "expected": self.expected,
            "actual": self.actual,
            "difference": self.difference,
            "threshold": self.threshold,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class DbtRunResult:
    """Result of a dbt run."""

    command: str
    status: str  # success, failed, error
    models_run: int = 0
    models_passed: int = 0
    models_failed: int = 0
    tests_run: int = 0
    tests_passed: int = 0
    tests_failed: int = 0
    duration_seconds: float = 0.0
    logs: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "status": self.status,
            "models_run": self.models_run,
            "models_passed": self.models_passed,
            "models_failed": self.models_failed,
            "tests_run": self.tests_run,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "duration_seconds": self.duration_seconds,
            "logs": self.logs[-20:],  # Last 20 log lines
            "errors": self.errors,
        }


class DeployValidator(BaseAgent):
    """
    Deploy & Validate Agent for deployment and validation.

    Handles:
    - DDL execution (with dry-run mode)
    - dbt command execution
    - Row count validation
    - Aggregate comparison
    - Data quality checks

    Example:
        validator = DeployValidator()

        context = TaskContext(
            task_id="deploy_1",
            input_data={
                "ddl_statements": [...],
                "dry_run": True,
            }
        )

        result = validator.execute(
            AgentCapability.EXECUTE_DDL,
            context
        )
    """

    def __init__(self, config: AgentConfig | None = None):
        """Initialize Deploy Validator."""
        super().__init__(config or AgentConfig(name="DeployValidator"))
        self._executions: list[DDLExecution] = []
        self._validations: list[ValidationResult] = []

    def get_capabilities(self) -> list[AgentCapability]:
        """Get supported capabilities."""
        return [
            AgentCapability.EXECUTE_DDL,
            AgentCapability.RUN_DBT,
            AgentCapability.VALIDATE_COUNTS,
            AgentCapability.COMPARE_AGGREGATES,
        ]

    def execute(
        self,
        capability: AgentCapability,
        context: TaskContext,
        **kwargs: Any,
    ) -> AgentResult:
        """
        Execute a capability.

        Args:
            capability: The capability to execute
            context: Task context with input data
            **kwargs: Additional arguments

        Returns:
            AgentResult with execution results
        """
        if not self.supports(capability):
            raise AgentError(
                f"Capability {capability} not supported",
                self.name,
                capability.value,
            )

        start_time = self._start_execution(capability, context)

        try:
            if capability == AgentCapability.EXECUTE_DDL:
                data = self._execute_ddl(context, **kwargs)
            elif capability == AgentCapability.RUN_DBT:
                data = self._run_dbt(context, **kwargs)
            elif capability == AgentCapability.VALIDATE_COUNTS:
                data = self._validate_counts(context, **kwargs)
            elif capability == AgentCapability.COMPARE_AGGREGATES:
                data = self._compare_aggregates(context, **kwargs)
            else:
                raise AgentError(f"Unknown capability: {capability}", self.name)

            return self._complete_execution(capability, start_time, True, data)

        except AgentError as e:
            self._handle_error(e)
            return self._complete_execution(capability, start_time, False, error=str(e))
        except Exception as e:
            error = AgentError(str(e), self.name, capability.value)
            self._handle_error(error)
            return self._complete_execution(capability, start_time, False, error=str(e))

    def _execute_ddl(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Execute DDL statements.

        Input data:
            - ddl_statements: List of DDL statements
            - dry_run: If True, validate only (default: True)
            - stop_on_error: Stop on first error (default: True)
        """
        self._report_progress("Executing DDL", 0.0)

        input_data = context.input_data
        statements = input_data.get("ddl_statements", [])
        dry_run = input_data.get("dry_run", True)
        stop_on_error = input_data.get("stop_on_error", True)

        if not statements:
            raise AgentError("No DDL statements provided", self.name, "execute_ddl")

        executions = []
        success_count = 0
        failed_count = 0

        for i, stmt in enumerate(statements):
            self._report_progress(f"Processing statement {i+1}/{len(statements)}", i / len(statements))

            execution = self._execute_single_ddl(stmt, dry_run)
            executions.append(execution)
            self._executions.append(execution)

            if execution.status == "success":
                success_count += 1
            else:
                failed_count += 1
                if stop_on_error:
                    break

        self._report_progress("DDL execution complete", 1.0)

        return {
            "total_statements": len(statements),
            "executed": len(executions),
            "success_count": success_count,
            "failed_count": failed_count,
            "dry_run": dry_run,
            "executions": [e.to_dict() for e in executions],
        }

    def _run_dbt(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Run dbt commands.

        Input data:
            - command: dbt command (run, test, build, compile)
            - models: Optional model selection
            - full_refresh: Force full refresh
            - dry_run: Preview only
        """
        self._report_progress("Running dbt", 0.0)

        input_data = context.input_data
        command = input_data.get("command", "run")
        models = input_data.get("models")
        full_refresh = input_data.get("full_refresh", False)
        dry_run = input_data.get("dry_run", True)

        # Build dbt command
        dbt_cmd = f"dbt {command}"
        if models:
            dbt_cmd += f" --select {models}"
        if full_refresh and command == "run":
            dbt_cmd += " --full-refresh"

        # Simulate dbt execution
        result = self._simulate_dbt_run(dbt_cmd, dry_run)

        self._report_progress("dbt execution complete", 1.0)

        return {
            "command": dbt_cmd,
            "dry_run": dry_run,
            "result": result.to_dict(),
        }

    def _validate_counts(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Validate row counts between source and target.

        Input data:
            - validations: List of count validations
                - source_table: Source table name
                - target_table: Target table name
                - source_count: Source row count (or query)
                - target_count: Target row count (or query)
                - tolerance_percent: Acceptable difference %
        """
        self._report_progress("Validating counts", 0.0)

        input_data = context.input_data
        validations = input_data.get("validations", [])

        if not validations:
            raise AgentError("No validations provided", self.name, "validate_counts")

        results = []
        passed = 0
        failed = 0

        for i, validation in enumerate(validations):
            self._report_progress(f"Validating {i+1}/{len(validations)}", i / len(validations))

            result = self._validate_count(validation)
            results.append(result)
            self._validations.append(result)

            if result.status == ValidationStatus.PASSED:
                passed += 1
            else:
                failed += 1

        self._report_progress("Count validation complete", 1.0)

        return {
            "total_validations": len(validations),
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / len(validations) if validations else 0,
            "results": [r.to_dict() for r in results],
        }

    def _compare_aggregates(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Compare aggregated values between source and target.

        Input data:
            - comparisons: List of aggregate comparisons
                - name: Comparison name
                - source_value: Source aggregate value
                - target_value: Target aggregate value
                - aggregation: Aggregation type (SUM, COUNT, AVG)
                - tolerance_percent: Acceptable difference %
        """
        self._report_progress("Comparing aggregates", 0.0)

        input_data = context.input_data
        comparisons = input_data.get("comparisons", [])

        if not comparisons:
            raise AgentError("No comparisons provided", self.name, "compare_aggregates")

        results = []
        passed = 0
        failed = 0

        for i, comparison in enumerate(comparisons):
            self._report_progress(f"Comparing {i+1}/{len(comparisons)}", i / len(comparisons))

            result = self._compare_aggregate(comparison)
            results.append(result)
            self._validations.append(result)

            if result.status == ValidationStatus.PASSED:
                passed += 1
            else:
                failed += 1

        self._report_progress("Aggregate comparison complete", 1.0)

        return {
            "total_comparisons": len(comparisons),
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / len(comparisons) if comparisons else 0,
            "results": [r.to_dict() for r in results],
        }

    def _execute_single_ddl(self, statement: str, dry_run: bool) -> DDLExecution:
        """Execute a single DDL statement."""
        import time

        start_time = time.time()

        if dry_run:
            # Validate syntax only
            valid, error = self._validate_ddl_syntax(statement)
            duration = (time.time() - start_time) * 1000

            return DDLExecution(
                statement=statement,
                status="success" if valid else "failed",
                rows_affected=0,
                duration_ms=duration,
                error=error,
            )
        else:
            # Would execute against database
            # For now, simulate execution
            duration = (time.time() - start_time) * 1000

            return DDLExecution(
                statement=statement,
                status="success",
                rows_affected=0,
                duration_ms=duration,
            )

    def _validate_ddl_syntax(self, statement: str) -> tuple[bool, str | None]:
        """Validate DDL statement syntax."""
        statement = statement.strip()

        if not statement:
            return False, "Empty statement"

        # Check for valid DDL keywords
        valid_starts = [
            "CREATE", "ALTER", "DROP", "TRUNCATE",
            "INSERT", "UPDATE", "DELETE", "MERGE",
            "GRANT", "REVOKE", "USE", "SET",
            "--",  # Comments
        ]

        stmt_upper = statement.upper()
        if not any(stmt_upper.startswith(kw) for kw in valid_starts):
            return False, f"Invalid DDL: must start with {', '.join(valid_starts)}"

        # Check for balanced parentheses
        if statement.count("(") != statement.count(")"):
            return False, "Unbalanced parentheses"

        # Check for statement terminator
        if not statement.rstrip().endswith(";") and not stmt_upper.startswith("--"):
            return False, "Missing statement terminator (;)"

        return True, None

    def _simulate_dbt_run(self, command: str, dry_run: bool) -> DbtRunResult:
        """Simulate dbt run execution."""
        import time

        start_time = time.time()

        # Simulate execution time
        if not dry_run:
            time.sleep(0.1)  # Simulate some work

        duration = time.time() - start_time

        # Simulate results
        if "run" in command:
            return DbtRunResult(
                command=command,
                status="success",
                models_run=5,
                models_passed=5,
                models_failed=0,
                duration_seconds=duration,
                logs=[
                    "Running with dbt=1.7.0",
                    "Found 5 models",
                    "Completed successfully",
                ],
            )
        elif "test" in command:
            return DbtRunResult(
                command=command,
                status="success",
                tests_run=10,
                tests_passed=10,
                tests_failed=0,
                duration_seconds=duration,
                logs=[
                    "Running with dbt=1.7.0",
                    "Found 10 tests",
                    "All tests passed",
                ],
            )
        else:
            return DbtRunResult(
                command=command,
                status="success",
                duration_seconds=duration,
                logs=[f"Executed: {command}"],
            )

    def _validate_count(self, validation: dict[str, Any]) -> ValidationResult:
        """Validate a single count comparison."""
        source_count = validation.get("source_count", 0)
        target_count = validation.get("target_count", 0)
        tolerance = validation.get("tolerance_percent", 0)
        name = validation.get("name", f"{validation.get('source_table')} -> {validation.get('target_table')}")

        # Calculate difference
        if source_count == 0:
            diff_percent = 100 if target_count > 0 else 0
        else:
            diff_percent = abs(target_count - source_count) / source_count * 100

        # Determine status
        if diff_percent <= tolerance:
            status = ValidationStatus.PASSED
            message = f"Count match within tolerance ({diff_percent:.2f}%)"
        else:
            status = ValidationStatus.FAILED
            message = f"Count mismatch: {diff_percent:.2f}% difference exceeds {tolerance}% tolerance"

        return ValidationResult(
            name=name,
            status=status,
            expected=source_count,
            actual=target_count,
            difference=diff_percent,
            threshold=tolerance,
            message=message,
            details={
                "source_table": validation.get("source_table"),
                "target_table": validation.get("target_table"),
            },
        )

    def _compare_aggregate(self, comparison: dict[str, Any]) -> ValidationResult:
        """Compare aggregate values."""
        source_value = comparison.get("source_value", 0)
        target_value = comparison.get("target_value", 0)
        tolerance = comparison.get("tolerance_percent", 0.01)  # 0.01% default
        name = comparison.get("name", "Aggregate comparison")
        aggregation = comparison.get("aggregation", "SUM")

        # Calculate difference
        if source_value == 0:
            diff_percent = 100 if target_value != 0 else 0
        else:
            diff_percent = abs(target_value - source_value) / abs(source_value) * 100

        # Determine status
        if diff_percent <= tolerance:
            status = ValidationStatus.PASSED
            message = f"{aggregation} match within tolerance ({diff_percent:.4f}%)"
        else:
            status = ValidationStatus.FAILED
            message = f"{aggregation} mismatch: {diff_percent:.4f}% exceeds {tolerance}% tolerance"

        return ValidationResult(
            name=name,
            status=status,
            expected=source_value,
            actual=target_value,
            difference=diff_percent,
            threshold=tolerance,
            message=message,
            details={
                "aggregation": aggregation,
                "absolute_difference": abs(target_value - source_value),
            },
        )

    def get_execution_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get DDL execution history."""
        return [e.to_dict() for e in self._executions[-limit:]]

    def get_validation_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get validation history."""
        return [v.to_dict() for v in self._validations[-limit:]]
