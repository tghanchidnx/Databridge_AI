"""
Validation Runner.

Executes expectation suites and data contracts against data:
- In-memory DataFrame validation
- Database query validation
- Result aggregation and reporting
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .types import (
    ExpectationSuite,
    Expectation,
    ExpectationType,
    ExpectationResult,
    ValidationResult,
    ValidationStatus,
    DataContract,
)

logger = logging.getLogger(__name__)


class ValidationRunner:
    """Runs validations against expectation suites."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/validation_results")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._results: Dict[str, List[ValidationResult]] = {}
        self._query_func: Optional[Callable] = None

    def set_query_function(self, query_func: Callable) -> None:
        """
        Set the function used to query databases.

        Args:
            query_func: Function that takes (connection_id, sql) and returns DataFrame-like result
        """
        self._query_func = query_func

    def validate_dataframe(
        self,
        suite: ExpectationSuite,
        data: List[Dict[str, Any]],
    ) -> ValidationResult:
        """
        Validate a suite against in-memory data.

        Args:
            suite: Expectation suite to validate
            data: List of row dictionaries

        Returns:
            ValidationResult
        """
        start_time = time.time()
        results = []

        for expectation in suite.expectations:
            result = self._validate_expectation(expectation, data)
            results.append(result)

        duration = time.time() - start_time

        # Aggregate results
        success_count = sum(1 for r in results if r.success)
        failure_count = sum(1 for r in results if not r.success and not r.exception_info)
        error_count = sum(1 for r in results if r.exception_info)

        status = ValidationStatus.SUCCESS
        if failure_count > 0:
            status = ValidationStatus.FAILURE
        if error_count > 0:
            status = ValidationStatus.ERROR

        validation_result = ValidationResult(
            suite_name=suite.name,
            status=status,
            run_at=datetime.now(),
            duration_seconds=round(duration, 3),
            success_count=success_count,
            failure_count=failure_count,
            error_count=error_count,
            total_expectations=len(results),
            results=results,
            data_asset_name=suite.data_asset_name,
            row_count=len(data),
        )

        # Store result
        self._store_result(suite.name, validation_result)

        return validation_result

    def validate_database(
        self,
        suite: ExpectationSuite,
        connection_id: str,
        limit: int = 10000,
    ) -> ValidationResult:
        """
        Validate a suite against database table.

        Args:
            suite: Expectation suite to validate
            connection_id: Database connection ID
            limit: Max rows to fetch for validation

        Returns:
            ValidationResult
        """
        if not self._query_func:
            raise ValueError("Query function not set. Call set_query_function first.")

        if not suite.table_name:
            raise ValueError("Suite must have table_name set for database validation")

        # Build query
        table_ref = f"{suite.database}.{suite.schema_name}.{suite.table_name}" if suite.database else suite.table_name
        sql = f"SELECT * FROM {table_ref} LIMIT {limit}"

        # Execute query
        try:
            result = self._query_func(connection_id, sql)
            if hasattr(result, "to_dict"):
                data = result.to_dict("records")
            elif isinstance(result, list):
                data = result
            else:
                data = []
        except Exception as e:
            logger.error(f"Failed to query database: {e}")
            return ValidationResult(
                suite_name=suite.name,
                status=ValidationStatus.ERROR,
                error_count=1,
                total_expectations=len(suite.expectations),
                meta={"error": str(e)},
            )

        return self.validate_dataframe(suite, data)

    def validate_contract(
        self,
        contract: DataContract,
        data: List[Dict[str, Any]],
    ) -> ValidationResult:
        """
        Validate a data contract against data.

        Args:
            contract: Data contract to validate
            data: List of row dictionaries

        Returns:
            ValidationResult
        """
        start_time = time.time()
        results = []

        # Validate columns exist
        if data:
            sample_row = data[0]
            for col in contract.columns:
                exists = col.name in sample_row
                results.append(ExpectationResult(
                    expectation_id=f"col_exists_{col.name}",
                    expectation_type="column_exists",
                    success=exists,
                    observed_value=col.name in sample_row,
                    expected_value=True,
                ))

                if exists and col.not_null:
                    null_count = sum(1 for row in data if row.get(col.name) is None)
                    results.append(ExpectationResult(
                        expectation_id=f"not_null_{col.name}",
                        expectation_type="not_null",
                        success=null_count == 0,
                        observed_value=null_count,
                        expected_value=0,
                        unexpected_count=null_count,
                    ))

                if exists and col.unique:
                    values = [row.get(col.name) for row in data]
                    unique_count = len(set(values))
                    is_unique = unique_count == len(values)
                    results.append(ExpectationResult(
                        expectation_id=f"unique_{col.name}",
                        expectation_type="unique",
                        success=is_unique,
                        observed_value=unique_count,
                        expected_value=len(values),
                    ))

                if exists and col.pattern:
                    pattern = re.compile(col.pattern)
                    matches = sum(1 for row in data if row.get(col.name) and pattern.match(str(row.get(col.name))))
                    all_match = matches == len(data)
                    results.append(ExpectationResult(
                        expectation_id=f"pattern_{col.name}",
                        expectation_type="pattern_match",
                        success=all_match,
                        observed_value=matches,
                        expected_value=len(data),
                        unexpected_count=len(data) - matches,
                    ))

        # Validate row count
        if contract.quality.row_count_min is not None:
            results.append(ExpectationResult(
                expectation_id="row_count_min",
                expectation_type="row_count_min",
                success=len(data) >= contract.quality.row_count_min,
                observed_value=len(data),
                expected_value=contract.quality.row_count_min,
            ))

        if contract.quality.row_count_max is not None:
            results.append(ExpectationResult(
                expectation_id="row_count_max",
                expectation_type="row_count_max",
                success=len(data) <= contract.quality.row_count_max,
                observed_value=len(data),
                expected_value=contract.quality.row_count_max,
            ))

        # Validate completeness
        if contract.quality.completeness_min_percent is not None and data:
            total_cells = len(data) * len(contract.columns)
            null_cells = 0
            for row in data:
                for col in contract.columns:
                    if row.get(col.name) is None:
                        null_cells += 1
            completeness = ((total_cells - null_cells) / total_cells) * 100 if total_cells > 0 else 100
            results.append(ExpectationResult(
                expectation_id="completeness",
                expectation_type="completeness",
                success=completeness >= contract.quality.completeness_min_percent,
                observed_value=round(completeness, 2),
                expected_value=contract.quality.completeness_min_percent,
            ))

        duration = time.time() - start_time

        success_count = sum(1 for r in results if r.success)
        failure_count = sum(1 for r in results if not r.success)

        status = ValidationStatus.SUCCESS if failure_count == 0 else ValidationStatus.FAILURE

        return ValidationResult(
            suite_name=f"contract:{contract.name}",
            status=status,
            run_at=datetime.now(),
            duration_seconds=round(duration, 3),
            success_count=success_count,
            failure_count=failure_count,
            total_expectations=len(results),
            results=results,
            row_count=len(data),
        )

    def get_results(
        self,
        suite_name: str,
        limit: int = 10,
    ) -> List[ValidationResult]:
        """
        Get validation results for a suite.

        Args:
            suite_name: Name of the suite
            limit: Maximum results to return

        Returns:
            List of ValidationResult
        """
        results = self._results.get(suite_name, [])
        return results[-limit:]

    def get_latest_result(self, suite_name: str) -> Optional[ValidationResult]:
        """Get the most recent validation result."""
        results = self._results.get(suite_name, [])
        return results[-1] if results else None

    def generate_report(
        self,
        result: ValidationResult,
        format: str = "markdown",
    ) -> str:
        """
        Generate a validation report.

        Args:
            result: Validation result
            format: Report format (markdown, json)

        Returns:
            Formatted report string
        """
        if format == "json":
            return json.dumps(result.model_dump(mode="json"), indent=2, default=str)

        # Markdown format
        lines = [
            f"# Validation Report: {result.suite_name}",
            "",
            f"**Status:** {result.status.value.upper()}",
            f"**Run At:** {result.run_at.isoformat()}",
            f"**Duration:** {result.duration_seconds}s",
            f"**Row Count:** {result.row_count}",
            "",
            "## Summary",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Expectations | {result.total_expectations} |",
            f"| Passed | {result.success_count} |",
            f"| Failed | {result.failure_count} |",
            f"| Errors | {result.error_count} |",
            f"| Success Rate | {result.success_percent:.1f}% |",
            "",
        ]

        if result.failure_count > 0:
            lines.extend([
                "## Failures",
                "",
            ])
            for r in result.results:
                if not r.success:
                    lines.append(f"- **{r.expectation_type}** ({r.expectation_id})")
                    lines.append(f"  - Expected: {r.expected_value}")
                    lines.append(f"  - Observed: {r.observed_value}")
                    if r.unexpected_count:
                        lines.append(f"  - Unexpected count: {r.unexpected_count}")
                    lines.append("")

        return "\n".join(lines)

    def _validate_expectation(
        self,
        expectation: Expectation,
        data: List[Dict[str, Any]],
    ) -> ExpectationResult:
        """Validate a single expectation against data."""
        try:
            exp_type = expectation.expectation_type
            column = expectation.column
            kwargs = expectation.kwargs

            if exp_type == ExpectationType.COLUMN_TO_EXIST:
                exists = any(column in row for row in data) if data else False
                return ExpectationResult(
                    expectation_id=expectation.id,
                    expectation_type=exp_type.value,
                    success=exists,
                    observed_value=exists,
                    expected_value=True,
                )

            if exp_type == ExpectationType.NOT_NULL:
                null_count = sum(1 for row in data if row.get(column) is None)
                return ExpectationResult(
                    expectation_id=expectation.id,
                    expectation_type=exp_type.value,
                    success=null_count == 0,
                    observed_value=null_count,
                    expected_value=0,
                    unexpected_count=null_count,
                    element_count=len(data),
                )

            if exp_type == ExpectationType.UNIQUE:
                values = [row.get(column) for row in data if row.get(column) is not None]
                unique_count = len(set(values))
                is_unique = unique_count == len(values)
                return ExpectationResult(
                    expectation_id=expectation.id,
                    expectation_type=exp_type.value,
                    success=is_unique,
                    observed_value=unique_count,
                    expected_value=len(values),
                    unexpected_count=len(values) - unique_count,
                    element_count=len(values),
                )

            if exp_type == ExpectationType.IN_SET:
                value_set = set(kwargs.get("value_set", []))
                values = [row.get(column) for row in data if row.get(column) is not None]
                unexpected = [v for v in values if v not in value_set]
                return ExpectationResult(
                    expectation_id=expectation.id,
                    expectation_type=exp_type.value,
                    success=len(unexpected) == 0,
                    observed_value=len(values) - len(unexpected),
                    expected_value=len(values),
                    unexpected_count=len(unexpected),
                    unexpected_values=unexpected[:10],
                    element_count=len(values),
                )

            if exp_type == ExpectationType.MATCH_REGEX:
                regex = kwargs.get("regex", ".*")
                pattern = re.compile(regex)
                values = [row.get(column) for row in data if row.get(column) is not None]
                unexpected = [v for v in values if not pattern.match(str(v))]
                return ExpectationResult(
                    expectation_id=expectation.id,
                    expectation_type=exp_type.value,
                    success=len(unexpected) == 0,
                    observed_value=len(values) - len(unexpected),
                    expected_value=len(values),
                    unexpected_count=len(unexpected),
                    unexpected_values=unexpected[:10],
                    element_count=len(values),
                )

            if exp_type == ExpectationType.ROW_COUNT_BETWEEN:
                min_val = kwargs.get("min_value", 0)
                max_val = kwargs.get("max_value", float("inf"))
                row_count = len(data)
                return ExpectationResult(
                    expectation_id=expectation.id,
                    expectation_type=exp_type.value,
                    success=min_val <= row_count <= max_val,
                    observed_value=row_count,
                    expected_value=f"{min_val}-{max_val}",
                    element_count=row_count,
                )

            # Default: unsupported expectation type
            return ExpectationResult(
                expectation_id=expectation.id,
                expectation_type=exp_type.value,
                success=True,
                exception_info={"message": f"Unsupported expectation type: {exp_type}"},
            )

        except Exception as e:
            return ExpectationResult(
                expectation_id=expectation.id,
                expectation_type=expectation.expectation_type.value,
                success=False,
                exception_info={"message": str(e), "type": type(e).__name__},
            )

    def _store_result(self, suite_name: str, result: ValidationResult) -> None:
        """Store a validation result."""
        if suite_name not in self._results:
            self._results[suite_name] = []

        self._results[suite_name].append(result)

        # Keep only last 100 results per suite
        if len(self._results[suite_name]) > 100:
            self._results[suite_name] = self._results[suite_name][-100:]

        # Persist to disk
        self._save_result(result)

    def _save_result(self, result: ValidationResult) -> None:
        """Save result to disk."""
        result_file = self.output_dir / f"{result.suite_name}_{result.id}.json"
        result_file.write_text(json.dumps(result.model_dump(mode="json"), indent=2, default=str))
