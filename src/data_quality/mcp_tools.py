"""
MCP Tools for Data Quality / Expectations Integration.

Provides 7 tools for data quality validation:
- generate_expectation_suite
- add_column_expectation
- create_data_contract
- run_validation
- get_validation_results
- list_expectation_suites
- export_data_contract
"""

import json
import logging
from typing import Any, Dict, List, Optional

from .types import (
    ExpectationType,
    SeverityLevel,
)
from .suite_generator import ExpectationSuiteGenerator
from .contract_generator import DataContractGenerator
from .validation_runner import ValidationRunner

logger = logging.getLogger(__name__)


def register_data_quality_tools(mcp, settings=None) -> Dict[str, Any]:
    """
    Register Data Quality MCP tools.

    Args:
        mcp: The FastMCP instance
        settings: Optional settings

    Returns:
        Dict with registration info
    """

    # Initialize components
    suite_gen = ExpectationSuiteGenerator()
    contract_gen = DataContractGenerator()
    validator = ValidationRunner()

    @mcp.tool()
    def generate_expectation_suite(
        name: str,
        hierarchy_project_id: Optional[str] = None,
        mappings: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate an expectation suite from hierarchy mappings or configuration.

        Creates a suite of data quality expectations that can be validated against
        source data. Expectations are derived from hierarchy mappings (source_column,
        source_uid patterns) or can be configured manually.

        Args:
            name: Suite name (e.g., "gl_accounts_suite")
            hierarchy_project_id: Source hierarchy project ID (optional)
            mappings: JSON string of hierarchy mappings (optional)
            database: Target database name
            schema_name: Target schema name
            table_name: Target table name
            description: Suite description

        Returns:
            Generated suite details

        Example:
            generate_expectation_suite(
                name="gl_accounts_suite",
                database="ANALYTICS",
                schema_name="FINANCE",
                table_name="GL_ACCOUNTS"
            )
        """
        try:
            # Create or get suite
            existing = suite_gen.get_suite(name)
            if existing:
                return {
                    "success": False,
                    "error": f"Suite '{name}' already exists. Use a different name.",
                }

            suite = suite_gen.create_suite(
                name=name,
                description=description,
                database=database,
                schema_name=schema_name,
                table_name=table_name,
                hierarchy_project_id=hierarchy_project_id,
            )

            # Generate from mappings if provided
            if mappings:
                mapping_data = json.loads(mappings)
                suite = suite_gen.generate_from_hierarchy(
                    suite_name=name,
                    hierarchy_nodes=[],
                    mappings=mapping_data,
                )

            return {
                "success": True,
                "suite_id": suite.id,
                "suite_name": suite.name,
                "expectations_count": len(suite.expectations),
                "data_asset": suite.data_asset_name,
                "message": f"Created expectation suite '{name}' with {len(suite.expectations)} expectations",
            }

        except Exception as e:
            logger.error(f"Failed to generate suite: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def add_column_expectation(
        suite_name: str,
        column: str,
        expectation_type: str,
        value_set: Optional[str] = None,
        regex: Optional[str] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: str = "medium",
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add a column expectation to a suite.

        Adds a specific data quality expectation for a column.

        Available expectation types:
        - not_null: Column values should not be null
        - unique: Column values should be unique
        - in_set: Values should be in specified set
        - match_regex: Values should match regex pattern
        - between: Values should be between min and max

        Args:
            suite_name: Name of the suite
            column: Column name
            expectation_type: Type of expectation (not_null, unique, in_set, match_regex, between)
            value_set: JSON array of allowed values (for in_set)
            regex: Regex pattern (for match_regex)
            min_value: Minimum value (for between)
            max_value: Maximum value (for between)
            severity: Failure severity (critical, high, medium, low, info)
            description: Human-readable description

        Returns:
            Added expectation details

        Example:
            add_column_expectation(
                suite_name="gl_accounts_suite",
                column="ACCOUNT_CODE",
                expectation_type="match_regex",
                regex="^[4-9][0-9]{3}$",
                severity="high"
            )
        """
        try:
            # Map string to enum
            type_map = {
                "not_null": ExpectationType.NOT_NULL,
                "unique": ExpectationType.UNIQUE,
                "in_set": ExpectationType.IN_SET,
                "match_regex": ExpectationType.MATCH_REGEX,
                "between": ExpectationType.BETWEEN,
                "exists": ExpectationType.COLUMN_TO_EXIST,
            }
            exp_type = type_map.get(expectation_type.lower())
            if not exp_type:
                return {"success": False, "error": f"Unknown expectation type: {expectation_type}"}

            severity_map = {
                "critical": SeverityLevel.CRITICAL,
                "high": SeverityLevel.HIGH,
                "medium": SeverityLevel.MEDIUM,
                "low": SeverityLevel.LOW,
                "info": SeverityLevel.INFO,
            }
            sev = severity_map.get(severity.lower(), SeverityLevel.MEDIUM)

            # Build kwargs
            kwargs = {}
            if value_set:
                kwargs["value_set"] = json.loads(value_set)
            if regex:
                kwargs["regex"] = regex
            if min_value is not None:
                kwargs["min_value"] = min_value
            if max_value is not None:
                kwargs["max_value"] = max_value

            expectation = suite_gen.add_expectation(
                suite_name=suite_name,
                expectation_type=exp_type,
                column=column,
                description=description,
                severity=sev,
                **kwargs,
            )

            return {
                "success": True,
                "expectation_id": expectation.id,
                "column": column,
                "type": expectation_type,
                "severity": severity,
                "message": f"Added {expectation_type} expectation for column '{column}'",
            }

        except Exception as e:
            logger.error(f"Failed to add expectation: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def create_data_contract(
        name: str,
        version: str = "1.0.0",
        owner: Optional[str] = None,
        team: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        columns: Optional[str] = None,
        freshness_hours: Optional[int] = None,
        completeness_percent: Optional[float] = None,
        validation_schedule: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a data contract with quality expectations.

        Data contracts define the expected schema, quality rules, and SLAs
        for a data asset. They can be exported to YAML for version control.

        Args:
            name: Contract name
            version: Contract version (e.g., "1.0.0")
            owner: Data owner
            team: Responsible team
            database: Target database
            schema_name: Target schema
            table_name: Target table
            columns: JSON array of column definitions
            freshness_hours: Max data age in hours
            completeness_percent: Min completeness percentage
            validation_schedule: Cron schedule for validation

        Returns:
            Created contract details

        Example:
            create_data_contract(
                name="gl_accounts_contract",
                owner="finance-team",
                database="ANALYTICS",
                schema_name="FINANCE",
                table_name="GL_ACCOUNTS",
                freshness_hours=24,
                completeness_percent=99.5,
                validation_schedule="0 6 * * *"
            )
        """
        try:
            contract = contract_gen.create_contract(
                name=name,
                version=version,
                owner=owner,
                team=team,
                database=database,
                schema_name=schema_name,
                table_name=table_name,
            )

            # Add columns if provided
            if columns:
                col_data = json.loads(columns)
                for col in col_data:
                    contract_gen.add_column(
                        contract_name=name,
                        name=col.get("name"),
                        data_type=col.get("type", "VARCHAR"),
                        not_null=col.get("not_null", False),
                        unique=col.get("unique", False),
                        pattern=col.get("pattern"),
                    )

            # Set quality rules
            if freshness_hours or completeness_percent:
                contract_gen.set_quality_rules(
                    contract_name=name,
                    freshness_max_age_hours=freshness_hours,
                    completeness_min_percent=completeness_percent,
                )

            # Set SLA
            if validation_schedule:
                contract_gen.set_sla(
                    contract_name=name,
                    validation_schedule=validation_schedule,
                    alert_on_failure=True,
                )

            return {
                "success": True,
                "contract_id": contract.id,
                "contract_name": contract.name,
                "version": contract.version,
                "columns_count": len(contract.columns),
                "message": f"Created data contract '{name}' v{version}",
            }

        except Exception as e:
            logger.error(f"Failed to create contract: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def run_validation(
        suite_name: str,
        data: Optional[str] = None,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run an expectation suite against data.

        Validates data against all expectations in a suite and returns
        detailed results including pass/fail status, unexpected values,
        and statistics.

        Args:
            suite_name: Name of the suite to run
            data: JSON array of row data (for in-memory validation)
            connection_id: Database connection ID (for database validation)

        Returns:
            Validation results with pass/fail details

        Example:
            # In-memory validation
            run_validation(
                suite_name="gl_accounts_suite",
                data='[{"ACCOUNT_CODE": "4100", "ACCOUNT_NAME": "Revenue"}]'
            )

            # Database validation (requires connection)
            run_validation(
                suite_name="gl_accounts_suite",
                connection_id="snowflake-prod"
            )
        """
        try:
            suite = suite_gen.get_suite(suite_name)
            if not suite:
                return {"success": False, "error": f"Suite '{suite_name}' not found"}

            if data:
                # In-memory validation
                row_data = json.loads(data)
                result = validator.validate_dataframe(suite, row_data)
            elif connection_id:
                # Database validation (would need query function set)
                return {
                    "success": False,
                    "error": "Database validation requires query function. Use data parameter for now.",
                }
            else:
                return {"success": False, "error": "Either data or connection_id required"}

            return {
                "success": True,
                "validation_id": result.id,
                "suite_name": result.suite_name,
                "status": result.status.value,
                "total_expectations": result.total_expectations,
                "passed": result.success_count,
                "failed": result.failure_count,
                "errors": result.error_count,
                "success_percent": round(result.success_percent, 1),
                "duration_seconds": result.duration_seconds,
                "row_count": result.row_count,
                "failures": [
                    {
                        "expectation": r.expectation_type,
                        "expected": r.expected_value,
                        "observed": r.observed_value,
                        "unexpected_count": r.unexpected_count,
                    }
                    for r in result.results if not r.success
                ],
            }

        except Exception as e:
            logger.error(f"Failed to run validation: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_validation_results(
        suite_name: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        Get validation results for a suite.

        Returns historical validation results including status,
        timing, and statistics.

        Args:
            suite_name: Name of the suite
            limit: Maximum number of results to return

        Returns:
            List of validation results

        Example:
            get_validation_results(suite_name="gl_accounts_suite", limit=5)
        """
        try:
            results = validator.get_results(suite_name, limit)

            return {
                "success": True,
                "suite_name": suite_name,
                "results_count": len(results),
                "results": [
                    {
                        "id": r.id,
                        "status": r.status.value,
                        "run_at": r.run_at.isoformat(),
                        "total": r.total_expectations,
                        "passed": r.success_count,
                        "failed": r.failure_count,
                        "success_percent": round(r.success_percent, 1),
                        "duration_seconds": r.duration_seconds,
                    }
                    for r in results
                ],
            }

        except Exception as e:
            logger.error(f"Failed to get results: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def list_expectation_suites() -> Dict[str, Any]:
        """
        List all available expectation suites.

        Returns a list of all configured suites with their metadata
        including name, description, expectations count, and target table.

        Returns:
            List of suite summaries

        Example:
            list_expectation_suites()
        """
        try:
            suites = suite_gen.list_suites()

            return {
                "success": True,
                "suites_count": len(suites),
                "suites": suites,
            }

        except Exception as e:
            logger.error(f"Failed to list suites: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def export_data_contract(
        contract_name: str,
        format: str = "yaml",
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Export a data contract to YAML or JSON.

        Exports the contract definition including schema, quality rules,
        and SLAs to a file or returns the content.

        Args:
            contract_name: Name of the contract
            format: Output format (yaml, json)
            output_path: Optional file path to write to

        Returns:
            Exported contract content or file path

        Example:
            export_data_contract(
                contract_name="gl_accounts_contract",
                format="yaml",
                output_path="./contracts/gl_accounts.yml"
            )
        """
        try:
            if output_path:
                file_path = contract_gen.export_to_file(
                    contract_name=contract_name,
                    output_path=output_path,
                    format=format,
                )
                return {
                    "success": True,
                    "contract_name": contract_name,
                    "format": format,
                    "file_path": file_path,
                    "message": f"Exported contract to {file_path}",
                }
            else:
                content = contract_gen.export_contract(contract_name, format)
                return {
                    "success": True,
                    "contract_name": contract_name,
                    "format": format,
                    "content": content,
                }

        except Exception as e:
            logger.error(f"Failed to export contract: {e}")
            return {"success": False, "error": str(e)}

    # Return registration info
    return {
        "tools_registered": 7,
        "tools": [
            "generate_expectation_suite",
            "add_column_expectation",
            "create_data_contract",
            "run_validation",
            "get_validation_results",
            "list_expectation_suites",
            "export_data_contract",
        ],
    }
