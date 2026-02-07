"""
Expectation Suite Generator.

Generates expectation suites from:
- DataBridge hierarchy definitions
- Source mappings
- Formula groups
- Manual configuration
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import (
    Expectation,
    ExpectationSuite,
    ExpectationType,
    SeverityLevel,
)

logger = logging.getLogger(__name__)


class ExpectationSuiteGenerator:
    """Generates expectation suites from various sources."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/expectation_suites")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._suites: Dict[str, ExpectationSuite] = {}
        self._load()

    def create_suite(
        self,
        name: str,
        description: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        hierarchy_project_id: Optional[str] = None,
    ) -> ExpectationSuite:
        """
        Create a new expectation suite.

        Args:
            name: Suite name
            description: Suite description
            database: Target database
            schema_name: Target schema
            table_name: Target table
            hierarchy_project_id: Source hierarchy project

        Returns:
            ExpectationSuite instance
        """
        if name in self._suites:
            raise ValueError(f"Suite '{name}' already exists")

        suite = ExpectationSuite(
            name=name,
            description=description,
            database=database,
            schema_name=schema_name,
            table_name=table_name,
            hierarchy_project_id=hierarchy_project_id,
            data_asset_name=f"{database}.{schema_name}.{table_name}" if all([database, schema_name, table_name]) else None,
        )

        self._suites[name] = suite
        self._save()

        logger.info(f"Created expectation suite: {name}")
        return suite

    def get_suite(self, name: str) -> Optional[ExpectationSuite]:
        """Get a suite by name."""
        return self._suites.get(name)

    def list_suites(self) -> List[Dict[str, Any]]:
        """List all suites."""
        return [
            {
                "id": s.id,
                "name": s.name,
                "description": s.description,
                "expectations_count": len(s.expectations),
                "table_name": s.table_name,
                "hierarchy_project_id": s.hierarchy_project_id,
                "created_at": s.created_at.isoformat(),
            }
            for s in self._suites.values()
        ]

    def delete_suite(self, name: str) -> bool:
        """Delete a suite."""
        if name in self._suites:
            del self._suites[name]
            self._save()
            return True
        return False

    def add_expectation(
        self,
        suite_name: str,
        expectation_type: ExpectationType,
        column: Optional[str] = None,
        description: Optional[str] = None,
        severity: SeverityLevel = SeverityLevel.MEDIUM,
        **kwargs,
    ) -> Expectation:
        """
        Add an expectation to a suite.

        Args:
            suite_name: Name of the suite
            expectation_type: Type of expectation
            column: Column name (for column expectations)
            description: Human-readable description
            severity: Failure severity
            **kwargs: Expectation-specific arguments

        Returns:
            Created Expectation
        """
        suite = self._suites.get(suite_name)
        if not suite:
            raise ValueError(f"Suite '{suite_name}' not found")

        expectation = Expectation(
            expectation_type=expectation_type,
            column=column,
            description=description,
            severity=severity,
            kwargs=kwargs,
        )

        suite.add_expectation(expectation)
        self._save()

        return expectation

    def generate_from_hierarchy(
        self,
        suite_name: str,
        hierarchy_nodes: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
    ) -> ExpectationSuite:
        """
        Generate expectations from hierarchy definitions.

        Args:
            suite_name: Name for the suite
            hierarchy_nodes: List of hierarchy nodes
            mappings: List of source mappings

        Returns:
            Generated ExpectationSuite
        """
        suite = self._suites.get(suite_name)
        if not suite:
            suite = self.create_suite(suite_name)

        # Extract unique source columns
        source_columns = {}
        for mapping in mappings:
            col = mapping.get("source_column", "")
            if col and col not in source_columns:
                source_columns[col] = {
                    "database": mapping.get("source_database"),
                    "schema": mapping.get("source_schema"),
                    "table": mapping.get("source_table"),
                    "uids": [],
                }
            if col:
                uid = mapping.get("source_uid", "")
                if uid:
                    source_columns[col]["uids"].append(uid)

        # Generate column expectations
        for col_name, col_info in source_columns.items():
            # Column should exist
            suite.add_expectation(Expectation(
                expectation_type=ExpectationType.COLUMN_TO_EXIST,
                column=col_name,
                description=f"Column {col_name} must exist",
                severity=SeverityLevel.CRITICAL,
            ))

            # Column should not be null for mapped values
            suite.add_expectation(Expectation(
                expectation_type=ExpectationType.NOT_NULL,
                column=col_name,
                description=f"Column {col_name} should not have null values",
                severity=SeverityLevel.HIGH,
            ))

            # Generate pattern expectations from source_uid values
            patterns = self._extract_patterns(col_info["uids"])
            for pattern in patterns:
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.MATCH_REGEX,
                    column=col_name,
                    kwargs={"regex": pattern["regex"]},
                    description=f"Values matching pattern: {pattern['description']}",
                    severity=SeverityLevel.MEDIUM,
                ))

        # Generate value set expectations
        value_sets = self._group_by_column(mappings)
        for col_name, values in value_sets.items():
            if len(values) > 0 and len(values) <= 100:  # Only for reasonable set sizes
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.IN_SET,
                    column=col_name,
                    kwargs={"value_set": list(values)},
                    description=f"Values should be in hierarchy mapping set",
                    severity=SeverityLevel.MEDIUM,
                ))

        self._save()
        return suite

    def generate_from_schema(
        self,
        suite_name: str,
        columns: List[Dict[str, Any]],
    ) -> ExpectationSuite:
        """
        Generate expectations from schema definition.

        Args:
            suite_name: Name for the suite
            columns: List of column definitions

        Returns:
            Generated ExpectationSuite
        """
        suite = self._suites.get(suite_name)
        if not suite:
            suite = self.create_suite(suite_name)

        for col in columns:
            col_name = col.get("name", "")
            data_type = col.get("data_type", "").upper()
            not_null = col.get("not_null", False)
            unique = col.get("unique", False)

            # Column exists
            suite.add_expectation(Expectation(
                expectation_type=ExpectationType.COLUMN_TO_EXIST,
                column=col_name,
                severity=SeverityLevel.CRITICAL,
            ))

            # Not null
            if not_null:
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.NOT_NULL,
                    column=col_name,
                    severity=SeverityLevel.HIGH,
                ))

            # Unique
            if unique:
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.UNIQUE,
                    column=col_name,
                    severity=SeverityLevel.HIGH,
                ))

            # Type-specific expectations
            if "INT" in data_type or "NUMBER" in data_type:
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.IN_TYPE_LIST,
                    column=col_name,
                    kwargs={"type_list": ["int", "float", "int64", "float64"]},
                    severity=SeverityLevel.MEDIUM,
                ))

            # Pattern from column definition
            pattern = col.get("pattern")
            if pattern:
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.MATCH_REGEX,
                    column=col_name,
                    kwargs={"regex": pattern},
                    severity=SeverityLevel.MEDIUM,
                ))

            # Min/max length
            min_len = col.get("min_length")
            max_len = col.get("max_length")
            if min_len or max_len:
                suite.add_expectation(Expectation(
                    expectation_type=ExpectationType.BETWEEN,
                    column=col_name,
                    kwargs={
                        "min_value": min_len,
                        "max_value": max_len,
                    },
                    description=f"Length between {min_len} and {max_len}",
                    severity=SeverityLevel.LOW,
                ))

        self._save()
        return suite

    def export_suite(
        self,
        suite_name: str,
        format: str = "json",
    ) -> str:
        """
        Export a suite to string format.

        Args:
            suite_name: Name of the suite
            format: Output format (json, yaml)

        Returns:
            Formatted string
        """
        suite = self._suites.get(suite_name)
        if not suite:
            raise ValueError(f"Suite '{suite_name}' not found")

        ge_format = suite.to_ge_format()

        if format == "json":
            return json.dumps(ge_format, indent=2, default=str)
        elif format == "yaml":
            import yaml
            return yaml.dump(ge_format, default_flow_style=False, sort_keys=False)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _extract_patterns(self, uids: List[str]) -> List[Dict[str, str]]:
        """Extract regex patterns from source_uid values."""
        patterns = []

        for uid in uids:
            if not uid:
                continue

            # Handle LIKE patterns (e.g., "4%", "41%")
            if uid.endswith("%"):
                prefix = uid[:-1]
                if prefix:
                    patterns.append({
                        "regex": f"^{re.escape(prefix)}.*",
                        "description": f"Starts with '{prefix}'",
                    })

            # Handle range patterns (e.g., "4100-4199")
            elif "-" in uid and uid.replace("-", "").isdigit():
                parts = uid.split("-")
                if len(parts) == 2:
                    patterns.append({
                        "regex": f"^[{parts[0][0]}-{parts[1][0]}].*",
                        "description": f"Range {uid}",
                    })

        # Deduplicate
        seen = set()
        unique_patterns = []
        for p in patterns:
            if p["regex"] not in seen:
                seen.add(p["regex"])
                unique_patterns.append(p)

        return unique_patterns

    def _group_by_column(self, mappings: List[Dict[str, Any]]) -> Dict[str, set]:
        """Group source_uid values by column."""
        result = {}
        for mapping in mappings:
            col = mapping.get("source_column", "")
            uid = mapping.get("source_uid", "")
            if col and uid and not uid.endswith("%"):  # Exclude patterns
                if col not in result:
                    result[col] = set()
                result[col].add(uid)
        return result

    def _save(self) -> None:
        """Persist suites to disk."""
        data_file = self.output_dir / "suites.json"

        data = {}
        for name, suite in self._suites.items():
            data[name] = suite.model_dump(mode="json")

        data_file.write_text(json.dumps(data, indent=2, default=str))

    def _load(self) -> None:
        """Load suites from disk."""
        data_file = self.output_dir / "suites.json"

        if data_file.exists():
            try:
                data = json.loads(data_file.read_text())
                for name, suite_data in data.items():
                    self._suites[name] = ExpectationSuite(**suite_data)
            except Exception as e:
                logger.error(f"Failed to load suites: {e}")
