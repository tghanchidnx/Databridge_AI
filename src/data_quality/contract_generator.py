"""
Data Contract Generator.

Generates YAML data contracts from:
- Hierarchy definitions
- Expectation suites
- Schema information
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml

from .types import (
    DataContract,
    ColumnSchema,
    QualityRules,
    SlaConfig,
    ExpectationSuite,
)

logger = logging.getLogger(__name__)


class DataContractGenerator:
    """Generates data contracts."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/data_contracts")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._contracts: Dict[str, DataContract] = {}
        self._load()

    def create_contract(
        self,
        name: str,
        version: str = "1.0.0",
        description: Optional[str] = None,
        owner: Optional[str] = None,
        team: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        hierarchy_project_id: Optional[str] = None,
    ) -> DataContract:
        """
        Create a new data contract.

        Args:
            name: Contract name
            version: Contract version
            description: Contract description
            owner: Data owner
            team: Responsible team
            database: Target database
            schema_name: Target schema
            table_name: Target table
            hierarchy_project_id: Source hierarchy project

        Returns:
            DataContract instance
        """
        if name in self._contracts:
            raise ValueError(f"Contract '{name}' already exists")

        contract = DataContract(
            name=name,
            version=version,
            description=description,
            owner=owner,
            team=team,
            database=database,
            schema_name=schema_name,
            table_name=table_name,
            hierarchy_project_id=hierarchy_project_id,
        )

        self._contracts[name] = contract
        self._save()

        logger.info(f"Created data contract: {name}")
        return contract

    def get_contract(self, name: str) -> Optional[DataContract]:
        """Get a contract by name."""
        return self._contracts.get(name)

    def list_contracts(self) -> List[Dict[str, Any]]:
        """List all contracts."""
        return [
            {
                "id": c.id,
                "name": c.name,
                "version": c.version,
                "owner": c.owner,
                "table_name": c.table_name,
                "columns_count": len(c.columns),
                "created_at": c.created_at.isoformat(),
            }
            for c in self._contracts.values()
        ]

    def delete_contract(self, name: str) -> bool:
        """Delete a contract."""
        if name in self._contracts:
            del self._contracts[name]
            self._save()
            return True
        return False

    def add_column(
        self,
        contract_name: str,
        name: str,
        data_type: str,
        description: Optional[str] = None,
        not_null: bool = False,
        unique: bool = False,
        primary_key: bool = False,
        pattern: Optional[str] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        allowed_values: Optional[List[str]] = None,
    ) -> ColumnSchema:
        """
        Add a column to a contract.

        Args:
            contract_name: Name of the contract
            name: Column name
            data_type: Data type (VARCHAR, NUMBER, etc.)
            description: Column description
            not_null: Not null constraint
            unique: Uniqueness constraint
            primary_key: Primary key flag
            pattern: Regex pattern
            min_value: Minimum value
            max_value: Maximum value
            min_length: Minimum length
            max_length: Maximum length
            allowed_values: List of allowed values

        Returns:
            Created ColumnSchema
        """
        contract = self._contracts.get(contract_name)
        if not contract:
            raise ValueError(f"Contract '{contract_name}' not found")

        column = ColumnSchema(
            name=name,
            data_type=data_type,
            description=description,
            not_null=not_null,
            unique=unique,
            primary_key=primary_key,
            pattern=pattern,
            min_value=min_value,
            max_value=max_value,
            min_length=min_length,
            max_length=max_length,
            allowed_values=allowed_values,
        )

        contract.columns.append(column)
        contract.updated_at = datetime.now()
        self._save()

        return column

    def set_quality_rules(
        self,
        contract_name: str,
        freshness_max_age_hours: Optional[int] = None,
        completeness_min_percent: Optional[float] = None,
        uniqueness_columns: Optional[List[str]] = None,
        row_count_min: Optional[int] = None,
        row_count_max: Optional[int] = None,
    ) -> QualityRules:
        """
        Set quality rules for a contract.

        Args:
            contract_name: Name of the contract
            freshness_max_age_hours: Maximum data age in hours
            completeness_min_percent: Minimum completeness percentage
            uniqueness_columns: Columns that should be unique together
            row_count_min: Minimum row count
            row_count_max: Maximum row count

        Returns:
            Updated QualityRules
        """
        contract = self._contracts.get(contract_name)
        if not contract:
            raise ValueError(f"Contract '{contract_name}' not found")

        if freshness_max_age_hours is not None:
            contract.quality.freshness_max_age_hours = freshness_max_age_hours
        if completeness_min_percent is not None:
            contract.quality.completeness_min_percent = completeness_min_percent
        if uniqueness_columns is not None:
            contract.quality.uniqueness_columns = uniqueness_columns
        if row_count_min is not None:
            contract.quality.row_count_min = row_count_min
        if row_count_max is not None:
            contract.quality.row_count_max = row_count_max

        contract.updated_at = datetime.now()
        self._save()

        return contract.quality

    def set_sla(
        self,
        contract_name: str,
        validation_schedule: Optional[str] = None,
        alert_on_failure: bool = True,
        alert_channels: Optional[List[str]] = None,
    ) -> SlaConfig:
        """
        Set SLA configuration for a contract.

        Args:
            contract_name: Name of the contract
            validation_schedule: Cron expression for scheduling
            alert_on_failure: Whether to alert on failures
            alert_channels: List of alert channels (e.g., slack://channel)

        Returns:
            Updated SlaConfig
        """
        contract = self._contracts.get(contract_name)
        if not contract:
            raise ValueError(f"Contract '{contract_name}' not found")

        if validation_schedule is not None:
            contract.sla.validation_schedule = validation_schedule
        contract.sla.alert_on_failure = alert_on_failure
        if alert_channels is not None:
            contract.sla.alert_channels = alert_channels

        contract.updated_at = datetime.now()
        self._save()

        return contract.sla

    def generate_from_hierarchy(
        self,
        contract_name: str,
        hierarchy_nodes: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
        owner: Optional[str] = None,
    ) -> DataContract:
        """
        Generate a contract from hierarchy definitions.

        Args:
            contract_name: Name for the contract
            hierarchy_nodes: List of hierarchy nodes
            mappings: List of source mappings
            owner: Contract owner

        Returns:
            Generated DataContract
        """
        # Extract source table info
        source_info = self._extract_source_info(mappings)

        contract = self.create_contract(
            name=contract_name,
            description=f"Data contract for hierarchy-mapped table",
            owner=owner,
            database=source_info.get("database"),
            schema_name=source_info.get("schema"),
            table_name=source_info.get("table"),
        )

        # Extract columns from mappings
        columns_info = self._extract_columns(mappings)
        for col_name, col_info in columns_info.items():
            self.add_column(
                contract_name=contract_name,
                name=col_name,
                data_type=col_info.get("data_type", "VARCHAR"),
                not_null=True,  # Mapped columns should not be null
                allowed_values=col_info.get("allowed_values"),
            )

        # Set quality rules
        self.set_quality_rules(
            contract_name=contract_name,
            completeness_min_percent=99.0,
            uniqueness_columns=list(columns_info.keys())[:1],  # First column as unique
        )

        return contract

    def generate_from_suite(
        self,
        contract_name: str,
        suite: ExpectationSuite,
        owner: Optional[str] = None,
    ) -> DataContract:
        """
        Generate a contract from an expectation suite.

        Args:
            contract_name: Name for the contract
            suite: Source expectation suite
            owner: Contract owner

        Returns:
            Generated DataContract
        """
        contract = self.create_contract(
            name=contract_name,
            description=f"Data contract derived from suite: {suite.name}",
            owner=owner,
            database=suite.database,
            schema_name=suite.schema_name,
            table_name=suite.table_name,
        )

        # Extract column info from expectations
        columns = {}
        for exp in suite.expectations:
            col = exp.column
            if not col:
                continue

            if col not in columns:
                columns[col] = {
                    "not_null": False,
                    "unique": False,
                    "pattern": None,
                }

            exp_type = exp.expectation_type
            if exp_type.value == "expect_column_values_to_not_be_null":
                columns[col]["not_null"] = True
            elif exp_type.value == "expect_column_values_to_be_unique":
                columns[col]["unique"] = True
            elif exp_type.value == "expect_column_values_to_match_regex":
                columns[col]["pattern"] = exp.kwargs.get("regex")

        # Add columns to contract
        for col_name, col_info in columns.items():
            self.add_column(
                contract_name=contract_name,
                name=col_name,
                data_type="VARCHAR",
                not_null=col_info["not_null"],
                unique=col_info["unique"],
                pattern=col_info["pattern"],
            )

        return contract

    def export_contract(
        self,
        contract_name: str,
        format: str = "yaml",
    ) -> str:
        """
        Export a contract to string format.

        Args:
            contract_name: Name of the contract
            format: Output format (yaml, json)

        Returns:
            Formatted string
        """
        contract = self._contracts.get(contract_name)
        if not contract:
            raise ValueError(f"Contract '{contract_name}' not found")

        # Build contract structure
        contract_dict = {
            "contract": {
                "name": contract.name,
                "version": contract.version,
                "owner": contract.owner,
                "team": contract.team,
            },
            "target": {
                "database": contract.database,
                "schema": contract.schema_name,
                "table": contract.table_name,
            },
            "schema": [
                {
                    "column": col.name,
                    "type": col.data_type,
                    **({"not_null": col.not_null} if col.not_null else {}),
                    **({"unique": col.unique} if col.unique else {}),
                    **({"pattern": col.pattern} if col.pattern else {}),
                    **({"min_value": col.min_value} if col.min_value is not None else {}),
                    **({"max_value": col.max_value} if col.max_value is not None else {}),
                    **({"max_length": col.max_length} if col.max_length else {}),
                }
                for col in contract.columns
            ],
            "quality": {
                **({"freshness": {"max_age_hours": contract.quality.freshness_max_age_hours}} if contract.quality.freshness_max_age_hours else {}),
                **({"completeness": {"min_percent": contract.quality.completeness_min_percent}} if contract.quality.completeness_min_percent else {}),
                **({"uniqueness": {"columns": contract.quality.uniqueness_columns}} if contract.quality.uniqueness_columns else {}),
            },
            "sla": {
                **({"validation_schedule": contract.sla.validation_schedule} if contract.sla.validation_schedule else {}),
                "alert_on_failure": contract.sla.alert_on_failure,
                **({"alert_channels": contract.sla.alert_channels} if contract.sla.alert_channels else {}),
            },
        }

        if format == "yaml":
            return yaml.dump(contract_dict, default_flow_style=False, sort_keys=False)
        elif format == "json":
            return json.dumps(contract_dict, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def export_to_file(
        self,
        contract_name: str,
        output_path: Optional[str] = None,
        format: str = "yaml",
    ) -> str:
        """
        Export a contract to file.

        Args:
            contract_name: Name of the contract
            output_path: Output file path
            format: Output format

        Returns:
            Path to exported file
        """
        content = self.export_contract(contract_name, format)

        if not output_path:
            ext = "yml" if format == "yaml" else format
            output_path = self.output_dir / f"{contract_name}.{ext}"
        else:
            output_path = Path(output_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content)

        return str(output_path)

    def _extract_source_info(self, mappings: List[Dict[str, Any]]) -> Dict[str, str]:
        """Extract common source info from mappings."""
        if not mappings:
            return {}

        # Use first mapping as reference
        first = mappings[0]
        return {
            "database": first.get("source_database", ""),
            "schema": first.get("source_schema", ""),
            "table": first.get("source_table", ""),
        }

    def _extract_columns(self, mappings: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Extract column info from mappings."""
        columns = {}

        for mapping in mappings:
            col = mapping.get("source_column", "")
            uid = mapping.get("source_uid", "")

            if not col:
                continue

            if col not in columns:
                columns[col] = {
                    "data_type": "VARCHAR",
                    "allowed_values": [],
                }

            if uid and not uid.endswith("%"):
                columns[col]["allowed_values"].append(uid)

        # Clean up empty allowed_values
        for col_info in columns.values():
            if not col_info["allowed_values"]:
                del col_info["allowed_values"]

        return columns

    def _save(self) -> None:
        """Persist contracts to disk."""
        data_file = self.output_dir / "contracts.json"

        data = {}
        for name, contract in self._contracts.items():
            data[name] = contract.model_dump(mode="json")

        data_file.write_text(json.dumps(data, indent=2, default=str))

    def _load(self) -> None:
        """Load contracts from disk."""
        data_file = self.output_dir / "contracts.json"

        if data_file.exists():
            try:
                data = json.loads(data_file.read_text())
                for name, contract_data in data.items():
                    self._contracts[name] = DataContract(**contract_data)
            except Exception as e:
                logger.error(f"Failed to load contracts: {e}")
