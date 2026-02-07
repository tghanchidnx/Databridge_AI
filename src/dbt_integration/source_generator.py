"""
dbt Source Generator.

Generates dbt source definitions (sources.yml) from:
- DataBridge hierarchy mappings
- Database schema information
- Manual configuration
"""

import logging
from typing import Any, Dict, List, Optional
import yaml

from .types import (
    DbtProject,
    DbtSource,
    DbtSourceTable,
    DbtColumn,
)

logger = logging.getLogger(__name__)


class DbtSourceGenerator:
    """Generates dbt source definitions."""

    def __init__(self):
        pass

    def generate_sources_yml(
        self,
        sources: List[DbtSource],
    ) -> str:
        """
        Generate sources.yml content.

        Args:
            sources: List of source definitions

        Returns:
            YAML string for sources.yml
        """
        sources_dict = {
            "version": 2,
            "sources": [],
        }

        for source in sources:
            source_def = {
                "name": source.name,
            }

            if source.description:
                source_def["description"] = source.description

            if source.database:
                source_def["database"] = source.database

            if source.schema_name:
                source_def["schema"] = source.schema_name

            if source.tags:
                source_def["tags"] = source.tags

            if source.meta:
                source_def["meta"] = source.meta

            # Tables
            if source.tables:
                source_def["tables"] = []
                for table in source.tables:
                    table_def = self._generate_table_def(table)
                    source_def["tables"].append(table_def)

            sources_dict["sources"].append(source_def)

        return yaml.dump(sources_dict, default_flow_style=False, sort_keys=False)

    def generate_schema_yml(
        self,
        models: List[Dict[str, Any]],
    ) -> str:
        """
        Generate schema.yml content for models.

        Args:
            models: List of model definitions

        Returns:
            YAML string for schema.yml
        """
        schema_dict = {
            "version": 2,
            "models": [],
        }

        for model in models:
            model_def = {
                "name": model.get("name"),
            }

            if model.get("description"):
                model_def["description"] = model["description"]

            if model.get("columns"):
                model_def["columns"] = model["columns"]

            if model.get("tests"):
                model_def["tests"] = model["tests"]

            if model.get("tags"):
                model_def["tags"] = model["tags"]

            schema_dict["models"].append(model_def)

        return yaml.dump(schema_dict, default_flow_style=False, sort_keys=False)

    def generate_from_hierarchy_mappings(
        self,
        mappings: List[Dict[str, Any]],
        source_name: str = "raw",
    ) -> DbtSource:
        """
        Generate a source from DataBridge hierarchy mappings.

        Args:
            mappings: List of hierarchy mappings
            source_name: Name for the source

        Returns:
            DbtSource instance
        """
        # Group mappings by table
        tables_dict: Dict[str, DbtSourceTable] = {}

        for mapping in mappings:
            # Extract table info
            database = mapping.get("source_database", "")
            schema = mapping.get("source_schema", "")
            table_name = mapping.get("source_table", "")
            column = mapping.get("source_column", "")

            if not table_name:
                continue

            # Create or update table definition
            table_key = f"{database}.{schema}.{table_name}"

            if table_key not in tables_dict:
                tables_dict[table_key] = DbtSourceTable(
                    name=table_name.lower(),
                    description=f"Source table: {table_name}",
                    database=database if database else None,
                    schema_name=schema if schema else None,
                    identifier=table_name,
                    columns=[],
                )

            # Add column if not already present
            table = tables_dict[table_key]
            col_names = [c.name for c in table.columns]

            if column and column.lower() not in col_names:
                table.columns.append(DbtColumn(
                    name=column.lower(),
                    description=f"Column used in hierarchy mapping",
                ))

        # Create source
        source = DbtSource(
            name=source_name,
            description="Source tables from DataBridge hierarchy mappings",
            tables=list(tables_dict.values()),
        )

        # Set database/schema from first table if available
        if tables_dict:
            first_table = list(tables_dict.values())[0]
            source.database = first_table.database
            source.schema_name = first_table.schema_name

        return source

    def add_source_to_project(
        self,
        project: DbtProject,
        source: DbtSource,
    ) -> None:
        """
        Add a source to a project.

        Args:
            project: The dbt project
            source: Source definition
        """
        # Check if source already exists
        existing = [s for s in project.sources if s.name == source.name]
        if existing:
            # Merge tables
            existing_source = existing[0]
            existing_table_names = [t.name for t in existing_source.tables]

            for table in source.tables:
                if table.name not in existing_table_names:
                    existing_source.tables.append(table)
        else:
            project.sources.append(source)

        # Update sources.yml in generated files
        sources_yml = self.generate_sources_yml(project.sources)
        project.generated_files["models/sources.yml"] = sources_yml

    def generate_source_freshness(
        self,
        source_name: str,
        table_name: str,
        loaded_at_field: str,
        warn_after_hours: int = 24,
        error_after_hours: int = 48,
    ) -> Dict[str, Any]:
        """
        Generate freshness configuration for a source table.

        Args:
            source_name: Source name
            table_name: Table name
            loaded_at_field: Column with load timestamp
            warn_after_hours: Hours before warning
            error_after_hours: Hours before error

        Returns:
            Freshness configuration dict
        """
        return {
            "loaded_at_field": loaded_at_field,
            "freshness": {
                "warn_after": {"count": warn_after_hours, "period": "hour"},
                "error_after": {"count": error_after_hours, "period": "hour"},
            },
        }

    def _generate_table_def(self, table: DbtSourceTable) -> Dict[str, Any]:
        """Generate table definition for sources.yml."""
        table_def = {
            "name": table.name,
        }

        if table.description:
            table_def["description"] = table.description

        if table.identifier and table.identifier != table.name:
            table_def["identifier"] = table.identifier

        if table.loaded_at_field:
            table_def["loaded_at_field"] = table.loaded_at_field

        if table.freshness_warn_after:
            if "freshness" not in table_def:
                table_def["freshness"] = {}
            table_def["freshness"]["warn_after"] = table.freshness_warn_after

        if table.freshness_error_after:
            if "freshness" not in table_def:
                table_def["freshness"] = {}
            table_def["freshness"]["error_after"] = table.freshness_error_after

        if table.columns:
            table_def["columns"] = []
            for col in table.columns:
                col_def = {"name": col.name}
                if col.description:
                    col_def["description"] = col.description
                if col.tests:
                    col_def["tests"] = col.tests
                table_def["columns"].append(col_def)

        if table.tags:
            table_def["tags"] = table.tags

        if table.meta:
            table_def["meta"] = table.meta

        return table_def


class DbtMetricsGenerator:
    """Generates dbt metrics from formula groups."""

    def __init__(self):
        pass

    def generate_metrics_yml(
        self,
        metrics: List[Dict[str, Any]],
    ) -> str:
        """
        Generate metrics.yml content.

        Args:
            metrics: List of metric definitions

        Returns:
            YAML string for metrics.yml
        """
        metrics_dict = {
            "version": 2,
            "metrics": [],
        }

        for metric in metrics:
            metric_def = {
                "name": metric.get("name"),
                "label": metric.get("label", metric.get("name")),
                "type": metric.get("type", "derived"),
            }

            if metric.get("description"):
                metric_def["description"] = metric["description"]

            if metric.get("expression"):
                metric_def["expression"] = metric["expression"]

            if metric.get("dimensions"):
                metric_def["dimensions"] = metric["dimensions"]

            if metric.get("time_grains"):
                metric_def["time_grains"] = metric["time_grains"]

            if metric.get("filters"):
                metric_def["filters"] = metric["filters"]

            metrics_dict["metrics"].append(metric_def)

        return yaml.dump(metrics_dict, default_flow_style=False, sort_keys=False)

    def generate_from_formula_groups(
        self,
        formula_groups: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Generate metrics from DataBridge formula groups.

        Args:
            formula_groups: List of formula group definitions

        Returns:
            List of metric definitions
        """
        metrics = []

        for group in formula_groups:
            group_name = group.get("name", "unnamed")
            rules = group.get("rules", [])

            for rule in rules:
                operation = rule.get("operation", "SUM")
                target = rule.get("target", "")
                operands = rule.get("operands", [])

                # Build expression
                if operation == "SUM":
                    expression = " + ".join(operands) if operands else target
                elif operation == "SUBTRACT":
                    expression = " - ".join(operands) if operands else target
                elif operation == "MULTIPLY":
                    expression = " * ".join(operands) if operands else target
                elif operation == "DIVIDE":
                    if len(operands) >= 2:
                        expression = f"{operands[0]} / NULLIF({operands[1]}, 0)"
                    else:
                        expression = target
                else:
                    expression = target

                metric = {
                    "name": f"{group_name}_{target}".lower().replace(" ", "_"),
                    "label": target,
                    "description": f"Calculated from formula group: {group_name}",
                    "type": "derived",
                    "expression": expression,
                }

                metrics.append(metric)

        return metrics
