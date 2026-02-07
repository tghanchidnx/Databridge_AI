"""
dbt Model Generator.

Generates dbt SQL models from DataBridge hierarchies:
- Staging models (stg_*)
- Intermediate models (int_*)
- Dimension models (dim_*)
- Fact models (fct_*)
"""

import logging
from typing import Any, Dict, List, Optional
import yaml

from .types import (
    DbtProject,
    DbtModelConfig,
    DbtModelType,
    DbtMaterialization,
    DbtColumn,
)

logger = logging.getLogger(__name__)


class DbtModelGenerator:
    """Generates dbt SQL models from hierarchies."""

    def __init__(self):
        pass

    def generate_staging_model(
        self,
        model_name: str,
        source_name: str,
        source_table: str,
        columns: Optional[List[str]] = None,
        case_mappings: Optional[List[Dict[str, str]]] = None,
        description: Optional[str] = None,
    ) -> str:
        """
        Generate a staging model SQL.

        Args:
            model_name: Name for the model (will be prefixed with stg_)
            source_name: dbt source name
            source_table: Source table name
            columns: List of column names to select
            case_mappings: CASE statement mappings for hierarchy categorization
            description: Model description

        Returns:
            SQL content for the model
        """
        sql_parts = []

        # Config block
        sql_parts.append("{{ config(materialized='view') }}")
        sql_parts.append("")

        # Description comment
        if description:
            sql_parts.append(f"-- {description}")
            sql_parts.append("")

        # Select statement
        sql_parts.append("SELECT")

        select_items = []

        # Add columns
        if columns:
            for col in columns:
                select_items.append(f"    {col}")

        # Add CASE mappings for hierarchy categorization
        if case_mappings:
            case_sql = self._generate_case_statement(case_mappings)
            select_items.append(f"    {case_sql}")

        # If no columns specified, select all
        if not select_items:
            select_items.append("    *")

        sql_parts.append(",\n".join(select_items))

        # From clause
        sql_parts.append(f"FROM {{{{ source('{source_name}', '{source_table}') }}}}")

        return "\n".join(sql_parts)

    def generate_intermediate_model(
        self,
        model_name: str,
        refs: List[str],
        joins: Optional[List[Dict[str, str]]] = None,
        select_columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        cte_name: str = "base",
        description: Optional[str] = None,
    ) -> str:
        """
        Generate an intermediate model SQL.

        Args:
            model_name: Name for the model (will be prefixed with int_)
            refs: List of model references
            joins: Join definitions
            select_columns: Columns to select
            where_clause: Optional WHERE clause
            cte_name: Name for the CTE
            description: Model description

        Returns:
            SQL content for the model
        """
        sql_parts = []

        # Config block
        sql_parts.append("{{ config(materialized='view') }}")
        sql_parts.append("")

        # Description comment
        if description:
            sql_parts.append(f"-- {description}")
            sql_parts.append("")

        # CTE
        if len(refs) == 1:
            sql_parts.append(f"WITH {cte_name} AS (")
            sql_parts.append(f"    SELECT * FROM {{{{ ref('{refs[0]}') }}}}")
            sql_parts.append(")")
        else:
            # Multiple refs with joins
            sql_parts.append(f"WITH {cte_name} AS (")
            sql_parts.append(f"    SELECT")

            if select_columns:
                sql_parts.append("        " + ",\n        ".join(select_columns))
            else:
                sql_parts.append("        *")

            sql_parts.append(f"    FROM {{{{ ref('{refs[0]}') }}}} AS t1")

            if joins:
                for i, join in enumerate(joins, 1):
                    join_type = join.get("type", "LEFT")
                    ref_name = join.get("ref", refs[i] if i < len(refs) else refs[0])
                    on_clause = join.get("on", "1=1")
                    sql_parts.append(f"    {join_type} JOIN {{{{ ref('{ref_name}') }}}} AS t{i+1}")
                    sql_parts.append(f"        ON {on_clause}")

            sql_parts.append(")")

        sql_parts.append("")

        # Final select
        sql_parts.append("SELECT")
        if select_columns:
            sql_parts.append("    " + ",\n    ".join(select_columns))
        else:
            sql_parts.append("    *")
        sql_parts.append(f"FROM {cte_name}")

        if where_clause:
            sql_parts.append(f"WHERE {where_clause}")

        return "\n".join(sql_parts)

    def generate_dimension_model(
        self,
        model_name: str,
        ref_model: str,
        hierarchy_columns: List[str],
        attribute_columns: Optional[List[str]] = None,
        surrogate_key_columns: Optional[List[str]] = None,
        description: Optional[str] = None,
    ) -> str:
        """
        Generate a dimension model SQL.

        Args:
            model_name: Name for the model (will be prefixed with dim_)
            ref_model: Reference model name
            hierarchy_columns: Hierarchy level columns (level_1, level_2, etc.)
            attribute_columns: Additional attribute columns
            surrogate_key_columns: Columns for surrogate key generation
            description: Model description

        Returns:
            SQL content for the model
        """
        sql_parts = []

        # Config block
        sql_parts.append("{{ config(materialized='table') }}")
        sql_parts.append("")

        # Description comment
        if description:
            sql_parts.append(f"-- {description}")
            sql_parts.append("")

        # CTE
        sql_parts.append("WITH source AS (")
        sql_parts.append(f"    SELECT * FROM {{{{ ref('{ref_model}') }}}}")
        sql_parts.append("),")
        sql_parts.append("")

        # Final dimension
        sql_parts.append("final AS (")
        sql_parts.append("    SELECT")

        select_items = []

        # Surrogate key
        if surrogate_key_columns:
            key_cols = ", ".join(surrogate_key_columns)
            select_items.append(f"        {{{{ dbt_utils.generate_surrogate_key(['{key_cols}']) }}}} AS {model_name}_key")
        else:
            select_items.append(f"        ROW_NUMBER() OVER (ORDER BY {hierarchy_columns[0]}) AS {model_name}_key")

        # Hierarchy columns
        for col in hierarchy_columns:
            select_items.append(f"        {col}")

        # Attribute columns
        if attribute_columns:
            for col in attribute_columns:
                select_items.append(f"        {col}")

        sql_parts.append(",\n".join(select_items))
        sql_parts.append("    FROM source")
        sql_parts.append(")")
        sql_parts.append("")

        sql_parts.append("SELECT * FROM final")

        return "\n".join(sql_parts)

    def generate_fact_model(
        self,
        model_name: str,
        ref_model: str,
        dimension_refs: List[Dict[str, str]],
        measure_columns: List[str],
        date_column: Optional[str] = None,
        description: Optional[str] = None,
    ) -> str:
        """
        Generate a fact model SQL.

        Args:
            model_name: Name for the model (will be prefixed with fct_)
            ref_model: Main reference model
            dimension_refs: Dimension references with join keys
            measure_columns: Measure/metric columns
            date_column: Date column for time dimension
            description: Model description

        Returns:
            SQL content for the model
        """
        sql_parts = []

        # Config block
        sql_parts.append("{{ config(materialized='table') }}")
        sql_parts.append("")

        # Description comment
        if description:
            sql_parts.append(f"-- {description}")
            sql_parts.append("")

        # Source CTE
        sql_parts.append("WITH source AS (")
        sql_parts.append(f"    SELECT * FROM {{{{ ref('{ref_model}') }}}}")
        sql_parts.append("),")
        sql_parts.append("")

        # Dimension CTEs
        for i, dim in enumerate(dimension_refs):
            dim_name = dim.get("name", f"dim_{i}")
            dim_ref = dim.get("ref", dim_name)
            sql_parts.append(f"{dim_name} AS (")
            sql_parts.append(f"    SELECT * FROM {{{{ ref('{dim_ref}') }}}}")
            sql_parts.append("),")
            sql_parts.append("")

        # Final fact
        sql_parts.append("final AS (")
        sql_parts.append("    SELECT")

        select_items = []

        # Surrogate key
        select_items.append(f"        {{{{ dbt_utils.generate_surrogate_key(['source.id']) }}}} AS {model_name}_key")

        # Date key
        if date_column:
            select_items.append(f"        source.{date_column} AS date_key")

        # Dimension keys
        for dim in dimension_refs:
            dim_name = dim.get("name", "dim")
            key_col = dim.get("key", f"{dim_name}_key")
            select_items.append(f"        {dim_name}.{key_col}")

        # Measures
        for col in measure_columns:
            select_items.append(f"        source.{col}")

        sql_parts.append(",\n".join(select_items))

        # Joins
        sql_parts.append("    FROM source")
        for dim in dimension_refs:
            dim_name = dim.get("name", "dim")
            join_on = dim.get("on", "1=1")
            sql_parts.append(f"    LEFT JOIN {dim_name}")
            sql_parts.append(f"        ON {join_on}")

        sql_parts.append(")")
        sql_parts.append("")

        sql_parts.append("SELECT * FROM final")

        return "\n".join(sql_parts)

    def generate_hierarchy_model(
        self,
        hierarchy_name: str,
        hierarchy_data: List[Dict[str, Any]],
        levels: int = 5,
        include_mappings: bool = True,
    ) -> str:
        """
        Generate a model from DataBridge hierarchy data.

        Args:
            hierarchy_name: Name for the hierarchy
            hierarchy_data: List of hierarchy nodes
            levels: Number of hierarchy levels
            include_mappings: Include source mappings

        Returns:
            SQL content for the hierarchy model
        """
        sql_parts = []

        # Config block
        sql_parts.append("{{ config(materialized='table') }}")
        sql_parts.append("")
        sql_parts.append(f"-- Hierarchy: {hierarchy_name}")
        sql_parts.append("-- Generated from DataBridge hierarchy project")
        sql_parts.append("")

        # Build VALUES clause from hierarchy data
        sql_parts.append("WITH hierarchy_data AS (")
        sql_parts.append("    SELECT * FROM (")
        sql_parts.append("        VALUES")

        values = []
        for node in hierarchy_data:
            row_values = [
                f"'{node.get('hierarchy_id', '')}'",
                f"'{node.get('hierarchy_name', '')}'",
                f"'{node.get('parent_id', '') or ''}'",
            ]

            # Add levels
            for i in range(1, levels + 1):
                level_val = node.get(f"level_{i}", "") or ""
                row_values.append(f"'{level_val}'")

            # Add sort order
            row_values.append(str(node.get("sort_order", 0)))

            values.append(f"        ({', '.join(row_values)})")

        sql_parts.append(",\n".join(values))

        # Column aliases
        col_names = ["hierarchy_id", "hierarchy_name", "parent_id"]
        col_names.extend([f"level_{i}" for i in range(1, levels + 1)])
        col_names.append("sort_order")

        sql_parts.append(f"    ) AS t({', '.join(col_names)})")
        sql_parts.append(")")
        sql_parts.append("")

        sql_parts.append("SELECT")
        sql_parts.append("    " + ",\n    ".join(col_names))
        sql_parts.append("FROM hierarchy_data")
        sql_parts.append("ORDER BY sort_order")

        return "\n".join(sql_parts)

    def generate_model_schema(
        self,
        model: DbtModelConfig,
    ) -> Dict[str, Any]:
        """
        Generate schema.yml content for a model.

        Args:
            model: Model configuration

        Returns:
            Dict for schema.yml
        """
        schema = {
            "name": model.name,
            "description": model.description or f"Model: {model.name}",
        }

        if model.columns:
            schema["columns"] = []
            for col in model.columns:
                col_def = {
                    "name": col.name,
                    "description": col.description or "",
                }
                if col.tests:
                    col_def["tests"] = col.tests
                schema["columns"].append(col_def)

        if model.tests:
            schema["tests"] = model.tests

        return schema

    def add_model_to_project(
        self,
        project: DbtProject,
        model: DbtModelConfig,
        sql_content: str,
    ) -> None:
        """
        Add a generated model to a project.

        Args:
            project: The dbt project
            model: Model configuration
            sql_content: Generated SQL content
        """
        # Determine path based on model type
        if model.model_type == DbtModelType.STAGING:
            path = f"models/staging/stg_{model.name}.sql"
        elif model.model_type == DbtModelType.INTERMEDIATE:
            path = f"models/intermediate/int_{model.name}.sql"
        elif model.model_type == DbtModelType.DIM:
            path = f"models/marts/dim_{model.name}.sql"
        elif model.model_type == DbtModelType.FACT:
            path = f"models/marts/fct_{model.name}.sql"
        else:
            path = f"models/marts/{model.name}.sql"

        # Add to project
        project.models.append(model)
        project.generated_files[path] = sql_content

    def _generate_case_statement(
        self,
        mappings: List[Dict[str, str]],
        column_name: str = "hierarchy_category",
    ) -> str:
        """Generate a CASE statement from mappings."""
        parts = ["CASE"]

        for mapping in mappings:
            condition = mapping.get("condition", "")
            result = mapping.get("result", "")
            parts.append(f"        WHEN {condition} THEN '{result}'")

        parts.append("        ELSE 'Other'")
        parts.append(f"    END AS {column_name}")

        return "\n".join(parts)
