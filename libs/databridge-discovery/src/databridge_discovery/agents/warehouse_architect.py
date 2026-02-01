"""
Warehouse Architect Agent for designing star schemas and generating models.

Capabilities:
- design_star_schema: Design dimensional model
- generate_dims: Generate dimension definitions
- generate_facts: Generate fact table definitions
- dbt_models: Generate dbt models
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from databridge_discovery.agents.base_agent import (
    BaseAgent,
    AgentCapability,
    AgentConfig,
    AgentResult,
    AgentError,
    TaskContext,
)


@dataclass
class DimensionSpec:
    """Specification for a dimension table."""

    name: str
    source_table: str
    key_column: str
    attributes: list[dict[str, Any]] = field(default_factory=list)
    hierarchy_levels: list[str] = field(default_factory=list)
    scd_type: int = 1  # 0, 1, 2, 3
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_table": self.source_table,
            "key_column": self.key_column,
            "attributes": self.attributes,
            "hierarchy_levels": self.hierarchy_levels,
            "scd_type": self.scd_type,
            "description": self.description,
        }


@dataclass
class FactSpec:
    """Specification for a fact table."""

    name: str
    source_tables: list[str] = field(default_factory=list)
    grain: str = ""
    dimensions: list[str] = field(default_factory=list)
    measures: list[dict[str, Any]] = field(default_factory=list)
    degenerate_dimensions: list[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_tables": self.source_tables,
            "grain": self.grain,
            "dimensions": self.dimensions,
            "measures": self.measures,
            "degenerate_dimensions": self.degenerate_dimensions,
            "description": self.description,
        }


@dataclass
class StarSchemaDesign:
    """Complete star schema design."""

    name: str
    facts: list[FactSpec] = field(default_factory=list)
    dimensions: list[DimensionSpec] = field(default_factory=list)
    conformed_dimensions: list[str] = field(default_factory=list)
    description: str = ""
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "facts": [f.to_dict() for f in self.facts],
            "dimensions": [d.to_dict() for d in self.dimensions],
            "conformed_dimensions": self.conformed_dimensions,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
        }


class WarehouseArchitect(BaseAgent):
    """
    Warehouse Architect Agent for designing star schemas and models.

    Designs and generates:
    - Star schema structures
    - Dimension tables with SCD support
    - Fact tables with measures
    - dbt models

    Example:
        architect = WarehouseArchitect()

        context = TaskContext(
            task_id="design_1",
            input_data={
                "tables": [...],
                "relationships": [...],
            }
        )

        result = architect.execute(
            AgentCapability.DESIGN_STAR_SCHEMA,
            context
        )
    """

    # Common dimension attributes
    COMMON_DIMENSION_COLUMNS = {
        "date": ["year", "quarter", "month", "week", "day", "day_of_week", "is_weekend", "is_holiday"],
        "customer": ["name", "type", "segment", "region", "country"],
        "product": ["name", "category", "subcategory", "brand", "sku"],
        "geography": ["country", "region", "state", "city", "postal_code"],
        "employee": ["name", "department", "title", "manager", "hire_date"],
    }

    # Measure aggregation patterns
    MEASURE_PATTERNS = {
        "sum": ["amount", "quantity", "total", "revenue", "cost", "price"],
        "count": ["count", "number", "qty"],
        "avg": ["average", "mean", "rate"],
        "min_max": ["date", "timestamp"],
    }

    def __init__(self, config: AgentConfig | None = None):
        """Initialize Warehouse Architect."""
        super().__init__(config or AgentConfig(name="WarehouseArchitect"))
        self._designs: dict[str, StarSchemaDesign] = {}

    def get_capabilities(self) -> list[AgentCapability]:
        """Get supported capabilities."""
        return [
            AgentCapability.DESIGN_STAR_SCHEMA,
            AgentCapability.GENERATE_DIMS,
            AgentCapability.GENERATE_FACTS,
            AgentCapability.DBT_MODELS,
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
            if capability == AgentCapability.DESIGN_STAR_SCHEMA:
                data = self._design_star_schema(context, **kwargs)
            elif capability == AgentCapability.GENERATE_DIMS:
                data = self._generate_dims(context, **kwargs)
            elif capability == AgentCapability.GENERATE_FACTS:
                data = self._generate_facts(context, **kwargs)
            elif capability == AgentCapability.DBT_MODELS:
                data = self._generate_dbt_models(context, **kwargs)
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

    def _design_star_schema(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Design a star schema from source tables.

        Input data:
            - tables: List of source table metadata
            - relationships: Table relationships
            - business_process: Business process name
        """
        self._report_progress("Designing star schema", 0.0)

        input_data = context.input_data
        tables = input_data.get("tables", [])
        relationships = input_data.get("relationships", [])
        business_process = input_data.get("business_process", "analytics")

        if not tables:
            raise AgentError("No tables provided", self.name, "design_star_schema")

        # Classify tables
        classified = self._classify_tables(tables)

        self._report_progress("Classified tables", 0.3)

        # Identify dimensions
        dimensions = self._identify_dimensions(
            classified["dimension_candidates"],
            relationships,
        )

        self._report_progress("Identified dimensions", 0.5)

        # Identify facts
        facts = self._identify_facts(
            classified["fact_candidates"],
            dimensions,
            relationships,
        )

        self._report_progress("Identified facts", 0.7)

        # Build design
        design = StarSchemaDesign(
            name=f"{business_process}_star",
            facts=facts,
            dimensions=dimensions,
            conformed_dimensions=self._find_conformed_dimensions(dimensions),
            description=f"Star schema for {business_process}",
        )

        # Store design
        self._designs[design.name] = design

        self._report_progress("Star schema design complete", 1.0)

        return {
            "design_name": design.name,
            "fact_count": len(design.facts),
            "dimension_count": len(design.dimensions),
            "conformed_dimensions": design.conformed_dimensions,
            "design": design.to_dict(),
        }

    def _generate_dims(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Generate dimension specifications.

        Input data:
            - tables: Source tables for dimensions
            - hierarchies: Optional hierarchy definitions
            - scd_type: SCD type to use (default: 2)
        """
        self._report_progress("Generating dimensions", 0.0)

        input_data = context.input_data
        tables = input_data.get("tables", [])
        hierarchies = input_data.get("hierarchies", [])
        scd_type = input_data.get("scd_type", 2)

        dimensions = []

        for i, table in enumerate(tables):
            self._report_progress(f"Processing {table.get('name', 'table')}", i / len(tables))

            dim = self._create_dimension_spec(table, hierarchies, scd_type)
            dimensions.append(dim)

        self._report_progress("Dimension generation complete", 1.0)

        return {
            "dimension_count": len(dimensions),
            "dimensions": [d.to_dict() for d in dimensions],
        }

    def _generate_facts(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Generate fact table specifications.

        Input data:
            - tables: Source tables for facts
            - dimensions: Available dimension specs
            - grain: Grain description
        """
        self._report_progress("Generating facts", 0.0)

        input_data = context.input_data
        tables = input_data.get("tables", [])
        dimensions = input_data.get("dimensions", [])
        grain = input_data.get("grain", "")

        facts = []

        for i, table in enumerate(tables):
            self._report_progress(f"Processing {table.get('name', 'table')}", i / len(tables))

            fact = self._create_fact_spec(table, dimensions, grain)
            facts.append(fact)

        self._report_progress("Fact generation complete", 1.0)

        return {
            "fact_count": len(facts),
            "facts": [f.to_dict() for f in facts],
        }

    def _generate_dbt_models(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Generate dbt models from design.

        Input data:
            - design: Star schema design
            - project_name: dbt project name
            - target_schema: Target schema
        """
        self._report_progress("Generating dbt models", 0.0)

        input_data = context.input_data
        design_data = input_data.get("design", {})
        project_name = input_data.get("project_name", "analytics")
        target_schema = input_data.get("target_schema", "ANALYTICS")

        models = []

        # Generate dimension models
        for dim_data in design_data.get("dimensions", []):
            model = self._generate_dim_dbt_model(dim_data, target_schema)
            models.append(model)

        self._report_progress("Generated dimension models", 0.5)

        # Generate fact models
        for fact_data in design_data.get("facts", []):
            model = self._generate_fact_dbt_model(fact_data, target_schema)
            models.append(model)

        self._report_progress("dbt model generation complete", 1.0)

        return {
            "project_name": project_name,
            "model_count": len(models),
            "models": models,
        }

    def _classify_tables(self, tables: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """Classify tables as dimension or fact candidates."""
        result = {
            "dimension_candidates": [],
            "fact_candidates": [],
            "unknown": [],
        }

        for table in tables:
            table_name = table.get("name", "").lower()
            columns = table.get("columns", [])

            # Check naming conventions
            if table_name.startswith(("dim_", "d_")):
                result["dimension_candidates"].append(table)
                continue
            if table_name.startswith(("fact_", "fct_", "f_")):
                result["fact_candidates"].append(table)
                continue

            # Analyze columns
            col_names = [c.get("name", "").lower() for c in columns]
            fk_count = sum(1 for c in col_names if c.endswith("_id"))
            measure_count = sum(1 for c in col_names if any(
                m in c for m in ["amount", "qty", "quantity", "total", "sum", "count"]
            ))

            # Facts typically have many FKs and measures
            if fk_count >= 3 and measure_count >= 1:
                result["fact_candidates"].append(table)
            # Dimensions typically have descriptive columns
            elif any("name" in c or "description" in c or "desc" in c for c in col_names):
                result["dimension_candidates"].append(table)
            else:
                result["unknown"].append(table)

        return result

    def _identify_dimensions(
        self,
        candidates: list[dict[str, Any]],
        relationships: list[dict[str, Any]],
    ) -> list[DimensionSpec]:
        """Identify and create dimension specs from candidates."""
        dimensions = []

        for table in candidates:
            table_name = table.get("name", "")
            columns = table.get("columns", [])

            # Find key column
            key_column = self._find_primary_key(columns)

            # Build attributes
            attributes = []
            for col in columns:
                col_name = col.get("name", "")
                if col_name != key_column:
                    attributes.append({
                        "name": col_name,
                        "data_type": col.get("data_type", "VARCHAR"),
                        "source_column": col_name,
                    })

            # Infer hierarchy levels
            hierarchy_levels = self._infer_hierarchy_levels(table_name, columns)

            dim = DimensionSpec(
                name=f"dim_{self._clean_name(table_name)}",
                source_table=table_name,
                key_column=key_column or "id",
                attributes=attributes,
                hierarchy_levels=hierarchy_levels,
                scd_type=2,
                description=f"Dimension for {table_name}",
            )
            dimensions.append(dim)

        return dimensions

    def _identify_facts(
        self,
        candidates: list[dict[str, Any]],
        dimensions: list[DimensionSpec],
        relationships: list[dict[str, Any]],
    ) -> list[FactSpec]:
        """Identify and create fact specs from candidates."""
        facts = []
        dim_names = {d.source_table.lower(): d.name for d in dimensions}

        for table in candidates:
            table_name = table.get("name", "")
            columns = table.get("columns", [])

            # Find dimension references
            fact_dims = []
            degenerate_dims = []
            measures = []

            for col in columns:
                col_name = col.get("name", "").lower()
                data_type = col.get("data_type", "").lower()

                # Check if FK to a dimension
                if col_name.endswith("_id"):
                    ref_table = col_name[:-3]
                    if ref_table in dim_names:
                        fact_dims.append(dim_names[ref_table])
                    else:
                        degenerate_dims.append(col.get("name"))
                # Check if measure
                elif any(t in data_type for t in ["int", "decimal", "numeric", "float", "number"]):
                    agg_type = self._determine_aggregation(col_name)
                    measures.append({
                        "name": col.get("name"),
                        "data_type": col.get("data_type"),
                        "aggregation": agg_type,
                    })

            fact = FactSpec(
                name=f"fact_{self._clean_name(table_name)}",
                source_tables=[table_name],
                grain=f"One row per {table_name}",
                dimensions=fact_dims,
                measures=measures,
                degenerate_dimensions=degenerate_dims,
                description=f"Fact table for {table_name}",
            )
            facts.append(fact)

        return facts

    def _create_dimension_spec(
        self,
        table: dict[str, Any],
        hierarchies: list[dict[str, Any]],
        scd_type: int,
    ) -> DimensionSpec:
        """Create a dimension specification from a table."""
        table_name = table.get("name", "")
        columns = table.get("columns", [])

        # Find key column
        key_column = self._find_primary_key(columns)

        # Build attributes
        attributes = []
        for col in columns:
            col_name = col.get("name", "")
            if col_name != key_column:
                attributes.append({
                    "name": col_name,
                    "data_type": col.get("data_type", "VARCHAR"),
                    "source_column": col_name,
                })

        # Check hierarchies for levels
        hierarchy_levels = []
        for hier in hierarchies:
            if hier.get("source_table") == table_name:
                hierarchy_levels = hier.get("levels", [])
                break

        if not hierarchy_levels:
            hierarchy_levels = self._infer_hierarchy_levels(table_name, columns)

        return DimensionSpec(
            name=f"dim_{self._clean_name(table_name)}",
            source_table=table_name,
            key_column=key_column or "id",
            attributes=attributes,
            hierarchy_levels=hierarchy_levels,
            scd_type=scd_type,
        )

    def _create_fact_spec(
        self,
        table: dict[str, Any],
        dimensions: list[dict[str, Any]],
        grain: str,
    ) -> FactSpec:
        """Create a fact specification from a table."""
        table_name = table.get("name", "")
        columns = table.get("columns", [])

        dim_names = [d.get("name", "") for d in dimensions]
        fact_dims = []
        measures = []
        degenerate = []

        for col in columns:
            col_name = col.get("name", "").lower()
            data_type = col.get("data_type", "").lower()

            # Check FK references
            if col_name.endswith("_id"):
                ref_dim = f"dim_{col_name[:-3]}"
                if ref_dim in dim_names:
                    fact_dims.append(ref_dim)
                else:
                    degenerate.append(col.get("name"))
            # Check measures
            elif any(t in data_type for t in ["int", "decimal", "numeric", "float"]):
                measures.append({
                    "name": col.get("name"),
                    "data_type": col.get("data_type"),
                    "aggregation": self._determine_aggregation(col_name),
                })

        return FactSpec(
            name=f"fact_{self._clean_name(table_name)}",
            source_tables=[table_name],
            grain=grain or f"One row per {table_name}",
            dimensions=fact_dims,
            measures=measures,
            degenerate_dimensions=degenerate,
        )

    def _generate_dim_dbt_model(
        self,
        dim_data: dict[str, Any],
        target_schema: str,
    ) -> dict[str, Any]:
        """Generate dbt model for a dimension."""
        dim_name = dim_data.get("name", "dim_unknown")
        source_table = dim_data.get("source_table", "")
        key_column = dim_data.get("key_column", "id")
        attributes = dim_data.get("attributes", [])

        # Build column list
        columns = [f"    {key_column} as {dim_name}_sk"]
        for attr in attributes:
            columns.append(f"    {attr.get('source_column', attr.get('name'))} as {attr.get('name')}")

        columns_str = ",\n".join(columns)

        sql = f"""-- dbt model for {dim_name}
{{{{ config(
    materialized='table',
    schema='{target_schema}'
) }}}}

WITH source AS (
    SELECT * FROM {{{{ source('raw', '{source_table}') }}}}
),

final AS (
    SELECT
{columns_str},
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM source
)

SELECT * FROM final
"""

        return {
            "name": dim_name,
            "type": "dimension",
            "sql": sql,
            "path": f"models/marts/{dim_name}.sql",
        }

    def _generate_fact_dbt_model(
        self,
        fact_data: dict[str, Any],
        target_schema: str,
    ) -> dict[str, Any]:
        """Generate dbt model for a fact."""
        fact_name = fact_data.get("name", "fact_unknown")
        source_tables = fact_data.get("source_tables", [])
        dimensions = fact_data.get("dimensions", [])
        measures = fact_data.get("measures", [])

        source_table = source_tables[0] if source_tables else "source"

        # Build columns
        columns = []
        for dim in dimensions:
            columns.append(f"    {dim}_sk")
        for measure in measures:
            columns.append(f"    {measure.get('name')}")

        columns_str = ",\n".join(columns) if columns else "    *"

        sql = f"""-- dbt model for {fact_name}
{{{{ config(
    materialized='table',
    schema='{target_schema}'
) }}}}

WITH source AS (
    SELECT * FROM {{{{ source('raw', '{source_table}') }}}}
),

final AS (
    SELECT
{columns_str},
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM source
)

SELECT * FROM final
"""

        return {
            "name": fact_name,
            "type": "fact",
            "sql": sql,
            "path": f"models/marts/{fact_name}.sql",
        }

    def _find_primary_key(self, columns: list[dict[str, Any]]) -> str | None:
        """Find primary key column."""
        for col in columns:
            if col.get("is_primary_key"):
                return col.get("name")
            col_name = col.get("name", "").lower()
            if col_name == "id" or col_name.endswith("_id"):
                return col.get("name")
        return None

    def _infer_hierarchy_levels(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
    ) -> list[str]:
        """Infer hierarchy levels from table."""
        levels = []
        col_names = [c.get("name", "").lower() for c in columns]

        # Check for level columns
        for col_name in col_names:
            if col_name.startswith("level_") or "level" in col_name:
                levels.append(col_name)

        # Check common patterns
        table_lower = table_name.lower()
        if "date" in table_lower or "time" in table_lower:
            return ["year", "quarter", "month", "day"]
        if "geo" in table_lower or "location" in table_lower:
            return ["country", "region", "state", "city"]
        if "product" in table_lower:
            return ["category", "subcategory", "product"]
        if "org" in table_lower or "employee" in table_lower:
            return ["department", "team", "employee"]

        return levels

    def _find_conformed_dimensions(self, dimensions: list[DimensionSpec]) -> list[str]:
        """Find conformed dimensions (shared across multiple facts)."""
        # Common conformed dimensions
        conformed_patterns = ["date", "time", "customer", "product", "geography", "employee"]

        conformed = []
        for dim in dimensions:
            dim_lower = dim.name.lower()
            if any(p in dim_lower for p in conformed_patterns):
                conformed.append(dim.name)

        return conformed

    def _determine_aggregation(self, column_name: str) -> str:
        """Determine aggregation type for a measure."""
        col_lower = column_name.lower()

        for agg_type, patterns in self.MEASURE_PATTERNS.items():
            if any(p in col_lower for p in patterns):
                return agg_type

        return "sum"

    def _clean_name(self, name: str) -> str:
        """Clean table name for use in model names."""
        import re
        # Remove common prefixes
        name = re.sub(r'^(dim_|fact_|fct_|d_|f_|stg_|raw_)', '', name.lower())
        # Clean special chars
        name = re.sub(r'[^a-z0-9_]', '_', name)
        return name
