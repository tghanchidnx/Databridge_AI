"""
dbt Generator for creating dbt models and project configurations.

This module generates complete dbt projects from discovered hierarchies:
- dbt_project.yml configuration
- Source definitions
- Staging models
- Intermediate models
- Mart models
- Schema files with tests
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


class DbtMaterialization(str, Enum):
    """dbt materialization strategies."""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class DbtModelLayer(str, Enum):
    """dbt model layers."""

    STAGING = "staging"
    INTERMEDIATE = "intermediate"
    MARTS = "marts"


@dataclass
class DbtModel:
    """A dbt model definition."""

    name: str
    layer: DbtModelLayer
    sql: str
    description: str = ""
    materialization: DbtMaterialization = DbtMaterialization.VIEW
    schema_name: str | None = None
    columns: list[dict[str, Any]] = field(default_factory=list)
    tests: list[dict[str, Any]] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def file_path(self) -> str:
        """Get relative file path for the model."""
        return f"models/{self.layer.value}/{self.name}.sql"


@dataclass
class DbtSource:
    """A dbt source definition."""

    name: str
    database: str
    schema: str
    tables: list[dict[str, Any]] = field(default_factory=list)
    description: str = ""


@dataclass
class DbtProject:
    """A complete dbt project."""

    name: str
    version: str = "1.0.0"
    config_version: int = 2
    models: list[DbtModel] = field(default_factory=list)
    sources: list[DbtSource] = field(default_factory=list)
    profile: str = "default"
    model_paths: list[str] = field(default_factory=lambda: ["models"])
    test_paths: list[str] = field(default_factory=lambda: ["tests"])
    seed_paths: list[str] = field(default_factory=lambda: ["seeds"])
    macro_paths: list[str] = field(default_factory=lambda: ["macros"])
    vars: dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def model_count(self) -> int:
        return len(self.models)

    def get_models_by_layer(self, layer: DbtModelLayer) -> list[DbtModel]:
        """Get models for a specific layer."""
        return [m for m in self.models if m.layer == layer]


@dataclass
class DbtGeneratorConfig:
    """Configuration for dbt generation."""

    project_name: str
    source_database: str = "RAW"
    source_schema: str = "HIERARCHIES"
    target_schema: str = "ANALYTICS"
    generate_tests: bool = True
    generate_docs: bool = True
    materialization: DbtMaterialization = DbtMaterialization.TABLE
    include_staging: bool = True
    include_intermediate: bool = True
    include_marts: bool = True
    tags: list[str] = field(default_factory=lambda: ["hierarchy", "discovery"])


class DbtGenerator:
    """
    Generates dbt projects from discovered hierarchies.

    Creates a complete dbt project structure with:
    - Project configuration
    - Source definitions
    - Staging models (stg_*)
    - Intermediate models (int_*)
    - Mart models (dim_*, fct_*)
    - Schema files with tests

    Example:
        generator = DbtGenerator()

        # Generate project from hierarchies
        project = generator.generate_project(
            hierarchies=[hierarchy1, hierarchy2],
            config=DbtGeneratorConfig(
                project_name="hierarchy_analytics",
                source_database="RAW"
            )
        )

        # Write to disk
        generator.write_project(project, output_dir="./dbt_project")
    """

    def __init__(self):
        """Initialize the dbt generator."""
        pass

    def generate_project(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: DbtGeneratorConfig,
    ) -> DbtProject:
        """
        Generate a complete dbt project from hierarchies.

        Args:
            hierarchies: List of hierarchies to include
            config: Generation configuration

        Returns:
            Complete DbtProject
        """
        project = DbtProject(
            name=self._sanitize_name(config.project_name),
            profile=config.project_name,
        )

        # Generate sources
        project.sources = self._generate_sources(hierarchies, config)

        # Generate staging models
        if config.include_staging:
            for hier in hierarchies:
                staging_model = self._generate_staging_model(hier, config)
                project.models.append(staging_model)

        # Generate intermediate models
        if config.include_intermediate:
            for hier in hierarchies:
                int_model = self._generate_intermediate_model(hier, config)
                project.models.append(int_model)

        # Generate mart models
        if config.include_marts:
            for hier in hierarchies:
                dim_model = self._generate_dimension_model(hier, config)
                project.models.append(dim_model)

        return project

    def generate_model(
        self,
        hierarchy: ConvertedHierarchy,
        layer: DbtModelLayer,
        config: DbtGeneratorConfig | None = None,
    ) -> DbtModel:
        """
        Generate a single dbt model for a hierarchy.

        Args:
            hierarchy: Hierarchy to generate model for
            layer: Model layer (staging, intermediate, marts)
            config: Generation configuration

        Returns:
            Generated DbtModel
        """
        config = config or DbtGeneratorConfig(project_name="default")

        if layer == DbtModelLayer.STAGING:
            return self._generate_staging_model(hierarchy, config)
        elif layer == DbtModelLayer.INTERMEDIATE:
            return self._generate_intermediate_model(hierarchy, config)
        elif layer == DbtModelLayer.MARTS:
            return self._generate_dimension_model(hierarchy, config)
        else:
            raise ValueError(f"Unsupported layer: {layer}")

    def _generate_sources(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: DbtGeneratorConfig,
    ) -> list[DbtSource]:
        """Generate source definitions."""
        tables = []

        for hier in hierarchies:
            table_name = self._sanitize_name(f"TBL_0_{hier.name}")
            tables.append({
                "name": table_name,
                "description": f"Hierarchy table for {hier.name}",
                "columns": self._get_source_columns(hier),
            })

            # Add mapping table if exists
            mapping_table = self._sanitize_name(f"TBL_0_{hier.name}_MAPPING")
            tables.append({
                "name": mapping_table,
                "description": f"Mapping table for {hier.name}",
            })

        return [DbtSource(
            name="hierarchy_raw",
            database=config.source_database,
            schema=config.source_schema,
            tables=tables,
            description="Raw hierarchy tables from The Librarian",
        )]

    def _generate_staging_model(
        self,
        hierarchy: ConvertedHierarchy,
        config: DbtGeneratorConfig,
    ) -> DbtModel:
        """Generate a staging model."""
        model_name = f"stg_{self._sanitize_name(hierarchy.name).lower()}"
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        # Build column list
        select_columns = self._build_staging_columns(hierarchy)

        sql = f"""-- Staging model for {hierarchy.name}
{{{{ config(
    materialized='view',
    schema='{config.target_schema}'
) }}}}

WITH source AS (
    SELECT * FROM {{{{ source('hierarchy_raw', '{source_table}') }}}}
),

renamed AS (
    SELECT
        {select_columns}
    FROM source
    WHERE include_flag = TRUE
)

SELECT * FROM renamed
"""

        # Build columns with tests
        columns = self._build_model_columns(hierarchy, include_tests=config.generate_tests)

        return DbtModel(
            name=model_name,
            layer=DbtModelLayer.STAGING,
            sql=sql,
            description=f"Staging model for {hierarchy.name} hierarchy",
            materialization=DbtMaterialization.VIEW,
            schema_name=config.target_schema,
            columns=columns,
            tags=config.tags + [hierarchy.entity_type],
            meta={
                "hierarchy_id": hierarchy.id,
                "entity_type": hierarchy.entity_type,
                "level_count": hierarchy.level_count,
            },
        )

    def _generate_intermediate_model(
        self,
        hierarchy: ConvertedHierarchy,
        config: DbtGeneratorConfig,
    ) -> DbtModel:
        """Generate an intermediate model."""
        model_name = f"int_{self._sanitize_name(hierarchy.name).lower()}"
        staging_model = f"stg_{self._sanitize_name(hierarchy.name).lower()}"

        sql = f"""-- Intermediate model for {hierarchy.name}
{{{{ config(
    materialized='table',
    schema='{config.target_schema}'
) }}}}

WITH staging AS (
    SELECT * FROM {{{{ ref('{staging_model}') }}}}
),

with_path AS (
    SELECT
        *,
        -- Build full hierarchy path
        COALESCE(level_1, '') ||
        CASE WHEN level_2 IS NOT NULL THEN ' > ' || level_2 ELSE '' END ||
        CASE WHEN level_3 IS NOT NULL THEN ' > ' || level_3 ELSE '' END ||
        CASE WHEN level_4 IS NOT NULL THEN ' > ' || level_4 ELSE '' END ||
        CASE WHEN level_5 IS NOT NULL THEN ' > ' || level_5 ELSE '' END
        AS hierarchy_path,

        -- Calculate depth
        CASE
            WHEN level_5 IS NOT NULL THEN 5
            WHEN level_4 IS NOT NULL THEN 4
            WHEN level_3 IS NOT NULL THEN 3
            WHEN level_2 IS NOT NULL THEN 2
            WHEN level_1 IS NOT NULL THEN 1
            ELSE 0
        END AS hierarchy_depth

    FROM staging
)

SELECT * FROM with_path
"""

        return DbtModel(
            name=model_name,
            layer=DbtModelLayer.INTERMEDIATE,
            sql=sql,
            description=f"Intermediate model for {hierarchy.name} with calculated fields",
            materialization=DbtMaterialization.TABLE,
            schema_name=config.target_schema,
            depends_on=[staging_model],
            tags=config.tags,
        )

    def _generate_dimension_model(
        self,
        hierarchy: ConvertedHierarchy,
        config: DbtGeneratorConfig,
    ) -> DbtModel:
        """Generate a dimension mart model."""
        model_name = f"dim_{self._sanitize_name(hierarchy.name).lower()}"
        int_model = f"int_{self._sanitize_name(hierarchy.name).lower()}"

        sql = f"""-- Dimension model for {hierarchy.name}
{{{{ config(
    materialized='table',
    schema='{config.target_schema}',
    unique_key='hierarchy_sk'
) }}}}

WITH intermediate AS (
    SELECT * FROM {{{{ ref('{int_model}') }}}}
),

final AS (
    SELECT
        -- Surrogate key
        {{{{ dbt_utils.generate_surrogate_key(['hierarchy_id']) }}}} AS hierarchy_sk,

        -- Natural key
        hierarchy_id,
        hierarchy_name,
        parent_id,

        -- Hierarchy levels
        level_1,
        level_2,
        level_3,
        level_4,
        level_5,

        -- Sort orders
        level_1_sort,
        level_2_sort,
        level_3_sort,

        -- Calculated fields
        hierarchy_path,
        hierarchy_depth,

        -- Flags
        include_flag,
        exclude_flag,
        formula_group,
        sort_order,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM intermediate
)

SELECT * FROM final
"""

        columns = [
            {
                "name": "hierarchy_sk",
                "description": "Surrogate key",
                "tests": ["unique", "not_null"],
            },
            {
                "name": "hierarchy_id",
                "description": "Natural key from source",
                "tests": ["unique", "not_null"],
            },
            {
                "name": "hierarchy_name",
                "description": "Display name",
                "tests": ["not_null"],
            },
        ]

        return DbtModel(
            name=model_name,
            layer=DbtModelLayer.MARTS,
            sql=sql,
            description=f"Dimension model for {hierarchy.name}",
            materialization=DbtMaterialization.TABLE,
            schema_name=config.target_schema,
            columns=columns,
            depends_on=[int_model],
            tags=config.tags + ["dim"],
        )

    def _build_staging_columns(self, hierarchy: ConvertedHierarchy) -> str:
        """Build column list for staging model."""
        columns = [
            "hierarchy_id",
            "hierarchy_name",
            "parent_id",
        ]

        # Add level columns
        for i in range(1, min(hierarchy.level_count + 2, 11)):
            columns.append(f"level_{i}")
            columns.append(f"level_{i}_sort")

        # Add standard columns
        columns.extend([
            "include_flag",
            "exclude_flag",
            "formula_group",
            "sort_order",
        ])

        return ",\n        ".join(columns)

    def _build_model_columns(
        self,
        hierarchy: ConvertedHierarchy,
        include_tests: bool = True,
    ) -> list[dict[str, Any]]:
        """Build column definitions with optional tests."""
        columns = [
            {
                "name": "hierarchy_id",
                "description": "Unique hierarchy node identifier",
                "tests": ["unique", "not_null"] if include_tests else [],
            },
            {
                "name": "hierarchy_name",
                "description": "Display name of the hierarchy node",
                "tests": ["not_null"] if include_tests else [],
            },
            {
                "name": "parent_id",
                "description": "Parent node identifier",
            },
        ]

        # Add level columns
        for i in range(1, min(hierarchy.level_count + 2, 6)):
            columns.append({
                "name": f"level_{i}",
                "description": f"Hierarchy level {i} value",
            })
            columns.append({
                "name": f"level_{i}_sort",
                "description": f"Sort order for level {i}",
            })

        return columns

    def _get_source_columns(self, hierarchy: ConvertedHierarchy) -> list[dict[str, str]]:
        """Get source column definitions."""
        return [
            {"name": "HIERARCHY_ID", "description": "Unique node identifier"},
            {"name": "HIERARCHY_NAME", "description": "Display name"},
            {"name": "PARENT_ID", "description": "Parent node ID"},
            {"name": "LEVEL_1", "description": "Level 1 value"},
            {"name": "LEVEL_2", "description": "Level 2 value"},
            {"name": "LEVEL_3", "description": "Level 3 value"},
            {"name": "INCLUDE_FLAG", "description": "Include in output"},
            {"name": "EXCLUDE_FLAG", "description": "Exclude from output"},
        ]

    def generate_schema_yml(self, project: DbtProject) -> str:
        """Generate schema.yml content."""
        schema = {
            "version": 2,
            "models": [],
        }

        for model in project.models:
            model_def = {
                "name": model.name,
                "description": model.description,
            }

            if model.columns:
                model_def["columns"] = []
                for col in model.columns:
                    col_def = {
                        "name": col["name"],
                        "description": col.get("description", ""),
                    }
                    if col.get("tests"):
                        col_def["tests"] = col["tests"]
                    model_def["columns"].append(col_def)

            if model.tags:
                model_def["tags"] = model.tags

            if model.meta:
                model_def["meta"] = model.meta

            schema["models"].append(model_def)

        return self._to_yaml(schema)

    def generate_sources_yml(self, project: DbtProject) -> str:
        """Generate sources.yml content."""
        sources = {
            "version": 2,
            "sources": [],
        }

        for source in project.sources:
            source_def = {
                "name": source.name,
                "database": source.database,
                "schema": source.schema,
                "description": source.description,
                "tables": source.tables,
            }
            sources["sources"].append(source_def)

        return self._to_yaml(sources)

    def generate_dbt_project_yml(self, project: DbtProject) -> str:
        """Generate dbt_project.yml content."""
        config = {
            "name": project.name,
            "version": project.version,
            "config-version": project.config_version,
            "profile": project.profile,
            "model-paths": project.model_paths,
            "test-paths": project.test_paths,
            "seed-paths": project.seed_paths,
            "macro-paths": project.macro_paths,
            "target-path": "target",
            "clean-targets": ["target", "dbt_packages"],
        }

        if project.vars:
            config["vars"] = project.vars

        return self._to_yaml(config)

    def write_project(
        self,
        project: DbtProject,
        output_dir: str,
    ) -> dict[str, str]:
        """
        Write dbt project to disk.

        Args:
            project: DbtProject to write
            output_dir: Output directory

        Returns:
            Dictionary of created file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        files_created: dict[str, str] = {}

        # Write dbt_project.yml
        project_yml_path = output_path / "dbt_project.yml"
        with open(project_yml_path, "w", encoding="utf-8") as f:
            f.write(self.generate_dbt_project_yml(project))
        files_created["dbt_project.yml"] = str(project_yml_path)

        # Write sources.yml
        sources_dir = output_path / "models"
        sources_dir.mkdir(exist_ok=True)
        sources_path = sources_dir / "sources.yml"
        with open(sources_path, "w", encoding="utf-8") as f:
            f.write(self.generate_sources_yml(project))
        files_created["sources.yml"] = str(sources_path)

        # Write schema.yml
        schema_path = sources_dir / "schema.yml"
        with open(schema_path, "w", encoding="utf-8") as f:
            f.write(self.generate_schema_yml(project))
        files_created["schema.yml"] = str(schema_path)

        # Write model files
        for model in project.models:
            model_dir = output_path / "models" / model.layer.value
            model_dir.mkdir(parents=True, exist_ok=True)

            model_path = model_dir / f"{model.name}.sql"
            with open(model_path, "w", encoding="utf-8") as f:
                f.write(model.sql)
            files_created[model.name] = str(model_path)

        return files_created

    def to_dict(self, project: DbtProject) -> dict[str, Any]:
        """Convert DbtProject to dictionary."""
        return {
            "name": project.name,
            "version": project.version,
            "model_count": project.model_count,
            "sources": [
                {
                    "name": s.name,
                    "database": s.database,
                    "schema": s.schema,
                    "table_count": len(s.tables),
                }
                for s in project.sources
            ],
            "models": [
                {
                    "name": m.name,
                    "layer": m.layer.value,
                    "materialization": m.materialization.value,
                    "file_path": m.file_path,
                }
                for m in project.models
            ],
            "generated_at": project.generated_at.isoformat(),
        }

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use in dbt identifiers."""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_')
        return sanitized

    def _to_yaml(self, data: dict) -> str:
        """Convert dict to YAML string."""
        try:
            import yaml
            return yaml.dump(data, default_flow_style=False, sort_keys=False)
        except ImportError:
            # Simple YAML-like fallback
            import json
            return json.dumps(data, indent=2)
