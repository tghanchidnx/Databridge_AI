"""
dbt Project Generator for creating dbt projects from hierarchy projects.

Generates complete dbt project structure:
- dbt_project.yml configuration
- Source definitions
- Staging models
- Mart models
- Schema files with tests
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import yaml


class DbtMaterialization(str, Enum):
    """dbt materialization strategies."""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


@dataclass
class GeneratedFile:
    """A generated file in the dbt project."""

    name: str
    path: str
    content: str
    file_type: str  # sql, yml, md


@dataclass
class GeneratedDbtProject:
    """A complete generated dbt project."""

    project_name: str
    files: List[GeneratedFile]
    model_count: int
    source_count: int
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def file_count(self) -> int:
        return len(self.files)


@dataclass
class DbtConfig:
    """Configuration for dbt project generation."""

    project_name: str
    source_database: str = "RAW"
    source_schema: str = "HIERARCHIES"
    target_schema: str = "ANALYTICS"
    profile_name: str = "default"

    # Generation options
    materialization: DbtMaterialization = DbtMaterialization.TABLE
    generate_tests: bool = True
    generate_docs: bool = True
    generate_staging: bool = True
    generate_marts: bool = True

    # Tags
    tags: List[str] = field(default_factory=lambda: ["hierarchy", "databridge"])


class DbtProjectGenerator:
    """
    Generates dbt projects from hierarchy projects.

    Creates a complete dbt project structure with:
    - Project configuration (dbt_project.yml)
    - Source definitions (sources.yml)
    - Staging models (stg_*)
    - Mart models (dim_hierarchy)
    - Schema files with tests

    Example:
        generator = DbtProjectGenerator()

        project = generator.generate(
            project=hierarchy_project,
            hierarchies=hierarchies,
            config=DbtConfig(
                project_name="hierarchy_analytics",
                source_database="RAW"
            )
        )

        # Write to disk
        generator.write_project(project, output_dir="./dbt_project")
    """

    def __init__(self):
        """Initialize the dbt project generator."""
        pass

    def generate(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DbtConfig,
    ) -> GeneratedDbtProject:
        """
        Generate a complete dbt project.

        Args:
            project: Hierarchy project object.
            hierarchies: List of hierarchy objects.
            config: dbt configuration.

        Returns:
            GeneratedDbtProject with all generated files.
        """
        files: List[GeneratedFile] = []
        model_count = 0
        source_count = 0

        # 1. Generate dbt_project.yml
        files.append(self._generate_project_yml(project, config))

        # 2. Generate profiles.yml (template)
        files.append(self._generate_profiles_yml(config))

        # 3. Generate sources.yml
        sources_file, source_count = self._generate_sources_yml(project, config)
        files.append(sources_file)

        # 4. Generate staging models
        if config.generate_staging:
            staging_files = self._generate_staging_models(project, hierarchies, config)
            files.extend(staging_files)
            model_count += len([f for f in staging_files if f.file_type == "sql"])

        # 5. Generate mart models
        if config.generate_marts:
            mart_files = self._generate_mart_models(project, hierarchies, config)
            files.extend(mart_files)
            model_count += len([f for f in mart_files if f.file_type == "sql"])

        # 6. Generate README
        if config.generate_docs:
            files.append(self._generate_readme(project, config))

        return GeneratedDbtProject(
            project_name=config.project_name,
            files=files,
            model_count=model_count,
            source_count=source_count,
        )

    def _generate_project_yml(
        self,
        project: Any,
        config: DbtConfig,
    ) -> GeneratedFile:
        """Generate dbt_project.yml."""
        project_config = {
            "name": self._safe_name(config.project_name),
            "version": "1.0.0",
            "config-version": 2,
            "profile": config.profile_name,
            "model-paths": ["models"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "analysis-paths": ["analyses"],
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                self._safe_name(config.project_name): {
                    "staging": {
                        "+materialized": "view",
                        "+schema": "staging",
                        "+tags": ["staging"],
                    },
                    "marts": {
                        "+materialized": config.materialization.value,
                        "+schema": config.target_schema.lower(),
                        "+tags": ["marts", "hierarchy"],
                    },
                },
            },
            "vars": {
                "source_database": config.source_database,
                "source_schema": config.source_schema,
            },
        }

        content = yaml.dump(project_config, default_flow_style=False, sort_keys=False)

        return GeneratedFile(
            name="dbt_project.yml",
            path="dbt_project.yml",
            content=content,
            file_type="yml",
        )

    def _generate_profiles_yml(self, config: DbtConfig) -> GeneratedFile:
        """Generate profiles.yml template."""
        profiles = {
            config.profile_name: {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "snowflake",
                        "account": "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
                        "user": "{{ env_var('SNOWFLAKE_USER') }}",
                        "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}",
                        "role": "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORMER') }}",
                        "warehouse": "{{ env_var('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') }}",
                        "database": config.source_database,
                        "schema": config.target_schema,
                        "threads": 4,
                    },
                },
            },
        }

        content = "# Copy this to ~/.dbt/profiles.yml\n"
        content += yaml.dump(profiles, default_flow_style=False, sort_keys=False)

        return GeneratedFile(
            name="profiles.yml.template",
            path="profiles.yml.template",
            content=content,
            file_type="yml",
        )

    def _generate_sources_yml(
        self,
        project: Any,
        config: DbtConfig,
    ) -> tuple[GeneratedFile, int]:
        """Generate sources.yml."""
        sources = {
            "version": 2,
            "sources": [
                {
                    "name": "hierarchy_raw",
                    "description": f"Raw hierarchy data for {project.name}",
                    "database": "{{ var('source_database') }}",
                    "schema": "{{ var('source_schema') }}",
                    "tables": [
                        {
                            "name": f"TBL_0_{self._safe_name(project.name)}_HIERARCHY",
                            "description": "Base hierarchy table",
                            "columns": [
                                {"name": "HIERARCHY_ID", "description": "Unique hierarchy node identifier"},
                                {"name": "PROJECT_ID", "description": "Parent project identifier"},
                                {"name": "HIERARCHY_NAME", "description": "Display name"},
                                {"name": "PARENT_ID", "description": "Parent node identifier"},
                                {"name": "HIERARCHY_TYPE", "description": "Type: standard, grouping, xref, calculation"},
                                {"name": "LEVEL_1", "description": "Level 1 value"},
                                {"name": "LEVEL_2", "description": "Level 2 value"},
                                {"name": "LEVEL_3", "description": "Level 3 value"},
                                {"name": "SOURCE_MAPPINGS", "description": "JSON array of source mappings"},
                            ],
                        },
                    ],
                },
            ],
        }

        content = yaml.dump(sources, default_flow_style=False, sort_keys=False)

        return GeneratedFile(
            name="sources.yml",
            path="models/staging/sources.yml",
            content=content,
            file_type="yml",
        ), 1

    def _generate_staging_models(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DbtConfig,
    ) -> List[GeneratedFile]:
        """Generate staging models."""
        files: List[GeneratedFile] = []
        project_name = self._safe_name(project.name)

        # Staging model for hierarchy
        stg_sql = f"""{{{{ config(
    materialized='view',
    tags=['staging', 'hierarchy']
) }}}}

WITH source AS (
    SELECT * FROM {{{{ source('hierarchy_raw', 'TBL_0_{project_name}_HIERARCHY') }}}}
),

renamed AS (
    SELECT
        HIERARCHY_ID AS hierarchy_id,
        PROJECT_ID AS project_id,
        HIERARCHY_NAME AS hierarchy_name,
        DESCRIPTION AS description,
        PARENT_ID AS parent_id,
        HIERARCHY_TYPE AS hierarchy_type,
        AGGREGATION_METHOD AS aggregation_method,
        LEVEL_1 AS level_1,
        LEVEL_2 AS level_2,
        LEVEL_3 AS level_3,
        LEVEL_4 AS level_4,
        LEVEL_5 AS level_5,
        LEVEL_1_SORT AS level_1_sort,
        LEVEL_2_SORT AS level_2_sort,
        LEVEL_3_SORT AS level_3_sort,
        INCLUDE_FLAG AS include_flag,
        EXCLUDE_FLAG AS exclude_flag,
        TRANSFORM_FLAG AS transform_flag,
        CALCULATION_FLAG AS calculation_flag,
        ACTIVE_FLAG AS active_flag,
        IS_LEAF_NODE AS is_leaf_node,
        SOURCE_MAPPINGS AS source_mappings,
        FORMULA_CONFIG AS formula_config,
        SORT_ORDER AS sort_order,
        CREATED_AT AS created_at,
        UPDATED_AT AS updated_at
    FROM source
    WHERE ACTIVE_FLAG = TRUE
)

SELECT * FROM renamed
"""

        files.append(GeneratedFile(
            name=f"stg_hierarchy_{project_name.lower()}.sql",
            path=f"models/staging/stg_hierarchy_{project_name.lower()}.sql",
            content=stg_sql,
            file_type="sql",
        ))

        # Staging schema with tests
        if config.generate_tests:
            schema = {
                "version": 2,
                "models": [
                    {
                        "name": f"stg_hierarchy_{project_name.lower()}",
                        "description": f"Staged hierarchy data for {project.name}",
                        "columns": [
                            {
                                "name": "hierarchy_id",
                                "description": "Unique identifier",
                                "tests": ["unique", "not_null"],
                            },
                            {
                                "name": "project_id",
                                "description": "Parent project",
                                "tests": ["not_null"],
                            },
                            {
                                "name": "hierarchy_name",
                                "description": "Display name",
                                "tests": ["not_null"],
                            },
                            {
                                "name": "hierarchy_type",
                                "description": "Type classification",
                                "tests": [
                                    {
                                        "accepted_values": {
                                            "values": ["standard", "grouping", "xref", "calculation", "allocation"],
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }

            files.append(GeneratedFile(
                name="stg_hierarchy_schema.yml",
                path="models/staging/stg_hierarchy_schema.yml",
                content=yaml.dump(schema, default_flow_style=False, sort_keys=False),
                file_type="yml",
            ))

        return files

    def _generate_mart_models(
        self,
        project: Any,
        hierarchies: List[Any],
        config: DbtConfig,
    ) -> List[GeneratedFile]:
        """Generate mart models."""
        files: List[GeneratedFile] = []
        project_name = self._safe_name(project.name)

        # Dimension model
        dim_sql = f"""{{{{ config(
    materialized='{config.materialization.value}',
    tags=['marts', 'dimension', 'hierarchy']
) }}}}

WITH staged AS (
    SELECT * FROM {{{{ ref('stg_hierarchy_{project_name.lower()}') }}}}
),

-- Build the full hierarchy path
hierarchy_with_path AS (
    SELECT
        hierarchy_id,
        project_id,
        hierarchy_name,
        description,
        parent_id,
        hierarchy_type,
        aggregation_method,
        level_1,
        level_2,
        level_3,
        level_4,
        level_5,
        level_1_sort,
        level_2_sort,
        level_3_sort,
        is_leaf_node,
        include_flag,
        exclude_flag,
        calculation_flag,
        source_mappings,
        formula_config,
        sort_order,
        -- Build hierarchy path
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
        END AS hierarchy_depth,
        created_at,
        updated_at
    FROM staged
)

SELECT
    hierarchy_id,
    project_id,
    hierarchy_name,
    description,
    parent_id,
    hierarchy_type,
    aggregation_method,
    hierarchy_path,
    hierarchy_depth,
    level_1,
    level_2,
    level_3,
    level_4,
    level_5,
    level_1_sort,
    level_2_sort,
    level_3_sort,
    is_leaf_node,
    include_flag,
    exclude_flag,
    calculation_flag,
    source_mappings,
    formula_config,
    sort_order,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM hierarchy_with_path
WHERE include_flag = TRUE
  AND (exclude_flag = FALSE OR exclude_flag IS NULL)
"""

        files.append(GeneratedFile(
            name=f"dim_hierarchy_{project_name.lower()}.sql",
            path=f"models/marts/dim_hierarchy_{project_name.lower()}.sql",
            content=dim_sql,
            file_type="sql",
        ))

        # Mapping fact model (flattened source mappings)
        mapping_sql = f"""{{{{ config(
    materialized='{config.materialization.value}',
    tags=['marts', 'fact', 'mapping']
) }}}}

WITH dimension AS (
    SELECT * FROM {{{{ ref('dim_hierarchy_{project_name.lower()}') }}}}
    WHERE is_leaf_node = TRUE
),

-- Flatten source mappings
flattened AS (
    SELECT
        d.hierarchy_id,
        d.project_id,
        d.hierarchy_name,
        d.hierarchy_path,
        d.level_1,
        d.level_2,
        d.level_3,
        m.value:source_database::VARCHAR AS source_database,
        m.value:source_schema::VARCHAR AS source_schema,
        m.value:source_table::VARCHAR AS source_table,
        m.value:source_column::VARCHAR AS source_column,
        m.value:source_uid::VARCHAR AS source_uid,
        m.value:precedence_group::VARCHAR AS precedence_group,
        m.index AS mapping_index
    FROM dimension d,
    LATERAL FLATTEN(input => d.source_mappings, outer => true) m
)

SELECT
    hierarchy_id,
    project_id,
    hierarchy_name,
    hierarchy_path,
    level_1,
    level_2,
    level_3,
    source_database,
    source_schema,
    source_table,
    source_column,
    source_uid,
    precedence_group,
    mapping_index,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM flattened
WHERE source_table IS NOT NULL
"""

        files.append(GeneratedFile(
            name=f"fct_hierarchy_mapping_{project_name.lower()}.sql",
            path=f"models/marts/fct_hierarchy_mapping_{project_name.lower()}.sql",
            content=mapping_sql,
            file_type="sql",
        ))

        # Mart schema with tests
        if config.generate_tests:
            schema = {
                "version": 2,
                "models": [
                    {
                        "name": f"dim_hierarchy_{project_name.lower()}",
                        "description": f"Dimension table for {project.name} hierarchy",
                        "columns": [
                            {
                                "name": "hierarchy_id",
                                "description": "Primary key",
                                "tests": ["unique", "not_null"],
                            },
                            {
                                "name": "hierarchy_path",
                                "description": "Full path from root to node",
                            },
                            {
                                "name": "hierarchy_depth",
                                "description": "Depth in the hierarchy tree",
                            },
                        ],
                    },
                    {
                        "name": f"fct_hierarchy_mapping_{project_name.lower()}",
                        "description": f"Fact table for {project.name} source mappings",
                        "columns": [
                            {
                                "name": "hierarchy_id",
                                "description": "Foreign key to dimension",
                                "tests": ["not_null"],
                            },
                            {
                                "name": "source_table",
                                "description": "Source table name",
                                "tests": ["not_null"],
                            },
                        ],
                    },
                ],
            }

            files.append(GeneratedFile(
                name="marts_schema.yml",
                path="models/marts/marts_schema.yml",
                content=yaml.dump(schema, default_flow_style=False, sort_keys=False),
                file_type="yml",
            ))

        return files

    def _generate_readme(
        self,
        project: Any,
        config: DbtConfig,
    ) -> GeneratedFile:
        """Generate README.md."""
        content = f"""# {config.project_name}

dbt project generated by DataBridge AI Librarian.

## Project Structure

```
{config.project_name}/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   └── stg_hierarchy_*.sql
│   └── marts/
│       ├── dim_hierarchy_*.sql
│       └── fct_hierarchy_mapping_*.sql
└── README.md
```

## Models

### Staging Layer
- `stg_hierarchy_*`: Cleaned and renamed source data

### Marts Layer
- `dim_hierarchy_*`: Hierarchy dimension with full paths
- `fct_hierarchy_mapping_*`: Flattened source mappings fact table

## Configuration

1. Copy `profiles.yml.template` to `~/.dbt/profiles.yml`
2. Set environment variables:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ROLE` (optional)
   - `SNOWFLAKE_WAREHOUSE` (optional)

## Usage

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Generated

- Source Project: {project.name}
- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
- Tags: {', '.join(config.tags)}
"""

        return GeneratedFile(
            name="README.md",
            path="README.md",
            content=content,
            file_type="md",
        )

    def write_project(
        self,
        project: GeneratedDbtProject,
        output_dir: str,
    ) -> List[str]:
        """
        Write the generated project to disk.

        Args:
            project: Generated dbt project.
            output_dir: Output directory path.

        Returns:
            List of created file paths.
        """
        output_path = Path(output_dir)
        created_files = []

        for file in project.files:
            file_path = output_path / file.path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(file.content, encoding="utf-8")
            created_files.append(str(file_path))

        return created_files

    def _safe_name(self, name: str) -> str:
        """Convert name to safe identifier."""
        import re
        return re.sub(r"[^a-zA-Z0-9_]", "_", name).upper()
