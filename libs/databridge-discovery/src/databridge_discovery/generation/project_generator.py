"""
Project Generator for creating complete Librarian projects.

This module generates complete hierarchy projects including:
- TBL_0 hierarchy data tables
- VW_1 unnest views
- DT_2 dimension tables
- DT_3A aggregation tables
- DT_3 final union tables
- Deployment scripts
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


class OutputFormat(str, Enum):
    """Supported output formats."""

    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    BIGQUERY = "bigquery"
    TSQL = "tsql"
    DBT = "dbt"


class ProjectTier(str, Enum):
    """Project tier levels."""

    TBL_0 = "TBL_0"  # Base hierarchy tables
    VW_1 = "VW_1"    # Unnest views
    DT_2 = "DT_2"    # Dimension join tables
    DT_3A = "DT_3A"  # Aggregation tables
    DT_3 = "DT_3"    # Final union tables


@dataclass
class ProjectConfig:
    """Configuration for project generation."""

    project_name: str
    output_format: OutputFormat = OutputFormat.SNOWFLAKE
    target_database: str = ""
    target_schema: str = "HIERARCHIES"
    include_tiers: list[ProjectTier] = field(default_factory=lambda: [
        ProjectTier.TBL_0, ProjectTier.VW_1
    ])
    generate_dbt: bool = False
    generate_tests: bool = True
    include_documentation: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class GeneratedFile:
    """A generated file in the project."""

    name: str
    path: str
    content: str
    file_type: str  # sql, yml, md, json
    tier: ProjectTier | None = None
    description: str = ""


@dataclass
class GeneratedProject:
    """Result of project generation."""

    project_name: str
    config: ProjectConfig
    files: list[GeneratedFile]
    hierarchies: list[str]  # Hierarchy IDs included
    tiers_generated: list[ProjectTier]
    output_dir: str | None = None
    generated_at: datetime = field(default_factory=datetime.now)
    notes: list[str] = field(default_factory=list)

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def sql_files(self) -> list[GeneratedFile]:
        return [f for f in self.files if f.file_type == "sql"]

    @property
    def dbt_files(self) -> list[GeneratedFile]:
        return [f for f in self.files if f.file_type == "yml" or f.name.endswith(".sql")]


class ProjectGenerator:
    """
    Generates complete Librarian projects from discovered hierarchies.

    Creates a full project structure with:
    - Hierarchy tables (TBL_0)
    - Mapping views (VW_1)
    - Optional dimension tables (DT_2, DT_3A, DT_3)
    - Deployment scripts
    - Documentation

    Example:
        generator = ProjectGenerator()

        # Generate from a converted hierarchy
        project = generator.generate(
            hierarchies=[hierarchy],
            config=ProjectConfig(
                project_name="GL_HIERARCHIES",
                output_format=OutputFormat.SNOWFLAKE
            )
        )

        # Write to disk
        generator.write_project(project, output_dir="./output")
    """

    def __init__(self, dialect: str = "snowflake"):
        """
        Initialize the generator.

        Args:
            dialect: SQL dialect for DDL generation
        """
        self.dialect = dialect

    def generate(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: ProjectConfig,
    ) -> GeneratedProject:
        """
        Generate a complete project from hierarchies.

        Args:
            hierarchies: List of hierarchies to include
            config: Project configuration

        Returns:
            GeneratedProject with all generated files
        """
        files: list[GeneratedFile] = []
        notes: list[str] = []

        # Generate TBL_0 (hierarchy tables)
        if ProjectTier.TBL_0 in config.include_tiers:
            for hier in hierarchies:
                tbl_files = self._generate_tbl_0(hier, config)
                files.extend(tbl_files)
                notes.append(f"Generated TBL_0 for {hier.name}")

        # Generate VW_1 (mapping views)
        if ProjectTier.VW_1 in config.include_tiers:
            for hier in hierarchies:
                vw_files = self._generate_vw_1(hier, config)
                files.extend(vw_files)
                notes.append(f"Generated VW_1 for {hier.name}")

        # Generate DT_2 (dimension tables)
        if ProjectTier.DT_2 in config.include_tiers:
            for hier in hierarchies:
                dt_files = self._generate_dt_2(hier, config)
                files.extend(dt_files)
                notes.append(f"Generated DT_2 for {hier.name}")

        # Generate DT_3A (aggregation tables)
        if ProjectTier.DT_3A in config.include_tiers:
            for hier in hierarchies:
                dt3a_files = self._generate_dt_3a(hier, config)
                files.extend(dt3a_files)

        # Generate DT_3 (union tables)
        if ProjectTier.DT_3 in config.include_tiers:
            for hier in hierarchies:
                dt3_files = self._generate_dt_3(hier, config)
                files.extend(dt3_files)

        # Generate dbt models if requested
        if config.generate_dbt:
            dbt_files = self._generate_dbt_files(hierarchies, config)
            files.extend(dbt_files)
            notes.append("Generated dbt models")

        # Generate documentation if requested
        if config.include_documentation:
            doc_files = self._generate_documentation(hierarchies, config)
            files.extend(doc_files)
            notes.append("Generated documentation")

        # Generate deployment script
        deploy_file = self._generate_deployment_script(files, config)
        files.append(deploy_file)

        return GeneratedProject(
            project_name=config.project_name,
            config=config,
            files=files,
            hierarchies=[h.id for h in hierarchies],
            tiers_generated=config.include_tiers,
            notes=notes,
        )

    def _generate_tbl_0(
        self,
        hierarchy: ConvertedHierarchy,
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate TBL_0 hierarchy table."""
        files = []
        table_name = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        # Build CREATE TABLE DDL
        columns = [
            "HIERARCHY_ID VARCHAR(100) NOT NULL",
            "HIERARCHY_NAME VARCHAR(500)",
            "PARENT_ID VARCHAR(100)",
            "DESCRIPTION VARCHAR(2000)",
        ]

        # Add level columns
        for i in range(1, min(hierarchy.level_count + 2, 11)):
            columns.append(f"LEVEL_{i} VARCHAR(500)")
            columns.append(f"LEVEL_{i}_SORT INTEGER")

        # Add standard columns
        columns.extend([
            "INCLUDE_FLAG BOOLEAN DEFAULT TRUE",
            "EXCLUDE_FLAG BOOLEAN DEFAULT FALSE",
            "FORMULA_GROUP VARCHAR(100)",
            "SORT_ORDER INTEGER",
            "CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        ])

        # Build DDL
        ddl = self._build_create_table(
            table_name,
            columns,
            config,
            primary_key="HIERARCHY_ID",
        )

        # Generate INSERT statements for hierarchy data
        inserts = self._generate_hierarchy_inserts(hierarchy, table_name, config)

        files.append(GeneratedFile(
            name=f"{table_name}.sql",
            path=f"tables/{table_name}.sql",
            content=ddl + "\n\n" + inserts,
            file_type="sql",
            tier=ProjectTier.TBL_0,
            description=f"Hierarchy table for {hierarchy.name}",
        ))

        return files

    def _generate_vw_1(
        self,
        hierarchy: ConvertedHierarchy,
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate VW_1 mapping view."""
        files = []
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}")
        source_table = self._sanitize_name(f"TBL_0_{hierarchy.name}")

        # Build SELECT with LATERAL FLATTEN for Snowflake
        if config.output_format == OutputFormat.SNOWFLAKE:
            view_sql = f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    h.HIERARCHY_ID,
    h.HIERARCHY_NAME,
    h.PARENT_ID,
    m.value::VARCHAR AS SOURCE_UID,
    m.index AS MAPPING_INDEX,
    h.LEVEL_1,
    h.LEVEL_2,
    h.LEVEL_3,
    h.LEVEL_4,
    h.LEVEL_5,
    h.INCLUDE_FLAG,
    h.EXCLUDE_FLAG,
    h.FORMULA_GROUP,
    h.SORT_ORDER
FROM {config.target_schema}.{source_table} h,
LATERAL FLATTEN(input => PARSE_JSON(h.SOURCE_MAPPINGS)) m
WHERE h.INCLUDE_FLAG = TRUE;
"""
        else:
            # Generic SQL for other dialects
            view_sql = f"""CREATE OR REPLACE VIEW {config.target_schema}.{view_name} AS
SELECT
    h.HIERARCHY_ID,
    h.HIERARCHY_NAME,
    h.PARENT_ID,
    h.LEVEL_1,
    h.LEVEL_2,
    h.LEVEL_3,
    h.LEVEL_4,
    h.LEVEL_5,
    h.INCLUDE_FLAG,
    h.EXCLUDE_FLAG,
    h.FORMULA_GROUP,
    h.SORT_ORDER
FROM {config.target_schema}.{source_table} h
WHERE h.INCLUDE_FLAG = TRUE;
"""

        files.append(GeneratedFile(
            name=f"{view_name}.sql",
            path=f"views/{view_name}.sql",
            content=view_sql,
            file_type="sql",
            tier=ProjectTier.VW_1,
            description=f"Mapping view for {hierarchy.name}",
        ))

        return files

    def _generate_dt_2(
        self,
        hierarchy: ConvertedHierarchy,
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate DT_2 dimension join table."""
        files = []
        table_name = self._sanitize_name(f"DT_2_{hierarchy.name}")
        view_name = self._sanitize_name(f"VW_1_{hierarchy.name}")

        dt_sql = f"""CREATE OR REPLACE TABLE {config.target_schema}.{table_name} AS
SELECT DISTINCT
    v.HIERARCHY_ID,
    v.HIERARCHY_NAME,
    v.SOURCE_UID,
    v.LEVEL_1,
    v.LEVEL_2,
    v.LEVEL_3,
    v.LEVEL_4,
    v.LEVEL_5,
    v.SORT_ORDER,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
FROM {config.target_schema}.{view_name} v
WHERE v.INCLUDE_FLAG = TRUE
  AND v.EXCLUDE_FLAG = FALSE;
"""

        files.append(GeneratedFile(
            name=f"{table_name}.sql",
            path=f"tables/{table_name}.sql",
            content=dt_sql,
            file_type="sql",
            tier=ProjectTier.DT_2,
            description=f"Dimension table for {hierarchy.name}",
        ))

        return files

    def _generate_dt_3a(
        self,
        hierarchy: ConvertedHierarchy,
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate DT_3A aggregation table."""
        files = []
        table_name = self._sanitize_name(f"DT_3A_{hierarchy.name}")

        # Aggregation table structure
        dt_sql = f"""CREATE OR REPLACE TABLE {config.target_schema}.{table_name} AS
SELECT
    LEVEL_1,
    LEVEL_2,
    COUNT(DISTINCT SOURCE_UID) AS MEMBER_COUNT,
    MIN(SORT_ORDER) AS MIN_SORT,
    MAX(SORT_ORDER) AS MAX_SORT,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
FROM {config.target_schema}.DT_2_{self._sanitize_name(hierarchy.name)}
GROUP BY LEVEL_1, LEVEL_2;
"""

        files.append(GeneratedFile(
            name=f"{table_name}.sql",
            path=f"tables/{table_name}.sql",
            content=dt_sql,
            file_type="sql",
            tier=ProjectTier.DT_3A,
            description=f"Aggregation table for {hierarchy.name}",
        ))

        return files

    def _generate_dt_3(
        self,
        hierarchy: ConvertedHierarchy,
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate DT_3 final union table."""
        files = []
        table_name = self._sanitize_name(f"DT_3_{hierarchy.name}")

        dt_sql = f"""CREATE OR REPLACE TABLE {config.target_schema}.{table_name} AS
SELECT
    'DETAIL' AS RECORD_TYPE,
    HIERARCHY_ID,
    HIERARCHY_NAME,
    SOURCE_UID,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3,
    LEVEL_4,
    LEVEL_5,
    SORT_ORDER,
    LOAD_TIMESTAMP
FROM {config.target_schema}.DT_2_{self._sanitize_name(hierarchy.name)}

UNION ALL

SELECT
    'SUMMARY' AS RECORD_TYPE,
    NULL AS HIERARCHY_ID,
    LEVEL_1 || ' - ' || LEVEL_2 AS HIERARCHY_NAME,
    NULL AS SOURCE_UID,
    LEVEL_1,
    LEVEL_2,
    NULL AS LEVEL_3,
    NULL AS LEVEL_4,
    NULL AS LEVEL_5,
    MIN_SORT AS SORT_ORDER,
    LOAD_TIMESTAMP
FROM {config.target_schema}.DT_3A_{self._sanitize_name(hierarchy.name)};
"""

        files.append(GeneratedFile(
            name=f"{table_name}.sql",
            path=f"tables/{table_name}.sql",
            content=dt_sql,
            file_type="sql",
            tier=ProjectTier.DT_3,
            description=f"Final union table for {hierarchy.name}",
        ))

        return files

    def _generate_dbt_files(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate dbt model files."""
        files = []

        # Generate dbt_project.yml
        dbt_project = {
            "name": config.project_name.lower().replace(" ", "_"),
            "version": "1.0.0",
            "config-version": 2,
            "model-paths": ["models"],
            "test-paths": ["tests"],
            "target-path": "target",
            "clean-targets": ["target", "dbt_packages"],
        }

        files.append(GeneratedFile(
            name="dbt_project.yml",
            path="dbt_project.yml",
            content=self._to_yaml(dbt_project),
            file_type="yml",
            description="dbt project configuration",
        ))

        # Generate schema.yml for models
        models = []
        for hier in hierarchies:
            model_name = self._sanitize_name(f"stg_{hier.name}").lower()
            models.append({
                "name": model_name,
                "description": f"Staging model for {hier.name} hierarchy",
                "columns": [
                    {"name": "hierarchy_id", "description": "Unique hierarchy node ID"},
                    {"name": "hierarchy_name", "description": "Display name"},
                    {"name": "parent_id", "description": "Parent node ID"},
                ],
            })

        schema = {"version": 2, "models": models}
        files.append(GeneratedFile(
            name="schema.yml",
            path="models/schema.yml",
            content=self._to_yaml(schema),
            file_type="yml",
            description="dbt model schema",
        ))

        # Generate model SQL files
        for hier in hierarchies:
            model_name = self._sanitize_name(f"stg_{hier.name}").lower()
            model_sql = f"""-- dbt model for {hier.name}
{{{{ config(materialized='table') }}}}

SELECT
    hierarchy_id,
    hierarchy_name,
    parent_id,
    level_1,
    level_2,
    level_3,
    include_flag,
    exclude_flag,
    sort_order
FROM {{{{ source('raw', 'TBL_0_{self._sanitize_name(hier.name)}') }}}}
WHERE include_flag = TRUE
"""
            files.append(GeneratedFile(
                name=f"{model_name}.sql",
                path=f"models/staging/{model_name}.sql",
                content=model_sql,
                file_type="sql",
                description=f"dbt staging model for {hier.name}",
            ))

        return files

    def _generate_documentation(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: ProjectConfig,
    ) -> list[GeneratedFile]:
        """Generate project documentation."""
        files = []

        # Generate README
        readme = f"""# {config.project_name}

## Overview

This project contains hierarchy definitions for the Librarian data platform.

## Hierarchies Included

"""
        for hier in hierarchies:
            readme += f"""### {hier.name}

- **Entity Type**: {hier.entity_type}
- **Levels**: {hier.level_count}
- **Total Nodes**: {hier.total_nodes}
- **Source Column**: {hier.source_column}

"""

        readme += """## Project Structure

```
├── tables/          # TBL_0 hierarchy tables
├── views/           # VW_1 mapping views
├── models/          # dbt models (if enabled)
├── docs/            # Documentation
└── deploy/          # Deployment scripts
```

## Deployment

Run the deployment script to create all objects:

```sql
-- Snowflake
@deploy/deploy_all.sql
```
"""

        files.append(GeneratedFile(
            name="README.md",
            path="README.md",
            content=readme,
            file_type="md",
            description="Project documentation",
        ))

        return files

    def _generate_deployment_script(
        self,
        files: list[GeneratedFile],
        config: ProjectConfig,
    ) -> GeneratedFile:
        """Generate deployment script that runs all SQL files."""
        sql_files = [f for f in files if f.file_type == "sql"]

        # Sort by tier for proper dependency order
        tier_order = {
            ProjectTier.TBL_0: 1,
            ProjectTier.VW_1: 2,
            ProjectTier.DT_2: 3,
            ProjectTier.DT_3A: 4,
            ProjectTier.DT_3: 5,
            None: 6,
        }
        sql_files.sort(key=lambda f: tier_order.get(f.tier, 99))

        deploy_sql = f"""-- Deployment Script for {config.project_name}
-- Generated: {datetime.now().isoformat()}
-- Target: {config.target_database}.{config.target_schema}

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS {config.target_schema};

USE SCHEMA {config.target_schema};

"""

        for f in sql_files:
            deploy_sql += f"\n-- ============================================\n"
            deploy_sql += f"-- {f.description or f.name}\n"
            deploy_sql += f"-- ============================================\n\n"
            deploy_sql += f.content
            deploy_sql += "\n\n"

        return GeneratedFile(
            name="deploy_all.sql",
            path="deploy/deploy_all.sql",
            content=deploy_sql,
            file_type="sql",
            description="Complete deployment script",
        )

    def _generate_hierarchy_inserts(
        self,
        hierarchy: ConvertedHierarchy,
        table_name: str,
        config: ProjectConfig,
    ) -> str:
        """Generate INSERT statements for hierarchy data."""
        inserts = []

        for node_id, node in hierarchy.nodes.items():
            # Build level values
            level_values = ["NULL"] * 10
            level_sorts = ["NULL"] * 10
            if node.level > 0 and node.level <= 10:
                level_values[node.level - 1] = f"'{self._escape_sql(node.value)}'"
                level_sorts[node.level - 1] = str(node.sort_order)

            insert = f"""INSERT INTO {config.target_schema}.{table_name} (
    HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID,
    LEVEL_1, LEVEL_1_SORT, LEVEL_2, LEVEL_2_SORT, LEVEL_3, LEVEL_3_SORT,
    LEVEL_4, LEVEL_4_SORT, LEVEL_5, LEVEL_5_SORT,
    INCLUDE_FLAG, EXCLUDE_FLAG, SORT_ORDER
) VALUES (
    '{node_id}', '{self._escape_sql(node.name)}', {f"'{node.parent_id}'" if node.parent_id else 'NULL'},
    {level_values[0]}, {level_sorts[0]}, {level_values[1]}, {level_sorts[1]}, {level_values[2]}, {level_sorts[2]},
    {level_values[3]}, {level_sorts[3]}, {level_values[4]}, {level_sorts[4]},
    TRUE, FALSE, {node.sort_order}
);"""
            inserts.append(insert)

        return "\n".join(inserts)

    def _build_create_table(
        self,
        table_name: str,
        columns: list[str],
        config: ProjectConfig,
        primary_key: str | None = None,
    ) -> str:
        """Build CREATE TABLE statement."""
        ddl = f"CREATE OR REPLACE TABLE {config.target_schema}.{table_name} (\n"
        ddl += ",\n".join(f"    {col}" for col in columns)

        if primary_key:
            ddl += f",\n    PRIMARY KEY ({primary_key})"

        ddl += "\n);"
        return ddl

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use in SQL identifiers."""
        # Replace spaces and special chars with underscores
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        return sanitized.upper()

    def _escape_sql(self, value: str) -> str:
        """Escape single quotes in SQL strings."""
        return value.replace("'", "''")

    def _to_yaml(self, data: dict) -> str:
        """Convert dict to YAML string."""
        import yaml
        try:
            return yaml.dump(data, default_flow_style=False, sort_keys=False)
        except ImportError:
            # Fallback to simple YAML-like format
            return json.dumps(data, indent=2)

    def write_project(
        self,
        project: GeneratedProject,
        output_dir: str,
    ) -> dict[str, str]:
        """
        Write generated project to disk.

        Args:
            project: Generated project
            output_dir: Output directory

        Returns:
            Dictionary of file paths created
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        files_created: dict[str, str] = {}

        for f in project.files:
            file_path = output_path / f.path
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, "w", encoding="utf-8") as fp:
                fp.write(f.content)

            files_created[f.name] = str(file_path.absolute())

        project.output_dir = str(output_path.absolute())

        return files_created

    def validate_project(
        self,
        project: GeneratedProject,
    ) -> dict[str, Any]:
        """
        Validate a generated project.

        Args:
            project: Project to validate

        Returns:
            Validation results
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Check for required files
        if not any(f.tier == ProjectTier.TBL_0 for f in project.files):
            warnings.append("No TBL_0 tables generated")

        if not any(f.tier == ProjectTier.VW_1 for f in project.files):
            warnings.append("No VW_1 views generated")

        # Check for deployment script
        if not any(f.name == "deploy_all.sql" for f in project.files):
            errors.append("Missing deployment script")

        # Check SQL syntax (basic validation)
        for f in project.files:
            if f.file_type == "sql":
                if "CREATE" not in f.content and "INSERT" not in f.content:
                    warnings.append(f"File {f.name} may have invalid SQL")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "file_count": project.file_count,
            "tiers": [t.value for t in project.tiers_generated],
        }
