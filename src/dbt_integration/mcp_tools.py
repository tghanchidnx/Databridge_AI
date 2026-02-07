"""
MCP Tools for dbt Integration.

Provides 8 tools for generating dbt projects from DataBridge hierarchies:
- create_dbt_project
- generate_dbt_model
- generate_dbt_sources
- generate_dbt_schema
- generate_dbt_metrics
- generate_cicd_pipeline
- validate_dbt_project
- export_dbt_project
"""

import json
import logging
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import (
    DbtProject,
    DbtProjectConfig,
    DbtModelConfig,
    DbtModelType,
    DbtMaterialization,
    DbtSource,
    DbtSourceTable,
    DbtColumn,
    CiCdConfig,
    CiCdPlatform,
    ValidationResult,
)
from .project_generator import DbtProjectGenerator
from .model_generator import DbtModelGenerator
from .source_generator import DbtSourceGenerator, DbtMetricsGenerator
from .cicd_generator import CiCdGenerator

logger = logging.getLogger(__name__)


def register_dbt_tools(mcp, settings=None) -> Dict[str, Any]:
    """
    Register dbt integration MCP tools.

    Args:
        mcp: The FastMCP instance
        settings: Optional settings

    Returns:
        Dict with registration info
    """

    # Initialize generators
    project_gen = DbtProjectGenerator()
    model_gen = DbtModelGenerator()
    source_gen = DbtSourceGenerator()
    metrics_gen = DbtMetricsGenerator()
    cicd_gen = CiCdGenerator()

    @mcp.tool()
    def create_dbt_project(
        name: str,
        profile: str,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        hierarchy_project_id: Optional[str] = None,
        include_cicd: bool = False,
        output_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new dbt project scaffold.

        Generates a complete dbt project structure including:
        - dbt_project.yml configuration
        - profiles.yml template
        - Directory structure (models/, seeds/, tests/, etc.)
        - README and .gitignore
        - Optional CI/CD pipeline

        Args:
            name: Project name (will be converted to lowercase with underscores)
            profile: dbt profile name for database connections
            target_database: Target database name (optional)
            target_schema: Target schema name (optional)
            hierarchy_project_id: Link to DataBridge hierarchy project (optional)
            include_cicd: Whether to include GitHub Actions CI/CD workflow
            output_dir: Directory to write files (optional, for immediate export)

        Returns:
            Project details with generated file list

        Example:
            create_dbt_project(
                name="finance_analytics",
                profile="snowflake_prod",
                target_database="ANALYTICS",
                target_schema="FINANCE",
                hierarchy_project_id="revenue-pl",
                include_cicd=True
            )
        """
        try:
            # Create project
            project = project_gen.create_project(
                name=name,
                profile=profile,
                target_database=target_database,
                target_schema=target_schema,
                hierarchy_project_id=hierarchy_project_id,
                include_cicd=include_cicd,
            )

            # Scaffold files
            files = project_gen.scaffold_project(project, output_dir)

            return {
                "success": True,
                "project_id": project.id,
                "project_name": project.config.name,
                "profile": project.config.profile,
                "files_generated": list(files.keys()),
                "file_count": len(files),
                "output_dir": output_dir,
                "message": f"Created dbt project '{project.config.name}' with {len(files)} files",
            }

        except Exception as e:
            logger.error(f"Failed to create dbt project: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    @mcp.tool()
    def generate_dbt_model(
        project_name: str,
        model_name: str,
        model_type: str = "staging",
        source_name: Optional[str] = None,
        source_table: Optional[str] = None,
        ref_models: Optional[str] = None,
        columns: Optional[str] = None,
        case_mappings: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a dbt model SQL file.

        Creates staging, intermediate, dimension, or fact models based on type.

        Args:
            project_name: Name of the dbt project
            model_name: Name for the model (prefix added automatically)
            model_type: Type of model (staging, intermediate, dimension, fact)
            source_name: Source name for staging models
            source_table: Source table for staging models
            ref_models: Comma-separated model references for non-staging models
            columns: JSON list of column names to include
            case_mappings: JSON list of CASE statement mappings
            description: Model description

        Returns:
            Generated model details

        Example:
            # Staging model
            generate_dbt_model(
                project_name="finance",
                model_name="gl_accounts",
                model_type="staging",
                source_name="raw",
                source_table="GL_ACCOUNTS"
            )

            # Dimension model
            generate_dbt_model(
                project_name="finance",
                model_name="account_hierarchy",
                model_type="dimension",
                ref_models="stg_gl_accounts"
            )
        """
        try:
            # Get project
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            # Parse inputs
            cols = json.loads(columns) if columns else None
            mappings = json.loads(case_mappings) if case_mappings else None
            refs = ref_models.split(",") if ref_models else []

            # Determine model type enum
            type_map = {
                "staging": DbtModelType.STAGING,
                "intermediate": DbtModelType.INTERMEDIATE,
                "dimension": DbtModelType.DIM,
                "dim": DbtModelType.DIM,
                "fact": DbtModelType.FACT,
                "mart": DbtModelType.MART,
            }
            model_type_enum = type_map.get(model_type.lower(), DbtModelType.STAGING)

            # Generate SQL based on type
            if model_type_enum == DbtModelType.STAGING:
                if not source_name or not source_table:
                    return {"success": False, "error": "source_name and source_table required for staging models"}

                sql = model_gen.generate_staging_model(
                    model_name=model_name,
                    source_name=source_name,
                    source_table=source_table,
                    columns=cols,
                    case_mappings=mappings,
                    description=description,
                )

            elif model_type_enum == DbtModelType.INTERMEDIATE:
                if not refs:
                    return {"success": False, "error": "ref_models required for intermediate models"}

                sql = model_gen.generate_intermediate_model(
                    model_name=model_name,
                    refs=refs,
                    select_columns=cols,
                    description=description,
                )

            elif model_type_enum == DbtModelType.DIM:
                if not refs:
                    return {"success": False, "error": "ref_models required for dimension models"}

                hierarchy_cols = cols if cols else ["hierarchy_id", "hierarchy_name", "parent_id"]
                sql = model_gen.generate_dimension_model(
                    model_name=model_name,
                    ref_model=refs[0],
                    hierarchy_columns=hierarchy_cols,
                    description=description,
                )

            elif model_type_enum == DbtModelType.FACT:
                if not refs:
                    return {"success": False, "error": "ref_models required for fact models"}

                sql = model_gen.generate_fact_model(
                    model_name=model_name,
                    ref_model=refs[0],
                    dimension_refs=[],
                    measure_columns=cols or ["amount"],
                    description=description,
                )

            else:
                sql = model_gen.generate_staging_model(
                    model_name=model_name,
                    source_name=source_name or "raw",
                    source_table=source_table or model_name,
                    description=description,
                )

            # Create model config
            model_config = DbtModelConfig(
                name=model_name,
                description=description,
                model_type=model_type_enum,
                source_name=source_name,
                source_table=source_table,
                refs=refs,
            )

            # Add to project
            model_gen.add_model_to_project(project, model_config, sql)
            project_gen._save()

            # Determine file path
            prefix_map = {
                DbtModelType.STAGING: ("staging", "stg_"),
                DbtModelType.INTERMEDIATE: ("intermediate", "int_"),
                DbtModelType.DIM: ("marts", "dim_"),
                DbtModelType.FACT: ("marts", "fct_"),
                DbtModelType.MART: ("marts", ""),
            }
            folder, prefix = prefix_map.get(model_type_enum, ("marts", ""))
            file_path = f"models/{folder}/{prefix}{model_name}.sql"

            return {
                "success": True,
                "model_name": f"{prefix}{model_name}",
                "model_type": model_type,
                "file_path": file_path,
                "sql_preview": sql[:500] + "..." if len(sql) > 500 else sql,
                "message": f"Generated {model_type} model: {prefix}{model_name}",
            }

        except Exception as e:
            logger.error(f"Failed to generate model: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_dbt_sources(
        project_name: str,
        source_name: str,
        mappings: Optional[str] = None,
        tables: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate sources.yml from hierarchy mappings or manual configuration.

        Creates source definitions for dbt that reference raw tables.

        Args:
            project_name: Name of the dbt project
            source_name: Name for the source (e.g., "raw", "finance")
            mappings: JSON string of DataBridge hierarchy mappings
            tables: JSON list of table definitions (alternative to mappings)
            database: Database name for all tables
            schema_name: Schema name for all tables

        Returns:
            Generated sources details

        Example:
            generate_dbt_sources(
                project_name="finance",
                source_name="raw",
                database="RAW_DB",
                schema_name="FINANCE",
                tables='[{"name": "gl_accounts", "columns": ["account_code", "account_name"]}]'
            )
        """
        try:
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            if mappings:
                # Generate from hierarchy mappings
                mapping_data = json.loads(mappings)
                source = source_gen.generate_from_hierarchy_mappings(mapping_data, source_name)

            elif tables:
                # Generate from manual table definitions
                table_data = json.loads(tables)
                source_tables = []

                for tbl in table_data:
                    columns = [
                        DbtColumn(name=col) for col in tbl.get("columns", [])
                    ]
                    source_tables.append(DbtSourceTable(
                        name=tbl.get("name"),
                        description=tbl.get("description"),
                        columns=columns,
                    ))

                source = DbtSource(
                    name=source_name,
                    database=database,
                    schema_name=schema_name,
                    tables=source_tables,
                )

            else:
                return {"success": False, "error": "Either mappings or tables required"}

            # Override database/schema if provided
            if database:
                source.database = database
            if schema_name:
                source.schema_name = schema_name

            # Add to project
            source_gen.add_source_to_project(project, source)
            project_gen._save()

            return {
                "success": True,
                "source_name": source_name,
                "tables_count": len(source.tables),
                "tables": [t.name for t in source.tables],
                "file_path": "models/sources.yml",
                "message": f"Generated sources.yml with {len(source.tables)} tables",
            }

        except Exception as e:
            logger.error(f"Failed to generate sources: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_dbt_schema(
        project_name: str,
        models: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate schema.yml with model documentation and tests.

        Creates schema definitions for all models in the project.

        Args:
            project_name: Name of the dbt project
            models: Optional JSON list of specific models to include

        Returns:
            Generated schema details

        Example:
            generate_dbt_schema(project_name="finance")
        """
        try:
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            # Filter models if specified
            if models:
                model_names = json.loads(models)
                project_models = [m for m in project.models if m.name in model_names]
            else:
                project_models = project.models

            # Generate schema definitions
            schema_models = []
            for model in project_models:
                schema_def = model_gen.generate_model_schema(model)
                schema_models.append(schema_def)

            # Generate YAML
            schema_yml = source_gen.generate_schema_yml(schema_models)

            # Add to project files
            project.generated_files["models/schema.yml"] = schema_yml
            project_gen._save()

            return {
                "success": True,
                "models_count": len(schema_models),
                "models": [m["name"] for m in schema_models],
                "file_path": "models/schema.yml",
                "message": f"Generated schema.yml with {len(schema_models)} models",
            }

        except Exception as e:
            logger.error(f"Failed to generate schema: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_dbt_metrics(
        project_name: str,
        formula_groups: Optional[str] = None,
        metrics: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate metrics.yml from formula groups or manual definitions.

        Creates dbt metrics for business calculations.

        Args:
            project_name: Name of the dbt project
            formula_groups: JSON string of DataBridge formula groups
            metrics: JSON list of metric definitions (alternative)

        Returns:
            Generated metrics details

        Example:
            generate_dbt_metrics(
                project_name="finance",
                metrics='[{"name": "total_revenue", "expression": "SUM(amount)", "type": "derived"}]'
            )
        """
        try:
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            if formula_groups:
                # Generate from DataBridge formula groups
                groups_data = json.loads(formula_groups)
                metrics_list = metrics_gen.generate_from_formula_groups(groups_data)

            elif metrics:
                # Use manual definitions
                metrics_list = json.loads(metrics)

            else:
                return {"success": False, "error": "Either formula_groups or metrics required"}

            # Generate YAML
            metrics_yml = metrics_gen.generate_metrics_yml(metrics_list)

            # Add to project files
            project.generated_files["models/metrics.yml"] = metrics_yml
            project_gen._save()

            return {
                "success": True,
                "metrics_count": len(metrics_list),
                "metrics": [m.get("name") for m in metrics_list],
                "file_path": "models/metrics.yml",
                "message": f"Generated metrics.yml with {len(metrics_list)} metrics",
            }

        except Exception as e:
            logger.error(f"Failed to generate metrics: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_cicd_pipeline(
        project_name: str,
        platform: str = "github_actions",
        trigger_branches: Optional[str] = None,
        dbt_version: str = "1.7.0",
        run_tests: bool = True,
        run_docs: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate CI/CD pipeline configuration.

        Creates automated build and deploy workflows for the dbt project.

        Args:
            project_name: Name of the dbt project
            platform: CI/CD platform (github_actions, gitlab_ci, azure_devops)
            trigger_branches: Comma-separated list of trigger branches
            dbt_version: dbt version to use
            run_tests: Whether to run dbt tests
            run_docs: Whether to generate documentation

        Returns:
            Generated pipeline details

        Example:
            generate_cicd_pipeline(
                project_name="finance",
                platform="github_actions",
                trigger_branches="main,develop"
            )
        """
        try:
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            # Parse platform
            platform_map = {
                "github_actions": CiCdPlatform.GITHUB_ACTIONS,
                "github": CiCdPlatform.GITHUB_ACTIONS,
                "gitlab_ci": CiCdPlatform.GITLAB_CI,
                "gitlab": CiCdPlatform.GITLAB_CI,
                "azure_devops": CiCdPlatform.AZURE_DEVOPS,
                "azure": CiCdPlatform.AZURE_DEVOPS,
            }
            platform_enum = platform_map.get(platform.lower(), CiCdPlatform.GITHUB_ACTIONS)

            # Create config
            branches = trigger_branches.split(",") if trigger_branches else ["main", "develop"]
            config = CiCdConfig(
                platform=platform_enum,
                trigger_branches=branches,
                dbt_version=dbt_version,
                run_tests=run_tests,
                run_docs=run_docs,
            )

            # Generate pipeline
            pipeline_content = cicd_gen.generate_pipeline(config, project.config.name)
            pipeline_path = cicd_gen.get_pipeline_path(platform_enum)

            # Add to project
            project.cicd_config = config
            project.generated_files[pipeline_path] = pipeline_content
            project_gen._save()

            return {
                "success": True,
                "platform": platform,
                "file_path": pipeline_path,
                "trigger_branches": branches,
                "features": {
                    "run_tests": run_tests,
                    "run_docs": run_docs,
                },
                "message": f"Generated {platform} pipeline at {pipeline_path}",
            }

        except Exception as e:
            logger.error(f"Failed to generate pipeline: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def validate_dbt_project(
        project_name: str,
    ) -> Dict[str, Any]:
        """
        Validate a dbt project structure and configuration.

        Checks for required files, valid YAML, and model references.

        Args:
            project_name: Name of the dbt project

        Returns:
            Validation results with errors and warnings

        Example:
            validate_dbt_project(project_name="finance")
        """
        try:
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            errors = []
            warnings = []
            missing_files = []

            files = project.generated_files

            # Check required files
            required_files = [
                "dbt_project.yml",
                "profiles.yml.template",
            ]

            for req_file in required_files:
                if req_file not in files:
                    missing_files.append(req_file)
                    errors.append(f"Missing required file: {req_file}")

            # Check for at least one model
            model_files = [f for f in files.keys() if f.endswith(".sql") and "models/" in f]
            if not model_files:
                warnings.append("No SQL models found in project")

            # Check sources
            if "models/sources.yml" not in files:
                warnings.append("No sources.yml found - staging models may not work")

            # Validate model references
            for model in project.models:
                for ref in model.refs:
                    ref_model = next((m for m in project.models if m.name == ref), None)
                    if not ref_model:
                        warnings.append(f"Model '{model.name}' references unknown model: {ref}")

            result = ValidationResult(
                valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                missing_files=missing_files,
            )

            return {
                "success": True,
                "valid": result.valid,
                "errors": result.errors,
                "warnings": result.warnings,
                "missing_files": result.missing_files,
                "files_count": len(files),
                "models_count": len(project.models),
                "sources_count": len(project.sources),
            }

        except Exception as e:
            logger.error(f"Failed to validate project: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def export_dbt_project(
        project_name: str,
        output_dir: Optional[str] = None,
        as_zip: bool = False,
    ) -> Dict[str, Any]:
        """
        Export a dbt project to directory or ZIP file.

        Writes all generated files to disk.

        Args:
            project_name: Name of the dbt project
            output_dir: Directory to export to (default: ./dbt_export/{project_name})
            as_zip: Whether to create a ZIP archive

        Returns:
            Export details with file paths

        Example:
            export_dbt_project(
                project_name="finance",
                output_dir="./my_dbt_project",
                as_zip=True
            )
        """
        try:
            project = project_gen.get_project(project_name)
            if not project:
                return {"success": False, "error": f"Project '{project_name}' not found"}

            # Determine output path
            if not output_dir:
                output_dir = f"./dbt_export/{project.config.name}"

            output_path = Path(output_dir)

            # Write files
            project_gen._write_files(str(output_path), project.generated_files)

            result = {
                "success": True,
                "project_name": project.config.name,
                "output_dir": str(output_path.absolute()),
                "files_exported": len(project.generated_files),
                "file_list": list(project.generated_files.keys()),
            }

            # Create ZIP if requested
            if as_zip:
                zip_path = output_path.with_suffix(".zip")
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for filepath, content in project.generated_files.items():
                        zf.writestr(filepath, content)

                result["zip_file"] = str(zip_path.absolute())
                result["message"] = f"Exported project to {output_path} and {zip_path}"
            else:
                result["message"] = f"Exported project to {output_path}"

            return result

        except Exception as e:
            logger.error(f"Failed to export project: {e}")
            return {"success": False, "error": str(e)}

    # Return registration info
    return {
        "tools_registered": 8,
        "tools": [
            "create_dbt_project",
            "generate_dbt_model",
            "generate_dbt_sources",
            "generate_dbt_schema",
            "generate_dbt_metrics",
            "generate_cicd_pipeline",
            "validate_dbt_project",
            "export_dbt_project",
        ],
    }
