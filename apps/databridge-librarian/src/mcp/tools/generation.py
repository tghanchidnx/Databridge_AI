"""
MCP Tools for DDL and dbt generation in DataBridge AI Librarian.

Provides tools for:
- DDL script generation
- dbt project generation
- Warehouse model preview
- Design validation
"""

from typing import Any, Dict, List, Optional

from fastmcp import FastMCP


def register_generation_tools(mcp: FastMCP) -> None:
    """Register all generation MCP tools."""

    @mcp.tool()
    def generate_ddl_scripts(
        project_id: str,
        dialect: str = "snowflake",
        target_schema: str = "HIERARCHIES",
        include_tbl_0: bool = True,
        include_vw_1: bool = True,
        include_dt_2: bool = False,
        include_dt_3a: bool = False,
        include_dt_3: bool = False,
        use_create_or_replace: bool = True,
        include_drop: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate DDL scripts for a hierarchy project.

        Creates SQL scripts for deploying hierarchies to a data warehouse:
        - TBL_0: Base hierarchy data table
        - VW_1: Mapping unnest view (flattens source mappings)
        - DT_2: Dimension join dynamic table
        - DT_3A: Pre-aggregation table
        - DT_3: Final transactional union

        Args:
            project_id: Project ID (can be partial).
            dialect: SQL dialect (snowflake, postgresql, bigquery, tsql, mysql).
            target_schema: Target schema name.
            include_tbl_0: Generate TBL_0 hierarchy table.
            include_vw_1: Generate VW_1 unnest view.
            include_dt_2: Generate DT_2 dimension table.
            include_dt_3a: Generate DT_3A pre-aggregation table.
            include_dt_3: Generate DT_3 final union table.
            use_create_or_replace: Use CREATE OR REPLACE (Snowflake).
            include_drop: Include DROP statements.

        Returns:
            Dictionary with generated scripts and metadata.

        Example:
            generate_ddl_scripts(
                project_id="abc123",
                dialect="snowflake",
                target_schema="ANALYTICS",
                include_dt_2=True
            )
        """
        from ...hierarchy import HierarchyService
        from ...generation import DDLGenerator, DDLConfig, SQLDialect
        from ...core.database import init_database

        init_database()
        service = HierarchyService()

        # Find project
        projects = service.list_projects()
        project = next((p for p in projects if str(p.id).startswith(project_id)), None)

        if not project:
            return {
                "success": False,
                "error": f"Project not found: {project_id}",
            }

        # Get hierarchies
        hierarchies = service.list_hierarchies(str(project.id))

        if not hierarchies:
            return {
                "success": False,
                "error": "No hierarchies found in project",
            }

        # Validate dialect
        try:
            sql_dialect = SQLDialect(dialect.lower())
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid dialect: {dialect}",
                "valid_dialects": [d.value for d in SQLDialect],
            }

        # Configure generator
        config = DDLConfig(
            dialect=sql_dialect,
            target_schema=target_schema,
            include_drop=include_drop,
            use_create_or_replace=use_create_or_replace,
            generate_tbl_0=include_tbl_0,
            generate_vw_1=include_vw_1,
            generate_dt_2=include_dt_2,
            generate_dt_3a=include_dt_3a,
            generate_dt_3=include_dt_3,
        )

        # Generate scripts
        generator = DDLGenerator(config)
        scripts = generator.generate(project, hierarchies, config)

        return {
            "success": True,
            "project_name": project.name,
            "project_id": str(project.id),
            "dialect": dialect,
            "target_schema": target_schema,
            "hierarchy_count": len(hierarchies),
            "script_count": len(scripts),
            "scripts": [
                {
                    "tier": s.tier,
                    "object_name": s.object_name,
                    "full_name": s.full_name,
                    "ddl_type": s.ddl_type.value,
                    "description": s.description,
                    "sql": s.sql,
                }
                for s in scripts
            ],
        }

    @mcp.tool()
    def generate_dbt_project(
        project_id: str,
        dbt_project_name: str = "",
        source_database: str = "RAW",
        source_schema: str = "HIERARCHIES",
        target_schema: str = "ANALYTICS",
        materialization: str = "table",
        generate_tests: bool = True,
        generate_docs: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate a complete dbt project from a hierarchy project.

        Creates a full dbt project structure with:
        - dbt_project.yml configuration
        - Source definitions (sources.yml)
        - Staging models (stg_*)
        - Mart models (dim_*, fct_*)
        - Schema files with tests

        Args:
            project_id: Project ID (can be partial).
            dbt_project_name: Name for the dbt project (defaults to hierarchy project name).
            source_database: Source database name in dbt.
            source_schema: Source schema name where hierarchy table exists.
            target_schema: Target schema for mart models.
            materialization: Model materialization (table, view, incremental).
            generate_tests: Include dbt tests in schema files.
            generate_docs: Include README and documentation.

        Returns:
            Dictionary with generated dbt project files.

        Example:
            generate_dbt_project(
                project_id="abc123",
                dbt_project_name="hierarchy_analytics",
                source_database="RAW",
                target_schema="ANALYTICS"
            )
        """
        from ...hierarchy import HierarchyService
        from ...generation import DbtProjectGenerator, DbtConfig, DbtMaterialization
        from ...core.database import init_database

        init_database()
        service = HierarchyService()

        # Find project
        projects = service.list_projects()
        project = next((p for p in projects if str(p.id).startswith(project_id)), None)

        if not project:
            return {
                "success": False,
                "error": f"Project not found: {project_id}",
            }

        # Get hierarchies
        hierarchies = service.list_hierarchies(str(project.id))

        if not hierarchies:
            return {
                "success": False,
                "error": "No hierarchies found in project",
            }

        # Validate materialization
        try:
            mat = DbtMaterialization(materialization.lower())
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid materialization: {materialization}",
                "valid_values": [m.value for m in DbtMaterialization],
            }

        # Configure generator
        config = DbtConfig(
            project_name=dbt_project_name or project.name.lower().replace(" ", "_"),
            source_database=source_database,
            source_schema=source_schema,
            target_schema=target_schema,
            materialization=mat,
            generate_tests=generate_tests,
            generate_docs=generate_docs,
        )

        # Generate project
        generator = DbtProjectGenerator()
        dbt_project = generator.generate(project, hierarchies, config)

        return {
            "success": True,
            "project_name": dbt_project.project_name,
            "source_project_id": str(project.id),
            "model_count": dbt_project.model_count,
            "source_count": dbt_project.source_count,
            "file_count": dbt_project.file_count,
            "files": [
                {
                    "name": f.name,
                    "path": f.path,
                    "file_type": f.file_type,
                    "content": f.content,
                }
                for f in dbt_project.files
            ],
        }

    @mcp.tool()
    def preview_warehouse_model(
        project_id: str,
        dialect: str = "snowflake",
        target_schema: str = "HIERARCHIES",
        include_all_tiers: bool = False,
    ) -> Dict[str, Any]:
        """
        Preview the warehouse model that will be generated.

        Shows what database objects will be created without generating
        the actual DDL scripts.

        Args:
            project_id: Project ID (can be partial).
            dialect: SQL dialect (snowflake, postgresql, bigquery, tsql).
            target_schema: Target schema name.
            include_all_tiers: Include all tiers (TBL_0 through DT_3).

        Returns:
            Dictionary with preview of objects to be created.

        Example:
            preview_warehouse_model(
                project_id="abc123",
                dialect="snowflake",
                include_all_tiers=True
            )
        """
        from ...hierarchy import HierarchyService
        from ...generation import DDLGenerator, DDLConfig, SQLDialect, ProjectTier
        from ...core.database import init_database

        init_database()
        service = HierarchyService()

        # Find project
        projects = service.list_projects()
        project = next((p for p in projects if str(p.id).startswith(project_id)), None)

        if not project:
            return {
                "success": False,
                "error": f"Project not found: {project_id}",
            }

        # Get hierarchies
        hierarchies = service.list_hierarchies(str(project.id))

        # Configure generator
        try:
            sql_dialect = SQLDialect(dialect.lower())
        except ValueError:
            sql_dialect = SQLDialect.SNOWFLAKE

        config = DDLConfig(
            dialect=sql_dialect,
            target_schema=target_schema,
            generate_tbl_0=True,
            generate_vw_1=True,
            generate_dt_2=include_all_tiers,
            generate_dt_3a=include_all_tiers,
            generate_dt_3=include_all_tiers,
        )

        # Generate preview
        generator = DDLGenerator(config)
        preview = generator.generate_preview(project, hierarchies, config)

        return {
            "success": True,
            "project_name": project.name,
            "project_id": str(project.id),
            "dialect": dialect,
            "target_schema": target_schema,
            "hierarchy_count": len(hierarchies),
            "leaf_count": sum(1 for h in hierarchies if h.is_leaf_node),
            "objects": preview["objects"],
            "estimated_rows": preview["estimated_rows"],
            "tiers": {
                "TBL_0": "Base hierarchy data table - stores all hierarchy nodes",
                "VW_1": "Mapping unnest view - flattens source_mappings JSON",
                "DT_2": "Dimension table - joins with source data",
                "DT_3A": "Pre-aggregation table - grouped summary",
                "DT_3": "Final table - complete transactional view",
            },
        }

    @mcp.tool()
    def validate_model_design(
        project_id: str,
    ) -> Dict[str, Any]:
        """
        Validate the hierarchy design before DDL generation.

        Checks for:
        - Orphaned hierarchies (parent not found)
        - Leaf nodes without source mappings
        - Calculation hierarchies without formulas
        - Circular parent-child dependencies

        Args:
            project_id: Project ID (can be partial).

        Returns:
            Dictionary with validation results and recommendations.

        Example:
            validate_model_design(project_id="abc123")
        """
        from ...hierarchy import HierarchyService
        from ...generation import ProjectGenerator
        from ...core.database import init_database

        init_database()
        service = HierarchyService()

        # Find project
        projects = service.list_projects()
        project = next((p for p in projects if str(p.id).startswith(project_id)), None)

        if not project:
            return {
                "success": False,
                "error": f"Project not found: {project_id}",
            }

        # Get hierarchies
        hierarchies = service.list_hierarchies(str(project.id))

        if not hierarchies:
            return {
                "success": False,
                "error": "No hierarchies found in project",
            }

        # Validate
        generator = ProjectGenerator()
        validation = generator.validate_design(project, hierarchies)

        return {
            "success": True,
            "project_name": project.name,
            "project_id": str(project.id),
            "is_valid": validation["is_valid"],
            "hierarchy_count": validation["hierarchy_count"],
            "leaf_count": validation["leaf_count"],
            "calculation_count": validation["calculation_count"],
            "error_count": validation["error_count"],
            "warning_count": validation["warning_count"],
            "errors": validation["errors"],
            "warnings": validation["warnings"],
            "recommendations": _get_recommendations(validation),
        }

    @mcp.tool()
    def get_hierarchy_types() -> Dict[str, Any]:
        """
        Get all available hierarchy types and their configurations.

        Returns information about:
        - STANDARD: Basic hierarchical grouping
        - GROUPING: Aggregation/rollup hierarchy
        - XREF: Cross-reference hierarchy
        - CALCULATION: Formula-based hierarchy
        - ALLOCATION: Distribution hierarchy

        Returns:
            Dictionary with hierarchy type information.
        """
        from ...hierarchy import get_all_hierarchy_types

        types = get_all_hierarchy_types()

        return {
            "success": True,
            "count": len(types),
            "types": types,
        }

    @mcp.tool()
    def generate_complete_project(
        project_id: str,
        output_format: str = "snowflake",
        target_schema: str = "HIERARCHIES",
        include_dbt: bool = False,
        include_all_tiers: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate a complete deployment package for a hierarchy project.

        Creates all necessary files for deployment:
        - DDL scripts (TBL_0, VW_1, optionally DT_2/DT_3A/DT_3)
        - Combined deploy script
        - Optional dbt project
        - Documentation and manifest

        Args:
            project_id: Project ID (can be partial).
            output_format: Output format (snowflake, postgresql, bigquery, tsql, dbt).
            target_schema: Target schema name.
            include_dbt: Also generate a dbt project.
            include_all_tiers: Include all tiers (TBL_0 through DT_3).

        Returns:
            Dictionary with generated project files.

        Example:
            generate_complete_project(
                project_id="abc123",
                output_format="snowflake",
                target_schema="ANALYTICS",
                include_dbt=True
            )
        """
        from ...hierarchy import HierarchyService
        from ...generation import ProjectGenerator, ProjectConfig, OutputFormat, ProjectTier
        from ...core.database import init_database

        init_database()
        service = HierarchyService()

        # Find project
        projects = service.list_projects()
        project = next((p for p in projects if str(p.id).startswith(project_id)), None)

        if not project:
            return {
                "success": False,
                "error": f"Project not found: {project_id}",
            }

        # Get hierarchies
        hierarchies = service.list_hierarchies(str(project.id))

        if not hierarchies:
            return {
                "success": False,
                "error": "No hierarchies found in project",
            }

        # Validate format
        try:
            fmt = OutputFormat(output_format.lower())
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid output format: {output_format}",
                "valid_formats": [f.value for f in OutputFormat],
            }

        # Configure tiers
        tiers = [ProjectTier.TBL_0, ProjectTier.VW_1]
        if include_all_tiers:
            tiers.extend([ProjectTier.DT_2, ProjectTier.DT_3A, ProjectTier.DT_3])

        # Configure generator
        config = ProjectConfig(
            project_name=project.name,
            output_format=fmt,
            target_schema=target_schema,
            include_tiers=tiers,
            generate_dbt=include_dbt,
            generate_docs=True,
            generate_manifest=True,
        )

        # Generate project
        generator = ProjectGenerator()
        result = generator.generate(project, hierarchies, config)

        return {
            "success": True,
            "project_name": result.project_name,
            "source_project_id": str(project.id),
            "output_format": result.output_format.value,
            "hierarchy_count": result.hierarchy_count,
            "tiers_generated": [t.value for t in result.tiers_generated],
            "file_count": result.file_count,
            "ddl_script_count": len(result.ddl_scripts),
            "dbt_project": {
                "name": result.dbt_project.project_name,
                "model_count": result.dbt_project.model_count,
            } if result.dbt_project else None,
            "files": [
                {
                    "name": f.name,
                    "path": f.path,
                    "file_type": f.file_type,
                    "tier": f.tier.value if f.tier else None,
                    "description": f.description,
                    "content": f.content,
                }
                for f in result.files
            ],
            "notes": result.notes,
        }


def _get_recommendations(validation: Dict[str, Any]) -> List[str]:
    """Generate recommendations from validation results."""
    recommendations = []

    if validation["error_count"] > 0:
        recommendations.append("Fix all errors before generating DDL scripts")

    for error in validation.get("errors", []):
        if error["type"] == "orphaned_hierarchy":
            recommendations.append(
                f"Set a valid parent_id for hierarchy '{error['hierarchy_name']}' or make it a root node"
            )
        elif error["type"] == "missing_formula":
            recommendations.append(
                f"Add formula configuration to calculation hierarchy '{error['hierarchy_name']}'"
            )
        elif error["type"] == "circular_dependency":
            recommendations.append("Review parent-child relationships to break the circular dependency")

    for warning in validation.get("warnings", []):
        if warning["type"] == "missing_mappings":
            recommendations.append(
                f"Add source mappings to leaf node '{warning['hierarchy_name']}' or mark as non-leaf"
            )

    if not recommendations and validation["is_valid"]:
        recommendations.append("Design is valid and ready for DDL generation")

    return recommendations
