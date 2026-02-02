"""
Project Generator for creating complete deployment packages.

Orchestrates the generation of:
- DDL scripts (multi-tier: TBL_0, VW_1, DT_2, DT_3A, DT_3)
- dbt projects
- Documentation
- Deployment scripts
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from .ddl_generator import DDLGenerator, DDLConfig, GeneratedDDL, SQLDialect
from .dbt_generator import DbtProjectGenerator, DbtConfig, GeneratedDbtProject


class ProjectTier(str, Enum):
    """Project tier levels for multi-layer generation."""

    TBL_0 = "TBL_0"  # Base hierarchy tables
    VW_1 = "VW_1"    # Unnest views
    DT_2 = "DT_2"    # Dimension join tables
    DT_3A = "DT_3A"  # Pre-aggregation tables
    DT_3 = "DT_3"    # Final union tables


class OutputFormat(str, Enum):
    """Supported output formats."""

    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    BIGQUERY = "bigquery"
    TSQL = "tsql"
    DBT = "dbt"


@dataclass
class GeneratedFile:
    """A generated file in the project."""

    name: str
    path: str
    content: str
    file_type: str  # sql, yml, md, json
    tier: Optional[ProjectTier] = None
    description: str = ""


@dataclass
class GeneratedProject:
    """A complete generated project."""

    project_name: str
    files: List[GeneratedFile]
    tiers_generated: List[ProjectTier]
    hierarchy_count: int
    output_format: OutputFormat
    ddl_scripts: List[GeneratedDDL] = field(default_factory=list)
    dbt_project: Optional[GeneratedDbtProject] = None
    generated_at: datetime = field(default_factory=datetime.now)
    notes: List[str] = field(default_factory=list)

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def sql_files(self) -> List[GeneratedFile]:
        return [f for f in self.files if f.file_type == "sql"]


@dataclass
class ProjectConfig:
    """Configuration for project generation."""

    project_name: str
    output_format: OutputFormat = OutputFormat.SNOWFLAKE
    target_database: str = ""
    target_schema: str = "HIERARCHIES"

    # Tier options
    include_tiers: List[ProjectTier] = field(default_factory=lambda: [
        ProjectTier.TBL_0, ProjectTier.VW_1
    ])

    # dbt options
    generate_dbt: bool = False
    dbt_source_database: str = ""
    dbt_source_schema: str = ""

    # Documentation
    generate_docs: bool = True
    generate_manifest: bool = True

    # DDL options
    include_drop: bool = True
    use_create_or_replace: bool = True
    include_grants: bool = False
    grant_roles: List[str] = field(default_factory=list)


class ProjectGenerator:
    """
    Orchestrates the complete project generation process.

    Generates:
    - Multi-tier DDL scripts (TBL_0 → DT_3)
    - Optional dbt project
    - Documentation
    - Deployment manifest

    Example:
        from src.hierarchy import HierarchyService

        service = HierarchyService()
        project = service.get_project(project_id)
        hierarchies = service.list_hierarchies(project_id)

        generator = ProjectGenerator()
        result = generator.generate(
            project=project,
            hierarchies=hierarchies,
            config=ProjectConfig(
                project_name="GL_HIERARCHIES",
                output_format=OutputFormat.SNOWFLAKE,
                include_tiers=[ProjectTier.TBL_0, ProjectTier.VW_1, ProjectTier.DT_2],
                generate_dbt=True
            )
        )

        # Write to disk
        generator.write_project(result, output_dir="./output")
    """

    def __init__(self):
        """Initialize the project generator."""
        self.ddl_generator = DDLGenerator()
        self.dbt_generator = DbtProjectGenerator()

    def generate(
        self,
        project: Any,
        hierarchies: List[Any],
        config: ProjectConfig,
    ) -> GeneratedProject:
        """
        Generate a complete project with all requested artifacts.

        Args:
            project: Hierarchy project object.
            hierarchies: List of hierarchy objects.
            config: Project configuration.

        Returns:
            GeneratedProject with all generated files.
        """
        files: List[GeneratedFile] = []
        ddl_scripts: List[GeneratedDDL] = []
        dbt_project: Optional[GeneratedDbtProject] = None
        notes: List[str] = []

        # 1. Generate DDL scripts
        if config.output_format != OutputFormat.DBT:
            ddl_config = DDLConfig(
                dialect=self._format_to_dialect(config.output_format),
                target_database=config.target_database,
                target_schema=config.target_schema,
                include_drop=config.include_drop,
                use_create_or_replace=config.use_create_or_replace,
                include_grants=config.include_grants,
                grant_roles=config.grant_roles,
                generate_tbl_0=ProjectTier.TBL_0 in config.include_tiers,
                generate_vw_1=ProjectTier.VW_1 in config.include_tiers,
                generate_dt_2=ProjectTier.DT_2 in config.include_tiers,
                generate_dt_3a=ProjectTier.DT_3A in config.include_tiers,
                generate_dt_3=ProjectTier.DT_3 in config.include_tiers,
            )

            ddl_scripts = self.ddl_generator.generate(project, hierarchies, ddl_config)

            # Convert DDL scripts to files
            for i, script in enumerate(ddl_scripts):
                tier_prefix = f"{script.tier}_" if script.tier else ""
                files.append(GeneratedFile(
                    name=f"{i+1:02d}_{tier_prefix}{script.object_name}.sql",
                    path=f"ddl/{i+1:02d}_{tier_prefix}{script.object_name}.sql",
                    content=script.sql,
                    file_type="sql",
                    tier=ProjectTier(script.tier) if script.tier else None,
                    description=script.description,
                ))

            # Generate combined deploy script
            combined_sql = self._generate_deploy_script(ddl_scripts, config)
            files.append(GeneratedFile(
                name="deploy_all.sql",
                path="ddl/deploy_all.sql",
                content=combined_sql,
                file_type="sql",
                description="Combined deployment script",
            ))

        # 2. Generate dbt project
        if config.generate_dbt or config.output_format == OutputFormat.DBT:
            dbt_config = DbtConfig(
                project_name=self._safe_name(config.project_name).lower(),
                source_database=config.dbt_source_database or config.target_database,
                source_schema=config.dbt_source_schema or config.target_schema,
                target_schema=config.target_schema,
                generate_tests=True,
                generate_docs=config.generate_docs,
            )

            dbt_project = self.dbt_generator.generate(project, hierarchies, dbt_config)

            # Add dbt files to project files
            for dbt_file in dbt_project.files:
                files.append(GeneratedFile(
                    name=dbt_file.name,
                    path=f"dbt/{dbt_file.path}",
                    content=dbt_file.content,
                    file_type=dbt_file.file_type,
                    description=f"dbt: {dbt_file.name}",
                ))

            notes.append(f"dbt project generated with {dbt_project.model_count} models")

        # 3. Generate documentation
        if config.generate_docs:
            doc_files = self._generate_documentation(project, hierarchies, config)
            files.extend(doc_files)

        # 4. Generate manifest
        if config.generate_manifest:
            manifest = self._generate_manifest(project, hierarchies, config, ddl_scripts, dbt_project)
            files.append(manifest)

        return GeneratedProject(
            project_name=config.project_name,
            files=files,
            tiers_generated=[t for t in config.include_tiers],
            hierarchy_count=len(hierarchies),
            output_format=config.output_format,
            ddl_scripts=ddl_scripts,
            dbt_project=dbt_project,
            notes=notes,
        )

    def write_project(
        self,
        project: GeneratedProject,
        output_dir: str,
    ) -> Dict[str, Any]:
        """
        Write the generated project to disk.

        Args:
            project: Generated project.
            output_dir: Output directory path.

        Returns:
            Dictionary with write statistics.
        """
        output_path = Path(output_dir)
        created_files = []
        created_dirs = set()

        for file in project.files:
            file_path = output_path / file.path

            # Create directory
            dir_path = file_path.parent
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                created_dirs.add(str(dir_path))

            # Write file
            file_path.write_text(file.content, encoding="utf-8")
            created_files.append(str(file_path))

        return {
            "output_dir": str(output_path),
            "files_created": len(created_files),
            "directories_created": len(created_dirs),
            "file_paths": created_files,
        }

    def preview(
        self,
        project: Any,
        hierarchies: List[Any],
        config: ProjectConfig,
    ) -> Dict[str, Any]:
        """
        Preview what will be generated without creating files.

        Args:
            project: Hierarchy project.
            hierarchies: List of hierarchies.
            config: Project configuration.

        Returns:
            Dictionary with preview information.
        """
        # Get DDL preview
        ddl_preview = None
        if config.output_format != OutputFormat.DBT:
            ddl_config = DDLConfig(
                dialect=self._format_to_dialect(config.output_format),
                target_schema=config.target_schema,
                generate_tbl_0=ProjectTier.TBL_0 in config.include_tiers,
                generate_vw_1=ProjectTier.VW_1 in config.include_tiers,
                generate_dt_2=ProjectTier.DT_2 in config.include_tiers,
                generate_dt_3a=ProjectTier.DT_3A in config.include_tiers,
                generate_dt_3=ProjectTier.DT_3 in config.include_tiers,
            )
            ddl_preview = self.ddl_generator.generate_preview(project, hierarchies, ddl_config)

        return {
            "project_name": config.project_name,
            "output_format": config.output_format.value,
            "target_schema": config.target_schema,
            "hierarchy_count": len(hierarchies),
            "tiers": [t.value for t in config.include_tiers],
            "generate_dbt": config.generate_dbt or config.output_format == OutputFormat.DBT,
            "generate_docs": config.generate_docs,
            "ddl_preview": ddl_preview,
            "estimated_files": self._estimate_file_count(config),
        }

    def validate_design(
        self,
        project: Any,
        hierarchies: List[Any],
    ) -> Dict[str, Any]:
        """
        Validate the hierarchy design before generation.

        Checks for:
        - Orphaned hierarchies
        - Missing source mappings on leaf nodes
        - Invalid formula references
        - Circular dependencies

        Args:
            project: Hierarchy project.
            hierarchies: List of hierarchies.

        Returns:
            Dictionary with validation results.
        """
        errors = []
        warnings = []

        # Build lookup maps
        hierarchy_ids = {h.hierarchy_id for h in hierarchies}
        parent_map = {h.hierarchy_id: h.parent_id for h in hierarchies}

        for h in hierarchies:
            # Check for orphaned hierarchies
            if h.parent_id and h.parent_id not in hierarchy_ids:
                errors.append({
                    "type": "orphaned_hierarchy",
                    "hierarchy_id": h.hierarchy_id,
                    "hierarchy_name": h.hierarchy_name,
                    "message": f"Parent '{h.parent_id}' not found",
                })

            # Check leaf nodes have mappings
            if h.is_leaf_node:
                mappings = h.source_mappings or []
                if not mappings and not h.calculation_flag:
                    warnings.append({
                        "type": "missing_mappings",
                        "hierarchy_id": h.hierarchy_id,
                        "hierarchy_name": h.hierarchy_name,
                        "message": "Leaf node has no source mappings",
                    })

            # Check calculation hierarchies have formulas
            if h.calculation_flag and not h.formula_config:
                errors.append({
                    "type": "missing_formula",
                    "hierarchy_id": h.hierarchy_id,
                    "hierarchy_name": h.hierarchy_name,
                    "message": "Calculation hierarchy has no formula",
                })

        # Check for circular dependencies
        circular = self._detect_circular_dependencies(hierarchies)
        if circular:
            errors.append({
                "type": "circular_dependency",
                "hierarchy_ids": circular,
                "message": "Circular parent-child relationship detected",
            })

        return {
            "is_valid": len(errors) == 0,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "errors": errors,
            "warnings": warnings,
            "hierarchy_count": len(hierarchies),
            "leaf_count": sum(1 for h in hierarchies if h.is_leaf_node),
            "calculation_count": sum(1 for h in hierarchies if h.calculation_flag),
        }

    def _format_to_dialect(self, format: OutputFormat) -> SQLDialect:
        """Convert output format to SQL dialect."""
        mapping = {
            OutputFormat.SNOWFLAKE: SQLDialect.SNOWFLAKE,
            OutputFormat.POSTGRESQL: SQLDialect.POSTGRESQL,
            OutputFormat.BIGQUERY: SQLDialect.BIGQUERY,
            OutputFormat.TSQL: SQLDialect.TSQL,
        }
        return mapping.get(format, SQLDialect.SNOWFLAKE)

    def _generate_deploy_script(
        self,
        scripts: List[GeneratedDDL],
        config: ProjectConfig,
    ) -> str:
        """Generate combined deployment script."""
        lines = [
            f"-- Deployment Script for {config.project_name}",
            f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- Target Schema: {config.target_schema}",
            "",
            "-- ============================================================",
            "-- DEPLOYMENT SCRIPT",
            "-- ============================================================",
            "",
        ]

        for script in scripts:
            lines.append(f"-- {script.tier or 'General'}: {script.object_name}")
            lines.append(f"-- {script.description}")
            lines.append(script.sql)
            lines.append("")

        return "\n".join(lines)

    def _generate_documentation(
        self,
        project: Any,
        hierarchies: List[Any],
        config: ProjectConfig,
    ) -> List[GeneratedFile]:
        """Generate documentation files."""
        files = []

        # README
        readme = f"""# {config.project_name}

Generated by DataBridge AI Librarian.

## Overview

- **Project**: {project.name}
- **Hierarchies**: {len(hierarchies)}
- **Target Schema**: {config.target_schema}
- **Output Format**: {config.output_format.value}

## Tiers Generated

{chr(10).join(f'- {t.value}' for t in config.include_tiers)}

## Deployment

1. Review the DDL scripts in `ddl/` directory
2. Execute `ddl/deploy_all.sql` in your target environment
3. Verify the created objects

## Files

See `manifest.json` for complete file listing.

## Generated

{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

        files.append(GeneratedFile(
            name="README.md",
            path="README.md",
            content=readme,
            file_type="md",
            description="Project documentation",
        ))

        return files

    def _generate_manifest(
        self,
        project: Any,
        hierarchies: List[Any],
        config: ProjectConfig,
        ddl_scripts: List[GeneratedDDL],
        dbt_project: Optional[GeneratedDbtProject],
    ) -> GeneratedFile:
        """Generate manifest.json."""
        manifest = {
            "project_name": config.project_name,
            "source_project": {
                "id": str(project.id),
                "name": project.name,
            },
            "generated_at": datetime.now().isoformat(),
            "output_format": config.output_format.value,
            "target_schema": config.target_schema,
            "hierarchy_count": len(hierarchies),
            "tiers": [t.value for t in config.include_tiers],
            "ddl_objects": [
                {
                    "type": s.ddl_type.value,
                    "name": s.object_name,
                    "tier": s.tier,
                    "full_name": s.full_name,
                }
                for s in ddl_scripts
            ],
            "dbt_project": {
                "name": dbt_project.project_name,
                "model_count": dbt_project.model_count,
                "source_count": dbt_project.source_count,
            } if dbt_project else None,
        }

        return GeneratedFile(
            name="manifest.json",
            path="manifest.json",
            content=json.dumps(manifest, indent=2),
            file_type="json",
            description="Project manifest",
        )

    def _estimate_file_count(self, config: ProjectConfig) -> int:
        """Estimate number of files that will be generated."""
        count = 0

        # DDL files (one per tier + deploy script)
        if config.output_format != OutputFormat.DBT:
            count += len(config.include_tiers) + 2  # +1 for inserts, +1 for deploy

        # dbt files
        if config.generate_dbt or config.output_format == OutputFormat.DBT:
            count += 8  # Approximate dbt file count

        # Documentation
        if config.generate_docs:
            count += 1  # README

        # Manifest
        if config.generate_manifest:
            count += 1

        return count

    def _detect_circular_dependencies(
        self,
        hierarchies: List[Any],
    ) -> List[str]:
        """Detect circular parent-child relationships."""
        hierarchy_map = {h.hierarchy_id: h for h in hierarchies}
        visited = set()
        path = set()
        circular = []

        def dfs(node_id: str) -> bool:
            if node_id in path:
                return True
            if node_id in visited:
                return False

            visited.add(node_id)
            path.add(node_id)

            node = hierarchy_map.get(node_id)
            if node and node.parent_id:
                if dfs(node.parent_id):
                    circular.append(node_id)
                    return True

            path.remove(node_id)
            return False

        for h in hierarchies:
            if h.hierarchy_id not in visited:
                dfs(h.hierarchy_id)

        return circular

    def _safe_name(self, name: str) -> str:
        """Convert name to safe identifier."""
        import re
        return re.sub(r"[^a-zA-Z0-9_]", "_", name).upper()
