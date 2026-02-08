"""
MCP Tools for Hierarchy-Driven Data Mart Factory (Wright Module).

Provides 18 tools for automated data mart generation:

Configuration Management (3):
- create_mart_config
- add_mart_join_pattern
- export_mart_config

Pipeline Generation (3):
- generate_mart_pipeline
- generate_mart_object
- generate_mart_dbt_project

AI Discovery (2):
- discover_hierarchy_pattern
- suggest_mart_config

Validation (2):
- validate_mart_config
- validate_mart_pipeline

Data Quality (Phase 31) (3):
- validate_hierarchy_quality
- normalize_id_sources
- get_alias_report

Multi-Round Filtering (Phase 31) (2):
- analyze_filter_precedence
- generate_filter_sql

DDL Comparison (Phase 31) (2):
- compare_ddl
- compare_pipeline_ddl

Utility (1):
- list_mart_configs
"""

import json
import logging
from typing import Any, Dict, List, Optional

from .types import (
    MartConfig,
    JoinPattern,
    DynamicColumnMapping,
    PipelineObject,
    PipelineLayer,
    FormulaPrecedence,
)
from .config_generator import MartConfigGenerator
from .pipeline_generator import MartPipelineGenerator
from .formula_engine import FormulaPrecedenceEngine, create_standard_los_formulas
from .cortex_discovery import CortexDiscoveryAgent
from .quality_validator import HierarchyQualityValidator, validate_hierarchy_quality
from .alias_normalizer import IDSourceNormalizer, get_normalizer
from .filter_engine import GroupFilterPrecedenceEngine, analyze_group_filter_precedence
from .ddl_diff import DDLDiffComparator, compare_generated_ddl

logger = logging.getLogger(__name__)


def register_mart_factory_tools(mcp, settings=None) -> Dict[str, Any]:
    """
    Register Mart Factory MCP tools.

    Args:
        mcp: The FastMCP instance
        settings: Optional settings

    Returns:
        Dict with registration info
    """

    # Initialize components
    config_gen = MartConfigGenerator()
    pipeline_gen = MartPipelineGenerator()
    formula_engine = FormulaPrecedenceEngine()
    discovery_agent = CortexDiscoveryAgent()

    # Query function placeholder (set via configure_mart_factory)
    _query_func = None
    _connection_id = None

    # ========================================
    # Configuration Management (3 tools)
    # ========================================

    @mcp.tool()
    def create_mart_config(
        project_name: str,
        report_type: str,
        hierarchy_table: str,
        mapping_table: str,
        account_segment: str,
        measure_prefix: Optional[str] = None,
        has_sign_change: bool = False,
        has_exclusions: bool = False,
        has_group_filter_precedence: bool = False,
        fact_table: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new data mart pipeline configuration.

        The configuration defines the 7 variables that parameterize
        the pipeline generation for any hierarchy type.

        Args:
            project_name: Unique name for this mart config
            report_type: Type of report (GROSS, NET, etc.)
            hierarchy_table: Fully qualified hierarchy table name
            mapping_table: Fully qualified mapping table name
            account_segment: Filter value for ACCOUNT_SEGMENT
            measure_prefix: Prefix for measure columns (default: report_type)
            has_sign_change: Whether to apply sign change logic
            has_exclusions: Whether mapping has exclusion rows
            has_group_filter_precedence: Whether to use multi-round filtering
            fact_table: Fact table for joins
            target_database: Target database for generated objects
            target_schema: Target schema for generated objects
            description: Configuration description

        Returns:
            Created configuration details

        Example:
            create_mart_config(
                project_name="upstream_gross",
                report_type="GROSS",
                hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
                mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
                account_segment="GROSS",
                has_group_filter_precedence=True
            )
        """
        try:
            config = config_gen.create_config(
                project_name=project_name,
                report_type=report_type,
                hierarchy_table=hierarchy_table,
                mapping_table=mapping_table,
                account_segment=account_segment,
                measure_prefix=measure_prefix,
                has_sign_change=has_sign_change,
                has_exclusions=has_exclusions,
                has_group_filter_precedence=has_group_filter_precedence,
                fact_table=fact_table,
                target_database=target_database,
                target_schema=target_schema,
                description=description,
            )

            return {
                "success": True,
                "config_id": config.id,
                "project_name": config.project_name,
                "report_type": config.report_type,
                "account_segment": config.account_segment,
                "message": f"Created mart configuration '{project_name}'",
            }

        except Exception as e:
            logger.error(f"Failed to create mart config: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def add_mart_join_pattern(
        config_name: str,
        name: str,
        join_keys: str,
        fact_keys: str,
        filter: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add a UNION ALL branch definition to a configuration.

        Each join pattern defines how hierarchy metadata joins to the fact table.
        Multiple patterns create UNION ALL branches in DT_3A.

        Args:
            config_name: Name of the configuration
            name: Branch name (e.g., "account", "deduct_product", "royalty")
            join_keys: Comma-separated DT_2 columns for join
            fact_keys: Comma-separated fact table columns for join
            filter: Optional WHERE clause filter (e.g., "ROYALTY_FILTER = 'Y'")
            description: Branch description

        Returns:
            Added pattern details

        Example:
            add_mart_join_pattern(
                config_name="upstream_gross",
                name="deduct_product",
                join_keys="LOS_DEDUCT_CODE_FILTER,LOS_PRODUCT_CODE_FILTER",
                fact_keys="FK_DEDUCT_KEY,FK_PRODUCT_KEY"
            )
        """
        try:
            # Parse comma-separated keys
            join_key_list = [k.strip() for k in join_keys.split(",")]
            fact_key_list = [k.strip() for k in fact_keys.split(",")]

            pattern = config_gen.add_join_pattern(
                config_name=config_name,
                name=name,
                join_keys=join_key_list,
                fact_keys=fact_key_list,
                filter=filter,
                description=description,
            )

            return {
                "success": True,
                "pattern_id": pattern.id,
                "name": pattern.name,
                "join_keys": pattern.join_keys,
                "fact_keys": pattern.fact_keys,
                "filter": pattern.filter,
                "message": f"Added join pattern '{name}' to configuration",
            }

        except Exception as e:
            logger.error(f"Failed to add join pattern: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def export_mart_config(
        config_name: str,
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Export configuration to dbt YAML format.

        Exports the mart configuration as a YAML file that can be used
        with dbt vars or version controlled.

        Args:
            config_name: Name of the configuration
            output_path: Optional output file path

        Returns:
            Exported YAML content or file path

        Example:
            export_mart_config(
                config_name="upstream_gross",
                output_path="./configs/upstream_gross.yml"
            )
        """
        try:
            if output_path:
                file_path = config_gen.export_to_file(config_name, output_path)
                return {
                    "success": True,
                    "config_name": config_name,
                    "file_path": file_path,
                    "message": f"Exported configuration to {file_path}",
                }
            else:
                yaml_content = config_gen.export_yaml(config_name)
                return {
                    "success": True,
                    "config_name": config_name,
                    "yaml_content": yaml_content,
                }

        except Exception as e:
            logger.error(f"Failed to export config: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Pipeline Generation (3 tools)
    # ========================================

    @mcp.tool()
    def generate_mart_pipeline(
        config_name: str,
        output_format: str = "ddl",
        include_formulas: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate the complete 4-object data mart pipeline.

        Creates:
        - VW_1: Translation View (CASE on ID_SOURCE)
        - DT_2: Granularity Dynamic Table (UNPIVOT, exclusions)
        - DT_3A: Pre-Aggregation Fact (UNION ALL branches)
        - DT_3: Data Mart (formula precedence, surrogates)

        Args:
            config_name: Name of the configuration to use
            output_format: Output format - "ddl" or "summary"
            include_formulas: Whether to include standard LOS formulas

        Returns:
            Generated pipeline objects

        Example:
            generate_mart_pipeline(
                config_name="upstream_gross",
                output_format="ddl"
            )
        """
        try:
            config = config_gen.get_config(config_name)
            if not config:
                return {"success": False, "error": f"Configuration '{config_name}' not found"}

            # Get formulas if requested
            formulas = None
            if include_formulas:
                formulas = create_standard_los_formulas(config.report_type)

            # Generate pipeline
            objects = pipeline_gen.generate_full_pipeline(config, formulas)

            if output_format == "ddl":
                return {
                    "success": True,
                    "config_name": config_name,
                    "object_count": len(objects),
                    "objects": [
                        {
                            "name": obj.object_name,
                            "layer": obj.layer.value,
                            "type": obj.object_type.value,
                            "ddl": obj.ddl,
                        }
                        for obj in objects
                    ],
                    "message": f"Generated {len(objects)} pipeline objects",
                }
            else:
                return {
                    "success": True,
                    "config_name": config_name,
                    "object_count": len(objects),
                    "objects": [obj.to_dict() for obj in objects],
                    "message": f"Generated {len(objects)} pipeline objects",
                }

        except Exception as e:
            logger.error(f"Failed to generate pipeline: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_mart_object(
        config_name: str,
        layer: str,
    ) -> Dict[str, Any]:
        """
        Generate a single pipeline object.

        Generate just one layer of the pipeline for inspection or testing.

        Args:
            config_name: Name of the configuration
            layer: Pipeline layer - "VW_1", "DT_2", "DT_3A", or "DT_3"

        Returns:
            Generated DDL for the specified layer

        Example:
            generate_mart_object(
                config_name="upstream_gross",
                layer="VW_1"
            )
        """
        try:
            config = config_gen.get_config(config_name)
            if not config:
                return {"success": False, "error": f"Configuration '{config_name}' not found"}

            layer_upper = layer.upper()

            if layer_upper == "VW_1":
                obj = pipeline_gen.generate_vw1(config)
            elif layer_upper == "DT_2":
                obj = pipeline_gen.generate_dt2(config)
            elif layer_upper == "DT_3A":
                obj = pipeline_gen.generate_dt3a(config)
            elif layer_upper == "DT_3":
                formulas = create_standard_los_formulas(config.report_type)
                obj = pipeline_gen.generate_dt3(config, formulas)
            else:
                return {"success": False, "error": f"Unknown layer: {layer}. Use VW_1, DT_2, DT_3A, or DT_3"}

            return {
                "success": True,
                "config_name": config_name,
                "layer": obj.layer.value,
                "object_name": obj.object_name,
                "object_type": obj.object_type.value,
                "ddl": obj.ddl,
                "description": obj.description,
            }

        except Exception as e:
            logger.error(f"Failed to generate object: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_mart_dbt_project(
        config_name: str,
        dbt_project_name: str,
        output_dir: str,
    ) -> Dict[str, Any]:
        """
        Generate a complete dbt project from mart configuration.

        Creates dbt model files, schema.yml, and project configuration
        from the mart pipeline configuration.

        Args:
            config_name: Name of the mart configuration
            dbt_project_name: Name for the dbt project
            output_dir: Output directory for dbt files

        Returns:
            Generated file paths

        Example:
            generate_mart_dbt_project(
                config_name="upstream_gross",
                dbt_project_name="upstream_gross_marts",
                output_dir="./dbt_projects/upstream_gross"
            )
        """
        try:
            config = config_gen.get_config(config_name)
            if not config:
                return {"success": False, "error": f"Configuration '{config_name}' not found"}

            # Generate formulas
            formulas = create_standard_los_formulas(config.report_type)

            # Generate dbt models
            models_dir = f"{output_dir}/models/marts"
            result = pipeline_gen.generate_dbt_models(config, models_dir, formulas)

            return {
                "success": True,
                "config_name": config_name,
                "dbt_project_name": dbt_project_name,
                "output_dir": output_dir,
                "files_created": list(result.keys()),
                "file_paths": result,
                "message": f"Generated dbt project with {len(result)} files",
            }

        except Exception as e:
            logger.error(f"Failed to generate dbt project: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # AI Discovery (2 tools)
    # ========================================

    @mcp.tool()
    def discover_hierarchy_pattern(
        hierarchy_table: str,
        mapping_table: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Use AI to discover hierarchy structure and suggest configuration.

        Scans the hierarchy and mapping tables to detect:
        - Hierarchy type (P&L, Balance Sheet, LOS, etc.)
        - Level structure and naming conventions
        - Optimal join patterns for UNION ALL branches
        - ID_SOURCE to physical column mappings
        - Data quality issues (typos, orphans, duplicates)

        Uses Snowflake Cortex COMPLETE() for intelligent pattern detection.

        Args:
            hierarchy_table: Fully qualified hierarchy table name
            mapping_table: Fully qualified mapping table name
            connection_id: Snowflake connection for queries

        Returns:
            Discovery result with suggested configuration

        Example:
            discover_hierarchy_pattern(
                hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
                mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
                connection_id="snowflake-prod"
            )
        """
        try:
            result = discovery_agent.discover_hierarchy(
                hierarchy_table=hierarchy_table,
                mapping_table=mapping_table,
                connection_id=connection_id,
            )

            return {
                "success": True,
                "hierarchy_table": result.hierarchy_table,
                "mapping_table": result.mapping_table,
                "hierarchy_type": result.hierarchy_type,
                "level_count": result.level_count,
                "node_count": result.node_count,
                "mapping_count": result.mapping_count,
                "id_source_distribution": result.id_source_distribution,
                "join_patterns": [p.to_dict() for p in result.join_pattern_suggestion],
                "column_mappings": [m.to_dict() for m in result.column_map_suggestion],
                "data_quality_issues": [i.to_dict() for i in result.data_quality_issues],
                "confidence_score": result.confidence_score,
                "explanation": result.explanation,
            }

        except Exception as e:
            logger.error(f"Failed to discover hierarchy: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def suggest_mart_config(
        hierarchy_table: str,
        mapping_table: str,
        project_name: Optional[str] = None,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get AI-recommended configuration for a hierarchy.

        Analyzes the hierarchy and mapping tables to generate a complete
        mart configuration recommendation.

        Args:
            hierarchy_table: Fully qualified hierarchy table name
            mapping_table: Fully qualified mapping table name
            project_name: Optional project name (auto-generated if not provided)
            connection_id: Snowflake connection for queries

        Returns:
            Recommended configuration

        Example:
            suggest_mart_config(
                hierarchy_table="ANALYTICS.PUBLIC.TBL_0_NET_LOS_REPORT_HIERARCHY",
                mapping_table="ANALYTICS.PUBLIC.TBL_0_NET_LOS_REPORT_HIERARCHY_MAPPING"
            )
        """
        try:
            result = discovery_agent.discover_hierarchy(
                hierarchy_table=hierarchy_table,
                mapping_table=mapping_table,
                connection_id=connection_id,
            )

            if not result.recommended_config:
                return {
                    "success": False,
                    "error": "Could not generate configuration recommendation",
                }

            config = result.recommended_config
            if project_name:
                config.project_name = project_name

            return {
                "success": True,
                "recommended_config": config.to_yaml_dict(),
                "confidence_score": result.confidence_score,
                "hierarchy_type": result.hierarchy_type,
                "data_quality_issues": len(result.data_quality_issues),
                "explanation": result.explanation,
            }

        except Exception as e:
            logger.error(f"Failed to suggest config: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Validation (2 tools)
    # ========================================

    @mcp.tool()
    def validate_mart_config(
        config_name: str,
    ) -> Dict[str, Any]:
        """
        Validate configuration completeness and consistency.

        Checks that:
        - All required fields are present
        - Join pattern key counts match
        - No duplicate ID_SOURCE values
        - Configuration is ready for pipeline generation

        Args:
            config_name: Name of the configuration to validate

        Returns:
            Validation result with errors and warnings

        Example:
            validate_mart_config(config_name="upstream_gross")
        """
        try:
            result = config_gen.validate_config(config_name)

            return {
                "success": True,
                "valid": result["valid"],
                "config_name": result["config_name"],
                "errors": result["errors"],
                "warnings": result["warnings"],
                "join_pattern_count": result["join_pattern_count"],
                "column_mapping_count": result["column_mapping_count"],
            }

        except Exception as e:
            logger.error(f"Failed to validate config: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def validate_mart_pipeline(
        config_name: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Test generated DDL against source data.

        Validates that:
        - Source tables exist and are accessible
        - Column references are valid
        - Generated DDL is syntactically correct
        - Join patterns produce expected row counts

        Args:
            config_name: Name of the configuration
            connection_id: Snowflake connection for validation queries

        Returns:
            Validation result with per-layer status

        Example:
            validate_mart_pipeline(
                config_name="upstream_gross",
                connection_id="snowflake-prod"
            )
        """
        try:
            config = config_gen.get_config(config_name)
            if not config:
                return {"success": False, "error": f"Configuration '{config_name}' not found"}

            errors = []
            warnings = []
            layer_results = {}

            # Validate configuration first
            config_validation = config_gen.validate_config(config_name)
            if not config_validation["valid"]:
                errors.extend(config_validation["errors"])

            warnings.extend(config_validation.get("warnings", []))

            # Generate pipeline to validate DDL syntax
            try:
                objects = pipeline_gen.generate_full_pipeline(config)
                for obj in objects:
                    layer_results[obj.layer.value] = {
                        "object_name": obj.object_name,
                        "ddl_length": len(obj.ddl),
                        "generated": True,
                    }
            except Exception as e:
                errors.append(f"DDL generation failed: {e}")

            # Additional validation with connection (if available)
            # TODO: Implement database validation when query function is available

            is_valid = len(errors) == 0

            return {
                "success": True,
                "valid": is_valid,
                "config_name": config_name,
                "layer_results": layer_results,
                "errors": errors,
                "warnings": warnings,
                "message": "Pipeline validation complete" + (
                    "" if is_valid else f" - {len(errors)} error(s) found"
                ),
            }

        except Exception as e:
            logger.error(f"Failed to validate pipeline: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Additional utility tools
    # ========================================

    @mcp.tool()
    def list_mart_configs() -> Dict[str, Any]:
        """
        List all configured data mart configurations.

        Returns:
            List of configuration summaries

        Example:
            list_mart_configs()
        """
        try:
            configs = config_gen.list_configs()

            return {
                "success": True,
                "config_count": len(configs),
                "configs": configs,
            }

        except Exception as e:
            logger.error(f"Failed to list configs: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Data Quality (Phase 31) - 3 tools
    # ========================================

    @mcp.tool()
    def validate_hierarchy_data_quality(
        hierarchies: str,
        mappings: str,
        hierarchy_table: str = "HIERARCHY",
        mapping_table: str = "MAPPING",
    ) -> Dict[str, Any]:
        """
        Validate hierarchy and mapping data for quality issues.

        Detects:
        - ID_SOURCE typos (e.g., BILLING_CATEGRY_CODE)
        - Duplicate hierarchy keys
        - Orphan nodes (no mappings)
        - Orphan mappings (no hierarchy)
        - FILTER_GROUP mismatches
        - Formula reference issues

        Args:
            hierarchies: JSON array of hierarchy records
            mappings: JSON array of mapping records
            hierarchy_table: Name of hierarchy table
            mapping_table: Name of mapping table

        Returns:
            Validation result with all detected issues

        Example:
            validate_hierarchy_data_quality(
                hierarchies='[{"HIERARCHY_ID": 1, "ACTIVE_FLAG": true}]',
                mappings='[{"FK_REPORT_KEY": 1, "ID_SOURCE": "BILLING_CATEGRY_CODE"}]'
            )
        """
        try:
            import json
            hierarchy_data = json.loads(hierarchies)
            mapping_data = json.loads(mappings)

            result = validate_hierarchy_quality(
                hierarchies=hierarchy_data,
                mappings=mapping_data,
                hierarchy_table=hierarchy_table,
                mapping_table=mapping_table,
            )

            return {
                "success": True,
                **result.to_dict(),
            }

        except Exception as e:
            logger.error(f"Failed to validate hierarchy quality: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def normalize_id_source_values(
        mappings: str,
        auto_detect: bool = True,
        id_source_key: str = "ID_SOURCE",
    ) -> Dict[str, Any]:
        """
        Normalize ID_SOURCE values in mapping data.

        Corrects known typos like:
        - BILLING_CATEGRY_CODE → BILLING_CATEGORY_CODE
        - BILLING_CATEGORY_TYPE → BILLING_CATEGORY_TYPE_CODE

        Args:
            mappings: JSON array of mapping records
            auto_detect: Whether to use fuzzy matching for unknown values
            id_source_key: Key for ID_SOURCE field

        Returns:
            Normalized mappings and correction details

        Example:
            normalize_id_source_values(
                mappings='[{"ID_SOURCE": "BILLING_CATEGRY_CODE", "ID": "4100"}]'
            )
        """
        try:
            import json
            mapping_data = json.loads(mappings)

            normalizer = get_normalizer()
            normalized, results = normalizer.normalize_mapping_data(
                mappings=mapping_data,
                id_source_key=id_source_key,
                auto_detect=auto_detect,
            )

            corrections = [
                {
                    "original": r.original,
                    "normalized": r.normalized,
                    "confidence": r.confidence,
                    "suggestion": r.suggestion,
                }
                for r in results if r.was_aliased
            ]

            return {
                "success": True,
                "mapping_count": len(mapping_data),
                "correction_count": len(corrections),
                "corrections": corrections,
                "normalized_mappings": normalized,
            }

        except Exception as e:
            logger.error(f"Failed to normalize ID_SOURCE values: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def get_id_source_alias_report() -> Dict[str, Any]:
        """
        Get a report of all ID_SOURCE aliases and mappings.

        Returns:
            Report with canonical mappings, aliases, and auto-detected corrections

        Example:
            get_id_source_alias_report()
        """
        try:
            normalizer = get_normalizer()
            report = normalizer.get_alias_report()

            return {
                "success": True,
                **report,
            }

        except Exception as e:
            logger.error(f"Failed to get alias report: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # Multi-Round Filtering (Phase 31) - 2 tools
    # ========================================

    @mcp.tool()
    def analyze_group_filter_precedence(
        mappings: str,
    ) -> Dict[str, Any]:
        """
        Analyze GROUP_FILTER_PRECEDENCE patterns in mapping data.

        Detects multi-round filtering patterns:
        - Precedence 1: Primary dimension join
        - Precedence 2: Secondary filter
        - Precedence 3: Tertiary filter

        Args:
            mappings: JSON array of mapping records with GROUP_FILTER_PRECEDENCE

        Returns:
            Analysis with detected patterns and recommended SQL

        Example:
            analyze_group_filter_precedence(
                mappings='[{"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 1}]'
            )
        """
        try:
            import json
            mapping_data = json.loads(mappings)

            result = analyze_group_filter_precedence(mapping_data)

            return {
                "success": True,
                **result,
            }

        except Exception as e:
            logger.error(f"Failed to analyze filter precedence: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_filter_precedence_sql(
        mappings: str,
    ) -> Dict[str, Any]:
        """
        Generate SQL for GROUP_FILTER_PRECEDENCE multi-round filtering.

        Generates DT_2 CTEs and UNION ALL branch definitions
        based on detected filter patterns.

        Args:
            mappings: JSON array of mapping records with GROUP_FILTER_PRECEDENCE

        Returns:
            SQL snippets for multi-round filtering

        Example:
            generate_filter_precedence_sql(
                mappings='[{"FILTER_GROUP_1": "Taxes", "GROUP_FILTER_PRECEDENCE": 2}]'
            )
        """
        try:
            import json
            mapping_data = json.loads(mappings)

            engine = GroupFilterPrecedenceEngine()
            patterns = engine.analyze_mappings(mapping_data)

            dt2_ctes = engine.generate_dt2_ctes(patterns)
            union_branches = engine.generate_union_branches(patterns)

            return {
                "success": True,
                "pattern_count": len(patterns),
                "dt2_ctes": dt2_ctes,
                "union_branches": union_branches,
                "summary": engine.get_pattern_summary(),
            }

        except Exception as e:
            logger.error(f"Failed to generate filter SQL: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # DDL Comparison (Phase 31) - 2 tools
    # ========================================

    @mcp.tool()
    def compare_ddl_content(
        generated_ddl: str,
        baseline_ddl: str,
        generated_name: str = "generated.sql",
        baseline_name: str = "baseline.sql",
    ) -> Dict[str, Any]:
        """
        Compare generated DDL against baseline DDL.

        Identifies:
        - Overall similarity
        - Column additions/removals/modifications
        - JOIN clause changes
        - WHERE clause changes
        - Breaking changes and warnings

        Args:
            generated_ddl: The generated DDL content
            baseline_ddl: The baseline DDL to compare against
            generated_name: Name of generated file
            baseline_name: Name of baseline file

        Returns:
            Comparison result with differences

        Example:
            compare_ddl_content(
                generated_ddl="CREATE VIEW VW_1 AS SELECT col1 FROM table1",
                baseline_ddl="CREATE VIEW VW_1 AS SELECT col1, col2 FROM table1"
            )
        """
        try:
            comparator = DDLDiffComparator()
            result = comparator.compare_ddl(
                generated_ddl=generated_ddl,
                baseline_ddl=baseline_ddl,
                generated_file=generated_name,
                baseline_file=baseline_name,
            )

            return {
                "success": True,
                **result.to_dict(),
                "unified_diff": result.unified_diff[:5000] if result.unified_diff else "",
            }

        except Exception as e:
            logger.error(f"Failed to compare DDL: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def compare_pipeline_to_baseline(
        config_name: str,
        baseline_dir: str,
    ) -> Dict[str, Any]:
        """
        Compare a generated pipeline against baseline DDL files.

        Compares all 4 pipeline objects (VW_1, DT_2, DT_3A, DT_3)
        against matching files in the baseline directory.

        Args:
            config_name: Name of the mart configuration
            baseline_dir: Directory containing baseline DDL files

        Returns:
            Comparison results for each pipeline object

        Example:
            compare_pipeline_to_baseline(
                config_name="upstream_gross",
                baseline_dir="C:/data/baseline_ddl"
            )
        """
        try:
            config = config_gen.get_config(config_name)
            if not config:
                return {"success": False, "error": f"Configuration '{config_name}' not found"}

            # Generate pipeline
            objects = pipeline_gen.generate_full_pipeline(config)

            # Convert to dicts for comparison
            obj_dicts = [
                {
                    "object_name": obj.object_name,
                    "ddl": obj.ddl,
                }
                for obj in objects
            ]

            # Compare against baseline
            comparator = DDLDiffComparator()
            results = comparator.compare_pipeline(obj_dicts, baseline_dir)

            # Summarize results
            summaries = {}
            breaking_changes = []
            warnings = []

            for obj_name, result in results.items():
                summaries[obj_name] = {
                    "similarity": result.similarity,
                    "is_identical": result.is_identical,
                    "column_diff_count": len(result.column_diffs),
                    "breaking_change_count": len(result.breaking_changes),
                }
                breaking_changes.extend(result.breaking_changes)
                warnings.extend(result.warnings)

            return {
                "success": True,
                "config_name": config_name,
                "object_count": len(objects),
                "baseline_dir": baseline_dir,
                "summaries": summaries,
                "total_breaking_changes": len(breaking_changes),
                "total_warnings": len(warnings),
                "breaking_changes": breaking_changes[:20],
                "warnings": warnings[:20],
            }

        except Exception as e:
            logger.error(f"Failed to compare pipeline: {e}")
            return {"success": False, "error": str(e)}

    # --- Phase 31 Enhancements ---

    @mcp.tool()
    def wright_version_pipeline(
        config_name: str,
        description: str = None,
        bump: str = "patch",
        user: str = None,
    ) -> Dict[str, Any]:
        """
        Create a versioned snapshot of a Wright pipeline configuration.
        Integrates with Data Versioning module (Phase 30).

        Args:
            config_name: Name of the pipeline configuration
            description: Description of changes made
            bump: Version bump type (major, minor, patch)
            user: User creating the version

        Returns:
            Version record with snapshot of pipeline config
        """
        try:
            config = _configs.get(config_name)
            if not config:
                return {"success": False, "error": f"Config not found: {config_name}"}

            # Try to use versioning module
            try:
                from src.versioning import VersionManager, VersionedObjectType, ChangeType
                manager = VersionManager()
                version = manager.snapshot(
                    object_type=VersionedObjectType.HIERARCHY_PROJECT,
                    object_id=f"wright:{config_name}",
                    data=config.model_dump(),
                    change_type=ChangeType.UPDATE,
                    description=description or f"Wright pipeline update: {config_name}",
                    user=user,
                    bump=bump,
                )
                return {
                    "success": True,
                    "config_name": config_name,
                    "version": version.version,
                    "version_number": version.version_number,
                    "changed_at": str(version.changed_at),
                }
            except ImportError:
                return {
                    "success": False,
                    "error": "Versioning module not available. Install Phase 30 first.",
                }

        except Exception as e:
            logger.error(f"Failed to version pipeline: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def wright_generate_test_queries(
        config_name: str,
        test_type: str = "all",
    ) -> Dict[str, Any]:
        """
        Generate SQL test queries to validate Wright pipeline output.

        Args:
            config_name: Name of the pipeline configuration
            test_type: Type of tests (row_count, null_check, formula_validation, all)

        Returns:
            Dictionary of SQL test queries for pipeline validation
        """
        try:
            config = _configs.get(config_name)
            if not config:
                return {"success": False, "error": f"Config not found: {config_name}"}

            tests = {}
            db = config.target_database
            schema = config.target_schema
            prefix = f"{config.report_type}_{config.project_name.upper()}"

            if test_type in ["row_count", "all"]:
                tests["row_count_vw1"] = f"SELECT COUNT(*) as row_count FROM {db}.{schema}.VW_1_{prefix}_TRANSLATED;"
                tests["row_count_dt2"] = f"SELECT COUNT(*) as row_count FROM {db}.{schema}.DT_2_{prefix}_GRANULARITY;"
                tests["row_count_dt3a"] = f"SELECT COUNT(*) as row_count FROM {db}.{schema}.DT_3A_{prefix}_PREAGG;"
                tests["row_count_dt3"] = f"SELECT COUNT(*) as row_count FROM {db}.{schema}.DT_3_{prefix}_MART;"
                tests["row_count_progression"] = f"""
-- Verify row count progression through pipeline
WITH counts AS (
    SELECT 'VW_1' as step, COUNT(*) as cnt FROM {db}.{schema}.VW_1_{prefix}_TRANSLATED
    UNION ALL SELECT 'DT_2', COUNT(*) FROM {db}.{schema}.DT_2_{prefix}_GRANULARITY
    UNION ALL SELECT 'DT_3A', COUNT(*) FROM {db}.{schema}.DT_3A_{prefix}_PREAGG
    UNION ALL SELECT 'DT_3', COUNT(*) FROM {db}.{schema}.DT_3_{prefix}_MART
)
SELECT * FROM counts ORDER BY step;"""

            if test_type in ["null_check", "all"]:
                tests["null_check_keys"] = f"""
-- Check for null surrogate keys in final mart
SELECT COUNT(*) as null_key_count
FROM {db}.{schema}.DT_3_{prefix}_MART
WHERE SURROGATE_KEY IS NULL;"""

            if test_type in ["formula_validation", "all"]:
                tests["formula_gross_profit"] = f"""
-- Validate Gross Profit = Revenue - Taxes - Deducts
WITH calcs AS (
    SELECT
        FK_DATE_KEY,
        SUM(CASE WHEN PRECEDENCE_GROUP = 'REVENUE' THEN {config.measure_prefix}AMOUNT ELSE 0 END) as revenue,
        SUM(CASE WHEN PRECEDENCE_GROUP IN ('TAXES', 'DEDUCTS') THEN {config.measure_prefix}AMOUNT ELSE 0 END) as deductions,
        SUM(CASE WHEN HIERARCHY_NAME = 'GROSS_PROFIT' THEN {config.measure_prefix}AMOUNT ELSE 0 END) as gross_profit
    FROM {db}.{schema}.DT_3_{prefix}_MART
    GROUP BY FK_DATE_KEY
)
SELECT *,
    revenue - deductions as expected_gross_profit,
    ABS(gross_profit - (revenue - deductions)) as variance
FROM calcs
WHERE ABS(gross_profit - (revenue - deductions)) > 0.01;"""

            return {
                "success": True,
                "config_name": config_name,
                "test_type": test_type,
                "test_count": len(tests),
                "tests": tests,
            }

        except Exception as e:
            logger.error(f"Failed to generate test queries: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def wright_analyze_pipeline_health(
        config_name: str,
    ) -> Dict[str, Any]:
        """
        Analyze the health and completeness of a Wright pipeline configuration.

        Args:
            config_name: Name of the pipeline configuration

        Returns:
            Health report with issues, warnings, and recommendations
        """
        try:
            config = _configs.get(config_name)
            if not config:
                return {"success": False, "error": f"Config not found: {config_name}"}

            issues = []
            warnings = []
            recommendations = []
            score = 100

            # Check required fields
            if not config.hierarchy_table:
                issues.append("Missing hierarchy_table - pipeline cannot generate")
                score -= 30
            if not config.mapping_table:
                issues.append("Missing mapping_table - pipeline cannot generate")
                score -= 30

            # Check join patterns
            if not config.join_patterns:
                warnings.append("No join patterns defined - DT_3A will be empty")
                score -= 10
            elif len(config.join_patterns) < 2:
                recommendations.append("Consider adding more join patterns for comprehensive dimension coverage")

            # Check feature flags
            if not config.has_group_filter_precedence:
                recommendations.append("Enable has_group_filter_precedence for multi-round filtering")
            if not config.has_exclusions:
                recommendations.append("Enable has_exclusions if you need to filter out specific categories")

            # Check dynamic column map
            if not config.dynamic_column_map:
                warnings.append("No ID_SOURCE mappings defined - VW_1 CASE statement will be empty")
                score -= 15

            # Determine health status
            if score >= 90:
                status = "HEALTHY"
            elif score >= 70:
                status = "WARNING"
            elif score >= 50:
                status = "DEGRADED"
            else:
                status = "CRITICAL"

            return {
                "success": True,
                "config_name": config_name,
                "health_score": score,
                "status": status,
                "issue_count": len(issues),
                "warning_count": len(warnings),
                "issues": issues,
                "warnings": warnings,
                "recommendations": recommendations,
            }

        except Exception as e:
            logger.error(f"Failed to analyze pipeline health: {e}")
            return {"success": False, "error": str(e)}

    # Return registration info
    return {
        "tools_registered": 21,
        "tools": [
            # Configuration Management
            "create_mart_config",
            "add_mart_join_pattern",
            "export_mart_config",
            # Pipeline Generation
            "generate_mart_pipeline",
            "generate_mart_object",
            "generate_mart_dbt_project",
            # AI Discovery
            "discover_hierarchy_pattern",
            "suggest_mart_config",
            # Validation
            "validate_mart_config",
            "validate_mart_pipeline",
            # Data Quality (Phase 31)
            "validate_hierarchy_data_quality",
            "normalize_id_source_values",
            "get_id_source_alias_report",
            # Multi-Round Filtering (Phase 31)
            "analyze_group_filter_precedence",
            "generate_filter_precedence_sql",
            # DDL Comparison (Phase 31)
            "compare_ddl_content",
            "compare_pipeline_to_baseline",
            # Phase 31 Enhancements
            "wright_version_pipeline",
            "wright_generate_test_queries",
            "wright_analyze_pipeline_health",
            # Utility
            "list_mart_configs",
        ],
    }
