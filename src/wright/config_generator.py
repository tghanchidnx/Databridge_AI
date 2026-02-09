"""
Mart Configuration Generator.

Generates and manages data mart pipeline configurations:
- Create configurations with 7 parameterization variables
- Add join patterns and column mappings
- Export to dbt YAML format
- Validate configuration completeness
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml

from .types import (
    MartConfig,
    JoinPattern,
    DynamicColumnMapping,
)

logger = logging.getLogger(__name__)


class MartConfigGenerator:
    """Generates and manages mart configurations."""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize the configuration generator.

        Args:
            output_dir: Directory for storing configurations
        """
        self.output_dir = Path(output_dir) if output_dir else Path("data/mart_configs")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._configs: Dict[str, MartConfig] = {}
        self._load()

    def create_config(
        self,
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
    ) -> MartConfig:
        """
        Create a new mart configuration.

        Args:
            project_name: Unique name for this configuration
            report_type: Type of report (GROSS, NET, etc.)
            hierarchy_table: Fully qualified hierarchy table name
            mapping_table: Fully qualified mapping table name
            account_segment: Filter value for ACCOUNT_SEGMENT
            measure_prefix: Prefix for measure columns
            has_sign_change: Whether to apply sign change logic
            has_exclusions: Whether mapping has exclusion rows
            has_group_filter_precedence: Whether to use multi-round filtering
            fact_table: Fact table for joins
            target_database: Target database for generated objects
            target_schema: Target schema for generated objects
            description: Configuration description

        Returns:
            Created MartConfig
        """
        if project_name in self._configs:
            raise ValueError(f"Configuration '{project_name}' already exists")

        config = MartConfig(
            project_name=project_name,
            description=description,
            report_type=report_type.upper(),
            hierarchy_table=hierarchy_table,
            mapping_table=mapping_table,
            fact_table=fact_table,
            account_segment=account_segment.upper(),
            measure_prefix=measure_prefix,
            has_sign_change=has_sign_change,
            has_exclusions=has_exclusions,
            has_group_filter_precedence=has_group_filter_precedence,
            target_database=target_database,
            target_schema=target_schema,
        )

        self._configs[project_name] = config
        self._save()

        logger.info(f"Created mart config: {project_name}")
        return config

    def get_config(self, name: str) -> Optional[MartConfig]:
        """Get a configuration by name."""
        return self._configs.get(name)

    def list_configs(self) -> List[Dict[str, Any]]:
        """List all configurations."""
        return [config.to_summary() for config in self._configs.values()]

    def delete_config(self, name: str) -> bool:
        """Delete a configuration."""
        if name in self._configs:
            del self._configs[name]
            self._save()
            return True
        return False

    def add_join_pattern(
        self,
        config_name: str,
        name: str,
        join_keys: List[str],
        fact_keys: List[str],
        filter: Optional[str] = None,
        description: Optional[str] = None,
    ) -> JoinPattern:
        """
        Add a UNION ALL branch definition to a configuration.

        Args:
            config_name: Name of the configuration
            name: Branch name (e.g., "account", "deduct_product")
            join_keys: DT_2 columns for join
            fact_keys: Fact table columns for join
            filter: Optional WHERE clause filter
            description: Branch description

        Returns:
            Created JoinPattern
        """
        config = self._configs.get(config_name)
        if not config:
            raise ValueError(f"Configuration '{config_name}' not found")

        # Validate same number of keys
        if len(join_keys) != len(fact_keys):
            raise ValueError("join_keys and fact_keys must have same length")

        pattern = JoinPattern(
            name=name,
            description=description,
            join_keys=join_keys,
            fact_keys=fact_keys,
            filter=filter,
        )

        config.add_join_pattern(pattern)
        self._save()

        logger.info(f"Added join pattern '{name}' to config '{config_name}'")
        return pattern

    def add_column_mapping(
        self,
        config_name: str,
        id_source: str,
        physical_column: str,
        dimension_table: Optional[str] = None,
        is_alias: bool = False,
    ) -> DynamicColumnMapping:
        """
        Add ID_SOURCE to physical column mapping.

        Args:
            config_name: Name of the configuration
            id_source: ID_SOURCE value from mapping table
            physical_column: Physical column reference (e.g., "ACCT.ACCOUNT_CODE")
            dimension_table: Dimension table name
            is_alias: Whether this is a typo correction alias

        Returns:
            Created DynamicColumnMapping
        """
        config = self._configs.get(config_name)
        if not config:
            raise ValueError(f"Configuration '{config_name}' not found")

        mapping = DynamicColumnMapping(
            id_source=id_source,
            physical_column=physical_column,
            dimension_table=dimension_table,
            is_alias=is_alias,
        )

        config.add_column_mapping(mapping)
        self._save()

        logger.info(f"Added column mapping '{id_source}' to config '{config_name}'")
        return mapping

    def from_hierarchy_project(
        self,
        project_id: str,
        hierarchy_service,  # HierarchyService instance
        report_type: str = "CUSTOM",
        account_segment: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> MartConfig:
        """
        Generate config from existing DataBridge hierarchy project.

        Uses HierarchyService.get_all_mappings() to extract source mappings
        and auto-detect report_type from hierarchy properties.

        Args:
            project_id: Hierarchy project ID
            hierarchy_service: HierarchyService instance
            report_type: Report type label (auto-detected if "CUSTOM")
            account_segment: Account segment filter
            database: Target database (auto-detected from mappings if not provided)
            schema: Target schema (auto-detected from mappings if not provided)

        Returns:
            Generated MartConfig
        """
        project = hierarchy_service.get_project(project_id)
        if not project:
            raise ValueError(f"Hierarchy project '{project_id}' not found")

        # Extract all mappings using the new get_all_mappings method
        mappings = hierarchy_service.get_all_mappings(project_id)

        # Auto-detect database/schema from first mapping if not provided
        if mappings and not database:
            database = mappings[0].get("source_database", "DB")
        if mappings and not schema:
            schema = mappings[0].get("source_schema", "SCHEMA")
        database = database or "DB"
        schema = schema or "SCHEMA"

        # Auto-detect report_type from hierarchy properties
        if report_type == "CUSTOM":
            hierarchies = hierarchy_service.list_hierarchies(project_id)
            for h in hierarchies:
                dim_props = h.get("dimension_props", {})
                fact_props = h.get("fact_props", {})
                if dim_props:
                    report_type = "DIMENSION"
                    break
                elif fact_props:
                    report_type = "FACT"
                    break

        # Analyze ID_SOURCE distribution
        id_source_counts: Dict[str, int] = {}
        for mapping in mappings:
            id_source = mapping.get("source_column", "")
            if id_source:
                id_source_counts[id_source] = id_source_counts.get(id_source, 0) + 1

        # Get project name for table naming
        proj_name = project.get("name", project_id).upper().replace(" ", "_")

        # Create config
        config = self.create_config(
            project_name=f"{proj_name}_mart",
            report_type=report_type,
            hierarchy_table=f"{database}.{schema}.{proj_name}_HIERARCHY",
            mapping_table=f"{database}.{schema}.{proj_name}_MAPPING",
            account_segment=account_segment or report_type,
            description=f"Generated from hierarchy project '{project.get('name', project_id)}'",
        )

        # Add column mappings from ID_SOURCE distribution
        for id_source in id_source_counts.keys():
            self.add_column_mapping(
                config_name=config.project_name,
                id_source=id_source,
                physical_column=f"DIM.{id_source}",
            )

        # Auto-generate join patterns from source mapping tables
        source_tables = set()
        for mapping in mappings:
            tbl = mapping.get("source_table", "")
            if tbl:
                fqn = f"{mapping.get('source_database', database)}.{mapping.get('source_schema', schema)}.{tbl}"
                source_tables.add(fqn)

        return config

    def validate_config(self, config_name: str) -> Dict[str, Any]:
        """
        Validate configuration completeness and consistency.

        Args:
            config_name: Name of the configuration

        Returns:
            Validation result with issues
        """
        config = self._configs.get(config_name)
        if not config:
            return {
                "valid": False,
                "errors": [f"Configuration '{config_name}' not found"],
            }

        errors = []
        warnings = []

        # Required fields
        if not config.hierarchy_table:
            errors.append("Missing hierarchy_table")
        if not config.mapping_table:
            errors.append("Missing mapping_table")
        if not config.account_segment:
            errors.append("Missing account_segment")

        # Join patterns
        if len(config.join_patterns) == 0:
            warnings.append("No join patterns defined - pipeline will have no UNION ALL branches")

        for pattern in config.join_patterns:
            if len(pattern.join_keys) != len(pattern.fact_keys):
                errors.append(f"Join pattern '{pattern.name}': mismatched key counts")

        # Column mappings
        if len(config.dynamic_column_map) == 0:
            warnings.append("No column mappings defined - VW_1 CASE statement will be empty")

        # Check for duplicate ID_SOURCE values
        id_sources = [m.id_source for m in config.dynamic_column_map]
        duplicates = [s for s in id_sources if id_sources.count(s) > 1]
        if duplicates:
            warnings.append(f"Duplicate ID_SOURCE values: {set(duplicates)}")

        return {
            "valid": len(errors) == 0,
            "config_name": config_name,
            "errors": errors,
            "warnings": warnings,
            "join_pattern_count": len(config.join_patterns),
            "column_mapping_count": len(config.dynamic_column_map),
        }

    def export_yaml(self, config_name: str) -> str:
        """
        Export configuration to dbt YAML format.

        Args:
            config_name: Name of the configuration

        Returns:
            YAML string
        """
        config = self._configs.get(config_name)
        if not config:
            raise ValueError(f"Configuration '{config_name}' not found")

        yaml_dict = config.to_yaml_dict()
        return yaml.dump(yaml_dict, default_flow_style=False, sort_keys=False)

    def export_to_file(
        self,
        config_name: str,
        output_path: Optional[str] = None,
    ) -> str:
        """
        Export configuration to a YAML file.

        Args:
            config_name: Name of the configuration
            output_path: Output file path

        Returns:
            Path to created file
        """
        yaml_content = self.export_yaml(config_name)

        if output_path:
            file_path = Path(output_path)
        else:
            file_path = self.output_dir / f"{config_name}.yml"

        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(yaml_content)

        return str(file_path)

    def import_yaml(self, yaml_content: str, project_name: str) -> MartConfig:
        """
        Import configuration from YAML.

        Args:
            yaml_content: YAML string
            project_name: Name for the imported configuration

        Returns:
            Imported MartConfig
        """
        data = yaml.safe_load(yaml_content)

        config = self.create_config(
            project_name=project_name,
            report_type=data.get("report_type", "CUSTOM"),
            hierarchy_table=data.get("hierarchy_table", ""),
            mapping_table=data.get("mapping_table", ""),
            account_segment=data.get("account_segment", ""),
            measure_prefix=data.get("measure_prefix"),
            has_sign_change=data.get("has_sign_change", False),
            has_exclusions=data.get("has_exclusions", False),
            has_group_filter_precedence=data.get("has_group_filter_precedence", False),
        )

        # Import column mappings
        for id_source, physical_column in data.get("dynamic_column_map", {}).items():
            self.add_column_mapping(
                config_name=project_name,
                id_source=id_source,
                physical_column=physical_column,
            )

        # Import join patterns
        for pattern_data in data.get("join_patterns", []):
            self.add_join_pattern(
                config_name=project_name,
                name=pattern_data.get("name", "unnamed"),
                join_keys=pattern_data.get("join_keys", []),
                fact_keys=pattern_data.get("fact_keys", []),
                filter=pattern_data.get("filter"),
            )

        return config

    def update_config(
        self,
        config_name: str,
        **kwargs,
    ) -> MartConfig:
        """
        Update configuration fields.

        Args:
            config_name: Name of the configuration
            **kwargs: Fields to update

        Returns:
            Updated MartConfig
        """
        config = self._configs.get(config_name)
        if not config:
            raise ValueError(f"Configuration '{config_name}' not found")

        updatable_fields = [
            "description", "report_type", "hierarchy_table", "mapping_table",
            "fact_table", "account_segment", "measure_prefix", "has_sign_change",
            "has_exclusions", "has_group_filter_precedence", "target_database",
            "target_schema",
        ]

        for field, value in kwargs.items():
            if field in updatable_fields:
                setattr(config, field, value)

        config.updated_at = datetime.now()
        self._save()

        return config

    def clone_config(
        self,
        source_name: str,
        new_name: str,
    ) -> MartConfig:
        """
        Clone an existing configuration.

        Args:
            source_name: Source configuration name
            new_name: New configuration name

        Returns:
            Cloned MartConfig
        """
        source = self._configs.get(source_name)
        if not source:
            raise ValueError(f"Configuration '{source_name}' not found")

        if new_name in self._configs:
            raise ValueError(f"Configuration '{new_name}' already exists")

        # Create new config with same settings
        new_config = self.create_config(
            project_name=new_name,
            report_type=source.report_type,
            hierarchy_table=source.hierarchy_table,
            mapping_table=source.mapping_table,
            account_segment=source.account_segment,
            measure_prefix=source.measure_prefix,
            has_sign_change=source.has_sign_change,
            has_exclusions=source.has_exclusions,
            has_group_filter_precedence=source.has_group_filter_precedence,
            fact_table=source.fact_table,
            target_database=source.target_database,
            target_schema=source.target_schema,
            description=f"Cloned from {source_name}",
        )

        # Clone join patterns
        for pattern in source.join_patterns:
            self.add_join_pattern(
                config_name=new_name,
                name=pattern.name,
                join_keys=pattern.join_keys.copy(),
                fact_keys=pattern.fact_keys.copy(),
                filter=pattern.filter,
                description=pattern.description,
            )

        # Clone column mappings
        for mapping in source.dynamic_column_map:
            self.add_column_mapping(
                config_name=new_name,
                id_source=mapping.id_source,
                physical_column=mapping.physical_column,
                dimension_table=mapping.dimension_table,
                is_alias=mapping.is_alias,
            )

        return new_config

    def _save(self) -> None:
        """Persist configurations to disk."""
        data = {}
        for name, config in self._configs.items():
            data[name] = {
                "id": config.id,
                "project_name": config.project_name,
                "description": config.description,
                "report_type": config.report_type,
                "hierarchy_table": config.hierarchy_table,
                "mapping_table": config.mapping_table,
                "fact_table": config.fact_table,
                "account_segment": config.account_segment,
                "measure_prefix": config.measure_prefix,
                "has_sign_change": config.has_sign_change,
                "has_exclusions": config.has_exclusions,
                "has_group_filter_precedence": config.has_group_filter_precedence,
                "target_database": config.target_database,
                "target_schema": config.target_schema,
                "dynamic_column_map": [m.to_dict() for m in config.dynamic_column_map],
                "join_patterns": [p.to_dict() for p in config.join_patterns],
                "created_at": config.created_at.isoformat(),
                "updated_at": config.updated_at.isoformat(),
            }

        config_file = self.output_dir / "configs.json"
        config_file.write_text(json.dumps(data, indent=2))

    def _load(self) -> None:
        """Load configurations from disk."""
        config_file = self.output_dir / "configs.json"
        if not config_file.exists():
            return

        try:
            data = json.loads(config_file.read_text())
            for name, config_data in data.items():
                config = MartConfig(
                    id=config_data.get("id"),
                    project_name=config_data["project_name"],
                    description=config_data.get("description"),
                    report_type=config_data["report_type"],
                    hierarchy_table=config_data["hierarchy_table"],
                    mapping_table=config_data["mapping_table"],
                    fact_table=config_data.get("fact_table"),
                    account_segment=config_data["account_segment"],
                    measure_prefix=config_data.get("measure_prefix"),
                    has_sign_change=config_data.get("has_sign_change", False),
                    has_exclusions=config_data.get("has_exclusions", False),
                    has_group_filter_precedence=config_data.get("has_group_filter_precedence", False),
                    target_database=config_data.get("target_database"),
                    target_schema=config_data.get("target_schema"),
                    created_at=datetime.fromisoformat(config_data.get("created_at", datetime.now().isoformat())),
                    updated_at=datetime.fromisoformat(config_data.get("updated_at", datetime.now().isoformat())),
                )

                # Load join patterns
                for p_data in config_data.get("join_patterns", []):
                    pattern = JoinPattern(
                        id=p_data.get("id"),
                        name=p_data["name"],
                        description=p_data.get("description"),
                        join_keys=p_data["join_keys"],
                        fact_keys=p_data["fact_keys"],
                        filter=p_data.get("filter"),
                    )
                    config.join_patterns.append(pattern)

                # Load column mappings
                for m_data in config_data.get("dynamic_column_map", []):
                    mapping = DynamicColumnMapping(
                        id=m_data.get("id"),
                        id_source=m_data["id_source"],
                        physical_column=m_data["physical_column"],
                        dimension_table=m_data.get("dimension_table"),
                        is_alias=m_data.get("is_alias", False),
                    )
                    config.dynamic_column_map.append(mapping)

                self._configs[name] = config

        except Exception as e:
            logger.error(f"Failed to load configurations: {e}")
