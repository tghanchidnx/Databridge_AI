"""
Semantic Model Manager for Cortex Analyst.

Manages the lifecycle of semantic models:
- Create and configure models
- Generate YAML format for Cortex Analyst
- Deploy to Snowflake stages
- Auto-generate from DataBridge hierarchies
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml

from .analyst_types import (
    BaseTableRef,
    DataType,
    Dimension,
    Fact,
    JoinColumn,
    JoinType,
    LogicalTable,
    Metric,
    RelationshipType,
    SemanticModelConfig,
    TableRelationship,
    TimeDimension,
)

logger = logging.getLogger(__name__)


class SemanticModelManager:
    """Manage semantic models for Cortex Analyst."""

    def __init__(
        self,
        data_dir: str = "data/cortex_agent",
        query_func: Optional[Callable[[str, str], List[Dict]]] = None,
    ):
        """
        Initialize the Semantic Model Manager.

        Args:
            data_dir: Directory for storing model configurations
            query_func: Optional function for executing Snowflake queries
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.models_file = self.data_dir / "semantic_models.json"
        self.query_func = query_func
        self._models: Dict[str, SemanticModelConfig] = {}
        self._load()

    # =========================================================================
    # Model CRUD Operations
    # =========================================================================

    def create_model(
        self,
        name: str,
        description: str,
        database: str,
        schema_name: str,
    ) -> SemanticModelConfig:
        """
        Create a new semantic model configuration.

        Args:
            name: Unique model name
            description: Model description
            database: Default database for tables
            schema_name: Default schema for tables

        Returns:
            Created SemanticModelConfig
        """
        if name in self._models:
            raise ValueError(f"Model '{name}' already exists")

        model = SemanticModelConfig(
            name=name,
            description=description,
            database=database,
            schema_name=schema_name,
            tables=[],
            relationships=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        self._models[name] = model
        self._save()

        logger.info(f"Created semantic model: {name}")
        return model

    def get_model(self, name: str) -> Optional[SemanticModelConfig]:
        """Get a semantic model by name."""
        return self._models.get(name)

    def list_models(self) -> List[Dict[str, Any]]:
        """List all configured semantic models."""
        return [
            {
                "name": m.name,
                "description": m.description,
                "database": m.database,
                "schema": m.schema_name,
                "table_count": len(m.tables),
                "relationship_count": len(m.relationships),
                "version": m.version,
                "updated_at": m.updated_at.isoformat() if m.updated_at else None,
            }
            for m in self._models.values()
        ]

    def delete_model(self, name: str) -> bool:
        """Delete a semantic model."""
        if name in self._models:
            del self._models[name]
            self._save()
            logger.info(f"Deleted semantic model: {name}")
            return True
        return False

    # =========================================================================
    # Table Management
    # =========================================================================

    def add_table(
        self,
        model_name: str,
        table_name: str,
        description: str,
        base_database: str,
        base_schema: str,
        base_table: str,
        dimensions: Optional[List[Dict[str, Any]]] = None,
        time_dimensions: Optional[List[Dict[str, Any]]] = None,
        metrics: Optional[List[Dict[str, Any]]] = None,
        facts: Optional[List[Dict[str, Any]]] = None,
        primary_key: Optional[str] = None,
    ) -> LogicalTable:
        """
        Add a logical table to a semantic model.

        Args:
            model_name: Name of the semantic model
            table_name: Logical table name
            description: Table description
            base_database: Physical table database
            base_schema: Physical table schema
            base_table: Physical table name
            dimensions: List of dimension definitions
            time_dimensions: List of time dimension definitions
            metrics: List of metric definitions
            facts: List of fact definitions
            primary_key: Primary key column name

        Returns:
            Created LogicalTable
        """
        model = self._models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found")

        # Check for duplicate table name
        if any(t.name == table_name for t in model.tables):
            raise ValueError(f"Table '{table_name}' already exists in model")

        # Create base table reference
        base_ref = BaseTableRef(
            database=base_database,
            schema=base_schema,
            table=base_table,
        )

        # Parse dimensions
        dim_list = [Dimension(**d) for d in (dimensions or [])]
        time_dim_list = [TimeDimension(**td) for td in (time_dimensions or [])]
        metric_list = [Metric(**m) for m in (metrics or [])]
        fact_list = [Fact(**f) for f in (facts or [])]

        logical_table = LogicalTable(
            name=table_name,
            description=description,
            base_table=base_ref,
            dimensions=dim_list,
            time_dimensions=time_dim_list,
            metrics=metric_list,
            facts=fact_list,
            primary_key=primary_key,
        )

        model.tables.append(logical_table)
        model.updated_at = datetime.now()
        self._save()

        logger.info(f"Added table '{table_name}' to model '{model_name}'")
        return logical_table

    def add_relationship(
        self,
        model_name: str,
        left_table: str,
        right_table: str,
        columns: List[Dict[str, str]],
        join_type: str = "left_outer",
        relationship_type: str = "many_to_one",
    ) -> TableRelationship:
        """
        Add a relationship between two tables.

        Args:
            model_name: Name of the semantic model
            left_table: Name of left table
            right_table: Name of right table
            columns: List of {"left_column": "...", "right_column": "..."} mappings
            join_type: Type of join (inner, left_outer, right_outer, full_outer)
            relationship_type: Cardinality (one_to_one, one_to_many, many_to_one, many_to_many)

        Returns:
            Created TableRelationship
        """
        model = self._models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found")

        # Validate tables exist
        table_names = [t.name for t in model.tables]
        if left_table not in table_names:
            raise ValueError(f"Table '{left_table}' not found in model")
        if right_table not in table_names:
            raise ValueError(f"Table '{right_table}' not found in model")

        join_columns = [
            JoinColumn(
                left_column=c["left_column"],
                right_column=c["right_column"]
            )
            for c in columns
        ]

        relationship = TableRelationship(
            left_table=left_table,
            right_table=right_table,
            join_type=JoinType(join_type),
            relationship_type=RelationshipType(relationship_type),
            columns=join_columns,
        )

        model.relationships.append(relationship)
        model.updated_at = datetime.now()
        self._save()

        logger.info(f"Added relationship {left_table} -> {right_table} in '{model_name}'")
        return relationship

    # =========================================================================
    # YAML Generation
    # =========================================================================

    def generate_yaml(self, model_name: str) -> str:
        """
        Generate YAML from a semantic model configuration.

        Args:
            model_name: Name of the model

        Returns:
            YAML string for Cortex Analyst
        """
        model = self._models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found")

        # Build YAML structure
        yaml_dict = {
            "name": model.name,
            "description": model.description,
            "tables": [],
        }

        # Add tables
        for table in model.tables:
            table_dict = {
                "name": table.name,
                "description": table.description,
                "base_table": {
                    "database": table.base_table.database,
                    "schema": table.base_table.schema_name,
                    "table": table.base_table.table,
                },
            }

            # Add dimensions
            if table.dimensions:
                table_dict["dimensions"] = [
                    self._dimension_to_dict(d) for d in table.dimensions
                ]

            # Add time dimensions
            if table.time_dimensions:
                table_dict["time_dimensions"] = [
                    self._time_dimension_to_dict(td) for td in table.time_dimensions
                ]

            # Add metrics
            if table.metrics:
                table_dict["metrics"] = [
                    self._metric_to_dict(m) for m in table.metrics
                ]

            # Add facts
            if table.facts:
                table_dict["facts"] = [
                    self._fact_to_dict(f) for f in table.facts
                ]

            yaml_dict["tables"].append(table_dict)

        # Add relationships
        if model.relationships:
            yaml_dict["relationships"] = [
                {
                    "left_table": r.left_table,
                    "right_table": r.right_table,
                    "join_type": r.join_type.value,
                    "relationship_type": r.relationship_type.value,
                    "columns": [
                        {"left_column": c.left_column, "right_column": c.right_column}
                        for c in r.columns
                    ],
                }
                for r in model.relationships
            ]

        return yaml.dump(yaml_dict, default_flow_style=False, sort_keys=False)

    def _dimension_to_dict(self, dim: Dimension) -> Dict[str, Any]:
        """Convert Dimension to YAML dict."""
        d = {
            "name": dim.name,
            "description": dim.description,
            "expr": dim.expr,
            "data_type": dim.data_type,
        }
        if dim.synonyms:
            d["synonyms"] = dim.synonyms
        if dim.unique:
            d["unique"] = dim.unique
        if dim.sample_values:
            d["sample_values"] = dim.sample_values
        return d

    def _time_dimension_to_dict(self, td: TimeDimension) -> Dict[str, Any]:
        """Convert TimeDimension to YAML dict."""
        d = {
            "name": td.name,
            "description": td.description,
            "expr": td.expr,
            "data_type": td.data_type,
        }
        if td.synonyms:
            d["synonyms"] = td.synonyms
        return d

    def _metric_to_dict(self, m: Metric) -> Dict[str, Any]:
        """Convert Metric to YAML dict."""
        d = {
            "name": m.name,
            "description": m.description,
            "expr": m.expr,
            "data_type": m.data_type,
        }
        if m.synonyms:
            d["synonyms"] = m.synonyms
        return d

    def _fact_to_dict(self, f: Fact) -> Dict[str, Any]:
        """Convert Fact to YAML dict."""
        d = {
            "name": f.name,
            "description": f.description,
            "expr": f.expr,
            "data_type": f.data_type,
        }
        if f.synonyms:
            d["synonyms"] = f.synonyms
        return d

    # =========================================================================
    # Deployment
    # =========================================================================

    def deploy_to_stage(
        self,
        model_name: str,
        stage_path: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Deploy semantic model YAML to a Snowflake stage.

        Args:
            model_name: Name of the model to deploy
            stage_path: Full stage path (e.g., @DB.SCHEMA.STAGE/models/model.yaml)
            connection_id: Snowflake connection ID for deployment

        Returns:
            Deployment status
        """
        yaml_content = self.generate_yaml(model_name)

        # Save YAML locally first
        local_path = self.data_dir / f"{model_name}.yaml"
        with open(local_path, "w") as f:
            f.write(yaml_content)

        result = {
            "model_name": model_name,
            "local_path": str(local_path),
            "stage_path": stage_path,
            "yaml_size_bytes": len(yaml_content),
            "deployed": False,
        }

        # Deploy to Snowflake if query function available
        if self.query_func and connection_id:
            try:
                # PUT command to upload file
                put_sql = f"PUT file://{local_path} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                self.query_func(connection_id, put_sql)
                result["deployed"] = True
                result["message"] = f"Deployed to {stage_path}"
                logger.info(f"Deployed model '{model_name}' to {stage_path}")
            except Exception as e:
                result["error"] = str(e)
                logger.error(f"Failed to deploy model: {e}")
        else:
            result["message"] = "YAML generated locally. Use PUT command to deploy to stage."

        return result

    # =========================================================================
    # Auto-Generation from DataBridge
    # =========================================================================

    def from_hierarchy_project(
        self,
        project_id: str,
        hierarchy_service,
        model_name: Optional[str] = None,
    ) -> SemanticModelConfig:
        """
        Auto-generate a semantic model from a DataBridge hierarchy project.

        Maps:
        - Hierarchy levels -> Dimensions
        - Source mappings -> Base tables
        - Formula groups -> Metrics

        Args:
            project_id: DataBridge hierarchy project ID
            hierarchy_service: HierarchyService instance
            model_name: Optional model name (defaults to project name)

        Returns:
            Generated SemanticModelConfig
        """
        # Get project details
        project = hierarchy_service.get_project(project_id)
        if not project:
            raise ValueError(f"Project '{project_id}' not found")

        model_name = model_name or self._slugify(project.get("name", project_id))

        # Get hierarchies in project
        hierarchies = hierarchy_service.list_hierarchies(project_id)

        # Extract source tables from mappings
        source_tables = {}
        for hier in hierarchies:
            hier_id = hier.get("hierarchy_id", hier.get("id"))
            mappings = hierarchy_service.get_source_mappings(project_id, hier_id)

            for mapping in mappings:
                table_key = (
                    mapping.get("source_database", ""),
                    mapping.get("source_schema", ""),
                    mapping.get("source_table", ""),
                )
                if all(table_key) and table_key not in source_tables:
                    source_tables[table_key] = {
                        "database": table_key[0],
                        "schema": table_key[1],
                        "table": table_key[2],
                        "column": mapping.get("source_column", ""),
                    }

        # Determine default database/schema
        if source_tables:
            first_table = next(iter(source_tables.values()))
            default_db = first_table["database"]
            default_schema = first_table["schema"]
        else:
            default_db = project.get("default_database", "ANALYTICS")
            default_schema = project.get("default_schema", "PUBLIC")

        # Create model
        model = self.create_model(
            name=model_name,
            description=f"Auto-generated from hierarchy project: {project.get('name', project_id)}",
            database=default_db,
            schema_name=default_schema,
        )

        # Add tables from source mappings
        for table_key, table_info in source_tables.items():
            table_name = self._slugify(table_info["table"])

            # Create dimension from source column
            dimensions = []
            if table_info["column"]:
                dimensions.append({
                    "name": table_info["column"].lower(),
                    "description": f"Source column from {table_info['table']}",
                    "expr": table_info["column"],
                    "data_type": "VARCHAR",
                })

            try:
                self.add_table(
                    model_name=model_name,
                    table_name=table_name,
                    description=f"Source table: {table_info['table']}",
                    base_database=table_info["database"],
                    base_schema=table_info["schema"],
                    base_table=table_info["table"],
                    dimensions=dimensions,
                )
            except ValueError:
                # Table already exists, skip
                pass

        # Add hierarchy levels as dimensions
        for hier in hierarchies:
            levels = hier.get("levels", {})
            for level_num in range(1, 11):
                level_value = levels.get(f"LEVEL_{level_num}")
                if level_value:
                    # Add as dimension to first table
                    if model.tables:
                        model.tables[0].dimensions.append(
                            Dimension(
                                name=f"level_{level_num}",
                                synonyms=[f"hierarchy level {level_num}"],
                                description=f"Hierarchy level {level_num}",
                                expr=f"LEVEL_{level_num}",
                                data_type="VARCHAR",
                            )
                        )

        # Add formula groups as metrics
        formula_groups = hierarchy_service.list_formula_groups(project_id)
        for fg in formula_groups:
            for rule in fg.get("rules", []):
                if model.tables:
                    model.tables[0].metrics.append(
                        Metric(
                            name=self._slugify(rule.get("name", "metric")),
                            description=rule.get("description", ""),
                            expr=rule.get("expression", "SUM(value)"),
                            data_type="NUMBER",
                        )
                    )

        self._save()
        return model

    # =========================================================================
    # Validation
    # =========================================================================

    def validate_model(
        self,
        model_name: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate a semantic model against the database.

        Args:
            model_name: Name of the model to validate
            connection_id: Optional Snowflake connection for live validation

        Returns:
            Validation results with errors and warnings
        """
        model = self._models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found")

        errors = []
        warnings = []

        # Check for required fields
        if not model.tables:
            errors.append("Model has no tables defined")

        for table in model.tables:
            if not table.dimensions and not table.metrics:
                warnings.append(f"Table '{table.name}' has no dimensions or metrics")

            # Check expressions
            for dim in table.dimensions:
                if not dim.expr:
                    errors.append(f"Dimension '{dim.name}' has no expression")

            for metric in table.metrics:
                if not metric.expr:
                    errors.append(f"Metric '{metric.name}' has no expression")

        # Check relationships reference valid tables
        table_names = {t.name for t in model.tables}
        for rel in model.relationships:
            if rel.left_table not in table_names:
                errors.append(f"Relationship references unknown table '{rel.left_table}'")
            if rel.right_table not in table_names:
                errors.append(f"Relationship references unknown table '{rel.right_table}'")

        # Live validation if connection available
        if self.query_func and connection_id:
            for table in model.tables:
                try:
                    fqn = table.base_table.fully_qualified()
                    self.query_func(connection_id, f"SELECT 1 FROM {fqn} LIMIT 1")
                except Exception as e:
                    errors.append(f"Table '{fqn}' not accessible: {e}")

        return {
            "model_name": model_name,
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "table_count": len(model.tables),
            "relationship_count": len(model.relationships),
        }

    # =========================================================================
    # Persistence
    # =========================================================================

    def _save(self) -> None:
        """Persist models to JSON file."""
        data = {
            name: {
                "name": m.name,
                "description": m.description,
                "database": m.database,
                "schema_name": m.schema_name,
                "version": m.version,
                "created_at": m.created_at.isoformat() if m.created_at else None,
                "updated_at": m.updated_at.isoformat() if m.updated_at else None,
                "tables": [t.model_dump() for t in m.tables],
                "relationships": [r.model_dump() for r in m.relationships],
            }
            for name, m in self._models.items()
        }
        with open(self.models_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _load(self) -> None:
        """Load models from JSON file."""
        if not self.models_file.exists():
            return

        try:
            with open(self.models_file, "r") as f:
                data = json.load(f)

            for name, model_data in data.items():
                # Parse tables
                tables = []
                for t in model_data.get("tables", []):
                    base_table_data = t.get("base_table", {})
                    tables.append(LogicalTable(
                        name=t["name"],
                        description=t.get("description", ""),
                        base_table=BaseTableRef(
                            database=base_table_data.get("database", ""),
                            schema=base_table_data.get("schema_name", base_table_data.get("schema", "")),
                            table=base_table_data.get("table", ""),
                        ),
                        dimensions=[Dimension(**d) for d in t.get("dimensions", [])],
                        time_dimensions=[TimeDimension(**td) for td in t.get("time_dimensions", [])],
                        metrics=[Metric(**m) for m in t.get("metrics", [])],
                        facts=[Fact(**f) for f in t.get("facts", [])],
                        primary_key=t.get("primary_key"),
                    ))

                # Parse relationships
                relationships = []
                for r in model_data.get("relationships", []):
                    relationships.append(TableRelationship(
                        left_table=r["left_table"],
                        right_table=r["right_table"],
                        join_type=JoinType(r.get("join_type", "left_outer")),
                        relationship_type=RelationshipType(r.get("relationship_type", "many_to_one")),
                        columns=[JoinColumn(**c) for c in r.get("columns", [])],
                    ))

                self._models[name] = SemanticModelConfig(
                    name=model_data["name"],
                    description=model_data.get("description", ""),
                    database=model_data.get("database", ""),
                    schema_name=model_data.get("schema_name", ""),
                    version=model_data.get("version", "1.0.0"),
                    created_at=datetime.fromisoformat(model_data["created_at"]) if model_data.get("created_at") else None,
                    updated_at=datetime.fromisoformat(model_data["updated_at"]) if model_data.get("updated_at") else None,
                    tables=tables,
                    relationships=relationships,
                )

        except Exception as e:
            logger.error(f"Failed to load semantic models: {e}")

    def _slugify(self, text: str) -> str:
        """Convert text to a valid identifier slug."""
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", text.lower())
        slug = re.sub(r"^_+|_+$", "", slug)
        return slug[:50]
