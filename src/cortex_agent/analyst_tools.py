"""
MCP Tools for Cortex Analyst.

Provides 10 MCP tools for natural language to SQL via semantic models:

Semantic Model Management (4):
- create_semantic_model: Create a new semantic model configuration
- add_semantic_table: Add a logical table with dimensions/metrics
- deploy_semantic_model: Deploy model YAML to Snowflake stage
- list_semantic_models: List configured semantic models

Natural Language Queries (3):
- analyst_ask: Ask question, get SQL + explanation
- analyst_ask_and_run: Ask question, execute SQL, return results
- analyst_conversation: Multi-turn conversation with context

Auto-Generation (2):
- generate_model_from_hierarchy: Auto-generate from DataBridge hierarchy
- generate_model_from_faux: Generate from Faux Objects project

Utilities (1):
- validate_semantic_model: Validate model against database
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .analyst_client import AnalystClient
from .analyst_types import AnalystQueryResult
from .semantic_model import SemanticModelManager

logger = logging.getLogger(__name__)

# Module-level state
_analyst_client: Optional[AnalystClient] = None
_model_manager: Optional[SemanticModelManager] = None


def _get_query_func(settings):
    """Get the query function from connections API."""
    try:
        try:
            from src.connections_api import get_client
        except ImportError:
            from connections_api import get_client

        client = get_client(settings)

        def query_func(connection_id: str, query: str) -> List[Dict]:
            return client.execute_query(connection_id, query)

        return query_func
    except Exception as e:
        logger.warning(f"Failed to get connections API client: {e}")
        return None


def _get_token_func(connection_id: str, settings):
    """Get a function that returns auth tokens for Snowflake."""
    def get_token() -> str:
        # This would be implemented based on your auth mechanism
        # For now, return a placeholder
        return "placeholder_token"
    return get_token


def _ensure_model_manager(settings) -> SemanticModelManager:
    """Ensure model manager is initialized."""
    global _model_manager
    if _model_manager is None:
        data_dir = Path(settings.data_dir) / "cortex_agent"
        _model_manager = SemanticModelManager(
            data_dir=str(data_dir),
            query_func=_get_query_func(settings),
        )
    return _model_manager


def _get_hierarchy_service():
    """Get the hierarchy service if available."""
    try:
        try:
            from src.hierarchy.service import HierarchyService
        except ImportError:
            from hierarchy.service import HierarchyService
        return HierarchyService()
    except Exception:
        return None


def register_analyst_tools(mcp, settings):
    """Register all Cortex Analyst MCP tools."""

    # =========================================================================
    # Semantic Model Management (4)
    # =========================================================================

    @mcp.tool()
    def create_semantic_model(
        name: str,
        description: str,
        database: str,
        schema_name: str,
    ) -> Dict[str, Any]:
        """
        Create a new semantic model configuration for Cortex Analyst.

        A semantic model defines the business context (tables, dimensions, metrics)
        that Cortex Analyst uses to translate natural language to SQL.

        Args:
            name: Unique model name (used as filename)
            description: Human-readable description of the model
            database: Default Snowflake database for tables
            schema_name: Default Snowflake schema for tables

        Returns:
            Created model configuration

        Example:
            create_semantic_model(
                name="sales_analytics",
                description="Sales data for revenue analysis",
                database="ANALYTICS",
                schema_name="PUBLIC"
            )
        """
        try:
            manager = _ensure_model_manager(settings)
            model = manager.create_model(
                name=name,
                description=description,
                database=database,
                schema_name=schema_name,
            )

            return {
                "status": "created",
                "model": {
                    "name": model.name,
                    "description": model.description,
                    "database": model.database,
                    "schema": model.schema_name,
                },
                "next_steps": [
                    "Use add_semantic_table to add tables with dimensions/metrics",
                    "Use deploy_semantic_model to deploy to Snowflake stage",
                ],
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to create semantic model: {e}")
            return {"error": f"Failed to create model: {e}"}

    @mcp.tool()
    def add_semantic_table(
        model_name: str,
        table_name: str,
        base_table: str,
        description: str = "",
        dimensions: Optional[str] = None,
        time_dimensions: Optional[str] = None,
        metrics: Optional[str] = None,
        facts: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add a logical table with dimensions and metrics to a semantic model.

        The base_table should be a fully qualified Snowflake table name.
        Dimensions, metrics, and facts define the business semantics.

        Args:
            model_name: Name of the semantic model
            table_name: Logical name for this table
            base_table: Fully qualified table (DATABASE.SCHEMA.TABLE)
            description: Table description
            dimensions: JSON array of dimension definitions
            time_dimensions: JSON array of time dimension definitions
            metrics: JSON array of metric definitions (with aggregations)
            facts: JSON array of fact/measure definitions

        Dimension format:
            [{"name": "region", "description": "Sales region", "expr": "REGION_NAME", "data_type": "VARCHAR"}]

        Metric format:
            [{"name": "total_revenue", "description": "Sum of revenue", "expr": "SUM(REVENUE)", "data_type": "NUMBER"}]

        Returns:
            Added table configuration

        Example:
            add_semantic_table(
                model_name="sales_analytics",
                table_name="sales",
                base_table="ANALYTICS.PUBLIC.SALES_FACT",
                dimensions='[{"name": "region", "expr": "region_name", "description": "Sales region", "data_type": "VARCHAR"}]',
                metrics='[{"name": "revenue", "expr": "SUM(amount)", "description": "Total revenue", "data_type": "NUMBER"}]'
            )
        """
        try:
            manager = _ensure_model_manager(settings)

            # Parse base table
            parts = base_table.split(".")
            if len(parts) != 3:
                return {"error": "base_table must be DATABASE.SCHEMA.TABLE format"}

            base_database, base_schema, base_table_name = parts

            # Parse JSON inputs
            dim_list = json.loads(dimensions) if dimensions else None
            time_dim_list = json.loads(time_dimensions) if time_dimensions else None
            metric_list = json.loads(metrics) if metrics else None
            fact_list = json.loads(facts) if facts else None

            table = manager.add_table(
                model_name=model_name,
                table_name=table_name,
                description=description,
                base_database=base_database,
                base_schema=base_schema,
                base_table=base_table_name,
                dimensions=dim_list,
                time_dimensions=time_dim_list,
                metrics=metric_list,
                facts=fact_list,
            )

            return {
                "status": "added",
                "table": {
                    "name": table.name,
                    "base_table": table.base_table.fully_qualified(),
                    "dimensions": len(table.dimensions),
                    "time_dimensions": len(table.time_dimensions),
                    "metrics": len(table.metrics),
                    "facts": len(table.facts),
                },
            }

        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON: {e}"}
        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to add table: {e}")
            return {"error": f"Failed to add table: {e}"}

    @mcp.tool()
    def deploy_semantic_model(
        model_name: str,
        stage_path: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Deploy a semantic model YAML to a Snowflake stage.

        The semantic model must be deployed to a stage that Cortex Analyst
        can access. The stage_path should include the filename.

        Args:
            model_name: Name of the semantic model to deploy
            stage_path: Stage path with filename (e.g., @DB.SCHEMA.STAGE/models/model.yaml)
            connection_id: Optional Snowflake connection for deployment

        Returns:
            Deployment status with local and remote paths

        Example:
            deploy_semantic_model(
                model_name="sales_analytics",
                stage_path="@ANALYTICS.PUBLIC.MODELS/sales_analytics.yaml",
                connection_id="snowflake-prod"
            )
        """
        try:
            manager = _ensure_model_manager(settings)

            # Generate and save YAML
            yaml_content = manager.generate_yaml(model_name)

            result = manager.deploy_to_stage(
                model_name=model_name,
                stage_path=stage_path,
                connection_id=connection_id,
            )

            # Include YAML preview
            result["yaml_preview"] = yaml_content[:500] + "..." if len(yaml_content) > 500 else yaml_content

            return result

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to deploy model: {e}")
            return {"error": f"Failed to deploy: {e}"}

    @mcp.tool()
    def list_semantic_models() -> Dict[str, Any]:
        """
        List all configured semantic models.

        Returns all models with their table counts and metadata.

        Returns:
            List of semantic models with summaries

        Example:
            list_semantic_models()
        """
        try:
            manager = _ensure_model_manager(settings)
            models = manager.list_models()

            return {
                "count": len(models),
                "models": models,
            }

        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            return {"error": f"Failed to list models: {e}"}

    # =========================================================================
    # Natural Language Queries (3)
    # =========================================================================

    @mcp.tool()
    def analyst_ask(
        question: str,
        semantic_model_file: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ask a natural language question and get SQL + explanation.

        Uses Cortex Analyst to translate the question to SQL based on
        the semantic model's business context.

        Args:
            question: Natural language question
            semantic_model_file: Stage path to semantic model YAML
            connection_id: Optional Snowflake connection for context

        Returns:
            Generated SQL and explanation

        Example:
            analyst_ask(
                question="What was total revenue by region last quarter?",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml"
            )
        """
        global _analyst_client

        # For now, return a simulated response since we need actual Snowflake auth
        # In production, this would call the Cortex Analyst REST API
        return {
            "success": True,
            "question": question,
            "semantic_model": semantic_model_file,
            "sql": f"-- SQL generated for: {question}\nSELECT * FROM table LIMIT 10",
            "explanation": f"This query answers your question about: {question}",
            "suggestions": [
                "Show me the trend over time",
                "Break down by product category",
                "Compare to previous period",
            ],
            "note": "Configure Cortex Agent with Snowflake connection for live queries",
        }

    @mcp.tool()
    def analyst_ask_and_run(
        question: str,
        semantic_model_file: str,
        connection_id: str,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Ask a question, generate SQL, and execute it.

        Combines natural language understanding with query execution
        to return actual data results.

        Args:
            question: Natural language question
            semantic_model_file: Stage path to semantic model YAML
            connection_id: Snowflake connection ID for execution
            limit: Maximum rows to return (default 100)

        Returns:
            SQL, explanation, and query results

        Example:
            analyst_ask_and_run(
                question="Show top 5 regions by revenue",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml",
                connection_id="snowflake-prod",
                limit=5
            )
        """
        # Get SQL from analyst
        analyst_response = analyst_ask(
            question=question,
            semantic_model_file=semantic_model_file,
            connection_id=connection_id,
        )

        if not analyst_response.get("success"):
            return analyst_response

        # Execute the SQL
        sql = analyst_response.get("sql", "")
        if not sql or sql.startswith("--"):
            return {
                "success": False,
                "question": question,
                "error": "No executable SQL generated. Configure Cortex Agent with Snowflake connection.",
            }

        try:
            query_func = _get_query_func(settings)
            if query_func:
                # Add limit if not present
                if limit and "LIMIT" not in sql.upper():
                    sql = f"{sql.rstrip(';')} LIMIT {limit}"

                rows = query_func(connection_id, sql)
                columns = list(rows[0].keys()) if rows else []

                return {
                    "success": True,
                    "question": question,
                    "sql": sql,
                    "explanation": analyst_response.get("explanation"),
                    "results": {
                        "columns": columns,
                        "rows": rows[:limit],
                        "row_count": len(rows),
                        "truncated": len(rows) > limit,
                    },
                    "suggestions": analyst_response.get("suggestions", []),
                }
            else:
                return {
                    "success": False,
                    "error": "Query function not available. Check connection configuration.",
                }

        except Exception as e:
            return {
                "success": False,
                "question": question,
                "sql": sql,
                "error": f"Query execution failed: {e}",
            }

    @mcp.tool()
    def analyst_conversation(
        question: str,
        semantic_model_file: str,
        conversation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Have a multi-turn conversation with Cortex Analyst.

        Maintains conversation context for follow-up questions like
        "now break that down by month" or "filter to just Q4".

        Args:
            question: Natural language question or follow-up
            semantic_model_file: Stage path to semantic model YAML
            conversation_id: Optional ID to continue existing conversation

        Returns:
            SQL, explanation, and conversation context

        Example:
            # First question
            result1 = analyst_conversation(
                question="What was total revenue last year?",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml"
            )
            conv_id = result1["conversation_id"]

            # Follow-up
            result2 = analyst_conversation(
                question="Break that down by quarter",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml",
                conversation_id=conv_id
            )
        """
        import uuid

        # Create or continue conversation
        conv_id = conversation_id or str(uuid.uuid4())

        result = analyst_ask(
            question=question,
            semantic_model_file=semantic_model_file,
        )

        result["conversation_id"] = conv_id
        result["is_followup"] = conversation_id is not None

        return result

    # =========================================================================
    # Auto-Generation (2)
    # =========================================================================

    @mcp.tool()
    def generate_model_from_hierarchy(
        project_id: str,
        model_name: Optional[str] = None,
        deploy_to_stage: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Auto-generate a semantic model from a DataBridge hierarchy project.

        Maps hierarchy levels to dimensions, source mappings to base tables,
        and formula groups to metrics.

        Args:
            project_id: DataBridge hierarchy project ID
            model_name: Optional name for the model (defaults to project name)
            deploy_to_stage: Optional stage path to deploy immediately

        Returns:
            Generated model configuration

        Example:
            generate_model_from_hierarchy(
                project_id="revenue-pl",
                model_name="revenue_semantic",
                deploy_to_stage="@ANALYTICS.PUBLIC.MODELS/revenue.yaml"
            )
        """
        try:
            manager = _ensure_model_manager(settings)
            hierarchy_service = _get_hierarchy_service()

            if not hierarchy_service:
                return {
                    "error": "Hierarchy service not available",
                    "suggestion": "Ensure hierarchy module is loaded",
                }

            model = manager.from_hierarchy_project(
                project_id=project_id,
                hierarchy_service=hierarchy_service,
                model_name=model_name,
            )

            result = {
                "status": "generated",
                "model": {
                    "name": model.name,
                    "description": model.description,
                    "tables": len(model.tables),
                    "relationships": len(model.relationships),
                },
                "source_project": project_id,
            }

            # Deploy if requested
            if deploy_to_stage:
                deploy_result = manager.deploy_to_stage(
                    model_name=model.name,
                    stage_path=deploy_to_stage,
                )
                result["deployment"] = deploy_result

            return result

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to generate model from hierarchy: {e}")
            return {"error": f"Generation failed: {e}"}

    @mcp.tool()
    def generate_model_from_schema(
        connection_id: str,
        database: str,
        schema_name: str,
        tables: Optional[str] = None,
        model_name: Optional[str] = None,
        include_sample_values: bool = False,
        deploy_to_stage: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Auto-generate a semantic model from Snowflake schema metadata.

        Scans tables and intelligently maps column types:
        - VARCHAR/TEXT -> Dimensions
        - DATE/TIMESTAMP -> Time Dimensions
        - NUMBER/FLOAT -> Facts (potential metrics)
        - Columns ending in _ID -> Join keys

        Also auto-detects relationships based on naming conventions.

        Args:
            connection_id: Snowflake connection ID
            database: Database name to scan
            schema_name: Schema name to scan
            tables: Optional comma-separated table names (defaults to all)
            model_name: Optional model name (defaults to schema name)
            include_sample_values: Fetch sample values for dimensions
            deploy_to_stage: Optional stage path to deploy immediately

        Returns:
            Generated model summary

        Example:
            generate_model_from_schema(
                connection_id="snowflake-prod",
                database="ANALYTICS",
                schema_name="PUBLIC",
                tables="SALES_FACT,DIM_CUSTOMER,DIM_PRODUCT",
                model_name="sales_model"
            )
        """
        try:
            manager = _ensure_model_manager(settings)

            # Parse tables list
            table_list = None
            if tables:
                table_list = [t.strip() for t in tables.split(",")]

            model = manager.from_snowflake_schema(
                connection_id=connection_id,
                database=database,
                schema_name=schema_name,
                tables=table_list,
                model_name=model_name,
                include_sample_values=include_sample_values,
            )

            result = {
                "status": "generated",
                "model": {
                    "name": model.name,
                    "description": model.description,
                    "tables": len(model.tables),
                    "relationships": len(model.relationships),
                },
                "source": {
                    "database": database,
                    "schema": schema_name,
                    "tables_scanned": table_list or "all",
                },
                "summary": {
                    "total_dimensions": sum(len(t.dimensions) for t in model.tables),
                    "total_time_dimensions": sum(len(t.time_dimensions) for t in model.tables),
                    "total_facts": sum(len(t.facts) for t in model.tables),
                    "total_metrics": sum(len(t.metrics) for t in model.tables),
                },
            }

            # Deploy if requested
            if deploy_to_stage:
                deploy_result = manager.deploy_to_stage(
                    model_name=model.name,
                    stage_path=deploy_to_stage,
                    connection_id=connection_id,
                )
                result["deployment"] = deploy_result

            return result

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to generate model from schema: {e}")
            return {"error": f"Generation failed: {e}"}

    @mcp.tool()
    def generate_model_from_faux(
        faux_project_id: str,
        model_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a semantic model from a Faux Objects project.

        Converts Faux Objects semantic view definitions into a
        Cortex Analyst semantic model.

        Args:
            faux_project_id: Faux Objects project ID
            model_name: Optional name for the model

        Returns:
            Generated model configuration

        Example:
            generate_model_from_faux(
                faux_project_id="abc-123",
                model_name="faux_semantic"
            )
        """
        try:
            # Get Faux Objects service
            try:
                try:
                    from src.faux_objects.service import FauxObjectsService
                except ImportError:
                    from faux_objects.service import FauxObjectsService

                faux_service = FauxObjectsService()
            except ImportError:
                return {
                    "error": "Faux Objects module not available",
                    "suggestion": "Ensure faux_objects module is loaded",
                }

            # Get project
            project = faux_service.get_project(faux_project_id)
            if not project:
                return {"error": f"Faux project '{faux_project_id}' not found"}

            manager = _ensure_model_manager(settings)

            # Create model from Faux project
            model_name = model_name or f"faux_{project.name}"

            model = manager.create_model(
                name=model_name,
                description=f"Generated from Faux project: {project.name}",
                database=project.target_database or "ANALYTICS",
                schema_name=project.target_schema or "PUBLIC",
            )

            # Add semantic view as a table
            if project.semantic_view_definition:
                manager.add_table(
                    model_name=model_name,
                    table_name="semantic_view",
                    description=project.description or "",
                    base_database=project.target_database or "ANALYTICS",
                    base_schema=project.target_schema or "PUBLIC",
                    base_table=project.semantic_view_name or "SEMANTIC_VIEW",
                )

            return {
                "status": "generated",
                "model": {
                    "name": model.name,
                    "tables": len(model.tables),
                },
                "source_faux_project": faux_project_id,
            }

        except Exception as e:
            logger.error(f"Failed to generate model from Faux: {e}")
            return {"error": f"Generation failed: {e}"}

    # =========================================================================
    # Utilities (1)
    # =========================================================================

    @mcp.tool()
    def validate_semantic_model(
        model_name: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate a semantic model configuration.

        Checks for required fields, valid references, and optionally
        validates against the live database.

        Args:
            model_name: Name of the model to validate
            connection_id: Optional Snowflake connection for live validation

        Returns:
            Validation results with errors and warnings

        Example:
            validate_semantic_model(
                model_name="sales_analytics",
                connection_id="snowflake-prod"
            )
        """
        try:
            manager = _ensure_model_manager(settings)
            result = manager.validate_model(
                model_name=model_name,
                connection_id=connection_id,
            )

            # Add YAML preview if valid
            if result.get("valid"):
                try:
                    yaml_content = manager.generate_yaml(model_name)
                    result["yaml_preview"] = yaml_content[:300] + "..." if len(yaml_content) > 300 else yaml_content
                except Exception:
                    pass

            return result

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {"error": f"Validation failed: {e}"}

    # =========================================================================
    # Template Tools (2)
    # =========================================================================

    @mcp.tool()
    def list_semantic_templates(
        domain: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List available semantic model templates.

        Templates provide pre-built semantic models for common use cases
        like sales analytics, financial reporting, and industry-specific models.

        Args:
            domain: Optional filter by domain (sales, finance, operations, marketing)
            industry: Optional filter by industry (general, retail, oil_gas, etc.)

        Returns:
            List of available templates with metadata

        Example:
            list_semantic_templates(domain="finance")
            list_semantic_templates(industry="oil_gas")
        """
        try:
            templates_dir = Path("semantic_templates")
            index_file = templates_dir / "index.json"

            if not index_file.exists():
                return {
                    "templates": [],
                    "message": "No templates directory found",
                }

            with open(index_file, "r") as f:
                index = json.load(f)

            templates = index.get("templates", [])

            # Apply filters
            if domain:
                templates = [t for t in templates if t.get("domain") == domain]
            if industry:
                templates = [t for t in templates if t.get("industry") == industry]

            return {
                "count": len(templates),
                "templates": templates,
                "filters_applied": {
                    "domain": domain,
                    "industry": industry,
                },
            }

        except Exception as e:
            logger.error(f"Failed to list templates: {e}")
            return {"error": str(e)}

    @mcp.tool()
    def create_model_from_template(
        template_id: str,
        model_name: str,
        database: str,
        schema_name: str,
        deploy_to_stage: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a semantic model from a template.

        Loads a pre-built template and customizes it with your database/schema.
        Templates include standard dimensions, metrics, and relationships.

        Args:
            template_id: Template ID (from list_semantic_templates)
            model_name: Name for the new model
            database: Target database name
            schema_name: Target schema name
            deploy_to_stage: Optional stage path to deploy immediately

        Returns:
            Created model configuration

        Example:
            create_model_from_template(
                template_id="sales_analytics",
                model_name="my_sales_model",
                database="ANALYTICS",
                schema_name="PUBLIC"
            )
        """
        try:
            import yaml

            templates_dir = Path("semantic_templates")
            index_file = templates_dir / "index.json"

            if not index_file.exists():
                return {"error": "Templates directory not found"}

            with open(index_file, "r") as f:
                index = json.load(f)

            # Find template
            template_info = None
            for t in index.get("templates", []):
                if t.get("id") == template_id:
                    template_info = t
                    break

            if not template_info:
                return {
                    "error": f"Template '{template_id}' not found",
                    "available": [t["id"] for t in index.get("templates", [])],
                }

            # Load template YAML
            template_file = templates_dir / template_info["file"]
            if not template_file.exists():
                return {"error": f"Template file not found: {template_info['file']}"}

            with open(template_file, "r") as f:
                template_content = f.read()

            # Replace placeholders
            template_content = template_content.replace("{{DATABASE}}", database)
            template_content = template_content.replace("{{SCHEMA}}", schema_name)

            template_data = yaml.safe_load(template_content)

            # Create the model
            manager = _ensure_model_manager(settings)

            model = manager.create_model(
                name=model_name,
                description=template_data.get("description", template_info.get("description", "")),
                database=database,
                schema_name=schema_name,
            )

            # Add tables from template
            for table_data in template_data.get("tables", []):
                base_table = table_data.get("base_table", {})
                manager.add_table(
                    model_name=model_name,
                    table_name=table_data["name"],
                    description=table_data.get("description", ""),
                    base_database=base_table.get("database", database),
                    base_schema=base_table.get("schema", schema_name),
                    base_table=base_table.get("table", table_data["name"].upper()),
                    dimensions=table_data.get("dimensions", []),
                    time_dimensions=table_data.get("time_dimensions", []),
                    metrics=table_data.get("metrics", []),
                    facts=table_data.get("facts", []),
                )

            # Add relationships from template
            for rel_data in template_data.get("relationships", []):
                try:
                    manager.add_relationship(
                        model_name=model_name,
                        left_table=rel_data["left_table"],
                        right_table=rel_data["right_table"],
                        columns=rel_data.get("columns", []),
                        join_type=rel_data.get("join_type", "left_outer"),
                        relationship_type=rel_data.get("relationship_type", "many_to_one"),
                    )
                except ValueError:
                    pass  # Skip if relationship tables don't exist

            result = {
                "status": "created",
                "template_id": template_id,
                "model": {
                    "name": model_name,
                    "tables": len(model.tables),
                    "relationships": len(model.relationships),
                },
                "customization": {
                    "database": database,
                    "schema": schema_name,
                },
            }

            # Deploy if requested
            if deploy_to_stage:
                deploy_result = manager.deploy_to_stage(
                    model_name=model_name,
                    stage_path=deploy_to_stage,
                )
                result["deployment"] = deploy_result

            return result

        except Exception as e:
            logger.error(f"Failed to create model from template: {e}")
            return {"error": str(e)}

    logger.info("Registered 13 Cortex Analyst MCP tools")
    return {
        "tools_registered": 13,
        "categories": {
            "model_management": [
                "create_semantic_model",
                "add_semantic_table",
                "deploy_semantic_model",
                "list_semantic_models",
            ],
            "natural_language": [
                "analyst_ask",
                "analyst_ask_and_run",
                "analyst_conversation",
            ],
            "auto_generation": [
                "generate_model_from_hierarchy",
                "generate_model_from_schema",
                "generate_model_from_faux",
            ],
            "templates": [
                "list_semantic_templates",
                "create_model_from_template",
            ],
            "utilities": [
                "validate_semantic_model",
            ],
        },
    }
