"""
MCP Tools for Source Intelligence in DataBridge AI Librarian.

Provides tools for managing canonical source models, including:
- Creating and analyzing source models
- Reviewing inferred entities and relationships
- Approving/rejecting mappings
- Defining column merges
"""

from typing import Any, Dict, List, Optional

from fastmcp import FastMCP


def register_source_tools(mcp: FastMCP) -> None:
    """Register all source intelligence MCP tools."""

    # ==================== Model Management Tools ====================

    @mcp.tool()
    def create_source_model(
        name: str,
        description: str = "",
        connection_id: Optional[str] = None,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """
        Create a new empty source model.

        Use this to create a model that can be populated manually
        or through analysis.

        Args:
            name: Model name.
            description: Model description.
            connection_id: Associated database connection ID.
            connection_name: Associated connection name for display.

        Returns:
            Dictionary with created model details.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()
        model = store.create_model(
            name=name,
            description=description,
            connection_id=connection_id,
            connection_name=connection_name,
        )

        return {
            "success": True,
            "model_id": model.id,
            "name": model.name,
            "status": model.status,
            "message": f"Created source model: {model.name}",
        }

    @mcp.tool()
    def list_source_models() -> Dict[str, Any]:
        """
        List all source models.

        Returns:
            Dictionary with list of model summaries.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()
        models = store.list_models()

        return {
            "success": True,
            "count": len(models),
            "models": [
                {
                    "id": m.id,
                    "name": m.name,
                    "status": m.status,
                    "connection_name": m.connection_name,
                    "table_count": len(m.tables),
                    "entity_count": len(m.entities),
                    "relationship_count": len(m.relationships),
                    "approval_progress": m.approval_progress["overall_progress"],
                    "updated_at": m.updated_at.isoformat(),
                }
                for m in models
            ],
        }

    @mcp.tool()
    def get_source_model(model_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a source model.

        Args:
            model_id: Model ID (can be partial).

        Returns:
            Dictionary with full model details.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        # Find by partial ID
        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        return {
            "success": True,
            "model": model.to_dict(),
        }

    @mcp.tool()
    def delete_source_model(model_id: str) -> Dict[str, Any]:
        """
        Delete a source model.

        Args:
            model_id: Model ID to delete.

        Returns:
            Dictionary with deletion status.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        # Find by partial ID
        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        if store.delete_model(model.id):
            return {
                "success": True,
                "message": f"Deleted model: {model.name}",
            }
        else:
            return {
                "success": False,
                "error": "Failed to delete model",
            }

    # ==================== Review Tools ====================

    @mcp.tool()
    def review_source_model(model_id: str) -> Dict[str, Any]:
        """
        Get a review summary of a source model.

        Shows approval progress, pending items, and recommendations.

        Args:
            model_id: Model ID to review.

        Returns:
            Dictionary with review summary including:
            - Approval progress (entities, relationships)
            - List of pending entities to review
            - List of pending relationships to review
            - Recommendations for next steps
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        # Get pending items
        pending_entities = [
            e.to_dict() for e in model.entities
            if not e.approved and not e.rejected
        ]

        pending_relationships = [
            r.to_dict() for r in model.relationships
            if not r.approved and not r.rejected
        ]

        # Generate recommendations
        recommendations = []
        if pending_entities:
            recommendations.append(
                f"Review {len(pending_entities)} pending entities using approve_entity or reject_entity"
            )
        if pending_relationships:
            recommendations.append(
                f"Review {len(pending_relationships)} pending relationships using approve_relationship"
            )
        if not model.column_merges and len(model.tables) > 1:
            recommendations.append(
                "Consider defining column merges for columns that represent the same concept"
            )

        return {
            "success": True,
            "model_id": model.id,
            "model_name": model.name,
            "status": model.status,
            "approval_progress": model.approval_progress,
            "pending_entities": pending_entities[:10],  # Limit for context
            "pending_relationships": pending_relationships[:10],
            "recommendations": recommendations,
        }

    @mcp.tool()
    def get_model_entities(model_id: str) -> Dict[str, Any]:
        """
        Get all entities in a source model.

        Args:
            model_id: Model ID.

        Returns:
            Dictionary with list of entities.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        return {
            "success": True,
            "count": len(model.entities),
            "entities": [e.to_dict() for e in model.entities],
        }

    @mcp.tool()
    def get_model_relationships(model_id: str) -> Dict[str, Any]:
        """
        Get all relationships in a source model.

        Args:
            model_id: Model ID.

        Returns:
            Dictionary with list of relationships.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        return {
            "success": True,
            "count": len(model.relationships),
            "relationships": [r.to_dict() for r in model.relationships],
        }

    # ==================== Approval Tools ====================

    @mcp.tool()
    def approve_entity(
        model_id: str,
        entity_name: str,
    ) -> Dict[str, Any]:
        """
        Approve an entity in a source model.

        Marks the entity as approved, indicating the inferred
        classification is correct.

        Args:
            model_id: Model ID.
            entity_name: Name of the entity to approve.

        Returns:
            Dictionary with approval status.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        if model.approve_entity(entity_name):
            store.save_model(model)
            return {
                "success": True,
                "message": f"Approved entity: {entity_name}",
                "approval_progress": model.approval_progress,
            }
        else:
            return {
                "success": False,
                "error": f"Entity not found: {entity_name}",
            }

    @mcp.tool()
    def reject_entity(
        model_id: str,
        entity_name: str,
        reason: str = "",
    ) -> Dict[str, Any]:
        """
        Reject an entity in a source model.

        Marks the entity as rejected, indicating the inferred
        classification is incorrect.

        Args:
            model_id: Model ID.
            entity_name: Name of the entity to reject.
            reason: Optional reason for rejection.

        Returns:
            Dictionary with rejection status.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        if model.reject_entity(entity_name, reason):
            store.save_model(model)
            return {
                "success": True,
                "message": f"Rejected entity: {entity_name}",
                "approval_progress": model.approval_progress,
            }
        else:
            return {
                "success": False,
                "error": f"Entity not found: {entity_name}",
            }

    @mcp.tool()
    def approve_relationship(
        model_id: str,
        relationship_id: str,
    ) -> Dict[str, Any]:
        """
        Approve a relationship in a source model.

        Args:
            model_id: Model ID.
            relationship_id: ID or name of the relationship to approve.

        Returns:
            Dictionary with approval status.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        if model.approve_relationship(relationship_id):
            store.save_model(model)
            return {
                "success": True,
                "message": f"Approved relationship: {relationship_id}",
                "approval_progress": model.approval_progress,
            }
        else:
            return {
                "success": False,
                "error": f"Relationship not found: {relationship_id}",
            }

    @mcp.tool()
    def reject_relationship(
        model_id: str,
        relationship_id: str,
    ) -> Dict[str, Any]:
        """
        Reject a relationship in a source model.

        Args:
            model_id: Model ID.
            relationship_id: ID or name of the relationship to reject.

        Returns:
            Dictionary with rejection status.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        if model.reject_relationship(relationship_id):
            store.save_model(model)
            return {
                "success": True,
                "message": f"Rejected relationship: {relationship_id}",
                "approval_progress": model.approval_progress,
            }
        else:
            return {
                "success": False,
                "error": f"Relationship not found: {relationship_id}",
            }

    # ==================== Modification Tools ====================

    @mcp.tool()
    def rename_entity(
        model_id: str,
        old_name: str,
        new_name: str,
    ) -> Dict[str, Any]:
        """
        Rename an entity in a source model.

        Args:
            model_id: Model ID.
            old_name: Current entity name.
            new_name: New entity name.

        Returns:
            Dictionary with rename status.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        if model.rename_entity(old_name, new_name):
            store.save_model(model)
            return {
                "success": True,
                "message": f"Renamed entity: {old_name} → {new_name}",
            }
        else:
            return {
                "success": False,
                "error": f"Entity not found: {old_name}",
            }

    @mcp.tool()
    def add_relationship(
        model_id: str,
        source_entity: str,
        target_entity: str,
        source_columns: List[str],
        target_columns: List[str],
        relationship_type: str = "many_to_one",
    ) -> Dict[str, Any]:
        """
        Add a new relationship between entities/tables.

        Args:
            model_id: Model ID.
            source_entity: Source table or entity name.
            target_entity: Target table or entity name.
            source_columns: List of source column names for the join.
            target_columns: List of target column names for the join.
            relationship_type: Type of relationship (one_to_one, one_to_many,
                             many_to_one, many_to_many).

        Returns:
            Dictionary with created relationship details.
        """
        from ...source import SourceModelStore
        from ...source.models import RelationshipType

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        try:
            rel_type = RelationshipType(relationship_type)
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid relationship type: {relationship_type}",
                "valid_types": [t.value for t in RelationshipType],
            }

        rel = model.add_relationship(
            source=source_entity,
            target=target_entity,
            source_columns=source_columns,
            target_columns=target_columns,
            relationship_type=rel_type,
            confidence=1.0,
            inferred_by="user",
        )

        store.save_model(model)

        return {
            "success": True,
            "relationship": rel.to_dict(),
            "message": f"Added relationship: {rel.name}",
        }

    @mcp.tool()
    def add_column_merge(
        model_id: str,
        canonical_name: str,
        source_columns: List[str],
        data_type: str = "",
        description: str = "",
    ) -> Dict[str, Any]:
        """
        Define a column merge (multiple source columns → one canonical column).

        Use this when the same concept exists in multiple tables with
        different column names.

        Args:
            model_id: Model ID.
            canonical_name: Name for the canonical column (e.g., "customer_id").
            source_columns: List of source column paths (e.g., ["orders.cust_id", "customers.customer_number"]).
            data_type: Data type for the canonical column.
            description: Description of what this column represents.

        Returns:
            Dictionary with created merge details.

        Example:
            add_column_merge(
                model_id="abc123",
                canonical_name="customer_id",
                source_columns=["orders.CUST_ID", "customers.CustomerNumber"],
                description="Unified customer identifier"
            )
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        merge = model.add_column_merge(
            canonical_name=canonical_name,
            source_columns=source_columns,
            data_type=data_type,
            description=description,
        )

        store.save_model(model)

        return {
            "success": True,
            "merge": merge.to_dict(),
            "message": f"Added column merge: {canonical_name}",
        }

    @mcp.tool()
    def get_column_merges(model_id: str) -> Dict[str, Any]:
        """
        Get all column merges in a source model.

        Args:
            model_id: Model ID.

        Returns:
            Dictionary with list of column merges.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        return {
            "success": True,
            "count": len(model.column_merges),
            "merges": [m.to_dict() for m in model.column_merges],
        }

    # ==================== Approval Batch Tools ====================

    @mcp.tool()
    def approve_all_pending(model_id: str) -> Dict[str, Any]:
        """
        Approve all pending entities and relationships in a model.

        Use with caution - this approves everything without review.

        Args:
            model_id: Model ID.

        Returns:
            Dictionary with approval statistics.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        approved_entities = 0
        approved_relationships = 0

        for e in model.entities:
            if not e.approved and not e.rejected:
                e.approved = True
                approved_entities += 1

        for r in model.relationships:
            if not r.approved and not r.rejected:
                r.approved = True
                approved_relationships += 1

        store.save_model(model)

        return {
            "success": True,
            "approved_entities": approved_entities,
            "approved_relationships": approved_relationships,
            "total_approved": approved_entities + approved_relationships,
            "approval_progress": model.approval_progress,
        }

    # ==================== Discovery Orchestration Tools ====================

    @mcp.tool()
    def discover_from_connection(
        connection_type: str,
        account: str,
        username: str,
        password: str = "",
        database: str = "",
        schema: str = "",
        warehouse: str = "",
        model_name: str = "Discovered Model",
        model_description: str = "",
        include_views: bool = True,
        private_key_path: str = "",
    ) -> Dict[str, Any]:
        """
        Run full source discovery on a database connection.

        Scans the schema, analyzes tables, infers entities and relationships,
        and creates a canonical source model for review.

        Args:
            connection_type: Type of database (snowflake, postgresql, mysql).
            account: Account/host identifier.
            username: Database username.
            password: Database password (or empty for key-pair auth).
            database: Database to scan.
            schema: Schema to scan.
            warehouse: Snowflake warehouse (if applicable).
            model_name: Name for the resulting model.
            model_description: Description for the model.
            include_views: Include views in analysis.
            private_key_path: Path to private key file for key-pair auth.

        Returns:
            Dictionary with discovery results and model ID.

        Example:
            discover_from_connection(
                connection_type="snowflake",
                account="myorg.snowflakecomputing.com",
                username="user",
                password="pass",
                database="ANALYTICS",
                schema="RAW",
                warehouse="COMPUTE_WH",
                model_name="Analytics Discovery"
            )
        """
        from ...source import SourceDiscoveryService, DiscoveryConfig

        # Create adapter based on connection type
        if connection_type.lower() == "snowflake":
            try:
                from ...connections.adapters import SnowflakeAdapter

                adapter = SnowflakeAdapter(
                    account=account,
                    username=username,
                    password=password if password else None,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    private_key_path=private_key_path if private_key_path else None,
                )
            except ImportError:
                return {
                    "success": False,
                    "error": "Snowflake adapter not available. Install snowflake-connector-python.",
                }
        else:
            return {
                "success": False,
                "error": f"Unsupported connection type: {connection_type}",
                "supported_types": ["snowflake"],
            }

        # Configure discovery
        config = DiscoveryConfig(include_views=include_views)
        service = SourceDiscoveryService(config=config)

        # Run discovery
        result = service.discover(
            adapter=adapter,
            database=database,
            schema=schema,
            model_name=model_name,
            model_description=model_description,
        )

        return {
            "success": result.status == "completed",
            "model_id": result.model_id,
            "model_name": result.model_name,
            "status": result.status,
            "tables_discovered": result.tables_discovered,
            "columns_discovered": result.columns_discovered,
            "entities_inferred": result.entities_inferred,
            "relationships_inferred": result.relationships_inferred,
            "high_confidence_entities": result.high_confidence_entities,
            "low_confidence_entities": result.low_confidence_entities,
            "tables_needing_review": result.tables_needing_review[:10],
            "duration_seconds": result.duration_seconds,
            "errors": result.errors,
            "warnings": result.warnings,
        }

    @mcp.tool()
    def discover_from_metadata(
        tables: List[Dict[str, Any]],
        model_name: str = "Discovered Model",
        model_description: str = "",
    ) -> Dict[str, Any]:
        """
        Run discovery from pre-extracted metadata (offline mode).

        Useful when you have table/column metadata from SchemaScanner,
        CSV files, or other sources and want entity/relationship inference.

        Args:
            tables: List of table metadata dictionaries. Each should have:
                - name: Table name
                - schema: Schema name (optional)
                - database: Database name (optional)
                - columns: List of column dicts with name, data_type, nullable
            model_name: Name for the resulting model.
            model_description: Description for the model.

        Returns:
            Dictionary with discovery results.

        Example:
            discover_from_metadata(
                tables=[
                    {
                        "name": "CUSTOMERS",
                        "columns": [
                            {"name": "CUSTOMER_ID", "data_type": "INTEGER"},
                            {"name": "CUSTOMER_NAME", "data_type": "VARCHAR"},
                        ]
                    }
                ],
                model_name="Sample Discovery"
            )
        """
        from ...source import SourceDiscoveryService

        service = SourceDiscoveryService()

        result = service.discover_from_metadata(
            tables=tables,
            model_name=model_name,
            model_description=model_description,
        )

        return {
            "success": result.status == "completed",
            "model_id": result.model_id,
            "model_name": result.model_name,
            "status": result.status,
            "tables_discovered": result.tables_discovered,
            "columns_discovered": result.columns_discovered,
            "entities_inferred": result.entities_inferred,
            "relationships_inferred": result.relationships_inferred,
            "high_confidence_entities": result.high_confidence_entities,
            "low_confidence_entities": result.low_confidence_entities,
            "tables_needing_review": result.tables_needing_review[:10],
            "duration_seconds": result.duration_seconds,
            "errors": result.errors,
            "warnings": result.warnings,
        }

    @mcp.tool()
    def get_entity_types() -> Dict[str, Any]:
        """
        Get all available entity types for classification.

        Returns:
            Dictionary with entity type values and descriptions.
        """
        from ...source.models import EntityType

        return {
            "success": True,
            "entity_types": [
                {
                    "value": t.value,
                    "name": t.name,
                    "description": _get_entity_description(t),
                }
                for t in EntityType
            ],
        }

    @mcp.tool()
    def set_table_entity_type(
        model_id: str,
        table_name: str,
        entity_type: str,
    ) -> Dict[str, Any]:
        """
        Override the inferred entity type for a table.

        Use this to manually classify a table when the automatic
        inference was incorrect.

        Args:
            model_id: Model ID.
            table_name: Name of the table to update.
            entity_type: New entity type (customer, vendor, product, etc.).

        Returns:
            Dictionary with update status.
        """
        from ...source import SourceModelStore
        from ...source.models import EntityType

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        # Validate entity type
        try:
            new_type = EntityType(entity_type)
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid entity type: {entity_type}",
                "valid_types": [t.value for t in EntityType],
            }

        # Find and update table
        table = next(
            (t for t in model.tables if t.name.lower() == table_name.lower()),
            None,
        )

        if not table:
            return {
                "success": False,
                "error": f"Table not found: {table_name}",
            }

        table.user_entity_type = new_type
        table.confidence = 1.0  # User override is 100% confident
        store.save_model(model)

        return {
            "success": True,
            "table": table_name,
            "entity_type": new_type.value,
            "message": f"Set entity type for {table_name} to {new_type.value}",
        }

    @mcp.tool()
    def get_discovery_quality_report(model_id: str) -> Dict[str, Any]:
        """
        Get a quality report for a discovered model.

        Analyzes the model to identify:
        - Tables needing manual classification
        - Low-confidence entities and relationships
        - Missing key columns
        - Data quality issues

        Args:
            model_id: Model ID.

        Returns:
            Dictionary with quality metrics and recommendations.
        """
        from ...source import SourceModelStore

        store = SourceModelStore()

        models = store.list_models()
        model = next((m for m in models if m.id.startswith(model_id)), None)

        if not model:
            return {
                "success": False,
                "error": f"Model not found: {model_id}",
            }

        # Analyze model quality
        unclassified_tables = []
        low_confidence_tables = []
        tables_without_keys = []

        for table in model.tables:
            if not table.entity_type and not table.user_entity_type:
                unclassified_tables.append(table.name)
            elif table.confidence < 0.5 and not table.user_entity_type:
                low_confidence_tables.append({
                    "name": table.name,
                    "confidence": table.confidence,
                    "inferred_type": table.entity_type.value if table.entity_type else None,
                })

            # Check for primary key
            has_pk = any(c.is_primary_key for c in table.columns)
            if not has_pk:
                tables_without_keys.append(table.name)

        # Entity quality
        low_confidence_entities = [
            {
                "name": e.name,
                "type": e.entity_type.value,
                "confidence": e.confidence,
            }
            for e in model.entities
            if e.confidence < 0.5
        ]

        # Relationship quality
        low_confidence_relationships = [
            {
                "name": r.name,
                "source": r.source_entity,
                "target": r.target_entity,
                "confidence": r.confidence,
            }
            for r in model.relationships
            if r.confidence < 0.5
        ]

        # Calculate overall quality score
        total_items = len(model.tables) + len(model.entities) + len(model.relationships)
        issues = (
            len(unclassified_tables)
            + len(low_confidence_tables)
            + len(tables_without_keys)
            + len(low_confidence_entities)
            + len(low_confidence_relationships)
        )
        quality_score = 1.0 - (issues / max(total_items, 1)) if total_items > 0 else 0.0

        recommendations = []
        if unclassified_tables:
            recommendations.append(
                f"Classify {len(unclassified_tables)} unclassified tables using set_table_entity_type"
            )
        if low_confidence_tables:
            recommendations.append(
                f"Review {len(low_confidence_tables)} low-confidence table classifications"
            )
        if tables_without_keys:
            recommendations.append(
                f"Consider adding primary keys to {len(tables_without_keys)} tables"
            )
        if low_confidence_relationships:
            recommendations.append(
                f"Review {len(low_confidence_relationships)} low-confidence relationships"
            )

        return {
            "success": True,
            "model_id": model.id,
            "model_name": model.name,
            "quality_score": round(quality_score, 2),
            "summary": {
                "total_tables": len(model.tables),
                "total_entities": len(model.entities),
                "total_relationships": len(model.relationships),
                "approval_progress": model.approval_progress["overall_progress"],
            },
            "issues": {
                "unclassified_tables": unclassified_tables[:10],
                "low_confidence_tables": low_confidence_tables[:10],
                "tables_without_keys": tables_without_keys[:10],
                "low_confidence_entities": low_confidence_entities[:10],
                "low_confidence_relationships": low_confidence_relationships[:10],
            },
            "recommendations": recommendations,
        }


def _get_entity_description(entity_type) -> str:
    """Get human-readable description of an entity type."""
    descriptions = {
        "employee": "Employee/Staff records",
        "customer": "Customer master data",
        "vendor": "Vendor/Supplier records",
        "product": "Product catalog/SKUs",
        "inventory": "Inventory levels and movements",
        "asset": "Fixed assets/Equipment",
        "department": "Organizational departments",
        "cost_center": "Cost centers/Profit centers",
        "location": "Physical locations/Sites",
        "company": "Legal entities/Companies",
        "chart_of_accounts": "GL Account structure",
        "account": "Financial accounts/Ledgers",
        "date": "Date/Calendar dimensions",
        "transaction": "Financial transactions/Journal entries",
    }
    return descriptions.get(entity_type.value, "Business entity")
