"""
Source model store for persisting and managing canonical models.

Provides CRUD operations for canonical models with JSON file storage.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import (
    CanonicalModel,
    SourceTable,
    SourceColumn,
    SourceEntity,
    SourceRelationship,
    ColumnMerge,
    EntityType,
    RelationshipType,
    ColumnRole,
    SCDType,
)


class SourceModelStore:
    """
    Store for managing canonical source models.

    Uses JSON file storage for persistence. Each model is stored
    as a separate JSON file in the models directory.

    Example:
        ```python
        store = SourceModelStore("data/source_models")

        # Create a new model
        model = store.create_model("Sales Analysis", "Analysis of sales data")

        # Add tables (typically from analyzer)
        model.tables.append(SourceTable(...))

        # Save changes
        store.save_model(model)

        # Load later
        model = store.get_model(model.id)
        ```
    """

    def __init__(self, storage_path: str = "data/source_models"):
        """
        Initialize the store.

        Args:
            storage_path: Directory path for storing model files.
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

    def _get_model_path(self, model_id: str) -> Path:
        """Get the file path for a model."""
        return self.storage_path / f"{model_id}.json"

    def _serialize_model(self, model: CanonicalModel) -> Dict[str, Any]:
        """Serialize a model to a dictionary for JSON storage."""
        return {
            "id": model.id,
            "name": model.name,
            "description": model.description,
            "connection_id": model.connection_id,
            "connection_name": model.connection_name,
            "status": model.status,
            "analyzed_at": model.analyzed_at.isoformat() if model.analyzed_at else None,
            "reviewed_at": model.reviewed_at.isoformat() if model.reviewed_at else None,
            "approved_at": model.approved_at.isoformat() if model.approved_at else None,
            "approved_by": model.approved_by,
            "created_at": model.created_at.isoformat(),
            "updated_at": model.updated_at.isoformat(),
            "tables": [self._serialize_table(t) for t in model.tables],
            "entities": [self._serialize_entity(e) for e in model.entities],
            "relationships": [self._serialize_relationship(r) for r in model.relationships],
            "column_merges": [m.to_dict() for m in model.column_merges],
        }

    def _serialize_table(self, table: SourceTable) -> Dict[str, Any]:
        """Serialize a table."""
        return {
            "id": table.id,
            "name": table.name,
            "schema": table.schema,
            "database": table.database,
            "table_type": table.table_type,
            "entity_type": table.entity_type.value if table.entity_type else None,
            "scd_type": table.scd_type.value if table.scd_type else None,
            "confidence": table.confidence,
            "row_count": table.row_count,
            "canonical_name": table.canonical_name,
            "user_entity_type": table.user_entity_type.value if table.user_entity_type else None,
            "approved": table.approved,
            "columns": [self._serialize_column(c) for c in table.columns],
        }

    def _serialize_column(self, col: SourceColumn) -> Dict[str, Any]:
        """Serialize a column."""
        return {
            "id": col.id,
            "name": col.name,
            "data_type": col.data_type,
            "source_table": col.source_table,
            "source_schema": col.source_schema,
            "source_database": col.source_database,
            "nullable": col.nullable,
            "is_primary_key": col.is_primary_key,
            "is_foreign_key": col.is_foreign_key,
            "foreign_key_reference": col.foreign_key_reference,
            "role": col.role.value,
            "entity_type": col.entity_type.value if col.entity_type else None,
            "confidence": col.confidence,
            "distinct_count": col.distinct_count,
            "null_count": col.null_count,
            "sample_values": col.sample_values,
            "canonical_name": col.canonical_name,
            "user_role": col.user_role.value if col.user_role else None,
            "user_entity_type": col.user_entity_type.value if col.user_entity_type else None,
            "approved": col.approved,
        }

    def _serialize_entity(self, entity: SourceEntity) -> Dict[str, Any]:
        """Serialize an entity."""
        return {
            "id": entity.id,
            "name": entity.name,
            "entity_type": entity.entity_type.value,
            "description": entity.description,
            "source_tables": entity.source_tables,
            "key_columns": entity.key_columns,
            "attribute_columns": entity.attribute_columns,
            "confidence": entity.confidence,
            "inferred_by": entity.inferred_by,
            "approved": entity.approved,
            "rejected": entity.rejected,
            "user_notes": entity.user_notes,
        }

    def _serialize_relationship(self, rel: SourceRelationship) -> Dict[str, Any]:
        """Serialize a relationship."""
        return {
            "id": rel.id,
            "name": rel.name,
            "source_entity": rel.source_entity,
            "target_entity": rel.target_entity,
            "relationship_type": rel.relationship_type.value,
            "source_columns": rel.source_columns,
            "target_columns": rel.target_columns,
            "confidence": rel.confidence,
            "inferred_by": rel.inferred_by,
            "approved": rel.approved,
            "rejected": rel.rejected,
        }

    def _deserialize_model(self, data: Dict[str, Any]) -> CanonicalModel:
        """Deserialize a model from dictionary."""
        model = CanonicalModel(
            id=data["id"],
            name=data.get("name", ""),
            description=data.get("description", ""),
            connection_id=data.get("connection_id"),
            connection_name=data.get("connection_name", ""),
            status=data.get("status", "draft"),
            approved_by=data.get("approved_by", ""),
        )

        # Parse timestamps
        if data.get("analyzed_at"):
            model.analyzed_at = datetime.fromisoformat(data["analyzed_at"])
        if data.get("reviewed_at"):
            model.reviewed_at = datetime.fromisoformat(data["reviewed_at"])
        if data.get("approved_at"):
            model.approved_at = datetime.fromisoformat(data["approved_at"])
        if data.get("created_at"):
            model.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("updated_at"):
            model.updated_at = datetime.fromisoformat(data["updated_at"])

        # Deserialize tables
        model.tables = [self._deserialize_table(t) for t in data.get("tables", [])]

        # Deserialize entities
        model.entities = [self._deserialize_entity(e) for e in data.get("entities", [])]

        # Deserialize relationships
        model.relationships = [
            self._deserialize_relationship(r) for r in data.get("relationships", [])
        ]

        # Deserialize column merges
        model.column_merges = [
            self._deserialize_column_merge(m) for m in data.get("column_merges", [])
        ]

        return model

    def _deserialize_table(self, data: Dict[str, Any]) -> SourceTable:
        """Deserialize a table."""
        table = SourceTable(
            name=data["name"],
            schema=data.get("schema", ""),
            database=data.get("database", ""),
            table_type=data.get("table_type", "TABLE"),
            confidence=data.get("confidence", 0.0),
            row_count=data.get("row_count"),
            canonical_name=data.get("canonical_name"),
            approved=data.get("approved", False),
        )
        table.id = data.get("id", table.id)

        # Parse enums
        if data.get("entity_type"):
            table.entity_type = EntityType(data["entity_type"])
        if data.get("user_entity_type"):
            table.user_entity_type = EntityType(data["user_entity_type"])
        if data.get("scd_type"):
            table.scd_type = SCDType(data["scd_type"])

        # Deserialize columns
        table.columns = [self._deserialize_column(c) for c in data.get("columns", [])]

        return table

    def _deserialize_column(self, data: Dict[str, Any]) -> SourceColumn:
        """Deserialize a column."""
        col = SourceColumn(
            name=data["name"],
            data_type=data.get("data_type", ""),
            source_table=data.get("source_table", ""),
            source_schema=data.get("source_schema", ""),
            source_database=data.get("source_database", ""),
            nullable=data.get("nullable", True),
            is_primary_key=data.get("is_primary_key", False),
            is_foreign_key=data.get("is_foreign_key", False),
            foreign_key_reference=data.get("foreign_key_reference"),
            confidence=data.get("confidence", 0.0),
            distinct_count=data.get("distinct_count"),
            null_count=data.get("null_count"),
            sample_values=data.get("sample_values", []),
            canonical_name=data.get("canonical_name"),
            approved=data.get("approved", False),
        )
        col.id = data.get("id", col.id)

        # Parse enums
        if data.get("role"):
            col.role = ColumnRole(data["role"])
        if data.get("user_role"):
            col.user_role = ColumnRole(data["user_role"])
        if data.get("entity_type"):
            col.entity_type = EntityType(data["entity_type"])
        if data.get("user_entity_type"):
            col.user_entity_type = EntityType(data["user_entity_type"])

        return col

    def _deserialize_entity(self, data: Dict[str, Any]) -> SourceEntity:
        """Deserialize an entity."""
        entity = SourceEntity(
            name=data["name"],
            entity_type=EntityType(data["entity_type"]),
            description=data.get("description", ""),
            source_tables=data.get("source_tables", []),
            key_columns=data.get("key_columns", []),
            attribute_columns=data.get("attribute_columns", []),
            confidence=data.get("confidence", 0.0),
            inferred_by=data.get("inferred_by", ""),
            approved=data.get("approved", False),
            rejected=data.get("rejected", False),
            user_notes=data.get("user_notes", ""),
        )
        entity.id = data.get("id", entity.id)
        return entity

    def _deserialize_relationship(self, data: Dict[str, Any]) -> SourceRelationship:
        """Deserialize a relationship."""
        rel = SourceRelationship(
            name=data["name"],
            source_entity=data["source_entity"],
            target_entity=data["target_entity"],
            relationship_type=RelationshipType(data["relationship_type"]),
            source_columns=data.get("source_columns", []),
            target_columns=data.get("target_columns", []),
            confidence=data.get("confidence", 0.0),
            inferred_by=data.get("inferred_by", ""),
            approved=data.get("approved", False),
            rejected=data.get("rejected", False),
        )
        rel.id = data.get("id", rel.id)
        return rel

    def _deserialize_column_merge(self, data: Dict[str, Any]) -> ColumnMerge:
        """Deserialize a column merge."""
        merge = ColumnMerge(
            canonical_name=data["canonical_name"],
            source_columns=data.get("source_columns", []),
            data_type=data.get("data_type", ""),
            description=data.get("description", ""),
        )
        merge.id = data.get("id", merge.id)
        return merge

    # ==================== CRUD Operations ====================

    def create_model(
        self,
        name: str,
        description: str = "",
        connection_id: Optional[str] = None,
        connection_name: str = "",
    ) -> CanonicalModel:
        """
        Create a new canonical model.

        Args:
            name: Model name.
            description: Model description.
            connection_id: Associated connection ID.
            connection_name: Associated connection name.

        Returns:
            The created model.
        """
        model = CanonicalModel(
            name=name,
            description=description,
            connection_id=connection_id,
            connection_name=connection_name,
        )
        self.save_model(model)
        return model

    def save_model(self, model: CanonicalModel) -> None:
        """
        Save a model to storage.

        Args:
            model: The model to save.
        """
        model.updated_at = datetime.utcnow()
        path = self._get_model_path(model.id)
        data = self._serialize_model(model)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def get_model(self, model_id: str) -> Optional[CanonicalModel]:
        """
        Get a model by ID.

        Args:
            model_id: The model ID.

        Returns:
            The model or None if not found.
        """
        path = self._get_model_path(model_id)
        if not path.exists():
            return None

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return self._deserialize_model(data)

    def list_models(self) -> List[CanonicalModel]:
        """
        List all stored models.

        Returns:
            List of models (without full table/column data for performance).
        """
        models = []
        for path in self.storage_path.glob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                model = self._deserialize_model(data)
                models.append(model)
            except Exception:
                continue

        return sorted(models, key=lambda m: m.updated_at, reverse=True)

    def delete_model(self, model_id: str) -> bool:
        """
        Delete a model.

        Args:
            model_id: The model ID to delete.

        Returns:
            True if deleted, False if not found.
        """
        path = self._get_model_path(model_id)
        if not path.exists():
            return False

        path.unlink()
        return True

    def model_exists(self, model_id: str) -> bool:
        """Check if a model exists."""
        return self._get_model_path(model_id).exists()

    # ==================== Query Operations ====================

    def find_by_connection(self, connection_id: str) -> List[CanonicalModel]:
        """Find all models for a specific connection."""
        return [m for m in self.list_models() if m.connection_id == connection_id]

    def find_by_status(self, status: str) -> List[CanonicalModel]:
        """Find all models with a specific status."""
        return [m for m in self.list_models() if m.status == status]

    def get_summary(self, model_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a summary of a model without loading full data.

        Args:
            model_id: The model ID.

        Returns:
            Summary dictionary or None.
        """
        model = self.get_model(model_id)
        if not model:
            return None

        return {
            "id": model.id,
            "name": model.name,
            "description": model.description,
            "status": model.status,
            "connection_name": model.connection_name,
            "table_count": len(model.tables),
            "entity_count": len(model.entities),
            "relationship_count": len(model.relationships),
            "column_merge_count": len(model.column_merges),
            "approval_progress": model.approval_progress,
            "created_at": model.created_at.isoformat(),
            "updated_at": model.updated_at.isoformat(),
        }
