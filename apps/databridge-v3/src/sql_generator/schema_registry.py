"""
Schema Registry Service for SQL Generator.

Manages schema metadata for validation and discovery:
- Register database objects (tables, views)
- Validate schema compatibility
- Detect schema drift
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import hashlib
import json

from sqlalchemy.orm import Session

from .models import SchemaRegistryEntry, ObjectType


class SchemaRegistryService:
    """
    Service for managing schema metadata and validation.

    Provides:
    - Schema registration and storage
    - Compatibility validation between objects
    - Schema drift detection via hash comparison
    """

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize the schema registry service.

        Args:
            session: Optional SQLAlchemy session. If not provided,
                    operations will require session parameter.
        """
        self._session = session

    def _get_session(self, session: Optional[Session] = None) -> Session:
        """Get session from parameter or instance."""
        if session:
            return session
        if self._session:
            return self._session
        raise ValueError("No database session available")

    def register_schema(
        self,
        project_id: str,
        object_type: ObjectType,
        object_name: str,
        column_definitions: List[Dict[str, Any]],
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        primary_key_columns: Optional[List[str]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
        indexes: Optional[List[Dict[str, Any]]] = None,
        validation_rules: Optional[List[Dict[str, Any]]] = None,
        session: Optional[Session] = None,
    ) -> SchemaRegistryEntry:
        """
        Register a new schema or update existing one.

        Args:
            project_id: Project ID
            object_type: Type of database object
            object_name: Name of the object
            column_definitions: List of column definitions
            database_name: Optional database name
            schema_name: Optional schema name
            primary_key_columns: List of PK column names
            foreign_keys: List of FK definitions
            indexes: List of index definitions
            validation_rules: List of validation rules
            session: Optional database session

        Returns:
            Created or updated SchemaRegistryEntry
        """
        db = self._get_session(session)

        # Check for existing entry
        existing = db.query(SchemaRegistryEntry).filter(
            SchemaRegistryEntry.project_id == project_id,
            SchemaRegistryEntry.database_name == database_name,
            SchemaRegistryEntry.schema_name == schema_name,
            SchemaRegistryEntry.object_name == object_name,
        ).first()

        # Calculate schema hash for drift detection
        schema_hash = self._calculate_schema_hash(column_definitions)

        if existing:
            # Update existing entry
            existing.object_type = object_type
            existing.column_definitions = column_definitions
            existing.primary_key_columns = primary_key_columns or []
            existing.foreign_keys = foreign_keys or []
            existing.indexes = indexes or []
            existing.validation_rules = validation_rules or []
            existing.schema_hash = schema_hash
            existing.updated_at = datetime.utcnow()
            db.commit()
            return existing

        # Create new entry
        entry = SchemaRegistryEntry(
            project_id=project_id,
            object_type=object_type,
            database_name=database_name,
            schema_name=schema_name,
            object_name=object_name,
            column_definitions=column_definitions,
            primary_key_columns=primary_key_columns or [],
            foreign_keys=foreign_keys or [],
            indexes=indexes or [],
            validation_rules=validation_rules or [],
            schema_hash=schema_hash,
        )
        db.add(entry)
        db.commit()
        db.refresh(entry)
        return entry

    def get_schema(
        self,
        project_id: str,
        object_name: str,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> Optional[SchemaRegistryEntry]:
        """
        Get a registered schema by name.

        Args:
            project_id: Project ID
            object_name: Name of the object
            database_name: Optional database name
            schema_name: Optional schema name
            session: Optional database session

        Returns:
            SchemaRegistryEntry or None if not found
        """
        db = self._get_session(session)

        return db.query(SchemaRegistryEntry).filter(
            SchemaRegistryEntry.project_id == project_id,
            SchemaRegistryEntry.database_name == database_name,
            SchemaRegistryEntry.schema_name == schema_name,
            SchemaRegistryEntry.object_name == object_name,
            SchemaRegistryEntry.is_active == True,
        ).first()

    def list_schemas(
        self,
        project_id: str,
        object_type: Optional[ObjectType] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> List[SchemaRegistryEntry]:
        """
        List registered schemas with optional filters.

        Args:
            project_id: Project ID
            object_type: Optional filter by object type
            database_name: Optional filter by database
            schema_name: Optional filter by schema
            session: Optional database session

        Returns:
            List of matching SchemaRegistryEntry objects
        """
        db = self._get_session(session)

        query = db.query(SchemaRegistryEntry).filter(
            SchemaRegistryEntry.project_id == project_id,
            SchemaRegistryEntry.is_active == True,
        )

        if object_type:
            query = query.filter(SchemaRegistryEntry.object_type == object_type)
        if database_name:
            query = query.filter(SchemaRegistryEntry.database_name == database_name)
        if schema_name:
            query = query.filter(SchemaRegistryEntry.schema_name == schema_name)

        return query.all()

    def validate_schema_compatibility(
        self,
        source_schema: SchemaRegistryEntry,
        target_schema: SchemaRegistryEntry,
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Validate compatibility between source and target schemas.

        Checks:
        - All source columns exist in target
        - Data types are compatible
        - Nullability constraints are compatible

        Args:
            source_schema: Source schema entry
            target_schema: Target schema entry

        Returns:
            Tuple of (is_compatible, list of issues)
        """
        issues = []

        source_cols = {
            c["name"].lower(): c for c in (source_schema.column_definitions or [])
        }
        target_cols = {
            c["name"].lower(): c for c in (target_schema.column_definitions or [])
        }

        # Check for missing columns
        for col_name, col_def in source_cols.items():
            if col_name not in target_cols:
                issues.append({
                    "type": "missing_column",
                    "severity": "error",
                    "column": col_def["name"],
                    "message": f"Column '{col_def['name']}' exists in source but not in target",
                })
                continue

            target_col = target_cols[col_name]

            # Check data type compatibility
            source_type = self._normalize_type(col_def.get("data_type", ""))
            target_type = self._normalize_type(target_col.get("data_type", ""))

            if not self._types_compatible(source_type, target_type):
                issues.append({
                    "type": "type_mismatch",
                    "severity": "warning",
                    "column": col_def["name"],
                    "source_type": col_def.get("data_type"),
                    "target_type": target_col.get("data_type"),
                    "message": f"Column '{col_def['name']}' type mismatch: {col_def.get('data_type')} vs {target_col.get('data_type')}",
                })

            # Check nullability
            source_nullable = col_def.get("nullable", True)
            target_nullable = target_col.get("nullable", True)

            if source_nullable and not target_nullable:
                issues.append({
                    "type": "nullability_conflict",
                    "severity": "warning",
                    "column": col_def["name"],
                    "message": f"Column '{col_def['name']}' is nullable in source but not in target",
                })

        # Check for extra columns in target (informational)
        for col_name in target_cols:
            if col_name not in source_cols:
                issues.append({
                    "type": "extra_column",
                    "severity": "info",
                    "column": target_cols[col_name]["name"],
                    "message": f"Column '{target_cols[col_name]['name']}' exists in target but not in source",
                })

        is_compatible = not any(i["severity"] == "error" for i in issues)
        return is_compatible, issues

    def detect_schema_drift(
        self,
        project_id: str,
        object_name: str,
        current_columns: List[Dict[str, Any]],
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Detect if schema has drifted from registered version.

        Args:
            project_id: Project ID
            object_name: Name of the object
            current_columns: Current column definitions
            database_name: Optional database name
            schema_name: Optional schema name
            session: Optional database session

        Returns:
            Tuple of (has_drifted, drift_details)
        """
        registered = self.get_schema(
            project_id, object_name, database_name, schema_name, session
        )

        if not registered:
            return True, {"reason": "Not registered", "registered_hash": None}

        current_hash = self._calculate_schema_hash(current_columns)

        if current_hash != registered.schema_hash:
            # Analyze what changed
            registered_cols = {
                c["name"].lower(): c for c in (registered.column_definitions or [])
            }
            current_cols = {
                c["name"].lower(): c for c in current_columns
            }

            added = [c for c in current_cols if c not in registered_cols]
            removed = [c for c in registered_cols if c not in current_cols]
            modified = []

            for col_name in set(registered_cols.keys()) & set(current_cols.keys()):
                if registered_cols[col_name] != current_cols[col_name]:
                    modified.append({
                        "column": col_name,
                        "old": registered_cols[col_name],
                        "new": current_cols[col_name],
                    })

            return True, {
                "reason": "Schema changed",
                "registered_hash": registered.schema_hash,
                "current_hash": current_hash,
                "columns_added": added,
                "columns_removed": removed,
                "columns_modified": modified,
            }

        return False, None

    def _calculate_schema_hash(self, column_definitions: List[Dict[str, Any]]) -> str:
        """
        Calculate a hash of column definitions for drift detection.

        Args:
            column_definitions: List of column definitions

        Returns:
            SHA256 hash string
        """
        # Sort columns by name for consistent hashing
        sorted_cols = sorted(
            column_definitions,
            key=lambda c: c.get("name", "").lower()
        )
        # Normalize column definitions
        normalized = [
            {
                "name": c.get("name", "").lower(),
                "data_type": self._normalize_type(c.get("data_type", "")),
                "nullable": c.get("nullable", True),
            }
            for c in sorted_cols
        ]
        json_str = json.dumps(normalized, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    def _normalize_type(self, data_type: str) -> str:
        """
        Normalize a data type for comparison.

        Args:
            data_type: SQL data type string

        Returns:
            Normalized type string (lowercase, no size spec)
        """
        # Remove size specifications
        base_type = data_type.lower().split("(")[0].strip()
        # Map common aliases
        type_map = {
            "int": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "varchar": "varchar",
            "nvarchar": "varchar",
            "char": "char",
            "nchar": "char",
            "text": "text",
            "number": "numeric",
            "decimal": "numeric",
            "float": "float",
            "real": "float",
            "double": "float",
            "bool": "boolean",
            "boolean": "boolean",
            "date": "date",
            "datetime": "timestamp",
            "datetime2": "timestamp",
            "timestamp": "timestamp",
            "timestamp_ntz": "timestamp",
            "timestamp_ltz": "timestamp",
            "timestamp_tz": "timestamp",
        }
        return type_map.get(base_type, base_type)

    def _types_compatible(self, source_type: str, target_type: str) -> bool:
        """
        Check if two normalized types are compatible.

        Args:
            source_type: Normalized source type
            target_type: Normalized target type

        Returns:
            True if types are compatible
        """
        if source_type == target_type:
            return True

        # Define type compatibility groups
        compatible_groups = [
            {"integer", "bigint", "smallint", "numeric"},
            {"float", "numeric", "double"},
            {"varchar", "char", "text", "string"},
            {"date", "timestamp"},
        ]

        for group in compatible_groups:
            if source_type in group and target_type in group:
                return True

        return False


# Singleton instance
_schema_registry = None


def get_schema_registry(session: Optional[Session] = None) -> SchemaRegistryService:
    """Get or create the schema registry singleton."""
    global _schema_registry
    if _schema_registry is None:
        _schema_registry = SchemaRegistryService(session)
    return _schema_registry
