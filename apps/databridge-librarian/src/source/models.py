"""
Data models for source intelligence and canonical model representation.

These models represent the inferred structure of source data systems,
including entities, relationships, and column mappings.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import uuid


class EntityType(str, Enum):
    """Standard business entity types for classification."""

    # People/Organizations
    EMPLOYEE = "employee"
    CUSTOMER = "customer"
    VENDOR = "vendor"
    BUSINESS_ASSOCIATE = "business_associate"  # Generic person/org

    # Organizational
    COMPANY = "company"
    DEPARTMENT = "department"
    COST_CENTER = "cost_center"
    LOCATION = "location"

    # Assets
    PRODUCT = "product"
    INVENTORY = "inventory"
    ASSET = "asset"
    EQUIPMENT = "equipment"

    # Financial
    CHART_OF_ACCOUNTS = "chart_of_accounts"
    ACCOUNT = "account"
    TRANSACTION = "transaction"

    # Time
    DATE = "date"
    PERIOD = "period"

    # Other
    UNKNOWN = "unknown"
    CUSTOM = "custom"


class RelationshipType(str, Enum):
    """Types of relationships between entities."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class SCDType(str, Enum):
    """Slowly Changing Dimension types."""

    TYPE_0 = "scd_0"  # No history
    TYPE_1 = "scd_1"  # Overwrite
    TYPE_2 = "scd_2"  # Full history with start/end dates
    TYPE_3 = "scd_3"  # Limited history (current + previous)


class ColumnRole(str, Enum):
    """Role of a column in analysis."""

    KEY = "key"  # Primary/foreign key
    MEASURE = "measure"  # Numeric value for aggregation
    DIMENSION = "dimension"  # Categorical/grouping
    ATTRIBUTE = "attribute"  # Descriptive
    DATE = "date"  # Date/time
    FLAG = "flag"  # Boolean indicator
    UNKNOWN = "unknown"


@dataclass
class SourceColumn:
    """
    Represents a column from a source table.

    Includes inferred metadata and user-provided overrides.
    """

    name: str
    data_type: str
    source_table: str
    source_schema: str = ""
    source_database: str = ""

    # Inferred properties
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: Optional[str] = None  # "table.column"

    # Analysis results
    role: ColumnRole = ColumnRole.UNKNOWN
    entity_type: Optional[EntityType] = None
    confidence: float = 0.0  # 0.0 to 1.0

    # Statistics (for profiling)
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    sample_values: List[str] = field(default_factory=list)

    # User overrides
    canonical_name: Optional[str] = None  # Mapped name in canonical model
    user_role: Optional[ColumnRole] = None  # User-specified role
    user_entity_type: Optional[EntityType] = None  # User-specified type
    approved: bool = False  # User has approved this mapping

    def __post_init__(self):
        if not hasattr(self, "id") or not self.id:
            self.id = str(uuid.uuid4())[:8]

    @property
    def full_path(self) -> str:
        """Get fully qualified column path."""
        parts = [self.source_database, self.source_schema, self.source_table, self.name]
        return ".".join(p for p in parts if p)

    @property
    def effective_role(self) -> ColumnRole:
        """Get the effective role (user override or inferred)."""
        return self.user_role or self.role

    @property
    def effective_entity_type(self) -> Optional[EntityType]:
        """Get the effective entity type (user override or inferred)."""
        return self.user_entity_type or self.entity_type

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "source_table": self.source_table,
            "source_schema": self.source_schema,
            "source_database": self.source_database,
            "full_path": self.full_path,
            "nullable": self.nullable,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "foreign_key_reference": self.foreign_key_reference,
            "role": self.effective_role.value,
            "entity_type": self.effective_entity_type.value if self.effective_entity_type else None,
            "confidence": self.confidence,
            "canonical_name": self.canonical_name,
            "approved": self.approved,
        }


@dataclass
class SourceTable:
    """
    Represents a source table with its columns.
    """

    name: str
    schema: str = ""
    database: str = ""
    table_type: str = "TABLE"  # TABLE, VIEW, etc.

    # Columns
    columns: List[SourceColumn] = field(default_factory=list)

    # Analysis results
    entity_type: Optional[EntityType] = None
    scd_type: Optional[SCDType] = None
    confidence: float = 0.0

    # Statistics
    row_count: Optional[int] = None

    # User overrides
    canonical_name: Optional[str] = None
    user_entity_type: Optional[EntityType] = None
    approved: bool = False

    def __post_init__(self):
        if not hasattr(self, "id") or not self.id:
            self.id = str(uuid.uuid4())[:8]

    @property
    def full_path(self) -> str:
        """Get fully qualified table path."""
        parts = [self.database, self.schema, self.name]
        return ".".join(p for p in parts if p)

    @property
    def effective_entity_type(self) -> Optional[EntityType]:
        """Get the effective entity type."""
        return self.user_entity_type or self.entity_type

    @property
    def primary_key_columns(self) -> List[SourceColumn]:
        """Get primary key columns."""
        return [c for c in self.columns if c.is_primary_key]

    def get_column(self, name: str) -> Optional[SourceColumn]:
        """Get a column by name (case-insensitive)."""
        name_upper = name.upper()
        for col in self.columns:
            if col.name.upper() == name_upper:
                return col
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "schema": self.schema,
            "database": self.database,
            "full_path": self.full_path,
            "table_type": self.table_type,
            "entity_type": self.effective_entity_type.value if self.effective_entity_type else None,
            "scd_type": self.scd_type.value if self.scd_type else None,
            "confidence": self.confidence,
            "row_count": self.row_count,
            "column_count": len(self.columns),
            "canonical_name": self.canonical_name,
            "approved": self.approved,
        }


@dataclass
class SourceEntity:
    """
    Represents a business entity identified across source tables.

    An entity may span multiple source tables (e.g., Customer info in
    both a customer table and an orders table).
    """

    name: str
    entity_type: EntityType
    description: str = ""

    # Source tables that contain this entity
    source_tables: List[str] = field(default_factory=list)  # Full paths

    # Key columns for this entity across sources
    key_columns: List[str] = field(default_factory=list)  # Full column paths

    # Attribute columns
    attribute_columns: List[str] = field(default_factory=list)

    # Analysis metadata
    confidence: float = 0.0
    inferred_by: str = ""  # "heuristic", "llm", "user"

    # User feedback
    approved: bool = False
    rejected: bool = False
    user_notes: str = ""

    def __post_init__(self):
        if not hasattr(self, "id") or not self.id:
            self.id = str(uuid.uuid4())[:8]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "entity_type": self.entity_type.value,
            "description": self.description,
            "source_tables": self.source_tables,
            "key_columns": self.key_columns,
            "confidence": self.confidence,
            "approved": self.approved,
            "rejected": self.rejected,
        }


@dataclass
class SourceRelationship:
    """
    Represents a relationship between two entities/tables.
    """

    name: str
    source_entity: str  # Entity name or table path
    target_entity: str  # Entity name or table path
    relationship_type: RelationshipType

    # Join columns
    source_columns: List[str] = field(default_factory=list)  # Column names
    target_columns: List[str] = field(default_factory=list)  # Column names

    # Analysis metadata
    confidence: float = 0.0
    inferred_by: str = ""  # "foreign_key", "name_match", "value_match", "user"

    # User feedback
    approved: bool = False
    rejected: bool = False

    def __post_init__(self):
        if not hasattr(self, "id") or not self.id:
            self.id = str(uuid.uuid4())[:8]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "source_entity": self.source_entity,
            "target_entity": self.target_entity,
            "relationship_type": self.relationship_type.value,
            "source_columns": self.source_columns,
            "target_columns": self.target_columns,
            "confidence": self.confidence,
            "inferred_by": self.inferred_by,
            "approved": self.approved,
            "rejected": self.rejected,
        }


@dataclass
class ColumnMerge:
    """
    Represents a merge of multiple source columns into one canonical column.

    Example: CUST_ID from table A + CustomerNumber from table B -> customer_id
    """

    canonical_name: str
    source_columns: List[str] = field(default_factory=list)  # Full column paths
    data_type: str = ""
    description: str = ""

    def __post_init__(self):
        if not hasattr(self, "id") or not self.id:
            self.id = str(uuid.uuid4())[:8]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "canonical_name": self.canonical_name,
            "source_columns": self.source_columns,
            "data_type": self.data_type,
            "description": self.description,
        }


@dataclass
class CanonicalModel:
    """
    The complete canonical data model inferred from source systems.

    This is the intermediate representation between raw sources and
    the final data warehouse design.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""

    # Source connection info
    connection_id: Optional[str] = None
    connection_name: str = ""

    # Model contents
    tables: List[SourceTable] = field(default_factory=list)
    entities: List[SourceEntity] = field(default_factory=list)
    relationships: List[SourceRelationship] = field(default_factory=list)
    column_merges: List[ColumnMerge] = field(default_factory=list)

    # Analysis status
    status: str = "draft"  # draft, reviewed, approved
    analyzed_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    approved_by: str = ""

    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def get_table(self, path: str) -> Optional[SourceTable]:
        """Get a table by its full path or name."""
        path_upper = path.upper()
        for table in self.tables:
            if table.full_path.upper() == path_upper or table.name.upper() == path_upper:
                return table
        return None

    def get_entity(self, name: str) -> Optional[SourceEntity]:
        """Get an entity by name."""
        name_upper = name.upper()
        for entity in self.entities:
            if entity.name.upper() == name_upper:
                return entity
        return None

    def get_relationship(self, id_or_name: str) -> Optional[SourceRelationship]:
        """Get a relationship by ID or name."""
        for rel in self.relationships:
            if rel.id == id_or_name or rel.name == id_or_name:
                return rel
        return None

    def add_relationship(
        self,
        source: str,
        target: str,
        source_columns: List[str],
        target_columns: List[str],
        relationship_type: RelationshipType = RelationshipType.MANY_TO_ONE,
        confidence: float = 1.0,
        inferred_by: str = "user",
    ) -> SourceRelationship:
        """Add a new relationship to the model."""
        rel = SourceRelationship(
            name=f"{source}_to_{target}",
            source_entity=source,
            target_entity=target,
            relationship_type=relationship_type,
            source_columns=source_columns,
            target_columns=target_columns,
            confidence=confidence,
            inferred_by=inferred_by,
            approved=inferred_by == "user",
        )
        self.relationships.append(rel)
        self.updated_at = datetime.now(timezone.utc)
        return rel

    def add_column_merge(
        self,
        canonical_name: str,
        source_columns: List[str],
        data_type: str = "",
        description: str = "",
    ) -> ColumnMerge:
        """Add a new column merge definition."""
        merge = ColumnMerge(
            canonical_name=canonical_name,
            source_columns=source_columns,
            data_type=data_type,
            description=description,
        )
        self.column_merges.append(merge)
        self.updated_at = datetime.now(timezone.utc)
        return merge

    def rename_entity(self, old_name: str, new_name: str) -> bool:
        """Rename an entity."""
        entity = self.get_entity(old_name)
        if not entity:
            return False

        entity.name = new_name
        self.updated_at = datetime.now(timezone.utc)
        return True

    def approve_entity(self, name: str) -> bool:
        """Mark an entity as approved."""
        entity = self.get_entity(name)
        if not entity:
            return False

        entity.approved = True
        entity.rejected = False
        self.updated_at = datetime.now(timezone.utc)
        return True

    def reject_entity(self, name: str, reason: str = "") -> bool:
        """Mark an entity as rejected."""
        entity = self.get_entity(name)
        if not entity:
            return False

        entity.rejected = True
        entity.approved = False
        entity.user_notes = reason
        self.updated_at = datetime.now(timezone.utc)
        return True

    def approve_relationship(self, id_or_name: str) -> bool:
        """Mark a relationship as approved."""
        rel = self.get_relationship(id_or_name)
        if not rel:
            return False

        rel.approved = True
        rel.rejected = False
        self.updated_at = datetime.now(timezone.utc)
        return True

    def reject_relationship(self, id_or_name: str) -> bool:
        """Mark a relationship as rejected."""
        rel = self.get_relationship(id_or_name)
        if not rel:
            return False

        rel.rejected = True
        rel.approved = False
        self.updated_at = datetime.now(timezone.utc)
        return True

    @property
    def approval_progress(self) -> Dict[str, Any]:
        """Get approval progress statistics."""
        total_entities = len(self.entities)
        approved_entities = sum(1 for e in self.entities if e.approved)
        rejected_entities = sum(1 for e in self.entities if e.rejected)

        total_relationships = len(self.relationships)
        approved_relationships = sum(1 for r in self.relationships if r.approved)
        rejected_relationships = sum(1 for r in self.relationships if r.rejected)

        return {
            "entities": {
                "total": total_entities,
                "approved": approved_entities,
                "rejected": rejected_entities,
                "pending": total_entities - approved_entities - rejected_entities,
            },
            "relationships": {
                "total": total_relationships,
                "approved": approved_relationships,
                "rejected": rejected_relationships,
                "pending": total_relationships - approved_relationships - rejected_relationships,
            },
            "overall_progress": (
                (approved_entities + approved_relationships)
                / max(total_entities + total_relationships, 1)
            ),
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "connection_id": self.connection_id,
            "connection_name": self.connection_name,
            "status": self.status,
            "tables": [t.to_dict() for t in self.tables],
            "entities": [e.to_dict() for e in self.entities],
            "relationships": [r.to_dict() for r in self.relationships],
            "column_merges": [m.to_dict() for m in self.column_merges],
            "approval_progress": self.approval_progress,
            "analyzed_at": self.analyzed_at.isoformat() if self.analyzed_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
