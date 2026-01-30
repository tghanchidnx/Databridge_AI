"""
SQLAlchemy models for SQL Generator module.

This module defines:
- GeneratedView: VW_1 tier auto-generated views
- SchemaRegistryEntry: Schema metadata for validation
- DataLineageEdge: NetworkX-backed lineage tracking
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import uuid

from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    DateTime,
    Text,
    ForeignKey,
    JSON,
    Index,
    Enum as SQLEnum,
)
from sqlalchemy.orm import relationship

# Import Base from core
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import Base, generate_uuid

# Try to import shared enums from databridge_models
try:
    from databridge_models.enums import (
        PatternType,
        TransformationType as BaseTransformationType,
    )
    # Use shared TransformationType
    TransformationType = BaseTransformationType
except ImportError:
    # Fallback: define locally if shared library not available
    class TransformationType(str, Enum):
        """Types of data transformations for lineage."""
        SELECT = "select"
        AGGREGATE = "aggregate"
        FILTER = "filter"
        JOIN = "join"
        UNION = "union"
        PIVOT = "pivot"
        FORMULA = "formula"
        CUSTOM = "custom"

    class PatternType(str, Enum):
        """Classification of table patterns."""
        FACT = "fact"
        DIMENSION = "dimension"
        BRIDGE = "bridge"
        AGGREGATE = "aggregate"
        STAGING = "staging"
        LOOKUP = "lookup"
        UNKNOWN = "unknown"


# =============================================================================
# V3-SPECIFIC ENUMS (not in shared library)
# =============================================================================


class ColumnType(str, Enum):
    """Classification of column types."""
    MEASURE = "measure"           # Numeric values to aggregate (amount, quantity)
    DIMENSION = "dimension"       # Categorical grouping columns
    PRIMARY_KEY = "primary_key"   # Table's primary key
    FOREIGN_KEY = "foreign_key"   # Reference to other tables
    DATE_KEY = "date_key"         # Date/time columns
    DEGENERATE = "degenerate"     # Transaction-level dimension
    METADATA = "metadata"         # Audit/system columns
    UNKNOWN = "unknown"


class ObjectType(str, Enum):
    """Types of database objects."""
    TABLE = "table"
    VIEW = "view"
    DYNAMIC_TABLE = "dynamic_table"
    MATERIALIZED_VIEW = "materialized_view"
    STREAM = "stream"


class SQLDialect(str, Enum):
    """Supported SQL dialects."""
    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    TSQL = "tsql"
    MYSQL = "mysql"


# =============================================================================
# GENERATED VIEW MODEL (VW_1 Tier)
# =============================================================================


class GeneratedView(Base):
    """
    VW_1 tier auto-generated views.

    Created from hierarchy definitions and source mappings.
    These views form the foundation for dynamic tables in V4.
    """

    __tablename__ = "generated_views"

    # Primary key
    id = Column(String(36), primary_key=True, default=generate_uuid)

    # Foreign key to project
    project_id = Column(String(36), ForeignKey("projects.id"), nullable=False, index=True)

    # View identification
    view_name = Column(String(255), nullable=False)
    display_name = Column(String(255))
    description = Column(Text)

    # Source reference
    source_database = Column(String(255))
    source_schema = Column(String(255))
    source_table = Column(String(255), nullable=False)

    # Pattern classification
    pattern_type = Column(SQLEnum(PatternType), default=PatternType.UNKNOWN)

    # Column metadata (JSON list of column definitions)
    # Format: [{"name": "col", "type": "measure", "data_type": "NUMBER", "source_column": "orig"}]
    detected_columns = Column(JSON, default=list)

    # Selected columns for the view
    select_columns = Column(JSON, default=list)

    # Join configuration (for multi-table views)
    # Format: [{"table": "dim_date", "alias": "d", "on": "f.date_key = d.date_key", "type": "LEFT"}]
    joins = Column(JSON, default=list)

    # Filter configuration
    # Format: [{"column": "is_active", "operator": "=", "value": "Y"}]
    filters = Column(JSON, default=list)

    # Generated SQL (stored for inspection)
    generated_sql = Column(Text)

    # SQL dialect used for generation
    dialect = Column(SQLEnum(SQLDialect), default=SQLDialect.SNOWFLAKE)

    # Target location (where to deploy)
    target_database = Column(String(255))
    target_schema = Column(String(255))

    # Linked hierarchy (optional - for hierarchy-based views)
    hierarchy_id = Column(String(255), index=True)

    # Status
    is_active = Column(Boolean, default=True)
    is_deployed = Column(Boolean, default=False)
    last_deployed_at = Column(DateTime)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    project = relationship("Project", backref="generated_views")
    lineage_sources = relationship(
        "DataLineageEdge",
        foreign_keys="DataLineageEdge.target_object_id",
        back_populates="target_view",
        cascade="all, delete-orphan",
    )
    lineage_targets = relationship(
        "DataLineageEdge",
        foreign_keys="DataLineageEdge.source_object_id",
        back_populates="source_view",
        cascade="all, delete-orphan",
    )

    # Indexes
    __table_args__ = (
        Index("ix_generated_view_project_name", "project_id", "view_name", unique=True),
        Index("ix_generated_view_source", "source_database", "source_schema", "source_table"),
    )

    def __repr__(self) -> str:
        return f"<GeneratedView(id={self.id}, name='{self.view_name}')>"

    @property
    def full_source_path(self) -> str:
        """Get fully qualified source table path."""
        parts = [self.source_database, self.source_schema, self.source_table]
        return ".".join(p for p in parts if p)

    @property
    def full_target_path(self) -> str:
        """Get fully qualified target view path."""
        parts = [self.target_database, self.target_schema, self.view_name]
        return ".".join(p for p in parts if p)

    def get_measure_columns(self) -> List[Dict[str, Any]]:
        """Get columns classified as measures."""
        return [c for c in (self.detected_columns or []) if c.get("type") == "measure"]

    def get_dimension_columns(self) -> List[Dict[str, Any]]:
        """Get columns classified as dimensions."""
        return [c for c in (self.detected_columns or []) if c.get("type") == "dimension"]


# =============================================================================
# SCHEMA REGISTRY MODEL
# =============================================================================


class SchemaRegistryEntry(Base):
    """
    Schema metadata registry for validation and discovery.

    Stores column definitions and validation rules for database objects.
    """

    __tablename__ = "schema_registry"

    # Primary key
    id = Column(String(36), primary_key=True, default=generate_uuid)

    # Foreign key to project
    project_id = Column(String(36), ForeignKey("projects.id"), nullable=False, index=True)

    # Object identification
    object_type = Column(SQLEnum(ObjectType), nullable=False)
    database_name = Column(String(255))
    schema_name = Column(String(255))
    object_name = Column(String(255), nullable=False)

    # Column definitions (JSON array)
    # Format: [{"name": "col", "data_type": "VARCHAR(255)", "nullable": true, "default": null}]
    column_definitions = Column(JSON, default=list)

    # Primary key columns
    primary_key_columns = Column(JSON, default=list)

    # Foreign key relationships
    # Format: [{"columns": ["dept_id"], "references_table": "departments", "references_columns": ["id"]}]
    foreign_keys = Column(JSON, default=list)

    # Indexes
    # Format: [{"name": "ix_name", "columns": ["col1", "col2"], "unique": false}]
    indexes = Column(JSON, default=list)

    # Validation rules (JSON array)
    # Format: [{"column": "amount", "rule": "not_null"}, {"column": "code", "rule": "unique"}]
    validation_rules = Column(JSON, default=list)

    # Statistics (optional)
    row_count = Column(Integer)
    size_bytes = Column(Integer)
    last_analyzed_at = Column(DateTime)

    # Schema fingerprint for drift detection
    schema_hash = Column(String(64))

    # Status
    is_active = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    project = relationship("Project", backref="schema_entries")

    # Indexes
    __table_args__ = (
        Index("ix_schema_registry_object", "project_id", "database_name", "schema_name", "object_name", unique=True),
    )

    def __repr__(self) -> str:
        return f"<SchemaRegistryEntry(id={self.id}, object='{self.full_object_path}')>"

    @property
    def full_object_path(self) -> str:
        """Get fully qualified object path."""
        parts = [self.database_name, self.schema_name, self.object_name]
        return ".".join(p for p in parts if p)

    def get_column_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get column definition by name."""
        for col in (self.column_definitions or []):
            if col.get("name", "").lower() == name.lower():
                return col
        return None

    def get_columns_by_type(self, data_type_pattern: str) -> List[Dict[str, Any]]:
        """Get columns matching a data type pattern."""
        import re
        pattern = re.compile(data_type_pattern, re.IGNORECASE)
        return [
            c for c in (self.column_definitions or [])
            if pattern.search(c.get("data_type", ""))
        ]


# =============================================================================
# DATA LINEAGE EDGE MODEL
# =============================================================================


class DataLineageEdge(Base):
    """
    Data lineage tracking for transformation graphs.

    Records relationships between source and target objects,
    enabling upstream/downstream traversal via NetworkX.
    """

    __tablename__ = "data_lineage_edges"

    # Primary key
    id = Column(String(36), primary_key=True, default=generate_uuid)

    # Foreign key to project
    project_id = Column(String(36), ForeignKey("projects.id"), nullable=False, index=True)

    # Source object (upstream)
    source_object_id = Column(String(36), ForeignKey("generated_views.id"), index=True)
    source_object_type = Column(SQLEnum(ObjectType))
    source_object_name = Column(String(500))  # Fully qualified name
    source_column = Column(String(255))  # Optional column-level lineage

    # Target object (downstream)
    target_object_id = Column(String(36), ForeignKey("generated_views.id"), index=True)
    target_object_type = Column(SQLEnum(ObjectType))
    target_object_name = Column(String(500))  # Fully qualified name
    target_column = Column(String(255))  # Optional column-level lineage

    # Transformation details
    transformation_type = Column(SQLEnum(TransformationType), nullable=False)
    transformation_logic = Column(Text)  # SQL expression or formula

    # Metadata
    description = Column(Text)
    confidence_score = Column(Integer)  # 0-100, for auto-detected lineage
    is_inferred = Column(Boolean, default=False)  # Auto-detected vs manual

    # Status
    is_active = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    project = relationship("Project", backref="lineage_edges")
    source_view = relationship(
        "GeneratedView",
        foreign_keys=[source_object_id],
        back_populates="lineage_targets",
    )
    target_view = relationship(
        "GeneratedView",
        foreign_keys=[target_object_id],
        back_populates="lineage_sources",
    )

    # Indexes
    __table_args__ = (
        Index("ix_lineage_source", "project_id", "source_object_name"),
        Index("ix_lineage_target", "project_id", "target_object_name"),
        Index("ix_lineage_edge", "source_object_id", "target_object_id"),
    )

    def __repr__(self) -> str:
        return f"<DataLineageEdge(id={self.id}, {self.source_object_name} -> {self.target_object_name})>"

    def to_edge_tuple(self) -> tuple:
        """Convert to NetworkX edge tuple (source, target, attributes)."""
        return (
            self.source_object_name,
            self.target_object_name,
            {
                "id": self.id,
                "transformation_type": self.transformation_type.value if self.transformation_type else None,
                "transformation_logic": self.transformation_logic,
                "source_column": self.source_column,
                "target_column": self.target_column,
            }
        )


# =============================================================================
# DETECTED PATTERN MODEL (Non-persisted, used for analysis)
# =============================================================================


class DetectedPattern:
    """
    Non-persisted model for pattern detection results.

    Used to return analysis results without database storage.
    """

    def __init__(
        self,
        table_name: str,
        pattern_type: PatternType,
        confidence: float,
        columns: List[Dict[str, Any]],
        reasoning: str,
    ):
        self.table_name = table_name
        self.pattern_type = pattern_type
        self.confidence = confidence  # 0.0 - 1.0
        self.columns = columns
        self.reasoning = reasoning

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "table_name": self.table_name,
            "pattern_type": self.pattern_type.value,
            "confidence": self.confidence,
            "columns": self.columns,
            "reasoning": self.reasoning,
        }


# =============================================================================
# COLUMN CLASSIFICATION MODEL (Non-persisted)
# =============================================================================


class ColumnClassification:
    """
    Non-persisted model for column classification results.
    """

    def __init__(
        self,
        column_name: str,
        column_type: ColumnType,
        data_type: str,
        confidence: float,
        sample_values: Optional[List[Any]] = None,
        statistics: Optional[Dict[str, Any]] = None,
    ):
        self.column_name = column_name
        self.column_type = column_type
        self.data_type = data_type
        self.confidence = confidence
        self.sample_values = sample_values or []
        self.statistics = statistics or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.column_name,
            "type": self.column_type.value,
            "data_type": self.data_type,
            "confidence": self.confidence,
            "sample_values": self.sample_values[:5],  # Limit to 5 samples
            "statistics": self.statistics,
        }
