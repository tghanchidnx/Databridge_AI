"""
SQLAlchemy database models and session management for DataBridge AI V3.

This module defines all ORM models and provides database connection management.
Uses shared Base from databridge-models when available.
"""

from datetime import datetime
from typing import Optional, List, Any
from contextlib import contextmanager
import uuid

from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    Text,
    ForeignKey,
    JSON,
    Index,
    event,
)
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker,
    Session,
)
from sqlalchemy.engine import Engine

from .config import get_settings

# Try to import Base from shared library, fallback to local definition
try:
    from databridge_models import Base
except ImportError:
    # Fallback for standalone usage
    Base = declarative_base()


def generate_uuid() -> str:
    """Generate a new UUID string."""
    return str(uuid.uuid4())


# =============================================================================
# PROJECT MODEL
# =============================================================================


class Project(Base):
    """
    Hierarchy project container.

    A project groups related hierarchies together (e.g., "FY2024 P&L").
    """

    __tablename__ = "projects"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)
    industry = Column(String(100))
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    hierarchies = relationship(
        "Hierarchy",
        back_populates="project",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    formula_groups = relationship(
        "FormulaGroup",
        back_populates="project",
        cascade="all, delete-orphan",
    )
    deployments = relationship(
        "DeploymentHistory",
        back_populates="project",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Project(id={self.id}, name='{self.name}')>"


# =============================================================================
# HIERARCHY MODEL
# =============================================================================


class Hierarchy(Base):
    """
    Financial hierarchy node.

    Represents a node in a hierarchical structure (e.g., "Revenue > Product Sales > Hardware").
    Supports up to 15 levels of depth with SCD Type 2 versioning.
    """

    __tablename__ = "hierarchies"

    # Primary keys
    id = Column(Integer, primary_key=True, autoincrement=True)
    hierarchy_id = Column(String(255), unique=True, nullable=False, index=True)

    # Foreign keys
    project_id = Column(String(36), ForeignKey("projects.id"), nullable=False, index=True)

    # Basic attributes
    hierarchy_name = Column(String(255), nullable=False)
    description = Column(Text)
    parent_id = Column(String(255), index=True)  # References hierarchy_id of parent

    # Level columns (up to 15 levels)
    level_1 = Column(String(255))
    level_2 = Column(String(255))
    level_3 = Column(String(255))
    level_4 = Column(String(255))
    level_5 = Column(String(255))
    level_6 = Column(String(255))
    level_7 = Column(String(255))
    level_8 = Column(String(255))
    level_9 = Column(String(255))
    level_10 = Column(String(255))
    level_11 = Column(String(255))
    level_12 = Column(String(255))
    level_13 = Column(String(255))
    level_14 = Column(String(255))
    level_15 = Column(String(255))

    # Sort order per level
    level_1_sort = Column(Integer, default=1)
    level_2_sort = Column(Integer, default=1)
    level_3_sort = Column(Integer, default=1)
    level_4_sort = Column(Integer, default=1)
    level_5_sort = Column(Integer, default=1)
    level_6_sort = Column(Integer, default=1)
    level_7_sort = Column(Integer, default=1)
    level_8_sort = Column(Integer, default=1)
    level_9_sort = Column(Integer, default=1)
    level_10_sort = Column(Integer, default=1)
    level_11_sort = Column(Integer, default=1)
    level_12_sort = Column(Integer, default=1)
    level_13_sort = Column(Integer, default=1)
    level_14_sort = Column(Integer, default=1)
    level_15_sort = Column(Integer, default=1)

    # Flags
    include_flag = Column(Boolean, default=True)
    exclude_flag = Column(Boolean, default=False)
    transform_flag = Column(Boolean, default=False)
    calculation_flag = Column(Boolean, default=False)
    active_flag = Column(Boolean, default=True)
    is_leaf_node = Column(Boolean, default=False)

    # JSON configuration fields
    source_mappings = Column(JSON, default=list)
    formula_config = Column(JSON)
    filter_config = Column(JSON)
    pivot_config = Column(JSON)
    metadata_config = Column(JSON)

    # SCD Type 2 columns for historical tracking
    effective_from = Column(DateTime, default=datetime.utcnow)
    effective_to = Column(DateTime, default=lambda: datetime(9999, 12, 31))
    is_current = Column(Boolean, default=True)
    version_number = Column(Integer, default=1)

    # Sort and ordering
    sort_order = Column(Integer, default=1)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    project = relationship("Project", back_populates="hierarchies")
    mappings = relationship(
        "SourceMapping",
        back_populates="hierarchy",
        cascade="all, delete-orphan",
    )

    # Indexes
    __table_args__ = (
        Index("ix_hierarchy_project_parent", "project_id", "parent_id"),
        Index("ix_hierarchy_current", "is_current", "effective_from"),
    )

    def __repr__(self) -> str:
        return f"<Hierarchy(id={self.hierarchy_id}, name='{self.hierarchy_name}')>"

    def get_level_path(self) -> List[str]:
        """Get the hierarchy path as a list of level values."""
        path = []
        for i in range(1, 16):
            level_val = getattr(self, f"level_{i}")
            if level_val:
                path.append(level_val)
            else:
                break
        return path

    def get_depth(self) -> int:
        """Get the depth of this node in the hierarchy."""
        return len(self.get_level_path())


# =============================================================================
# SOURCE MAPPING MODEL
# =============================================================================


class SourceMapping(Base):
    """
    Source-to-hierarchy mapping.

    Maps a database column to a hierarchy node for data extraction.
    """

    __tablename__ = "source_mappings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hierarchy_id = Column(
        String(255),
        ForeignKey("hierarchies.hierarchy_id"),
        nullable=False,
        index=True,
    )

    # Mapping position
    mapping_index = Column(Integer, default=0)

    # Source reference
    source_database = Column(String(255))
    source_schema = Column(String(255))
    source_table = Column(String(255))
    source_column = Column(String(255))
    source_uid = Column(String(255))  # Filter value (e.g., "HW%" for hardware)

    # Grouping
    precedence_group = Column(String(100), default="DEFAULT")

    # Flags
    include_flag = Column(Boolean, default=True)
    exclude_flag = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    hierarchy = relationship("Hierarchy", back_populates="mappings")

    def __repr__(self) -> str:
        return f"<SourceMapping(hierarchy={self.hierarchy_id}, source={self.source_table}.{self.source_column})>"

    @property
    def full_source_path(self) -> str:
        """Get the full qualified source path."""
        parts = [
            self.source_database or "",
            self.source_schema or "",
            self.source_table or "",
            self.source_column or "",
        ]
        return ".".join(p for p in parts if p)


# =============================================================================
# CONNECTION MODEL
# =============================================================================


class Connection(Base):
    """
    Database connection configuration.

    Stores encrypted credentials for connecting to data sources.
    """

    __tablename__ = "connections"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False, unique=True)
    connection_type = Column(String(50), nullable=False)  # snowflake, mysql, postgresql, sqlserver

    # Connection details
    host = Column(String(255))
    port = Column(Integer)
    database = Column(String(255))
    username = Column(String(255))
    password_encrypted = Column(Text)  # Fernet encrypted
    extra_config = Column(JSON)  # Additional config (warehouse, role, etc.)

    # Status
    is_active = Column(Boolean, default=True)
    last_tested = Column(DateTime)
    last_test_success = Column(Boolean)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    deployments = relationship("DeploymentHistory", back_populates="connection")

    def __repr__(self) -> str:
        return f"<Connection(id={self.id}, name='{self.name}', type='{self.connection_type}')>"


# =============================================================================
# FORMULA GROUP MODEL
# =============================================================================


class FormulaGroup(Base):
    """
    Group of formula rules for calculated hierarchies.

    Defines calculations like SUM, SUBTRACT, MULTIPLY, DIVIDE.
    """

    __tablename__ = "formula_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(255), nullable=False)
    description = Column(Text)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    project = relationship("Project", back_populates="formula_groups")
    rules = relationship(
        "FormulaRule",
        back_populates="group",
        cascade="all, delete-orphan",
        order_by="FormulaRule.rule_order",
    )

    def __repr__(self) -> str:
        return f"<FormulaGroup(id={self.id}, name='{self.name}')>"


# =============================================================================
# FORMULA RULE MODEL
# =============================================================================


class FormulaRule(Base):
    """
    Individual formula rule within a formula group.

    Defines a calculation rule like: TARGET = SUM(A, B, C)
    """

    __tablename__ = "formula_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey("formula_groups.id"), nullable=False, index=True)

    # Rule definition
    target_hierarchy_id = Column(String(255), nullable=False)  # The hierarchy to calculate
    source_hierarchy_ids = Column(Text, nullable=False)  # Comma-separated list of source hierarchy IDs
    operation = Column(String(50), nullable=False)  # SUM, SUBTRACT, MULTIPLY, DIVIDE, PERCENT, etc.
    rule_order = Column(Integer, default=0)  # Order of execution within the group

    # Optional configuration
    round_decimals = Column(Integer)  # Number of decimal places to round to
    null_handling = Column(String(20), default="zero")  # zero, skip, error

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    group = relationship("FormulaGroup", back_populates="rules")

    def __repr__(self) -> str:
        return f"<FormulaRule(id={self.id}, target='{self.target_hierarchy_id}', op='{self.operation}')>"

    def get_source_ids(self) -> List[str]:
        """Get the list of source hierarchy IDs."""
        if not self.source_hierarchy_ids:
            return []
        return [s.strip() for s in self.source_hierarchy_ids.split(",") if s.strip()]


# =============================================================================
# DEPLOYMENT HISTORY MODEL
# =============================================================================


class DeploymentHistory(Base):
    """
    Deployment history tracking.

    Records deployments of hierarchies to target systems.
    """

    __tablename__ = "deployment_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey("projects.id"), nullable=False, index=True)
    connection_id = Column(String(36), ForeignKey("connections.id"), index=True)

    # Deployment details
    script_type = Column(String(50))  # INSERT, VIEW, MAPPING
    script_content = Column(Text)
    target_database = Column(String(255))
    target_schema = Column(String(255))
    target_table = Column(String(255))

    # Status
    status = Column(String(20), default="pending")  # pending, success, failed
    error_message = Column(Text)
    rows_affected = Column(Integer)

    # Execution info
    executed_at = Column(DateTime)
    executed_by = Column(String(255))
    duration_ms = Column(Integer)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    project = relationship("Project", back_populates="deployments")
    connection = relationship("Connection", back_populates="deployments")

    def __repr__(self) -> str:
        return f"<DeploymentHistory(id={self.id}, status='{self.status}')>"


# =============================================================================
# CLIENT PROFILE MODEL
# =============================================================================


class ClientProfile(Base):
    """
    Client-specific knowledge base profile.

    Stores client configurations and custom prompts.
    """

    __tablename__ = "client_profiles"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False, unique=True)
    industry = Column(String(100))

    # Configuration
    custom_prompts = Column(JSON, default=list)
    mappings = Column(JSON, default=dict)
    preferences = Column(JSON, default=dict)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<ClientProfile(id={self.id}, name='{self.name}')>"


# =============================================================================
# AUDIT LOG MODEL
# =============================================================================


class AuditLog(Base):
    """
    Audit trail for compliance tracking.

    Records all significant actions without storing PII.
    """

    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    # Action details
    action = Column(String(100), nullable=False, index=True)
    entity_type = Column(String(50))  # project, hierarchy, mapping, etc.
    entity_id = Column(String(255))
    user_id = Column(String(255))

    # Context
    details = Column(JSON)
    source = Column(String(50))  # cli, mcp, api
    ip_address = Column(String(45))

    def __repr__(self) -> str:
        return f"<AuditLog(id={self.id}, action='{self.action}')>"


# =============================================================================
# ENGINE AND SESSION MANAGEMENT
# =============================================================================

# Module-level engine and session factory
_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


def get_engine() -> Engine:
    """
    Get or create the SQLAlchemy engine.

    Returns:
        Engine: SQLAlchemy database engine.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database_url,
            echo=settings.database.echo_sql,
            pool_pre_ping=True,
        )
        # Enable foreign key support for SQLite
        if "sqlite" in settings.database_url:

            @event.listens_for(_engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

    return _engine


def get_session() -> Session:
    """
    Get a new database session.

    Returns:
        Session: New SQLAlchemy session.
    """
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine(),
        )
    return _SessionLocal()


@contextmanager
def session_scope():
    """
    Context manager for database sessions.

    Provides automatic commit on success and rollback on error.

    Yields:
        Session: Database session.
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_database() -> None:
    """
    Initialize the database schema.

    Creates all tables if they don't exist.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)


def reset_database() -> None:
    """
    Reset the database (for testing only).

    Drops and recreates all tables.
    """
    engine = get_engine()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
