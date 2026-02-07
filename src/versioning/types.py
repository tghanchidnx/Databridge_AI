"""
Data Versioning Types - Pydantic models for version control.

Phase 30: Unified version control system for DataBridge objects.
"""

from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid


class VersionedObjectType(str, Enum):
    """Types of objects that can be versioned."""
    HIERARCHY_PROJECT = "hierarchy_project"
    HIERARCHY = "hierarchy"
    CATALOG_ASSET = "catalog_asset"
    GLOSSARY_TERM = "glossary_term"
    SEMANTIC_MODEL = "semantic_model"
    DATA_CONTRACT = "data_contract"
    EXPECTATION_SUITE = "expectation_suite"
    FORMULA_GROUP = "formula_group"
    SOURCE_MAPPING = "source_mapping"


class ChangeType(str, Enum):
    """Type of change made."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"


class VersionBump(str, Enum):
    """Version increment type."""
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


def _utc_now() -> datetime:
    """Get current UTC time."""
    from datetime import timezone
    return datetime.now(timezone.utc)


class Version(BaseModel):
    """A single version record."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    object_type: VersionedObjectType
    object_id: str
    version: str  # Semantic version "1.0.0"
    version_number: int  # Sequential number for easy comparison

    # Change metadata
    change_type: ChangeType
    change_description: Optional[str] = None
    changed_by: Optional[str] = None
    changed_at: datetime = Field(default_factory=_utc_now)

    # Snapshot - full object state at this version
    snapshot: Dict[str, Any]

    # Diff from previous (optional, computed on demand)
    changes: Optional[Dict[str, Any]] = None

    # Tags for filtering
    tags: List[str] = Field(default_factory=list)
    is_major: bool = False  # Flag for significant versions


class VersionHistory(BaseModel):
    """Version history for an object."""
    object_type: VersionedObjectType
    object_id: str
    object_name: Optional[str] = None  # Human-readable name
    current_version: str = "0.0.0"
    current_version_number: int = 0
    versions: List[Version] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


class VersionDiff(BaseModel):
    """Difference between two versions."""
    object_type: VersionedObjectType
    object_id: str
    from_version: str
    to_version: str

    # Changes
    added: Dict[str, Any] = Field(default_factory=dict)
    removed: Dict[str, Any] = Field(default_factory=dict)
    modified: Dict[str, Dict[str, Any]] = Field(default_factory=dict)  # {field: {old, new}}

    # Summary
    total_changes: int = 0
    change_summary: str = ""


class VersionQuery(BaseModel):
    """Query parameters for version search."""
    object_type: Optional[VersionedObjectType] = None
    object_id: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    changed_by: Optional[str] = None
    change_type: Optional[ChangeType] = None
    tag: Optional[str] = None
    is_major: Optional[bool] = None
    limit: int = 50
    offset: int = 0


class VersionStats(BaseModel):
    """Statistics about versioned objects."""
    total_objects: int = 0
    total_versions: int = 0
    objects_by_type: Dict[str, int] = Field(default_factory=dict)
    versions_by_type: Dict[str, int] = Field(default_factory=dict)
    recent_changes: int = 0  # Last 24 hours
    top_changers: List[Dict[str, Any]] = Field(default_factory=list)


class RollbackPreview(BaseModel):
    """Preview of what a rollback would restore."""
    object_type: VersionedObjectType
    object_id: str
    current_version: str
    target_version: str
    snapshot: Dict[str, Any]
    diff: Optional[VersionDiff] = None
    warning: Optional[str] = None


class VersionTag(BaseModel):
    """A tag attached to a version."""
    tag: str
    added_at: datetime = Field(default_factory=datetime.utcnow)
    added_by: Optional[str] = None


class VersionCreateRequest(BaseModel):
    """Request to create a new version."""
    object_type: VersionedObjectType
    object_id: str
    object_name: Optional[str] = None
    snapshot: Dict[str, Any]
    change_type: ChangeType = ChangeType.UPDATE
    change_description: Optional[str] = None
    changed_by: Optional[str] = None
    version_bump: VersionBump = VersionBump.PATCH
    tags: List[str] = Field(default_factory=list)


class VersionResult(BaseModel):
    """Result of a version operation."""
    success: bool
    message: str
    version: Optional[Version] = None
    history: Optional[VersionHistory] = None
    diff: Optional[VersionDiff] = None
