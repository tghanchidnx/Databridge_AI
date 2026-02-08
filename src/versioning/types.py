"""
Data Versioning Types - Phase 30
Unified version control for DataBridge objects.
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


class ChangeType(str, Enum):
    """Type of change made."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"


class VersionBump(str, Enum):
    """Version bump type for semantic versioning."""
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


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
    changed_at: datetime = Field(default_factory=datetime.utcnow)

    # Snapshot
    snapshot: Dict[str, Any]  # Full object state at this version

    # Diff from previous (optional, computed on demand)
    changes: Optional[Dict[str, Any]] = None

    # Tags for filtering
    tags: List[str] = Field(default_factory=list)
    is_major: bool = False  # Flag for significant versions

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class VersionHistory(BaseModel):
    """Version history for an object."""
    object_type: VersionedObjectType
    object_id: str
    current_version: str
    current_version_number: int
    versions: List[Version] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class VersionDiff(BaseModel):
    """Difference between two versions."""
    object_type: VersionedObjectType
    object_id: str
    from_version: str
    to_version: str
    added: Dict[str, Any] = Field(default_factory=dict)
    removed: Dict[str, Any] = Field(default_factory=dict)
    modified: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
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
    limit: int = 50
