"""
Data Versioning Types - Pydantic models for version control.

Provides unified version control for DataBridge objects:
- Hierarchy Projects & Hierarchies
- Data Catalog Assets
- Semantic Models
- Data Contracts & Expectation Suites
"""

from enum import Enum
from pydantic import BaseModel, Field, ConfigDict
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
    MART_CONFIG = "mart_config"
    LINEAGE_GRAPH = "lineage_graph"


class ChangeType(str, Enum):
    """Type of change made."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"


class VersionBump(str, Enum):
    """Version bump type."""
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

    # Snapshot - full object state at this version
    snapshot: Dict[str, Any]

    # Diff from previous (optional, computed on demand)
    changes: Optional[Dict[str, Any]] = None

    # Tags for filtering
    tags: List[str] = Field(default_factory=list)
    is_major: bool = False  # Flag for significant versions

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class VersionHistory(BaseModel):
    """Version history for an object."""
    object_type: VersionedObjectType
    object_id: str
    object_name: Optional[str] = None  # Human-readable name
    current_version: str
    current_version_number: int
    versions: List[Version] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(ser_json_timedelta="iso8601")


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

    # Detailed path-based changes for nested objects
    path_changes: List[Dict[str, Any]] = Field(default_factory=list)


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
    offset: int = 0


class VersionStats(BaseModel):
    """Statistics about versioning."""
    total_objects: int = 0
    total_versions: int = 0
    objects_by_type: Dict[str, int] = Field(default_factory=dict)
    versions_by_type: Dict[str, int] = Field(default_factory=dict)
    recent_changes: int = 0  # Last 24 hours
    top_changers: List[Dict[str, Any]] = Field(default_factory=list)


class RollbackResult(BaseModel):
    """Result of a rollback operation."""
    success: bool
    object_type: VersionedObjectType
    object_id: str
    from_version: str
    to_version: str
    new_version: str  # The version created by the rollback
    snapshot: Dict[str, Any]  # The restored data
    message: str


class RollbackPreview(BaseModel):
    """Preview of a rollback operation."""
    current_version: str
    target_version: str
    snapshot: Dict[str, Any]
    diff: Optional[Any] = None  # VersionDiff or None


class VersionTag(BaseModel):
    """A tag applied to a version."""
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None


# Common version bump types
VERSION_BUMP_MAJOR = "major"
VERSION_BUMP_MINOR = "minor"
VERSION_BUMP_PATCH = "patch"


def parse_version(version_str: str) -> tuple:
    """Parse a semantic version string into components."""
    try:
        parts = version_str.split(".")
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        return (0, 0, 0)


def compare_versions(v1: str, v2: str) -> int:
    """
    Compare two version strings.
    Returns: -1 if v1 < v2, 0 if equal, 1 if v1 > v2
    """
    p1 = parse_version(v1)
    p2 = parse_version(v2)

    if p1 < p2:
        return -1
    elif p1 > p2:
        return 1
    return 0


def format_version(major: int, minor: int, patch: int) -> str:
    """Format version components into a string."""
    return f"{major}.{minor}.{patch}"
