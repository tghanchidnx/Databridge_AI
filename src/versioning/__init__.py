"""
Data Versioning Module - Phase 30

Unified version control system for DataBridge objects with change tracking,
snapshots, and rollback support.

Objects versioned:
- Hierarchy Projects & Hierarchies
- Data Catalog Assets
- Semantic Models
- Data Contracts & Expectation Suites
- Formula Groups & Source Mappings

Features:
- Semantic versioning (major.minor.patch)
- Change history with before/after snapshots
- Rollback to any previous version
- Diff between versions
- Change descriptions and metadata
- Tag-based filtering

Example usage:
    from src.versioning import get_version_manager, VersionedObjectType

    manager = get_version_manager()

    # Create a version
    version = manager.snapshot(
        object_type=VersionedObjectType.HIERARCHY,
        object_id="revenue-pl",
        data={"name": "Revenue P&L", "hierarchies": [...]},
        description="Added cost center mappings",
    )

    # Get history
    versions = manager.get_history(
        VersionedObjectType.HIERARCHY,
        "revenue-pl"
    )

    # Compare versions
    diff = manager.diff(
        VersionedObjectType.HIERARCHY,
        "revenue-pl",
        from_version="1.0.0",
        to_version="1.1.0"
    )

    # Rollback
    snapshot = manager.rollback(
        VersionedObjectType.HIERARCHY,
        "revenue-pl",
        to_version="1.0.0"
    )
"""

from .types import (
    VersionedObjectType,
    ChangeType,
    VersionBump,
    Version,
    VersionHistory,
    VersionDiff,
    VersionQuery,
    VersionStats,
    RollbackPreview,
    VersionTag,
    VersionCreateRequest,
    VersionResult,
)
from .version_store import VersionStore, get_version_store
from .version_manager import VersionManager, get_version_manager
from .mcp_tools import register_versioning_tools

__all__ = [
    # Types
    "VersionedObjectType",
    "ChangeType",
    "VersionBump",
    "Version",
    "VersionHistory",
    "VersionDiff",
    "VersionQuery",
    "VersionStats",
    "RollbackPreview",
    "VersionTag",
    "VersionCreateRequest",
    "VersionResult",
    # Store
    "VersionStore",
    "get_version_store",
    # Manager
    "VersionManager",
    "get_version_manager",
    # MCP Tools
    "register_versioning_tools",
]
