"""
Version Manager - High-level version operations.

Phase 30: Data Versioning business logic.
"""

import logging
from typing import Dict, List, Optional, Any

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
)
from .version_store import VersionStore, get_version_store

logger = logging.getLogger(__name__)


class VersionManager:
    """High-level version operations."""

    def __init__(self, store: Optional[VersionStore] = None):
        self.store = store or get_version_store()

    def snapshot(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        data: Dict[str, Any],
        change_type: ChangeType = ChangeType.UPDATE,
        description: Optional[str] = None,
        user: Optional[str] = None,
        bump: VersionBump = VersionBump.PATCH,
        tags: Optional[List[str]] = None,
        object_name: Optional[str] = None,
    ) -> Version:
        """Create a versioned snapshot of an object."""
        return self.store.create_version(
            object_type=object_type,
            object_id=object_id,
            snapshot=data,
            change_type=change_type,
            change_description=description,
            changed_by=user,
            version_bump=bump,
            tags=tags,
            object_name=object_name,
        )

    def diff(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        from_version: str,
        to_version: Optional[str] = None,  # None = current
    ) -> VersionDiff:
        """Compare two versions and return differences."""
        v1 = self.store.get_version(object_type, object_id, from_version)
        v2 = self.store.get_version(object_type, object_id, to_version)

        if not v1:
            raise ValueError(f"Version {from_version} not found")
        if not v2:
            raise ValueError(f"Version {to_version or 'latest'} not found")

        return self._compute_diff(v1, v2)

    def rollback(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        to_version: str,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Rollback to a previous version.

        Returns the snapshot data that should be applied.
        The caller is responsible for actually updating the object.
        """
        version = self.store.get_version(object_type, object_id, to_version)
        if not version:
            raise ValueError(f"Version {to_version} not found")

        # Create a new version recording the rollback
        self.store.create_version(
            object_type=object_type,
            object_id=object_id,
            snapshot=version.snapshot,
            change_type=ChangeType.RESTORE,
            change_description=f"Rollback to version {to_version}",
            changed_by=user,
            version_bump=VersionBump.MINOR,
        )

        logger.info(f"Rolled back {object_type.value}:{object_id} to version {to_version}")
        return version.snapshot

    def preview_rollback(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        to_version: str,
    ) -> RollbackPreview:
        """Preview what a rollback would restore without applying it."""
        target_version = self.store.get_version(object_type, object_id, to_version)
        current_version = self.store.get_version(object_type, object_id)

        if not target_version:
            raise ValueError(f"Version {to_version} not found")
        if not current_version:
            raise ValueError("No current version found")

        # Compute diff between current and target
        diff = self._compute_diff(current_version, target_version)

        warning = None
        if target_version.version_number < current_version.version_number - 5:
            warning = "Rolling back more than 5 versions. Please verify carefully."

        return RollbackPreview(
            object_type=object_type,
            object_id=object_id,
            current_version=current_version.version,
            target_version=target_version.version,
            snapshot=target_version.snapshot,
            diff=diff,
            warning=warning,
        )

    def get_latest(
        self,
        object_type: VersionedObjectType,
        object_id: str,
    ) -> Optional[Version]:
        """Get the latest version."""
        return self.store.get_version(object_type, object_id)

    def get_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
    ) -> Optional[Version]:
        """Get a specific version."""
        return self.store.get_version(object_type, object_id, version)

    def get_history(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        limit: int = 20,
    ) -> List[Version]:
        """Get version history (most recent first)."""
        return self.store.list_versions(object_type, object_id, limit)

    def search(self, query: VersionQuery) -> List[Version]:
        """Search versions across objects."""
        return self.store.search_versions(query)

    def tag_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
        tag: str,
    ) -> bool:
        """Add a tag to a version."""
        return self.store.add_tag(object_type, object_id, version, tag)

    def untag_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
        tag: str,
    ) -> bool:
        """Remove a tag from a version."""
        return self.store.remove_tag(object_type, object_id, version, tag)

    def get_stats(self) -> VersionStats:
        """Get versioning statistics."""
        return self.store.get_stats()

    def list_objects(
        self,
        object_type: Optional[VersionedObjectType] = None
    ) -> List[Dict[str, Any]]:
        """List all versioned objects."""
        return self.store.list_objects(object_type)

    def delete_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
    ) -> bool:
        """Delete a specific version."""
        return self.store.delete_version(object_type, object_id, version)

    def delete_all_versions(
        self,
        object_type: VersionedObjectType,
        object_id: str,
    ) -> bool:
        """Delete all versions for an object."""
        return self.store.delete_all_versions(object_type, object_id)

    def _compute_diff(self, v1: Version, v2: Version) -> VersionDiff:
        """Compute detailed diff between two snapshots."""
        added: Dict[str, Any] = {}
        removed: Dict[str, Any] = {}
        modified: Dict[str, Dict[str, Any]] = {}

        s1 = v1.snapshot
        s2 = v2.snapshot

        all_keys = set(s1.keys()) | set(s2.keys())

        for key in all_keys:
            if key not in s1:
                added[key] = s2[key]
            elif key not in s2:
                removed[key] = s1[key]
            elif s1[key] != s2[key]:
                modified[key] = {
                    "old": s1[key],
                    "new": s2[key],
                    "diff": self._value_diff(s1[key], s2[key])
                }

        total = len(added) + len(removed) + len(modified)

        summary_parts = []
        if added:
            summary_parts.append(f"{len(added)} added")
        if removed:
            summary_parts.append(f"{len(removed)} removed")
        if modified:
            summary_parts.append(f"{len(modified)} modified")

        return VersionDiff(
            object_type=v1.object_type,
            object_id=v1.object_id,
            from_version=v1.version,
            to_version=v2.version,
            added=added,
            removed=removed,
            modified=modified,
            total_changes=total,
            change_summary=", ".join(summary_parts) if summary_parts else "No changes",
        )

    def _value_diff(self, old: Any, new: Any) -> Optional[Dict[str, Any]]:
        """Compute detailed diff for a single value."""
        if isinstance(old, dict) and isinstance(new, dict):
            # Recursive diff for nested dicts
            nested_added = {}
            nested_removed = {}
            nested_modified = {}

            all_keys = set(old.keys()) | set(new.keys())
            for key in all_keys:
                if key not in old:
                    nested_added[key] = new[key]
                elif key not in new:
                    nested_removed[key] = old[key]
                elif old[key] != new[key]:
                    nested_modified[key] = {"old": old[key], "new": new[key]}

            return {
                "type": "dict",
                "added": nested_added,
                "removed": nested_removed,
                "modified": nested_modified,
            }
        elif isinstance(old, list) and isinstance(new, list):
            # List diff
            old_set = set(str(x) for x in old)
            new_set = set(str(x) for x in new)
            return {
                "type": "list",
                "added": [x for x in new if str(x) not in old_set],
                "removed": [x for x in old if str(x) not in new_set],
                "old_count": len(old),
                "new_count": len(new),
            }
        else:
            return {
                "type": "value",
                "old": old,
                "new": new,
            }


# Global instance
_version_manager: Optional[VersionManager] = None


def get_version_manager() -> VersionManager:
    """Get or create the global version manager instance."""
    global _version_manager
    if _version_manager is None:
        _version_manager = VersionManager()
    return _version_manager
