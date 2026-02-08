"""
Version Manager - High-level version operations.
"""
from typing import Dict, List, Optional, Any

from .types import (
    VersionedObjectType, ChangeType, Version,
    VersionHistory, VersionDiff, VersionQuery,
    VersionStats, RollbackPreview
)
from .version_store import VersionStore


class VersionManager:
    """High-level version operations."""

    def __init__(self, store: Optional[VersionStore] = None):
        self.store = store or VersionStore()

    def snapshot(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        data: Dict[str, Any],
        change_type: ChangeType = ChangeType.UPDATE,
        description: str = None,
        user: str = None,
        bump: str = "patch",
        tags: List[str] = None,
        object_name: str = None,
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
        to_version: str = None,
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
        user: str = None,
    ) -> Dict[str, Any]:
        """Rollback to a previous version. Returns the snapshot to apply."""
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
            version_bump="minor",
        )

        return version.snapshot

    def preview_rollback(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        to_version: str,
    ) -> RollbackPreview:
        """Preview what rollback would restore without executing."""
        version = self.store.get_version(object_type, object_id, to_version)
        if not version:
            raise ValueError(f"Version {to_version} not found")

        current = self.store.get_version(object_type, object_id)
        diff = self._compute_diff(current, version) if current else None

        return RollbackPreview(
            current_version=current.version if current else "0.0.0",
            target_version=to_version,
            snapshot=version.snapshot,
            diff=diff,
        )

    def get_latest(self, object_type: VersionedObjectType, object_id: str) -> Optional[Version]:
        """Get the latest version."""
        return self.store.get_version(object_type, object_id)

    def get_version(self, object_type: VersionedObjectType, object_id: str, version: str = None) -> Optional[Version]:
        """Get a specific version or latest."""
        return self.store.get_version(object_type, object_id, version)

    def get_history(self, object_type: VersionedObjectType, object_id: str, limit: int = 20) -> List[Version]:
        """Get version history."""
        return self.store.list_versions(object_type, object_id, limit)

    def search(self, query: VersionQuery) -> List[Version]:
        """Search versions."""
        return self.store.search_versions(query)

    def tag_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
        tag: str,
        remove: bool = False,
    ) -> bool:
        """Add or remove a tag from a version."""
        if remove:
            return self.store.remove_tag(object_type, object_id, version, tag)
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

    def list_objects(
        self,
        object_type: Optional[VersionedObjectType] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List versioned objects."""
        results = []
        for key, history in self.store._histories.items():
            if object_type and history.object_type != object_type:
                continue
            results.append({
                "object_type": history.object_type.value,
                "object_id": history.object_id,
                "current_version": history.current_version,
                "version_count": len(history.versions),
            })
        return results[:limit]

    def get_stats(self) -> VersionStats:
        """Get versioning statistics."""
        return self.store.get_stats()

    def _compute_diff(self, v1: Version, v2: Version) -> VersionDiff:
        """Compute detailed diff between two snapshots."""
        added = {}
        removed = {}
        modified = {}

        s1 = v1.snapshot
        s2 = v2.snapshot

        all_keys = set(s1.keys()) | set(s2.keys())

        for key in all_keys:
            if key not in s1:
                added[key] = s2[key]
            elif key not in s2:
                removed[key] = s1[key]
            elif s1[key] != s2[key]:
                modified[key] = {"old": s1[key], "new": s2[key]}

        total = len(added) + len(removed) + len(modified)

        return VersionDiff(
            object_type=v1.object_type,
            object_id=v1.object_id,
            from_version=v1.version,
            to_version=v2.version,
            added=added,
            removed=removed,
            modified=modified,
            total_changes=total,
            change_summary=f"{len(added)} added, {len(removed)} removed, {len(modified)} modified",
        )
