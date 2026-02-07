"""
Version Store - Persistence layer for version history.

Phase 30: Data Versioning storage and retrieval.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone

from .types import (
    VersionedObjectType,
    ChangeType,
    VersionBump,
    Version,
    VersionHistory,
    VersionQuery,
    VersionStats,
)

logger = logging.getLogger(__name__)


class VersionStore:
    """Persistence layer for version history."""

    def __init__(self, data_dir: str = "data/versioning"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.snapshots_dir = self.data_dir / "snapshots"
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)

        self._histories: Dict[str, VersionHistory] = {}
        self._index: Dict[str, List[str]] = {}  # object_type -> [object_ids]
        self._load()

    def _get_key(self, object_type: VersionedObjectType, object_id: str) -> str:
        """Generate unique key for object."""
        return f"{object_type.value}:{object_id}"

    def _get_snapshot_path(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str
    ) -> Path:
        """Get path for snapshot file."""
        type_dir = self.snapshots_dir / object_type.value
        type_dir.mkdir(parents=True, exist_ok=True)
        obj_dir = type_dir / object_id
        obj_dir.mkdir(parents=True, exist_ok=True)
        return obj_dir / f"v{version}.json"

    def create_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        snapshot: Dict[str, Any],
        change_type: ChangeType,
        change_description: Optional[str] = None,
        changed_by: Optional[str] = None,
        version_bump: VersionBump = VersionBump.PATCH,
        tags: Optional[List[str]] = None,
        object_name: Optional[str] = None,
    ) -> Version:
        """Create a new version for an object."""
        key = self._get_key(object_type, object_id)

        # Get or create history
        if key not in self._histories:
            history = VersionHistory(
                object_type=object_type,
                object_id=object_id,
                object_name=object_name,
                current_version="0.0.0",
                current_version_number=0,
            )
            self._histories[key] = history

            # Update index
            type_key = object_type.value
            if type_key not in self._index:
                self._index[type_key] = []
            if object_id not in self._index[type_key]:
                self._index[type_key].append(object_id)
        else:
            history = self._histories[key]
            if object_name:
                history.object_name = object_name

        # Calculate new version
        new_version = self._bump_version(history.current_version, version_bump)
        new_version_number = history.current_version_number + 1

        # Create version record
        version = Version(
            object_type=object_type,
            object_id=object_id,
            version=new_version,
            version_number=new_version_number,
            change_type=change_type,
            change_description=change_description,
            changed_by=changed_by,
            snapshot=snapshot,
            tags=tags or [],
            is_major=version_bump == VersionBump.MAJOR,
        )

        # Store large snapshots separately
        if len(json.dumps(snapshot)) > 10000:  # 10KB threshold
            snapshot_path = self._get_snapshot_path(object_type, object_id, new_version)
            with open(snapshot_path, "w") as f:
                json.dump(snapshot, f, indent=2, default=str)
            version.snapshot = {"_snapshot_file": str(snapshot_path)}

        # Update history
        history.versions.append(version)
        history.current_version = new_version
        history.current_version_number = new_version_number
        history.updated_at = datetime.now(timezone.utc)

        self._save()
        logger.info(f"Created version {new_version} for {object_type.value}:{object_id}")
        return version

    def get_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: Optional[str] = None,
    ) -> Optional[Version]:
        """Get a specific version or latest."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)

        if not history or not history.versions:
            return None

        if version is None or version == "latest":
            v = history.versions[-1]
        else:
            v = next(
                (v for v in history.versions if v.version == version),
                None
            )

        if v and "_snapshot_file" in v.snapshot:
            # Load from file
            snapshot_path = Path(v.snapshot["_snapshot_file"])
            if snapshot_path.exists():
                with open(snapshot_path) as f:
                    v.snapshot = json.load(f)

        return v

    def get_version_by_number(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version_number: int,
    ) -> Optional[Version]:
        """Get a version by its sequential number."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)

        if not history:
            return None

        v = next(
            (v for v in history.versions if v.version_number == version_number),
            None
        )

        if v and "_snapshot_file" in v.snapshot:
            snapshot_path = Path(v.snapshot["_snapshot_file"])
            if snapshot_path.exists():
                with open(snapshot_path) as f:
                    v.snapshot = json.load(f)

        return v

    def get_history(
        self,
        object_type: VersionedObjectType,
        object_id: str,
    ) -> Optional[VersionHistory]:
        """Get full version history for an object."""
        key = self._get_key(object_type, object_id)
        return self._histories.get(key)

    def list_versions(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        limit: int = 20,
        include_snapshots: bool = False,
    ) -> List[Version]:
        """List versions for an object (most recent first)."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)

        if not history:
            return []

        versions = list(reversed(history.versions))[:limit]

        if not include_snapshots:
            # Return versions without full snapshot data
            return [
                Version(
                    id=v.id,
                    object_type=v.object_type,
                    object_id=v.object_id,
                    version=v.version,
                    version_number=v.version_number,
                    change_type=v.change_type,
                    change_description=v.change_description,
                    changed_by=v.changed_by,
                    changed_at=v.changed_at,
                    snapshot={},  # Exclude snapshot for list
                    tags=v.tags,
                    is_major=v.is_major,
                )
                for v in versions
            ]

        return versions

    def search_versions(self, query: VersionQuery) -> List[Version]:
        """Search versions across all objects."""
        results = []

        for key, history in self._histories.items():
            # Filter by object type
            if query.object_type and history.object_type != query.object_type:
                continue

            # Filter by object id
            if query.object_id and history.object_id != query.object_id:
                continue

            for v in reversed(history.versions):
                # Date filters
                if query.from_date and v.changed_at < query.from_date:
                    continue
                if query.to_date and v.changed_at > query.to_date:
                    continue

                # User filter
                if query.changed_by and v.changed_by != query.changed_by:
                    continue

                # Change type filter
                if query.change_type and v.change_type != query.change_type:
                    continue

                # Tag filter
                if query.tag and query.tag not in v.tags:
                    continue

                # Major version filter
                if query.is_major is not None and v.is_major != query.is_major:
                    continue

                # Return without snapshot
                results.append(Version(
                    id=v.id,
                    object_type=v.object_type,
                    object_id=v.object_id,
                    version=v.version,
                    version_number=v.version_number,
                    change_type=v.change_type,
                    change_description=v.change_description,
                    changed_by=v.changed_by,
                    changed_at=v.changed_at,
                    snapshot={},
                    tags=v.tags,
                    is_major=v.is_major,
                ))

                if len(results) >= query.limit + query.offset:
                    break

        # Sort by changed_at descending
        results.sort(key=lambda x: x.changed_at, reverse=True)

        # Apply offset and limit
        return results[query.offset:query.offset + query.limit]

    def delete_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
    ) -> bool:
        """Delete a specific version (admin only)."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)

        if not history:
            return False

        original_count = len(history.versions)
        history.versions = [v for v in history.versions if v.version != version]

        if len(history.versions) < original_count:
            # Delete snapshot file if exists
            snapshot_path = self._get_snapshot_path(object_type, object_id, version)
            if snapshot_path.exists():
                snapshot_path.unlink()

            self._save()
            logger.info(f"Deleted version {version} for {object_type.value}:{object_id}")
            return True

        return False

    def delete_all_versions(
        self,
        object_type: VersionedObjectType,
        object_id: str,
    ) -> bool:
        """Delete all versions for an object."""
        key = self._get_key(object_type, object_id)

        if key not in self._histories:
            return False

        # Delete snapshot directory
        obj_dir = self.snapshots_dir / object_type.value / object_id
        if obj_dir.exists():
            import shutil
            shutil.rmtree(obj_dir)

        # Remove from histories
        del self._histories[key]

        # Update index
        type_key = object_type.value
        if type_key in self._index and object_id in self._index[type_key]:
            self._index[type_key].remove(object_id)

        self._save()
        logger.info(f"Deleted all versions for {object_type.value}:{object_id}")
        return True

    def add_tag(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
        tag: str,
    ) -> bool:
        """Add a tag to a version."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)

        if not history:
            return False

        for v in history.versions:
            if v.version == version:
                if tag not in v.tags:
                    v.tags.append(tag)
                    self._save()
                return True

        return False

    def remove_tag(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        version: str,
        tag: str,
    ) -> bool:
        """Remove a tag from a version."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)

        if not history:
            return False

        for v in history.versions:
            if v.version == version:
                if tag in v.tags:
                    v.tags.remove(tag)
                    self._save()
                return True

        return False

    def get_stats(self) -> VersionStats:
        """Get versioning statistics."""
        now = datetime.now(timezone.utc)
        day_ago = now - timedelta(days=1)

        total_versions = 0
        recent_changes = 0
        objects_by_type: Dict[str, int] = {}
        versions_by_type: Dict[str, int] = {}
        changers: Dict[str, int] = {}

        for history in self._histories.values():
            type_key = history.object_type.value
            objects_by_type[type_key] = objects_by_type.get(type_key, 0) + 1
            versions_by_type[type_key] = versions_by_type.get(type_key, 0) + len(history.versions)
            total_versions += len(history.versions)

            for v in history.versions:
                if v.changed_at >= day_ago:
                    recent_changes += 1
                if v.changed_by:
                    changers[v.changed_by] = changers.get(v.changed_by, 0) + 1

        # Sort changers by count
        top_changers = [
            {"user": user, "count": count}
            for user, count in sorted(changers.items(), key=lambda x: x[1], reverse=True)[:10]
        ]

        return VersionStats(
            total_objects=len(self._histories),
            total_versions=total_versions,
            objects_by_type=objects_by_type,
            versions_by_type=versions_by_type,
            recent_changes=recent_changes,
            top_changers=top_changers,
        )

    def list_objects(
        self,
        object_type: Optional[VersionedObjectType] = None
    ) -> List[Dict[str, Any]]:
        """List all versioned objects."""
        results = []

        for key, history in self._histories.items():
            if object_type and history.object_type != object_type:
                continue

            results.append({
                "object_type": history.object_type.value,
                "object_id": history.object_id,
                "object_name": history.object_name,
                "current_version": history.current_version,
                "version_count": len(history.versions),
                "created_at": history.created_at.isoformat(),
                "updated_at": history.updated_at.isoformat(),
            })

        return results

    def _bump_version(self, current: str, bump: VersionBump) -> str:
        """Increment version number."""
        try:
            parts = [int(x) for x in current.split(".")]
            if len(parts) != 3:
                parts = [0, 0, 0]
        except (ValueError, AttributeError):
            parts = [0, 0, 0]

        if bump == VersionBump.MAJOR:
            return f"{parts[0] + 1}.0.0"
        elif bump == VersionBump.MINOR:
            return f"{parts[0]}.{parts[1] + 1}.0"
        else:  # patch
            return f"{parts[0]}.{parts[1]}.{parts[2] + 1}"

    def _save(self) -> None:
        """Persist to disk."""
        histories_file = self.data_dir / "histories.json"

        # Convert to serializable format
        data = {
            "histories": {},
            "index": self._index,
        }

        for key, history in self._histories.items():
            # Convert history to dict
            history_dict = history.model_dump()

            # Handle datetime serialization
            history_dict["created_at"] = history.created_at.isoformat()
            history_dict["updated_at"] = history.updated_at.isoformat()

            # Handle versions
            versions_list = []
            for v in history.versions:
                v_dict = v.model_dump()
                v_dict["changed_at"] = v.changed_at.isoformat()
                versions_list.append(v_dict)
            history_dict["versions"] = versions_list

            data["histories"][key] = history_dict

        with open(histories_file, "w") as f:
            json.dump(data, f, indent=2)

    def _load(self) -> None:
        """Load from disk."""
        histories_file = self.data_dir / "histories.json"

        if not histories_file.exists():
            return

        try:
            with open(histories_file) as f:
                data = json.load(f)

            self._index = data.get("index", {})

            for key, history_dict in data.get("histories", {}).items():
                # Parse datetime strings
                history_dict["created_at"] = datetime.fromisoformat(history_dict["created_at"])
                history_dict["updated_at"] = datetime.fromisoformat(history_dict["updated_at"])

                # Parse versions
                versions = []
                for v_dict in history_dict.get("versions", []):
                    v_dict["changed_at"] = datetime.fromisoformat(v_dict["changed_at"])
                    versions.append(Version(**v_dict))
                history_dict["versions"] = versions

                self._histories[key] = VersionHistory(**history_dict)

            logger.info(f"Loaded {len(self._histories)} version histories")
        except Exception as e:
            logger.error(f"Error loading version store: {e}")
            self._histories = {}
            self._index = {}


# Global instance
_version_store: Optional[VersionStore] = None


def get_version_store() -> VersionStore:
    """Get or create the global version store instance."""
    global _version_store
    if _version_store is None:
        _version_store = VersionStore()
    return _version_store
