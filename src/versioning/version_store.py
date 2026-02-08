"""
Version Store - Persistence layer for version history.
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from .types import (
    VersionedObjectType, ChangeType, VersionBump, Version,
    VersionHistory, VersionQuery
)


class VersionStore:
    """Persistence layer for version history."""

    def __init__(self, data_dir: str = "data/versioning"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._histories: Dict[str, VersionHistory] = {}
        self._load()

    def _get_key(self, object_type: VersionedObjectType, object_id: str) -> str:
        """Generate unique key for object."""
        return f"{object_type.value}:{object_id}"

    def _get_history_file(self) -> Path:
        """Get path to histories file."""
        return self.data_dir / "histories.json"

    def create_version(
        self,
        object_type: VersionedObjectType,
        object_id: str,
        snapshot: Dict[str, Any],
        change_type: ChangeType,
        change_description: Optional[str] = None,
        changed_by: Optional[str] = None,
        version_bump: Union[str, VersionBump] = "patch",
        tags: List[str] = None,
    ) -> Version:
        """Create a new version for an object."""
        # Convert VersionBump enum to string if needed
        if isinstance(version_bump, VersionBump):
            version_bump = version_bump.value

        key = self._get_key(object_type, object_id)

        if key not in self._histories:
            history = VersionHistory(
                object_type=object_type,
                object_id=object_id,
                current_version="0.0.0",
                current_version_number=0,
            )
            self._histories[key] = history
        else:
            history = self._histories[key]

        new_version = self._bump_version(history.current_version, version_bump)
        new_version_number = history.current_version_number + 1

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
            is_major=version_bump == "major",
        )

        history.versions.append(version)
        history.current_version = new_version
        history.current_version_number = new_version_number

        self._save()
        return version

    def get_version(self, object_type: VersionedObjectType, object_id: str, version: Optional[str] = None) -> Optional[Version]:
        """Get a specific version or latest."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)
        if not history or not history.versions:
            return None
        if version is None:
            return history.versions[-1]
        for v in history.versions:
            if v.version == version:
                return v
        return None

    def get_history(self, object_type: VersionedObjectType, object_id: str) -> Optional[VersionHistory]:
        """Get full version history for an object."""
        key = self._get_key(object_type, object_id)
        return self._histories.get(key)

    def list_versions(self, object_type: VersionedObjectType, object_id: str, limit: int = 20) -> List[Version]:
        """List versions for an object."""
        key = self._get_key(object_type, object_id)
        history = self._histories.get(key)
        if not history:
            return []
        return list(reversed(history.versions[-limit:]))

    def search_versions(self, query: VersionQuery) -> List[Version]:
        """Search versions across all objects."""
        results = []
        for history in self._histories.values():
            if query.object_type and history.object_type != query.object_type:
                continue
            if query.object_id and history.object_id != query.object_id:
                continue
            for version in history.versions:
                if query.change_type and version.change_type != query.change_type:
                    continue
                if query.changed_by and version.changed_by != query.changed_by:
                    continue
                if query.tag and query.tag not in version.tags:
                    continue
                results.append(version)
        results.sort(key=lambda v: v.changed_at, reverse=True)
        return results[:query.limit]

    def add_tag(self, object_type: VersionedObjectType, object_id: str, version: str, tag: str) -> bool:
        """Add a tag to a version."""
        v = self.get_version(object_type, object_id, version)
        if v and tag not in v.tags:
            v.tags.append(tag)
            self._save()
            return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        """Get versioning statistics."""
        total_objects = len(self._histories)
        total_versions = sum(len(h.versions) for h in self._histories.values())
        by_type = {}
        for history in self._histories.values():
            t = history.object_type.value
            if t not in by_type:
                by_type[t] = {"objects": 0, "versions": 0}
            by_type[t]["objects"] += 1
            by_type[t]["versions"] += len(history.versions)
        return {"total_objects": total_objects, "total_versions": total_versions, "by_type": by_type}

    def _bump_version(self, current: str, bump: str) -> str:
        """Increment version number."""
        parts = [int(x) for x in current.split(".")]
        if len(parts) != 3:
            parts = [0, 0, 0]
        if bump == "major":
            return f"{parts[0] + 1}.0.0"
        elif bump == "minor":
            return f"{parts[0]}.{parts[1] + 1}.0"
        else:
            return f"{parts[0]}.{parts[1]}.{parts[2] + 1}"

    def _save(self) -> None:
        """Persist to disk."""
        data = {}
        for key, history in self._histories.items():
            data[key] = history.model_dump(mode="json")
        with open(self._get_history_file(), "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _load(self) -> None:
        """Load from disk."""
        history_file = self._get_history_file()
        if not history_file.exists():
            return
        try:
            with open(history_file, "r") as f:
                data = json.load(f)
            for key, hist_data in data.items():
                self._histories[key] = VersionHistory(**hist_data)
        except Exception as e:
            print(f"Warning: Could not load version history: {e}")
