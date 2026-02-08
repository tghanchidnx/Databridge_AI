"""
Data Versioning MCP Tools - Phase 30
12 tools for version control of DataBridge objects.
"""
from typing import Optional
from .types import VersionedObjectType, ChangeType, VersionQuery
from .version_store import VersionStore
from .version_manager import VersionManager

_store = None
_manager = None

def get_manager():
    global _store, _manager
    if _manager is None:
        _store = VersionStore()
        _manager = VersionManager(_store)
    return _manager

def register_versioning_tools(mcp):
    manager = get_manager()

    @mcp.tool()
    def version_create(object_type: str, object_id: str, description: str = None, bump: str = "patch", user: str = None, tags: str = None) -> dict:
        """Create a versioned snapshot of an object."""
        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"error": f"Invalid object_type. Valid: {[e.value for e in VersionedObjectType]}"}
        snapshot = {"id": object_id, "type": object_type}
        tag_list = [t.strip() for t in tags.split(",")] if tags else []
        version = manager.snapshot(obj_type, object_id, snapshot, ChangeType.UPDATE, description, user, bump, tag_list)
        return version.model_dump(mode="json")

    @mcp.tool()
    def version_create_hierarchy(project_id: str, description: str = None, bump: str = "patch", user: str = None) -> dict:
        """Version a hierarchy project with all its hierarchies."""
        snapshot = {"project_id": project_id, "hierarchies": [], "mappings": []}
        version = manager.snapshot(VersionedObjectType.HIERARCHY_PROJECT, project_id, snapshot, ChangeType.UPDATE, description, user, bump)
        return version.model_dump(mode="json")

    @mcp.tool()
    def version_create_catalog_asset(asset_id: str, description: str = None, bump: str = "patch", user: str = None) -> dict:
        """Version a catalog asset."""
        snapshot = {"asset_id": asset_id}
        version = manager.snapshot(VersionedObjectType.CATALOG_ASSET, asset_id, snapshot, ChangeType.UPDATE, description, user, bump)
        return version.model_dump(mode="json")

    @mcp.tool()
    def version_get(object_type: str, object_id: str, version: str = None) -> dict:
        """Get a specific version or latest."""
        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"error": "Invalid object_type"}
        v = manager.store.get_version(obj_type, object_id, version)
        return v.model_dump(mode="json") if v else {"error": "Version not found"}

    @mcp.tool()
    def version_list(object_type: str, object_id: str, limit: int = 20) -> dict:
        """List version history for an object."""
        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"error": "Invalid object_type"}
        versions = manager.get_history(obj_type, object_id, limit)
        return {"object_type": object_type, "object_id": object_id, "count": len(versions), "versions": [v.model_dump(mode="json") for v in versions]}

    @mcp.tool()
    def version_search(object_type: str = None, changed_by: str = None, tag: str = None, limit: int = 50) -> dict:
        """Search versions across all objects."""
        query = VersionQuery(object_type=VersionedObjectType(object_type) if object_type else None, changed_by=changed_by, tag=tag, limit=limit)
        versions = manager.search(query)
        return {"count": len(versions), "versions": [v.model_dump(mode="json") for v in versions]}

    @mcp.tool()
    def version_diff(object_type: str, object_id: str, from_version: str, to_version: str = None) -> dict:
        """Compare two versions and show differences."""
        try:
            obj_type = VersionedObjectType(object_type)
            diff = manager.diff(obj_type, object_id, from_version, to_version)
            return diff.model_dump()
        except ValueError as e:
            return {"error": str(e)}

    @mcp.tool()
    def version_diff_latest(object_type: str, object_id: str) -> dict:
        """Compare current version to the previous one."""
        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"error": "Invalid object_type"}
        versions = manager.get_history(obj_type, object_id, 2)
        if len(versions) < 2:
            return {"error": "Need at least 2 versions to compare"}
        diff = manager.diff(obj_type, object_id, versions[1].version, versions[0].version)
        return diff.model_dump()

    @mcp.tool()
    def version_rollback(object_type: str, object_id: str, to_version: str, user: str = None) -> dict:
        """Rollback object to a previous version."""
        try:
            obj_type = VersionedObjectType(object_type)
            snapshot = manager.rollback(obj_type, object_id, to_version, user)
            return {"success": True, "rolled_back_to": to_version, "snapshot": snapshot}
        except ValueError as e:
            return {"error": str(e)}

    @mcp.tool()
    def version_preview_rollback(object_type: str, object_id: str, to_version: str) -> dict:
        """Preview what rollback would restore."""
        try:
            obj_type = VersionedObjectType(object_type)
            return manager.preview_rollback(obj_type, object_id, to_version)
        except ValueError as e:
            return {"error": str(e)}

    @mcp.tool()
    def version_tag(object_type: str, object_id: str, version: str, tag: str, remove: bool = False) -> dict:
        """Add or remove tags on a version."""
        try:
            obj_type = VersionedObjectType(object_type)
            success = manager.tag_version(obj_type, object_id, version, tag, remove)
            action = "removed" if remove else "added"
            return {"success": success, "message": f"Tag {action}: {tag}"}
        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def version_get_stats() -> dict:
        """Get versioning statistics."""
        return manager.get_stats()

    print("Data Versioning tools registered (12 tools)")
    return manager
