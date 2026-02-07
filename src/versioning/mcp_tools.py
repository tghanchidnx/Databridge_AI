"""
MCP Tools for Data Versioning.

Phase 30: 12 MCP tools for version control operations.
"""

import logging
from typing import Optional, List
from datetime import datetime

from .types import (
    VersionedObjectType,
    ChangeType,
    VersionBump,
    VersionQuery,
)
from .version_manager import get_version_manager

logger = logging.getLogger(__name__)


def register_versioning_tools(mcp):
    """Register all versioning MCP tools."""

    # =========================================================================
    # VERSION CREATION (3 tools)
    # =========================================================================

    @mcp.tool()
    def version_create(
        object_type: str,
        object_id: str,
        snapshot: str,
        change_description: Optional[str] = None,
        changed_by: Optional[str] = None,
        version_bump: str = "patch",
        change_type: str = "update",
        tags: Optional[str] = None,
        object_name: Optional[str] = None,
    ) -> dict:
        """
        Create a versioned snapshot of any object.

        This is the core versioning tool that creates a new version record
        with a full snapshot of the object's current state.

        Args:
            object_type: Type of object (hierarchy_project, hierarchy, catalog_asset,
                        glossary_term, semantic_model, data_contract, expectation_suite,
                        formula_group, source_mapping)
            object_id: Unique identifier for the object
            snapshot: JSON string of the complete object state
            change_description: Human-readable description of changes
            changed_by: User who made the change
            version_bump: Type of version increment (major, minor, patch)
            change_type: Type of change (create, update, delete, restore)
            tags: Comma-separated tags (e.g., "release,approved,production")
            object_name: Human-readable name for the object

        Returns:
            Version record with id, version string, and metadata
        """
        import json

        manager = get_version_manager()

        # Parse object type
        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid object_type: {object_type}. Valid types: {[t.value for t in VersionedObjectType]}"
            }

        # Parse snapshot
        try:
            snapshot_data = json.loads(snapshot) if isinstance(snapshot, str) else snapshot
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"Invalid JSON snapshot: {e}"}

        # Parse version bump
        try:
            bump = VersionBump(version_bump)
        except ValueError:
            bump = VersionBump.PATCH

        # Parse change type
        try:
            c_type = ChangeType(change_type)
        except ValueError:
            c_type = ChangeType.UPDATE

        # Parse tags
        tag_list = [t.strip() for t in tags.split(",")] if tags else None

        try:
            version = manager.snapshot(
                object_type=obj_type,
                object_id=object_id,
                data=snapshot_data,
                change_type=c_type,
                description=change_description,
                user=changed_by,
                bump=bump,
                tags=tag_list,
                object_name=object_name,
            )

            return {
                "success": True,
                "message": f"Created version {version.version} for {object_type}:{object_id}",
                "version": {
                    "id": version.id,
                    "version": version.version,
                    "version_number": version.version_number,
                    "change_type": version.change_type.value,
                    "change_description": version.change_description,
                    "changed_by": version.changed_by,
                    "changed_at": version.changed_at.isoformat(),
                    "tags": version.tags,
                    "is_major": version.is_major,
                }
            }
        except Exception as e:
            logger.error(f"Error creating version: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def version_create_hierarchy(
        project_id: str,
        hierarchies: str,
        change_description: Optional[str] = None,
        changed_by: Optional[str] = None,
        version_bump: str = "patch",
        project_name: Optional[str] = None,
    ) -> dict:
        """
        Create a versioned snapshot of a hierarchy project with all its hierarchies.

        This is a convenience tool for versioning hierarchy projects that automatically
        includes all child hierarchies in the snapshot.

        Args:
            project_id: The hierarchy project ID
            hierarchies: JSON string with project data including hierarchies array
            change_description: Description of what changed
            changed_by: User who made the change
            version_bump: Version increment type (major, minor, patch)
            project_name: Human-readable project name

        Returns:
            Version record for the project
        """
        import json

        manager = get_version_manager()

        try:
            hierarchy_data = json.loads(hierarchies) if isinstance(hierarchies, str) else hierarchies
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"Invalid JSON: {e}"}

        try:
            bump = VersionBump(version_bump)
        except ValueError:
            bump = VersionBump.PATCH

        try:
            version = manager.snapshot(
                object_type=VersionedObjectType.HIERARCHY_PROJECT,
                object_id=project_id,
                data=hierarchy_data,
                change_type=ChangeType.UPDATE,
                description=change_description,
                user=changed_by,
                bump=bump,
                object_name=project_name,
            )

            return {
                "success": True,
                "message": f"Created version {version.version} for hierarchy project {project_id}",
                "version": {
                    "id": version.id,
                    "version": version.version,
                    "version_number": version.version_number,
                    "changed_at": version.changed_at.isoformat(),
                },
                "hierarchy_count": len(hierarchy_data.get("hierarchies", [])) if isinstance(hierarchy_data, dict) else 0,
            }
        except Exception as e:
            logger.error(f"Error versioning hierarchy: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def version_create_catalog_asset(
        asset_id: str,
        asset_data: str,
        change_description: Optional[str] = None,
        changed_by: Optional[str] = None,
        version_bump: str = "patch",
        asset_name: Optional[str] = None,
    ) -> dict:
        """
        Create a versioned snapshot of a data catalog asset.

        Args:
            asset_id: The catalog asset ID
            asset_data: JSON string with the asset metadata
            change_description: Description of what changed
            changed_by: User who made the change
            version_bump: Version increment type (major, minor, patch)
            asset_name: Human-readable asset name

        Returns:
            Version record for the asset
        """
        import json

        manager = get_version_manager()

        try:
            data = json.loads(asset_data) if isinstance(asset_data, str) else asset_data
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"Invalid JSON: {e}"}

        try:
            bump = VersionBump(version_bump)
        except ValueError:
            bump = VersionBump.PATCH

        try:
            version = manager.snapshot(
                object_type=VersionedObjectType.CATALOG_ASSET,
                object_id=asset_id,
                data=data,
                change_type=ChangeType.UPDATE,
                description=change_description,
                user=changed_by,
                bump=bump,
                object_name=asset_name,
            )

            return {
                "success": True,
                "message": f"Created version {version.version} for catalog asset {asset_id}",
                "version": {
                    "id": version.id,
                    "version": version.version,
                    "version_number": version.version_number,
                    "changed_at": version.changed_at.isoformat(),
                }
            }
        except Exception as e:
            logger.error(f"Error versioning catalog asset: {e}")
            return {"success": False, "error": str(e)}

    # =========================================================================
    # VERSION HISTORY (3 tools)
    # =========================================================================

    @mcp.tool()
    def version_get(
        object_type: str,
        object_id: str,
        version: Optional[str] = None,
        include_snapshot: bool = True,
    ) -> dict:
        """
        Get a specific version or the latest version of an object.

        Args:
            object_type: Type of object
            object_id: Object identifier
            version: Specific version string (e.g., "1.2.3") or None for latest
            include_snapshot: Whether to include the full snapshot data

        Returns:
            Version record with optional snapshot
        """
        manager = get_version_manager()

        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"success": False, "error": f"Invalid object_type: {object_type}"}

        v = manager.get_version(obj_type, object_id, version) if version else manager.get_latest(obj_type, object_id)

        if not v:
            return {
                "success": False,
                "error": f"Version not found for {object_type}:{object_id}"
            }

        result = {
            "success": True,
            "version": {
                "id": v.id,
                "object_type": v.object_type.value,
                "object_id": v.object_id,
                "version": v.version,
                "version_number": v.version_number,
                "change_type": v.change_type.value,
                "change_description": v.change_description,
                "changed_by": v.changed_by,
                "changed_at": v.changed_at.isoformat(),
                "tags": v.tags,
                "is_major": v.is_major,
            }
        }

        if include_snapshot:
            result["version"]["snapshot"] = v.snapshot

        return result

    @mcp.tool()
    def version_list(
        object_type: str,
        object_id: str,
        limit: int = 20,
    ) -> dict:
        """
        List version history for an object (most recent first).

        Args:
            object_type: Type of object
            object_id: Object identifier
            limit: Maximum number of versions to return

        Returns:
            List of versions without snapshot data
        """
        manager = get_version_manager()

        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"success": False, "error": f"Invalid object_type: {object_type}"}

        versions = manager.get_history(obj_type, object_id, limit)

        history = manager.store.get_history(obj_type, object_id)

        return {
            "success": True,
            "object_type": object_type,
            "object_id": object_id,
            "current_version": history.current_version if history else None,
            "total_versions": len(history.versions) if history else 0,
            "versions": [
                {
                    "version": v.version,
                    "version_number": v.version_number,
                    "change_type": v.change_type.value,
                    "change_description": v.change_description,
                    "changed_by": v.changed_by,
                    "changed_at": v.changed_at.isoformat(),
                    "tags": v.tags,
                    "is_major": v.is_major,
                }
                for v in versions
            ]
        }

    @mcp.tool()
    def version_search(
        object_type: Optional[str] = None,
        object_id: Optional[str] = None,
        changed_by: Optional[str] = None,
        change_type: Optional[str] = None,
        tag: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        is_major: Optional[bool] = None,
        limit: int = 50,
    ) -> dict:
        """
        Search versions across all objects with filters.

        Args:
            object_type: Filter by object type
            object_id: Filter by specific object
            changed_by: Filter by user who made changes
            change_type: Filter by change type (create, update, delete, restore)
            tag: Filter by tag
            from_date: Start date (ISO format)
            to_date: End date (ISO format)
            is_major: Filter for major versions only
            limit: Maximum results

        Returns:
            List of matching versions
        """
        manager = get_version_manager()

        # Build query
        query = VersionQuery(limit=limit)

        if object_type:
            try:
                query.object_type = VersionedObjectType(object_type)
            except ValueError:
                return {"success": False, "error": f"Invalid object_type: {object_type}"}

        if object_id:
            query.object_id = object_id

        if changed_by:
            query.changed_by = changed_by

        if change_type:
            try:
                query.change_type = ChangeType(change_type)
            except ValueError:
                pass

        if tag:
            query.tag = tag

        if from_date:
            try:
                query.from_date = datetime.fromisoformat(from_date)
            except ValueError:
                pass

        if to_date:
            try:
                query.to_date = datetime.fromisoformat(to_date)
            except ValueError:
                pass

        if is_major is not None:
            query.is_major = is_major

        versions = manager.search(query)

        return {
            "success": True,
            "count": len(versions),
            "versions": [
                {
                    "object_type": v.object_type.value,
                    "object_id": v.object_id,
                    "version": v.version,
                    "version_number": v.version_number,
                    "change_type": v.change_type.value,
                    "change_description": v.change_description,
                    "changed_by": v.changed_by,
                    "changed_at": v.changed_at.isoformat(),
                    "tags": v.tags,
                    "is_major": v.is_major,
                }
                for v in versions
            ]
        }

    # =========================================================================
    # COMPARISON (2 tools)
    # =========================================================================

    @mcp.tool()
    def version_diff(
        object_type: str,
        object_id: str,
        from_version: str,
        to_version: Optional[str] = None,
    ) -> dict:
        """
        Compare two versions and show differences.

        Args:
            object_type: Type of object
            object_id: Object identifier
            from_version: Starting version (e.g., "1.0.0")
            to_version: Ending version (None = latest)

        Returns:
            Diff with added, removed, and modified fields
        """
        manager = get_version_manager()

        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"success": False, "error": f"Invalid object_type: {object_type}"}

        try:
            diff = manager.diff(obj_type, object_id, from_version, to_version)

            return {
                "success": True,
                "object_type": object_type,
                "object_id": object_id,
                "from_version": diff.from_version,
                "to_version": diff.to_version,
                "total_changes": diff.total_changes,
                "change_summary": diff.change_summary,
                "added": diff.added,
                "removed": diff.removed,
                "modified": diff.modified,
            }
        except ValueError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def version_diff_latest(
        object_type: str,
        object_id: str,
        compare_to_version: str,
    ) -> dict:
        """
        Compare current (latest) version to a previous version.

        This is a convenience wrapper for version_diff that compares
        a specific historical version to the current state.

        Args:
            object_type: Type of object
            object_id: Object identifier
            compare_to_version: Historical version to compare against

        Returns:
            Diff showing what changed since that version
        """
        return version_diff(
            object_type=object_type,
            object_id=object_id,
            from_version=compare_to_version,
            to_version=None,  # Latest
        )

    # =========================================================================
    # ROLLBACK (2 tools)
    # =========================================================================

    @mcp.tool()
    def version_rollback(
        object_type: str,
        object_id: str,
        to_version: str,
        changed_by: Optional[str] = None,
    ) -> dict:
        """
        Rollback an object to a previous version.

        This creates a new version with the state from the target version,
        recording it as a RESTORE change type. The caller is responsible
        for actually applying the snapshot to the underlying object.

        Args:
            object_type: Type of object
            object_id: Object identifier
            to_version: Target version to restore
            changed_by: User performing the rollback

        Returns:
            The snapshot data to apply and the new version record
        """
        manager = get_version_manager()

        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"success": False, "error": f"Invalid object_type: {object_type}"}

        try:
            snapshot = manager.rollback(obj_type, object_id, to_version, changed_by)

            # Get the new version that was created
            new_version = manager.get_latest(obj_type, object_id)

            return {
                "success": True,
                "message": f"Rolled back to version {to_version}. New version: {new_version.version}",
                "restored_version": to_version,
                "new_version": new_version.version,
                "snapshot": snapshot,
                "note": "Apply the snapshot data to the underlying object to complete the rollback."
            }
        except ValueError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def version_preview_rollback(
        object_type: str,
        object_id: str,
        to_version: str,
    ) -> dict:
        """
        Preview what a rollback would restore without applying it.

        Use this to see the differences before committing to a rollback.

        Args:
            object_type: Type of object
            object_id: Object identifier
            to_version: Target version to preview

        Returns:
            Preview with diff and optional warnings
        """
        manager = get_version_manager()

        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"success": False, "error": f"Invalid object_type: {object_type}"}

        try:
            preview = manager.preview_rollback(obj_type, object_id, to_version)

            return {
                "success": True,
                "object_type": object_type,
                "object_id": object_id,
                "current_version": preview.current_version,
                "target_version": preview.target_version,
                "warning": preview.warning,
                "diff": {
                    "total_changes": preview.diff.total_changes,
                    "change_summary": preview.diff.change_summary,
                    "added": preview.diff.added,
                    "removed": preview.diff.removed,
                    "modified": preview.diff.modified,
                } if preview.diff else None,
                "snapshot_preview": {
                    k: v for i, (k, v) in enumerate(preview.snapshot.items()) if i < 10
                },  # First 10 fields only
                "snapshot_fields": list(preview.snapshot.keys()),
            }
        except ValueError as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # TAGS & METADATA (2 tools)
    # =========================================================================

    @mcp.tool()
    def version_tag(
        object_type: str,
        object_id: str,
        version: str,
        add_tags: Optional[str] = None,
        remove_tags: Optional[str] = None,
    ) -> dict:
        """
        Add or remove tags on a version.

        Tags are useful for marking releases, approvals, or other milestones.
        Common tags: "release", "approved", "production", "staging", "archived"

        Args:
            object_type: Type of object
            object_id: Object identifier
            version: Version string to tag
            add_tags: Comma-separated tags to add
            remove_tags: Comma-separated tags to remove

        Returns:
            Updated version with current tags
        """
        manager = get_version_manager()

        try:
            obj_type = VersionedObjectType(object_type)
        except ValueError:
            return {"success": False, "error": f"Invalid object_type: {object_type}"}

        added = []
        removed = []

        if add_tags:
            for tag in add_tags.split(","):
                tag = tag.strip()
                if tag and manager.tag_version(obj_type, object_id, version, tag):
                    added.append(tag)

        if remove_tags:
            for tag in remove_tags.split(","):
                tag = tag.strip()
                if tag and manager.untag_version(obj_type, object_id, version, tag):
                    removed.append(tag)

        # Get current version with updated tags
        v = manager.get_version(obj_type, object_id, version)

        return {
            "success": True,
            "object_type": object_type,
            "object_id": object_id,
            "version": version,
            "added_tags": added,
            "removed_tags": removed,
            "current_tags": v.tags if v else [],
        }

    @mcp.tool()
    def version_get_stats() -> dict:
        """
        Get versioning statistics across all objects.

        Returns counts by object type, recent activity, and top contributors.

        Returns:
            Statistics including total counts, by-type breakdowns, and activity
        """
        manager = get_version_manager()
        stats = manager.get_stats()

        # Also get list of objects
        objects = manager.list_objects()

        return {
            "success": True,
            "stats": {
                "total_objects": stats.total_objects,
                "total_versions": stats.total_versions,
                "objects_by_type": stats.objects_by_type,
                "versions_by_type": stats.versions_by_type,
                "recent_changes_24h": stats.recent_changes,
                "top_changers": stats.top_changers,
            },
            "object_types": [t.value for t in VersionedObjectType],
            "recent_objects": objects[:10] if objects else [],
        }

    logger.info("Registered 12 versioning MCP tools")
