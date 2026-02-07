"""
Unit tests for Phase 30 - Data Versioning Module.

Tests version creation, history, diff, rollback, and tagging.
"""

import pytest
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta

from src.versioning import (
    VersionedObjectType,
    ChangeType,
    VersionBump,
    Version,
    VersionHistory,
    VersionDiff,
    VersionQuery,
    VersionStore,
    VersionManager,
)


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary data directory for testing."""
    data_dir = tmp_path / "versioning"
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir)


@pytest.fixture
def version_store(temp_data_dir):
    """Create a fresh VersionStore for testing."""
    return VersionStore(data_dir=temp_data_dir)


@pytest.fixture
def version_manager(version_store):
    """Create a VersionManager with the test store."""
    return VersionManager(store=version_store)


class TestVersionTypes:
    """Test Pydantic models and enums."""

    def test_versioned_object_types(self):
        """Test all object types are defined."""
        types = list(VersionedObjectType)
        assert VersionedObjectType.HIERARCHY_PROJECT in types
        assert VersionedObjectType.HIERARCHY in types
        assert VersionedObjectType.CATALOG_ASSET in types
        assert VersionedObjectType.SEMANTIC_MODEL in types
        assert VersionedObjectType.DATA_CONTRACT in types

    def test_change_types(self):
        """Test all change types."""
        assert ChangeType.CREATE.value == "create"
        assert ChangeType.UPDATE.value == "update"
        assert ChangeType.DELETE.value == "delete"
        assert ChangeType.RESTORE.value == "restore"

    def test_version_bump_types(self):
        """Test version bump types."""
        assert VersionBump.MAJOR.value == "major"
        assert VersionBump.MINOR.value == "minor"
        assert VersionBump.PATCH.value == "patch"

    def test_version_model(self):
        """Test Version model creation."""
        version = Version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-123",
            version="1.0.0",
            version_number=1,
            change_type=ChangeType.CREATE,
            snapshot={"name": "Test Hierarchy"},
        )
        assert version.version == "1.0.0"
        assert version.object_type == VersionedObjectType.HIERARCHY
        assert version.snapshot["name"] == "Test Hierarchy"

    def test_version_query_model(self):
        """Test VersionQuery defaults."""
        query = VersionQuery()
        assert query.limit == 50
        assert query.offset == 0
        assert query.object_type is None


class TestVersionStore:
    """Test VersionStore persistence and operations."""

    def test_create_first_version(self, version_store):
        """Test creating the first version of an object."""
        snapshot = {"name": "Revenue P&L", "hierarchies": []}

        version = version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY_PROJECT,
            object_id="revenue-pl",
            snapshot=snapshot,
            change_type=ChangeType.CREATE,
            change_description="Initial creation",
            changed_by="test-user",
        )

        assert version.version == "0.0.1"
        assert version.version_number == 1
        assert version.change_type == ChangeType.CREATE
        assert version.changed_by == "test-user"

    def test_version_bumping_patch(self, version_store):
        """Test patch version bumping."""
        # Create initial version
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-1",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )

        # Patch bump
        v2 = version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-1",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
            version_bump=VersionBump.PATCH,
        )

        assert v2.version == "0.0.2"

    def test_version_bumping_minor(self, version_store):
        """Test minor version bumping."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-2",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )

        v2 = version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-2",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
            version_bump=VersionBump.MINOR,
        )

        assert v2.version == "0.1.0"

    def test_version_bumping_major(self, version_store):
        """Test major version bumping."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-3",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )

        v2 = version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-3",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
            version_bump=VersionBump.MAJOR,
        )

        assert v2.version == "1.0.0"
        assert v2.is_major is True

    def test_get_version_latest(self, version_store):
        """Test getting latest version."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-4",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-4",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
        )

        latest = version_store.get_version(
            VersionedObjectType.HIERARCHY,
            "test-4"
        )

        assert latest.snapshot["v"] == 2

    def test_get_specific_version(self, version_store):
        """Test getting a specific version."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-5",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-5",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
        )

        v1 = version_store.get_version(
            VersionedObjectType.HIERARCHY,
            "test-5",
            "0.0.1"
        )

        assert v1.snapshot["v"] == 1

    def test_list_versions(self, version_store):
        """Test listing versions (most recent first)."""
        for i in range(5):
            version_store.create_version(
                object_type=VersionedObjectType.HIERARCHY,
                object_id="test-6",
                snapshot={"v": i + 1},
                change_type=ChangeType.UPDATE,
            )

        versions = version_store.list_versions(
            VersionedObjectType.HIERARCHY,
            "test-6",
            limit=3
        )

        assert len(versions) == 3
        assert versions[0].version == "0.0.5"  # Most recent first
        assert versions[2].version == "0.0.3"

    def test_get_history(self, version_store):
        """Test getting full history."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-7",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
            object_name="Test Hierarchy",
        )

        history = version_store.get_history(
            VersionedObjectType.HIERARCHY,
            "test-7"
        )

        assert history is not None
        assert history.object_name == "Test Hierarchy"
        assert history.current_version == "0.0.1"
        assert len(history.versions) == 1

    def test_add_and_remove_tag(self, version_store):
        """Test tagging versions."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="test-8",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )

        # Add tag
        result = version_store.add_tag(
            VersionedObjectType.HIERARCHY,
            "test-8",
            "0.0.1",
            "release"
        )
        assert result is True

        v = version_store.get_version(
            VersionedObjectType.HIERARCHY,
            "test-8",
            "0.0.1"
        )
        assert "release" in v.tags

        # Remove tag
        result = version_store.remove_tag(
            VersionedObjectType.HIERARCHY,
            "test-8",
            "0.0.1",
            "release"
        )
        assert result is True

        v = version_store.get_version(
            VersionedObjectType.HIERARCHY,
            "test-8",
            "0.0.1"
        )
        assert "release" not in v.tags

    def test_search_versions(self, version_store):
        """Test searching versions."""
        # Create versions by different users
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="search-1",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
            changed_by="alice",
        )
        version_store.create_version(
            object_type=VersionedObjectType.CATALOG_ASSET,
            object_id="search-2",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
            changed_by="bob",
        )

        # Search by user
        query = VersionQuery(changed_by="alice")
        results = version_store.search_versions(query)
        assert len(results) == 1
        assert results[0].changed_by == "alice"

        # Search by object type
        query = VersionQuery(object_type=VersionedObjectType.CATALOG_ASSET)
        results = version_store.search_versions(query)
        assert len(results) == 1
        assert results[0].object_type == VersionedObjectType.CATALOG_ASSET

    def test_get_stats(self, version_store):
        """Test getting statistics."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="stats-1",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
            changed_by="user1",
        )
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="stats-1",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
            changed_by="user1",
        )

        stats = version_store.get_stats()

        assert stats.total_objects >= 1
        assert stats.total_versions >= 2
        assert "hierarchy" in stats.objects_by_type

    def test_delete_version(self, version_store):
        """Test deleting a version."""
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="delete-1",
            snapshot={"v": 1},
            change_type=ChangeType.CREATE,
        )
        version_store.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="delete-1",
            snapshot={"v": 2},
            change_type=ChangeType.UPDATE,
        )

        result = version_store.delete_version(
            VersionedObjectType.HIERARCHY,
            "delete-1",
            "0.0.1"
        )

        assert result is True

        # Version should be gone
        v = version_store.get_version(
            VersionedObjectType.HIERARCHY,
            "delete-1",
            "0.0.1"
        )
        assert v is None

    def test_persistence(self, temp_data_dir):
        """Test that data persists across store instances."""
        # Create and save
        store1 = VersionStore(data_dir=temp_data_dir)
        store1.create_version(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="persist-1",
            snapshot={"test": "data"},
            change_type=ChangeType.CREATE,
        )

        # Load in new instance
        store2 = VersionStore(data_dir=temp_data_dir)
        v = store2.get_version(
            VersionedObjectType.HIERARCHY,
            "persist-1"
        )

        assert v is not None
        assert v.snapshot["test"] == "data"


class TestVersionManager:
    """Test VersionManager high-level operations."""

    def test_snapshot(self, version_manager):
        """Test creating a snapshot."""
        version = version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY_PROJECT,
            object_id="manager-1",
            data={"name": "Test Project", "hierarchies": []},
            change_type=ChangeType.CREATE,
            description="Initial creation",
            user="test-user",
        )

        assert version.version == "0.0.1"
        assert version.change_description == "Initial creation"

    def test_diff(self, version_manager):
        """Test computing diff between versions."""
        # Create two versions
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="diff-1",
            data={"name": "Original", "count": 5},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="diff-1",
            data={"name": "Updated", "count": 10, "new_field": True},
            change_type=ChangeType.UPDATE,
        )

        diff = version_manager.diff(
            VersionedObjectType.HIERARCHY,
            "diff-1",
            "0.0.1",
            "0.0.2"
        )

        assert diff.total_changes == 3
        assert "new_field" in diff.added
        assert "name" in diff.modified
        assert "count" in diff.modified
        assert diff.modified["name"]["old"] == "Original"
        assert diff.modified["name"]["new"] == "Updated"

    def test_diff_added_removed(self, version_manager):
        """Test diff detects added and removed fields."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="diff-2",
            data={"field_a": 1, "field_b": 2},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="diff-2",
            data={"field_a": 1, "field_c": 3},
            change_type=ChangeType.UPDATE,
        )

        diff = version_manager.diff(
            VersionedObjectType.HIERARCHY,
            "diff-2",
            "0.0.1",
            "0.0.2"
        )

        assert "field_c" in diff.added
        assert "field_b" in diff.removed
        assert diff.total_changes == 2

    def test_rollback(self, version_manager):
        """Test rollback to previous version."""
        # Create versions
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="rollback-1",
            data={"state": "original"},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="rollback-1",
            data={"state": "modified"},
            change_type=ChangeType.UPDATE,
        )

        # Rollback
        snapshot = version_manager.rollback(
            VersionedObjectType.HIERARCHY,
            "rollback-1",
            "0.0.1",
            user="admin"
        )

        assert snapshot["state"] == "original"

        # Check a new version was created
        latest = version_manager.get_latest(
            VersionedObjectType.HIERARCHY,
            "rollback-1"
        )
        assert latest.change_type == ChangeType.RESTORE
        assert latest.version == "0.1.0"  # Minor bump for rollback

    def test_preview_rollback(self, version_manager):
        """Test previewing a rollback."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="preview-1",
            data={"value": 100},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="preview-1",
            data={"value": 200},
            change_type=ChangeType.UPDATE,
        )

        preview = version_manager.preview_rollback(
            VersionedObjectType.HIERARCHY,
            "preview-1",
            "0.0.1"
        )

        assert preview.current_version == "0.0.2"
        assert preview.target_version == "0.0.1"
        assert preview.snapshot["value"] == 100
        assert preview.diff is not None

    def test_get_history(self, version_manager):
        """Test getting version history."""
        for i in range(3):
            version_manager.snapshot(
                object_type=VersionedObjectType.HIERARCHY,
                object_id="history-1",
                data={"iteration": i + 1},
                change_type=ChangeType.UPDATE if i > 0 else ChangeType.CREATE,
            )

        history = version_manager.get_history(
            VersionedObjectType.HIERARCHY,
            "history-1"
        )

        assert len(history) == 3
        assert history[0].version == "0.0.3"  # Most recent first

    def test_tag_and_untag(self, version_manager):
        """Test tagging versions through manager."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="tag-1",
            data={"v": 1},
            change_type=ChangeType.CREATE,
        )

        # Add tag
        result = version_manager.tag_version(
            VersionedObjectType.HIERARCHY,
            "tag-1",
            "0.0.1",
            "production"
        )
        assert result is True

        v = version_manager.get_version(
            VersionedObjectType.HIERARCHY,
            "tag-1",
            "0.0.1"
        )
        assert "production" in v.tags

        # Remove tag
        result = version_manager.untag_version(
            VersionedObjectType.HIERARCHY,
            "tag-1",
            "0.0.1",
            "production"
        )
        assert result is True

    def test_search(self, version_manager):
        """Test searching versions."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="search-mgr-1",
            data={"v": 1},
            change_type=ChangeType.CREATE,
            user="alice",
        )

        query = VersionQuery(
            object_type=VersionedObjectType.HIERARCHY,
            changed_by="alice"
        )
        results = version_manager.search(query)

        assert len(results) >= 1

    def test_get_stats(self, version_manager):
        """Test getting statistics through manager."""
        version_manager.snapshot(
            object_type=VersionedObjectType.CATALOG_ASSET,
            object_id="stats-mgr-1",
            data={"name": "Test Asset"},
            change_type=ChangeType.CREATE,
        )

        stats = version_manager.get_stats()

        assert stats.total_objects >= 1
        assert "catalog_asset" in stats.objects_by_type


class TestMCPToolsIntegration:
    """
    Test MCP tool registration and functionality.

    Note: MCP tool functions are nested inside register_versioning_tools()
    and cannot be imported directly. These tests verify the registration
    process and test the underlying manager functionality that the tools use.
    """

    def test_tools_register_without_error(self):
        """Test that tools can be registered with a mock MCP."""
        from unittest.mock import MagicMock
        from src.versioning.mcp_tools import register_versioning_tools

        mock_mcp = MagicMock()
        mock_mcp.tool = MagicMock(return_value=lambda f: f)

        # Should not raise
        register_versioning_tools(mock_mcp)

        # Verify tool decorator was called multiple times (12 tools)
        assert mock_mcp.tool.call_count == 12

    def test_version_create_via_manager(self, version_manager):
        """Test version creation that MCP tool would use."""
        version = version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-test-1",
            data={"name": "Test"},
            change_type=ChangeType.CREATE,
            description="Test creation",
            user="test",
        )

        assert version.version == "0.0.1"
        assert version.change_description == "Test creation"

    def test_version_list_via_manager(self, version_manager):
        """Test version listing that MCP tool would use."""
        for i in range(3):
            version_manager.snapshot(
                object_type=VersionedObjectType.HIERARCHY,
                object_id="mcp-list-1",
                data={"iteration": i + 1},
                change_type=ChangeType.UPDATE if i > 0 else ChangeType.CREATE,
            )

        versions = version_manager.get_history(
            VersionedObjectType.HIERARCHY,
            "mcp-list-1"
        )

        assert len(versions) == 3

    def test_version_diff_via_manager(self, version_manager):
        """Test version diff that MCP tool would use."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-diff-1",
            data={"value": 1},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-diff-1",
            data={"value": 2},
            change_type=ChangeType.UPDATE,
        )

        diff = version_manager.diff(
            VersionedObjectType.HIERARCHY,
            "mcp-diff-1",
            "0.0.1",
            "0.0.2",
        )

        assert diff.total_changes == 1
        assert "value" in diff.modified

    def test_version_rollback_via_manager(self, version_manager):
        """Test version rollback that MCP tool would use."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-rollback-1",
            data={"state": "original"},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-rollback-1",
            data={"state": "modified"},
            change_type=ChangeType.UPDATE,
        )

        snapshot = version_manager.rollback(
            VersionedObjectType.HIERARCHY,
            "mcp-rollback-1",
            "0.0.1",
            user="admin",
        )

        assert snapshot["state"] == "original"

    def test_version_tag_via_manager(self, version_manager):
        """Test version tagging that MCP tool would use."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-tag-1",
            data={},
            change_type=ChangeType.CREATE,
        )

        result = version_manager.tag_version(
            VersionedObjectType.HIERARCHY,
            "mcp-tag-1",
            "0.0.1",
            "release",
        )
        assert result is True

        result = version_manager.tag_version(
            VersionedObjectType.HIERARCHY,
            "mcp-tag-1",
            "0.0.1",
            "approved",
        )
        assert result is True

        v = version_manager.get_version(
            VersionedObjectType.HIERARCHY,
            "mcp-tag-1",
            "0.0.1"
        )
        assert "release" in v.tags
        assert "approved" in v.tags

    def test_version_search_via_manager(self, version_manager):
        """Test version search that MCP tool would use."""
        version_manager.snapshot(
            object_type=VersionedObjectType.CATALOG_ASSET,
            object_id="mcp-search-1",
            data={},
            change_type=ChangeType.CREATE,
            user="searcher",
        )

        query = VersionQuery(
            object_type=VersionedObjectType.CATALOG_ASSET,
            changed_by="searcher"
        )
        results = version_manager.search(query)

        assert len(results) >= 1

    def test_version_stats_via_manager(self, version_manager):
        """Test version stats that MCP tool would use."""
        version_manager.snapshot(
            object_type=VersionedObjectType.SEMANTIC_MODEL,
            object_id="mcp-stats-1",
            data={},
            change_type=ChangeType.CREATE,
        )

        stats = version_manager.get_stats()

        assert stats.total_objects >= 1

    def test_hierarchy_project_versioning(self, version_manager):
        """Test hierarchy project versioning that MCP tool would use."""
        project_data = {
            "name": "Test Project",
            "hierarchies": [
                {"id": "h1", "name": "Hierarchy 1"},
                {"id": "h2", "name": "Hierarchy 2"},
            ]
        }

        version = version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY_PROJECT,
            object_id="project-1",
            data=project_data,
            change_type=ChangeType.CREATE,
            description="Initial project",
            object_name="Test Project",
        )

        assert version.version == "0.0.1"

        # Verify hierarchy count from snapshot
        v = version_manager.get_latest(
            VersionedObjectType.HIERARCHY_PROJECT,
            "project-1"
        )
        assert len(v.snapshot.get("hierarchies", [])) == 2

    def test_preview_rollback_via_manager(self, version_manager):
        """Test preview rollback that MCP tool would use."""
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-preview-1",
            data={"v": 1},
            change_type=ChangeType.CREATE,
        )
        version_manager.snapshot(
            object_type=VersionedObjectType.HIERARCHY,
            object_id="mcp-preview-1",
            data={"v": 2},
            change_type=ChangeType.UPDATE,
        )

        preview = version_manager.preview_rollback(
            VersionedObjectType.HIERARCHY,
            "mcp-preview-1",
            "0.0.1",
        )

        assert preview.current_version == "0.0.2"
        assert preview.target_version == "0.0.1"
        assert preview.diff is not None


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_version_not_found(self, version_manager):
        """Test getting non-existent version."""
        v = version_manager.get_version(
            VersionedObjectType.HIERARCHY,
            "non-existent",
            "1.0.0"
        )
        assert v is None

    def test_diff_version_not_found(self, version_manager):
        """Test diff with non-existent version."""
        with pytest.raises(ValueError, match="not found"):
            version_manager.diff(
                VersionedObjectType.HIERARCHY,
                "non-existent",
                "1.0.0",
                "2.0.0"
            )

    def test_rollback_version_not_found(self, version_manager):
        """Test rollback to non-existent version."""
        with pytest.raises(ValueError, match="not found"):
            version_manager.rollback(
                VersionedObjectType.HIERARCHY,
                "non-existent",
                "1.0.0"
            )

    def test_empty_history(self, version_manager):
        """Test getting history for object with no versions."""
        history = version_manager.get_history(
            VersionedObjectType.HIERARCHY,
            "no-versions"
        )
        assert history == []

    def test_multiple_objects_same_type(self, version_manager):
        """Test versioning multiple objects of same type."""
        for i in range(3):
            version_manager.snapshot(
                object_type=VersionedObjectType.HIERARCHY,
                object_id=f"multi-{i}",
                data={"id": i},
                change_type=ChangeType.CREATE,
            )

        objects = version_manager.list_objects(VersionedObjectType.HIERARCHY)
        multi_objects = [o for o in objects if o["object_id"].startswith("multi-")]
        assert len(multi_objects) == 3

    def test_large_snapshot_storage(self, temp_data_dir):
        """Test that large snapshots are stored separately."""
        store = VersionStore(data_dir=temp_data_dir)

        # Create a large snapshot (>10KB)
        large_data = {"data": "x" * 15000}

        version = store.create_version(
            object_type=VersionedObjectType.HIERARCHY_PROJECT,
            object_id="large-1",
            snapshot=large_data,
            change_type=ChangeType.CREATE,
        )

        # Check snapshot file was created
        snapshot_dir = Path(temp_data_dir) / "snapshots" / "hierarchy_project" / "large-1"
        assert snapshot_dir.exists()

        # Verify we can retrieve it
        retrieved = store.get_version(
            VersionedObjectType.HIERARCHY_PROJECT,
            "large-1"
        )
        assert len(retrieved.snapshot["data"]) == 15000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
