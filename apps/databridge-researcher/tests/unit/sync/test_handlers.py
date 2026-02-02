"""Unit tests for sync handlers."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock

from src.sync.handlers import (
    SyncEvent,
    HandlerResult,
    SyncHandler,
    HierarchySyncHandler,
    DimensionSyncHandler,
    CacheInvalidationHandler,
)


class TestSyncEvent:
    """Tests for SyncEvent dataclass."""

    def test_sync_event_from_dict(self):
        """Test creating SyncEvent from dictionary."""
        data = {
            "event_id": "evt-123",
            "event_type": "project:created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "librarian",
            "project_id": "proj-123",
        }

        event = SyncEvent.from_dict(data)

        assert event.event_id == "evt-123"
        assert event.event_type == "project:created"
        assert event.source == "librarian"

    def test_sync_event_from_dict_defaults(self):
        """Test SyncEvent from dict with missing fields."""
        data = {"event_type": "project:created"}

        event = SyncEvent.from_dict(data)

        assert event.event_id == ""
        assert event.source == "librarian"
        assert event.timestamp is not None


class TestHandlerResult:
    """Tests for HandlerResult dataclass."""

    def test_handler_result_creation(self):
        """Test handler result creation."""
        result = HandlerResult(
            success=True,
            message="Operation completed",
            changes_applied=5,
        )

        assert result.success is True
        assert result.message == "Operation completed"
        assert result.changes_applied == 5

    def test_handler_result_defaults(self):
        """Test handler result default values."""
        result = HandlerResult(success=True)

        assert result.message == ""
        assert result.changes_applied == 0
        assert result.errors == []

    def test_handler_result_to_dict(self):
        """Test handler result serialization."""
        result = HandlerResult(
            success=False,
            message="Failed",
            changes_applied=0,
            errors=["Error 1", "Error 2"],
        )
        data = result.to_dict()

        assert data["success"] is False
        assert data["message"] == "Failed"
        assert data["changes_applied"] == 0
        assert len(data["errors"]) == 2


class TestHierarchySyncHandler:
    """Tests for HierarchySyncHandler class."""

    def test_handler_initialization(self):
        """Test handler initializes correctly."""
        handler = HierarchySyncHandler()

        assert handler._hierarchy_cache == {}
        assert handler._project_cache == {}

    def test_handled_events(self):
        """Test handler declares correct event types."""
        handler = HierarchySyncHandler()

        assert "project:created" in handler.handled_events
        assert "project:updated" in handler.handled_events
        assert "project:deleted" in handler.handled_events
        assert "hierarchy:created" in handler.handled_events
        assert "hierarchy:updated" in handler.handled_events
        assert "hierarchy:deleted" in handler.handled_events
        assert "hierarchy:moved" in handler.handled_events

    def test_can_handle_matching_event(self):
        """Test can_handle returns True for matching event."""
        handler = HierarchySyncHandler()

        assert handler.can_handle("project:created") is True
        assert handler.can_handle("hierarchy:updated") is True

    def test_can_handle_non_matching_event(self):
        """Test can_handle returns False for non-matching event."""
        handler = HierarchySyncHandler()

        assert handler.can_handle("mapping:added") is False
        assert handler.can_handle("unknown:event") is False

    def test_handle_project_created(self):
        """Test handling project created event."""
        handler = HierarchySyncHandler()

        event = SyncEvent.from_dict({
            "event_type": "project:created",
            "project_id": "proj-123",
            "project_name": "Test Project",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert "proj-123" in handler._project_cache
        assert handler._project_cache["proj-123"]["name"] == "Test Project"

    def test_handle_project_updated(self):
        """Test handling project updated event."""
        handler = HierarchySyncHandler()

        # First create the project
        create_event = SyncEvent.from_dict({
            "event_type": "project:created",
            "project_id": "proj-123",
            "project_name": "Test Project",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        handler.handle(create_event)

        # Then update it
        update_event = SyncEvent.from_dict({
            "event_type": "project:updated",
            "project_id": "proj-123",
            "project_name": "Updated Project",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(update_event)

        assert result.success is True
        assert handler._project_cache["proj-123"]["name"] == "Updated Project"

    def test_handle_project_deleted(self):
        """Test handling project deleted event."""
        handler = HierarchySyncHandler()

        # First create project and hierarchy
        handler._project_cache["proj-123"] = {"id": "proj-123", "name": "Test"}
        handler._hierarchy_cache["hier-1"] = {"id": "hier-1", "project_id": "proj-123"}
        handler._hierarchy_cache["hier-2"] = {"id": "hier-2", "project_id": "proj-123"}

        event = SyncEvent.from_dict({
            "event_type": "project:deleted",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert "proj-123" not in handler._project_cache
        assert "hier-1" not in handler._hierarchy_cache
        assert "hier-2" not in handler._hierarchy_cache

    def test_handle_hierarchy_created(self):
        """Test handling hierarchy created event."""
        handler = HierarchySyncHandler()

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:created",
            "hierarchy_id": "hier-123",
            "hierarchy_name": "Revenue",
            "project_id": "proj-123",
            "parent_id": "parent-456",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert "hier-123" in handler._hierarchy_cache
        assert handler._hierarchy_cache["hier-123"]["name"] == "Revenue"
        assert handler._hierarchy_cache["hier-123"]["parent_id"] == "parent-456"

    def test_handle_hierarchy_updated(self):
        """Test handling hierarchy updated event."""
        handler = HierarchySyncHandler()

        # First create
        handler._hierarchy_cache["hier-123"] = {
            "id": "hier-123",
            "name": "Revenue",
            "project_id": "proj-123",
        }

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:updated",
            "hierarchy_id": "hier-123",
            "hierarchy_name": "Updated Revenue",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert handler._hierarchy_cache["hier-123"]["name"] == "Updated Revenue"

    def test_handle_hierarchy_updated_not_in_cache(self):
        """Test handling hierarchy updated for non-cached hierarchy."""
        handler = HierarchySyncHandler()

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:updated",
            "hierarchy_id": "hier-123",
            "hierarchy_name": "Revenue",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert "hier-123" in handler._hierarchy_cache

    def test_handle_hierarchy_deleted(self):
        """Test handling hierarchy deleted event."""
        handler = HierarchySyncHandler()

        handler._hierarchy_cache["hier-123"] = {"id": "hier-123", "name": "Revenue"}

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:deleted",
            "hierarchy_id": "hier-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert "hier-123" not in handler._hierarchy_cache

    def test_handle_hierarchy_moved(self):
        """Test handling hierarchy moved event."""
        handler = HierarchySyncHandler()

        handler._hierarchy_cache["hier-123"] = {
            "id": "hier-123",
            "parent_id": "old-parent",
        }

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:moved",
            "hierarchy_id": "hier-123",
            "changes": {"new_parent_id": "new-parent"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert handler._hierarchy_cache["hier-123"]["parent_id"] == "new-parent"

    def test_get_cached_projects(self):
        """Test getting cached projects."""
        handler = HierarchySyncHandler()

        handler._project_cache["proj-1"] = {"id": "proj-1", "name": "Project 1"}
        handler._project_cache["proj-2"] = {"id": "proj-2", "name": "Project 2"}

        projects = handler.get_cached_projects()

        assert len(projects) == 2
        assert "proj-1" in projects
        assert "proj-2" in projects

    def test_get_cached_hierarchies_all(self):
        """Test getting all cached hierarchies."""
        handler = HierarchySyncHandler()

        handler._hierarchy_cache["hier-1"] = {"id": "hier-1", "project_id": "proj-1"}
        handler._hierarchy_cache["hier-2"] = {"id": "hier-2", "project_id": "proj-2"}

        hierarchies = handler.get_cached_hierarchies()

        assert len(hierarchies) == 2

    def test_get_cached_hierarchies_filtered(self):
        """Test getting cached hierarchies filtered by project."""
        handler = HierarchySyncHandler()

        handler._hierarchy_cache["hier-1"] = {"id": "hier-1", "project_id": "proj-1"}
        handler._hierarchy_cache["hier-2"] = {"id": "hier-2", "project_id": "proj-2"}

        hierarchies = handler.get_cached_hierarchies(project_id="proj-1")

        assert len(hierarchies) == 1
        assert "hier-1" in hierarchies


class TestDimensionSyncHandler:
    """Tests for DimensionSyncHandler class."""

    def test_handler_initialization(self):
        """Test handler initializes correctly."""
        handler = DimensionSyncHandler()

        assert handler._mapper is None
        assert handler._pending_updates == []

    def test_handled_events_patterns(self):
        """Test handler uses pattern-based events."""
        handler = DimensionSyncHandler()

        assert "hierarchy:*" in handler.handled_events
        assert "mapping:*" in handler.handled_events

    def test_can_handle_pattern_matching(self):
        """Test can_handle with patterns."""
        handler = DimensionSyncHandler()

        assert handler.can_handle("hierarchy:created") is True
        assert handler.can_handle("hierarchy:updated") is True
        assert handler.can_handle("mapping:added") is True
        assert handler.can_handle("project:created") is False

    def test_handle_adds_to_pending(self):
        """Test handling event adds project to pending."""
        handler = DimensionSyncHandler()

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:created",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        assert "proj-123" in handler._pending_updates

    def test_handle_no_duplicate_pending(self):
        """Test duplicate project IDs not added to pending."""
        handler = DimensionSyncHandler()

        event = SyncEvent.from_dict({
            "event_type": "hierarchy:created",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        handler.handle(event)
        handler.handle(event)

        assert handler._pending_updates.count("proj-123") == 1

    def test_get_pending_projects(self):
        """Test getting pending projects."""
        handler = DimensionSyncHandler()

        handler._pending_updates = ["proj-1", "proj-2"]

        pending = handler.get_pending_projects()

        assert len(pending) == 2
        assert "proj-1" in pending
        assert "proj-2" in pending

    def test_process_pending_no_mapper(self):
        """Test processing pending without mapper."""
        handler = DimensionSyncHandler()
        handler._pending_updates = ["proj-1"]

        results = handler.process_pending()

        assert len(results) == 1
        assert results[0].success is False
        assert "No dimension mapper" in results[0].message

    def test_process_pending_clears_list(self):
        """Test process_pending clears pending list."""
        handler = DimensionSyncHandler()
        handler._pending_updates = ["proj-1", "proj-2"]

        handler.process_pending()

        assert len(handler._pending_updates) == 0


class TestCacheInvalidationHandler:
    """Tests for CacheInvalidationHandler class."""

    def test_handler_initialization(self):
        """Test handler initializes correctly."""
        handler = CacheInvalidationHandler()

        assert handler._invalidation_callbacks == []

    def test_handled_events(self):
        """Test handler declares correct events."""
        handler = CacheInvalidationHandler()

        assert "cache:invalidate" in handler.handled_events
        assert "cache:invalidate_all" in handler.handled_events

    def test_register_callback(self):
        """Test registering a cache callback."""
        handler = CacheInvalidationHandler()

        callback = Mock()
        handler.register_cache_callback(callback)

        assert callback in handler._invalidation_callbacks

    def test_handle_invalidate_calls_callbacks(self):
        """Test handling invalidate calls registered callbacks."""
        handler = CacheInvalidationHandler()

        callback = Mock()
        handler.register_cache_callback(callback)

        event = SyncEvent.from_dict({
            "event_type": "cache:invalidate",
            "cache_keys": ["key1", "key2"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        callback.assert_called_once_with(["key1", "key2"])

    def test_handle_invalidate_all(self):
        """Test handling invalidate all event."""
        handler = CacheInvalidationHandler()

        callback = Mock()
        handler.register_cache_callback(callback)

        event = SyncEvent.from_dict({
            "event_type": "cache:invalidate_all",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is True
        callback.assert_called_once_with(["*"])

    def test_handle_callback_error(self):
        """Test handling when callback raises error."""
        handler = CacheInvalidationHandler()

        def failing_callback(keys):
            raise RuntimeError("Callback failed")

        handler.register_cache_callback(failing_callback)

        event = SyncEvent.from_dict({
            "event_type": "cache:invalidate",
            "cache_keys": ["key1"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        result = handler.handle(event)

        assert result.success is False
        assert "Callback failed" in result.errors[0]
