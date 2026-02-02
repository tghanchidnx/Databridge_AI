"""Unit tests for event publisher."""

import pytest
from unittest.mock import Mock, patch

from src.events.models import EventType, ProjectEvent, HierarchyEvent
from src.events.publisher import EventPublisher, get_publisher, reset_publisher
from src.events.bus import EventBus, reset_event_bus


class TestEventPublisher:
    """Tests for EventPublisher class."""

    def setup_method(self):
        """Reset state before each test."""
        reset_event_bus()
        reset_publisher()

    def teardown_method(self):
        """Clean up after each test."""
        reset_event_bus()
        reset_publisher()

    def test_publisher_initialization(self):
        """Test publisher initializes with event bus."""
        publisher = EventPublisher()

        assert publisher._bus is not None

    def test_publisher_with_custom_bus(self):
        """Test publisher with custom event bus."""
        bus = EventBus()
        publisher = EventPublisher(event_bus=bus)

        assert publisher._bus is bus

    def test_project_created_event(self):
        """Test publishing project created event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.PROJECT_CREATED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.project_created(
            project_id="proj-123",
            project_name="Test Project",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.project_id == "proj-123"
        assert event.project_name == "Test Project"
        assert event.action == "created"

    def test_project_created_with_metadata(self):
        """Test project created with metadata."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.PROJECT_CREATED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.project_created(
            project_id="proj-123",
            project_name="Test",
            metadata={"key": "value"},
        )

        event = handler.call_args[0][0]
        assert event.metadata == {"key": "value"}

    def test_project_updated_event(self):
        """Test publishing project updated event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.PROJECT_UPDATED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.project_updated(
            project_id="proj-123",
            project_name="Updated Project",
            changes={"name": "Updated"},
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.project_id == "proj-123"
        assert event.action == "updated"

    def test_project_deleted_event(self):
        """Test publishing project deleted event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.PROJECT_DELETED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.project_deleted(
            project_id="proj-123",
            project_name="Test",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.action == "deleted"

    def test_hierarchy_created_event(self):
        """Test publishing hierarchy created event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.HIERARCHY_CREATED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.hierarchy_created(
            hierarchy_id="hier-123",
            hierarchy_name="Revenue",
            project_id="proj-123",
            parent_id="parent-456",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.hierarchy_id == "hier-123"
        assert event.hierarchy_name == "Revenue"
        assert event.project_id == "proj-123"
        assert event.parent_id == "parent-456"

    def test_hierarchy_updated_event(self):
        """Test publishing hierarchy updated event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.HIERARCHY_UPDATED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.hierarchy_updated(
            hierarchy_id="hier-123",
            hierarchy_name="Updated Revenue",
            project_id="proj-123",
            changes={"name": "Updated Revenue"},
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.changes == {"name": "Updated Revenue"}

    def test_hierarchy_deleted_event(self):
        """Test publishing hierarchy deleted event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.HIERARCHY_DELETED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.hierarchy_deleted(
            hierarchy_id="hier-123",
            hierarchy_name="Revenue",
            project_id="proj-123",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.action == "deleted"

    def test_hierarchy_moved_event(self):
        """Test publishing hierarchy moved event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.HIERARCHY_MOVED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.hierarchy_moved(
            hierarchy_id="hier-123",
            hierarchy_name="Revenue",
            project_id="proj-123",
            old_parent_id="old-parent",
            new_parent_id="new-parent",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.parent_id == "new-parent"
        assert event.changes["old_parent_id"] == "old-parent"
        assert event.changes["new_parent_id"] == "new-parent"

    def test_mapping_added_event(self):
        """Test publishing mapping added event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.MAPPING_ADDED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.mapping_added(
            hierarchy_id="hier-123",
            project_id="proj-123",
            mapping_index=0,
            mapping_data={"source_table": "GL_ACCOUNTS"},
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.mapping_index == 0
        assert event.mapping_data["source_table"] == "GL_ACCOUNTS"

    def test_mapping_removed_event(self):
        """Test publishing mapping removed event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.MAPPING_REMOVED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.mapping_removed(
            hierarchy_id="hier-123",
            project_id="proj-123",
            mapping_index=0,
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.action == "removed"

    def test_deployment_started_event(self):
        """Test publishing deployment started event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.DEPLOYMENT_STARTED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.deployment_started(
            deployment_id="dep-123",
            project_id="proj-123",
            target_database="WAREHOUSE",
            target_schema="FINANCE",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.deployment_id == "dep-123"
        assert event.target_database == "WAREHOUSE"
        assert event.status == "started"

    def test_deployment_completed_event(self):
        """Test publishing deployment completed event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.DEPLOYMENT_COMPLETED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.deployment_completed(
            deployment_id="dep-123",
            project_id="proj-123",
            target_database="WAREHOUSE",
            target_schema="FINANCE",
            scripts_executed=10,
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.status == "completed"
        assert event.scripts_executed == 10

    def test_deployment_failed_event(self):
        """Test publishing deployment failed event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.DEPLOYMENT_FAILED, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.deployment_failed(
            deployment_id="dep-123",
            project_id="proj-123",
            target_database="WAREHOUSE",
            target_schema="FINANCE",
            error_message="Connection refused",
            scripts_executed=5,
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.status == "failed"
        assert event.error_message == "Connection refused"
        assert event.scripts_executed == 5

    def test_invalidate_cache_event(self):
        """Test publishing cache invalidation event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.CACHE_INVALIDATE, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.invalidate_cache(
            cache_keys=["key1", "key2"],
            reason="Hierarchy updated",
        )

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.cache_keys == ["key1", "key2"]
        assert event.invalidate_all is False

    def test_invalidate_all_cache_event(self):
        """Test publishing invalidate all cache event."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.CACHE_INVALIDATE_ALL, handler)

        publisher = EventPublisher(event_bus=bus)
        publisher.invalidate_all_cache(reason="Full refresh")

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.invalidate_all is True


class TestGlobalPublisher:
    """Tests for global publisher functions."""

    def setup_method(self):
        """Reset state before each test."""
        reset_event_bus()
        reset_publisher()

    def teardown_method(self):
        """Clean up after each test."""
        reset_event_bus()
        reset_publisher()

    def test_get_publisher_creates_instance(self):
        """Test get_publisher creates singleton."""
        publisher = get_publisher()

        assert publisher is not None
        assert isinstance(publisher, EventPublisher)

    def test_get_publisher_returns_same_instance(self):
        """Test get_publisher returns same instance."""
        pub1 = get_publisher()
        pub2 = get_publisher()

        assert pub1 is pub2

    def test_reset_publisher(self):
        """Test reset_publisher creates new instance."""
        pub1 = get_publisher()
        reset_publisher()
        pub2 = get_publisher()

        assert pub1 is not pub2
