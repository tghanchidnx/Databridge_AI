"""Unit tests for event models."""

import pytest
from datetime import datetime, timezone

from src.events.models import (
    EventType,
    Event,
    ProjectEvent,
    HierarchyEvent,
    MappingEvent,
    DeploymentEvent,
    CacheInvalidationEvent,
)


class TestEventType:
    """Tests for EventType enum."""

    def test_project_event_types(self):
        """Test project event types exist."""
        assert EventType.PROJECT_CREATED.value == "project:created"
        assert EventType.PROJECT_UPDATED.value == "project:updated"
        assert EventType.PROJECT_DELETED.value == "project:deleted"

    def test_hierarchy_event_types(self):
        """Test hierarchy event types exist."""
        assert EventType.HIERARCHY_CREATED.value == "hierarchy:created"
        assert EventType.HIERARCHY_UPDATED.value == "hierarchy:updated"
        assert EventType.HIERARCHY_DELETED.value == "hierarchy:deleted"
        assert EventType.HIERARCHY_MOVED.value == "hierarchy:moved"

    def test_mapping_event_types(self):
        """Test mapping event types exist."""
        assert EventType.MAPPING_ADDED.value == "mapping:added"
        assert EventType.MAPPING_REMOVED.value == "mapping:removed"
        assert EventType.MAPPING_UPDATED.value == "mapping:updated"

    def test_deployment_event_types(self):
        """Test deployment event types exist."""
        assert EventType.DEPLOYMENT_STARTED.value == "deployment:started"
        assert EventType.DEPLOYMENT_COMPLETED.value == "deployment:completed"
        assert EventType.DEPLOYMENT_FAILED.value == "deployment:failed"
        assert EventType.DEPLOYMENT_ROLLED_BACK.value == "deployment:rolled_back"

    def test_cache_event_types(self):
        """Test cache event types exist."""
        assert EventType.CACHE_INVALIDATE.value == "cache:invalidate"
        assert EventType.CACHE_INVALIDATE_ALL.value == "cache:invalidate_all"

    def test_sync_event_types(self):
        """Test sync event types exist."""
        assert EventType.SYNC_REQUESTED.value == "sync:requested"
        assert EventType.SYNC_COMPLETED.value == "sync:completed"


class TestEvent:
    """Tests for base Event class."""

    def test_event_creation_defaults(self):
        """Test event creation with defaults."""
        event = Event(event_type=EventType.PROJECT_CREATED)

        assert event.event_type == EventType.PROJECT_CREATED
        assert event.source == "librarian"
        assert event.event_id is not None
        assert event.timestamp is not None
        assert event.metadata == {}

    def test_event_creation_with_values(self):
        """Test event creation with custom values."""
        event = Event(
            event_type=EventType.PROJECT_UPDATED,
            source="test",
            metadata={"key": "value"},
        )

        assert event.event_type == EventType.PROJECT_UPDATED
        assert event.source == "test"
        assert event.metadata == {"key": "value"}

    def test_event_to_dict(self):
        """Test event serialization to dict."""
        event = Event(event_type=EventType.PROJECT_CREATED)
        data = event.to_dict()

        assert data["event_type"] == "project:created"
        assert data["source"] == "librarian"
        assert "event_id" in data
        assert "timestamp" in data
        assert "metadata" in data

    def test_event_from_dict(self):
        """Test event deserialization from dict."""
        data = {
            "event_id": "test-id",
            "event_type": "project:created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test",
            "metadata": {"key": "value"},
        }

        event = Event.from_dict(data)

        assert event.event_id == "test-id"
        assert event.event_type == EventType.PROJECT_CREATED
        assert event.source == "test"
        assert event.metadata == {"key": "value"}


class TestProjectEvent:
    """Tests for ProjectEvent class."""

    def test_project_event_creation(self):
        """Test project event creation."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="proj-123",
            project_name="Test Project",
            action="created",
        )

        assert event.project_id == "proj-123"
        assert event.project_name == "Test Project"
        assert event.action == "created"

    def test_project_event_to_dict(self):
        """Test project event serialization."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="proj-123",
            project_name="Test Project",
            action="created",
        )
        data = event.to_dict()

        assert data["project_id"] == "proj-123"
        assert data["project_name"] == "Test Project"
        assert data["action"] == "created"
        assert data["event_type"] == "project:created"


class TestHierarchyEvent:
    """Tests for HierarchyEvent class."""

    def test_hierarchy_event_creation(self):
        """Test hierarchy event creation."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_CREATED,
            hierarchy_id="hier-123",
            hierarchy_name="Revenue",
            project_id="proj-123",
            parent_id="parent-456",
            action="created",
        )

        assert event.hierarchy_id == "hier-123"
        assert event.hierarchy_name == "Revenue"
        assert event.project_id == "proj-123"
        assert event.parent_id == "parent-456"
        assert event.action == "created"

    def test_hierarchy_event_with_changes(self):
        """Test hierarchy event with changes."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_MOVED,
            hierarchy_id="hier-123",
            hierarchy_name="Revenue",
            project_id="proj-123",
            action="moved",
            changes={
                "old_parent_id": "old-parent",
                "new_parent_id": "new-parent",
            },
        )

        assert event.changes["old_parent_id"] == "old-parent"
        assert event.changes["new_parent_id"] == "new-parent"

    def test_hierarchy_event_to_dict(self):
        """Test hierarchy event serialization."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_CREATED,
            hierarchy_id="hier-123",
            hierarchy_name="Revenue",
            project_id="proj-123",
            action="created",
        )
        data = event.to_dict()

        assert data["hierarchy_id"] == "hier-123"
        assert data["hierarchy_name"] == "Revenue"
        assert data["project_id"] == "proj-123"


class TestMappingEvent:
    """Tests for MappingEvent class."""

    def test_mapping_event_creation(self):
        """Test mapping event creation."""
        event = MappingEvent(
            event_type=EventType.MAPPING_ADDED,
            hierarchy_id="hier-123",
            project_id="proj-123",
            mapping_index=0,
            action="added",
            mapping_data={"source_table": "GL_ACCOUNTS"},
        )

        assert event.hierarchy_id == "hier-123"
        assert event.project_id == "proj-123"
        assert event.mapping_index == 0
        assert event.action == "added"
        assert event.mapping_data["source_table"] == "GL_ACCOUNTS"

    def test_mapping_event_to_dict(self):
        """Test mapping event serialization."""
        event = MappingEvent(
            event_type=EventType.MAPPING_ADDED,
            hierarchy_id="hier-123",
            project_id="proj-123",
            mapping_index=0,
            action="added",
        )
        data = event.to_dict()

        assert data["hierarchy_id"] == "hier-123"
        assert data["mapping_index"] == 0


class TestDeploymentEvent:
    """Tests for DeploymentEvent class."""

    def test_deployment_event_creation(self):
        """Test deployment event creation."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_STARTED,
            deployment_id="dep-123",
            project_id="proj-123",
            target_database="WAREHOUSE",
            target_schema="FINANCE",
            status="started",
        )

        assert event.deployment_id == "dep-123"
        assert event.target_database == "WAREHOUSE"
        assert event.target_schema == "FINANCE"
        assert event.status == "started"

    def test_deployment_event_with_error(self):
        """Test deployment event with error."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_FAILED,
            deployment_id="dep-123",
            project_id="proj-123",
            target_database="WAREHOUSE",
            target_schema="FINANCE",
            status="failed",
            scripts_executed=5,
            error_message="Connection refused",
        )

        assert event.status == "failed"
        assert event.scripts_executed == 5
        assert event.error_message == "Connection refused"

    def test_deployment_event_to_dict(self):
        """Test deployment event serialization."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_COMPLETED,
            deployment_id="dep-123",
            project_id="proj-123",
            target_database="WAREHOUSE",
            target_schema="FINANCE",
            status="completed",
            scripts_executed=10,
        )
        data = event.to_dict()

        assert data["deployment_id"] == "dep-123"
        assert data["scripts_executed"] == 10


class TestCacheInvalidationEvent:
    """Tests for CacheInvalidationEvent class."""

    def test_cache_event_creation(self):
        """Test cache invalidation event creation."""
        event = CacheInvalidationEvent(
            event_type=EventType.CACHE_INVALIDATE,
            cache_keys=["key1", "key2"],
            invalidate_all=False,
            reason="Hierarchy updated",
        )

        assert event.cache_keys == ["key1", "key2"]
        assert event.invalidate_all is False
        assert event.reason == "Hierarchy updated"

    def test_cache_event_invalidate_all(self):
        """Test cache invalidate all event."""
        event = CacheInvalidationEvent(
            event_type=EventType.CACHE_INVALIDATE_ALL,
            cache_keys=[],
            invalidate_all=True,
            reason="Full refresh",
        )

        assert event.invalidate_all is True
        assert event.cache_keys == []

    def test_cache_event_to_dict(self):
        """Test cache event serialization."""
        event = CacheInvalidationEvent(
            event_type=EventType.CACHE_INVALIDATE,
            cache_keys=["key1"],
            invalidate_all=False,
        )
        data = event.to_dict()

        assert data["cache_keys"] == ["key1"]
        assert data["invalidate_all"] is False
