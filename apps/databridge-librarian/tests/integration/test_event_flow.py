"""Integration tests for event flow.

Tests the complete event publishing → bus → handler flow.
"""

import pytest
from datetime import datetime

from src.events.models import (
    Event,
    EventType,
    ProjectEvent,
    HierarchyEvent,
    MappingEvent,
    DeploymentEvent,
    CacheInvalidationEvent,
)
from src.events.bus import EventBus, get_event_bus, reset_event_bus
from src.events.publisher import EventPublisher, get_publisher, reset_publisher


class TestEventPublishingFlow:
    """Integration tests for event publishing flow."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_event_bus()
        reset_publisher()

    def test_project_event_flow(self):
        """Test complete project event flow."""
        bus = get_event_bus()
        publisher = get_publisher()

        received_events = []

        # Subscribe to project events using pattern
        bus.subscribe_pattern("project:*", received_events.append)

        # Publish project created event
        publisher.project_created(
            project_id="test-proj-1",
            project_name="Test Project",
        )

        # Event should have been received
        assert len(received_events) == 1
        assert received_events[0].project_id == "test-proj-1"

    def test_hierarchy_event_cascade(self):
        """Test hierarchy events with project relationship."""
        bus = get_event_bus()
        publisher = get_publisher()

        hierarchy_events = []
        project_events = []

        bus.subscribe_pattern("hierarchy:*", hierarchy_events.append)
        bus.subscribe_pattern("project:*", project_events.append)

        # Create project first
        publisher.project_created(
            project_id="cascade-proj",
            project_name="Cascade Project",
        )

        # Create hierarchy under project
        publisher.hierarchy_created(
            hierarchy_id="hier-1",
            hierarchy_name="Test Hierarchy",
            project_id="cascade-proj",
        )

        # Update hierarchy
        publisher.hierarchy_updated(
            hierarchy_id="hier-1",
            hierarchy_name="Updated Hierarchy",
            project_id="cascade-proj",
            changes={"name": "Updated Hierarchy"},
        )

        assert len(project_events) == 1
        assert len(hierarchy_events) == 2

    def test_mapping_event_flow(self):
        """Test mapping events."""
        bus = get_event_bus()
        publisher = get_publisher()

        mapping_events = []

        bus.subscribe_pattern("mapping:*", mapping_events.append)

        # Add mapping
        publisher.mapping_added(
            project_id="map-proj",
            hierarchy_id="hier-1",
            mapping_index=0,
            mapping_data={"source_table": "DIM_PRODUCT"},
        )

        # Remove mapping
        publisher.mapping_removed(
            project_id="map-proj",
            hierarchy_id="hier-1",
            mapping_index=0,
        )

        assert len(mapping_events) == 2
        assert mapping_events[0].event_type == EventType.MAPPING_ADDED
        assert mapping_events[1].event_type == EventType.MAPPING_REMOVED

    def test_deployment_event_lifecycle(self):
        """Test deployment events through complete lifecycle."""
        bus = get_event_bus()
        publisher = get_publisher()

        deployment_events = []

        bus.subscribe_pattern("deployment:*", deployment_events.append)

        project_id = "deploy-proj"
        deployment_id = "deploy-1"

        # Start deployment
        publisher.deployment_started(
            deployment_id=deployment_id,
            project_id=project_id,
            target_database="PROD_DW",
            target_schema="PUBLIC",
        )

        # Complete deployment
        publisher.deployment_completed(
            deployment_id=deployment_id,
            project_id=project_id,
            target_database="PROD_DW",
            target_schema="PUBLIC",
            scripts_executed=5,
        )

        assert len(deployment_events) == 2
        assert deployment_events[0].event_type == EventType.DEPLOYMENT_STARTED
        assert deployment_events[1].event_type == EventType.DEPLOYMENT_COMPLETED

    def test_deployment_failure_event(self):
        """Test deployment failure event."""
        bus = get_event_bus()
        publisher = get_publisher()

        failure_events = []

        def capture_failure(event: Event):
            if event.event_type == EventType.DEPLOYMENT_FAILED:
                failure_events.append(event)

        bus.subscribe_pattern("deployment:*", capture_failure)

        # Simulate failed deployment
        publisher.deployment_failed(
            deployment_id="fail-deploy",
            project_id="fail-proj",
            target_database="PROD_DW",
            target_schema="PUBLIC",
            error_message="Connection timeout",
        )

        assert len(failure_events) == 1
        assert failure_events[0].error_message == "Connection timeout"


class TestEventBusFeatures:
    """Integration tests for EventBus features."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_event_bus()

    def test_pattern_subscription(self):
        """Test pattern-based subscriptions."""
        bus = get_event_bus()

        all_events = []
        hierarchy_only = []
        specific_only = []

        bus.subscribe_pattern("*", all_events.append)
        bus.subscribe_pattern("hierarchy:*", hierarchy_only.append)
        bus.subscribe(EventType.HIERARCHY_CREATED, specific_only.append)

        # Publish various events
        bus.publish(ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="p1",
            project_name="Project 1",
        ))
        bus.publish(HierarchyEvent(
            event_type=EventType.HIERARCHY_CREATED,
            hierarchy_id="h1",
            hierarchy_name="Hierarchy 1",
            project_id="p1",
        ))
        bus.publish(HierarchyEvent(
            event_type=EventType.HIERARCHY_UPDATED,
            hierarchy_id="h1",
            hierarchy_name="Hierarchy 1 Updated",
            project_id="p1",
        ))

        # Check correct routing
        # Note: "*" pattern matches everything starting with ""
        assert len(hierarchy_only) == 2
        assert len(specific_only) == 1

    def test_event_history(self):
        """Test event history tracking."""
        bus = get_event_bus()

        # Publish some events
        for i in range(5):
            bus.publish(ProjectEvent(
                event_type=EventType.PROJECT_CREATED,
                project_id=f"p{i}",
                project_name=f"Project {i}",
            ))

        # Check history
        history = bus.get_event_history(limit=10)
        assert len(history) == 5

        # Check last event
        assert history[-1]["project_id"] == "p4"

    def test_unsubscribe(self):
        """Test unsubscribing from events."""
        bus = get_event_bus()

        events = []

        def handler(event):
            events.append(event)

        bus.subscribe(EventType.PROJECT_CREATED, handler)

        # Publish event
        bus.publish(ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="unsub-test",
            project_name="Unsub Test",
        ))
        assert len(events) == 1

        # Unsubscribe
        bus.unsubscribe(EventType.PROJECT_CREATED, handler)

        # Publish another event
        bus.publish(ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="unsub-test-2",
            project_name="Unsub Test 2",
        ))
        assert len(events) == 1  # Should not have received second event


class TestCacheInvalidation:
    """Integration tests for cache invalidation events."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_event_bus()
        reset_publisher()

    def test_single_cache_invalidation(self):
        """Test single key cache invalidation."""
        bus = get_event_bus()
        publisher = get_publisher()

        invalidations = []

        bus.subscribe_pattern("cache:*", invalidations.append)

        # Invalidate specific cache
        publisher.invalidate_cache(["project:test-1"])

        assert len(invalidations) == 1
        assert "project:test-1" in invalidations[0].cache_keys

    def test_all_cache_invalidation(self):
        """Test full cache invalidation."""
        bus = get_event_bus()
        publisher = get_publisher()

        invalidations = []

        bus.subscribe_pattern("cache:*", invalidations.append)

        # Invalidate all
        publisher.invalidate_all_cache()

        assert len(invalidations) == 1
        assert invalidations[0].invalidate_all is True


class TestEventSerialization:
    """Integration tests for event serialization."""

    def test_project_event_serialization(self):
        """Test project event serialization."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="ser-test",
            project_name="Serialization Test",
            action="created",
        )

        # Serialize
        data = event.to_dict()

        # Check structure
        assert data["event_type"] == "project:created"
        assert data["project_id"] == "ser-test"
        assert "timestamp" in data

    def test_hierarchy_event_serialization(self):
        """Test hierarchy event serialization."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_CREATED,
            project_id="hier-ser",
            hierarchy_id="h1",
            hierarchy_name="Test",
            parent_id="root",
        )

        data = event.to_dict()

        assert data["event_type"] == "hierarchy:created"
        assert data["hierarchy_id"] == "h1"
        assert data["parent_id"] == "root"

    def test_deployment_event_serialization(self):
        """Test deployment event serialization."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_COMPLETED,
            project_id="dep-ser",
            deployment_id="d1",
            scripts_executed=10,
            status="completed",
        )

        data = event.to_dict()

        assert data["event_type"] == "deployment:completed"
        assert data["scripts_executed"] == 10


class TestEventTimestamps:
    """Integration tests for event timestamps."""

    def test_timestamp_generation(self):
        """Test that timestamps are automatically generated."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="ts-test",
            project_name="Timestamp Test",
        )

        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)

    def test_timestamp_ordering(self):
        """Test that events maintain timestamp order."""
        import time

        bus = get_event_bus()

        events = []
        bus.subscribe_pattern("project:*", events.append)

        for i in range(3):
            bus.publish(ProjectEvent(
                event_type=EventType.PROJECT_CREATED,
                project_id=f"ts-order-{i}",
                project_name=f"Order Test {i}",
            ))
            time.sleep(0.01)  # Small delay

        # Check timestamps are in order
        for i in range(len(events) - 1):
            assert events[i].timestamp <= events[i + 1].timestamp
