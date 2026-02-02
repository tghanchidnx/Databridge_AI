"""Unit tests for event bus."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.events.models import Event, EventType, ProjectEvent
from src.events.bus import EventBus, get_event_bus, reset_event_bus


class TestEventBus:
    """Tests for EventBus class."""

    def setup_method(self):
        """Reset event bus before each test."""
        reset_event_bus()

    def teardown_method(self):
        """Clean up after each test."""
        reset_event_bus()

    def test_event_bus_initialization(self):
        """Test event bus initializes correctly."""
        bus = EventBus()

        assert bus._redis_client is None
        assert len(bus._handlers) == 0
        assert len(bus._event_history) == 0

    def test_subscribe_handler(self):
        """Test subscribing a handler to event type."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe(EventType.PROJECT_CREATED, handler)

        assert handler in bus._handlers["project:created"]

    def test_subscribe_prevents_duplicates(self):
        """Test duplicate handlers are not added."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe(EventType.PROJECT_CREATED, handler)
        bus.subscribe(EventType.PROJECT_CREATED, handler)

        assert len(bus._handlers["project:created"]) == 1

    def test_unsubscribe_handler(self):
        """Test unsubscribing a handler."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe(EventType.PROJECT_CREATED, handler)
        bus.unsubscribe(EventType.PROJECT_CREATED, handler)

        assert handler not in bus._handlers["project:created"]

    def test_publish_event_calls_handlers(self):
        """Test publishing an event calls subscribed handlers."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe(EventType.PROJECT_CREATED, handler)

        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="proj-123",
            project_name="Test",
            action="created",
        )
        bus.publish(event)

        handler.assert_called_once_with(event)

    def test_publish_event_records_history(self):
        """Test published events are recorded in history."""
        bus = EventBus()

        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="proj-123",
            project_name="Test",
            action="created",
        )
        bus.publish(event)

        history = bus.get_event_history()
        assert len(history) == 1
        assert history[0]["project_id"] == "proj-123"

    def test_publish_event_pattern_matching(self):
        """Test pattern-based subscriptions."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe_pattern("hierarchy:*", handler)

        event = Event(event_type=EventType.HIERARCHY_CREATED)
        bus.publish(event)

        handler.assert_called_once_with(event)

    def test_pattern_matching_project_events(self):
        """Test pattern matching for project events."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe_pattern("project:*", handler)

        event = Event(event_type=EventType.PROJECT_DELETED)
        bus.publish(event)

        handler.assert_called_once()

    def test_pattern_no_match(self):
        """Test pattern that doesn't match."""
        bus = EventBus()
        handler = Mock()

        bus.subscribe_pattern("hierarchy:*", handler)

        event = Event(event_type=EventType.PROJECT_CREATED)
        bus.publish(event)

        handler.assert_not_called()

    def test_handler_error_continues_processing(self):
        """Test that handler errors don't stop other handlers."""
        bus = EventBus()

        def failing_handler(event):
            raise ValueError("Handler failed")

        success_handler = Mock()

        bus.subscribe(EventType.PROJECT_CREATED, failing_handler)
        bus.subscribe(EventType.PROJECT_CREATED, success_handler)

        event = Event(event_type=EventType.PROJECT_CREATED)
        bus.publish(event)  # Should not raise

        success_handler.assert_called_once()

    def test_get_event_history_filtered(self):
        """Test getting filtered event history."""
        bus = EventBus()

        bus.publish(Event(event_type=EventType.PROJECT_CREATED))
        bus.publish(Event(event_type=EventType.HIERARCHY_CREATED))
        bus.publish(Event(event_type=EventType.PROJECT_UPDATED))

        history = bus.get_event_history(event_type=EventType.PROJECT_CREATED)

        assert len(history) == 1
        assert history[0]["event_type"] == "project:created"

    def test_get_event_history_limited(self):
        """Test event history respects limit."""
        bus = EventBus()

        for _ in range(10):
            bus.publish(Event(event_type=EventType.PROJECT_CREATED))

        history = bus.get_event_history(limit=5)
        assert len(history) == 5

    def test_event_history_max_size(self):
        """Test event history doesn't exceed max size."""
        bus = EventBus()
        bus._max_history = 10

        for _ in range(20):
            bus.publish(Event(event_type=EventType.PROJECT_CREATED))

        assert len(bus._event_history) == 10

    def test_get_subscriber_count(self):
        """Test getting subscriber count."""
        bus = EventBus()

        bus.subscribe(EventType.PROJECT_CREATED, Mock())
        bus.subscribe(EventType.PROJECT_CREATED, Mock())
        bus.subscribe_async(EventType.PROJECT_CREATED, Mock())

        count = bus.get_subscriber_count(EventType.PROJECT_CREATED)
        assert count == 3

    def test_clear_handlers(self):
        """Test clearing all handlers."""
        bus = EventBus()

        bus.subscribe(EventType.PROJECT_CREATED, Mock())
        bus.subscribe_pattern("hierarchy:*", Mock())

        bus.clear_handlers()

        assert len(bus._handlers) == 0
        assert len(bus._pattern_handlers) == 0

    def test_shutdown(self):
        """Test event bus shutdown."""
        bus = EventBus()
        bus.shutdown()  # Should not raise

    def test_redis_initialization_without_redis(self):
        """Test Redis initialization gracefully handles missing redis module."""
        # When redis is not installed or unavailable, bus should still work
        bus = EventBus()  # No redis_url means no redis init
        assert bus._redis_client is None

    def test_publish_without_redis(self):
        """Test publishing events works without Redis."""
        bus = EventBus()
        handler = Mock()
        bus.subscribe(EventType.PROJECT_CREATED, handler)

        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id="proj-123",
            project_name="Test",
            action="created",
        )
        bus.publish(event)

        # Event should still be published to local handlers
        handler.assert_called_once()
        assert bus._redis_client is None


class TestGlobalEventBus:
    """Tests for global event bus functions."""

    def setup_method(self):
        """Reset event bus before each test."""
        reset_event_bus()

    def teardown_method(self):
        """Clean up after each test."""
        reset_event_bus()

    def test_get_event_bus_creates_instance(self):
        """Test get_event_bus creates singleton."""
        bus = get_event_bus()

        assert bus is not None
        assert isinstance(bus, EventBus)

    def test_get_event_bus_returns_same_instance(self):
        """Test get_event_bus returns same instance."""
        bus1 = get_event_bus()
        bus2 = get_event_bus()

        assert bus1 is bus2

    def test_reset_event_bus(self):
        """Test reset_event_bus creates new instance."""
        bus1 = get_event_bus()
        reset_event_bus()
        bus2 = get_event_bus()

        assert bus1 is not bus2


class TestAsyncEventHandling:
    """Tests for async event handling."""

    def setup_method(self):
        """Reset event bus before each test."""
        reset_event_bus()

    def teardown_method(self):
        """Clean up after each test."""
        reset_event_bus()

    def test_subscribe_async_handler(self):
        """Test subscribing async handler."""
        bus = EventBus()

        async def async_handler(event):
            pass

        bus.subscribe_async(EventType.PROJECT_CREATED, async_handler)

        assert async_handler in bus._async_handlers["project:created"]

    def test_async_handler_prevents_duplicates(self):
        """Test duplicate async handlers are not added."""
        bus = EventBus()

        async def async_handler(event):
            pass

        bus.subscribe_async(EventType.PROJECT_CREATED, async_handler)
        bus.subscribe_async(EventType.PROJECT_CREATED, async_handler)

        assert len(bus._async_handlers["project:created"]) == 1
