"""Unit tests for sync manager."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock, patch

from src.sync.manager import (
    SyncStatus,
    SyncResult,
    SyncManager,
    get_sync_manager,
    reset_sync_manager,
)
from src.sync.handlers import SyncHandler, SyncEvent, HandlerResult


class TestSyncStatus:
    """Tests for SyncStatus enum."""

    def test_sync_status_values(self):
        """Test sync status values exist."""
        assert SyncStatus.IDLE.value == "idle"
        assert SyncStatus.SYNCING.value == "syncing"
        assert SyncStatus.ERROR.value == "error"
        assert SyncStatus.CONNECTED.value == "connected"
        assert SyncStatus.DISCONNECTED.value == "disconnected"


class TestSyncResult:
    """Tests for SyncResult dataclass."""

    def test_sync_result_creation(self):
        """Test sync result creation."""
        result = SyncResult(
            success=True,
            status=SyncStatus.IDLE,
            events_processed=5,
        )

        assert result.success is True
        assert result.status == SyncStatus.IDLE
        assert result.events_processed == 5

    def test_sync_result_defaults(self):
        """Test sync result default values."""
        result = SyncResult(
            success=True,
            status=SyncStatus.IDLE,
        )

        assert result.events_processed == 0
        assert result.errors == []
        assert result.handler_results == []
        assert result.timestamp is not None

    def test_sync_result_to_dict(self):
        """Test sync result serialization."""
        result = SyncResult(
            success=False,
            status=SyncStatus.ERROR,
            events_processed=3,
            errors=["Error 1"],
        )
        data = result.to_dict()

        assert data["success"] is False
        assert data["status"] == "error"
        assert data["events_processed"] == 3
        assert len(data["errors"]) == 1
        assert "timestamp" in data


class TestSyncManager:
    """Tests for SyncManager class."""

    def setup_method(self):
        """Reset sync manager before each test."""
        reset_sync_manager()

    def teardown_method(self):
        """Clean up after each test."""
        reset_sync_manager()

    def test_manager_initialization(self):
        """Test manager initializes correctly."""
        manager = SyncManager()

        assert manager._status == SyncStatus.IDLE
        assert manager._redis_client is None
        assert len(manager._handlers) == 3  # Default handlers

    def test_manager_with_librarian_url(self):
        """Test manager with custom librarian URL."""
        manager = SyncManager(librarian_url="http://custom:8000")

        assert manager._librarian_url == "http://custom:8000"

    def test_register_handler(self):
        """Test registering a custom handler."""
        manager = SyncManager()
        initial_count = len(manager._handlers)

        class CustomHandler(SyncHandler):
            @property
            def handled_events(self):
                return ["custom:event"]

            def handle(self, event):
                return HandlerResult(success=True)

        handler = CustomHandler()
        manager.register_handler(handler)

        assert len(manager._handlers) == initial_count + 1
        assert handler in manager._handlers

    def test_register_cache_invalidation_callback(self):
        """Test registering cache invalidation callback."""
        manager = SyncManager()
        callback = Mock()

        manager.register_cache_invalidation_callback(callback)

        assert callback in manager._cache_handler._invalidation_callbacks

    def test_process_event(self):
        """Test processing a single event."""
        manager = SyncManager()

        event_data = {
            "event_id": "evt-123",
            "event_type": "project:created",
            "project_id": "proj-123",
            "project_name": "Test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        result = manager.process_event(event_data)

        assert result.success is True
        assert result.events_processed == 1
        assert manager._event_count == 1

    def test_process_event_updates_status(self):
        """Test processing event updates status."""
        manager = SyncManager()

        event_data = {
            "event_id": "evt-123",
            "event_type": "project:created",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        manager.process_event(event_data)

        assert manager._status == SyncStatus.IDLE
        assert manager._last_sync is not None

    def test_process_events_multiple(self):
        """Test processing multiple events."""
        manager = SyncManager()

        events = [
            {
                "event_type": "project:created",
                "project_id": "proj-1",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            {
                "event_type": "project:created",
                "project_id": "proj-2",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        ]

        result = manager.process_events(events)

        assert result.success is True
        assert result.events_processed == 2

    def test_get_status(self):
        """Test getting sync status."""
        manager = SyncManager()

        status = manager.get_status()

        assert status["status"] == "idle"
        assert status["event_count"] == 0
        assert status["error_count"] == 0
        assert status["redis_connected"] is False
        assert status["listener_running"] is False

    def test_get_health(self):
        """Test getting health check."""
        manager = SyncManager()

        health = manager.get_health()

        assert health["healthy"] is True
        assert health["status"] == "idle"
        assert "checks" in health

    def test_get_health_unhealthy(self):
        """Test health when error count high."""
        manager = SyncManager()
        manager._error_count = 15

        health = manager.get_health()

        assert health["healthy"] is False

    def test_reset_stats(self):
        """Test resetting statistics."""
        manager = SyncManager()
        manager._event_count = 10
        manager._error_count = 5
        manager._last_sync = datetime.now(timezone.utc)

        manager.reset_stats()

        assert manager._event_count == 0
        assert manager._error_count == 0
        assert manager._last_sync is None

    def test_get_cached_hierarchies(self):
        """Test accessing cached hierarchies."""
        manager = SyncManager()

        # Process an event to cache data
        event_data = {
            "event_type": "hierarchy:created",
            "hierarchy_id": "hier-123",
            "hierarchy_name": "Revenue",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        manager.process_event(event_data)

        hierarchies = manager.get_cached_hierarchies()

        assert "hier-123" in hierarchies

    def test_get_cached_projects(self):
        """Test accessing cached projects."""
        manager = SyncManager()

        # Process an event to cache data
        event_data = {
            "event_type": "project:created",
            "project_id": "proj-123",
            "project_name": "Test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        manager.process_event(event_data)

        projects = manager.get_cached_projects()

        assert "proj-123" in projects

    def test_get_pending_dimension_updates(self):
        """Test getting pending dimension updates."""
        manager = SyncManager()

        # Process hierarchy event
        event_data = {
            "event_type": "hierarchy:created",
            "hierarchy_id": "hier-123",
            "project_id": "proj-123",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        manager.process_event(event_data)

        pending = manager.get_pending_dimension_updates()

        assert "proj-123" in pending

    def test_shutdown(self):
        """Test manager shutdown."""
        manager = SyncManager()
        manager.shutdown()  # Should not raise

    def test_start_listener_no_redis(self):
        """Test starting listener without Redis."""
        manager = SyncManager()

        result = manager.start_listener()

        assert result is False

    def test_stop_listener(self):
        """Test stopping listener."""
        manager = SyncManager()

        manager.stop_listener()  # Should not raise

    def test_poll_librarian_connection_error(self):
        """Test polling librarian handles connection errors."""
        manager = SyncManager(librarian_url="http://127.0.0.1:59999")

        result = manager.poll_librarian()

        # Should handle error gracefully
        assert result.success is False
        assert result.status == SyncStatus.ERROR
        assert len(result.errors) > 0

    def test_poll_librarian_no_httpx(self):
        """Test polling without httpx module."""
        manager = SyncManager()

        # poll_librarian handles ImportError for httpx
        result = manager.poll_librarian()

        # Will fail either because httpx not installed or connection error
        assert result.success is False

    def test_manager_without_redis(self):
        """Test manager works without Redis."""
        manager = SyncManager()

        assert manager._redis_client is None
        assert manager._status == SyncStatus.IDLE

    def test_manager_redis_url_no_redis_installed(self):
        """Test manager handles missing redis module gracefully."""
        # When redis module isn't installed, should still initialize
        manager = SyncManager(redis_url="redis://localhost:6379")

        # Either connects to redis or falls back gracefully
        # Status should be CONNECTED, DISCONNECTED, or IDLE
        assert manager._status in (
            SyncStatus.CONNECTED,
            SyncStatus.DISCONNECTED,
            SyncStatus.IDLE,
        )


class TestGlobalSyncManager:
    """Tests for global sync manager functions."""

    def setup_method(self):
        """Reset sync manager before each test."""
        reset_sync_manager()

    def teardown_method(self):
        """Clean up after each test."""
        reset_sync_manager()

    def test_get_sync_manager_creates_instance(self):
        """Test get_sync_manager creates singleton."""
        manager = get_sync_manager()

        assert manager is not None
        assert isinstance(manager, SyncManager)

    def test_get_sync_manager_returns_same_instance(self):
        """Test get_sync_manager returns same instance."""
        manager1 = get_sync_manager()
        manager2 = get_sync_manager()

        assert manager1 is manager2

    def test_reset_sync_manager(self):
        """Test reset_sync_manager creates new instance."""
        manager1 = get_sync_manager()
        reset_sync_manager()
        manager2 = get_sync_manager()

        assert manager1 is not manager2

    def test_get_sync_manager_with_params(self):
        """Test get_sync_manager with custom params."""
        manager = get_sync_manager(librarian_url="http://custom:8000")

        assert manager._librarian_url == "http://custom:8000"
