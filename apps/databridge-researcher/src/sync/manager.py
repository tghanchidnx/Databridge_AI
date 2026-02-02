"""
Sync Manager for coordinating synchronization with Librarian.
"""

import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor

from .handlers import (
    SyncHandler,
    SyncEvent,
    HandlerResult,
    HierarchySyncHandler,
    DimensionSyncHandler,
    CacheInvalidationHandler,
)

logger = logging.getLogger(__name__)


class SyncStatus(str, Enum):
    """Status of sync operations."""

    IDLE = "idle"
    SYNCING = "syncing"
    ERROR = "error"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"


@dataclass
class SyncResult:
    """Result of a sync operation."""

    success: bool
    status: SyncStatus
    events_processed: int = 0
    errors: List[str] = field(default_factory=list)
    handler_results: List[HandlerResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "status": self.status.value,
            "events_processed": self.events_processed,
            "errors": self.errors,
            "handler_results": [r.to_dict() for r in self.handler_results],
            "timestamp": self.timestamp.isoformat(),
        }


class SyncManager:
    """
    Manager for synchronization between Researcher and Librarian.

    Coordinates:
    - Event consumption from Librarian
    - Handler routing and execution
    - Status tracking and health checks
    - Redis subscription (if available)
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        librarian_url: str = "http://localhost:8000",
    ):
        """
        Initialize sync manager.

        Args:
            redis_url: Optional Redis URL for pub/sub
            librarian_url: URL of Librarian service
        """
        self._redis_url = redis_url
        self._librarian_url = librarian_url
        self._redis_client = None
        self._pubsub = None
        self._handlers: List[SyncHandler] = []
        self._status = SyncStatus.IDLE
        self._last_sync: Optional[datetime] = None
        self._event_count = 0
        self._error_count = 0
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._running = False
        self._listener_thread: Optional[threading.Thread] = None

        # Initialize default handlers
        self._init_default_handlers()

        # Initialize Redis if available
        if redis_url:
            self._init_redis(redis_url)

    def _init_default_handlers(self) -> None:
        """Initialize default sync handlers."""
        self._hierarchy_handler = HierarchySyncHandler()
        self._dimension_handler = DimensionSyncHandler()
        self._cache_handler = CacheInvalidationHandler()

        self._handlers = [
            self._hierarchy_handler,
            self._dimension_handler,
            self._cache_handler,
        ]

    def _init_redis(self, redis_url: str) -> None:
        """Initialize Redis connection."""
        try:
            import redis

            self._redis_client = redis.from_url(redis_url)
            self._pubsub = self._redis_client.pubsub()
            self._status = SyncStatus.CONNECTED
            logger.info(f"Connected to Redis at {redis_url}")
        except ImportError:
            logger.warning("Redis not available")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            self._status = SyncStatus.DISCONNECTED

    def register_handler(self, handler: SyncHandler) -> None:
        """
        Register a custom sync handler.

        Args:
            handler: Handler to register
        """
        self._handlers.append(handler)
        logger.info(f"Registered handler for: {handler.handled_events}")

    def register_cache_invalidation_callback(
        self, callback: Callable[[List[str]], None]
    ) -> None:
        """
        Register a callback for cache invalidation.

        Args:
            callback: Function to call when caches need invalidation
        """
        self._cache_handler.register_cache_callback(callback)

    def process_event(self, event_data: Dict[str, Any]) -> SyncResult:
        """
        Process a single event.

        Args:
            event_data: Event data dictionary

        Returns:
            SyncResult with processing status
        """
        self._status = SyncStatus.SYNCING
        event = SyncEvent.from_dict(event_data)
        handler_results = []
        errors = []

        for handler in self._handlers:
            if handler.can_handle(event.event_type):
                try:
                    result = handler.handle(event)
                    handler_results.append(result)
                    if not result.success:
                        errors.extend(result.errors)
                except Exception as e:
                    logger.exception(f"Handler error for {event.event_type}")
                    errors.append(str(e))

        self._event_count += 1
        self._last_sync = datetime.now(timezone.utc)

        if errors:
            self._error_count += 1
            self._status = SyncStatus.ERROR
        else:
            self._status = SyncStatus.IDLE

        return SyncResult(
            success=len(errors) == 0,
            status=self._status,
            events_processed=1,
            errors=errors,
            handler_results=handler_results,
        )

    def process_events(self, events: List[Dict[str, Any]]) -> SyncResult:
        """
        Process multiple events.

        Args:
            events: List of event data dictionaries

        Returns:
            Combined SyncResult
        """
        self._status = SyncStatus.SYNCING
        all_results = []
        all_errors = []
        processed = 0

        for event_data in events:
            result = self.process_event(event_data)
            all_results.extend(result.handler_results)
            all_errors.extend(result.errors)
            processed += 1

        self._status = SyncStatus.IDLE if not all_errors else SyncStatus.ERROR

        return SyncResult(
            success=len(all_errors) == 0,
            status=self._status,
            events_processed=processed,
            errors=all_errors,
            handler_results=all_results,
        )

    def start_listener(self, channels: Optional[List[str]] = None) -> bool:
        """
        Start listening for events from Redis.

        Args:
            channels: Optional list of channels to subscribe to

        Returns:
            True if listener started
        """
        if not self._redis_client or not self._pubsub:
            logger.warning("Redis not available, cannot start listener")
            return False

        if self._running:
            logger.warning("Listener already running")
            return True

        # Default channels
        if channels is None:
            channels = [
                "databridge:project:*",
                "databridge:hierarchy:*",
                "databridge:mapping:*",
                "databridge:deployment:*",
                "databridge:cache:*",
            ]

        # Subscribe to channels
        for channel in channels:
            self._pubsub.psubscribe(channel)

        self._running = True
        self._listener_thread = threading.Thread(
            target=self._listen_loop, daemon=True
        )
        self._listener_thread.start()
        logger.info(f"Started listener for {len(channels)} channels")
        return True

    def _listen_loop(self) -> None:
        """Background loop for listening to Redis messages."""
        while self._running:
            try:
                message = self._pubsub.get_message(timeout=1.0)
                if message and message["type"] == "pmessage":
                    data = json.loads(message["data"])
                    self._executor.submit(self.process_event, data)
            except Exception as e:
                logger.error(f"Listener error: {e}")

    def stop_listener(self) -> None:
        """Stop the Redis listener."""
        self._running = False
        if self._listener_thread:
            self._listener_thread.join(timeout=2.0)
        if self._pubsub:
            self._pubsub.close()
        logger.info("Stopped listener")

    def poll_librarian(self) -> SyncResult:
        """
        Poll Librarian for changes (HTTP fallback when Redis unavailable).

        Returns:
            SyncResult from processing any new events
        """
        try:
            import httpx

            with httpx.Client(timeout=30.0) as client:
                response = client.get(
                    f"{self._librarian_url}/api/events/pending",
                    params={"limit": 100},
                )
                if response.status_code == 200:
                    events = response.json().get("events", [])
                    if events:
                        return self.process_events(events)
                    return SyncResult(
                        success=True,
                        status=SyncStatus.IDLE,
                        events_processed=0,
                    )
                else:
                    return SyncResult(
                        success=False,
                        status=SyncStatus.ERROR,
                        errors=[f"HTTP {response.status_code}"],
                    )
        except ImportError:
            return SyncResult(
                success=False,
                status=SyncStatus.ERROR,
                errors=["httpx not available"],
            )
        except Exception as e:
            return SyncResult(
                success=False,
                status=SyncStatus.ERROR,
                errors=[str(e)],
            )

    def get_status(self) -> Dict[str, Any]:
        """
        Get current sync status.

        Returns:
            Status dictionary
        """
        return {
            "status": self._status.value,
            "last_sync": self._last_sync.isoformat() if self._last_sync else None,
            "event_count": self._event_count,
            "error_count": self._error_count,
            "redis_connected": self._redis_client is not None,
            "listener_running": self._running,
            "handlers": [
                {"events": h.handled_events} for h in self._handlers
            ],
        }

    def get_health(self) -> Dict[str, Any]:
        """
        Get health check information.

        Returns:
            Health status dictionary
        """
        healthy = (
            self._status not in (SyncStatus.ERROR, SyncStatus.DISCONNECTED)
            and self._error_count < 10
        )
        return {
            "healthy": healthy,
            "status": self._status.value,
            "checks": {
                "redis": self._redis_client is not None,
                "handlers": len(self._handlers) > 0,
                "error_rate": self._error_count / max(self._event_count, 1),
            },
        }

    def reset_stats(self) -> None:
        """Reset sync statistics."""
        self._event_count = 0
        self._error_count = 0
        self._last_sync = None
        logger.info("Reset sync stats")

    def shutdown(self) -> None:
        """Shutdown the sync manager."""
        self.stop_listener()
        self._executor.shutdown(wait=True)
        if self._redis_client:
            self._redis_client.close()
        logger.info("Sync manager shutdown")

    # Convenience access to handlers

    def get_cached_hierarchies(
        self, project_id: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get cached hierarchies from hierarchy handler."""
        return self._hierarchy_handler.get_cached_hierarchies(project_id)

    def get_cached_projects(self) -> Dict[str, Dict[str, Any]]:
        """Get cached projects from hierarchy handler."""
        return self._hierarchy_handler.get_cached_projects()

    def get_pending_dimension_updates(self) -> List[str]:
        """Get projects pending dimension updates."""
        return self._dimension_handler.get_pending_projects()

    def process_pending_dimensions(self) -> List[HandlerResult]:
        """Process pending dimension updates."""
        return self._dimension_handler.process_pending()


# Global sync manager instance
_sync_manager: Optional[SyncManager] = None


def get_sync_manager(
    redis_url: Optional[str] = None,
    librarian_url: str = "http://localhost:8000",
) -> SyncManager:
    """
    Get the global sync manager instance.

    Args:
        redis_url: Optional Redis URL
        librarian_url: Librarian service URL

    Returns:
        SyncManager instance
    """
    global _sync_manager
    if _sync_manager is None:
        _sync_manager = SyncManager(
            redis_url=redis_url,
            librarian_url=librarian_url,
        )
    return _sync_manager


def reset_sync_manager() -> None:
    """Reset the global sync manager (for testing)."""
    global _sync_manager
    if _sync_manager:
        _sync_manager.shutdown()
    _sync_manager = None
