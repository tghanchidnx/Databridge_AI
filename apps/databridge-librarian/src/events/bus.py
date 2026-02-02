"""
Event Bus for pub/sub functionality.

Supports:
- In-process event handlers (callbacks)
- Optional Redis pub/sub for distributed systems
"""

import asyncio
import json
import logging
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor

from .models import Event, EventType

logger = logging.getLogger(__name__)

# Type alias for event handlers
EventHandler = Callable[[Event], None]
AsyncEventHandler = Callable[[Event], Any]


class EventBus:
    """
    Event Bus for publishing and subscribing to events.

    Supports:
    - Synchronous handlers
    - Asynchronous handlers
    - Pattern-based subscriptions (e.g., "hierarchy:*")
    - Optional Redis backend for distributed pub/sub
    """

    _instance: Optional["EventBus"] = None

    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize event bus.

        Args:
            redis_url: Optional Redis URL for distributed pub/sub
        """
        self._handlers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._async_handlers: Dict[str, List[AsyncEventHandler]] = defaultdict(list)
        self._pattern_handlers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._redis_client = None
        self._redis_url = redis_url
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._event_history: List[Dict[str, Any]] = []
        self._max_history = 1000

        if redis_url:
            self._init_redis(redis_url)

    def _init_redis(self, redis_url: str) -> None:
        """Initialize Redis connection if available."""
        try:
            import redis

            self._redis_client = redis.from_url(redis_url)
            logger.info(f"Connected to Redis at {redis_url}")
        except ImportError:
            logger.warning("Redis not available, using in-process events only")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")

    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """
        Subscribe to a specific event type.

        Args:
            event_type: Type of event to subscribe to
            handler: Callback function to handle the event
        """
        key = event_type.value
        if handler not in self._handlers[key]:
            self._handlers[key].append(handler)
            logger.debug(f"Subscribed handler to {key}")

    def subscribe_async(
        self,
        event_type: EventType,
        handler: AsyncEventHandler,
    ) -> None:
        """
        Subscribe an async handler to a specific event type.

        Args:
            event_type: Type of event to subscribe to
            handler: Async callback function to handle the event
        """
        key = event_type.value
        if handler not in self._async_handlers[key]:
            self._async_handlers[key].append(handler)
            logger.debug(f"Subscribed async handler to {key}")

    def subscribe_pattern(
        self,
        pattern: str,
        handler: EventHandler,
    ) -> None:
        """
        Subscribe to events matching a pattern.

        Args:
            pattern: Pattern to match (e.g., "hierarchy:*", "deployment:*")
            handler: Callback function to handle matching events
        """
        if handler not in self._pattern_handlers[pattern]:
            self._pattern_handlers[pattern].append(handler)
            logger.debug(f"Subscribed handler to pattern {pattern}")

    def unsubscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """
        Unsubscribe from a specific event type.

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler to remove
        """
        key = event_type.value
        if handler in self._handlers[key]:
            self._handlers[key].remove(handler)
            logger.debug(f"Unsubscribed handler from {key}")

    def publish(self, event: Event) -> None:
        """
        Publish an event to all subscribers.

        Args:
            event: Event to publish
        """
        event_key = event.event_type.value

        # Record in history
        self._record_event(event)

        # Call direct handlers
        for handler in self._handlers.get(event_key, []):
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Handler error for {event_key}: {e}")

        # Call pattern handlers
        for pattern, handlers in self._pattern_handlers.items():
            if self._matches_pattern(event_key, pattern):
                for handler in handlers:
                    try:
                        handler(event)
                    except Exception as e:
                        logger.error(f"Pattern handler error for {pattern}: {e}")

        # Call async handlers
        for handler in self._async_handlers.get(event_key, []):
            try:
                self._executor.submit(self._run_async_handler, handler, event)
            except Exception as e:
                logger.error(f"Async handler submission error: {e}")

        # Publish to Redis if available
        if self._redis_client:
            try:
                self._redis_client.publish(
                    f"databridge:{event_key}",
                    json.dumps(event.to_dict()),
                )
            except Exception as e:
                logger.error(f"Redis publish error: {e}")

        logger.debug(f"Published event: {event_key}")

    def _run_async_handler(
        self, handler: AsyncEventHandler, event: Event
    ) -> None:
        """Run async handler in thread pool."""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(handler(event))
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Async handler error: {e}")

    def _matches_pattern(self, event_key: str, pattern: str) -> bool:
        """Check if event key matches a pattern."""
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return event_key.startswith(prefix)
        return event_key == pattern

    def _record_event(self, event: Event) -> None:
        """Record event in history."""
        self._event_history.append(event.to_dict())
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

    def get_event_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get recent event history.

        Args:
            event_type: Optional filter by event type
            limit: Maximum events to return

        Returns:
            List of event dictionaries
        """
        events = self._event_history
        if event_type:
            events = [e for e in events if e["event_type"] == event_type.value]
        return events[-limit:]

    def get_subscriber_count(self, event_type: EventType) -> int:
        """Get number of subscribers for an event type."""
        key = event_type.value
        return (
            len(self._handlers.get(key, []))
            + len(self._async_handlers.get(key, []))
        )

    def clear_handlers(self) -> None:
        """Clear all handlers (useful for testing)."""
        self._handlers.clear()
        self._async_handlers.clear()
        self._pattern_handlers.clear()

    def shutdown(self) -> None:
        """Shutdown the event bus."""
        self._executor.shutdown(wait=True)
        if self._redis_client:
            self._redis_client.close()


# Global event bus instance
_event_bus: Optional[EventBus] = None


def get_event_bus(redis_url: Optional[str] = None) -> EventBus:
    """
    Get the global event bus instance.

    Args:
        redis_url: Optional Redis URL for first initialization

    Returns:
        EventBus instance
    """
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus(redis_url=redis_url)
    return _event_bus


def reset_event_bus() -> None:
    """Reset the global event bus (for testing)."""
    global _event_bus
    if _event_bus:
        _event_bus.shutdown()
    _event_bus = None
