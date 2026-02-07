"""
Redis Pub/Sub Broadcaster for WebSocket Console.

Handles message broadcasting between:
- MCP tools (publishers)
- WebSocket clients (subscribers)
- Inter-agent communication
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
import threading

from .types import (
    WebSocketMessage,
    WebSocketMessageType,
    ConsoleLogMessage,
    ConsoleLogLevel,
)

logger = logging.getLogger(__name__)


class InMemoryBroadcaster:
    """
    In-memory message broadcaster (no Redis required).

    Useful for development and single-instance deployments.
    For multi-instance deployments, use RedisBroadcaster.
    """

    def __init__(self):
        self._subscribers: Dict[str, Set[Callable]] = {}
        self._message_history: List[WebSocketMessage] = []
        self._max_history = 1000
        self._lock = threading.Lock()

    def subscribe(self, channel: str, callback: Callable[[WebSocketMessage], None]) -> None:
        """Subscribe to a channel."""
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = set()
            self._subscribers[channel].add(callback)
            logger.debug(f"Subscribed to channel: {channel}")

    def unsubscribe(self, channel: str, callback: Callable) -> None:
        """Unsubscribe from a channel."""
        with self._lock:
            if channel in self._subscribers:
                self._subscribers[channel].discard(callback)
                if not self._subscribers[channel]:
                    del self._subscribers[channel]

    def publish(self, channel: str, message: WebSocketMessage) -> int:
        """
        Publish a message to a channel.

        Returns number of subscribers that received the message.
        """
        with self._lock:
            # Store in history
            self._message_history.append(message)
            if len(self._message_history) > self._max_history:
                self._message_history = self._message_history[-self._max_history:]

            # Get subscribers
            subscribers = self._subscribers.get(channel, set()).copy()

        # Notify subscribers outside the lock
        count = 0
        for callback in subscribers:
            try:
                callback(message)
                count += 1
            except Exception as e:
                logger.error(f"Error in subscriber callback: {e}")

        return count

    def publish_to_all(self, message: WebSocketMessage) -> int:
        """Publish to all channels."""
        total = 0
        channels = list(self._subscribers.keys())
        for channel in channels:
            total += self.publish(channel, message)
        return total

    def get_history(
        self,
        limit: int = 100,
        message_type: Optional[WebSocketMessageType] = None,
        since: Optional[datetime] = None,
    ) -> List[WebSocketMessage]:
        """Get message history with optional filters."""
        with self._lock:
            messages = self._message_history.copy()

        if message_type:
            messages = [m for m in messages if m.type == message_type]

        if since:
            messages = [m for m in messages if m.timestamp >= since]

        return messages[-limit:]

    def get_channel_count(self) -> Dict[str, int]:
        """Get subscriber count per channel."""
        with self._lock:
            return {ch: len(subs) for ch, subs in self._subscribers.items()}

    def clear_history(self) -> None:
        """Clear message history."""
        with self._lock:
            self._message_history.clear()


class RedisBroadcaster:
    """
    Redis-based message broadcaster for multi-instance deployments.

    Requires redis-py: pip install redis
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        channel_prefix: str = "databridge:console:",
    ):
        self.redis_url = redis_url
        self.channel_prefix = channel_prefix
        self._redis = None
        self._pubsub = None
        self._local_callbacks: Dict[str, Set[Callable]] = {}
        self._listener_thread: Optional[threading.Thread] = None
        self._running = False

    def connect(self) -> bool:
        """Connect to Redis."""
        try:
            import redis
            self._redis = redis.from_url(self.redis_url)
            self._pubsub = self._redis.pubsub()
            self._running = True
            self._start_listener()
            logger.info(f"Connected to Redis: {self.redis_url}")
            return True
        except ImportError:
            logger.warning("redis-py not installed, falling back to in-memory")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from Redis."""
        self._running = False
        if self._pubsub:
            self._pubsub.close()
        if self._redis:
            self._redis.close()

    def _start_listener(self) -> None:
        """Start the pub/sub listener thread."""
        def listener():
            while self._running:
                try:
                    message = self._pubsub.get_message(timeout=1.0)
                    if message and message["type"] == "message":
                        self._handle_redis_message(message)
                except Exception as e:
                    if self._running:
                        logger.error(f"Redis listener error: {e}")

        self._listener_thread = threading.Thread(target=listener, daemon=True)
        self._listener_thread.start()

    def _handle_redis_message(self, message: Dict) -> None:
        """Handle incoming Redis message."""
        try:
            channel = message["channel"].decode() if isinstance(message["channel"], bytes) else message["channel"]
            data = json.loads(message["data"])
            ws_message = WebSocketMessage(**data)

            # Remove prefix from channel
            short_channel = channel.replace(self.channel_prefix, "")

            # Notify local callbacks
            callbacks = self._local_callbacks.get(short_channel, set())
            for callback in callbacks:
                try:
                    callback(ws_message)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

        except Exception as e:
            logger.error(f"Failed to handle Redis message: {e}")

    def subscribe(self, channel: str, callback: Callable[[WebSocketMessage], None]) -> None:
        """Subscribe to a channel."""
        full_channel = f"{self.channel_prefix}{channel}"

        if channel not in self._local_callbacks:
            self._local_callbacks[channel] = set()
            if self._pubsub:
                self._pubsub.subscribe(full_channel)

        self._local_callbacks[channel].add(callback)

    def unsubscribe(self, channel: str, callback: Callable) -> None:
        """Unsubscribe from a channel."""
        if channel in self._local_callbacks:
            self._local_callbacks[channel].discard(callback)
            if not self._local_callbacks[channel]:
                del self._local_callbacks[channel]
                if self._pubsub:
                    self._pubsub.unsubscribe(f"{self.channel_prefix}{channel}")

    def publish(self, channel: str, message: WebSocketMessage) -> int:
        """Publish a message to a channel."""
        if not self._redis:
            return 0

        full_channel = f"{self.channel_prefix}{channel}"
        data = message.model_dump_json()

        try:
            return self._redis.publish(full_channel, data)
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")
            return 0


class ConsoleBroadcaster:
    """
    High-level broadcaster for console messages.

    Automatically selects Redis or in-memory based on availability.
    Provides convenience methods for common message types.
    """

    # Singleton instance
    _instance: Optional["ConsoleBroadcaster"] = None

    def __init__(self, redis_url: Optional[str] = None):
        self._redis_broadcaster: Optional[RedisBroadcaster] = None
        self._memory_broadcaster = InMemoryBroadcaster()
        self._use_redis = False

        if redis_url:
            self._redis_broadcaster = RedisBroadcaster(redis_url)
            self._use_redis = self._redis_broadcaster.connect()

    @classmethod
    def get_instance(cls, redis_url: Optional[str] = None) -> "ConsoleBroadcaster":
        """Get or create singleton instance."""
        if cls._instance is None:
            cls._instance = cls(redis_url)
        return cls._instance

    @property
    def broadcaster(self):
        """Get the active broadcaster."""
        if self._use_redis and self._redis_broadcaster:
            return self._redis_broadcaster
        return self._memory_broadcaster

    def subscribe(self, channel: str, callback: Callable[[WebSocketMessage], None]) -> None:
        """Subscribe to a channel."""
        self.broadcaster.subscribe(channel, callback)

    def unsubscribe(self, channel: str, callback: Callable) -> None:
        """Unsubscribe from a channel."""
        self.broadcaster.unsubscribe(channel, callback)

    def publish(self, channel: str, message: WebSocketMessage) -> int:
        """Publish a message."""
        return self.broadcaster.publish(channel, message)

    # Convenience methods

    def log(
        self,
        message: str,
        level: ConsoleLogLevel = ConsoleLogLevel.INFO,
        source: str = "system",
        conversation_id: Optional[str] = None,
        **metadata,
    ) -> None:
        """Log a console message."""
        log_msg = ConsoleLogMessage(
            level=level,
            source=source,
            message=message,
            conversation_id=conversation_id,
            metadata=metadata,
        )
        self.publish("console", log_msg.to_ws_message())

    def log_info(self, message: str, source: str = "system", **kwargs) -> None:
        """Log an info message."""
        self.log(message, ConsoleLogLevel.INFO, source, **kwargs)

    def log_warning(self, message: str, source: str = "system", **kwargs) -> None:
        """Log a warning message."""
        self.log(message, ConsoleLogLevel.WARNING, source, **kwargs)

    def log_error(self, message: str, source: str = "system", **kwargs) -> None:
        """Log an error message."""
        self.log(message, ConsoleLogLevel.ERROR, source, **kwargs)

    def log_success(self, message: str, source: str = "system", **kwargs) -> None:
        """Log a success message."""
        self.log(message, ConsoleLogLevel.SUCCESS, source, **kwargs)

    def log_debug(self, message: str, source: str = "system", **kwargs) -> None:
        """Log a debug message."""
        self.log(message, ConsoleLogLevel.DEBUG, source, **kwargs)

    def reasoning_step(
        self,
        conversation_id: str,
        step_number: int,
        phase: str,
        title: str,
        content: str,
        **kwargs,
    ) -> None:
        """Publish a reasoning step."""
        from .types import ReasoningStepMessage, ReasoningPhase

        step_msg = ReasoningStepMessage(
            conversation_id=conversation_id,
            step_number=step_number,
            phase=ReasoningPhase(phase),
            title=title,
            content=content,
            **kwargs,
        )
        self.publish("reasoning", step_msg.to_ws_message())

    def agent_status(
        self,
        agent_id: str,
        agent_name: str,
        status: str = "active",
        **kwargs,
    ) -> None:
        """Publish agent status update."""
        from .types import AgentActivityMessage

        agent_msg = AgentActivityMessage(
            agent_id=agent_id,
            agent_name=agent_name,
            status=status,
            **kwargs,
        )
        self.publish("agents", agent_msg.to_ws_message())

    def get_history(self, limit: int = 100, **kwargs) -> List[WebSocketMessage]:
        """Get message history (in-memory only)."""
        if isinstance(self.broadcaster, InMemoryBroadcaster):
            return self.broadcaster.get_history(limit, **kwargs)
        return []

    def get_stats(self) -> Dict[str, Any]:
        """Get broadcaster statistics."""
        stats = {
            "backend": "redis" if self._use_redis else "memory",
        }

        if isinstance(self.broadcaster, InMemoryBroadcaster):
            stats["channels"] = self.broadcaster.get_channel_count()
            stats["history_size"] = len(self.broadcaster._message_history)

        return stats
