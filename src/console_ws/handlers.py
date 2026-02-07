"""
WebSocket message handlers for the Console Dashboard.

Handles:
- Client connections and disconnections
- Subscription management
- Message routing
- Heartbeat/ping-pong
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
import uuid

from .types import (
    WebSocketMessage,
    WebSocketMessageType,
    ConnectionInfo,
    SubscriptionRequest,
    ConsoleLogMessage,
    ConsoleLogLevel,
)
from .broadcaster import ConsoleBroadcaster

logger = logging.getLogger(__name__)


class WebSocketConnection:
    """Represents a single WebSocket connection."""

    def __init__(
        self,
        connection_id: str,
        websocket: Any,  # WebSocket instance (framework-agnostic)
        client_ip: str = "unknown",
    ):
        self.connection_id = connection_id
        self.websocket = websocket
        self.client_ip = client_ip
        self.connected_at = datetime.now()
        self.subscriptions: Set[str] = set()
        self.filters: Dict[str, Any] = {}
        self.message_count = 0
        self.last_activity = datetime.now()
        self._send_queue: asyncio.Queue = asyncio.Queue()

    def to_info(self) -> ConnectionInfo:
        """Convert to ConnectionInfo."""
        return ConnectionInfo(
            connection_id=self.connection_id,
            client_ip=self.client_ip,
            connected_at=self.connected_at,
            subscriptions=list(self.subscriptions),
            message_count=self.message_count,
            last_activity=self.last_activity,
        )

    async def send(self, message: WebSocketMessage) -> bool:
        """Send a message to this connection."""
        try:
            data = message.model_dump_json()
            await self.websocket.send_text(data)
            self.message_count += 1
            self.last_activity = datetime.now()
            return True
        except Exception as e:
            logger.error(f"Failed to send to {self.connection_id}: {e}")
            return False

    def matches_filter(self, message: WebSocketMessage) -> bool:
        """Check if message passes connection filters."""
        payload = message.payload

        # Check conversation filter
        if "conversation_id" in self.filters:
            msg_conv = payload.get("conversation_id")
            if msg_conv and msg_conv != self.filters["conversation_id"]:
                return False

        # Check agent filter
        if "agent_id" in self.filters:
            msg_agent = payload.get("agent_id")
            if msg_agent and msg_agent != self.filters["agent_id"]:
                return False

        return True


class ConnectionManager:
    """Manages all WebSocket connections."""

    def __init__(self, broadcaster: Optional[ConsoleBroadcaster] = None):
        self.connections: Dict[str, WebSocketConnection] = {}
        self.broadcaster = broadcaster or ConsoleBroadcaster.get_instance()
        self._lock = asyncio.Lock()

    async def connect(
        self,
        websocket: Any,
        client_ip: str = "unknown",
    ) -> WebSocketConnection:
        """Register a new connection."""
        connection_id = str(uuid.uuid4())
        connection = WebSocketConnection(
            connection_id=connection_id,
            websocket=websocket,
            client_ip=client_ip,
        )

        async with self._lock:
            self.connections[connection_id] = connection

        # Send welcome message
        welcome = WebSocketMessage(
            type=WebSocketMessageType.CONNECT,
            payload={
                "connection_id": connection_id,
                "message": "Connected to DataBridge Console",
                "available_channels": ["console", "reasoning", "agents", "cortex"],
            },
        )
        await connection.send(welcome)

        logger.info(f"Client connected: {connection_id} from {client_ip}")
        return connection

    async def disconnect(self, connection_id: str) -> None:
        """Unregister a connection."""
        async with self._lock:
            if connection_id in self.connections:
                connection = self.connections[connection_id]

                # Unsubscribe from all channels
                for channel in connection.subscriptions:
                    self.broadcaster.unsubscribe(
                        channel,
                        lambda msg, conn=connection: asyncio.create_task(conn.send(msg))
                    )

                del self.connections[connection_id]
                logger.info(f"Client disconnected: {connection_id}")

    async def handle_message(
        self,
        connection: WebSocketConnection,
        raw_message: str,
    ) -> Optional[WebSocketMessage]:
        """Handle an incoming message from a client."""
        try:
            data = json.loads(raw_message)
            message = WebSocketMessage(**data)

            connection.last_activity = datetime.now()

            # Handle based on message type
            if message.type == WebSocketMessageType.PING:
                return await self._handle_ping(connection)

            elif message.type == WebSocketMessageType.SUBSCRIBE:
                return await self._handle_subscribe(connection, message)

            elif message.type == WebSocketMessageType.UNSUBSCRIBE:
                return await self._handle_unsubscribe(connection, message)

            else:
                logger.warning(f"Unknown message type: {message.type}")
                return WebSocketMessage(
                    type=WebSocketMessageType.ERROR,
                    payload={"error": f"Unknown message type: {message.type}"},
                )

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return WebSocketMessage(
                type=WebSocketMessageType.ERROR,
                payload={"error": "Invalid JSON"},
            )
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return WebSocketMessage(
                type=WebSocketMessageType.ERROR,
                payload={"error": str(e)},
            )

    async def _handle_ping(self, connection: WebSocketConnection) -> WebSocketMessage:
        """Handle ping message."""
        return WebSocketMessage(
            type=WebSocketMessageType.PONG,
            payload={"timestamp": datetime.now().isoformat()},
        )

    async def _handle_subscribe(
        self,
        connection: WebSocketConnection,
        message: WebSocketMessage,
    ) -> WebSocketMessage:
        """Handle subscription request."""
        payload = message.payload
        channels = payload.get("channels", ["console"])

        # Store filters
        if "conversation_id" in payload:
            connection.filters["conversation_id"] = payload["conversation_id"]
        if "agent_id" in payload:
            connection.filters["agent_id"] = payload["agent_id"]

        # Subscribe to channels
        subscribed = []
        for channel in channels:
            if channel not in connection.subscriptions:
                connection.subscriptions.add(channel)

                # Create callback that sends to this connection
                def create_callback(conn: WebSocketConnection):
                    def callback(msg: WebSocketMessage):
                        if conn.matches_filter(msg):
                            asyncio.create_task(conn.send(msg))
                    return callback

                self.broadcaster.subscribe(channel, create_callback(connection))
                subscribed.append(channel)

        return WebSocketMessage(
            type=WebSocketMessageType.SUBSCRIBE,
            payload={
                "subscribed": subscribed,
                "active_subscriptions": list(connection.subscriptions),
                "filters": connection.filters,
            },
        )

    async def _handle_unsubscribe(
        self,
        connection: WebSocketConnection,
        message: WebSocketMessage,
    ) -> WebSocketMessage:
        """Handle unsubscription request."""
        channels = message.payload.get("channels", [])

        unsubscribed = []
        for channel in channels:
            if channel in connection.subscriptions:
                connection.subscriptions.remove(channel)
                # Note: We can't easily remove the specific callback without tracking it
                unsubscribed.append(channel)

        return WebSocketMessage(
            type=WebSocketMessageType.UNSUBSCRIBE,
            payload={
                "unsubscribed": unsubscribed,
                "active_subscriptions": list(connection.subscriptions),
            },
        )

    async def broadcast(
        self,
        message: WebSocketMessage,
        channel: Optional[str] = None,
    ) -> int:
        """Broadcast a message to subscribed connections."""
        count = 0
        async with self._lock:
            connections = list(self.connections.values())

        for connection in connections:
            # Check if subscribed to the channel
            if channel and channel not in connection.subscriptions:
                continue

            # Check filters
            if not connection.matches_filter(message):
                continue

            if await connection.send(message):
                count += 1

        return count

    def get_connections(self) -> List[ConnectionInfo]:
        """Get info about all connections."""
        return [conn.to_info() for conn in self.connections.values()]

    def get_connection_count(self) -> int:
        """Get number of active connections."""
        return len(self.connections)

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        connections = list(self.connections.values())

        return {
            "total_connections": len(connections),
            "total_messages_sent": sum(c.message_count for c in connections),
            "subscriptions": {
                channel: sum(1 for c in connections if channel in c.subscriptions)
                for channel in ["console", "reasoning", "agents", "cortex"]
            },
        }
