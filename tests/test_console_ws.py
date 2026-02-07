"""
Tests for Phase 23: Console Dashboard WebSocket Server.

Tests the WebSocket types, broadcaster, handlers, and MCP tools.
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

# Import types
from src.console_ws.types import (
    WebSocketMessage,
    WebSocketMessageType,
    ConsoleLogMessage,
    ConsoleLogLevel,
    ReasoningPhase,
    ReasoningStepMessage,
    AgentActivityMessage,
    CortexQueryMessage,
    CortexResultMessage,
    SubscriptionRequest,
    ConnectionInfo,
)

# Import broadcaster
from src.console_ws.broadcaster import (
    InMemoryBroadcaster,
    ConsoleBroadcaster,
)

# Import handlers
from src.console_ws.handlers import (
    WebSocketConnection,
    ConnectionManager,
)


class TestWebSocketTypes:
    """Test WebSocket message types."""

    def test_websocket_message_creation(self):
        """Test creating a WebSocket message."""
        msg = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"message": "Test"},
        )
        assert msg.type == WebSocketMessageType.CONSOLE_LOG
        assert msg.payload["message"] == "Test"
        assert msg.id is not None
        assert msg.timestamp is not None

    def test_console_log_message(self):
        """Test console log message creation."""
        log = ConsoleLogMessage(
            level=ConsoleLogLevel.INFO,
            source="test",
            message="Test message",
        )
        assert log.level == ConsoleLogLevel.INFO
        assert log.source == "test"
        assert log.message == "Test message"

    def test_console_log_to_ws_message(self):
        """Test converting console log to WebSocket message."""
        log = ConsoleLogMessage(
            level=ConsoleLogLevel.WARNING,
            source="system",
            message="Warning!",
        )
        ws_msg = log.to_ws_message()
        assert ws_msg.type == WebSocketMessageType.CONSOLE_LOG
        assert ws_msg.payload["level"] == "warning"
        assert ws_msg.payload["message"] == "Warning!"

    def test_reasoning_step_message(self):
        """Test reasoning step message."""
        step = ReasoningStepMessage(
            conversation_id="conv-123",
            step_number=1,
            phase=ReasoningPhase.OBSERVE,
            title="Observing data",
            content="Looking at the table structure",
        )
        assert step.phase == ReasoningPhase.OBSERVE
        assert step.step_number == 1

    def test_reasoning_step_to_ws_message(self):
        """Test converting reasoning step to WebSocket message."""
        step = ReasoningStepMessage(
            conversation_id="conv-123",
            step_number=2,
            phase=ReasoningPhase.PLAN,
            title="Planning",
            content="Creating execution plan",
        )
        ws_msg = step.to_ws_message()
        assert ws_msg.type == WebSocketMessageType.REASONING_STEP
        assert ws_msg.payload["step_number"] == 2

    def test_agent_activity_message(self):
        """Test agent activity message."""
        agent = AgentActivityMessage(
            agent_id="agent-1",
            agent_name="TestAgent",
            status="active",
            current_task="Processing data",
        )
        assert agent.agent_id == "agent-1"
        assert agent.status == "active"

    def test_cortex_query_message(self):
        """Test Cortex query message."""
        query = CortexQueryMessage(
            conversation_id="conv-123",
            function="COMPLETE",
            query="Explain data quality",
            model="mistral-large",
        )
        assert query.function == "COMPLETE"
        assert query.model == "mistral-large"

    def test_cortex_result_message(self):
        """Test Cortex result message."""
        result = CortexResultMessage(
            conversation_id="conv-123",
            function="SUMMARIZE",
            result="Summary of the data",
            duration_ms=150,
            success=True,
        )
        assert result.success is True
        assert result.duration_ms == 150

    def test_subscription_request(self):
        """Test subscription request."""
        sub = SubscriptionRequest(
            channels=["console", "reasoning"],
            conversation_id="conv-123",
        )
        assert "console" in sub.channels
        assert sub.conversation_id == "conv-123"

    def test_connection_info(self):
        """Test connection info."""
        info = ConnectionInfo(
            connection_id="conn-123",
            client_ip="127.0.0.1",
            connected_at=datetime.now(),
            subscriptions=["console"],
            message_count=5,
        )
        assert info.connection_id == "conn-123"
        assert info.message_count == 5


class TestInMemoryBroadcaster:
    """Test in-memory broadcaster."""

    def test_subscribe(self):
        """Test subscribing to a channel."""
        broadcaster = InMemoryBroadcaster()
        callback = Mock()

        broadcaster.subscribe("console", callback)

        assert "console" in broadcaster._subscribers
        assert callback in broadcaster._subscribers["console"]

    def test_unsubscribe(self):
        """Test unsubscribing from a channel."""
        broadcaster = InMemoryBroadcaster()
        callback = Mock()

        broadcaster.subscribe("console", callback)
        broadcaster.unsubscribe("console", callback)

        assert "console" not in broadcaster._subscribers

    def test_publish(self):
        """Test publishing a message."""
        broadcaster = InMemoryBroadcaster()
        callback = Mock()

        broadcaster.subscribe("console", callback)

        msg = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"message": "Test"},
        )
        count = broadcaster.publish("console", msg)

        assert count == 1
        callback.assert_called_once_with(msg)

    def test_publish_to_multiple_subscribers(self):
        """Test publishing to multiple subscribers."""
        broadcaster = InMemoryBroadcaster()
        callback1 = Mock()
        callback2 = Mock()

        broadcaster.subscribe("console", callback1)
        broadcaster.subscribe("console", callback2)

        msg = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"message": "Test"},
        )
        count = broadcaster.publish("console", msg)

        assert count == 2
        callback1.assert_called_once()
        callback2.assert_called_once()

    def test_message_history(self):
        """Test message history."""
        broadcaster = InMemoryBroadcaster()

        for i in range(5):
            msg = WebSocketMessage(
                type=WebSocketMessageType.CONSOLE_LOG,
                payload={"index": i},
            )
            broadcaster.publish("console", msg)

        history = broadcaster.get_history(limit=3)
        assert len(history) == 3

    def test_clear_history(self):
        """Test clearing message history."""
        broadcaster = InMemoryBroadcaster()

        msg = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"message": "Test"},
        )
        broadcaster.publish("console", msg)

        broadcaster.clear_history()
        history = broadcaster.get_history()
        assert len(history) == 0


class TestConsoleBroadcaster:
    """Test high-level console broadcaster."""

    def setup_method(self):
        """Reset singleton before each test."""
        ConsoleBroadcaster._instance = None

    def test_singleton(self):
        """Test singleton pattern."""
        b1 = ConsoleBroadcaster.get_instance()
        b2 = ConsoleBroadcaster.get_instance()
        assert b1 is b2

    def test_log_convenience_method(self):
        """Test log convenience method."""
        broadcaster = ConsoleBroadcaster.get_instance()
        callback = Mock()

        broadcaster.subscribe("console", callback)
        broadcaster.log("Test message", ConsoleLogLevel.INFO, "test")

        assert callback.called
        call_args = callback.call_args[0][0]
        assert call_args.type == WebSocketMessageType.CONSOLE_LOG

    def test_log_info(self):
        """Test log_info convenience method."""
        broadcaster = ConsoleBroadcaster.get_instance()
        callback = Mock()

        broadcaster.subscribe("console", callback)
        broadcaster.log_info("Info message")

        assert callback.called

    def test_log_error(self):
        """Test log_error convenience method."""
        broadcaster = ConsoleBroadcaster.get_instance()
        callback = Mock()

        broadcaster.subscribe("console", callback)
        broadcaster.log_error("Error message")

        assert callback.called

    def test_get_stats(self):
        """Test getting broadcaster stats."""
        broadcaster = ConsoleBroadcaster.get_instance()
        stats = broadcaster.get_stats()

        assert "backend" in stats
        assert stats["backend"] == "memory"


class TestWebSocketConnection:
    """Test WebSocket connection wrapper."""

    def test_connection_creation(self):
        """Test creating a connection."""
        mock_ws = Mock()
        conn = WebSocketConnection(
            connection_id="test-123",
            websocket=mock_ws,
            client_ip="127.0.0.1",
        )

        assert conn.connection_id == "test-123"
        assert conn.client_ip == "127.0.0.1"
        assert len(conn.subscriptions) == 0

    def test_to_info(self):
        """Test converting to ConnectionInfo."""
        mock_ws = Mock()
        conn = WebSocketConnection(
            connection_id="test-123",
            websocket=mock_ws,
        )
        conn.subscriptions.add("console")

        info = conn.to_info()
        assert info.connection_id == "test-123"
        assert "console" in info.subscriptions

    @pytest.mark.asyncio
    async def test_send(self):
        """Test sending a message."""
        mock_ws = AsyncMock()
        conn = WebSocketConnection(
            connection_id="test-123",
            websocket=mock_ws,
        )

        msg = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"message": "Test"},
        )
        result = await conn.send(msg)

        assert result is True
        mock_ws.send_text.assert_called_once()
        assert conn.message_count == 1

    def test_matches_filter_no_filter(self):
        """Test filter matching with no filters."""
        mock_ws = Mock()
        conn = WebSocketConnection(
            connection_id="test-123",
            websocket=mock_ws,
        )

        msg = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"conversation_id": "conv-1"},
        )
        assert conn.matches_filter(msg) is True

    def test_matches_filter_with_conversation_filter(self):
        """Test filter matching with conversation filter."""
        mock_ws = Mock()
        conn = WebSocketConnection(
            connection_id="test-123",
            websocket=mock_ws,
        )
        conn.filters["conversation_id"] = "conv-1"

        msg1 = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"conversation_id": "conv-1"},
        )
        msg2 = WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload={"conversation_id": "conv-2"},
        )

        assert conn.matches_filter(msg1) is True
        assert conn.matches_filter(msg2) is False


class TestConnectionManager:
    """Test connection manager."""

    def setup_method(self):
        """Reset broadcaster singleton before each test."""
        ConsoleBroadcaster._instance = None

    @pytest.mark.asyncio
    async def test_connect(self):
        """Test connecting a client."""
        manager = ConnectionManager()
        mock_ws = AsyncMock()

        conn = await manager.connect(mock_ws, "127.0.0.1")

        assert conn.connection_id in manager.connections
        assert manager.get_connection_count() == 1
        mock_ws.send_text.assert_called()  # Welcome message

    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Test disconnecting a client."""
        manager = ConnectionManager()
        mock_ws = AsyncMock()

        conn = await manager.connect(mock_ws)
        await manager.disconnect(conn.connection_id)

        assert conn.connection_id not in manager.connections
        assert manager.get_connection_count() == 0

    @pytest.mark.asyncio
    async def test_handle_ping(self):
        """Test handling ping message."""
        manager = ConnectionManager()
        mock_ws = AsyncMock()

        conn = await manager.connect(mock_ws)
        ping_msg = '{"type": "ping", "payload": {}}'

        response = await manager.handle_message(conn, ping_msg)

        assert response.type == WebSocketMessageType.PONG

    @pytest.mark.asyncio
    async def test_handle_subscribe(self):
        """Test handling subscribe message."""
        manager = ConnectionManager()
        mock_ws = AsyncMock()

        conn = await manager.connect(mock_ws)
        sub_msg = '{"type": "subscribe", "payload": {"channels": ["console", "reasoning"]}}'

        response = await manager.handle_message(conn, sub_msg)

        assert response.type == WebSocketMessageType.SUBSCRIBE
        assert "console" in conn.subscriptions
        assert "reasoning" in conn.subscriptions

    @pytest.mark.asyncio
    async def test_handle_invalid_json(self):
        """Test handling invalid JSON."""
        manager = ConnectionManager()
        mock_ws = AsyncMock()

        conn = await manager.connect(mock_ws)
        response = await manager.handle_message(conn, "not valid json")

        assert response.type == WebSocketMessageType.ERROR
        assert "Invalid JSON" in response.payload["error"]

    def test_get_connections(self):
        """Test getting all connections."""
        manager = ConnectionManager()

        # No connections initially
        conns = manager.get_connections()
        assert len(conns) == 0

    def test_get_stats(self):
        """Test getting connection stats."""
        manager = ConnectionManager()
        stats = manager.get_stats()

        assert "total_connections" in stats
        assert "total_messages_sent" in stats
        assert "subscriptions" in stats


class TestMCPTools:
    """Test MCP tools registration."""

    def test_register_console_tools(self):
        """Test registering console tools."""
        from src.console_ws.mcp_tools import register_console_tools

        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        result = register_console_tools(mock_mcp)

        assert result["tools_registered"] == 5
        assert "start_console_server" in result["tools"]
        assert "stop_console_server" in result["tools"]
        assert "get_console_connections" in result["tools"]
        assert "broadcast_console_message" in result["tools"]
        assert "get_console_server_status" in result["tools"]


class TestModuleExports:
    """Test module exports."""

    def test_all_exports(self):
        """Test that all expected items are exported."""
        from src.console_ws import (
            # Types
            WebSocketMessage,
            WebSocketMessageType,
            ConsoleLogMessage,
            ConsoleLogLevel,
            ReasoningPhase,
            ReasoningStepMessage,
            AgentActivityMessage,
            # Broadcaster
            InMemoryBroadcaster,
            ConsoleBroadcaster,
            # Handlers
            WebSocketConnection,
            ConnectionManager,
            # Server
            ConsoleServer,
            get_server,
            reset_server,
            # MCP
            register_console_tools,
        )

        # Just verify imports work
        assert WebSocketMessageType.CONSOLE_LOG is not None
        assert ConsoleLogLevel.INFO is not None
        assert ReasoningPhase.OBSERVE is not None
