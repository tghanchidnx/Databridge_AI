"""
Console Dashboard WebSocket Module.

Provides real-time streaming of agent activity via WebSocket:
- Console log entries
- Reasoning loop visualization
- Agent activity monitoring
- Cortex AI interaction tracking

Components:
- ConsoleServer: FastAPI WebSocket server
- ConsoleBroadcaster: Pub/Sub message distribution
- ConnectionManager: WebSocket connection handling
- MCP Tools: Server control and broadcasting
"""

from .types import (
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

from .broadcaster import (
    InMemoryBroadcaster,
    RedisBroadcaster,
    ConsoleBroadcaster,
)

from .handlers import (
    WebSocketConnection,
    ConnectionManager,
)

from .server import (
    ConsoleServer,
    get_server,
    reset_server,
)

from .mcp_tools import register_console_tools

__all__ = [
    # Types
    "WebSocketMessage",
    "WebSocketMessageType",
    "ConsoleLogMessage",
    "ConsoleLogLevel",
    "ReasoningPhase",
    "ReasoningStepMessage",
    "AgentActivityMessage",
    "CortexQueryMessage",
    "CortexResultMessage",
    "SubscriptionRequest",
    "ConnectionInfo",
    # Broadcaster
    "InMemoryBroadcaster",
    "RedisBroadcaster",
    "ConsoleBroadcaster",
    # Handlers
    "WebSocketConnection",
    "ConnectionManager",
    # Server
    "ConsoleServer",
    "get_server",
    "reset_server",
    # MCP
    "register_console_tools",
]
