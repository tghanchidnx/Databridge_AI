"""
WebSocket message types for the Console Dashboard.

Defines the message structures for real-time streaming of:
- Console log entries
- Reasoning loop steps
- Agent activity updates
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class WebSocketMessageType(str, Enum):
    """Types of WebSocket messages."""
    # Connection management
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    PING = "ping"
    PONG = "pong"
    ERROR = "error"

    # Console messages
    CONSOLE_LOG = "console.log"
    CONSOLE_CLEAR = "console.clear"

    # Reasoning loop
    REASONING_START = "reasoning.start"
    REASONING_STEP = "reasoning.step"
    REASONING_COMPLETE = "reasoning.complete"
    REASONING_ERROR = "reasoning.error"

    # Agent activity
    AGENT_REGISTER = "agent.register"
    AGENT_UNREGISTER = "agent.unregister"
    AGENT_MESSAGE = "agent.message"
    AGENT_STATUS = "agent.status"

    # Cortex activity
    CORTEX_QUERY = "cortex.query"
    CORTEX_RESULT = "cortex.result"

    # Subscriptions
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"


class ConsoleLogLevel(str, Enum):
    """Log levels for console messages."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


class ReasoningPhase(str, Enum):
    """Phases in the reasoning loop."""
    OBSERVE = "observe"
    PLAN = "plan"
    EXECUTE = "execute"
    REFLECT = "reflect"
    COMPLETE = "complete"


class WebSocketMessage(BaseModel):
    """Base WebSocket message structure."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: WebSocketMessageType
    timestamp: datetime = Field(default_factory=datetime.now)
    payload: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class ConsoleLogMessage(BaseModel):
    """Console log entry for streaming."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    level: ConsoleLogLevel = ConsoleLogLevel.INFO
    source: str = Field(default="system", description="Source of the log (agent name, tool, etc)")
    message: str
    conversation_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}

    def to_ws_message(self) -> WebSocketMessage:
        """Convert to WebSocket message."""
        return WebSocketMessage(
            type=WebSocketMessageType.CONSOLE_LOG,
            payload=self.model_dump(),
        )


class ReasoningStepMessage(BaseModel):
    """Reasoning loop step for visualization."""
    conversation_id: str
    step_number: int
    phase: ReasoningPhase
    title: str
    content: str
    cortex_query: Optional[str] = None
    cortex_result: Optional[str] = None
    duration_ms: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}

    def to_ws_message(self) -> WebSocketMessage:
        """Convert to WebSocket message."""
        return WebSocketMessage(
            type=WebSocketMessageType.REASONING_STEP,
            payload=self.model_dump(),
        )


class AgentActivityMessage(BaseModel):
    """Agent activity update."""
    agent_id: str
    agent_name: str
    status: str = "active"  # active, idle, busy, error
    current_task: Optional[str] = None
    message_count: int = 0
    last_activity: datetime = Field(default_factory=datetime.now)
    capabilities: List[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    def to_ws_message(self) -> WebSocketMessage:
        """Convert to WebSocket message."""
        return WebSocketMessage(
            type=WebSocketMessageType.AGENT_STATUS,
            payload=self.model_dump(),
        )


class CortexQueryMessage(BaseModel):
    """Cortex query for visualization."""
    conversation_id: str
    function: str  # COMPLETE, SUMMARIZE, etc.
    query: str
    model: Optional[str] = None

    model_config = {"extra": "allow"}

    def to_ws_message(self) -> WebSocketMessage:
        """Convert to WebSocket message."""
        return WebSocketMessage(
            type=WebSocketMessageType.CORTEX_QUERY,
            payload=self.model_dump(),
        )


class CortexResultMessage(BaseModel):
    """Cortex result for visualization."""
    conversation_id: str
    function: str
    result: Any
    duration_ms: int
    success: bool
    error: Optional[str] = None

    model_config = {"extra": "allow"}

    def to_ws_message(self) -> WebSocketMessage:
        """Convert to WebSocket message."""
        return WebSocketMessage(
            type=WebSocketMessageType.CORTEX_RESULT,
            payload=self.model_dump(),
        )


class SubscriptionRequest(BaseModel):
    """Request to subscribe to specific channels."""
    channels: List[str] = Field(
        default_factory=lambda: ["console", "reasoning", "agents"],
        description="Channels to subscribe to"
    )
    conversation_id: Optional[str] = Field(
        default=None,
        description="Filter to specific conversation"
    )
    agent_id: Optional[str] = Field(
        default=None,
        description="Filter to specific agent"
    )

    model_config = {"extra": "allow"}


class ConnectionInfo(BaseModel):
    """Information about a WebSocket connection."""
    connection_id: str
    client_ip: str
    connected_at: datetime
    subscriptions: List[str] = Field(default_factory=list)
    message_count: int = 0
    last_activity: datetime = Field(default_factory=datetime.now)

    model_config = {"extra": "allow"}
