"""
Pydantic models and types for CortexAgent.

Defines all data structures used across the Cortex Agent module including:
- Message types for communication console
- Agent states for reasoning loop
- Cortex function enumerations
- Configuration models
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class MessageType(str, Enum):
    """Types of messages in the communication console."""
    REQUEST = "request"           # User/system request
    RESPONSE = "response"         # Final response to user
    THINKING = "thinking"         # Internal reasoning step
    PLAN = "plan"                 # Execution plan
    OBSERVATION = "observation"   # Data observation
    EXECUTION = "execution"       # Step execution result
    REFLECTION = "reflection"     # Goal completion check
    ERROR = "error"               # Error message
    SYSTEM = "system"             # System message


class AgentState(str, Enum):
    """States in the reasoning loop."""
    IDLE = "idle"                 # No active task
    OBSERVING = "observing"       # Analyzing goal and context
    PLANNING = "planning"         # Creating execution plan
    EXECUTING = "executing"       # Running a step
    REFLECTING = "reflecting"     # Checking if goal complete
    SYNTHESIZING = "synthesizing" # Combining results
    COMPLETED = "completed"       # Task finished
    ERROR = "error"               # Error state


class CortexFunction(str, Enum):
    """Available Snowflake Cortex AI functions."""
    COMPLETE = "COMPLETE"             # Text generation
    SUMMARIZE = "SUMMARIZE"           # Text summarization
    SENTIMENT = "SENTIMENT"           # Sentiment analysis
    TRANSLATE = "TRANSLATE"           # Language translation
    EXTRACT_ANSWER = "EXTRACT_ANSWER" # QA extraction
    EMBED_TEXT = "EMBED_TEXT"         # Text embeddings (future)


class CortexModel(str, Enum):
    """Available Cortex LLM models."""
    MISTRAL_LARGE = "mistral-large"
    MISTRAL_7B = "mistral-7b"
    LLAMA3_8B = "llama3-8b"
    LLAMA3_70B = "llama3-70b"
    CLAUDE_3_SONNET = "claude-3-sonnet"
    GEMMA_7B = "gemma-7b"


@dataclass
class AgentMessage:
    """A message in the communication console."""
    id: str
    conversation_id: str
    timestamp: datetime
    from_agent: str
    to_agent: str
    message_type: MessageType
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    thinking: Optional[str] = None
    cortex_query: Optional[str] = None
    cortex_result: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "conversation_id": self.conversation_id,
            "timestamp": self.timestamp.isoformat(),
            "from_agent": self.from_agent,
            "to_agent": self.to_agent,
            "message_type": self.message_type.value,
            "content": self.content,
            "metadata": self.metadata,
            "thinking": self.thinking,
            "cortex_query": self.cortex_query,
            "cortex_result": self.cortex_result,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentMessage":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            conversation_id=data["conversation_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            from_agent=data["from_agent"],
            to_agent=data["to_agent"],
            message_type=MessageType(data["message_type"]),
            content=data["content"],
            metadata=data.get("metadata", {}),
            thinking=data.get("thinking"),
            cortex_query=data.get("cortex_query"),
            cortex_result=data.get("cortex_result"),
        )

    @classmethod
    def create(
        cls,
        conversation_id: str,
        from_agent: str,
        to_agent: str,
        message_type: MessageType,
        content: str,
        **kwargs
    ) -> "AgentMessage":
        """Factory method to create a new message."""
        return cls(
            id=str(uuid.uuid4()),
            conversation_id=conversation_id,
            timestamp=datetime.now(),
            from_agent=from_agent,
            to_agent=to_agent,
            message_type=message_type,
            content=content,
            **kwargs
        )


@dataclass
class ThinkingStep:
    """A single step in the reasoning process."""
    step_number: int
    phase: AgentState
    content: str
    cortex_function: Optional[CortexFunction] = None
    cortex_query: Optional[str] = None
    cortex_result: Optional[str] = None
    duration_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_number": self.step_number,
            "phase": self.phase.value,
            "content": self.content,
            "cortex_function": self.cortex_function.value if self.cortex_function else None,
            "cortex_query": self.cortex_query,
            "cortex_result": self.cortex_result,
            "duration_ms": self.duration_ms,
        }


@dataclass
class PlanStep:
    """A step in the execution plan."""
    step_number: int
    action: str
    description: str
    cortex_function: Optional[CortexFunction] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_number": self.step_number,
            "action": self.action,
            "description": self.description,
            "cortex_function": self.cortex_function.value if self.cortex_function else None,
            "parameters": self.parameters,
            "depends_on": self.depends_on,
        }


@dataclass
class ExecutionPlan:
    """An execution plan for achieving a goal."""
    plan_id: str
    goal: str
    steps: List[PlanStep]
    estimated_steps: int
    confidence: float
    reasoning: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "plan_id": self.plan_id,
            "goal": self.goal,
            "steps": [s.to_dict() for s in self.steps],
            "estimated_steps": self.estimated_steps,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
        }


@dataclass
class StepResult:
    """Result of executing a plan step."""
    step_number: int
    success: bool
    result: Any
    error: Optional[str] = None
    cortex_query: Optional[str] = None
    cortex_raw_result: Optional[str] = None
    duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_number": self.step_number,
            "success": self.success,
            "result": self.result,
            "error": self.error,
            "cortex_query": self.cortex_query,
            "cortex_raw_result": self.cortex_raw_result,
            "duration_ms": self.duration_ms,
        }


@dataclass
class Conversation:
    """A full conversation with thinking steps."""
    conversation_id: str
    goal: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    state: AgentState = AgentState.IDLE
    messages: List[AgentMessage] = field(default_factory=list)
    thinking_steps: List[ThinkingStep] = field(default_factory=list)
    plan: Optional[ExecutionPlan] = None
    final_result: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "conversation_id": self.conversation_id,
            "goal": self.goal,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "state": self.state.value,
            "messages": [m.to_dict() for m in self.messages],
            "thinking_steps": [t.to_dict() for t in self.thinking_steps],
            "plan": self.plan.to_dict() if self.plan else None,
            "final_result": self.final_result,
        }


@dataclass
class AgentResponse:
    """Final response from the reasoning loop."""
    conversation_id: str
    goal: str
    success: bool
    result: str
    thinking_steps: List[ThinkingStep]
    total_cortex_calls: int
    total_duration_ms: int
    plan: Optional[ExecutionPlan] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "conversation_id": self.conversation_id,
            "goal": self.goal,
            "success": self.success,
            "result": self.result,
            "thinking_steps": [t.to_dict() for t in self.thinking_steps],
            "total_cortex_calls": self.total_cortex_calls,
            "total_duration_ms": self.total_duration_ms,
            "plan": self.plan.to_dict() if self.plan else None,
            "error": self.error,
        }


class CortexAgentConfig(BaseModel):
    """Configuration for CortexAgent."""

    connection_id: str = Field(
        ...,
        description="Reference to existing Snowflake connection"
    )
    cortex_model: str = Field(
        default="mistral-large",
        description="Default Cortex model for COMPLETE()"
    )
    max_reasoning_steps: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum steps in reasoning loop"
    )
    temperature: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Temperature for Cortex COMPLETE()"
    )
    enable_console: bool = Field(
        default=True,
        description="Enable communication console logging"
    )
    console_outputs: List[str] = Field(
        default=["cli", "file"],
        description="Console output targets (cli, file, database, websocket)"
    )

    model_config = {"extra": "allow"}


class CortexQueryResult(BaseModel):
    """Result from a Cortex SQL query."""

    function: CortexFunction
    query: str
    result: Any
    duration_ms: int
    success: bool
    error: Optional[str] = None
    raw_response: Optional[str] = None

    model_config = {"extra": "allow"}
