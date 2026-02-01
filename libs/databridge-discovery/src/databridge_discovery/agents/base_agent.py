"""
Base Agent interface for DataBridge Discovery multi-agent architecture.

Provides the abstract interface and common functionality for all agents:
- Schema Scanner
- Logic Extractor
- Warehouse Architect
- Deploy & Validate
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable
import uuid


class AgentState(str, Enum):
    """Agent execution states."""

    IDLE = "idle"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AgentCapability(str, Enum):
    """Standard agent capabilities."""

    # Schema Scanner capabilities
    SCAN_SCHEMA = "scan_schema"
    EXTRACT_METADATA = "extract_metadata"
    DETECT_KEYS = "detect_keys"
    SAMPLE_PROFILES = "sample_profiles"

    # Logic Extractor capabilities
    PARSE_SQL = "parse_sql"
    EXTRACT_CASE = "extract_case"
    IDENTIFY_CALCS = "identify_calcs"
    DETECT_AGGREGATIONS = "detect_aggregations"

    # Warehouse Architect capabilities
    DESIGN_STAR_SCHEMA = "design_star_schema"
    GENERATE_DIMS = "generate_dims"
    GENERATE_FACTS = "generate_facts"
    DBT_MODELS = "dbt_models"

    # Deploy & Validate capabilities
    EXECUTE_DDL = "execute_ddl"
    RUN_DBT = "run_dbt"
    VALIDATE_COUNTS = "validate_counts"
    COMPARE_AGGREGATES = "compare_aggregates"


@dataclass
class AgentConfig:
    """Configuration for an agent."""

    name: str
    timeout_seconds: int = 300
    max_retries: int = 3
    retry_delay_seconds: int = 5
    parallel_tasks: int = 1
    log_level: str = "INFO"
    custom_settings: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "retry_delay_seconds": self.retry_delay_seconds,
            "parallel_tasks": self.parallel_tasks,
            "log_level": self.log_level,
            "custom_settings": self.custom_settings,
        }


@dataclass
class AgentResult:
    """Result from an agent execution."""

    agent_id: str
    agent_name: str
    capability: str
    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    duration_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "capability": self.capability,
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "metadata": self.metadata,
        }


class AgentError(Exception):
    """Exception raised by agents."""

    def __init__(
        self,
        message: str,
        agent_name: str,
        capability: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.agent_name = agent_name
        self.capability = capability
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "message": str(self),
            "agent_name": self.agent_name,
            "capability": self.capability,
            "details": self.details,
        }


@dataclass
class TaskContext:
    """Context passed to agent tasks."""

    task_id: str
    workflow_id: str | None = None
    parent_task_id: str | None = None
    input_data: dict[str, Any] = field(default_factory=dict)
    shared_state: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


class BaseAgent(ABC):
    """
    Abstract base class for all discovery agents.

    Provides common functionality:
    - Configuration management
    - State management
    - Error handling
    - Logging
    - Capability registration

    Each agent must implement:
    - execute(): Main execution method
    - get_capabilities(): List of supported capabilities
    """

    def __init__(self, config: AgentConfig | None = None):
        """
        Initialize the agent.

        Args:
            config: Agent configuration
        """
        self._id = str(uuid.uuid4())[:8]
        self._config = config or AgentConfig(name=self.__class__.__name__)
        self._state = AgentState.IDLE
        self._current_task: TaskContext | None = None
        self._results: list[AgentResult] = []
        self._error_handlers: list[Callable[[AgentError], None]] = []
        self._progress_callbacks: list[Callable[[str, float], None]] = []
        self._created_at = datetime.now()
        self._last_execution_at: datetime | None = None

    @property
    def id(self) -> str:
        """Get agent ID."""
        return self._id

    @property
    def name(self) -> str:
        """Get agent name."""
        return self._config.name

    @property
    def state(self) -> AgentState:
        """Get current state."""
        return self._state

    @property
    def config(self) -> AgentConfig:
        """Get configuration."""
        return self._config

    @abstractmethod
    def get_capabilities(self) -> list[AgentCapability]:
        """
        Get the list of capabilities this agent supports.

        Returns:
            List of AgentCapability enums
        """
        pass

    @abstractmethod
    def execute(
        self,
        capability: AgentCapability,
        context: TaskContext,
        **kwargs: Any,
    ) -> AgentResult:
        """
        Execute a capability.

        Args:
            capability: The capability to execute
            context: Task context with input data
            **kwargs: Additional arguments

        Returns:
            AgentResult with execution results
        """
        pass

    def supports(self, capability: AgentCapability) -> bool:
        """
        Check if agent supports a capability.

        Args:
            capability: Capability to check

        Returns:
            True if supported
        """
        return capability in self.get_capabilities()

    def configure(self, **kwargs: Any) -> None:
        """
        Update agent configuration.

        Args:
            **kwargs: Configuration parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self._config, key):
                setattr(self._config, key, value)
            else:
                self._config.custom_settings[key] = value

    def get_status(self) -> dict[str, Any]:
        """
        Get current agent status.

        Returns:
            Status dictionary
        """
        return {
            "agent_id": self._id,
            "agent_name": self.name,
            "state": self._state.value,
            "capabilities": [c.value for c in self.get_capabilities()],
            "created_at": self._created_at.isoformat(),
            "last_execution_at": self._last_execution_at.isoformat() if self._last_execution_at else None,
            "total_executions": len(self._results),
            "successful_executions": sum(1 for r in self._results if r.success),
            "current_task": self._current_task.task_id if self._current_task else None,
        }

    def pause(self) -> bool:
        """
        Pause agent execution.

        Returns:
            True if paused successfully
        """
        if self._state == AgentState.RUNNING:
            self._state = AgentState.PAUSED
            return True
        return False

    def resume(self) -> bool:
        """
        Resume paused agent.

        Returns:
            True if resumed successfully
        """
        if self._state == AgentState.PAUSED:
            self._state = AgentState.RUNNING
            return True
        return False

    def cancel(self) -> bool:
        """
        Cancel agent execution.

        Returns:
            True if cancelled successfully
        """
        if self._state in [AgentState.RUNNING, AgentState.PAUSED]:
            self._state = AgentState.CANCELLED
            return True
        return False

    def reset(self) -> None:
        """Reset agent to idle state."""
        self._state = AgentState.IDLE
        self._current_task = None

    def on_error(self, handler: Callable[[AgentError], None]) -> None:
        """
        Register error handler.

        Args:
            handler: Callback function for errors
        """
        self._error_handlers.append(handler)

    def on_progress(self, callback: Callable[[str, float], None]) -> None:
        """
        Register progress callback.

        Args:
            callback: Callback function (message, progress 0-1)
        """
        self._progress_callbacks.append(callback)

    def _report_progress(self, message: str, progress: float) -> None:
        """
        Report progress to registered callbacks.

        Args:
            message: Progress message
            progress: Progress value (0-1)
        """
        for callback in self._progress_callbacks:
            try:
                callback(message, min(max(progress, 0), 1))
            except Exception:
                pass  # Don't fail on callback errors

    def _handle_error(self, error: AgentError) -> None:
        """
        Handle error with registered handlers.

        Args:
            error: The error to handle
        """
        for handler in self._error_handlers:
            try:
                handler(error)
            except Exception:
                pass  # Don't fail on handler errors

    def _start_execution(self, capability: AgentCapability, context: TaskContext) -> datetime:
        """
        Mark start of execution.

        Args:
            capability: Capability being executed
            context: Task context

        Returns:
            Start time
        """
        self._state = AgentState.RUNNING
        self._current_task = context
        self._last_execution_at = datetime.now()
        return self._last_execution_at

    def _complete_execution(
        self,
        capability: AgentCapability,
        start_time: datetime,
        success: bool,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> AgentResult:
        """
        Mark completion of execution.

        Args:
            capability: Capability that was executed
            start_time: Execution start time
            success: Whether execution succeeded
            data: Result data
            error: Error message if failed

        Returns:
            AgentResult
        """
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = AgentResult(
            agent_id=self._id,
            agent_name=self.name,
            capability=capability.value,
            success=success,
            data=data or {},
            error=error,
            started_at=start_time,
            completed_at=end_time,
            duration_seconds=duration,
        )

        self._results.append(result)
        self._state = AgentState.COMPLETED if success else AgentState.FAILED
        self._current_task = None

        return result

    def get_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        Get execution history.

        Args:
            limit: Maximum number of results to return

        Returns:
            List of result dictionaries
        """
        return [r.to_dict() for r in self._results[-limit:]]

    def to_dict(self) -> dict[str, Any]:
        """Convert agent to dictionary."""
        return {
            "agent_id": self._id,
            "agent_name": self.name,
            "agent_type": self.__class__.__name__,
            "state": self._state.value,
            "capabilities": [c.value for c in self.get_capabilities()],
            "config": self._config.to_dict(),
            "created_at": self._created_at.isoformat(),
            "last_execution_at": self._last_execution_at.isoformat() if self._last_execution_at else None,
        }
