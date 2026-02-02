"""
Workflow Events for DataBridge Analytics Researcher.

Extends the event system for workflow-specific events including:
- Step execution events
- Approval events
- Close period events
- Forecast events
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List, Callable
import uuid
import logging

logger = logging.getLogger(__name__)


class WorkflowEventType(str, Enum):
    """Types of workflow events."""

    # Step events
    STEP_STARTED = "workflow:step:started"
    STEP_COMPLETED = "workflow:step:completed"
    STEP_FAILED = "workflow:step:failed"
    STEP_SKIPPED = "workflow:step:skipped"

    # Approval events
    APPROVAL_REQUESTED = "workflow:approval:requested"
    APPROVAL_GRANTED = "workflow:approval:granted"
    APPROVAL_REJECTED = "workflow:approval:rejected"
    APPROVAL_EXPIRED = "workflow:approval:expired"

    # Close period events
    CLOSE_STARTED = "workflow:close:started"
    CLOSE_COMPLETED = "workflow:close:completed"
    CLOSE_LOCKED = "workflow:close:locked"
    CLOSE_VALIDATION_COMPLETED = "workflow:close:validation_completed"

    # Forecast events
    FORECAST_UPDATED = "workflow:forecast:updated"
    SCENARIO_CREATED = "workflow:scenario:created"
    SCENARIO_MODIFIED = "workflow:scenario:modified"

    # Variance events
    VARIANCE_ANALYZED = "workflow:variance:analyzed"
    VARIANCE_THRESHOLD_EXCEEDED = "workflow:variance:threshold_exceeded"

    # Recovery events
    ROLLBACK_STARTED = "workflow:rollback:started"
    ROLLBACK_COMPLETED = "workflow:rollback:completed"
    CHECKPOINT_CREATED = "workflow:checkpoint:created"


@dataclass
class WorkflowEvent:
    """Base workflow event class."""

    event_type: WorkflowEventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source: str = "researcher"
    workflow_type: str = ""  # monthly_close, variance, forecast
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "workflow_type": self.workflow_type,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkflowEvent":
        """Create from dictionary."""
        return cls(
            event_id=data.get("event_id", str(uuid.uuid4())),
            event_type=WorkflowEventType(data["event_type"]),
            timestamp=datetime.fromisoformat(data["timestamp"])
            if "timestamp" in data
            else datetime.now(timezone.utc),
            source=data.get("source", "researcher"),
            workflow_type=data.get("workflow_type", ""),
            metadata=data.get("metadata", {}),
        )


@dataclass
class StepEvent(WorkflowEvent):
    """Event for workflow step changes."""

    step_id: str = ""
    step_name: str = ""
    step_type: str = ""
    period_key: str = ""
    duration_seconds: float = 0.0
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update({
            "step_id": self.step_id,
            "step_name": self.step_name,
            "step_type": self.step_type,
            "period_key": self.period_key,
            "duration_seconds": self.duration_seconds,
            "result": self.result,
            "error_message": self.error_message,
        })
        return data


@dataclass
class ApprovalEvent(WorkflowEvent):
    """Event for approval workflow changes."""

    approval_id: str = ""
    step_id: str = ""
    approver: str = ""
    requested_by: str = ""
    approval_type: str = ""  # step_completion, period_lock, adjustment
    notes: str = ""
    deadline: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update({
            "approval_id": self.approval_id,
            "step_id": self.step_id,
            "approver": self.approver,
            "requested_by": self.requested_by,
            "approval_type": self.approval_type,
            "notes": self.notes,
            "deadline": self.deadline.isoformat() if self.deadline else None,
        })
        return data


@dataclass
class CloseEvent(WorkflowEvent):
    """Event for close period changes."""

    period_key: str = ""
    year: int = 0
    month: int = 0
    fiscal_year: Optional[int] = None
    fiscal_period: Optional[int] = None
    locked_by: Optional[str] = None
    completed_steps: int = 0
    total_steps: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update({
            "period_key": self.period_key,
            "year": self.year,
            "month": self.month,
            "fiscal_year": self.fiscal_year,
            "fiscal_period": self.fiscal_period,
            "locked_by": self.locked_by,
            "completed_steps": self.completed_steps,
            "total_steps": self.total_steps,
        })
        return data


@dataclass
class ForecastEvent(WorkflowEvent):
    """Event for forecast changes."""

    scenario_id: str = ""
    scenario_name: str = ""
    fiscal_year: str = ""
    as_of_period: str = ""
    method: str = ""
    full_year_outlook: float = 0.0
    variance_to_budget: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update({
            "scenario_id": self.scenario_id,
            "scenario_name": self.scenario_name,
            "fiscal_year": self.fiscal_year,
            "as_of_period": self.as_of_period,
            "method": self.method,
            "full_year_outlook": self.full_year_outlook,
            "variance_to_budget": self.variance_to_budget,
        })
        return data


@dataclass
class VarianceEvent(WorkflowEvent):
    """Event for variance analysis."""

    comparison_type: str = ""  # budget, prior_year, forecast
    period: str = ""
    total_variance: float = 0.0
    variance_percent: float = 0.0
    is_favorable: bool = True
    threshold_exceeded: bool = False
    top_driver: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update({
            "comparison_type": self.comparison_type,
            "period": self.period,
            "total_variance": self.total_variance,
            "variance_percent": self.variance_percent,
            "is_favorable": self.is_favorable,
            "threshold_exceeded": self.threshold_exceeded,
            "top_driver": self.top_driver,
        })
        return data


@dataclass
class RollbackEvent(WorkflowEvent):
    """Event for rollback operations."""

    checkpoint_id: str = ""
    rollback_reason: str = ""
    steps_rolled_back: int = 0
    original_status: str = ""
    target_status: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update({
            "checkpoint_id": self.checkpoint_id,
            "rollback_reason": self.rollback_reason,
            "steps_rolled_back": self.steps_rolled_back,
            "original_status": self.original_status,
            "target_status": self.target_status,
        })
        return data


# Type alias for event handlers
WorkflowEventHandler = Callable[[WorkflowEvent], None]


class WorkflowEventBus:
    """
    Event bus for workflow events.

    Provides publish/subscribe functionality for workflow-specific events.
    Can integrate with the Librarian's EventBus for cross-service communication.
    """

    def __init__(self):
        """Initialize the workflow event bus."""
        self._handlers: Dict[str, List[WorkflowEventHandler]] = {}
        self._pattern_handlers: Dict[str, List[WorkflowEventHandler]] = {}
        self._event_history: List[Dict[str, Any]] = []
        self._max_history = 500
        self._external_publisher: Optional[Callable[[WorkflowEvent], None]] = None

    def set_external_publisher(
        self,
        publisher: Callable[[WorkflowEvent], None],
    ) -> None:
        """
        Set an external publisher for cross-service events.

        Args:
            publisher: Function to publish events to external systems
        """
        self._external_publisher = publisher

    def subscribe(
        self,
        event_type: WorkflowEventType,
        handler: WorkflowEventHandler,
    ) -> None:
        """
        Subscribe to a specific event type.

        Args:
            event_type: Type of event to subscribe to
            handler: Callback function to handle the event
        """
        key = event_type.value
        if key not in self._handlers:
            self._handlers[key] = []
        if handler not in self._handlers[key]:
            self._handlers[key].append(handler)
            logger.debug(f"Subscribed handler to {key}")

    def subscribe_pattern(
        self,
        pattern: str,
        handler: WorkflowEventHandler,
    ) -> None:
        """
        Subscribe to events matching a pattern.

        Args:
            pattern: Pattern to match (e.g., "workflow:step:*", "workflow:approval:*")
            handler: Callback function to handle matching events
        """
        if pattern not in self._pattern_handlers:
            self._pattern_handlers[pattern] = []
        if handler not in self._pattern_handlers[pattern]:
            self._pattern_handlers[pattern].append(handler)
            logger.debug(f"Subscribed handler to pattern {pattern}")

    def unsubscribe(
        self,
        event_type: WorkflowEventType,
        handler: WorkflowEventHandler,
    ) -> None:
        """
        Unsubscribe from a specific event type.

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler to remove
        """
        key = event_type.value
        if key in self._handlers and handler in self._handlers[key]:
            self._handlers[key].remove(handler)
            logger.debug(f"Unsubscribed handler from {key}")

    def publish(self, event: WorkflowEvent) -> None:
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

        # Publish to external systems
        if self._external_publisher:
            try:
                self._external_publisher(event)
            except Exception as e:
                logger.error(f"External publisher error: {e}")

        logger.debug(f"Published workflow event: {event_key}")

    def _matches_pattern(self, event_key: str, pattern: str) -> bool:
        """Check if event key matches a pattern."""
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return event_key.startswith(prefix)
        return event_key == pattern

    def _record_event(self, event: WorkflowEvent) -> None:
        """Record event in history."""
        self._event_history.append(event.to_dict())
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

    def get_event_history(
        self,
        event_type: Optional[WorkflowEventType] = None,
        workflow_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get recent event history.

        Args:
            event_type: Optional filter by event type
            workflow_type: Optional filter by workflow type
            limit: Maximum events to return

        Returns:
            List of event dictionaries
        """
        events = self._event_history

        if event_type:
            events = [e for e in events if e["event_type"] == event_type.value]

        if workflow_type:
            events = [e for e in events if e.get("workflow_type") == workflow_type]

        return events[-limit:]

    def clear_handlers(self) -> None:
        """Clear all handlers (useful for testing)."""
        self._handlers.clear()
        self._pattern_handlers.clear()

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()


# Global workflow event bus instance
_workflow_event_bus: Optional[WorkflowEventBus] = None


def get_workflow_event_bus() -> WorkflowEventBus:
    """Get the global workflow event bus instance."""
    global _workflow_event_bus
    if _workflow_event_bus is None:
        _workflow_event_bus = WorkflowEventBus()
    return _workflow_event_bus


def reset_workflow_event_bus() -> None:
    """Reset the global workflow event bus (for testing)."""
    global _workflow_event_bus
    _workflow_event_bus = None
