"""
Unit tests for workflow events module.
"""

import pytest
from datetime import datetime, timezone

from src.workflows.events import (
    WorkflowEventType,
    WorkflowEvent,
    StepEvent,
    ApprovalEvent,
    CloseEvent,
    ForecastEvent,
    VarianceEvent,
    RollbackEvent,
    WorkflowEventBus,
    get_workflow_event_bus,
    reset_workflow_event_bus,
)


class TestWorkflowEventType:
    """Tests for WorkflowEventType enum."""

    def test_step_events(self):
        """Test step event types exist."""
        assert WorkflowEventType.STEP_STARTED == "workflow:step:started"
        assert WorkflowEventType.STEP_COMPLETED == "workflow:step:completed"
        assert WorkflowEventType.STEP_FAILED == "workflow:step:failed"
        assert WorkflowEventType.STEP_SKIPPED == "workflow:step:skipped"

    def test_approval_events(self):
        """Test approval event types exist."""
        assert WorkflowEventType.APPROVAL_REQUESTED == "workflow:approval:requested"
        assert WorkflowEventType.APPROVAL_GRANTED == "workflow:approval:granted"
        assert WorkflowEventType.APPROVAL_REJECTED == "workflow:approval:rejected"
        assert WorkflowEventType.APPROVAL_EXPIRED == "workflow:approval:expired"

    def test_close_events(self):
        """Test close event types exist."""
        assert WorkflowEventType.CLOSE_STARTED == "workflow:close:started"
        assert WorkflowEventType.CLOSE_COMPLETED == "workflow:close:completed"
        assert WorkflowEventType.CLOSE_LOCKED == "workflow:close:locked"

    def test_forecast_events(self):
        """Test forecast event types exist."""
        assert WorkflowEventType.FORECAST_UPDATED == "workflow:forecast:updated"
        assert WorkflowEventType.SCENARIO_CREATED == "workflow:scenario:created"

    def test_rollback_events(self):
        """Test rollback event types exist."""
        assert WorkflowEventType.ROLLBACK_STARTED == "workflow:rollback:started"
        assert WorkflowEventType.ROLLBACK_COMPLETED == "workflow:rollback:completed"
        assert WorkflowEventType.CHECKPOINT_CREATED == "workflow:checkpoint:created"


class TestWorkflowEvent:
    """Tests for WorkflowEvent dataclass."""

    def test_basic_creation(self):
        """Test creating a basic event."""
        event = WorkflowEvent(
            event_type=WorkflowEventType.STEP_STARTED,
            workflow_type="monthly_close",
        )

        assert event.event_type == WorkflowEventType.STEP_STARTED
        assert event.workflow_type == "monthly_close"
        assert event.source == "researcher"
        assert event.event_id is not None

    def test_to_dict(self):
        """Test serialization to dictionary."""
        event = WorkflowEvent(
            event_type=WorkflowEventType.STEP_COMPLETED,
            workflow_type="variance",
            metadata={"key": "value"},
        )

        data = event.to_dict()

        assert data["event_type"] == "workflow:step:completed"
        assert data["workflow_type"] == "variance"
        assert data["metadata"]["key"] == "value"
        assert "timestamp" in data

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "event_type": "workflow:step:completed",
            "workflow_type": "forecast",
            "timestamp": "2024-01-15T10:00:00+00:00",
        }

        event = WorkflowEvent.from_dict(data)

        assert event.event_type == WorkflowEventType.STEP_COMPLETED
        assert event.workflow_type == "forecast"


class TestStepEvent:
    """Tests for StepEvent dataclass."""

    def test_creation(self):
        """Test creating a step event."""
        event = StepEvent(
            event_type=WorkflowEventType.STEP_COMPLETED,
            workflow_type="monthly_close",
            step_id="data_sync",
            step_name="Data Sync",
            step_type="data_sync",
            period_key="2024-01",
            duration_seconds=5.5,
            result={"records": 100},
        )

        assert event.step_id == "data_sync"
        assert event.duration_seconds == 5.5
        assert event.result["records"] == 100

    def test_to_dict_includes_step_fields(self):
        """Test that to_dict includes step-specific fields."""
        event = StepEvent(
            event_type=WorkflowEventType.STEP_FAILED,
            step_id="validation",
            step_name="Validation",
            error_message="Validation failed",
        )

        data = event.to_dict()

        assert data["step_id"] == "validation"
        assert data["step_name"] == "Validation"
        assert data["error_message"] == "Validation failed"


class TestApprovalEvent:
    """Tests for ApprovalEvent dataclass."""

    def test_creation(self):
        """Test creating an approval event."""
        event = ApprovalEvent(
            event_type=WorkflowEventType.APPROVAL_REQUESTED,
            workflow_type="monthly_close",
            approval_id="apr-123",
            step_id="mgmt_review",
            approver="manager@example.com",
            requested_by="analyst@example.com",
            approval_type="step_completion",
        )

        assert event.approval_id == "apr-123"
        assert event.approver == "manager@example.com"
        assert event.approval_type == "step_completion"

    def test_to_dict_includes_deadline(self):
        """Test that to_dict includes deadline."""
        deadline = datetime(2024, 1, 15, 18, 0, 0, tzinfo=timezone.utc)
        event = ApprovalEvent(
            event_type=WorkflowEventType.APPROVAL_REQUESTED,
            approval_id="apr-456",
            deadline=deadline,
        )

        data = event.to_dict()

        assert data["deadline"] == deadline.isoformat()


class TestCloseEvent:
    """Tests for CloseEvent dataclass."""

    def test_creation(self):
        """Test creating a close event."""
        event = CloseEvent(
            event_type=WorkflowEventType.CLOSE_LOCKED,
            workflow_type="monthly_close",
            period_key="2024-01",
            year=2024,
            month=1,
            fiscal_year=2024,
            fiscal_period=1,
            locked_by="controller@example.com",
            completed_steps=8,
            total_steps=8,
        )

        assert event.period_key == "2024-01"
        assert event.locked_by == "controller@example.com"
        assert event.completed_steps == 8

    def test_to_dict(self):
        """Test serialization."""
        event = CloseEvent(
            event_type=WorkflowEventType.CLOSE_COMPLETED,
            period_key="2024-02",
            year=2024,
            month=2,
        )

        data = event.to_dict()

        assert data["period_key"] == "2024-02"
        assert data["year"] == 2024
        assert data["month"] == 2


class TestForecastEvent:
    """Tests for ForecastEvent dataclass."""

    def test_creation(self):
        """Test creating a forecast event."""
        event = ForecastEvent(
            event_type=WorkflowEventType.FORECAST_UPDATED,
            workflow_type="forecast",
            scenario_id="base",
            scenario_name="Base Forecast",
            fiscal_year="2024",
            as_of_period="2024-06",
            method="ytd_run_rate",
            full_year_outlook=1000000.0,
            variance_to_budget=-50000.0,
        )

        assert event.scenario_id == "base"
        assert event.full_year_outlook == 1000000.0
        assert event.variance_to_budget == -50000.0


class TestVarianceEvent:
    """Tests for VarianceEvent dataclass."""

    def test_creation(self):
        """Test creating a variance event."""
        event = VarianceEvent(
            event_type=WorkflowEventType.VARIANCE_ANALYZED,
            workflow_type="variance",
            comparison_type="budget",
            period="2024-01",
            total_variance=50000.0,
            variance_percent=5.0,
            is_favorable=True,
            threshold_exceeded=False,
        )

        assert event.comparison_type == "budget"
        assert event.is_favorable is True

    def test_threshold_exceeded(self):
        """Test threshold exceeded flag."""
        event = VarianceEvent(
            event_type=WorkflowEventType.VARIANCE_THRESHOLD_EXCEEDED,
            total_variance=-100000.0,
            variance_percent=-15.0,
            is_favorable=False,
            threshold_exceeded=True,
            top_driver={"dimension": "Product", "variance": -80000.0},
        )

        assert event.threshold_exceeded is True
        assert event.top_driver["dimension"] == "Product"


class TestRollbackEvent:
    """Tests for RollbackEvent dataclass."""

    def test_creation(self):
        """Test creating a rollback event."""
        event = RollbackEvent(
            event_type=WorkflowEventType.ROLLBACK_COMPLETED,
            workflow_type="monthly_close",
            checkpoint_id="chk-123",
            rollback_reason="Validation failed",
            steps_rolled_back=3,
            original_status="completed",
            target_status="pending",
        )

        assert event.checkpoint_id == "chk-123"
        assert event.steps_rolled_back == 3


class TestWorkflowEventBus:
    """Tests for WorkflowEventBus class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_workflow_event_bus()
        self.bus = WorkflowEventBus()
        self.received_events = []

    def test_subscribe_and_publish(self):
        """Test basic subscribe and publish."""
        def handler(event):
            self.received_events.append(event)

        self.bus.subscribe(WorkflowEventType.STEP_COMPLETED, handler)

        event = WorkflowEvent(event_type=WorkflowEventType.STEP_COMPLETED)
        self.bus.publish(event)

        assert len(self.received_events) == 1
        assert self.received_events[0].event_type == WorkflowEventType.STEP_COMPLETED

    def test_pattern_subscription(self):
        """Test pattern-based subscription."""
        def handler(event):
            self.received_events.append(event)

        self.bus.subscribe_pattern("workflow:step:*", handler)

        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_STARTED))
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_COMPLETED))
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.APPROVAL_REQUESTED))

        assert len(self.received_events) == 2

    def test_unsubscribe(self):
        """Test unsubscribing from events."""
        def handler(event):
            self.received_events.append(event)

        self.bus.subscribe(WorkflowEventType.STEP_FAILED, handler)
        self.bus.unsubscribe(WorkflowEventType.STEP_FAILED, handler)

        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_FAILED))

        assert len(self.received_events) == 0

    def test_event_history(self):
        """Test event history tracking."""
        self.bus.publish(WorkflowEvent(
            event_type=WorkflowEventType.STEP_STARTED,
            workflow_type="close"
        ))
        self.bus.publish(WorkflowEvent(
            event_type=WorkflowEventType.STEP_COMPLETED,
            workflow_type="close"
        ))
        self.bus.publish(WorkflowEvent(
            event_type=WorkflowEventType.APPROVAL_REQUESTED,
            workflow_type="approval"
        ))

        # Get all history
        history = self.bus.get_event_history()
        assert len(history) == 3

        # Filter by event type
        step_history = self.bus.get_event_history(
            event_type=WorkflowEventType.STEP_COMPLETED
        )
        assert len(step_history) == 1

        # Filter by workflow type
        close_history = self.bus.get_event_history(workflow_type="close")
        assert len(close_history) == 2

    def test_external_publisher(self):
        """Test external publisher callback."""
        external_events = []

        def external_publisher(event):
            external_events.append(event)

        self.bus.set_external_publisher(external_publisher)
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.CLOSE_STARTED))

        assert len(external_events) == 1

    def test_handler_error_isolation(self):
        """Test that handler errors don't stop other handlers."""
        def failing_handler(event):
            raise ValueError("Handler error")

        def working_handler(event):
            self.received_events.append(event)

        self.bus.subscribe(WorkflowEventType.STEP_STARTED, failing_handler)
        self.bus.subscribe(WorkflowEventType.STEP_STARTED, working_handler)

        # Should not raise despite failing handler
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_STARTED))

        assert len(self.received_events) == 1

    def test_clear_handlers(self):
        """Test clearing all handlers."""
        def handler(event):
            self.received_events.append(event)

        self.bus.subscribe(WorkflowEventType.STEP_STARTED, handler)
        self.bus.subscribe_pattern("workflow:*", handler)

        self.bus.clear_handlers()
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_STARTED))

        assert len(self.received_events) == 0

    def test_clear_history(self):
        """Test clearing event history."""
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_STARTED))
        self.bus.publish(WorkflowEvent(event_type=WorkflowEventType.STEP_COMPLETED))

        self.bus.clear_history()

        assert len(self.bus.get_event_history()) == 0


class TestSingletonBehavior:
    """Tests for singleton behavior."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        reset_workflow_event_bus()

    def test_get_returns_singleton(self):
        """Test that get_workflow_event_bus returns singleton."""
        bus1 = get_workflow_event_bus()
        bus2 = get_workflow_event_bus()

        assert bus1 is bus2

    def test_reset_creates_new_instance(self):
        """Test that reset creates a new instance."""
        bus1 = get_workflow_event_bus()
        reset_workflow_event_bus()
        bus2 = get_workflow_event_bus()

        assert bus1 is not bus2
