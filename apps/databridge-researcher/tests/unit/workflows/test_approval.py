"""
Unit tests for approval workflow queue module.
"""

import pytest
from datetime import datetime, timezone, timedelta

from src.workflows.approval import (
    ApprovalStatus,
    ApprovalPriority,
    ApprovalType,
    ApprovalRequest,
    ApprovalResult,
    ApprovalQueue,
    get_approval_queue,
    reset_approval_queue,
)
from src.workflows.events import reset_workflow_event_bus


class TestApprovalStatus:
    """Tests for ApprovalStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected statuses exist."""
        assert ApprovalStatus.PENDING == "pending"
        assert ApprovalStatus.APPROVED == "approved"
        assert ApprovalStatus.REJECTED == "rejected"
        assert ApprovalStatus.EXPIRED == "expired"
        assert ApprovalStatus.DELEGATED == "delegated"
        assert ApprovalStatus.CANCELLED == "cancelled"


class TestApprovalPriority:
    """Tests for ApprovalPriority enum."""

    def test_all_priorities_exist(self):
        """Test all expected priorities exist."""
        assert ApprovalPriority.LOW == "low"
        assert ApprovalPriority.NORMAL == "normal"
        assert ApprovalPriority.HIGH == "high"
        assert ApprovalPriority.URGENT == "urgent"


class TestApprovalType:
    """Tests for ApprovalType enum."""

    def test_all_types_exist(self):
        """Test all expected types exist."""
        assert ApprovalType.STEP_COMPLETION == "step_completion"
        assert ApprovalType.PERIOD_LOCK == "period_lock"
        assert ApprovalType.ADJUSTMENT == "adjustment"
        assert ApprovalType.FORECAST_PUBLISH == "forecast_publish"
        assert ApprovalType.VARIANCE_REVIEW == "variance_review"
        assert ApprovalType.CUSTOM == "custom"


class TestApprovalRequest:
    """Tests for ApprovalRequest dataclass."""

    def test_basic_creation(self):
        """Test creating a basic request."""
        request = ApprovalRequest(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Complete Data Sync",
            description="Please approve the data sync step",
            requested_by="analyst@example.com",
        )

        assert request.workflow_type == "monthly_close"
        assert request.status == ApprovalStatus.PENDING
        assert request.approval_id is not None

    def test_to_dict(self):
        """Test serialization to dictionary."""
        request = ApprovalRequest(
            workflow_type="variance",
            approval_type=ApprovalType.VARIANCE_REVIEW,
            title="Review Variance",
            requested_by="analyst@example.com",
            approvers=["manager@example.com"],
            priority=ApprovalPriority.HIGH,
        )

        data = request.to_dict()

        assert data["workflow_type"] == "variance"
        assert data["approval_type"] == "variance_review"
        assert data["priority"] == "high"
        assert "manager@example.com" in data["approvers"]

    def test_is_overdue_false_when_no_deadline(self):
        """Test is_overdue returns False when no deadline."""
        request = ApprovalRequest()
        request.deadline = None

        assert request.is_overdue is False

    def test_is_overdue_true_when_past_deadline(self):
        """Test is_overdue returns True when past deadline."""
        request = ApprovalRequest()
        request.deadline = datetime.now(timezone.utc) - timedelta(hours=1)

        assert request.is_overdue is True

    def test_is_overdue_false_when_not_pending(self):
        """Test is_overdue returns False when not pending."""
        request = ApprovalRequest()
        request.deadline = datetime.now(timezone.utc) - timedelta(hours=1)
        request.status = ApprovalStatus.APPROVED

        assert request.is_overdue is False

    def test_time_remaining_hours(self):
        """Test time_remaining_hours calculation."""
        request = ApprovalRequest()
        request.deadline = datetime.now(timezone.utc) + timedelta(hours=5)

        remaining = request.time_remaining_hours
        assert remaining is not None
        assert 4.9 < remaining < 5.1

    def test_time_remaining_none_when_no_deadline(self):
        """Test time_remaining_hours returns None when no deadline."""
        request = ApprovalRequest()
        request.deadline = None

        assert request.time_remaining_hours is None

    def test_add_note(self):
        """Test adding notes to request."""
        request = ApprovalRequest()
        request.add_note("user@example.com", "This looks good")

        assert len(request.notes) == 1
        assert request.notes[0]["author"] == "user@example.com"
        assert request.notes[0]["content"] == "This looks good"
        assert "timestamp" in request.notes[0]


class TestApprovalQueue:
    """Tests for ApprovalQueue class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_approval_queue()
        reset_workflow_event_bus()
        self.queue = ApprovalQueue(default_deadline_hours=24.0)

    def test_request_approval(self):
        """Test creating an approval request."""
        result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Complete Data Sync",
            description="Please approve",
            requested_by="analyst@example.com",
        )

        assert result.success is True
        assert result.approval is not None
        assert result.approval.status == ApprovalStatus.PENDING

    def test_request_approval_with_approvers(self):
        """Test creating a request with specific approvers."""
        result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.PERIOD_LOCK,
            title="Lock Period",
            description="Lock January 2024",
            requested_by="analyst@example.com",
            approvers=["manager@example.com", "controller@example.com"],
        )

        assert result.success is True
        assert "manager@example.com" in result.approval.approvers

    def test_request_approval_with_custom_deadline(self):
        """Test creating a request with custom deadline."""
        result = self.queue.request_approval(
            workflow_type="variance",
            approval_type=ApprovalType.VARIANCE_REVIEW,
            title="Review Variance",
            description="Review budget variance",
            requested_by="analyst@example.com",
            deadline_hours=48.0,
        )

        assert result.success is True
        # Deadline should be ~48 hours from now
        deadline_diff = result.approval.deadline - datetime.now(timezone.utc)
        assert 47.9 < deadline_diff.total_seconds() / 3600 < 48.1

    def test_approve_pending_request(self):
        """Test approving a pending request."""
        # Create request
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )

        # Approve
        result = self.queue.approve(
            approval_id=create_result.approval.approval_id,
            approver="manager@example.com",
            notes="Looks good",
        )

        assert result.success is True
        assert result.approval.status == ApprovalStatus.APPROVED
        assert result.approval.approved_by == "manager@example.com"

    def test_approve_with_restricted_approvers(self):
        """Test approval with restricted approvers list."""
        # Create request with specific approvers
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.PERIOD_LOCK,
            title="Lock Period",
            description="Lock period",
            requested_by="analyst@example.com",
            approvers=["manager@example.com"],
        )

        # Try to approve with unauthorized user
        result = self.queue.approve(
            approval_id=create_result.approval.approval_id,
            approver="random@example.com",
        )

        assert result.success is False
        assert "not authorized" in result.message.lower()

    def test_approve_non_pending(self):
        """Test approving a non-pending request."""
        # Create and approve
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )

        self.queue.approve(
            approval_id=create_result.approval.approval_id,
            approver="manager@example.com",
        )

        # Try to approve again
        result = self.queue.approve(
            approval_id=create_result.approval.approval_id,
            approver="other@example.com",
        )

        assert result.success is False
        assert "not pending" in result.message.lower()

    def test_reject_request(self):
        """Test rejecting a request."""
        # Create request
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.ADJUSTMENT,
            title="Adjustment",
            description="Manual adjustment",
            requested_by="analyst@example.com",
        )

        # Reject
        result = self.queue.reject(
            approval_id=create_result.approval.approval_id,
            rejector="manager@example.com",
            reason="Insufficient documentation",
        )

        assert result.success is True
        assert result.approval.status == ApprovalStatus.REJECTED
        assert result.approval.rejection_reason == "Insufficient documentation"

    def test_delegate_request(self):
        """Test delegating a request."""
        # Create request
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
            approvers=["manager@example.com"],
        )

        # Delegate
        result = self.queue.delegate(
            approval_id=create_result.approval.approval_id,
            delegator="manager@example.com",
            delegate_to="deputy@example.com",
            notes="I'm on vacation",
        )

        assert result.success is True
        assert result.approval.delegated_to == "deputy@example.com"
        assert "deputy@example.com" in result.approval.approvers

    def test_cancel_request(self):
        """Test cancelling a request."""
        # Create request
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )

        # Cancel (by requestor)
        result = self.queue.cancel(
            approval_id=create_result.approval.approval_id,
            cancelled_by="analyst@example.com",
            reason="No longer needed",
        )

        assert result.success is True
        assert result.approval.status == ApprovalStatus.CANCELLED

    def test_cancel_by_non_requestor(self):
        """Test that only requestor can cancel."""
        # Create request
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )

        # Try to cancel by another user
        result = self.queue.cancel(
            approval_id=create_result.approval.approval_id,
            cancelled_by="other@example.com",
        )

        assert result.success is False
        assert "requestor" in result.message.lower()

    def test_expire_overdue(self):
        """Test expiring overdue requests."""
        # Create request with very short deadline
        create_result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
            deadline_hours=0.0,  # Immediate deadline
        )

        # Force deadline to past
        create_result.approval.deadline = datetime.now(timezone.utc) - timedelta(hours=1)

        # Expire overdue
        result = self.queue.expire_overdue()

        assert result.success is True
        assert len(result.approvals) == 1
        assert result.approvals[0].status == ApprovalStatus.EXPIRED

    def test_get_pending(self):
        """Test getting pending requests."""
        # Create multiple requests
        self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test 1",
            description="Test",
            requested_by="analyst@example.com",
        )

        self.queue.request_approval(
            workflow_type="variance",
            approval_type=ApprovalType.VARIANCE_REVIEW,
            title="Test 2",
            description="Test",
            requested_by="analyst@example.com",
        )

        # Get all pending
        pending = self.queue.get_pending()
        assert len(pending) == 2

        # Filter by workflow type
        close_pending = self.queue.get_pending(workflow_type="monthly_close")
        assert len(close_pending) == 1

    def test_get_pending_by_approver(self):
        """Test getting pending requests for specific approver."""
        # Create request with specific approvers
        self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.PERIOD_LOCK,
            title="Lock Period",
            description="Test",
            requested_by="analyst@example.com",
            approvers=["manager@example.com"],
        )

        # Create request without approvers (anyone can approve)
        self.queue.request_approval(
            workflow_type="variance",
            approval_type=ApprovalType.VARIANCE_REVIEW,
            title="Review",
            description="Test",
            requested_by="analyst@example.com",
        )

        # Manager should see both
        manager_pending = self.queue.get_pending(approver="manager@example.com")
        assert len(manager_pending) == 2

        # Random user should only see the one without approvers
        other_pending = self.queue.get_pending(approver="random@example.com")
        assert len(other_pending) == 1

    def test_get_pending_sorted_by_priority(self):
        """Test that pending requests are sorted by priority."""
        self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Low Priority",
            description="Test",
            requested_by="analyst@example.com",
            priority=ApprovalPriority.LOW,
        )

        self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.PERIOD_LOCK,
            title="Urgent",
            description="Test",
            requested_by="analyst@example.com",
            priority=ApprovalPriority.URGENT,
        )

        pending = self.queue.get_pending()

        assert pending[0].priority == ApprovalPriority.URGENT
        assert pending[1].priority == ApprovalPriority.LOW

    def test_get_by_status(self):
        """Test getting requests by status."""
        # Create and approve one
        result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )
        self.queue.approve(result.approval.approval_id, "manager@example.com")

        # Create another pending
        self.queue.request_approval(
            workflow_type="variance",
            approval_type=ApprovalType.VARIANCE_REVIEW,
            title="Test 2",
            description="Test",
            requested_by="analyst@example.com",
        )

        approved = self.queue.get_by_status(ApprovalStatus.APPROVED)
        pending = self.queue.get_by_status(ApprovalStatus.PENDING)

        assert len(approved) == 1
        assert len(pending) == 1

    def test_get_by_period(self):
        """Test getting requests by period."""
        self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.PERIOD_LOCK,
            title="Lock Jan",
            description="Test",
            requested_by="analyst@example.com",
            period_key="2024-01",
        )

        self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.PERIOD_LOCK,
            title="Lock Feb",
            description="Test",
            requested_by="analyst@example.com",
            period_key="2024-02",
        )

        jan_approvals = self.queue.get_by_period("2024-01")
        assert len(jan_approvals) == 1
        assert jan_approvals[0].period_key == "2024-01"

    def test_get_summary(self):
        """Test getting approval queue summary."""
        # Create some requests in different states
        result1 = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test 1",
            description="Test",
            requested_by="analyst@example.com",
        )
        self.queue.approve(result1.approval.approval_id, "manager@example.com")

        result2 = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test 2",
            description="Test",
            requested_by="analyst@example.com",
        )
        self.queue.reject(result2.approval.approval_id, "manager@example.com", "No")

        self.queue.request_approval(
            workflow_type="variance",
            approval_type=ApprovalType.VARIANCE_REVIEW,
            title="Test 3",
            description="Test",
            requested_by="analyst@example.com",
            priority=ApprovalPriority.HIGH,
        )

        summary = self.queue.get_summary()

        assert summary["total"] == 3
        assert summary["approved"] == 1
        assert summary["rejected"] == 1
        assert summary["pending"] == 1
        assert summary["by_priority"]["high"] == 1

    def test_approval_callback(self):
        """Test approval callback is called."""
        callback_received = []

        def on_approval(approval):
            callback_received.append(approval)

        self.queue.set_approval_callback(on_approval)

        result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )

        self.queue.approve(result.approval.approval_id, "manager@example.com")

        assert len(callback_received) == 1
        assert callback_received[0].status == ApprovalStatus.APPROVED

    def test_rejection_callback(self):
        """Test rejection callback is called."""
        callback_received = []

        def on_rejection(approval):
            callback_received.append(approval)

        self.queue.set_rejection_callback(on_rejection)

        result = self.queue.request_approval(
            workflow_type="monthly_close",
            approval_type=ApprovalType.STEP_COMPLETION,
            title="Test",
            description="Test",
            requested_by="analyst@example.com",
        )

        self.queue.reject(result.approval.approval_id, "manager@example.com", "No")

        assert len(callback_received) == 1
        assert callback_received[0].status == ApprovalStatus.REJECTED


class TestSingletonBehavior:
    """Tests for singleton behavior."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        reset_approval_queue()
        reset_workflow_event_bus()

    def test_get_returns_singleton(self):
        """Test that get_approval_queue returns singleton."""
        queue1 = get_approval_queue()
        queue2 = get_approval_queue()

        assert queue1 is queue2

    def test_reset_creates_new_instance(self):
        """Test that reset creates a new instance."""
        queue1 = get_approval_queue()
        reset_approval_queue()
        queue2 = get_approval_queue()

        assert queue1 is not queue2
