"""
Approval Workflow Queue for DataBridge Analytics Researcher.

Manages pending approvals for workflow steps including:
- Approval requests
- Approval status tracking
- Expiration handling
- Delegation support
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Callable
import uuid
import logging

from .events import (
    WorkflowEventType,
    ApprovalEvent,
    get_workflow_event_bus,
)

logger = logging.getLogger(__name__)


class ApprovalStatus(str, Enum):
    """Status of an approval request."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    DELEGATED = "delegated"
    CANCELLED = "cancelled"


class ApprovalPriority(str, Enum):
    """Priority levels for approval requests."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class ApprovalType(str, Enum):
    """Types of approval requests."""
    STEP_COMPLETION = "step_completion"
    PERIOD_LOCK = "period_lock"
    ADJUSTMENT = "adjustment"
    FORECAST_PUBLISH = "forecast_publish"
    VARIANCE_REVIEW = "variance_review"
    CUSTOM = "custom"


@dataclass
class ApprovalRequest:
    """A request for approval."""

    approval_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workflow_type: str = ""
    approval_type: ApprovalType = ApprovalType.CUSTOM
    step_id: Optional[str] = None
    period_key: Optional[str] = None
    title: str = ""
    description: str = ""
    requested_by: str = ""
    approvers: List[str] = field(default_factory=list)  # List of allowed approvers
    status: ApprovalStatus = ApprovalStatus.PENDING
    priority: ApprovalPriority = ApprovalPriority.NORMAL
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    deadline: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    approved_by: Optional[str] = None
    rejection_reason: Optional[str] = None
    delegated_to: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    notes: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "approval_id": self.approval_id,
            "workflow_type": self.workflow_type,
            "approval_type": self.approval_type.value,
            "step_id": self.step_id,
            "period_key": self.period_key,
            "title": self.title,
            "description": self.description,
            "requested_by": self.requested_by,
            "approvers": self.approvers,
            "status": self.status.value,
            "priority": self.priority.value,
            "created_at": self.created_at.isoformat(),
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "approved_by": self.approved_by,
            "rejection_reason": self.rejection_reason,
            "delegated_to": self.delegated_to,
            "context": self.context,
            "notes": self.notes,
            "is_overdue": self.is_overdue,
            "time_remaining_hours": self.time_remaining_hours,
        }

    @property
    def is_overdue(self) -> bool:
        """Check if approval is overdue."""
        if self.status != ApprovalStatus.PENDING:
            return False
        if not self.deadline:
            return False
        return datetime.now(timezone.utc) > self.deadline

    @property
    def time_remaining_hours(self) -> Optional[float]:
        """Get hours remaining until deadline."""
        if not self.deadline:
            return None
        if self.status != ApprovalStatus.PENDING:
            return None
        remaining = self.deadline - datetime.now(timezone.utc)
        return remaining.total_seconds() / 3600

    def add_note(self, author: str, content: str) -> None:
        """Add a note to the approval request."""
        self.notes.append({
            "author": author,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })


@dataclass
class ApprovalResult:
    """Result of approval operations."""

    success: bool
    message: str = ""
    approval: Optional[ApprovalRequest] = None
    approvals: List[ApprovalRequest] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "approval": self.approval.to_dict() if self.approval else None,
            "approvals": [a.to_dict() for a in self.approvals],
            "errors": self.errors,
        }


class ApprovalQueue:
    """
    Queue for managing approval requests.

    Provides:
    - Approval request creation
    - Status tracking
    - Expiration handling
    - Delegation support
    - Event notifications
    """

    def __init__(
        self,
        default_deadline_hours: float = 24.0,
        auto_expire: bool = True,
    ):
        """
        Initialize the approval queue.

        Args:
            default_deadline_hours: Default deadline for approvals
            auto_expire: Whether to auto-expire overdue approvals
        """
        self.default_deadline_hours = default_deadline_hours
        self.auto_expire = auto_expire
        self._approvals: Dict[str, ApprovalRequest] = {}
        self._event_bus = get_workflow_event_bus()
        self._on_approval_callback: Optional[Callable[[ApprovalRequest], None]] = None
        self._on_rejection_callback: Optional[Callable[[ApprovalRequest], None]] = None

    def set_approval_callback(
        self,
        callback: Callable[[ApprovalRequest], None],
    ) -> None:
        """Set callback for when an approval is granted."""
        self._on_approval_callback = callback

    def set_rejection_callback(
        self,
        callback: Callable[[ApprovalRequest], None],
    ) -> None:
        """Set callback for when an approval is rejected."""
        self._on_rejection_callback = callback

    def request_approval(
        self,
        workflow_type: str,
        approval_type: ApprovalType,
        title: str,
        description: str,
        requested_by: str,
        approvers: Optional[List[str]] = None,
        step_id: Optional[str] = None,
        period_key: Optional[str] = None,
        priority: ApprovalPriority = ApprovalPriority.NORMAL,
        deadline_hours: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> ApprovalResult:
        """
        Create a new approval request.

        Args:
            workflow_type: Type of workflow (monthly_close, variance, forecast)
            approval_type: Type of approval being requested
            title: Short title for the approval
            description: Detailed description
            requested_by: User requesting approval
            approvers: List of allowed approvers (None = any)
            step_id: Optional workflow step ID
            period_key: Optional period key
            priority: Priority level
            deadline_hours: Hours until deadline (uses default if None)
            context: Additional context data

        Returns:
            ApprovalResult with created request
        """
        try:
            hours = deadline_hours or self.default_deadline_hours
            deadline = datetime.now(timezone.utc) + timedelta(hours=hours)

            approval = ApprovalRequest(
                workflow_type=workflow_type,
                approval_type=approval_type,
                step_id=step_id,
                period_key=period_key,
                title=title,
                description=description,
                requested_by=requested_by,
                approvers=approvers or [],
                priority=priority,
                deadline=deadline,
                context=context or {},
            )

            self._approvals[approval.approval_id] = approval

            # Emit event
            self._event_bus.publish(ApprovalEvent(
                event_type=WorkflowEventType.APPROVAL_REQUESTED,
                workflow_type=workflow_type,
                approval_id=approval.approval_id,
                step_id=step_id or "",
                requested_by=requested_by,
                approval_type=approval_type.value,
                deadline=deadline,
            ))

            logger.info(f"Created approval request: {approval.approval_id}")

            return ApprovalResult(
                success=True,
                message=f"Approval requested: {title}",
                approval=approval,
            )

        except Exception as e:
            logger.error(f"Failed to create approval request: {e}")
            return ApprovalResult(
                success=False,
                message=f"Failed to create approval: {str(e)}",
                errors=[str(e)],
            )

    def approve(
        self,
        approval_id: str,
        approver: str,
        notes: Optional[str] = None,
    ) -> ApprovalResult:
        """
        Approve a pending request.

        Args:
            approval_id: ID of the approval to approve
            approver: User granting approval
            notes: Optional approval notes

        Returns:
            ApprovalResult with updated approval
        """
        try:
            approval = self._approvals.get(approval_id)
            if not approval:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not found: {approval_id}",
                    errors=["Approval not found"],
                )

            if approval.status != ApprovalStatus.PENDING:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not pending: {approval.status.value}",
                    errors=[f"Cannot approve - status is {approval.status.value}"],
                )

            # Check if approver is allowed
            if approval.approvers and approver not in approval.approvers:
                return ApprovalResult(
                    success=False,
                    message=f"User '{approver}' not authorized to approve",
                    errors=["Not authorized to approve"],
                )

            # Grant approval
            approval.status = ApprovalStatus.APPROVED
            approval.approved_at = datetime.now(timezone.utc)
            approval.approved_by = approver

            if notes:
                approval.add_note(approver, notes)

            # Emit event
            self._event_bus.publish(ApprovalEvent(
                event_type=WorkflowEventType.APPROVAL_GRANTED,
                workflow_type=approval.workflow_type,
                approval_id=approval.approval_id,
                step_id=approval.step_id or "",
                approver=approver,
                requested_by=approval.requested_by,
                approval_type=approval.approval_type.value,
                notes=notes or "",
            ))

            # Call approval callback
            if self._on_approval_callback:
                try:
                    self._on_approval_callback(approval)
                except Exception as e:
                    logger.error(f"Approval callback error: {e}")

            logger.info(f"Approval granted: {approval_id} by {approver}")

            return ApprovalResult(
                success=True,
                message=f"Approved by {approver}",
                approval=approval,
            )

        except Exception as e:
            logger.error(f"Failed to approve: {e}")
            return ApprovalResult(
                success=False,
                message=f"Failed to approve: {str(e)}",
                errors=[str(e)],
            )

    def reject(
        self,
        approval_id: str,
        rejector: str,
        reason: str,
    ) -> ApprovalResult:
        """
        Reject a pending request.

        Args:
            approval_id: ID of the approval to reject
            rejector: User rejecting the approval
            reason: Reason for rejection

        Returns:
            ApprovalResult with updated approval
        """
        try:
            approval = self._approvals.get(approval_id)
            if not approval:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not found: {approval_id}",
                    errors=["Approval not found"],
                )

            if approval.status != ApprovalStatus.PENDING:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not pending: {approval.status.value}",
                    errors=[f"Cannot reject - status is {approval.status.value}"],
                )

            # Check if rejector is allowed (approvers can reject)
            if approval.approvers and rejector not in approval.approvers:
                return ApprovalResult(
                    success=False,
                    message=f"User '{rejector}' not authorized to reject",
                    errors=["Not authorized to reject"],
                )

            # Reject
            approval.status = ApprovalStatus.REJECTED
            approval.approved_at = datetime.now(timezone.utc)
            approval.approved_by = rejector
            approval.rejection_reason = reason
            approval.add_note(rejector, f"Rejected: {reason}")

            # Emit event
            self._event_bus.publish(ApprovalEvent(
                event_type=WorkflowEventType.APPROVAL_REJECTED,
                workflow_type=approval.workflow_type,
                approval_id=approval.approval_id,
                step_id=approval.step_id or "",
                approver=rejector,
                requested_by=approval.requested_by,
                approval_type=approval.approval_type.value,
                notes=reason,
            ))

            # Call rejection callback
            if self._on_rejection_callback:
                try:
                    self._on_rejection_callback(approval)
                except Exception as e:
                    logger.error(f"Rejection callback error: {e}")

            logger.info(f"Approval rejected: {approval_id} by {rejector}")

            return ApprovalResult(
                success=True,
                message=f"Rejected by {rejector}: {reason}",
                approval=approval,
            )

        except Exception as e:
            logger.error(f"Failed to reject: {e}")
            return ApprovalResult(
                success=False,
                message=f"Failed to reject: {str(e)}",
                errors=[str(e)],
            )

    def delegate(
        self,
        approval_id: str,
        delegator: str,
        delegate_to: str,
        notes: Optional[str] = None,
    ) -> ApprovalResult:
        """
        Delegate an approval to another user.

        Args:
            approval_id: ID of the approval to delegate
            delegator: User delegating the approval
            delegate_to: User to delegate to
            notes: Optional delegation notes

        Returns:
            ApprovalResult with updated approval
        """
        try:
            approval = self._approvals.get(approval_id)
            if not approval:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not found: {approval_id}",
                    errors=["Approval not found"],
                )

            if approval.status != ApprovalStatus.PENDING:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not pending: {approval.status.value}",
                    errors=[f"Cannot delegate - status is {approval.status.value}"],
                )

            # Update status
            approval.status = ApprovalStatus.DELEGATED
            approval.delegated_to = delegate_to

            # Add delegate to approvers
            if delegate_to not in approval.approvers:
                approval.approvers.append(delegate_to)

            if notes:
                approval.add_note(delegator, f"Delegated to {delegate_to}: {notes}")
            else:
                approval.add_note(delegator, f"Delegated to {delegate_to}")

            # Reset status to pending for new delegate
            approval.status = ApprovalStatus.PENDING

            logger.info(f"Approval delegated: {approval_id} to {delegate_to}")

            return ApprovalResult(
                success=True,
                message=f"Delegated to {delegate_to}",
                approval=approval,
            )

        except Exception as e:
            logger.error(f"Failed to delegate: {e}")
            return ApprovalResult(
                success=False,
                message=f"Failed to delegate: {str(e)}",
                errors=[str(e)],
            )

    def cancel(
        self,
        approval_id: str,
        cancelled_by: str,
        reason: Optional[str] = None,
    ) -> ApprovalResult:
        """
        Cancel an approval request.

        Args:
            approval_id: ID of the approval to cancel
            cancelled_by: User cancelling the request
            reason: Optional cancellation reason

        Returns:
            ApprovalResult with updated approval
        """
        try:
            approval = self._approvals.get(approval_id)
            if not approval:
                return ApprovalResult(
                    success=False,
                    message=f"Approval not found: {approval_id}",
                    errors=["Approval not found"],
                )

            if approval.status not in [ApprovalStatus.PENDING, ApprovalStatus.DELEGATED]:
                return ApprovalResult(
                    success=False,
                    message=f"Cannot cancel - status is {approval.status.value}",
                    errors=[f"Cannot cancel - status is {approval.status.value}"],
                )

            # Check if canceller is the requestor
            if cancelled_by != approval.requested_by:
                return ApprovalResult(
                    success=False,
                    message="Only the requestor can cancel",
                    errors=["Not authorized to cancel"],
                )

            approval.status = ApprovalStatus.CANCELLED
            approval.add_note(cancelled_by, f"Cancelled: {reason or 'No reason provided'}")

            logger.info(f"Approval cancelled: {approval_id}")

            return ApprovalResult(
                success=True,
                message="Approval cancelled",
                approval=approval,
            )

        except Exception as e:
            logger.error(f"Failed to cancel: {e}")
            return ApprovalResult(
                success=False,
                message=f"Failed to cancel: {str(e)}",
                errors=[str(e)],
            )

    def expire_overdue(self) -> ApprovalResult:
        """
        Expire all overdue approvals.

        Returns:
            ApprovalResult with list of expired approvals
        """
        try:
            expired = []
            now = datetime.now(timezone.utc)

            for approval in self._approvals.values():
                if approval.status == ApprovalStatus.PENDING and approval.deadline:
                    if now > approval.deadline:
                        approval.status = ApprovalStatus.EXPIRED
                        approval.add_note("system", "Expired due to deadline")
                        expired.append(approval)

                        # Emit event
                        self._event_bus.publish(ApprovalEvent(
                            event_type=WorkflowEventType.APPROVAL_EXPIRED,
                            workflow_type=approval.workflow_type,
                            approval_id=approval.approval_id,
                            step_id=approval.step_id or "",
                            requested_by=approval.requested_by,
                            approval_type=approval.approval_type.value,
                        ))

            if expired:
                logger.info(f"Expired {len(expired)} overdue approvals")

            return ApprovalResult(
                success=True,
                message=f"Expired {len(expired)} approvals",
                approvals=expired,
            )

        except Exception as e:
            logger.error(f"Failed to expire approvals: {e}")
            return ApprovalResult(
                success=False,
                message=f"Failed to expire approvals: {str(e)}",
                errors=[str(e)],
            )

    def get_approval(self, approval_id: str) -> Optional[ApprovalRequest]:
        """Get a specific approval by ID."""
        return self._approvals.get(approval_id)

    def get_pending(
        self,
        workflow_type: Optional[str] = None,
        approver: Optional[str] = None,
    ) -> List[ApprovalRequest]:
        """
        Get pending approval requests.

        Args:
            workflow_type: Optional filter by workflow type
            approver: Optional filter by approver

        Returns:
            List of pending approvals
        """
        if self.auto_expire:
            self.expire_overdue()

        approvals = [
            a for a in self._approvals.values()
            if a.status == ApprovalStatus.PENDING
        ]

        if workflow_type:
            approvals = [a for a in approvals if a.workflow_type == workflow_type]

        if approver:
            approvals = [
                a for a in approvals
                if not a.approvers or approver in a.approvers
            ]

        # Sort by priority then creation date
        priority_order = {
            ApprovalPriority.URGENT: 0,
            ApprovalPriority.HIGH: 1,
            ApprovalPriority.NORMAL: 2,
            ApprovalPriority.LOW: 3,
        }
        approvals.sort(key=lambda x: (priority_order[x.priority], x.created_at))

        return approvals

    def get_by_status(
        self,
        status: ApprovalStatus,
        workflow_type: Optional[str] = None,
    ) -> List[ApprovalRequest]:
        """
        Get approvals by status.

        Args:
            status: Status to filter by
            workflow_type: Optional filter by workflow type

        Returns:
            List of matching approvals
        """
        approvals = [
            a for a in self._approvals.values()
            if a.status == status
        ]

        if workflow_type:
            approvals = [a for a in approvals if a.workflow_type == workflow_type]

        return approvals

    def get_by_period(
        self,
        period_key: str,
        workflow_type: Optional[str] = None,
    ) -> List[ApprovalRequest]:
        """
        Get approvals for a specific period.

        Args:
            period_key: Period key to filter by
            workflow_type: Optional filter by workflow type

        Returns:
            List of matching approvals
        """
        approvals = [
            a for a in self._approvals.values()
            if a.period_key == period_key
        ]

        if workflow_type:
            approvals = [a for a in approvals if a.workflow_type == workflow_type]

        return approvals

    def get_summary(self) -> Dict[str, Any]:
        """Get approval queue summary."""
        if self.auto_expire:
            self.expire_overdue()

        by_status = {}
        for status in ApprovalStatus:
            by_status[status.value] = len([
                a for a in self._approvals.values()
                if a.status == status
            ])

        by_priority = {}
        for priority in ApprovalPriority:
            by_priority[priority.value] = len([
                a for a in self._approvals.values()
                if a.status == ApprovalStatus.PENDING and a.priority == priority
            ])

        overdue = [
            a for a in self._approvals.values()
            if a.is_overdue
        ]

        return {
            "total": len(self._approvals),
            "pending": by_status.get("pending", 0),
            "approved": by_status.get("approved", 0),
            "rejected": by_status.get("rejected", 0),
            "expired": by_status.get("expired", 0),
            "by_status": by_status,
            "by_priority": by_priority,
            "overdue_count": len(overdue),
        }

    def clear(self) -> None:
        """Clear all approvals (for testing)."""
        self._approvals.clear()


# Global approval queue instance
_approval_queue: Optional[ApprovalQueue] = None


def get_approval_queue(
    default_deadline_hours: float = 24.0,
) -> ApprovalQueue:
    """Get the global approval queue instance."""
    global _approval_queue
    if _approval_queue is None:
        _approval_queue = ApprovalQueue(default_deadline_hours=default_deadline_hours)
    return _approval_queue


def reset_approval_queue() -> None:
    """Reset the global approval queue (for testing)."""
    global _approval_queue
    _approval_queue = None
