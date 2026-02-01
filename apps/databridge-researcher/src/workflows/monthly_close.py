"""
Monthly Close Workflow for DataBridge Analytics V4.

Automates the FP&A month-end close process including data validation,
reconciliation, and period locking.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import datetime, date
import logging


logger = logging.getLogger(__name__)


class CloseStatus(str, Enum):
    """Status of the close process."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    PENDING_REVIEW = "pending_review"
    COMPLETED = "completed"
    LOCKED = "locked"
    ERROR = "error"


class CloseStepType(str, Enum):
    """Type of close step."""
    DATA_SYNC = "data_sync"
    VALIDATION = "validation"
    RECONCILIATION = "reconciliation"
    ADJUSTMENT = "adjustment"
    REVIEW = "review"
    APPROVAL = "approval"
    LOCK = "lock"


@dataclass
class CloseStep:
    """A single step in the close process."""

    step_id: str
    name: str
    step_type: CloseStepType
    description: str = ""
    status: CloseStatus = CloseStatus.NOT_STARTED
    order: int = 0
    is_required: bool = True
    is_automated: bool = False
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_id": self.step_id,
            "name": self.name,
            "step_type": self.step_type.value,
            "description": self.description,
            "status": self.status.value,
            "order": self.order,
            "is_required": self.is_required,
            "is_automated": self.is_automated,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "errors": self.errors,
        }


@dataclass
class ClosePeriod:
    """A close period (e.g., January 2024)."""

    period_key: str  # e.g., "2024-01"
    year: int
    month: int
    fiscal_year: Optional[int] = None
    fiscal_period: Optional[int] = None
    status: CloseStatus = CloseStatus.NOT_STARTED
    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "period_key": self.period_key,
            "year": self.year,
            "month": self.month,
            "fiscal_year": self.fiscal_year,
            "fiscal_period": self.fiscal_period,
            "status": self.status.value,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "locked_at": self.locked_at.isoformat() if self.locked_at else None,
            "locked_by": self.locked_by,
        }


@dataclass
class CloseValidation:
    """Validation result for close readiness."""

    is_ready: bool
    checks: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    blockers: List[str] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_ready": self.is_ready,
            "checks": self.checks,
            "warnings": self.warnings,
            "blockers": self.blockers,
            "summary": self.summary,
            "passed_count": len([c for c in self.checks if c.get("passed", False)]),
            "total_checks": len(self.checks),
        }


@dataclass
class CloseResult:
    """Result from close workflow operations."""

    success: bool
    message: str = ""
    period: Optional[ClosePeriod] = None
    steps: List[CloseStep] = field(default_factory=list)
    validation: Optional[CloseValidation] = None
    data: Any = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "period": self.period.to_dict() if self.period else None,
            "steps": [s.to_dict() for s in self.steps],
            "validation": self.validation.to_dict() if self.validation else None,
            "data": self.data,
            "errors": self.errors,
        }


class MonthlyCloseWorkflow:
    """
    Orchestrates the monthly close process.

    Standard close steps:
    1. Data Sync - Pull latest data from source systems
    2. Validation - Validate data completeness and accuracy
    3. Subledger Reconciliation - Reconcile AR, AP, FA to GL
    4. Intercompany Reconciliation - Match intercompany transactions
    5. Accruals & Adjustments - Review and post adjustments
    6. Trial Balance Review - Review TB for anomalies
    7. Management Review - Final review and approval
    8. Period Lock - Lock the period
    """

    DEFAULT_STEPS = [
        CloseStep(
            step_id="data_sync",
            name="Data Sync",
            step_type=CloseStepType.DATA_SYNC,
            description="Pull latest data from source systems",
            order=1,
            is_automated=True,
        ),
        CloseStep(
            step_id="validation",
            name="Data Validation",
            step_type=CloseStepType.VALIDATION,
            description="Validate data completeness and accuracy",
            order=2,
            is_automated=True,
        ),
        CloseStep(
            step_id="subledger_recon",
            name="Subledger Reconciliation",
            step_type=CloseStepType.RECONCILIATION,
            description="Reconcile subledgers (AR, AP, FA) to GL",
            order=3,
            is_automated=False,
        ),
        CloseStep(
            step_id="intercompany_recon",
            name="Intercompany Reconciliation",
            step_type=CloseStepType.RECONCILIATION,
            description="Match intercompany transactions",
            order=4,
            is_automated=False,
        ),
        CloseStep(
            step_id="accruals",
            name="Accruals & Adjustments",
            step_type=CloseStepType.ADJUSTMENT,
            description="Review and post period-end adjustments",
            order=5,
            is_automated=False,
        ),
        CloseStep(
            step_id="tb_review",
            name="Trial Balance Review",
            step_type=CloseStepType.REVIEW,
            description="Review trial balance for anomalies",
            order=6,
            is_automated=False,
        ),
        CloseStep(
            step_id="mgmt_review",
            name="Management Review",
            step_type=CloseStepType.APPROVAL,
            description="Final management review and approval",
            order=7,
            is_automated=False,
        ),
        CloseStep(
            step_id="period_lock",
            name="Period Lock",
            step_type=CloseStepType.LOCK,
            description="Lock the period to prevent changes",
            order=8,
            is_automated=True,
        ),
    ]

    def __init__(
        self,
        steps: Optional[List[CloseStep]] = None,
        auto_advance: bool = False,
    ):
        """
        Initialize the monthly close workflow.

        Args:
            steps: Custom close steps (uses defaults if not provided).
            auto_advance: Whether to automatically advance to next step.
        """
        self.steps = steps or [CloseStep(**s.__dict__) for s in self.DEFAULT_STEPS]
        self.auto_advance = auto_advance
        self._current_period: Optional[ClosePeriod] = None

    def initialize_period(
        self,
        year: int,
        month: int,
        fiscal_year: Optional[int] = None,
        fiscal_period: Optional[int] = None,
    ) -> CloseResult:
        """
        Initialize a close period.

        Args:
            year: Calendar year.
            month: Calendar month (1-12).
            fiscal_year: Optional fiscal year.
            fiscal_period: Optional fiscal period.

        Returns:
            CloseResult with initialized period.
        """
        try:
            period_key = f"{year}-{month:02d}"

            self._current_period = ClosePeriod(
                period_key=period_key,
                year=year,
                month=month,
                fiscal_year=fiscal_year or year,
                fiscal_period=fiscal_period or month,
                status=CloseStatus.NOT_STARTED,
                opened_at=datetime.now(),
            )

            # Reset all steps
            for step in self.steps:
                step.status = CloseStatus.NOT_STARTED
                step.started_at = None
                step.completed_at = None
                step.result = None
                step.errors = []

            return CloseResult(
                success=True,
                message=f"Initialized close period {period_key}",
                period=self._current_period,
                steps=self.steps,
            )

        except Exception as e:
            logger.error(f"Failed to initialize period: {e}")
            return CloseResult(
                success=False,
                message=f"Failed to initialize period: {str(e)}",
                errors=[str(e)],
            )

    def validate_close_readiness(
        self,
        data_source: Optional[Dict[str, Any]] = None,
    ) -> CloseResult:
        """
        Validate readiness for close.

        Args:
            data_source: Optional data source for validation.

        Returns:
            CloseResult with validation details.
        """
        try:
            checks = []
            warnings = []
            blockers = []

            # Check 1: Period initialized
            check_period = {
                "name": "Period Initialized",
                "description": "Close period has been initialized",
                "passed": self._current_period is not None,
            }
            checks.append(check_period)
            if not check_period["passed"]:
                blockers.append("Period not initialized")

            # Check 2: Data freshness (simulated)
            check_data = {
                "name": "Data Freshness",
                "description": "Data is up to date (within 24 hours)",
                "passed": True,  # Would check actual data timestamps
            }
            checks.append(check_data)

            # Check 3: No open items (simulated)
            check_open_items = {
                "name": "Open Items",
                "description": "No critical open items pending",
                "passed": True,  # Would check for open reconciling items
            }
            checks.append(check_open_items)

            # Check 4: Prior period closed
            check_prior = {
                "name": "Prior Period Closed",
                "description": "Prior period has been closed",
                "passed": True,  # Would check prior period status
            }
            checks.append(check_prior)

            # Check 5: Intercompany balanced (simulated)
            check_ic = {
                "name": "Intercompany Balance",
                "description": "Intercompany eliminations are balanced",
                "passed": True,  # Would check IC balances
            }
            checks.append(check_ic)

            is_ready = len(blockers) == 0 and all(c.get("passed", False) for c in checks if c.get("required", True))

            validation = CloseValidation(
                is_ready=is_ready,
                checks=checks,
                warnings=warnings,
                blockers=blockers,
                summary=f"Ready for close" if is_ready else f"{len(blockers)} blocker(s) found",
            )

            return CloseResult(
                success=True,
                message=validation.summary,
                period=self._current_period,
                validation=validation,
            )

        except Exception as e:
            logger.error(f"Failed to validate close readiness: {e}")
            return CloseResult(
                success=False,
                message=f"Failed to validate: {str(e)}",
                errors=[str(e)],
            )

    def execute_step(
        self,
        step_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> CloseResult:
        """
        Execute a close step.

        Args:
            step_id: ID of the step to execute.
            params: Optional parameters for the step.

        Returns:
            CloseResult with step execution result.
        """
        try:
            step = next((s for s in self.steps if s.step_id == step_id), None)
            if not step:
                return CloseResult(
                    success=False,
                    errors=[f"Step '{step_id}' not found"],
                )

            # Check prerequisites (prior steps completed)
            for prior_step in self.steps:
                if prior_step.order < step.order and prior_step.is_required:
                    if prior_step.status not in [CloseStatus.COMPLETED, CloseStatus.LOCKED]:
                        return CloseResult(
                            success=False,
                            message=f"Prior step '{prior_step.name}' not completed",
                            errors=[f"Complete '{prior_step.name}' first"],
                        )

            # Execute the step
            step.status = CloseStatus.IN_PROGRESS
            step.started_at = datetime.now()

            # Simulated execution based on step type
            if step.step_type == CloseStepType.DATA_SYNC:
                step.result = self._execute_data_sync(params)
            elif step.step_type == CloseStepType.VALIDATION:
                step.result = self._execute_validation(params)
            elif step.step_type == CloseStepType.RECONCILIATION:
                step.result = self._execute_reconciliation(step_id, params)
            elif step.step_type == CloseStepType.LOCK:
                step.result = self._execute_lock(params)
            else:
                step.result = {"status": "manual", "message": "Requires manual completion"}

            # Mark completed if automated or successful
            if step.is_automated or step.result.get("auto_complete", False):
                step.status = CloseStatus.COMPLETED
                step.completed_at = datetime.now()
            else:
                step.status = CloseStatus.PENDING_REVIEW

            # Update period status
            if self._current_period:
                self._current_period.status = CloseStatus.IN_PROGRESS

            return CloseResult(
                success=True,
                message=f"Step '{step.name}' executed",
                period=self._current_period,
                steps=[step],
                data=step.result,
            )

        except Exception as e:
            if step:
                step.status = CloseStatus.ERROR
                step.errors.append(str(e))
            logger.error(f"Failed to execute step: {e}")
            return CloseResult(
                success=False,
                message=f"Failed to execute step: {str(e)}",
                errors=[str(e)],
            )

    def complete_step(
        self,
        step_id: str,
        notes: Optional[str] = None,
    ) -> CloseResult:
        """
        Mark a step as completed.

        Args:
            step_id: ID of the step to complete.
            notes: Optional completion notes.

        Returns:
            CloseResult confirming completion.
        """
        try:
            step = next((s for s in self.steps if s.step_id == step_id), None)
            if not step:
                return CloseResult(
                    success=False,
                    errors=[f"Step '{step_id}' not found"],
                )

            step.status = CloseStatus.COMPLETED
            step.completed_at = datetime.now()
            if notes:
                step.result = step.result or {}
                step.result["completion_notes"] = notes

            return CloseResult(
                success=True,
                message=f"Step '{step.name}' completed",
                period=self._current_period,
                steps=[step],
            )

        except Exception as e:
            logger.error(f"Failed to complete step: {e}")
            return CloseResult(
                success=False,
                message=f"Failed to complete step: {str(e)}",
                errors=[str(e)],
            )

    def lock_period(
        self,
        locked_by: str = "system",
    ) -> CloseResult:
        """
        Lock the current period.

        Args:
            locked_by: User/system locking the period.

        Returns:
            CloseResult confirming lock.
        """
        try:
            if not self._current_period:
                return CloseResult(
                    success=False,
                    errors=["No period initialized"],
                )

            # Check all required steps completed
            incomplete = [
                s for s in self.steps
                if s.is_required and s.status not in [CloseStatus.COMPLETED, CloseStatus.LOCKED]
            ]

            if incomplete:
                return CloseResult(
                    success=False,
                    message=f"{len(incomplete)} required step(s) not completed",
                    errors=[f"Complete '{s.name}' first" for s in incomplete],
                )

            self._current_period.status = CloseStatus.LOCKED
            self._current_period.closed_at = datetime.now()
            self._current_period.locked_at = datetime.now()
            self._current_period.locked_by = locked_by

            # Mark lock step as completed
            lock_step = next((s for s in self.steps if s.step_type == CloseStepType.LOCK), None)
            if lock_step:
                lock_step.status = CloseStatus.LOCKED
                lock_step.completed_at = datetime.now()

            return CloseResult(
                success=True,
                message=f"Period {self._current_period.period_key} locked",
                period=self._current_period,
                steps=self.steps,
            )

        except Exception as e:
            logger.error(f"Failed to lock period: {e}")
            return CloseResult(
                success=False,
                message=f"Failed to lock period: {str(e)}",
                errors=[str(e)],
            )

    def get_status(self) -> CloseResult:
        """
        Get current close status.

        Returns:
            CloseResult with current status.
        """
        completed = [s for s in self.steps if s.status == CloseStatus.COMPLETED]
        in_progress = [s for s in self.steps if s.status == CloseStatus.IN_PROGRESS]
        pending = [s for s in self.steps if s.status in [CloseStatus.NOT_STARTED, CloseStatus.PENDING_REVIEW]]

        summary = {
            "completed_steps": len(completed),
            "in_progress_steps": len(in_progress),
            "pending_steps": len(pending),
            "total_steps": len(self.steps),
            "percent_complete": round(len(completed) / len(self.steps) * 100, 1) if self.steps else 0,
        }

        return CloseResult(
            success=True,
            message=f"{summary['percent_complete']}% complete ({summary['completed_steps']}/{summary['total_steps']} steps)",
            period=self._current_period,
            steps=self.steps,
            data=summary,
        )

    # ==================== Private Execution Methods ====================

    def _execute_data_sync(self, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute data sync step."""
        # Simulated - would pull from data warehouse
        return {
            "records_synced": 10000,
            "tables_updated": 5,
            "last_sync": datetime.now().isoformat(),
            "auto_complete": True,
        }

    def _execute_validation(self, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute validation step."""
        # Simulated - would validate data quality
        return {
            "checks_passed": 15,
            "checks_failed": 0,
            "warnings": 2,
            "auto_complete": True,
        }

    def _execute_reconciliation(self, step_id: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute reconciliation step."""
        # Simulated - would reconcile accounts
        return {
            "items_reconciled": 500,
            "items_outstanding": 5,
            "variance": 0.00,
            "auto_complete": False,  # Requires review
        }

    def _execute_lock(self, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute period lock step."""
        return {
            "locked": True,
            "locked_at": datetime.now().isoformat(),
            "auto_complete": True,
        }
