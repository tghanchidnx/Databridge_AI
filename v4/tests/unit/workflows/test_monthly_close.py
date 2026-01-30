"""
Tests for Monthly Close Workflow.

Tests the month-end close process automation.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.workflows.monthly_close import (
    MonthlyCloseWorkflow,
    CloseStatus,
    CloseStepType,
    CloseStep,
    ClosePeriod,
    CloseValidation,
    CloseResult,
)


class TestCloseStatus:
    """Tests for CloseStatus enum."""

    def test_status_values(self):
        """Test all status values exist."""
        assert CloseStatus.NOT_STARTED.value == "not_started"
        assert CloseStatus.IN_PROGRESS.value == "in_progress"
        assert CloseStatus.PENDING_REVIEW.value == "pending_review"
        assert CloseStatus.COMPLETED.value == "completed"
        assert CloseStatus.LOCKED.value == "locked"
        assert CloseStatus.ERROR.value == "error"


class TestCloseStepType:
    """Tests for CloseStepType enum."""

    def test_step_types(self):
        """Test all step types exist."""
        assert CloseStepType.DATA_SYNC.value == "data_sync"
        assert CloseStepType.VALIDATION.value == "validation"
        assert CloseStepType.RECONCILIATION.value == "reconciliation"
        assert CloseStepType.ADJUSTMENT.value == "adjustment"
        assert CloseStepType.REVIEW.value == "review"
        assert CloseStepType.APPROVAL.value == "approval"
        assert CloseStepType.LOCK.value == "lock"


class TestCloseStep:
    """Tests for CloseStep dataclass."""

    def test_step_creation(self):
        """Test creating a close step."""
        step = CloseStep(
            step_id="step-1",
            name="Sync Data",
            step_type=CloseStepType.DATA_SYNC,
            description="Sync data from source systems",
        )

        assert step.step_id == "step-1"
        assert step.step_type == CloseStepType.DATA_SYNC
        assert step.status == CloseStatus.NOT_STARTED

    def test_step_defaults(self):
        """Test step default values."""
        step = CloseStep(
            step_id="s1",
            name="Test",
            step_type=CloseStepType.VALIDATION,
        )

        assert step.status == CloseStatus.NOT_STARTED
        assert step.is_required is True
        assert step.is_automated is False
        assert step.order == 0
        assert step.errors == []

    def test_step_to_dict(self):
        """Test step to dictionary conversion."""
        step = CloseStep(
            step_id="s1",
            name="Trial Balance Review",
            step_type=CloseStepType.REVIEW,
            status=CloseStatus.COMPLETED,
            order=5,
            completed_at=datetime(2024, 1, 15, 12, 0, 0),
        )

        result = step.to_dict()

        assert result["step_id"] == "s1"
        assert result["step_type"] == "review"
        assert result["status"] == "completed"
        assert result["order"] == 5
        assert "completed_at" in result


class TestClosePeriod:
    """Tests for ClosePeriod dataclass."""

    def test_period_creation(self):
        """Test creating a close period."""
        period = ClosePeriod(
            period_key="2024-01",
            year=2024,
            month=1,
        )

        assert period.period_key == "2024-01"
        assert period.year == 2024
        assert period.month == 1
        assert period.status == CloseStatus.NOT_STARTED

    def test_period_with_fiscal_year(self):
        """Test period with fiscal year."""
        period = ClosePeriod(
            period_key="2024-01",
            year=2024,
            month=1,
            fiscal_year=2024,
            fiscal_period=7,
        )

        assert period.fiscal_year == 2024
        assert period.fiscal_period == 7

    def test_period_to_dict(self):
        """Test period to dictionary conversion."""
        period = ClosePeriod(
            period_key="2024-01",
            year=2024,
            month=1,
            locked_by="admin",
        )

        result = period.to_dict()

        assert result["period_key"] == "2024-01"
        assert result["year"] == 2024
        assert result["month"] == 1
        assert result["locked_by"] == "admin"


class TestCloseValidation:
    """Tests for CloseValidation dataclass."""

    def test_validation_passed(self):
        """Test a passed validation."""
        validation = CloseValidation(
            is_ready=True,
            checks=[
                {"name": "Data Check", "passed": True},
                {"name": "Balance Check", "passed": True},
            ],
            summary="Ready for close",
        )

        assert validation.is_ready is True
        assert len(validation.checks) == 2

    def test_validation_failed_with_blockers(self):
        """Test a failed validation with blockers."""
        validation = CloseValidation(
            is_ready=False,
            checks=[
                {"name": "Data Check", "passed": False},
            ],
            blockers=["Missing data for 3 accounts"],
            summary="1 blocker(s) found",
        )

        assert validation.is_ready is False
        assert len(validation.blockers) == 1

    def test_validation_to_dict(self):
        """Test validation to dictionary conversion."""
        validation = CloseValidation(
            is_ready=True,
            checks=[{"name": "Test", "passed": True}],
        )

        result = validation.to_dict()

        assert result["is_ready"] is True
        assert result["passed_count"] == 1
        assert result["total_checks"] == 1


class TestCloseResult:
    """Tests for CloseResult dataclass."""

    def test_result_success(self):
        """Test successful result."""
        result = CloseResult(
            success=True,
            message="Close completed",
        )

        assert result.success is True
        assert result.errors == []

    def test_result_with_period(self):
        """Test result with period."""
        period = ClosePeriod(
            period_key="2024-01",
            year=2024,
            month=1,
        )
        result = CloseResult(
            success=True,
            period=period,
        )

        assert result.period.period_key == "2024-01"

    def test_result_to_dict(self):
        """Test result to dictionary conversion."""
        result = CloseResult(
            success=True,
            message="Done",
            data={"key": "value"},
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["data"]["key"] == "value"


class TestMonthlyCloseWorkflow:
    """Tests for MonthlyCloseWorkflow."""

    def test_workflow_initialization(self):
        """Test workflow initialization."""
        workflow = MonthlyCloseWorkflow()

        assert len(workflow.steps) == 8
        assert workflow.auto_advance is False

    def test_workflow_custom_steps(self):
        """Test workflow with custom steps."""
        custom_steps = [
            CloseStep(step_id="s1", name="Custom Sync", step_type=CloseStepType.DATA_SYNC),
        ]

        workflow = MonthlyCloseWorkflow(steps=custom_steps)

        assert len(workflow.steps) == 1

    def test_initialize_period(self):
        """Test initializing a close period."""
        workflow = MonthlyCloseWorkflow()

        result = workflow.initialize_period(
            year=2024,
            month=1,
        )

        assert result.success is True
        assert result.period is not None
        assert result.period.period_key == "2024-01"
        assert result.period.year == 2024
        assert result.period.month == 1

    def test_initialize_period_with_fiscal(self):
        """Test initializing period with fiscal info."""
        workflow = MonthlyCloseWorkflow()

        result = workflow.initialize_period(
            year=2024,
            month=7,
            fiscal_year=2025,
            fiscal_period=1,
        )

        assert result.success is True
        assert result.period.fiscal_year == 2025
        assert result.period.fiscal_period == 1

    def test_validate_close_readiness(self):
        """Test validating close readiness."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)

        result = workflow.validate_close_readiness()

        assert result.success is True
        assert result.validation is not None
        assert result.validation.is_ready is True

    def test_validate_close_readiness_no_period(self):
        """Test validating without initialized period."""
        workflow = MonthlyCloseWorkflow()

        result = workflow.validate_close_readiness()

        assert result.success is True
        assert result.validation.is_ready is False
        assert len(result.validation.blockers) > 0

    def test_execute_step(self):
        """Test executing a close step."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)

        result = workflow.execute_step("data_sync")

        assert result.success is True
        assert len(result.steps) == 1
        assert result.steps[0].status == CloseStatus.COMPLETED  # Auto-complete

    def test_execute_step_not_found(self):
        """Test executing non-existent step."""
        workflow = MonthlyCloseWorkflow()

        result = workflow.execute_step("invalid_step")

        assert result.success is False
        assert len(result.errors) > 0

    def test_execute_step_order_enforced(self):
        """Test step execution order is enforced."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)

        # Try to execute step 3 without completing steps 1-2
        result = workflow.execute_step("subledger_recon")

        assert result.success is False
        assert "not completed" in result.message

    def test_complete_step(self):
        """Test completing a close step."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)
        workflow.execute_step("data_sync")
        workflow.execute_step("validation")
        workflow.execute_step("subledger_recon")

        result = workflow.complete_step(
            step_id="subledger_recon",
            notes="Reconciliation completed successfully",
        )

        assert result.success is True
        assert result.steps[0].status == CloseStatus.COMPLETED

    def test_complete_step_not_found(self):
        """Test completing non-existent step."""
        workflow = MonthlyCloseWorkflow()

        result = workflow.complete_step("invalid_step")

        assert result.success is False

    def test_lock_period(self):
        """Test locking a period."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)

        # Complete all steps first
        for step in workflow.steps:
            workflow.execute_step(step.step_id)
            if step.status != CloseStatus.COMPLETED:
                workflow.complete_step(step.step_id)

        result = workflow.lock_period(locked_by="admin")

        assert result.success is True
        assert result.period.status == CloseStatus.LOCKED
        assert result.period.locked_by == "admin"

    def test_lock_period_incomplete_steps(self):
        """Test locking period with incomplete steps fails."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)

        result = workflow.lock_period(locked_by="admin")

        assert result.success is False
        assert "not completed" in result.message

    def test_lock_period_no_period(self):
        """Test locking without initialized period."""
        workflow = MonthlyCloseWorkflow()

        result = workflow.lock_period(locked_by="admin")

        assert result.success is False
        assert "No period initialized" in result.errors[0]

    def test_get_status(self):
        """Test getting period status."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)

        result = workflow.get_status()

        assert result.success is True
        assert result.data is not None
        assert "completed_steps" in result.data
        assert "pending_steps" in result.data
        assert "percent_complete" in result.data

    def test_get_status_after_steps(self):
        """Test getting status after executing steps."""
        workflow = MonthlyCloseWorkflow()
        workflow.initialize_period(2024, 1)
        workflow.execute_step("data_sync")  # Auto-completes
        workflow.execute_step("validation")  # Auto-completes

        result = workflow.get_status()

        assert result.data["completed_steps"] == 2
        assert result.data["percent_complete"] == 25.0  # 2/8

    def test_default_steps(self):
        """Test that default steps are correct."""
        workflow = MonthlyCloseWorkflow()

        step_ids = [s.step_id for s in workflow.steps]

        assert "data_sync" in step_ids
        assert "validation" in step_ids
        assert "subledger_recon" in step_ids
        assert "intercompany_recon" in step_ids
        assert "accruals" in step_ids
        assert "tb_review" in step_ids
        assert "mgmt_review" in step_ids
        assert "period_lock" in step_ids

    def test_step_order(self):
        """Test steps are in correct order."""
        workflow = MonthlyCloseWorkflow()

        orders = [s.order for s in workflow.steps]

        assert orders == sorted(orders)
        assert orders == [1, 2, 3, 4, 5, 6, 7, 8]

    def test_automated_steps(self):
        """Test identifying automated steps."""
        workflow = MonthlyCloseWorkflow()

        automated = [s for s in workflow.steps if s.is_automated]
        manual = [s for s in workflow.steps if not s.is_automated]

        assert len(automated) == 3  # data_sync, validation, period_lock
        assert len(manual) == 5
