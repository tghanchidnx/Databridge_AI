"""
Unit tests for workflow execution module.
"""

import pytest
import time
from datetime import datetime, timezone
from typing import Dict, Any

from src.workflows.execution import (
    ExecutionStatus,
    StepStatus,
    StepDefinition,
    StepResult,
    Checkpoint,
    ExecutionResult,
    WorkflowExecutor,
    get_workflow_executor,
    reset_workflow_executor,
)
from src.workflows.events import reset_workflow_event_bus


class TestExecutionStatus:
    """Tests for ExecutionStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected statuses exist."""
        assert ExecutionStatus.PENDING == "pending"
        assert ExecutionStatus.RUNNING == "running"
        assert ExecutionStatus.COMPLETED == "completed"
        assert ExecutionStatus.FAILED == "failed"
        assert ExecutionStatus.ROLLED_BACK == "rolled_back"
        assert ExecutionStatus.PAUSED == "paused"


class TestStepStatus:
    """Tests for StepStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected statuses exist."""
        assert StepStatus.PENDING == "pending"
        assert StepStatus.RUNNING == "running"
        assert StepStatus.COMPLETED == "completed"
        assert StepStatus.FAILED == "failed"
        assert StepStatus.SKIPPED == "skipped"
        assert StepStatus.ROLLED_BACK == "rolled_back"


class TestStepDefinition:
    """Tests for StepDefinition dataclass."""

    def test_basic_creation(self):
        """Test creating a basic step definition."""
        def executor(**kwargs):
            return {"success": True}

        step = StepDefinition(
            step_id="test_step",
            name="Test Step",
            executor=executor,
        )

        assert step.step_id == "test_step"
        assert step.name == "Test Step"
        assert step.can_run_parallel is True
        assert step.is_required is True

    def test_to_dict(self):
        """Test serialization to dictionary."""
        def executor(**kwargs):
            return {}

        def rollback(**kwargs):
            pass

        step = StepDefinition(
            step_id="test_step",
            name="Test Step",
            executor=executor,
            dependencies=["dep1", "dep2"],
            can_run_parallel=False,
            timeout_seconds=30.0,
            retry_count=3,
            rollback_fn=rollback,
        )

        data = step.to_dict()

        assert data["step_id"] == "test_step"
        assert data["dependencies"] == ["dep1", "dep2"]
        assert data["can_run_parallel"] is False
        assert data["timeout_seconds"] == 30.0
        assert data["retry_count"] == 3
        assert data["has_rollback"] is True


class TestStepResult:
    """Tests for StepResult dataclass."""

    def test_basic_creation(self):
        """Test creating a basic step result."""
        result = StepResult(
            step_id="test_step",
            status=StepStatus.COMPLETED,
            result={"records": 100},
            duration_seconds=1.5,
        )

        assert result.step_id == "test_step"
        assert result.status == StepStatus.COMPLETED
        assert result.result["records"] == 100

    def test_to_dict(self):
        """Test serialization to dictionary."""
        now = datetime.now(timezone.utc)
        result = StepResult(
            step_id="test_step",
            status=StepStatus.FAILED,
            started_at=now,
            completed_at=now,
            error="Something failed",
            retry_attempts=2,
            duration_seconds=3.456,
        )

        data = result.to_dict()

        assert data["step_id"] == "test_step"
        assert data["status"] == "failed"
        assert data["error"] == "Something failed"
        assert data["retry_attempts"] == 2
        assert data["duration_seconds"] == 3.456


class TestCheckpoint:
    """Tests for Checkpoint dataclass."""

    def test_basic_creation(self):
        """Test creating a basic checkpoint."""
        checkpoint = Checkpoint(
            step_results={
                "step1": StepResult(step_id="step1", status=StepStatus.COMPLETED)
            },
            workflow_state={"counter": 10},
        )

        assert checkpoint.checkpoint_id is not None
        assert "step1" in checkpoint.step_results
        assert checkpoint.workflow_state["counter"] == 10

    def test_to_dict(self):
        """Test serialization to dictionary."""
        checkpoint = Checkpoint(
            step_results={
                "step1": StepResult(step_id="step1", status=StepStatus.COMPLETED)
            },
            metadata={"version": "1.0"},
        )

        data = checkpoint.to_dict()

        assert "checkpoint_id" in data
        assert "step1" in data["step_results"]
        assert data["metadata"]["version"] == "1.0"


class TestExecutionResult:
    """Tests for ExecutionResult dataclass."""

    def test_basic_creation(self):
        """Test creating a basic execution result."""
        result = ExecutionResult(
            success=True,
            status=ExecutionStatus.COMPLETED,
            message="Workflow completed",
        )

        assert result.success is True
        assert result.status == ExecutionStatus.COMPLETED

    def test_to_dict_includes_counts(self):
        """Test that to_dict includes completion counts."""
        result = ExecutionResult(
            success=True,
            status=ExecutionStatus.COMPLETED,
            step_results={
                "step1": StepResult(step_id="step1", status=StepStatus.COMPLETED),
                "step2": StepResult(step_id="step2", status=StepStatus.COMPLETED),
                "step3": StepResult(step_id="step3", status=StepStatus.SKIPPED),
            },
        )

        data = result.to_dict()

        assert data["completed_count"] == 2
        assert data["total_steps"] == 3


class TestWorkflowExecutor:
    """Tests for WorkflowExecutor class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_workflow_executor()
        reset_workflow_event_bus()
        self.executor = WorkflowExecutor(max_workers=2)

    def teardown_method(self):
        """Clean up after each test."""
        if hasattr(self, 'executor'):
            self.executor.shutdown()

    def _create_step(
        self,
        step_id: str,
        name: str = None,
        dependencies: list = None,
        can_parallel: bool = True,
        fail: bool = False,
        sleep_time: float = 0.0,
        rollback_fn=None,
    ) -> StepDefinition:
        """Helper to create step definitions."""
        def executor(state: Dict[str, Any] = None, **kwargs):
            if sleep_time:
                time.sleep(sleep_time)
            if fail:
                raise ValueError(f"Step {step_id} failed")
            return {"step_id": step_id, "success": True}

        return StepDefinition(
            step_id=step_id,
            name=name or step_id,
            executor=executor,
            dependencies=dependencies or [],
            can_run_parallel=can_parallel,
            rollback_fn=rollback_fn,
        )

    def test_execute_single_step(self):
        """Test executing a single step."""
        steps = [self._create_step("step1")]

        result = self.executor.execute(steps, workflow_type="test")

        assert result.success is True
        assert result.status == ExecutionStatus.COMPLETED
        assert "step1" in result.step_results
        assert result.step_results["step1"].status == StepStatus.COMPLETED

    def test_execute_sequential_steps(self):
        """Test executing steps sequentially with dependencies."""
        steps = [
            self._create_step("step1"),
            self._create_step("step2", dependencies=["step1"]),
            self._create_step("step3", dependencies=["step2"]),
        ]

        result = self.executor.execute(steps, workflow_type="test")

        assert result.success is True
        assert all(
            sr.status == StepStatus.COMPLETED
            for sr in result.step_results.values()
        )

    def test_execute_parallel_steps(self):
        """Test executing independent steps in parallel."""
        steps = [
            self._create_step("step1", sleep_time=0.1),
            self._create_step("step2", sleep_time=0.1),
            self._create_step("step3", sleep_time=0.1),
        ]

        start = time.time()
        result = self.executor.execute(steps, workflow_type="test")
        elapsed = time.time() - start

        assert result.success is True
        # All 3 should complete, but in parallel should be ~0.1-0.2 sec, not 0.3
        assert elapsed < 0.3

    def test_execute_mixed_parallel_sequential(self):
        """Test mix of parallel and sequential steps."""
        steps = [
            self._create_step("step1", can_parallel=True),
            self._create_step("step2", can_parallel=True),
            self._create_step("step3", dependencies=["step1", "step2"], can_parallel=False),
        ]

        result = self.executor.execute(steps, workflow_type="test")

        assert result.success is True
        assert len(result.step_results) == 3

    def test_execute_with_failing_step(self):
        """Test execution with a failing step."""
        steps = [
            self._create_step("step1"),
            self._create_step("step2", fail=True),
            self._create_step("step3", dependencies=["step2"]),
        ]

        result = self.executor.execute(steps, workflow_type="test")

        assert result.success is False
        assert result.status == ExecutionStatus.FAILED
        assert result.step_results["step2"].status == StepStatus.FAILED
        assert result.step_results["step3"].status == StepStatus.SKIPPED

    def test_execute_continue_on_failure(self):
        """Test execution continues past failure when stop_on_failure=False."""
        executor = WorkflowExecutor(stop_on_failure=False)

        steps = [
            self._create_step("step1"),
            self._create_step("step2", fail=True),
            self._create_step("step3"),  # Should still run
        ]

        result = executor.execute(steps, workflow_type="test")

        assert result.success is False
        assert result.step_results["step1"].status == StepStatus.COMPLETED
        assert result.step_results["step2"].status == StepStatus.FAILED
        assert result.step_results["step3"].status == StepStatus.COMPLETED

        executor.shutdown()

    def test_execute_with_retry(self):
        """Test step retry on failure."""
        attempt_count = [0]

        def executor_fn(**kwargs):
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Not yet")
            return {"success": True}

        step = StepDefinition(
            step_id="retry_step",
            name="Retry Step",
            executor=executor_fn,
            retry_count=2,  # 2 retries = 3 total attempts
        )

        result = self.executor.execute([step], workflow_type="test")

        assert result.success is True
        assert result.step_results["retry_step"].retry_attempts == 2

    def test_execute_with_workflow_state(self):
        """Test passing workflow state to steps."""
        state_received = []

        def executor_fn(state: Dict[str, Any] = None, **kwargs):
            state_received.append(state.copy() if state else None)
            return {"success": True}

        step = StepDefinition(
            step_id="state_step",
            name="State Step",
            executor=executor_fn,
        )

        initial_state = {"counter": 42, "name": "test"}
        result = self.executor.execute(
            [step],
            workflow_type="test",
            workflow_state=initial_state,
        )

        assert result.success is True
        assert state_received[0]["counter"] == 42

    def test_checkpoint_creation(self):
        """Test automatic checkpoint creation."""
        executor = WorkflowExecutor(auto_checkpoint=True)

        steps = [
            self._create_step("step1"),
            self._create_step("step2"),
        ]

        result = executor.execute(steps, workflow_type="test")

        assert result.success is True
        assert result.checkpoint is not None
        assert "step1" in result.checkpoint.step_results
        assert "step2" in result.checkpoint.step_results

        executor.shutdown()

    def test_resume_from_checkpoint(self):
        """Test resuming from a checkpoint."""
        # First execution - partial
        executor = WorkflowExecutor(auto_checkpoint=True)

        steps = [
            self._create_step("step1"),
            self._create_step("step2", fail=True),
            self._create_step("step3", dependencies=["step2"]),
        ]

        result1 = executor.execute(steps, workflow_type="test")
        checkpoint_id = result1.checkpoint.checkpoint_id if result1.checkpoint else None

        # Create new steps with step2 fixed
        fixed_steps = [
            self._create_step("step1"),
            self._create_step("step2", fail=False),  # Fixed
            self._create_step("step3", dependencies=["step2"]),
        ]

        # Resume from checkpoint
        result2 = executor.execute(
            fixed_steps,
            workflow_type="test",
            resume_from_checkpoint=checkpoint_id,
        )

        # step1 should be from checkpoint (already completed)
        assert result2.step_results["step1"].status == StepStatus.COMPLETED

        executor.shutdown()

    def test_rollback_steps(self):
        """Test rolling back completed steps."""
        rollback_called = []

        def create_rollback_fn(step_id):
            def rollback(**kwargs):
                rollback_called.append(step_id)
            return rollback

        steps = [
            self._create_step(
                "step1",
                rollback_fn=create_rollback_fn("step1")
            ),
            self._create_step(
                "step2",
                dependencies=["step1"],
                rollback_fn=create_rollback_fn("step2")
            ),
        ]

        # Execute
        exec_result = self.executor.execute(steps, workflow_type="test")

        # Rollback
        rollback_result = self.executor.rollback(
            steps=steps,
            step_results=exec_result.step_results,
            reason="Test rollback",
        )

        assert rollback_result.status == ExecutionStatus.ROLLED_BACK
        assert len(rollback_called) == 2
        # Rollback should be in reverse order
        assert rollback_called[0] == "step2"
        assert rollback_called[1] == "step1"

    def test_rollback_to_checkpoint(self):
        """Test rolling back to a specific checkpoint."""
        rollback_called = []

        def create_rollback_fn(step_id):
            def rollback(**kwargs):
                rollback_called.append(step_id)
            return rollback

        executor = WorkflowExecutor(auto_checkpoint=True)

        # Create and save a checkpoint after step1
        initial_checkpoint = Checkpoint(
            step_results={
                "step1": StepResult(step_id="step1", status=StepStatus.COMPLETED)
            },
        )
        executor._checkpoints[initial_checkpoint.checkpoint_id] = initial_checkpoint

        # Simulate results after more steps
        step_results = {
            "step1": StepResult(step_id="step1", status=StepStatus.COMPLETED),
            "step2": StepResult(step_id="step2", status=StepStatus.COMPLETED),
            "step3": StepResult(step_id="step3", status=StepStatus.COMPLETED),
        }

        steps = [
            self._create_step("step1", rollback_fn=create_rollback_fn("step1")),
            self._create_step("step2", rollback_fn=create_rollback_fn("step2")),
            self._create_step("step3", rollback_fn=create_rollback_fn("step3")),
        ]

        # Rollback to checkpoint (should only rollback step2, step3)
        rollback_result = executor.rollback(
            steps=steps,
            step_results=step_results,
            reason="Rollback to checkpoint",
            rollback_to_checkpoint=initial_checkpoint.checkpoint_id,
        )

        assert "step1" not in rollback_called
        assert "step2" in rollback_called
        assert "step3" in rollback_called

        executor.shutdown()

    def test_progress_callback(self):
        """Test progress callback is called."""
        progress_updates = []

        def on_progress(step_id: str, result: StepResult):
            progress_updates.append((step_id, result.status))

        self.executor.set_progress_callback(on_progress)

        steps = [
            self._create_step("step1"),
            self._create_step("step2"),
        ]

        self.executor.execute(steps, workflow_type="test")

        assert len(progress_updates) == 2
        assert ("step1", StepStatus.COMPLETED) in progress_updates
        assert ("step2", StepStatus.COMPLETED) in progress_updates

    def test_list_checkpoints(self):
        """Test listing checkpoints."""
        executor = WorkflowExecutor(auto_checkpoint=True)

        steps1 = [self._create_step("step1")]
        steps2 = [self._create_step("step2")]

        executor.execute(steps1, workflow_type="test")
        executor.execute(steps2, workflow_type="test")

        checkpoints = executor.list_checkpoints()
        assert len(checkpoints) == 2

        executor.shutdown()

    def test_clear_checkpoints(self):
        """Test clearing checkpoints."""
        executor = WorkflowExecutor(auto_checkpoint=True)

        steps = [self._create_step("step1")]
        executor.execute(steps, workflow_type="test")

        assert len(executor.list_checkpoints()) == 1

        executor.clear_checkpoints()
        assert len(executor.list_checkpoints()) == 0

        executor.shutdown()

    def test_duration_tracking(self):
        """Test execution duration is tracked."""
        steps = [self._create_step("step1", sleep_time=0.1)]

        result = self.executor.execute(steps, workflow_type="test")

        assert result.duration_seconds >= 0.1
        assert result.step_results["step1"].duration_seconds >= 0.1


class TestSingletonBehavior:
    """Tests for singleton behavior."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        reset_workflow_executor()
        reset_workflow_event_bus()

    def test_get_returns_singleton(self):
        """Test that get_workflow_executor returns singleton."""
        exec1 = get_workflow_executor()
        exec2 = get_workflow_executor()

        assert exec1 is exec2

        exec1.shutdown()

    def test_reset_creates_new_instance(self):
        """Test that reset creates a new instance."""
        exec1 = get_workflow_executor()
        reset_workflow_executor()
        exec2 = get_workflow_executor()

        assert exec1 is not exec2

        exec2.shutdown()
