"""
Workflow Execution Engine for DataBridge Analytics Researcher.

Provides:
- Concurrent step execution
- Checkpointing and rollback
- Recovery mechanisms
- Progress tracking
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import threading
import logging
import uuid
import json
import copy

from .events import (
    WorkflowEventType,
    StepEvent,
    RollbackEvent,
    get_workflow_event_bus,
)

logger = logging.getLogger(__name__)


class ExecutionStatus(str, Enum):
    """Status of workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PAUSED = "paused"


class StepStatus(str, Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"


@dataclass
class StepDefinition:
    """Definition of a workflow step."""

    step_id: str
    name: str
    executor: Callable[..., Dict[str, Any]]  # Function to execute
    dependencies: List[str] = field(default_factory=list)  # Step IDs this depends on
    can_run_parallel: bool = True
    is_required: bool = True
    timeout_seconds: Optional[float] = None
    retry_count: int = 0
    rollback_fn: Optional[Callable[..., None]] = None  # Function to rollback this step
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (without callables)."""
        return {
            "step_id": self.step_id,
            "name": self.name,
            "dependencies": self.dependencies,
            "can_run_parallel": self.can_run_parallel,
            "is_required": self.is_required,
            "timeout_seconds": self.timeout_seconds,
            "retry_count": self.retry_count,
            "has_rollback": self.rollback_fn is not None,
            "params": self.params,
        }


@dataclass
class StepResult:
    """Result of executing a step."""

    step_id: str
    status: StepStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_attempts: int = 0
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "step_id": self.step_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "error": self.error,
            "retry_attempts": self.retry_attempts,
            "duration_seconds": round(self.duration_seconds, 3),
        }


@dataclass
class Checkpoint:
    """A checkpoint for workflow recovery."""

    checkpoint_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    workflow_state: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "created_at": self.created_at.isoformat(),
            "step_results": {
                k: v.to_dict() for k, v in self.step_results.items()
            },
            "workflow_state": self.workflow_state,
            "metadata": self.metadata,
        }


@dataclass
class ExecutionResult:
    """Result of workflow execution."""

    success: bool
    message: str = ""
    status: ExecutionStatus = ExecutionStatus.PENDING
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    checkpoint: Optional[Checkpoint] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "status": self.status.value,
            "step_results": {
                k: v.to_dict() for k, v in self.step_results.items()
            },
            "checkpoint": self.checkpoint.to_dict() if self.checkpoint else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "errors": self.errors,
            "completed_count": len([
                r for r in self.step_results.values()
                if r.status == StepStatus.COMPLETED
            ]),
            "total_steps": len(self.step_results),
        }


class WorkflowExecutor:
    """
    Executes workflow steps with support for:
    - Parallel execution of independent steps
    - Dependency resolution
    - Checkpointing and recovery
    - Rollback on failure
    """

    def __init__(
        self,
        max_workers: int = 4,
        auto_checkpoint: bool = True,
        stop_on_failure: bool = True,
    ):
        """
        Initialize the workflow executor.

        Args:
            max_workers: Maximum parallel workers
            auto_checkpoint: Create checkpoints automatically
            stop_on_failure: Stop execution on first failure
        """
        self.max_workers = max_workers
        self.auto_checkpoint = auto_checkpoint
        self.stop_on_failure = stop_on_failure
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._event_bus = get_workflow_event_bus()
        self._checkpoints: Dict[str, Checkpoint] = {}
        self._lock = threading.Lock()
        self._progress_callback: Optional[Callable[[str, StepResult], None]] = None

    def set_progress_callback(
        self,
        callback: Callable[[str, StepResult], None],
    ) -> None:
        """Set callback for progress updates."""
        self._progress_callback = callback

    def execute(
        self,
        steps: List[StepDefinition],
        workflow_type: str = "",
        workflow_state: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[str] = None,
    ) -> ExecutionResult:
        """
        Execute workflow steps.

        Args:
            steps: List of step definitions
            workflow_type: Type of workflow
            workflow_state: Initial workflow state
            resume_from_checkpoint: Optional checkpoint ID to resume from

        Returns:
            ExecutionResult with all step results
        """
        result = ExecutionResult(
            success=True,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

        step_results: Dict[str, StepResult] = {}
        state = workflow_state or {}

        try:
            # Resume from checkpoint if specified
            if resume_from_checkpoint:
                checkpoint = self._checkpoints.get(resume_from_checkpoint)
                if checkpoint:
                    step_results = copy.deepcopy(checkpoint.step_results)
                    state = copy.deepcopy(checkpoint.workflow_state)
                    logger.info(f"Resuming from checkpoint: {resume_from_checkpoint}")

            # Build dependency graph
            step_map = {s.step_id: s for s in steps}
            completed = {
                sid for sid, sr in step_results.items()
                if sr.status == StepStatus.COMPLETED
            }

            # Execute steps respecting dependencies
            while True:
                # Find ready steps (dependencies met, not completed)
                ready = self._find_ready_steps(steps, completed, step_results)

                if not ready:
                    # Check if we're done or stuck
                    pending = [
                        s for s in steps
                        if s.step_id not in completed
                        and s.step_id not in step_results
                    ]
                    if pending:
                        # Stuck due to failed dependencies
                        for s in pending:
                            step_results[s.step_id] = StepResult(
                                step_id=s.step_id,
                                status=StepStatus.SKIPPED,
                                error="Dependencies not met",
                            )
                    break

                # Group into parallel and sequential
                parallel_steps = [s for s in ready if s.can_run_parallel]
                sequential_steps = [s for s in ready if not s.can_run_parallel]

                # Execute parallel steps
                if parallel_steps:
                    parallel_results = self._execute_parallel(
                        parallel_steps, workflow_type, state
                    )
                    for sr in parallel_results:
                        step_results[sr.step_id] = sr
                        if sr.status == StepStatus.COMPLETED:
                            completed.add(sr.step_id)
                        elif sr.status == StepStatus.FAILED and self.stop_on_failure:
                            result.success = False
                            result.errors.append(f"Step '{sr.step_id}' failed: {sr.error}")

                    if not result.success:
                        # Mark remaining steps as skipped
                        for step in steps:
                            if step.step_id not in step_results:
                                step_results[step.step_id] = StepResult(
                                    step_id=step.step_id,
                                    status=StepStatus.SKIPPED,
                                    error="Workflow stopped due to prior failure",
                                )
                        break

                # Execute sequential steps
                for step in sequential_steps:
                    sr = self._execute_step(step, workflow_type, state)
                    step_results[sr.step_id] = sr
                    if sr.status == StepStatus.COMPLETED:
                        completed.add(sr.step_id)
                    elif sr.status == StepStatus.FAILED and self.stop_on_failure:
                        result.success = False
                        result.errors.append(f"Step '{sr.step_id}' failed: {sr.error}")
                        break

                if not result.success:
                    # Mark remaining steps as skipped
                    for step in steps:
                        if step.step_id not in step_results:
                            step_results[step.step_id] = StepResult(
                                step_id=step.step_id,
                                status=StepStatus.SKIPPED,
                                error="Workflow stopped due to prior failure",
                            )
                    break

                # Create auto checkpoint
                if self.auto_checkpoint:
                    checkpoint = self._create_checkpoint(step_results, state)
                    result.checkpoint = checkpoint

            # Finalize result
            result.step_results = step_results
            result.completed_at = datetime.now(timezone.utc)
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()

            # Check if any step failed
            has_failures = any(
                sr.status == StepStatus.FAILED
                for sr in step_results.values()
            )

            if has_failures:
                result.success = False
                result.status = ExecutionStatus.FAILED
                if not result.errors:
                    # Collect errors from failed steps
                    for sr in step_results.values():
                        if sr.status == StepStatus.FAILED and sr.error:
                            result.errors.append(f"Step '{sr.step_id}' failed: {sr.error}")
                result.message = f"Workflow failed: {len(result.errors)} error(s)"
            else:
                result.status = ExecutionStatus.COMPLETED
                result.message = "Workflow completed successfully"

            return result

        except Exception as e:
            logger.error(f"Workflow execution error: {e}")
            result.success = False
            result.status = ExecutionStatus.FAILED
            result.message = str(e)
            result.errors.append(str(e))
            result.completed_at = datetime.now(timezone.utc)
            return result

    def _find_ready_steps(
        self,
        steps: List[StepDefinition],
        completed: set,
        results: Dict[str, StepResult],
    ) -> List[StepDefinition]:
        """Find steps that are ready to execute."""
        ready = []

        for step in steps:
            if step.step_id in completed:
                continue

            if step.step_id in results:
                status = results[step.step_id].status
                if status in [StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED]:
                    continue

            # Check dependencies
            deps_met = all(
                dep in completed
                for dep in step.dependencies
            )

            # Check if any dependency failed
            deps_failed = any(
                dep in results and results[dep].status == StepStatus.FAILED
                for dep in step.dependencies
            )

            if deps_met and not deps_failed:
                ready.append(step)

        return ready

    def _execute_parallel(
        self,
        steps: List[StepDefinition],
        workflow_type: str,
        state: Dict[str, Any],
    ) -> List[StepResult]:
        """Execute steps in parallel."""
        results = []
        futures: Dict[Future, StepDefinition] = {}

        for step in steps:
            future = self._executor.submit(
                self._execute_step, step, workflow_type, state
            )
            futures[future] = step

        for future in as_completed(futures):
            step = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Parallel step error for {step.step_id}: {e}")
                results.append(StepResult(
                    step_id=step.step_id,
                    status=StepStatus.FAILED,
                    error=str(e),
                ))

        return results

    def _execute_step(
        self,
        step: StepDefinition,
        workflow_type: str,
        state: Dict[str, Any],
    ) -> StepResult:
        """Execute a single step."""
        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

        # Emit started event
        self._event_bus.publish(StepEvent(
            event_type=WorkflowEventType.STEP_STARTED,
            workflow_type=workflow_type,
            step_id=step.step_id,
            step_name=step.name,
        ))

        attempts = 0
        last_error = None

        while attempts <= step.retry_count:
            try:
                # Execute the step
                step_output = step.executor(**step.params, state=state)

                result.status = StepStatus.COMPLETED
                result.result = step_output
                result.completed_at = datetime.now(timezone.utc)
                result.duration_seconds = (
                    result.completed_at - result.started_at
                ).total_seconds()

                # Emit completed event
                self._event_bus.publish(StepEvent(
                    event_type=WorkflowEventType.STEP_COMPLETED,
                    workflow_type=workflow_type,
                    step_id=step.step_id,
                    step_name=step.name,
                    duration_seconds=result.duration_seconds,
                    result=step_output,
                ))

                # Notify progress
                if self._progress_callback:
                    self._progress_callback(step.step_id, result)

                return result

            except Exception as e:
                last_error = str(e)
                attempts += 1
                result.retry_attempts = attempts
                logger.warning(f"Step {step.step_id} attempt {attempts} failed: {e}")

        # All retries exhausted
        result.status = StepStatus.FAILED
        result.error = last_error
        result.completed_at = datetime.now(timezone.utc)
        result.duration_seconds = (
            result.completed_at - result.started_at
        ).total_seconds()

        # Emit failed event
        self._event_bus.publish(StepEvent(
            event_type=WorkflowEventType.STEP_FAILED,
            workflow_type=workflow_type,
            step_id=step.step_id,
            step_name=step.name,
            duration_seconds=result.duration_seconds,
            error_message=last_error,
        ))

        # Notify progress
        if self._progress_callback:
            self._progress_callback(step.step_id, result)

        return result

    def _create_checkpoint(
        self,
        step_results: Dict[str, StepResult],
        workflow_state: Dict[str, Any],
    ) -> Checkpoint:
        """Create a checkpoint."""
        checkpoint = Checkpoint(
            step_results=copy.deepcopy(step_results),
            workflow_state=copy.deepcopy(workflow_state),
        )

        with self._lock:
            self._checkpoints[checkpoint.checkpoint_id] = checkpoint

        # Emit checkpoint event
        self._event_bus.publish(RollbackEvent(
            event_type=WorkflowEventType.CHECKPOINT_CREATED,
            checkpoint_id=checkpoint.checkpoint_id,
        ))

        logger.debug(f"Created checkpoint: {checkpoint.checkpoint_id}")
        return checkpoint

    def rollback(
        self,
        steps: List[StepDefinition],
        step_results: Dict[str, StepResult],
        reason: str = "",
        rollback_to_checkpoint: Optional[str] = None,
    ) -> ExecutionResult:
        """
        Rollback completed steps.

        Args:
            steps: Step definitions
            step_results: Current step results
            reason: Reason for rollback
            rollback_to_checkpoint: Optional checkpoint to rollback to

        Returns:
            ExecutionResult with rollback status
        """
        result = ExecutionResult(
            success=True,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

        # Emit rollback started
        self._event_bus.publish(RollbackEvent(
            event_type=WorkflowEventType.ROLLBACK_STARTED,
            rollback_reason=reason,
            checkpoint_id=rollback_to_checkpoint or "",
        ))

        steps_rolled_back = 0
        step_map = {s.step_id: s for s in steps}

        # Determine which steps to rollback
        if rollback_to_checkpoint:
            checkpoint = self._checkpoints.get(rollback_to_checkpoint)
            if checkpoint:
                # Rollback steps completed after checkpoint
                checkpoint_completed = {
                    sid for sid, sr in checkpoint.step_results.items()
                    if sr.status == StepStatus.COMPLETED
                }
                to_rollback = [
                    sid for sid, sr in step_results.items()
                    if sr.status == StepStatus.COMPLETED
                    and sid not in checkpoint_completed
                ]
            else:
                result.success = False
                result.errors.append(f"Checkpoint not found: {rollback_to_checkpoint}")
                return result
        else:
            # Rollback all completed steps
            to_rollback = [
                sid for sid, sr in step_results.items()
                if sr.status == StepStatus.COMPLETED
            ]

        # Rollback in reverse order
        for step_id in reversed(to_rollback):
            step = step_map.get(step_id)
            if step and step.rollback_fn:
                try:
                    step.rollback_fn(**step.params)
                    step_results[step_id].status = StepStatus.ROLLED_BACK
                    steps_rolled_back += 1
                    logger.info(f"Rolled back step: {step_id}")
                except Exception as e:
                    logger.error(f"Rollback failed for {step_id}: {e}")
                    result.errors.append(f"Rollback failed for {step_id}: {e}")

        # Emit rollback completed
        self._event_bus.publish(RollbackEvent(
            event_type=WorkflowEventType.ROLLBACK_COMPLETED,
            rollback_reason=reason,
            steps_rolled_back=steps_rolled_back,
            checkpoint_id=rollback_to_checkpoint or "",
        ))

        result.step_results = step_results
        result.status = ExecutionStatus.ROLLED_BACK
        result.message = f"Rolled back {steps_rolled_back} step(s)"
        result.completed_at = datetime.now(timezone.utc)
        result.duration_seconds = (
            result.completed_at - result.started_at
        ).total_seconds()

        return result

    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get a checkpoint by ID."""
        return self._checkpoints.get(checkpoint_id)

    def list_checkpoints(self) -> List[Checkpoint]:
        """List all checkpoints."""
        return list(self._checkpoints.values())

    def clear_checkpoints(self) -> None:
        """Clear all checkpoints."""
        with self._lock:
            self._checkpoints.clear()

    def shutdown(self) -> None:
        """Shutdown the executor."""
        self._executor.shutdown(wait=True)


# Global executor instance
_workflow_executor: Optional[WorkflowExecutor] = None


def get_workflow_executor(
    max_workers: int = 4,
) -> WorkflowExecutor:
    """Get the global workflow executor instance."""
    global _workflow_executor
    if _workflow_executor is None:
        _workflow_executor = WorkflowExecutor(max_workers=max_workers)
    return _workflow_executor


def reset_workflow_executor() -> None:
    """Reset the global workflow executor (for testing)."""
    global _workflow_executor
    if _workflow_executor:
        _workflow_executor.shutdown()
    _workflow_executor = None
