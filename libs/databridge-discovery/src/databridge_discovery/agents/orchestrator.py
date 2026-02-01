"""
Orchestrator for coordinating multi-agent workflows.

The orchestrator manages:
- Agent lifecycle
- Workflow execution
- Inter-agent communication
- State management
- Error recovery
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable
import uuid

from databridge_discovery.agents.base_agent import (
    BaseAgent,
    AgentCapability,
    AgentConfig,
    AgentState,
    AgentResult,
    AgentError,
    TaskContext,
)


class WorkflowState(str, Enum):
    """Workflow execution states."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowStep:
    """A step in a workflow."""

    id: str
    name: str
    agent_type: str
    capability: AgentCapability
    input_mapping: dict[str, str] = field(default_factory=dict)
    output_key: str = ""
    depends_on: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    result: AgentResult | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "agent_type": self.agent_type,
            "capability": self.capability.value,
            "input_mapping": self.input_mapping,
            "output_key": self.output_key,
            "depends_on": self.depends_on,
            "config": self.config,
            "status": self.status,
            "result": self.result.to_dict() if self.result else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class WorkflowDefinition:
    """Definition of a workflow."""

    id: str
    name: str
    description: str = ""
    steps: list[WorkflowStep] = field(default_factory=list)
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "steps": [s.to_dict() for s in self.steps],
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
        }


@dataclass
class WorkflowExecution:
    """An execution of a workflow."""

    id: str
    workflow_id: str
    workflow_name: str
    state: WorkflowState = WorkflowState.PENDING
    current_step: str | None = None
    steps_completed: int = 0
    steps_total: int = 0
    input_data: dict[str, Any] = field(default_factory=dict)
    output_data: dict[str, Any] = field(default_factory=dict)
    shared_state: dict[str, Any] = field(default_factory=dict)
    step_results: dict[str, AgentResult] = field(default_factory=dict)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "state": self.state.value,
            "current_step": self.current_step,
            "steps_completed": self.steps_completed,
            "steps_total": self.steps_total,
            "progress": self.steps_completed / self.steps_total if self.steps_total > 0 else 0,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
        }


@dataclass
class OrchestratorConfig:
    """Configuration for the orchestrator."""

    max_parallel_agents: int = 4
    default_timeout_seconds: int = 300
    retry_failed_steps: bool = True
    max_retries: int = 3
    log_level: str = "INFO"


class Orchestrator:
    """
    Orchestrator for coordinating multi-agent workflows.

    Manages:
    - Agent registration and lifecycle
    - Workflow definition and execution
    - Inter-agent communication via shared state
    - Error handling and recovery
    - Progress tracking

    Example:
        orchestrator = Orchestrator()

        # Register agents
        orchestrator.register_agent("scanner", SchemaScanner())
        orchestrator.register_agent("extractor", LogicExtractor())

        # Define workflow
        workflow = orchestrator.create_workflow("discovery", [
            {"agent": "scanner", "capability": "scan_schema"},
            {"agent": "extractor", "capability": "extract_case"},
        ])

        # Execute
        result = orchestrator.execute_workflow(workflow.id, input_data)
    """

    def __init__(self, config: OrchestratorConfig | None = None):
        """Initialize the orchestrator."""
        self._id = str(uuid.uuid4())[:8]
        self._config = config or OrchestratorConfig()
        self._agents: dict[str, BaseAgent] = {}
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._executions: dict[str, WorkflowExecution] = {}
        self._event_handlers: dict[str, list[Callable]] = {
            "workflow_started": [],
            "workflow_completed": [],
            "workflow_failed": [],
            "step_started": [],
            "step_completed": [],
            "step_failed": [],
        }

    @property
    def id(self) -> str:
        """Get orchestrator ID."""
        return self._id

    def register_agent(
        self,
        name: str,
        agent: BaseAgent,
        replace: bool = False,
    ) -> None:
        """
        Register an agent with the orchestrator.

        Args:
            name: Agent name for reference
            agent: Agent instance
            replace: Replace if already registered
        """
        if name in self._agents and not replace:
            raise ValueError(f"Agent '{name}' already registered")

        self._agents[name] = agent

    def unregister_agent(self, name: str) -> bool:
        """
        Unregister an agent.

        Args:
            name: Agent name

        Returns:
            True if unregistered
        """
        if name in self._agents:
            del self._agents[name]
            return True
        return False

    def get_agent(self, name: str) -> BaseAgent | None:
        """
        Get a registered agent.

        Args:
            name: Agent name

        Returns:
            Agent instance or None
        """
        return self._agents.get(name)

    def list_agents(self) -> list[dict[str, Any]]:
        """
        List all registered agents.

        Returns:
            List of agent info dictionaries
        """
        return [
            {
                "name": name,
                "type": agent.__class__.__name__,
                "state": agent.state.value,
                "capabilities": [c.value for c in agent.get_capabilities()],
            }
            for name, agent in self._agents.items()
        ]

    def create_workflow(
        self,
        name: str,
        steps: list[dict[str, Any]],
        description: str = "",
    ) -> WorkflowDefinition:
        """
        Create a workflow definition.

        Args:
            name: Workflow name
            steps: List of step definitions
            description: Workflow description

        Returns:
            WorkflowDefinition
        """
        workflow_id = str(uuid.uuid4())[:8]
        workflow_steps = []

        for i, step_def in enumerate(steps):
            step = WorkflowStep(
                id=f"step_{i+1}",
                name=step_def.get("name", f"Step {i+1}"),
                agent_type=step_def.get("agent", step_def.get("agent_type", "")),
                capability=self._parse_capability(step_def.get("capability", "")),
                input_mapping=step_def.get("input_mapping", {}),
                output_key=step_def.get("output_key", f"step_{i+1}_output"),
                depends_on=step_def.get("depends_on", []),
                config=step_def.get("config", {}),
            )
            workflow_steps.append(step)

        workflow = WorkflowDefinition(
            id=workflow_id,
            name=name,
            description=description,
            steps=workflow_steps,
        )

        self._workflows[workflow_id] = workflow
        return workflow

    def execute_workflow(
        self,
        workflow_id: str,
        input_data: dict[str, Any],
    ) -> WorkflowExecution:
        """
        Execute a workflow.

        Args:
            workflow_id: Workflow ID
            input_data: Input data for workflow

        Returns:
            WorkflowExecution
        """
        if workflow_id not in self._workflows:
            raise ValueError(f"Workflow not found: {workflow_id}")

        workflow = self._workflows[workflow_id]

        # Create execution
        execution = WorkflowExecution(
            id=str(uuid.uuid4())[:8],
            workflow_id=workflow_id,
            workflow_name=workflow.name,
            steps_total=len(workflow.steps),
            input_data=input_data,
            shared_state={"input": input_data},
        )

        self._executions[execution.id] = execution
        self._emit_event("workflow_started", execution)

        try:
            self._run_workflow(workflow, execution)
        except Exception as e:
            execution.state = WorkflowState.FAILED
            execution.error = str(e)
            execution.completed_at = datetime.now()
            self._emit_event("workflow_failed", execution)

        return execution

    def _run_workflow(
        self,
        workflow: WorkflowDefinition,
        execution: WorkflowExecution,
    ) -> None:
        """Run workflow steps."""
        execution.state = WorkflowState.RUNNING
        execution.started_at = datetime.now()

        # Build dependency graph
        pending_steps = {s.id: s for s in workflow.steps}
        completed_steps: set[str] = set()

        while pending_steps and execution.state == WorkflowState.RUNNING:
            # Find steps ready to execute
            ready_steps = [
                step for step in pending_steps.values()
                if all(dep in completed_steps for dep in step.depends_on)
            ]

            if not ready_steps:
                if pending_steps:
                    raise AgentError(
                        "Workflow deadlock: no steps ready to execute",
                        "Orchestrator",
                    )
                break

            # Execute ready steps (could be parallelized)
            for step in ready_steps:
                self._execute_step(step, execution)

                if step.status == "completed":
                    completed_steps.add(step.id)
                    execution.steps_completed += 1
                    del pending_steps[step.id]
                elif step.status == "failed":
                    execution.state = WorkflowState.FAILED
                    execution.error = f"Step {step.id} failed"
                    break

        if execution.state == WorkflowState.RUNNING:
            execution.state = WorkflowState.COMPLETED
            execution.completed_at = datetime.now()
            self._emit_event("workflow_completed", execution)

    def _execute_step(
        self,
        step: WorkflowStep,
        execution: WorkflowExecution,
    ) -> None:
        """Execute a single workflow step."""
        step.status = "running"
        step.started_at = datetime.now()
        execution.current_step = step.id

        self._emit_event("step_started", step, execution)

        # Get agent
        agent = self._agents.get(step.agent_type)
        if not agent:
            step.status = "failed"
            step.result = AgentResult(
                agent_id="",
                agent_name=step.agent_type,
                capability=step.capability.value,
                success=False,
                error=f"Agent not found: {step.agent_type}",
            )
            self._emit_event("step_failed", step, execution)
            return

        # Build input data
        input_data = self._resolve_inputs(step.input_mapping, execution.shared_state)

        # Create task context
        context = TaskContext(
            task_id=f"{execution.id}_{step.id}",
            workflow_id=execution.id,
            input_data=input_data,
            shared_state=execution.shared_state,
        )

        # Execute
        try:
            result = agent.execute(step.capability, context, **step.config)
            step.result = result

            if result.success:
                step.status = "completed"
                # Store output in shared state
                if step.output_key:
                    execution.shared_state[step.output_key] = result.data
                execution.step_results[step.id] = result
                self._emit_event("step_completed", step, execution)
            else:
                step.status = "failed"
                self._emit_event("step_failed", step, execution)

        except Exception as e:
            step.status = "failed"
            step.result = AgentResult(
                agent_id=agent.id,
                agent_name=agent.name,
                capability=step.capability.value,
                success=False,
                error=str(e),
            )
            self._emit_event("step_failed", step, execution)

        step.completed_at = datetime.now()

    def _resolve_inputs(
        self,
        mapping: dict[str, str],
        shared_state: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve input mapping from shared state."""
        result = {}

        for target_key, source_path in mapping.items():
            value = self._get_nested_value(shared_state, source_path)
            if value is not None:
                result[target_key] = value

        # Include all shared state if no mapping specified
        if not mapping:
            result = dict(shared_state)

        return result

    def _get_nested_value(
        self,
        data: dict[str, Any],
        path: str,
    ) -> Any:
        """Get nested value from dictionary using dot notation."""
        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    def _parse_capability(self, capability_str: str) -> AgentCapability:
        """Parse capability string to enum."""
        try:
            return AgentCapability(capability_str)
        except ValueError:
            # Try uppercase
            try:
                return AgentCapability(capability_str.upper())
            except ValueError:
                raise ValueError(f"Unknown capability: {capability_str}")

    def pause_workflow(self, execution_id: str) -> bool:
        """
        Pause a running workflow.

        Args:
            execution_id: Execution ID

        Returns:
            True if paused
        """
        execution = self._executions.get(execution_id)
        if execution and execution.state == WorkflowState.RUNNING:
            execution.state = WorkflowState.PAUSED
            return True
        return False

    def resume_workflow(self, execution_id: str) -> bool:
        """
        Resume a paused workflow.

        Args:
            execution_id: Execution ID

        Returns:
            True if resumed
        """
        execution = self._executions.get(execution_id)
        if execution and execution.state == WorkflowState.PAUSED:
            execution.state = WorkflowState.RUNNING
            # Continue execution would happen here
            return True
        return False

    def cancel_workflow(self, execution_id: str) -> bool:
        """
        Cancel a workflow.

        Args:
            execution_id: Execution ID

        Returns:
            True if cancelled
        """
        execution = self._executions.get(execution_id)
        if execution and execution.state in [WorkflowState.RUNNING, WorkflowState.PAUSED]:
            execution.state = WorkflowState.CANCELLED
            execution.completed_at = datetime.now()
            return True
        return False

    def get_workflow_status(self, execution_id: str) -> dict[str, Any] | None:
        """
        Get workflow execution status.

        Args:
            execution_id: Execution ID

        Returns:
            Status dictionary or None
        """
        execution = self._executions.get(execution_id)
        if execution:
            return execution.to_dict()
        return None

    def get_workflow_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        Get workflow execution history.

        Args:
            limit: Maximum number of executions

        Returns:
            List of execution summaries
        """
        executions = sorted(
            self._executions.values(),
            key=lambda e: e.started_at or datetime.min,
            reverse=True,
        )
        return [e.to_dict() for e in executions[:limit]]

    def on(self, event: str, handler: Callable) -> None:
        """
        Register event handler.

        Args:
            event: Event name
            handler: Handler function
        """
        if event in self._event_handlers:
            self._event_handlers[event].append(handler)

    def _emit_event(self, event: str, *args: Any) -> None:
        """Emit event to handlers."""
        for handler in self._event_handlers.get(event, []):
            try:
                handler(*args)
            except Exception:
                pass  # Don't fail on handler errors

    def to_dict(self) -> dict[str, Any]:
        """Convert orchestrator state to dictionary."""
        return {
            "id": self._id,
            "agents": self.list_agents(),
            "workflows": [w.to_dict() for w in self._workflows.values()],
            "active_executions": [
                e.to_dict() for e in self._executions.values()
                if e.state in [WorkflowState.RUNNING, WorkflowState.PAUSED]
            ],
            "config": {
                "max_parallel_agents": self._config.max_parallel_agents,
                "default_timeout_seconds": self._config.default_timeout_seconds,
            },
        }
