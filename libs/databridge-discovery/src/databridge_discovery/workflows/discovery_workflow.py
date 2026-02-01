"""
Discovery Workflow for end-to-end hierarchy discovery.

This workflow orchestrates the full discovery process:
1. Scan source schema
2. Extract SQL logic
3. Design star schema
4. Generate models
5. Validate deployment
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from databridge_discovery.agents.base_agent import AgentCapability, TaskContext
from databridge_discovery.agents.orchestrator import (
    Orchestrator,
    WorkflowDefinition,
    WorkflowExecution,
    WorkflowState,
)


class DiscoveryPhase(str, Enum):
    """Phases of the discovery workflow."""

    SCAN = "scan"
    EXTRACT = "extract"
    DESIGN = "design"
    GENERATE = "generate"
    VALIDATE = "validate"


@dataclass
class DiscoveryWorkflowConfig:
    """Configuration for discovery workflow."""

    name: str = "discovery_workflow"
    scan_schema: bool = True
    extract_logic: bool = True
    design_model: bool = True
    generate_dbt: bool = True
    validate_deployment: bool = False
    dry_run: bool = True
    target_schema: str = "ANALYTICS"
    dialect: str = "snowflake"


@dataclass
class DiscoveryWorkflowResult:
    """Result of discovery workflow execution."""

    workflow_id: str
    execution_id: str
    status: WorkflowState
    phases_completed: list[str] = field(default_factory=list)
    discovered_hierarchies: list[dict[str, Any]] = field(default_factory=list)
    generated_models: list[dict[str, Any]] = field(default_factory=list)
    validation_results: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "execution_id": self.execution_id,
            "status": self.status.value,
            "phases_completed": self.phases_completed,
            "hierarchy_count": len(self.discovered_hierarchies),
            "model_count": len(self.generated_models),
            "validation_count": len(self.validation_results),
            "error_count": len(self.errors),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
        }


class DiscoveryWorkflow:
    """
    End-to-end discovery workflow.

    Orchestrates:
    1. Schema scanning to extract metadata
    2. SQL logic extraction (CASE statements, calculations)
    3. Star schema design
    4. dbt model generation
    5. Deployment validation

    Example:
        workflow = DiscoveryWorkflow()

        result = workflow.execute({
            "tables": [...],
            "sql_queries": [...],
        })
    """

    def __init__(
        self,
        orchestrator: Orchestrator | None = None,
        config: DiscoveryWorkflowConfig | None = None,
    ):
        """
        Initialize discovery workflow.

        Args:
            orchestrator: Orchestrator instance (creates default if None)
            config: Workflow configuration
        """
        self._config = config or DiscoveryWorkflowConfig()
        self._orchestrator = orchestrator or self._create_orchestrator()
        self._workflow: WorkflowDefinition | None = None

    def _create_orchestrator(self) -> Orchestrator:
        """Create and configure default orchestrator."""
        from databridge_discovery.agents.schema_scanner import SchemaScanner
        from databridge_discovery.agents.logic_extractor import LogicExtractor
        from databridge_discovery.agents.warehouse_architect import WarehouseArchitect
        from databridge_discovery.agents.deploy_validator import DeployValidator

        orchestrator = Orchestrator()

        # Register agents
        orchestrator.register_agent("scanner", SchemaScanner())
        orchestrator.register_agent("extractor", LogicExtractor())
        orchestrator.register_agent("architect", WarehouseArchitect())
        orchestrator.register_agent("validator", DeployValidator())

        return orchestrator

    def create_workflow(self) -> WorkflowDefinition:
        """Create the workflow definition."""
        steps = []

        # Phase 1: Schema Scan
        if self._config.scan_schema:
            steps.append({
                "name": "Scan Schema",
                "agent": "scanner",
                "capability": AgentCapability.SCAN_SCHEMA.value,
                "input_mapping": {"tables": "input.tables", "schema_name": "input.schema_name"},
                "output_key": "schema_scan",
            })

            steps.append({
                "name": "Detect Keys",
                "agent": "scanner",
                "capability": AgentCapability.DETECT_KEYS.value,
                "input_mapping": {"tables": "schema_scan.tables"},
                "output_key": "key_detection",
                "depends_on": ["step_1"],
            })

        # Phase 2: Logic Extraction
        if self._config.extract_logic:
            steps.append({
                "name": "Parse SQL",
                "agent": "extractor",
                "capability": AgentCapability.PARSE_SQL.value,
                "input_mapping": {"sql": "input.sql"},
                "output_key": "parsed_sql",
            })

            steps.append({
                "name": "Extract CASE Statements",
                "agent": "extractor",
                "capability": AgentCapability.EXTRACT_CASE.value,
                "input_mapping": {"sql": "input.sql"},
                "output_key": "case_extraction",
            })

            steps.append({
                "name": "Detect Aggregations",
                "agent": "extractor",
                "capability": AgentCapability.DETECT_AGGREGATIONS.value,
                "input_mapping": {"sql": "input.sql"},
                "output_key": "aggregations",
            })

        # Phase 3: Star Schema Design
        if self._config.design_model:
            step_id = len(steps) + 1
            depends = []
            if self._config.scan_schema:
                depends.append("step_2")  # Key detection
            if self._config.extract_logic:
                depends.append("step_4")  # CASE extraction

            steps.append({
                "name": "Design Star Schema",
                "agent": "architect",
                "capability": AgentCapability.DESIGN_STAR_SCHEMA.value,
                "input_mapping": {
                    "tables": "schema_scan.tables" if self._config.scan_schema else "input.tables",
                    "relationships": "key_detection.foreign_keys" if self._config.scan_schema else "input.relationships",
                },
                "output_key": "star_schema",
                "depends_on": depends,
            })

        # Phase 4: Model Generation
        if self._config.generate_dbt:
            depends = []
            if self._config.design_model:
                depends.append(f"step_{len(steps)}")

            steps.append({
                "name": "Generate dbt Models",
                "agent": "architect",
                "capability": AgentCapability.DBT_MODELS.value,
                "input_mapping": {
                    "design": "star_schema.design",
                    "target_schema": "input.target_schema",
                },
                "output_key": "dbt_models",
                "depends_on": depends,
                "config": {"target_schema": self._config.target_schema},
            })

        # Phase 5: Validation
        if self._config.validate_deployment:
            depends = []
            if self._config.generate_dbt:
                depends.append(f"step_{len(steps)}")

            steps.append({
                "name": "Validate Deployment",
                "agent": "validator",
                "capability": AgentCapability.EXECUTE_DDL.value,
                "input_mapping": {"ddl_statements": "dbt_models.models"},
                "output_key": "validation",
                "depends_on": depends,
                "config": {"dry_run": self._config.dry_run},
            })

        self._workflow = self._orchestrator.create_workflow(
            name=self._config.name,
            steps=steps,
            description="End-to-end hierarchy discovery workflow",
        )

        return self._workflow

    def execute(
        self,
        input_data: dict[str, Any],
    ) -> DiscoveryWorkflowResult:
        """
        Execute the discovery workflow.

        Args:
            input_data: Input data containing:
                - tables: List of table metadata
                - sql: SQL query with CASE statements
                - schema_name: Source schema name
                - target_schema: Target schema name

        Returns:
            DiscoveryWorkflowResult
        """
        # Create workflow if not exists
        if not self._workflow:
            self.create_workflow()

        # Add defaults to input
        if "target_schema" not in input_data:
            input_data["target_schema"] = self._config.target_schema

        # Execute workflow
        execution = self._orchestrator.execute_workflow(
            self._workflow.id,
            input_data,
        )

        # Build result
        return self._build_result(execution)

    def _build_result(self, execution: WorkflowExecution) -> DiscoveryWorkflowResult:
        """Build result from execution."""
        result = DiscoveryWorkflowResult(
            workflow_id=execution.workflow_id,
            execution_id=execution.id,
            status=execution.state,
            started_at=execution.started_at,
            completed_at=execution.completed_at,
        )

        if execution.started_at and execution.completed_at:
            result.duration_seconds = (
                execution.completed_at - execution.started_at
            ).total_seconds()

        # Extract phase results
        shared_state = execution.shared_state

        # Schema scan results
        if "schema_scan" in shared_state:
            result.phases_completed.append("scan")

        # Case extraction results
        if "case_extraction" in shared_state:
            result.phases_completed.append("extract")
            case_data = shared_state["case_extraction"]
            if "hierarchies" in case_data:
                result.discovered_hierarchies = case_data["hierarchies"]

        # Star schema results
        if "star_schema" in shared_state:
            result.phases_completed.append("design")

        # dbt models
        if "dbt_models" in shared_state:
            result.phases_completed.append("generate")
            result.generated_models = shared_state["dbt_models"].get("models", [])

        # Validation results
        if "validation" in shared_state:
            result.phases_completed.append("validate")
            result.validation_results = shared_state["validation"].get("executions", [])

        # Errors
        if execution.error:
            result.errors.append(execution.error)

        for step_id, step_result in execution.step_results.items():
            if not step_result.success and step_result.error:
                result.errors.append(f"{step_id}: {step_result.error}")

        return result

    def get_status(self, execution_id: str) -> dict[str, Any] | None:
        """
        Get workflow execution status.

        Args:
            execution_id: Execution ID

        Returns:
            Status dictionary
        """
        return self._orchestrator.get_workflow_status(execution_id)

    def pause(self, execution_id: str) -> bool:
        """Pause workflow execution."""
        return self._orchestrator.pause_workflow(execution_id)

    def resume(self, execution_id: str) -> bool:
        """Resume workflow execution."""
        return self._orchestrator.resume_workflow(execution_id)

    def cancel(self, execution_id: str) -> bool:
        """Cancel workflow execution."""
        return self._orchestrator.cancel_workflow(execution_id)
