"""
PlannerAgent - AI-powered workflow planning using Claude.

This agent uses Claude to intelligently decompose user requests into
executable workflow steps, identifying the right agents and capabilities
to accomplish complex data engineering tasks.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
import uuid

try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

logger = logging.getLogger(__name__)


class PlannerCapability(str, Enum):
    """Planner agent capabilities."""

    PLAN_WORKFLOW = "plan_workflow"
    ANALYZE_REQUEST = "analyze_request"
    SUGGEST_AGENTS = "suggest_agents"
    OPTIMIZE_PLAN = "optimize_plan"
    EXPLAIN_PLAN = "explain_plan"


class PlannerState(str, Enum):
    """Planner execution states."""

    IDLE = "idle"
    PLANNING = "planning"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AgentInfo:
    """Information about an available agent."""

    name: str
    agent_type: str
    description: str
    capabilities: list[str]
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "agent_type": self.agent_type,
            "description": self.description,
            "capabilities": self.capabilities,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
        }


@dataclass
class PlannedStep:
    """A step in the planned workflow."""

    id: str
    name: str
    description: str
    agent_type: str
    capability: str
    input_mapping: dict[str, str] = field(default_factory=dict)
    output_key: str = ""
    depends_on: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    reasoning: str = ""  # Why this step is needed

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "agent_type": self.agent_type,
            "capability": self.capability,
            "input_mapping": self.input_mapping,
            "output_key": self.output_key,
            "depends_on": self.depends_on,
            "config": self.config,
            "reasoning": self.reasoning,
        }


@dataclass
class WorkflowPlan:
    """A complete workflow plan."""

    id: str
    name: str
    description: str
    user_request: str
    steps: list[PlannedStep] = field(default_factory=list)
    estimated_duration: str = ""
    confidence_score: float = 0.0
    warnings: list[str] = field(default_factory=list)
    alternatives: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "user_request": self.user_request,
            "steps": [s.to_dict() for s in self.steps],
            "step_count": len(self.steps),
            "estimated_duration": self.estimated_duration,
            "confidence_score": self.confidence_score,
            "warnings": self.warnings,
            "alternatives": self.alternatives,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class PlannerConfig:
    """Configuration for the PlannerAgent."""

    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = 4096
    temperature: float = 0.3  # Lower for more consistent planning
    max_steps: int = 10
    enable_parallel_steps: bool = True
    include_reasoning: bool = True
    api_key: Optional[str] = None


# Default available agents in the DataBridge system
DEFAULT_AGENTS: list[AgentInfo] = [
    AgentInfo(
        name="schema_scanner",
        agent_type="SchemaScanner",
        description="Scans database schemas to discover tables, columns, keys, and data types. Profiles data quality and infers table types (fact/dimension/staging).",
        capabilities=["scan_schema", "extract_metadata", "detect_keys", "sample_profiles"],
        input_schema={"connection_string": "str", "schema_name": "str", "sample_size": "int"},
        output_schema={"tables": "list", "columns": "list", "keys": "list", "profiles": "dict"},
    ),
    AgentInfo(
        name="logic_extractor",
        agent_type="LogicExtractor",
        description="Parses SQL statements to extract CASE logic, calculations, aggregations, and business rules for hierarchy creation.",
        capabilities=["parse_sql", "extract_case", "identify_calcs", "detect_aggregations"],
        input_schema={"sql": "str", "sql_file_path": "str"},
        output_schema={"case_statements": "list", "calculations": "list", "aggregations": "list"},
    ),
    AgentInfo(
        name="warehouse_architect",
        agent_type="WarehouseArchitect",
        description="Designs star schema structures, generates dimension and fact table specifications, and creates dbt models.",
        capabilities=["design_star_schema", "generate_dims", "generate_facts", "dbt_models"],
        input_schema={"tables": "list", "business_rules": "dict"},
        output_schema={"star_schema": "dict", "dimensions": "list", "facts": "list", "dbt_models": "list"},
    ),
    AgentInfo(
        name="deploy_validator",
        agent_type="DeployValidator",
        description="Executes DDL statements, runs dbt commands, and validates deployments with row counts and aggregate comparisons.",
        capabilities=["execute_ddl", "run_dbt", "validate_counts", "compare_aggregates"],
        input_schema={"ddl_statements": "list", "dbt_project": "str"},
        output_schema={"execution_results": "list", "validation_results": "dict"},
    ),
    AgentInfo(
        name="hierarchy_builder",
        agent_type="HierarchyBuilder",
        description="Creates and manages hierarchical data structures using MCP tools. Supports flexible import, property management, and deployment.",
        capabilities=["create_hierarchy", "import_hierarchy", "add_properties", "deploy_hierarchy"],
        input_schema={"project_id": "str", "hierarchy_data": "dict", "properties": "dict"},
        output_schema={"hierarchy_id": "str", "mappings": "list", "deployment_status": "str"},
    ),
    AgentInfo(
        name="data_reconciler",
        agent_type="DataReconciler",
        description="Compares data between sources, identifies discrepancies, performs fuzzy matching, and generates reconciliation reports.",
        capabilities=["compare_sources", "fuzzy_match", "identify_orphans", "generate_report"],
        input_schema={"source_a": "dict", "source_b": "dict", "match_columns": "list"},
        output_schema={"matches": "list", "orphans": "list", "conflicts": "list", "report": "dict"},
    ),
    # Unified Agent agents for Book ↔ Librarian ↔ Researcher operations
    AgentInfo(
        name="book_manipulator",
        agent_type="BookManipulator",
        description="Manipulates Book hierarchies in-memory. Creates Books from CSV/JSON, adds/removes nodes, applies formulas, and exports to various formats.",
        capabilities=["create_book", "add_node", "remove_node", "apply_formula", "export_book"],
        input_schema={"book_name": "str", "node_data": "dict", "formula": "dict"},
        output_schema={"book": "dict", "node_count": "int", "export_path": "str"},
    ),
    AgentInfo(
        name="librarian_sync",
        agent_type="LibrarianSync",
        description="Synchronizes between Book (Python) and Librarian (NestJS). Checks out projects to Books, promotes Books to Librarian, and handles bidirectional sync.",
        capabilities=["checkout_project", "promote_book", "sync_changes", "diff_systems"],
        input_schema={"book_name": "str", "project_id": "str", "direction": "str"},
        output_schema={"sync_result": "dict", "created": "int", "updated": "int", "diff": "dict"},
    ),
    AgentInfo(
        name="researcher_analyst",
        agent_type="ResearcherAnalyst",
        description="Runs analytics on Book/Librarian data using Researcher API. Validates source mappings, compares hierarchy data with databases, profiles source columns.",
        capabilities=["validate_mappings", "compare_schemas", "profile_sources", "analyze_data"],
        input_schema={"book_name": "str", "connection_id": "str", "analysis_type": "str"},
        output_schema={"validation": "dict", "profile": "dict", "comparison": "dict"},
    ),
    # Cortex Agent for Snowflake Cortex AI integration
    AgentInfo(
        name="cortex_agent",
        agent_type="CortexAgent",
        description="Executes Snowflake Cortex AI functions with orchestrated reasoning. Handles text generation, summarization, sentiment analysis, and multi-step data cleaning tasks.",
        capabilities=["complete", "summarize", "sentiment", "translate", "extract_answer", "reason", "analyze_data", "clean_data"],
        input_schema={"goal": "str", "table_name": "str", "context": "dict"},
        output_schema={"result": "str", "thinking_steps": "list", "conversation_id": "str"},
    ),
]


class PlannerAgent:
    """
    AI-powered workflow planner using Claude.

    The PlannerAgent analyzes user requests and creates executable workflow
    plans by selecting appropriate agents and ordering steps based on
    dependencies.

    Example:
        planner = PlannerAgent()

        # Plan a workflow
        plan = planner.plan_workflow(
            "Extract hierarchies from SQL CASE statements and deploy to Snowflake"
        )

        # Get workflow definition for orchestrator
        workflow_def = planner.to_workflow_definition(plan)
    """

    def __init__(
        self,
        config: Optional[PlannerConfig] = None,
        available_agents: Optional[list[AgentInfo]] = None,
    ):
        """
        Initialize the PlannerAgent.

        Args:
            config: Planner configuration
            available_agents: List of available agents (uses defaults if not provided)
        """
        self._id = str(uuid.uuid4())[:8]
        self._config = config or PlannerConfig()
        self._state = PlannerState.IDLE
        self._available_agents = available_agents or DEFAULT_AGENTS
        self._plans: list[WorkflowPlan] = []
        self._created_at = datetime.now()

        # Initialize Anthropic client
        self._client: Optional[Anthropic] = None
        if ANTHROPIC_AVAILABLE:
            api_key = self._config.api_key or os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                self._client = Anthropic(api_key=api_key)

    @property
    def id(self) -> str:
        """Get planner ID."""
        return self._id

    @property
    def state(self) -> PlannerState:
        """Get current state."""
        return self._state

    @property
    def available_agents(self) -> list[AgentInfo]:
        """Get available agents."""
        return self._available_agents

    def register_agent(self, agent: AgentInfo) -> None:
        """
        Register an additional agent.

        Args:
            agent: Agent information
        """
        # Remove existing with same name
        self._available_agents = [a for a in self._available_agents if a.name != agent.name]
        self._available_agents.append(agent)

    def plan_workflow(
        self,
        user_request: str,
        context: Optional[dict[str, Any]] = None,
    ) -> WorkflowPlan:
        """
        Create a workflow plan from a user request.

        Args:
            user_request: Natural language description of what the user wants
            context: Optional additional context (e.g., available data, constraints)

        Returns:
            WorkflowPlan with steps to execute
        """
        self._state = PlannerState.PLANNING

        try:
            if self._client:
                plan = self._plan_with_claude(user_request, context)
            else:
                plan = self._plan_fallback(user_request, context)

            self._plans.append(plan)
            self._state = PlannerState.COMPLETED
            return plan

        except Exception as e:
            self._state = PlannerState.FAILED
            logger.error(f"Planning failed: {e}")
            raise

    def _plan_with_claude(
        self,
        user_request: str,
        context: Optional[dict[str, Any]] = None,
    ) -> WorkflowPlan:
        """Use Claude to create an intelligent plan."""

        # Build the system prompt
        system_prompt = self._build_system_prompt()

        # Build the user message
        user_message = self._build_user_message(user_request, context)

        # Call Claude
        response = self._client.messages.create(
            model=self._config.model,
            max_tokens=self._config.max_tokens,
            temperature=self._config.temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        # Parse the response
        plan = self._parse_claude_response(response.content[0].text, user_request)

        return plan

    def _build_system_prompt(self) -> str:
        """Build the system prompt for Claude."""

        agents_description = "\n".join([
            f"- **{a.name}** ({a.agent_type}): {a.description}\n"
            f"  Capabilities: {', '.join(a.capabilities)}"
            for a in self._available_agents
        ])

        return f"""You are a workflow planning agent for DataBridge AI, a data engineering platform.
Your job is to analyze user requests and create optimal workflow plans using available agents.

## Available Agents

{agents_description}

## Planning Guidelines

1. **Identify the Goal**: Understand what the user wants to achieve
2. **Select Agents**: Choose the minimum set of agents needed
3. **Order Steps**: Determine dependencies and optimal execution order
4. **Map Data Flow**: Define how outputs from one step feed into the next
5. **Consider Parallelism**: Steps without dependencies can run in parallel

## Output Format

Respond with a JSON object containing the workflow plan:

```json
{{
    "name": "Descriptive workflow name",
    "description": "What this workflow accomplishes",
    "confidence_score": 0.95,
    "estimated_duration": "5-10 minutes",
    "steps": [
        {{
            "id": "step_1",
            "name": "Step name",
            "description": "What this step does",
            "agent_type": "agent_name",
            "capability": "capability_name",
            "input_mapping": {{"param": "source.path"}},
            "output_key": "step_1_output",
            "depends_on": [],
            "reasoning": "Why this step is needed"
        }}
    ],
    "warnings": ["Any concerns or limitations"],
    "alternatives": ["Other approaches considered"]
}}
```

## Important Rules

- Only use agents and capabilities from the available list
- Ensure step dependencies form a valid DAG (no cycles)
- Use clear, descriptive names for steps and output keys
- Include reasoning for each step when beneficial
- Flag any ambiguities or missing information as warnings
- Keep plans focused and avoid unnecessary steps
"""

    def _build_user_message(
        self,
        user_request: str,
        context: Optional[dict[str, Any]] = None,
    ) -> str:
        """Build the user message for Claude."""

        message = f"""Please create a workflow plan for the following request:

## User Request
{user_request}
"""

        if context:
            message += f"""
## Additional Context
```json
{json.dumps(context, indent=2)}
```
"""

        message += """
Please respond with a valid JSON workflow plan.
"""

        return message

    def _parse_claude_response(
        self,
        response_text: str,
        user_request: str,
    ) -> WorkflowPlan:
        """Parse Claude's response into a WorkflowPlan."""

        # Extract JSON from response
        json_start = response_text.find("{")
        json_end = response_text.rfind("}") + 1

        if json_start == -1 or json_end == 0:
            raise ValueError("No JSON found in Claude response")

        json_str = response_text[json_start:json_end]
        data = json.loads(json_str)

        # Build steps
        steps = []
        for step_data in data.get("steps", []):
            step = PlannedStep(
                id=step_data.get("id", f"step_{len(steps)+1}"),
                name=step_data.get("name", "Unnamed Step"),
                description=step_data.get("description", ""),
                agent_type=step_data.get("agent_type", ""),
                capability=step_data.get("capability", ""),
                input_mapping=step_data.get("input_mapping", {}),
                output_key=step_data.get("output_key", f"step_{len(steps)+1}_output"),
                depends_on=step_data.get("depends_on", []),
                config=step_data.get("config", {}),
                reasoning=step_data.get("reasoning", ""),
            )
            steps.append(step)

        # Create plan
        plan = WorkflowPlan(
            id=str(uuid.uuid4())[:8],
            name=data.get("name", "Generated Workflow"),
            description=data.get("description", ""),
            user_request=user_request,
            steps=steps,
            estimated_duration=data.get("estimated_duration", "Unknown"),
            confidence_score=data.get("confidence_score", 0.8),
            warnings=data.get("warnings", []),
            alternatives=data.get("alternatives", []),
        )

        return plan

    def _plan_fallback(
        self,
        user_request: str,
        context: Optional[dict[str, Any]] = None,
    ) -> WorkflowPlan:
        """
        Create a basic plan without Claude (fallback mode).

        Uses keyword matching to identify relevant agents.
        """
        request_lower = user_request.lower()
        steps = []

        # Keyword-based agent selection
        agent_keywords = {
            "schema_scanner": ["scan", "schema", "database", "tables", "columns", "metadata"],
            "logic_extractor": ["sql", "case", "extract", "parse", "logic", "calculation"],
            "warehouse_architect": ["design", "star schema", "dimension", "fact", "dbt", "model"],
            "deploy_validator": ["deploy", "execute", "validate", "ddl", "test"],
            "hierarchy_builder": ["hierarchy", "tree", "parent", "child", "level", "mapping"],
            "data_reconciler": ["reconcile", "compare", "match", "discrepancy", "orphan"],
        }

        selected_agents = []
        for agent_name, keywords in agent_keywords.items():
            if any(kw in request_lower for kw in keywords):
                agent_info = next(
                    (a for a in self._available_agents if a.name == agent_name),
                    None
                )
                if agent_info:
                    selected_agents.append(agent_info)

        # Create steps for selected agents
        for i, agent in enumerate(selected_agents):
            step = PlannedStep(
                id=f"step_{i+1}",
                name=f"{agent.agent_type} Execution",
                description=agent.description,
                agent_type=agent.name,
                capability=agent.capabilities[0] if agent.capabilities else "",
                output_key=f"step_{i+1}_output",
                depends_on=[f"step_{i}"] if i > 0 else [],
                reasoning=f"Selected based on keyword match in user request",
            )
            steps.append(step)

        # Default to a generic step if no agents matched
        if not steps:
            steps.append(PlannedStep(
                id="step_1",
                name="Manual Review Required",
                description="Could not automatically plan this request",
                agent_type="manual",
                capability="review",
                reasoning="No agents matched the user request",
            ))

        return WorkflowPlan(
            id=str(uuid.uuid4())[:8],
            name="Auto-Generated Workflow",
            description=f"Workflow for: {user_request[:100]}",
            user_request=user_request,
            steps=steps,
            confidence_score=0.5,  # Lower confidence for fallback
            warnings=["Generated without AI assistance - review recommended"],
        )

    def analyze_request(
        self,
        user_request: str,
    ) -> dict[str, Any]:
        """
        Analyze a user request without creating a full plan.

        Args:
            user_request: Natural language request

        Returns:
            Analysis including intent, entities, and suggested agents
        """
        if not self._client:
            return self._analyze_fallback(user_request)

        response = self._client.messages.create(
            model=self._config.model,
            max_tokens=1024,
            temperature=0.2,
            system="""Analyze the user's data engineering request and extract:
1. Primary intent (what they want to achieve)
2. Key entities (tables, schemas, files mentioned)
3. Constraints (time, quality, format requirements)
4. Ambiguities (unclear aspects that need clarification)

Respond in JSON format.""",
            messages=[{"role": "user", "content": user_request}],
        )

        try:
            text = response.content[0].text
            json_start = text.find("{")
            json_end = text.rfind("}") + 1
            return json.loads(text[json_start:json_end])
        except Exception:
            return self._analyze_fallback(user_request)

    def _analyze_fallback(self, user_request: str) -> dict[str, Any]:
        """Basic analysis without Claude."""
        return {
            "intent": "unknown",
            "entities": [],
            "constraints": [],
            "ambiguities": ["Full analysis requires Claude API"],
            "suggested_agents": [
                a.name for a in self._available_agents
                if any(word in user_request.lower() for word in a.description.lower().split()[:5])
            ],
        }

    def suggest_agents(
        self,
        user_request: str,
    ) -> list[dict[str, Any]]:
        """
        Suggest which agents could handle a request.

        Args:
            user_request: Natural language request

        Returns:
            List of agent suggestions with relevance scores
        """
        analysis = self.analyze_request(user_request)

        suggestions = []
        for agent in self._available_agents:
            # Calculate relevance based on capability match
            relevance = 0.0
            matched_capabilities = []

            for cap in agent.capabilities:
                if cap.lower() in user_request.lower():
                    relevance += 0.3
                    matched_capabilities.append(cap)

            # Check description keywords
            desc_words = agent.description.lower().split()
            request_words = user_request.lower().split()
            word_matches = sum(1 for w in desc_words if w in request_words)
            relevance += min(word_matches * 0.1, 0.5)

            if relevance > 0:
                suggestions.append({
                    "agent_name": agent.name,
                    "agent_type": agent.agent_type,
                    "relevance_score": min(relevance, 1.0),
                    "matched_capabilities": matched_capabilities,
                    "description": agent.description,
                })

        # Sort by relevance
        suggestions.sort(key=lambda x: x["relevance_score"], reverse=True)

        return suggestions

    def optimize_plan(
        self,
        plan: WorkflowPlan,
    ) -> WorkflowPlan:
        """
        Optimize an existing plan for better performance.

        Args:
            plan: Existing workflow plan

        Returns:
            Optimized plan
        """
        if not self._config.enable_parallel_steps:
            return plan

        # Identify steps that can run in parallel
        # (steps with no dependencies on each other)
        optimized_steps = list(plan.steps)

        # Group steps by their dependency level
        levels: dict[int, list[PlannedStep]] = {}
        step_levels: dict[str, int] = {}

        for step in optimized_steps:
            if not step.depends_on:
                step_levels[step.id] = 0
            else:
                max_dep_level = max(
                    step_levels.get(dep, 0) for dep in step.depends_on
                )
                step_levels[step.id] = max_dep_level + 1

            level = step_levels[step.id]
            if level not in levels:
                levels[level] = []
            levels[level].append(step)

        # Add parallelism hints
        for level, steps_at_level in levels.items():
            if len(steps_at_level) > 1:
                for step in steps_at_level:
                    step.config["parallel_group"] = f"level_{level}"

        return WorkflowPlan(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            user_request=plan.user_request,
            steps=optimized_steps,
            estimated_duration=plan.estimated_duration,
            confidence_score=plan.confidence_score,
            warnings=plan.warnings + [f"Optimized: {len(levels)} parallel levels identified"],
            alternatives=plan.alternatives,
            created_at=plan.created_at,
        )

    def explain_plan(
        self,
        plan: WorkflowPlan,
    ) -> str:
        """
        Generate a human-readable explanation of a plan.

        Args:
            plan: Workflow plan to explain

        Returns:
            Markdown explanation
        """
        explanation = f"""# Workflow Plan: {plan.name}

## Overview
{plan.description}

**Confidence Score:** {plan.confidence_score:.0%}
**Estimated Duration:** {plan.estimated_duration}
**Total Steps:** {len(plan.steps)}

## User Request
> {plan.user_request}

## Execution Steps

"""

        for i, step in enumerate(plan.steps, 1):
            deps = ", ".join(step.depends_on) if step.depends_on else "None (can start immediately)"

            explanation += f"""### Step {i}: {step.name}

- **Agent:** {step.agent_type}
- **Capability:** {step.capability}
- **Dependencies:** {deps}
- **Output Key:** {step.output_key}

{step.description}

"""
            if step.reasoning:
                explanation += f"**Reasoning:** {step.reasoning}\n\n"

        if plan.warnings:
            explanation += "## Warnings\n\n"
            for warning in plan.warnings:
                explanation += f"- {warning}\n"
            explanation += "\n"

        if plan.alternatives:
            explanation += "## Alternative Approaches\n\n"
            for alt in plan.alternatives:
                explanation += f"- {alt}\n"

        return explanation

    def to_workflow_definition(
        self,
        plan: WorkflowPlan,
    ) -> dict[str, Any]:
        """
        Convert a plan to a workflow definition for the Orchestrator.

        Args:
            plan: Workflow plan

        Returns:
            Workflow definition dict compatible with Orchestrator.create_workflow()
        """
        return {
            "name": plan.name,
            "description": plan.description,
            "steps": [
                {
                    "name": step.name,
                    "agent": step.agent_type,
                    "capability": step.capability,
                    "input_mapping": step.input_mapping,
                    "output_key": step.output_key,
                    "depends_on": step.depends_on,
                    "config": step.config,
                }
                for step in plan.steps
            ],
        }

    def get_status(self) -> dict[str, Any]:
        """Get planner status."""
        return {
            "id": self._id,
            "state": self._state.value,
            "available_agents": len(self._available_agents),
            "plans_created": len(self._plans),
            "claude_available": self._client is not None,
            "model": self._config.model,
            "created_at": self._created_at.isoformat(),
        }

    def get_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get planning history."""
        return [p.to_dict() for p in self._plans[-limit:]]


# Convenience function for quick planning
def plan_workflow(
    request: str,
    context: Optional[dict[str, Any]] = None,
    api_key: Optional[str] = None,
) -> WorkflowPlan:
    """
    Quick function to plan a workflow.

    Args:
        request: User request
        context: Optional context
        api_key: Optional Anthropic API key

    Returns:
        WorkflowPlan
    """
    config = PlannerConfig(api_key=api_key) if api_key else None
    planner = PlannerAgent(config=config)
    return planner.plan_workflow(request, context)
