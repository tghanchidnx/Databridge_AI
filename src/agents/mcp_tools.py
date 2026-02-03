"""
MCP Tools for the PlannerAgent.

Exposes AI-powered workflow planning capabilities through the MCP protocol.
"""

import json
import logging
from typing import Any, Optional

from .planner_agent import (
    PlannerAgent,
    PlannerConfig,
    AgentInfo,
    WorkflowPlan,
)

logger = logging.getLogger(__name__)

# Singleton planner instance
_planner: Optional[PlannerAgent] = None


def get_planner() -> PlannerAgent:
    """Get or create the singleton PlannerAgent."""
    global _planner
    if _planner is None:
        _planner = PlannerAgent()
    return _planner


def reset_planner(config: Optional[PlannerConfig] = None) -> PlannerAgent:
    """Reset the planner with new configuration."""
    global _planner
    _planner = PlannerAgent(config=config)
    return _planner


def register_planner_tools(mcp: Any) -> None:
    """
    Register all planner MCP tools.

    Args:
        mcp: The FastMCP instance
    """

    @mcp.tool()
    def plan_workflow(
        request: str,
        context: str = "{}",
    ) -> str:
        """
        Create a workflow plan from a natural language request using AI.

        The PlannerAgent uses Claude to analyze the request and generate
        an optimal sequence of steps using available agents.

        Args:
            request: Natural language description of what you want to accomplish.
                     Examples:
                     - "Extract hierarchies from SQL CASE statements and deploy to Snowflake"
                     - "Scan the FINANCE schema and design a star schema for reporting"
                     - "Reconcile data between the staging and production tables"
            context: Optional JSON context with additional information like:
                     {"schema": "FINANCE", "database": "WAREHOUSE", "constraints": [...]}

        Returns:
            JSON workflow plan with steps, agents, and execution order
        """
        planner = get_planner()

        try:
            ctx = json.loads(context) if context else None
        except json.JSONDecodeError:
            ctx = None

        plan = planner.plan_workflow(request, ctx)
        return json.dumps(plan.to_dict(), indent=2)

    @mcp.tool()
    def analyze_request(
        request: str,
    ) -> str:
        """
        Analyze a user request to understand intent and requirements.

        Uses AI to extract:
        - Primary intent (what the user wants to achieve)
        - Key entities (tables, schemas, files mentioned)
        - Constraints (time, quality, format requirements)
        - Ambiguities (unclear aspects needing clarification)

        Args:
            request: Natural language description of the task

        Returns:
            JSON analysis of the request
        """
        planner = get_planner()
        analysis = planner.analyze_request(request)
        return json.dumps(analysis, indent=2)

    @mcp.tool()
    def suggest_agents(
        request: str,
    ) -> str:
        """
        Suggest which agents could handle a specific request.

        Analyzes the request and returns a ranked list of agents
        with relevance scores and matched capabilities.

        Args:
            request: Natural language description of the task

        Returns:
            JSON list of agent suggestions with relevance scores
        """
        planner = get_planner()
        suggestions = planner.suggest_agents(request)
        return json.dumps(suggestions, indent=2)

    @mcp.tool()
    def explain_plan(
        plan_json: str,
    ) -> str:
        """
        Generate a human-readable explanation of a workflow plan.

        Converts a JSON plan into a detailed markdown explanation
        that describes each step, dependencies, and reasoning.

        Args:
            plan_json: JSON string of a workflow plan (from plan_workflow)

        Returns:
            Markdown explanation of the plan
        """
        planner = get_planner()

        data = json.loads(plan_json)

        # Reconstruct the plan
        from .planner_agent import PlannedStep

        steps = [
            PlannedStep(
                id=s.get("id", f"step_{i}"),
                name=s.get("name", ""),
                description=s.get("description", ""),
                agent_type=s.get("agent_type", ""),
                capability=s.get("capability", ""),
                input_mapping=s.get("input_mapping", {}),
                output_key=s.get("output_key", ""),
                depends_on=s.get("depends_on", []),
                config=s.get("config", {}),
                reasoning=s.get("reasoning", ""),
            )
            for i, s in enumerate(data.get("steps", []))
        ]

        plan = WorkflowPlan(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            user_request=data.get("user_request", ""),
            steps=steps,
            estimated_duration=data.get("estimated_duration", ""),
            confidence_score=data.get("confidence_score", 0.0),
            warnings=data.get("warnings", []),
            alternatives=data.get("alternatives", []),
        )

        return planner.explain_plan(plan)

    @mcp.tool()
    def optimize_plan(
        plan_json: str,
    ) -> str:
        """
        Optimize a workflow plan for better performance.

        Analyzes step dependencies and identifies opportunities
        for parallel execution. Adds parallel_group hints to steps
        that can run concurrently.

        Args:
            plan_json: JSON string of a workflow plan (from plan_workflow)

        Returns:
            JSON optimized plan with parallelism hints
        """
        planner = get_planner()

        data = json.loads(plan_json)

        from .planner_agent import PlannedStep

        steps = [
            PlannedStep(
                id=s.get("id", f"step_{i}"),
                name=s.get("name", ""),
                description=s.get("description", ""),
                agent_type=s.get("agent_type", ""),
                capability=s.get("capability", ""),
                input_mapping=s.get("input_mapping", {}),
                output_key=s.get("output_key", ""),
                depends_on=s.get("depends_on", []),
                config=s.get("config", {}),
                reasoning=s.get("reasoning", ""),
            )
            for i, s in enumerate(data.get("steps", []))
        ]

        plan = WorkflowPlan(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            user_request=data.get("user_request", ""),
            steps=steps,
            estimated_duration=data.get("estimated_duration", ""),
            confidence_score=data.get("confidence_score", 0.0),
            warnings=data.get("warnings", []),
            alternatives=data.get("alternatives", []),
        )

        optimized = planner.optimize_plan(plan)
        return json.dumps(optimized.to_dict(), indent=2)

    @mcp.tool()
    def get_workflow_definition(
        plan_json: str,
    ) -> str:
        """
        Convert a plan to a workflow definition for the Orchestrator.

        Transforms the AI-generated plan into a format compatible
        with the Orchestrator.create_workflow() method.

        Args:
            plan_json: JSON string of a workflow plan (from plan_workflow)

        Returns:
            JSON workflow definition for the Orchestrator
        """
        planner = get_planner()

        data = json.loads(plan_json)

        from .planner_agent import PlannedStep

        steps = [
            PlannedStep(
                id=s.get("id", f"step_{i}"),
                name=s.get("name", ""),
                description=s.get("description", ""),
                agent_type=s.get("agent_type", ""),
                capability=s.get("capability", ""),
                input_mapping=s.get("input_mapping", {}),
                output_key=s.get("output_key", ""),
                depends_on=s.get("depends_on", []),
                config=s.get("config", {}),
                reasoning=s.get("reasoning", ""),
            )
            for i, s in enumerate(data.get("steps", []))
        ]

        plan = WorkflowPlan(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            user_request=data.get("user_request", ""),
            steps=steps,
        )

        workflow_def = planner.to_workflow_definition(plan)
        return json.dumps(workflow_def, indent=2)

    @mcp.tool()
    def list_available_agents() -> str:
        """
        List all agents available for workflow planning.

        Returns information about each agent including:
        - Name and type
        - Description
        - Available capabilities
        - Input/output schemas

        Returns:
            JSON list of available agents
        """
        planner = get_planner()
        agents = [a.to_dict() for a in planner.available_agents]
        return json.dumps(agents, indent=2)

    @mcp.tool()
    def register_custom_agent(
        name: str,
        agent_type: str,
        description: str,
        capabilities: str,
        input_schema: str = "{}",
        output_schema: str = "{}",
    ) -> str:
        """
        Register a custom agent for workflow planning.

        Adds a new agent to the planner's registry so it can be
        included in generated workflow plans.

        Args:
            name: Unique agent name (e.g., "my_custom_agent")
            agent_type: Agent class name (e.g., "CustomProcessor")
            description: What the agent does
            capabilities: JSON array of capability names
            input_schema: JSON schema for agent inputs
            output_schema: JSON schema for agent outputs

        Returns:
            Confirmation message
        """
        planner = get_planner()

        agent = AgentInfo(
            name=name,
            agent_type=agent_type,
            description=description,
            capabilities=json.loads(capabilities),
            input_schema=json.loads(input_schema),
            output_schema=json.loads(output_schema),
        )

        planner.register_agent(agent)

        return json.dumps({
            "status": "success",
            "message": f"Agent '{name}' registered successfully",
            "agent": agent.to_dict(),
        }, indent=2)

    @mcp.tool()
    def get_planner_status() -> str:
        """
        Get the current status of the PlannerAgent.

        Returns:
            JSON status including state, available agents, and history count
        """
        planner = get_planner()
        return json.dumps(planner.get_status(), indent=2)

    @mcp.tool()
    def get_planning_history(
        limit: int = 10,
    ) -> str:
        """
        Get the history of recent workflow plans.

        Args:
            limit: Maximum number of plans to return (default 10)

        Returns:
            JSON list of recent plans
        """
        planner = get_planner()
        history = planner.get_history(limit)
        return json.dumps(history, indent=2)

    @mcp.tool()
    def configure_planner(
        model: str = "",
        temperature: float = -1,
        max_steps: int = -1,
        enable_parallel: str = "",
    ) -> str:
        """
        Configure the PlannerAgent settings.

        Args:
            model: Claude model to use (e.g., "claude-sonnet-4-20250514")
            temperature: Sampling temperature (0.0-1.0, lower = more focused)
            max_steps: Maximum steps allowed in a plan
            enable_parallel: "true" or "false" to enable parallel optimization

        Returns:
            Updated configuration
        """
        planner = get_planner()

        if model:
            planner._config.model = model
        if temperature >= 0:
            planner._config.temperature = temperature
        if max_steps > 0:
            planner._config.max_steps = max_steps
        if enable_parallel:
            planner._config.enable_parallel_steps = enable_parallel.lower() == "true"

        return json.dumps({
            "status": "success",
            "config": {
                "model": planner._config.model,
                "temperature": planner._config.temperature,
                "max_steps": planner._config.max_steps,
                "enable_parallel_steps": planner._config.enable_parallel_steps,
            },
        }, indent=2)

    logger.info("Registered 11 PlannerAgent MCP tools")
