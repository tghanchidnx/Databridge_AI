"""Tests for the PlannerAgent."""
import pytest
import json
from src.agents.planner_agent import (
    PlannerAgent,
    PlannerConfig,
    PlannerState,
    AgentInfo,
    PlannedStep,
    WorkflowPlan,
    DEFAULT_AGENTS,
)


class TestPlannerAgent:
    """Tests for PlannerAgent class."""

    @pytest.fixture
    def planner(self):
        """Create a PlannerAgent without Claude API."""
        config = PlannerConfig(api_key=None)  # Force fallback mode
        return PlannerAgent(config=config)

    def test_init(self, planner):
        """Test planner initialization."""
        assert planner.state == PlannerState.IDLE
        assert len(planner.available_agents) == len(DEFAULT_AGENTS)

    def test_register_agent(self, planner):
        """Test registering a custom agent."""
        custom_agent = AgentInfo(
            name="custom_agent",
            agent_type="CustomAgent",
            description="A custom test agent",
            capabilities=["custom_capability"],
        )
        planner.register_agent(custom_agent)

        assert any(a.name == "custom_agent" for a in planner.available_agents)

    def test_plan_workflow_fallback(self, planner):
        """Test workflow planning in fallback mode."""
        plan = planner.plan_workflow(
            "Extract hierarchies from SQL CASE statements"
        )

        assert isinstance(plan, WorkflowPlan)
        assert plan.user_request == "Extract hierarchies from SQL CASE statements"
        assert len(plan.steps) > 0
        assert plan.confidence_score == 0.5  # Fallback confidence

    def test_plan_workflow_with_context(self, planner):
        """Test planning with additional context."""
        plan = planner.plan_workflow(
            "Scan the database schema",
            context={"schema": "FINANCE", "database": "WAREHOUSE"},
        )

        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) > 0

    def test_suggest_agents(self, planner):
        """Test agent suggestion."""
        suggestions = planner.suggest_agents(
            "I want to parse SQL and extract CASE statements"
        )

        assert isinstance(suggestions, list)
        # Should suggest logic_extractor
        agent_names = [s["agent_name"] for s in suggestions]
        assert "logic_extractor" in agent_names

    def test_suggest_agents_schema(self, planner):
        """Test agent suggestion for schema scanning."""
        suggestions = planner.suggest_agents(
            "Scan the database schema and profile data quality"
        )

        assert isinstance(suggestions, list)
        agent_names = [s["agent_name"] for s in suggestions]
        assert "schema_scanner" in agent_names

    def test_analyze_request_fallback(self, planner):
        """Test request analysis in fallback mode."""
        analysis = planner.analyze_request(
            "Build a star schema for the sales data"
        )

        assert isinstance(analysis, dict)
        assert "ambiguities" in analysis

    def test_optimize_plan(self, planner):
        """Test plan optimization."""
        # Create a plan with some steps
        plan = WorkflowPlan(
            id="test",
            name="Test Workflow",
            description="Test",
            user_request="test",
            steps=[
                PlannedStep(
                    id="step_1",
                    name="Step 1",
                    description="First step",
                    agent_type="schema_scanner",
                    capability="scan_schema",
                    depends_on=[],
                ),
                PlannedStep(
                    id="step_2",
                    name="Step 2",
                    description="Second step",
                    agent_type="logic_extractor",
                    capability="parse_sql",
                    depends_on=[],  # No dependency - can run in parallel
                ),
                PlannedStep(
                    id="step_3",
                    name="Step 3",
                    description="Third step",
                    agent_type="warehouse_architect",
                    capability="design_star_schema",
                    depends_on=["step_1", "step_2"],
                ),
            ],
        )

        optimized = planner.optimize_plan(plan)

        # Steps 1 and 2 should be in the same parallel group (both at level 0)
        assert optimized.steps[0].config.get("parallel_group") == "level_0"
        assert optimized.steps[1].config.get("parallel_group") == "level_0"
        # Step 3 is alone at level 1, so no parallel_group added (only added when > 1 step at level)
        # The optimization note should mention parallel levels
        assert any("parallel" in w.lower() for w in optimized.warnings)

    def test_explain_plan(self, planner):
        """Test plan explanation."""
        plan = WorkflowPlan(
            id="test",
            name="Test Workflow",
            description="A test workflow",
            user_request="Test request",
            steps=[
                PlannedStep(
                    id="step_1",
                    name="Scan Schema",
                    description="Scan the database schema",
                    agent_type="schema_scanner",
                    capability="scan_schema",
                    reasoning="Need to understand the data structure first",
                ),
            ],
            confidence_score=0.95,
            estimated_duration="5 minutes",
        )

        explanation = planner.explain_plan(plan)

        assert "# Workflow Plan: Test Workflow" in explanation
        assert "Scan Schema" in explanation
        assert "schema_scanner" in explanation
        assert "95%" in explanation

    def test_to_workflow_definition(self, planner):
        """Test conversion to orchestrator format."""
        plan = WorkflowPlan(
            id="test",
            name="Test Workflow",
            description="Test",
            user_request="test",
            steps=[
                PlannedStep(
                    id="step_1",
                    name="Step 1",
                    description="First step",
                    agent_type="schema_scanner",
                    capability="scan_schema",
                    input_mapping={"schema": "input.schema"},
                    output_key="scan_result",
                ),
            ],
        )

        workflow_def = planner.to_workflow_definition(plan)

        assert workflow_def["name"] == "Test Workflow"
        assert len(workflow_def["steps"]) == 1
        assert workflow_def["steps"][0]["agent"] == "schema_scanner"
        assert workflow_def["steps"][0]["capability"] == "scan_schema"

    def test_get_status(self, planner):
        """Test planner status."""
        status = planner.get_status()

        assert "id" in status
        assert status["state"] == "idle"
        assert status["available_agents"] == len(DEFAULT_AGENTS)
        assert status["claude_available"] is False  # No API key

    def test_get_history(self, planner):
        """Test planning history."""
        # Create a plan
        planner.plan_workflow("Test request")

        history = planner.get_history()

        assert len(history) == 1
        assert history[0]["user_request"] == "Test request"


class TestPlannedStep:
    """Tests for PlannedStep dataclass."""

    def test_to_dict(self):
        """Test step serialization."""
        step = PlannedStep(
            id="step_1",
            name="Test Step",
            description="A test step",
            agent_type="test_agent",
            capability="test_capability",
            input_mapping={"a": "b"},
            output_key="output",
            depends_on=["step_0"],
            reasoning="For testing",
        )

        data = step.to_dict()

        assert data["id"] == "step_1"
        assert data["name"] == "Test Step"
        assert data["agent_type"] == "test_agent"
        assert data["depends_on"] == ["step_0"]


class TestWorkflowPlan:
    """Tests for WorkflowPlan dataclass."""

    def test_to_dict(self):
        """Test plan serialization."""
        plan = WorkflowPlan(
            id="plan_1",
            name="Test Plan",
            description="A test plan",
            user_request="Do something",
            steps=[],
            confidence_score=0.8,
            warnings=["Warning 1"],
        )

        data = plan.to_dict()

        assert data["id"] == "plan_1"
        assert data["name"] == "Test Plan"
        assert data["step_count"] == 0
        assert data["confidence_score"] == 0.8
        assert data["warnings"] == ["Warning 1"]


class TestDefaultAgents:
    """Tests for default agent definitions."""

    def test_default_agents_exist(self):
        """Test that default agents are defined."""
        assert len(DEFAULT_AGENTS) >= 6

    def test_default_agent_structure(self):
        """Test default agent structure."""
        for agent in DEFAULT_AGENTS:
            assert agent.name
            assert agent.agent_type
            assert agent.description
            assert len(agent.capabilities) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
