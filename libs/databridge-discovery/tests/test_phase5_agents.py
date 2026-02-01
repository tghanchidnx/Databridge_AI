"""
Unit tests for Phase 5: Multi-Agent Orchestration.

Tests cover:
- Base agent interface
- Schema Scanner agent
- Logic Extractor agent
- Warehouse Architect agent
- Deploy Validator agent
- Orchestrator
- Workflows
- MCP tools
"""

import pytest
from datetime import datetime
from typing import Any


# =============================================================================
# Base Agent Tests
# =============================================================================


class TestBaseAgent:
    """Tests for base agent interface."""

    def test_agent_capability_enum(self):
        """Test AgentCapability enum values."""
        from databridge_discovery.agents.base_agent import AgentCapability

        # Schema Scanner capabilities
        assert AgentCapability.SCAN_SCHEMA.value == "scan_schema"
        assert AgentCapability.EXTRACT_METADATA.value == "extract_metadata"
        assert AgentCapability.DETECT_KEYS.value == "detect_keys"
        assert AgentCapability.SAMPLE_PROFILES.value == "sample_profiles"

        # Logic Extractor capabilities
        assert AgentCapability.PARSE_SQL.value == "parse_sql"
        assert AgentCapability.EXTRACT_CASE.value == "extract_case"
        assert AgentCapability.IDENTIFY_CALCS.value == "identify_calcs"
        assert AgentCapability.DETECT_AGGREGATIONS.value == "detect_aggregations"

    def test_agent_state_enum(self):
        """Test AgentState enum values."""
        from databridge_discovery.agents.base_agent import AgentState

        assert AgentState.IDLE.value == "idle"
        assert AgentState.RUNNING.value == "running"
        assert AgentState.COMPLETED.value == "completed"
        assert AgentState.FAILED.value == "failed"

    def test_agent_config(self):
        """Test AgentConfig dataclass."""
        from databridge_discovery.agents.base_agent import AgentConfig

        config = AgentConfig(
            name="test_agent",
            timeout_seconds=120,
            max_retries=5,
        )

        assert config.name == "test_agent"
        assert config.timeout_seconds == 120
        assert config.max_retries == 5

    def test_task_context(self):
        """Test TaskContext dataclass."""
        from databridge_discovery.agents.base_agent import TaskContext

        context = TaskContext(
            task_id="task_123",
            workflow_id="wf_456",
            input_data={"key": "value"},
        )

        assert context.task_id == "task_123"
        assert context.workflow_id == "wf_456"
        assert context.input_data == {"key": "value"}

    def test_agent_result(self):
        """Test AgentResult dataclass."""
        from databridge_discovery.agents.base_agent import AgentResult

        result = AgentResult(
            agent_id="agent_1",
            agent_name="TestAgent",
            capability="test_cap",
            success=True,
            data={"result": "data"},
            duration_seconds=0.1005,
        )

        assert result.agent_id == "agent_1"
        assert result.success is True
        assert result.data == {"result": "data"}
        assert result.duration_seconds == 0.1005

        # Test to_dict
        d = result.to_dict()
        assert d["agent_name"] == "TestAgent"
        assert d["success"] is True


# =============================================================================
# Schema Scanner Tests
# =============================================================================


class TestSchemaScanner:
    """Tests for Schema Scanner agent."""

    @pytest.fixture
    def scanner(self):
        """Create Schema Scanner instance."""
        from databridge_discovery.agents.schema_scanner import SchemaScanner

        return SchemaScanner()

    @pytest.fixture
    def sample_tables(self):
        """Sample table metadata for testing."""
        return [
            {
                "name": "orders",
                "schema": "public",
                "columns": [
                    {"name": "order_id", "type": "INTEGER", "nullable": False},
                    {"name": "customer_id", "type": "INTEGER", "nullable": False},
                    {"name": "order_date", "type": "DATE", "nullable": False},
                    {"name": "total_amount", "type": "DECIMAL", "nullable": True},
                ],
            },
            {
                "name": "customers",
                "schema": "public",
                "columns": [
                    {"name": "customer_id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR", "nullable": False},
                    {"name": "email", "type": "VARCHAR", "nullable": True},
                ],
            },
        ]

    def test_get_capabilities(self, scanner):
        """Test scanner capabilities."""
        from databridge_discovery.agents.base_agent import AgentCapability

        caps = scanner.get_capabilities()

        assert AgentCapability.SCAN_SCHEMA in caps
        assert AgentCapability.EXTRACT_METADATA in caps
        assert AgentCapability.DETECT_KEYS in caps
        assert AgentCapability.SAMPLE_PROFILES in caps

    def test_supports_capability(self, scanner):
        """Test capability support check."""
        from databridge_discovery.agents.base_agent import AgentCapability

        assert scanner.supports(AgentCapability.SCAN_SCHEMA) is True
        assert scanner.supports(AgentCapability.DETECT_KEYS) is True
        assert scanner.supports(AgentCapability.DESIGN_STAR_SCHEMA) is False

    def test_scan_schema(self, scanner, sample_tables):
        """Test schema scanning."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        context = TaskContext(
            task_id="test_scan",
            input_data={"tables": sample_tables},
        )

        result = scanner.execute(AgentCapability.SCAN_SCHEMA, context)

        assert result.success is True
        assert "tables" in result.data
        assert len(result.data["tables"]) == 2

    def test_detect_keys(self, scanner, sample_tables):
        """Test key detection."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        context = TaskContext(
            task_id="test_keys",
            input_data={"tables": sample_tables},
        )

        result = scanner.execute(AgentCapability.DETECT_KEYS, context)

        assert result.success is True
        assert "primary_keys" in result.data
        assert "foreign_keys" in result.data

    def test_sample_profiles(self, scanner, sample_tables):
        """Test data profiling."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        # Add sample data
        tables_with_data = sample_tables.copy()
        tables_with_data[0]["data"] = [
            {"order_id": 1, "customer_id": 100, "order_date": "2024-01-01", "total_amount": 99.99},
            {"order_id": 2, "customer_id": 101, "order_date": "2024-01-02", "total_amount": 149.99},
        ]

        context = TaskContext(
            task_id="test_profile",
            input_data={"tables": tables_with_data},
        )

        result = scanner.execute(AgentCapability.SAMPLE_PROFILES, context)

        assert result.success is True
        assert "profiles" in result.data


# =============================================================================
# Logic Extractor Tests
# =============================================================================


class TestLogicExtractor:
    """Tests for Logic Extractor agent."""

    @pytest.fixture
    def extractor(self):
        """Create Logic Extractor instance."""
        from databridge_discovery.agents.logic_extractor import LogicExtractor

        return LogicExtractor()

    @pytest.fixture
    def sample_sql(self):
        """Sample SQL with CASE statement."""
        return """
        SELECT
            account_code,
            CASE
                WHEN account_code BETWEEN '4000' AND '4999' THEN 'Revenue'
                WHEN account_code BETWEEN '5000' AND '5999' THEN 'COGS'
                WHEN account_code BETWEEN '6000' AND '6999' THEN 'Operating Expenses'
                ELSE 'Other'
            END AS account_category,
            SUM(amount) AS total_amount
        FROM general_ledger
        GROUP BY account_code, account_category
        """

    def test_get_capabilities(self, extractor):
        """Test extractor capabilities."""
        from databridge_discovery.agents.base_agent import AgentCapability

        caps = extractor.get_capabilities()

        assert AgentCapability.PARSE_SQL in caps
        assert AgentCapability.EXTRACT_CASE in caps
        assert AgentCapability.IDENTIFY_CALCS in caps
        assert AgentCapability.DETECT_AGGREGATIONS in caps

    def test_parse_sql(self, extractor, sample_sql):
        """Test SQL parsing."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        context = TaskContext(
            task_id="test_parse",
            input_data={"sql": sample_sql},
        )

        result = extractor.execute(AgentCapability.PARSE_SQL, context)

        assert result.success is True
        assert "query_type" in result.data
        assert result.data["query_type"] == "SELECT"

    def test_extract_case(self, extractor, sample_sql):
        """Test CASE statement extraction."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        context = TaskContext(
            task_id="test_case",
            input_data={"sql": sample_sql},
        )

        result = extractor.execute(AgentCapability.EXTRACT_CASE, context)

        assert result.success is True
        assert "case_statements" in result.data
        assert len(result.data["case_statements"]) > 0

    def test_detect_aggregations(self, extractor, sample_sql):
        """Test aggregation detection."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        context = TaskContext(
            task_id="test_agg",
            input_data={"sql": sample_sql},
        )

        result = extractor.execute(AgentCapability.DETECT_AGGREGATIONS, context)

        assert result.success is True
        assert "aggregations" in result.data


# =============================================================================
# Warehouse Architect Tests
# =============================================================================


class TestWarehouseArchitect:
    """Tests for Warehouse Architect agent."""

    @pytest.fixture
    def architect(self):
        """Create Warehouse Architect instance."""
        from databridge_discovery.agents.warehouse_architect import WarehouseArchitect

        return WarehouseArchitect()

    @pytest.fixture
    def sample_tables(self):
        """Sample table metadata for star schema design."""
        return [
            {
                "name": "orders",
                "table_type": "fact",
                "columns": [
                    {"name": "order_id", "type": "INTEGER", "is_key": True},
                    {"name": "customer_id", "type": "INTEGER", "is_foreign_key": True},
                    {"name": "product_id", "type": "INTEGER", "is_foreign_key": True},
                    {"name": "quantity", "type": "INTEGER", "is_measure": True},
                    {"name": "amount", "type": "DECIMAL", "is_measure": True},
                ],
            },
            {
                "name": "customers",
                "table_type": "dimension",
                "columns": [
                    {"name": "customer_id", "type": "INTEGER", "is_key": True},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "region", "type": "VARCHAR"},
                ],
            },
        ]

    def test_get_capabilities(self, architect):
        """Test architect capabilities."""
        from databridge_discovery.agents.base_agent import AgentCapability

        caps = architect.get_capabilities()

        assert AgentCapability.DESIGN_STAR_SCHEMA in caps
        assert AgentCapability.GENERATE_DIMS in caps
        assert AgentCapability.GENERATE_FACTS in caps
        assert AgentCapability.DBT_MODELS in caps

    def test_design_star_schema(self, architect, sample_tables):
        """Test star schema design."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        context = TaskContext(
            task_id="test_design",
            input_data={"tables": sample_tables},
        )

        result = architect.execute(AgentCapability.DESIGN_STAR_SCHEMA, context)

        assert result.success is True
        assert "design" in result.data

    def test_generate_dims(self, architect, sample_tables):
        """Test dimension generation."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        # Create design first
        design_context = TaskContext(
            task_id="test_design",
            input_data={"tables": sample_tables},
        )
        design_result = architect.execute(AgentCapability.DESIGN_STAR_SCHEMA, design_context)

        # Generate dims
        context = TaskContext(
            task_id="test_dims",
            input_data={"design": design_result.data.get("design")},
        )

        result = architect.execute(AgentCapability.GENERATE_DIMS, context)

        assert result.success is True
        assert "dimensions" in result.data


# =============================================================================
# Deploy Validator Tests
# =============================================================================


class TestDeployValidator:
    """Tests for Deploy Validator agent."""

    @pytest.fixture
    def validator(self):
        """Create Deploy Validator instance."""
        from databridge_discovery.agents.deploy_validator import DeployValidator

        return DeployValidator()

    def test_get_capabilities(self, validator):
        """Test validator capabilities."""
        from databridge_discovery.agents.base_agent import AgentCapability

        caps = validator.get_capabilities()

        assert AgentCapability.EXECUTE_DDL in caps
        assert AgentCapability.RUN_DBT in caps
        assert AgentCapability.VALIDATE_COUNTS in caps
        assert AgentCapability.COMPARE_AGGREGATES in caps

    def test_execute_ddl_dry_run(self, validator):
        """Test DDL execution in dry run mode."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        ddl = [
            "CREATE TABLE test_table (id INT PRIMARY KEY);",
            "CREATE VIEW test_view AS SELECT * FROM test_table;",
        ]

        context = TaskContext(
            task_id="test_ddl",
            input_data={"ddl_statements": ddl, "dry_run": True},
        )

        result = validator.execute(AgentCapability.EXECUTE_DDL, context)

        assert result.success is True
        assert result.data["dry_run"] is True
        assert result.data["success_count"] == 2

    def test_validate_counts(self, validator):
        """Test count validation."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        validations = [
            {
                "source_table": "source.orders",
                "target_table": "target.orders",
                "source_count": 1000,
                "target_count": 998,
                "tolerance_percent": 1,
            },
        ]

        context = TaskContext(
            task_id="test_counts",
            input_data={"validations": validations},
        )

        result = validator.execute(AgentCapability.VALIDATE_COUNTS, context)

        assert result.success is True
        assert result.data["passed"] == 1

    def test_compare_aggregates(self, validator):
        """Test aggregate comparison."""
        from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

        comparisons = [
            {
                "name": "revenue_total",
                "source_value": 1000000.00,
                "target_value": 1000000.00,
                "aggregation": "SUM",
                "tolerance_percent": 0.01,
            },
        ]

        context = TaskContext(
            task_id="test_aggs",
            input_data={"comparisons": comparisons},
        )

        result = validator.execute(AgentCapability.COMPARE_AGGREGATES, context)

        assert result.success is True
        assert result.data["passed"] == 1


# =============================================================================
# Orchestrator Tests
# =============================================================================


class TestOrchestrator:
    """Tests for Orchestrator."""

    @pytest.fixture
    def orchestrator(self):
        """Create Orchestrator instance."""
        from databridge_discovery.agents.orchestrator import Orchestrator

        return Orchestrator()

    @pytest.fixture
    def orchestrator_with_agents(self):
        """Create Orchestrator with registered agents."""
        from databridge_discovery.agents.orchestrator import Orchestrator
        from databridge_discovery.agents.schema_scanner import SchemaScanner
        from databridge_discovery.agents.logic_extractor import LogicExtractor

        orch = Orchestrator()
        orch.register_agent("scanner", SchemaScanner())
        orch.register_agent("extractor", LogicExtractor())
        return orch

    def test_orchestrator_init(self, orchestrator):
        """Test orchestrator initialization."""
        assert orchestrator.id is not None
        assert len(orchestrator.id) == 8

    def test_register_agent(self, orchestrator):
        """Test agent registration."""
        from databridge_discovery.agents.schema_scanner import SchemaScanner

        scanner = SchemaScanner()
        orchestrator.register_agent("scanner", scanner)

        assert orchestrator.get_agent("scanner") is scanner

    def test_register_duplicate_agent(self, orchestrator):
        """Test duplicate agent registration fails."""
        from databridge_discovery.agents.schema_scanner import SchemaScanner

        scanner1 = SchemaScanner()
        scanner2 = SchemaScanner()

        orchestrator.register_agent("scanner", scanner1)

        with pytest.raises(ValueError, match="already registered"):
            orchestrator.register_agent("scanner", scanner2)

    def test_register_replace_agent(self, orchestrator):
        """Test replacing registered agent."""
        from databridge_discovery.agents.schema_scanner import SchemaScanner

        scanner1 = SchemaScanner()
        scanner2 = SchemaScanner()

        orchestrator.register_agent("scanner", scanner1)
        orchestrator.register_agent("scanner", scanner2, replace=True)

        assert orchestrator.get_agent("scanner") is scanner2

    def test_list_agents(self, orchestrator_with_agents):
        """Test listing agents."""
        agents = orchestrator_with_agents.list_agents()

        assert len(agents) == 2
        names = [a["name"] for a in agents]
        assert "scanner" in names
        assert "extractor" in names

    def test_create_workflow(self, orchestrator_with_agents):
        """Test workflow creation."""
        steps = [
            {
                "name": "Scan Schema",
                "agent": "scanner",
                "capability": "scan_schema",
                "output_key": "scan_result",
            },
            {
                "name": "Extract Logic",
                "agent": "extractor",
                "capability": "parse_sql",
                "output_key": "parse_result",
            },
        ]

        workflow = orchestrator_with_agents.create_workflow(
            name="test_workflow",
            steps=steps,
            description="Test workflow",
        )

        assert workflow.name == "test_workflow"
        assert len(workflow.steps) == 2

    def test_execute_simple_workflow(self, orchestrator_with_agents):
        """Test simple workflow execution."""
        steps = [
            {
                "name": "Scan Schema",
                "agent": "scanner",
                "capability": "scan_schema",
                "output_key": "scan_result",
            },
        ]

        workflow = orchestrator_with_agents.create_workflow("simple", steps)

        execution = orchestrator_with_agents.execute_workflow(
            workflow.id,
            {
                "tables": [
                    {"name": "test", "columns": [{"name": "id", "type": "INT"}]}
                ]
            },
        )

        assert execution.workflow_id == workflow.id
        assert execution.steps_completed == 1

    def test_workflow_state_transitions(self, orchestrator_with_agents):
        """Test workflow state transitions."""
        from databridge_discovery.agents.orchestrator import WorkflowState

        steps = [
            {
                "name": "Test Step",
                "agent": "scanner",
                "capability": "scan_schema",
            },
        ]

        workflow = orchestrator_with_agents.create_workflow("state_test", steps)

        execution = orchestrator_with_agents.execute_workflow(
            workflow.id,
            {"tables": [{"name": "t", "columns": []}]},
        )

        assert execution.state == WorkflowState.COMPLETED


# =============================================================================
# Workflow Tests
# =============================================================================


class TestDiscoveryWorkflow:
    """Tests for Discovery Workflow."""

    @pytest.fixture
    def workflow(self):
        """Create Discovery Workflow instance."""
        from databridge_discovery.workflows.discovery_workflow import (
            DiscoveryWorkflow,
            DiscoveryWorkflowConfig,
        )

        config = DiscoveryWorkflowConfig(
            scan_schema=True,
            extract_logic=True,
            design_model=False,
            generate_dbt=False,
            validate_deployment=False,
        )

        return DiscoveryWorkflow(config=config)

    def test_create_workflow(self, workflow):
        """Test workflow creation."""
        wf = workflow.create_workflow()

        assert wf.name == "discovery_workflow"
        assert len(wf.steps) > 0


class TestValidationWorkflow:
    """Tests for Validation Workflow."""

    def test_validation_types(self):
        """Test validation type enum."""
        from databridge_discovery.workflows.validation_workflow import ValidationType

        assert ValidationType.SCHEMA.value == "schema"
        assert ValidationType.DATA_QUALITY.value == "data_quality"
        assert ValidationType.REFERENTIAL_INTEGRITY.value == "referential_integrity"
        assert ValidationType.BUSINESS_RULE.value == "business_rule"
        assert ValidationType.AGGREGATION.value == "aggregation"

    def test_validation_levels(self):
        """Test validation level enum."""
        from databridge_discovery.workflows.validation_workflow import ValidationLevel

        assert ValidationLevel.ERROR.value == "error"
        assert ValidationLevel.WARNING.value == "warning"
        assert ValidationLevel.INFO.value == "info"

    def test_validation_workflow_execute(self):
        """Test validation workflow execution."""
        from databridge_discovery.workflows.validation_workflow import (
            ValidationWorkflow,
            ValidationCheck,
            ValidationType,
            ValidationLevel,
        )

        workflow = ValidationWorkflow()

        # Add data quality check
        workflow.add_check(ValidationCheck(
            name="null_check",
            validation_type=ValidationType.DATA_QUALITY,
            level=ValidationLevel.WARNING,
            expected={"max_null_percent": 10},
        ))

        # Execute with sample data
        result = workflow.execute({
            "records": [
                {"id": 1, "name": "Test1"},
                {"id": 2, "name": None},
            ]
        })

        assert result.total_checks == 1


class TestIncrementalSyncWorkflow:
    """Tests for Incremental Sync Workflow."""

    def test_sync_modes(self):
        """Test sync mode enum."""
        from databridge_discovery.workflows.incremental_sync import SyncMode

        assert SyncMode.FULL.value == "full"
        assert SyncMode.INCREMENTAL.value == "incremental"
        assert SyncMode.MERGE.value == "merge"

    def test_full_sync(self):
        """Test full sync workflow."""
        from databridge_discovery.workflows.incremental_sync import (
            IncrementalSyncWorkflow,
            SyncConfig,
            SyncMode,
        )

        workflow = IncrementalSyncWorkflow()

        config = SyncConfig(
            mode=SyncMode.FULL,
            source_table="source.data",
            target_table="target.data",
        )

        source_data = [
            {"id": 1, "value": "A"},
            {"id": 2, "value": "B"},
        ]

        result = workflow.execute(config, source_data=source_data)

        assert result.mode == SyncMode.FULL
        assert result.records_inserted == 2

    def test_incremental_sync(self):
        """Test incremental sync workflow."""
        from databridge_discovery.workflows.incremental_sync import (
            IncrementalSyncWorkflow,
            SyncConfig,
            SyncMode,
        )

        workflow = IncrementalSyncWorkflow()

        config = SyncConfig(
            mode=SyncMode.INCREMENTAL,
            source_table="source.data",
            target_table="target.data",
            key_columns=["id"],
        )

        source_data = [
            {"id": 1, "value": "A"},
            {"id": 2, "value": "B"},
            {"id": 3, "value": "C"},
        ]

        target_data = [
            {"id": 1, "value": "A"},
        ]

        result = workflow.execute(config, source_data=source_data, target_data=target_data)

        assert result.mode == SyncMode.INCREMENTAL
        assert result.records_inserted == 2  # id=2 and id=3


# =============================================================================
# MCP Tool Tests
# =============================================================================


class TestPhase5MCPTools:
    """Tests for Phase 5 MCP tools."""

    def test_start_discovery_workflow(self):
        """Test start_discovery_workflow tool."""
        from databridge_discovery.mcp.tools import start_discovery_workflow

        result = start_discovery_workflow(
            sql="SELECT CASE WHEN x=1 THEN 'A' END AS cat FROM t",
            scan_schema=False,
            extract_logic=True,
            design_model=False,
            generate_dbt=False,
        )

        assert "execution_id" in result
        assert "status" in result

    def test_get_workflow_status_not_found(self):
        """Test get_workflow_status with invalid ID."""
        from databridge_discovery.mcp.tools import get_workflow_status

        result = get_workflow_status("invalid_id")

        assert "error" in result

    def test_get_agent_capabilities(self):
        """Test get_agent_capabilities tool."""
        from databridge_discovery.mcp.tools import get_agent_capabilities

        result = get_agent_capabilities()

        assert "agents" in result
        assert len(result["agents"]) == 4
        assert result["total_capabilities"] > 0

    def test_get_agent_capabilities_specific(self):
        """Test get_agent_capabilities for specific agent."""
        from databridge_discovery.mcp.tools import get_agent_capabilities

        result = get_agent_capabilities(agent_name="scanner")

        assert len(result["agents"]) == 1
        assert result["agents"][0]["name"] == "scanner"

    def test_validate_workflow_config_valid(self):
        """Test validate_workflow_config with valid config."""
        from databridge_discovery.mcp.tools import validate_workflow_config

        result = validate_workflow_config(
            scan_schema=True,
            extract_logic=True,
            tables=[{"name": "test"}],
            sql="SELECT * FROM test",
        )

        assert result["valid"] is True
        assert "scan" in result["phases"]
        assert "extract" in result["phases"]

    def test_validate_workflow_config_invalid(self):
        """Test validate_workflow_config with invalid config."""
        from databridge_discovery.mcp.tools import validate_workflow_config

        result = validate_workflow_config(
            scan_schema=False,
            extract_logic=False,
            design_model=False,
            generate_dbt=False,
        )

        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_invoke_schema_scanner(self):
        """Test invoke_schema_scanner tool."""
        from databridge_discovery.mcp.tools import invoke_schema_scanner

        result = invoke_schema_scanner(
            tables=[
                {
                    "name": "test_table",
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "VARCHAR"},
                    ],
                }
            ],
            capability="scan_schema",
        )

        assert result["success"] is True
        assert result["agent"] == "schema_scanner"

    def test_invoke_logic_extractor(self):
        """Test invoke_logic_extractor tool."""
        from databridge_discovery.mcp.tools import invoke_logic_extractor

        result = invoke_logic_extractor(
            sql="SELECT CASE WHEN x=1 THEN 'A' ELSE 'B' END AS cat FROM t",
            capability="extract_case",
        )

        assert result["success"] is True
        assert result["agent"] == "logic_extractor"

    def test_invoke_deploy_validator_dry_run(self):
        """Test invoke_deploy_validator in dry run mode."""
        from databridge_discovery.mcp.tools import invoke_deploy_validator

        result = invoke_deploy_validator(
            ddl_statements=[
                "CREATE TABLE test (id INT);",
            ],
            capability="execute_ddl",
            dry_run=True,
        )

        assert result["success"] is True
        assert result["dry_run"] is True

    def test_configure_agent(self):
        """Test configure_agent tool."""
        from databridge_discovery.mcp.tools import configure_agent

        result = configure_agent(
            agent_name="scanner",
            config={"timeout_seconds": 300},
        )

        assert result["success"] is True
        assert result["config"]["timeout_seconds"] == 300

    def test_get_workflow_history(self):
        """Test get_workflow_history tool."""
        from databridge_discovery.mcp.tools import get_workflow_history

        result = get_workflow_history(limit=5)

        assert "executions" in result
        assert "total_count" in result


# =============================================================================
# Integration Tests
# =============================================================================


class TestPhase5Integration:
    """Integration tests for Phase 5 components."""

    def test_full_agent_orchestration(self):
        """Test full agent orchestration flow."""
        from databridge_discovery.agents.orchestrator import Orchestrator
        from databridge_discovery.agents.schema_scanner import SchemaScanner
        from databridge_discovery.agents.logic_extractor import LogicExtractor

        # Create orchestrator
        orch = Orchestrator()
        orch.register_agent("scanner", SchemaScanner())
        orch.register_agent("extractor", LogicExtractor())

        # Create workflow with dependencies
        steps = [
            {
                "name": "Scan",
                "agent": "scanner",
                "capability": "scan_schema",
                "output_key": "scan_result",
            },
        ]

        workflow = orch.create_workflow("integration_test", steps)

        # Execute
        execution = orch.execute_workflow(
            workflow.id,
            {"tables": [{"name": "t", "columns": [{"name": "c", "type": "INT"}]}]},
        )

        # Verify
        assert execution.steps_completed == 1
        assert "scan_result" in execution.shared_state

    def test_workflow_event_handlers(self):
        """Test workflow event handling."""
        from databridge_discovery.agents.orchestrator import Orchestrator
        from databridge_discovery.agents.schema_scanner import SchemaScanner

        events_received = []

        def on_started(execution):
            events_received.append(("started", execution.id))

        def on_completed(execution):
            events_received.append(("completed", execution.id))

        orch = Orchestrator()
        orch.register_agent("scanner", SchemaScanner())
        orch.on("workflow_started", on_started)
        orch.on("workflow_completed", on_completed)

        workflow = orch.create_workflow("event_test", [
            {"agent": "scanner", "capability": "scan_schema"},
        ])

        execution = orch.execute_workflow(
            workflow.id,
            {"tables": []},
        )

        # Verify events
        assert len(events_received) == 2
        assert events_received[0][0] == "started"
        assert events_received[1][0] == "completed"
