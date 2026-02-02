"""
Unit tests for Deployment Executor.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict, Any

from src.deployment.executor import (
    DeploymentExecutor,
    DeploymentError,
    ValidationError,
    ExecutionError,
)
from src.deployment.models import (
    DeploymentStatus,
    DeploymentMode,
    ScriptType,
    DeploymentScript,
    DeploymentConfig,
    DeploymentPlan,
)
from src.generation.ddl_generator import GeneratedDDL, DDLType, SQLDialect
from src.connections.base import AbstractDatabaseAdapter, QueryResult


class MockAdapter(AbstractDatabaseAdapter):
    """Mock database adapter for testing."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.connected = False
        self.queries_executed = []
        self.should_fail = False
        self.fail_on_query = None

    @property
    def adapter_type(self) -> str:
        return "mock"

    def connect(self) -> bool:
        self.connected = True
        return True

    def disconnect(self) -> None:
        self.connected = False

    def test_connection(self) -> Tuple[bool, str]:
        return True, "OK"

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        max_rows: int = 1000,
    ) -> QueryResult:
        self.queries_executed.append(query)
        if self.should_fail:
            raise Exception("Simulated failure")
        if self.fail_on_query and self.fail_on_query in query:
            raise Exception(f"Failed on: {self.fail_on_query}")
        return QueryResult(
            columns=["result"],
            rows=[["OK"]],
            execution_time_ms=10,
            row_count=1,
            truncated=False,
        )

    def list_databases(self) -> List[str]:
        return ["TEST_DB"]

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        return ["PUBLIC"]

    def list_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[str]:
        return []

    def list_columns(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return []

    def get_distinct_values(
        self,
        table: str,
        column: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 100,
    ) -> List[Any]:
        return []


class TestDeploymentExecutorInit:
    """Tests for DeploymentExecutor initialization."""

    def test_init_with_adapter(self):
        """Test executor initialization."""
        adapter = MockAdapter()
        executor = DeploymentExecutor(adapter)

        assert executor.adapter == adapter

    def test_set_progress_callback(self):
        """Test setting progress callback."""
        adapter = MockAdapter()
        executor = DeploymentExecutor(adapter)

        callback = Mock()
        executor.set_progress_callback(callback)

        # Trigger a progress report
        executor._report_progress("Test", 1, 10)

        callback.assert_called_once_with("Test", 1, 10)


class TestDeploymentExecutorCreatePlan:
    """Tests for plan creation."""

    @pytest.fixture
    def executor(self):
        """Create executor with mock adapter."""
        return DeploymentExecutor(MockAdapter())

    @pytest.fixture
    def config(self):
        """Create deployment config."""
        return DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

    def test_create_plan_from_ddl(self, executor, config):
        """Test creating plan from generated DDL."""
        generated_ddl = [
            GeneratedDDL(
                ddl_type=DDLType.CREATE_TABLE,
                object_name="TBL_0_TEST",
                schema_name="ANALYTICS",
                sql="CREATE TABLE TBL_0_TEST (id INT)",
                dialect=SQLDialect.SNOWFLAKE,
                tier="TBL_0",
            ),
            GeneratedDDL(
                ddl_type=DDLType.INSERT,
                object_name="TBL_0_TEST",
                schema_name="ANALYTICS",
                sql="INSERT INTO TBL_0_TEST VALUES (1)",
                dialect=SQLDialect.SNOWFLAKE,
                tier="TBL_0",
            ),
        ]

        plan = executor.create_plan(
            generated_ddl=generated_ddl,
            config=config,
            project_id="test-123",
            project_name="Test Project",
        )

        assert plan.project_id == "test-123"
        assert plan.project_name == "Test Project"
        assert len(plan.scripts) == 2
        assert plan.is_valid is True

    def test_create_plan_empty(self, executor, config):
        """Test creating plan with no DDL."""
        plan = executor.create_plan(
            generated_ddl=[],
            config=config,
            project_id="test-123",
            project_name="Test Project",
        )

        assert plan.is_valid is False
        assert "No scripts to deploy" in plan.validation_errors

    def test_create_plan_missing_database(self, executor):
        """Test validation error for missing database."""
        config = DeploymentConfig(
            target_database="",
            target_schema="ANALYTICS",
        )

        generated_ddl = [
            GeneratedDDL(
                ddl_type=DDLType.CREATE_TABLE,
                object_name="TEST",
                schema_name="ANALYTICS",
                sql="CREATE TABLE TEST (id INT)",
                dialect=SQLDialect.SNOWFLAKE,
            ),
        ]

        plan = executor.create_plan(
            generated_ddl=generated_ddl,
            config=config,
            project_id="test-123",
            project_name="Test Project",
        )

        assert plan.is_valid is False
        assert any("database" in e.lower() for e in plan.validation_errors)


class TestDeploymentExecutorExecution:
    """Tests for deployment execution."""

    @pytest.fixture
    def adapter(self):
        """Create mock adapter."""
        return MockAdapter()

    @pytest.fixture
    def executor(self, adapter):
        """Create executor with mock adapter."""
        return DeploymentExecutor(adapter)

    @pytest.fixture
    def valid_plan(self):
        """Create a valid deployment plan."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
            schema_name="ANALYTICS",
            database_name="WAREHOUSE",
        )
        return DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=[script],
            execution_order=[0],
            estimated_objects=1,
        )

    @pytest.fixture
    def config(self):
        """Create deployment config."""
        return DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

    def test_execute_dry_run(self, executor, valid_plan):
        """Test dry run mode."""
        config = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            mode=DeploymentMode.DRY_RUN,
        )

        result = executor.execute(valid_plan, config)

        assert result.status == DeploymentStatus.SUCCESS
        assert result.mode == DeploymentMode.DRY_RUN
        # No actual execution in dry run
        assert len(executor.adapter.queries_executed) == 0

    def test_execute_success(self, executor, valid_plan, config):
        """Test successful execution."""
        result = executor.execute(valid_plan, config)

        assert result.status == DeploymentStatus.SUCCESS
        assert result.successful_scripts == 1
        assert result.failed_scripts == 0
        assert executor.adapter.connected is False  # Disconnected after

    def test_execute_with_failure(self, adapter, valid_plan, config):
        """Test execution with failure."""
        adapter.should_fail = True
        executor = DeploymentExecutor(adapter)

        result = executor.execute(valid_plan, config)

        assert result.status == DeploymentStatus.FAILED
        assert result.error_message is not None

    def test_execute_invalid_plan(self, executor, config):
        """Test execution with invalid plan."""
        invalid_plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            validation_errors=["Missing permissions"],
        )

        result = executor.execute(invalid_plan, config)

        assert result.status == DeploymentStatus.FAILED
        assert "Missing permissions" in result.error_message

    def test_execute_multiple_scripts(self, executor, config):
        """Test executing multiple scripts."""
        scripts = [
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_1",
                sql="CREATE TABLE TABLE_1 (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_2",
                sql="CREATE TABLE TABLE_2 (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.INSERT,
                object_name="TABLE_1",
                sql="INSERT INTO TABLE_1 VALUES (1)",
            ),
        ]

        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=scripts,
            execution_order=[0, 1, 2],
            estimated_objects=3,
        )

        result = executor.execute(plan, config)

        assert result.status == DeploymentStatus.SUCCESS
        assert result.total_scripts == 3
        assert result.successful_scripts == 3

    def test_execute_stop_on_error(self, adapter, config):
        """Test stop on error behavior."""
        adapter.fail_on_query = "TABLE_2"
        executor = DeploymentExecutor(adapter)

        scripts = [
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_1",
                sql="CREATE TABLE TABLE_1 (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_2",
                sql="CREATE TABLE TABLE_2 (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_3",
                sql="CREATE TABLE TABLE_3 (id INT)",
            ),
        ]

        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=scripts,
            execution_order=[0, 1, 2],
            estimated_objects=3,
        )

        config_stop = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            stop_on_error=True,
        )

        result = executor.execute(plan, config_stop)

        assert result.status == DeploymentStatus.FAILED
        # Should have stopped at TABLE_2
        assert result.successful_scripts == 1
        assert result.failed_scripts == 1
        assert len(result.script_results) == 2  # Didn't reach TABLE_3

    def test_execute_continue_on_error(self, adapter, config):
        """Test continue on error behavior."""
        adapter.fail_on_query = "TABLE_2"
        executor = DeploymentExecutor(adapter)

        scripts = [
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_1",
                sql="CREATE TABLE TABLE_1 (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_2",
                sql="CREATE TABLE TABLE_2 (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_3",
                sql="CREATE TABLE TABLE_3 (id INT)",
            ),
        ]

        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=scripts,
            execution_order=[0, 1, 2],
            estimated_objects=3,
        )

        config_continue = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            stop_on_error=False,
        )

        result = executor.execute(plan, config_continue)

        assert result.status == DeploymentStatus.PARTIAL
        assert result.successful_scripts == 2  # TABLE_1 and TABLE_3
        assert result.failed_scripts == 1
        assert len(result.script_results) == 3


class TestDeploymentExecutorRollback:
    """Tests for deployment rollback."""

    @pytest.fixture
    def adapter(self):
        """Create mock adapter."""
        return MockAdapter()

    @pytest.fixture
    def executor(self, adapter):
        """Create executor with mock adapter."""
        return DeploymentExecutor(adapter)

    def test_rollback_success(self, executor):
        """Test successful rollback."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
            schema_name="ANALYTICS",
            database_name="WAREHOUSE",
        )

        from src.deployment.models import ScriptExecutionResult, DeploymentResult
        result = DeploymentResult(
            deployment_id="deploy-123",
            project_id="test-123",
            status=DeploymentStatus.SUCCESS,
            mode=DeploymentMode.EXECUTE,
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            started_at=datetime.now(timezone.utc),
            script_results=[
                ScriptExecutionResult(
                    script=script,
                    status=DeploymentStatus.SUCCESS,
                )
            ],
        )

        config = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

        rollback_result = executor.rollback(result, config)

        assert rollback_result.rollback_executed is True
        assert rollback_result.status == DeploymentStatus.ROLLED_BACK


class TestDeploymentExecutorValidation:
    """Tests for validation methods."""

    @pytest.fixture
    def executor(self):
        """Create executor with mock adapter."""
        return DeploymentExecutor(MockAdapter())

    def test_validate_connection_success(self, executor):
        """Test successful connection validation."""
        assert executor.validate_connection() is True

    def test_dangerous_pattern_detection(self, executor):
        """Test detection of dangerous SQL patterns."""
        # These should be detected as dangerous
        dangerous_sqls = [
            "SELECT 1; DROP DATABASE test",
            "CREATE TABLE t; DROP SCHEMA public CASCADE",
        ]

        for sql in dangerous_sqls:
            assert executor._contains_dangerous_patterns(sql) is True

    def test_safe_sql_not_flagged(self, executor):
        """Test that safe SQL is not flagged."""
        safe_sqls = [
            "CREATE TABLE test (id INT)",
            "INSERT INTO test VALUES (1)",
            "CREATE VIEW v AS SELECT * FROM t",
            "DROP TABLE IF EXISTS test",  # Single DROP is OK
        ]

        for sql in safe_sqls:
            assert executor._contains_dangerous_patterns(sql) is False


class TestDeploymentExecutorDependencies:
    """Tests for dependency resolution."""

    @pytest.fixture
    def executor(self):
        """Create executor with mock adapter."""
        return DeploymentExecutor(MockAdapter())

    def test_resolve_execution_order_no_deps(self, executor):
        """Test execution order with no dependencies."""
        scripts = [
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="A",
                sql="CREATE TABLE A (id INT)",
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="B",
                sql="CREATE TABLE B (id INT)",
            ),
        ]

        order = executor._resolve_execution_order(scripts)

        # All scripts should be in the order
        assert set(order) == {0, 1}

    def test_resolve_execution_order_with_deps(self, executor):
        """Test execution order respects dependencies."""
        scripts = [
            DeploymentScript(
                script_type=ScriptType.CREATE_VIEW,
                object_name="VIEW_B",
                sql="CREATE VIEW VIEW_B AS SELECT * FROM TABLE_A",
                dependencies=["TABLE_A"],
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_TABLE,
                object_name="TABLE_A",
                sql="CREATE TABLE TABLE_A (id INT)",
            ),
        ]

        order = executor._resolve_execution_order(scripts)

        # TABLE_A (index 1) must come before VIEW_B (index 0)
        assert order.index(1) < order.index(0)

    def test_circular_dependency_detection(self, executor):
        """Test circular dependency raises error."""
        scripts = [
            DeploymentScript(
                script_type=ScriptType.CREATE_VIEW,
                object_name="A",
                sql="CREATE VIEW A AS SELECT * FROM B",
                dependencies=["B"],
            ),
            DeploymentScript(
                script_type=ScriptType.CREATE_VIEW,
                object_name="B",
                sql="CREATE VIEW B AS SELECT * FROM A",
                dependencies=["A"],
            ),
        ]

        with pytest.raises(ValidationError, match="Circular dependency"):
            executor._resolve_execution_order(scripts)
