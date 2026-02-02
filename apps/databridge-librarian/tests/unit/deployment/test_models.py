"""
Unit tests for Deployment Models.
"""

import pytest
from datetime import datetime, timezone

from src.deployment.models import (
    DeploymentStatus,
    DeploymentMode,
    ScriptType,
    DeploymentScript,
    ScriptExecutionResult,
    DeploymentPlan,
    DeploymentResult,
    DeploymentConfig,
)


class TestDeploymentStatus:
    """Tests for DeploymentStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected statuses exist."""
        assert DeploymentStatus.PENDING
        assert DeploymentStatus.VALIDATING
        assert DeploymentStatus.EXECUTING
        assert DeploymentStatus.SUCCESS
        assert DeploymentStatus.FAILED
        assert DeploymentStatus.ROLLED_BACK
        assert DeploymentStatus.PARTIAL

    def test_status_values(self):
        """Test status string values."""
        assert DeploymentStatus.PENDING.value == "pending"
        assert DeploymentStatus.SUCCESS.value == "success"
        assert DeploymentStatus.FAILED.value == "failed"


class TestDeploymentMode:
    """Tests for DeploymentMode enum."""

    def test_all_modes_exist(self):
        """Test all expected modes exist."""
        assert DeploymentMode.DRY_RUN
        assert DeploymentMode.EXECUTE
        assert DeploymentMode.EXECUTE_WITH_ROLLBACK

    def test_mode_values(self):
        """Test mode string values."""
        assert DeploymentMode.DRY_RUN.value == "dry_run"
        assert DeploymentMode.EXECUTE.value == "execute"


class TestScriptType:
    """Tests for ScriptType enum."""

    def test_all_types_exist(self):
        """Test all expected script types exist."""
        assert ScriptType.CREATE_TABLE
        assert ScriptType.CREATE_VIEW
        assert ScriptType.CREATE_DYNAMIC_TABLE
        assert ScriptType.INSERT
        assert ScriptType.MERGE
        assert ScriptType.DROP
        assert ScriptType.ALTER
        assert ScriptType.GRANT


class TestDeploymentScript:
    """Tests for DeploymentScript dataclass."""

    def test_basic_script(self):
        """Test basic script creation."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
        )

        assert script.script_type == ScriptType.CREATE_TABLE
        assert script.object_name == "TEST_TABLE"
        assert "CREATE TABLE" in script.sql

    def test_full_name_with_all_parts(self):
        """Test full_name with database, schema, and object."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
            schema_name="ANALYTICS",
            database_name="WAREHOUSE",
        )

        assert script.full_name == "WAREHOUSE.ANALYTICS.TEST_TABLE"

    def test_full_name_without_database(self):
        """Test full_name without database."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
            schema_name="ANALYTICS",
        )

        assert script.full_name == "ANALYTICS.TEST_TABLE"

    def test_full_name_object_only(self):
        """Test full_name with object only."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
        )

        assert script.full_name == "TEST_TABLE"

    def test_dependencies_default(self):
        """Test dependencies default to empty list."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
        )

        assert script.dependencies == []

    def test_with_dependencies(self):
        """Test script with dependencies."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_VIEW,
            object_name="TEST_VIEW",
            sql="CREATE VIEW TEST_VIEW AS SELECT * FROM BASE_TABLE",
            dependencies=["BASE_TABLE"],
        )

        assert "BASE_TABLE" in script.dependencies


class TestScriptExecutionResult:
    """Tests for ScriptExecutionResult dataclass."""

    def test_successful_result(self):
        """Test successful execution result."""
        script = DeploymentScript(
            script_type=ScriptType.INSERT,
            object_name="TEST_TABLE",
            sql="INSERT INTO TEST_TABLE VALUES (1)",
        )

        result = ScriptExecutionResult(
            script=script,
            status=DeploymentStatus.SUCCESS,
            rows_affected=100,
            execution_time_ms=50,
        )

        assert result.success is True
        assert result.rows_affected == 100
        assert result.execution_time_ms == 50

    def test_failed_result(self):
        """Test failed execution result."""
        script = DeploymentScript(
            script_type=ScriptType.INSERT,
            object_name="TEST_TABLE",
            sql="INSERT INTO TEST_TABLE VALUES (1)",
        )

        result = ScriptExecutionResult(
            script=script,
            status=DeploymentStatus.FAILED,
            error_message="Table does not exist",
        )

        assert result.success is False
        assert result.error_message == "Table does not exist"


class TestDeploymentPlan:
    """Tests for DeploymentPlan dataclass."""

    def test_empty_plan(self):
        """Test empty deployment plan."""
        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

        assert plan.is_valid is False  # No scripts
        assert plan.estimated_objects == 0

    def test_valid_plan(self):
        """Test valid deployment plan."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
        )

        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=[script],
            estimated_objects=1,
        )

        assert plan.is_valid is True
        assert plan.estimated_objects == 1

    def test_plan_with_validation_errors(self):
        """Test plan with validation errors."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
        )

        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=[script],
            validation_errors=["Missing permissions"],
        )

        assert plan.is_valid is False

    def test_plan_to_dict(self):
        """Test plan serialization."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
            tier="TBL_0",
        )

        plan = DeploymentPlan(
            project_id="test-123",
            project_name="Test Project",
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            scripts=[script],
            estimated_objects=1,
        )

        result = plan.to_dict()

        assert result["project_id"] == "test-123"
        assert result["project_name"] == "Test Project"
        assert result["script_count"] == 1
        assert result["is_valid"] is True
        assert len(result["scripts"]) == 1
        assert result["scripts"][0]["tier"] == "TBL_0"


class TestDeploymentResult:
    """Tests for DeploymentResult dataclass."""

    def test_successful_deployment(self):
        """Test successful deployment result."""
        script = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TEST_TABLE",
            sql="CREATE TABLE TEST_TABLE (id INT)",
        )

        script_result = ScriptExecutionResult(
            script=script,
            status=DeploymentStatus.SUCCESS,
            rows_affected=0,
            execution_time_ms=100,
        )

        result = DeploymentResult(
            deployment_id="deploy-123",
            project_id="test-123",
            status=DeploymentStatus.SUCCESS,
            mode=DeploymentMode.EXECUTE,
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            started_at=datetime.now(timezone.utc),
            script_results=[script_result],
        )

        assert result.success is True
        assert result.total_scripts == 1
        assert result.successful_scripts == 1
        assert result.failed_scripts == 0
        assert result.total_execution_time_ms == 100

    def test_partial_deployment(self):
        """Test partial deployment result."""
        script1 = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TABLE_1",
            sql="CREATE TABLE TABLE_1 (id INT)",
        )
        script2 = DeploymentScript(
            script_type=ScriptType.CREATE_TABLE,
            object_name="TABLE_2",
            sql="CREATE TABLE TABLE_2 (id INT)",
        )

        result = DeploymentResult(
            deployment_id="deploy-123",
            project_id="test-123",
            status=DeploymentStatus.PARTIAL,
            mode=DeploymentMode.EXECUTE,
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            started_at=datetime.now(timezone.utc),
            script_results=[
                ScriptExecutionResult(
                    script=script1,
                    status=DeploymentStatus.SUCCESS,
                    rows_affected=0,
                    execution_time_ms=50,
                ),
                ScriptExecutionResult(
                    script=script2,
                    status=DeploymentStatus.FAILED,
                    error_message="Permission denied",
                ),
            ],
        )

        assert result.success is False
        assert result.total_scripts == 2
        assert result.successful_scripts == 1
        assert result.failed_scripts == 1

    def test_result_to_dict(self):
        """Test result serialization."""
        script = DeploymentScript(
            script_type=ScriptType.INSERT,
            object_name="TEST_TABLE",
            sql="INSERT INTO TEST_TABLE VALUES (1)",
        )

        result = DeploymentResult(
            deployment_id="deploy-123",
            project_id="test-123",
            status=DeploymentStatus.SUCCESS,
            mode=DeploymentMode.EXECUTE,
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            started_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
            script_results=[
                ScriptExecutionResult(
                    script=script,
                    status=DeploymentStatus.SUCCESS,
                    rows_affected=100,
                    execution_time_ms=75,
                ),
            ],
        )

        output = result.to_dict()

        assert output["deployment_id"] == "deploy-123"
        assert output["status"] == "success"
        assert output["total_scripts"] == 1
        assert output["total_rows_affected"] == 100
        assert len(output["script_results"]) == 1


class TestDeploymentConfig:
    """Tests for DeploymentConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

        assert config.target_database == "WAREHOUSE"
        assert config.target_schema == "ANALYTICS"
        assert config.mode == DeploymentMode.EXECUTE
        assert config.stop_on_error is True
        assert config.use_transactions is True
        assert config.create_schema_if_not_exists is True
        assert config.drop_existing is False
        assert config.executed_by == "system"

    def test_custom_config(self):
        """Test custom configuration."""
        config = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
            mode=DeploymentMode.DRY_RUN,
            stop_on_error=False,
            drop_existing=True,
            grant_roles=["ANALYST", "DEVELOPER"],
            executed_by="test_user",
        )

        assert config.mode == DeploymentMode.DRY_RUN
        assert config.stop_on_error is False
        assert config.drop_existing is True
        assert "ANALYST" in config.grant_roles
        assert config.executed_by == "test_user"

    def test_config_to_dict(self):
        """Test config serialization."""
        config = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

        output = config.to_dict()

        assert output["target_database"] == "WAREHOUSE"
        assert output["target_schema"] == "ANALYTICS"
        assert output["mode"] == "execute"
