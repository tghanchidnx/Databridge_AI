"""
Unit tests for Deployment Service.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from datetime import datetime, timezone

from src.deployment.service import (
    DeploymentService,
    DeploymentServiceError,
    ProjectNotFoundError,
    ConnectionNotFoundError,
)
from src.deployment.models import (
    DeploymentStatus,
    DeploymentMode,
    DeploymentConfig,
)
from src.generation.ddl_generator import SQLDialect


class TestDeploymentServiceInit:
    """Tests for DeploymentService initialization."""

    def test_init_without_session(self):
        """Test service initialization without session."""
        service = DeploymentService()

        assert service._session is None
        assert service._external_session is False

    def test_init_with_session(self):
        """Test service initialization with session."""
        mock_session = Mock()
        service = DeploymentService(session=mock_session)

        assert service._session == mock_session
        assert service._external_session is True


class TestDeploymentServiceDialect:
    """Tests for dialect mapping."""

    def test_get_dialect_snowflake(self):
        """Test Snowflake dialect mapping."""
        service = DeploymentService()

        assert service._get_dialect("snowflake") == SQLDialect.SNOWFLAKE
        assert service._get_dialect("SNOWFLAKE") == SQLDialect.SNOWFLAKE

    def test_get_dialect_postgresql(self):
        """Test PostgreSQL dialect mapping."""
        service = DeploymentService()

        assert service._get_dialect("postgresql") == SQLDialect.POSTGRESQL
        assert service._get_dialect("postgres") == SQLDialect.POSTGRESQL

    def test_get_dialect_bigquery(self):
        """Test BigQuery dialect mapping."""
        service = DeploymentService()

        assert service._get_dialect("bigquery") == SQLDialect.BIGQUERY

    def test_get_dialect_sqlserver(self):
        """Test SQL Server dialect mapping."""
        service = DeploymentService()

        assert service._get_dialect("sqlserver") == SQLDialect.TSQL

    def test_get_dialect_mysql(self):
        """Test MySQL dialect mapping."""
        service = DeploymentService()

        assert service._get_dialect("mysql") == SQLDialect.MYSQL

    def test_get_dialect_unknown(self):
        """Test unknown dialect defaults to Snowflake."""
        service = DeploymentService()

        assert service._get_dialect("unknown") == SQLDialect.SNOWFLAKE


class TestDeploymentServiceGetAdapter:
    """Tests for adapter creation."""

    def test_get_adapter_snowflake(self):
        """Test Snowflake adapter creation."""
        service = DeploymentService()

        mock_connection = Mock()
        mock_connection.connection_type = "snowflake"
        mock_connection.host = "test_account"
        mock_connection.username = "test_user"
        mock_connection.password_encrypted = "test_pass"
        mock_connection.database = "TEST_DB"
        mock_connection.extra_config = {
            "warehouse": "TEST_WH",
            "role": "TEST_ROLE",
        }

        adapter = service.get_adapter_for_connection(mock_connection)

        assert adapter is not None
        assert adapter.host == "test_account"

    def test_get_adapter_unsupported(self):
        """Test unsupported connection type raises error."""
        service = DeploymentService()

        mock_connection = Mock()
        mock_connection.connection_type = "oracle"

        with pytest.raises(DeploymentServiceError, match="Unsupported"):
            service.get_adapter_for_connection(mock_connection)


class TestDeploymentServiceHistory:
    """Tests for deployment history operations."""

    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        session = MagicMock()
        return session

    @pytest.fixture
    def service(self, mock_session):
        """Create service with mock session."""
        return DeploymentService(session=mock_session)

    def test_get_deployment_history(self, service, mock_session):
        """Test getting deployment history."""
        # Create mock history records
        mock_history = Mock()
        mock_history.id = 1
        mock_history.project_id = "test-123"
        mock_history.connection_id = "conn-456"
        mock_history.script_type = "CREATE_TABLE"
        mock_history.target_database = "WAREHOUSE"
        mock_history.target_schema = "ANALYTICS"
        mock_history.target_table = "TEST_TABLE"
        mock_history.status = "success"
        mock_history.error_message = None
        mock_history.rows_affected = 0
        mock_history.executed_at = datetime.now(timezone.utc)
        mock_history.executed_by = "test_user"
        mock_history.duration_ms = 100
        mock_history.created_at = datetime.now(timezone.utc)

        # Configure mock query chain
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = [mock_history]
        mock_session.query.return_value = mock_query

        history = service.get_deployment_history("test-123", limit=10)

        assert len(history) == 1
        assert history[0]["id"] == 1
        assert history[0]["status"] == "success"

    def test_get_deployment_summary(self, service, mock_session):
        """Test getting deployment summary."""
        # Configure mock query for counts
        mock_count_query = MagicMock()
        mock_count_query.filter.return_value = mock_count_query
        mock_count_query.count.side_effect = [10, 8, 2]

        # Configure mock query for latest
        mock_latest = Mock()
        mock_latest.id = 5
        mock_latest.status = "success"
        mock_latest.executed_at = datetime.now(timezone.utc)
        mock_latest.target_database = "WAREHOUSE"
        mock_latest.target_schema = "ANALYTICS"

        mock_latest_query = MagicMock()
        mock_latest_query.filter.return_value = mock_latest_query
        mock_latest_query.order_by.return_value = mock_latest_query
        mock_latest_query.first.return_value = mock_latest

        # Setup query returns based on call order
        call_count = [0]
        def query_side_effect(*args):
            call_count[0] += 1
            if call_count[0] <= 3:
                return mock_count_query
            return mock_latest_query

        mock_session.query.side_effect = query_side_effect

        summary = service.get_deployment_summary("test-123")

        assert summary["project_id"] == "test-123"


class TestDeploymentServiceComparison:
    """Tests for deployment comparison."""

    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        session = MagicMock()
        return session

    @pytest.fixture
    def service(self, mock_session):
        """Create service with mock session."""
        return DeploymentService(session=mock_session)

    def test_compare_deployment_versions(self, service, mock_session):
        """Test comparing two deployments."""
        # Create mock deployments
        mock_d1 = Mock()
        mock_d1.id = 1
        mock_d1.executed_at = datetime.now(timezone.utc)
        mock_d1.target_database = "WAREHOUSE"
        mock_d1.target_schema = "ANALYTICS"
        mock_d1.target_table = "TEST_TABLE"
        mock_d1.status = "success"
        mock_d1.script_type = "CREATE_TABLE"
        mock_d1.script_content = "CREATE TABLE TEST_TABLE (id INT)"

        mock_d2 = Mock()
        mock_d2.id = 2
        mock_d2.executed_at = datetime.now(timezone.utc)
        mock_d2.target_database = "WAREHOUSE"
        mock_d2.target_schema = "ANALYTICS"
        mock_d2.target_table = "TEST_TABLE"
        mock_d2.status = "success"
        mock_d2.script_type = "CREATE_TABLE"
        mock_d2.script_content = "CREATE TABLE TEST_TABLE (id INT, name VARCHAR)"

        # Configure mock query chain
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.first.side_effect = [mock_d1, mock_d2]
        mock_session.query.return_value = mock_query

        result = service.compare_deployment_versions(
            project_id="test-123",
            deployment_id_1=1,
            deployment_id_2=2,
        )

        assert "deployment_1" in result
        assert "deployment_2" in result
        assert "differences" in result
        assert result["differences"]["same_target"] is True
        assert result["differences"]["script_content_changed"] is True

    def test_compare_deployment_not_found(self, service, mock_session):
        """Test comparison when deployment not found."""
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query

        result = service.compare_deployment_versions(
            project_id="test-123",
            deployment_id_1=1,
            deployment_id_2=2,
        )

        assert "error" in result


class TestDeploymentServiceCreatePlan:
    """Tests for deployment plan creation."""

    def test_create_plan_project_not_found(self):
        """Test plan creation when project not found."""
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query

        service = DeploymentService(session=mock_session)

        config = DeploymentConfig(
            target_database="WAREHOUSE",
            target_schema="ANALYTICS",
        )

        with pytest.raises(ProjectNotFoundError):
            service.create_deployment_plan(
                project_id="nonexistent",
                connection_id="conn-123",
                config=config,
            )
