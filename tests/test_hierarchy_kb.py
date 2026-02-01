"""Tests for Hierarchy Knowledge Base MCP Tools.

This test suite covers the new modules added for the Hierarchy Knowledge Base:
- Connections Module
- Schema Matcher Module
- Data Matcher Module
- Enhanced Hierarchy Tools

Note: Some tests require the NestJS backend to be running at localhost:3001.
Tests that require the backend are marked with @pytest.mark.backend.
"""
import pytest
import json
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# =============================================================================
# Test Configuration
# =============================================================================

class TestConfiguration:
    """Tests for enhanced configuration settings."""

    def test_config_has_new_endpoints(self):
        """Test that config includes new endpoint settings."""
        from config import settings

        assert hasattr(settings, 'nestjs_connections_endpoint')
        assert hasattr(settings, 'nestjs_schema_matcher_endpoint')
        assert hasattr(settings, 'nestjs_data_matcher_endpoint')

        assert settings.nestjs_connections_endpoint == "/connections"
        assert settings.nestjs_schema_matcher_endpoint == "/schema-matcher"
        assert settings.nestjs_data_matcher_endpoint == "/data-matcher"

    def test_config_has_backend_url(self):
        """Test that backend URL is configured."""
        from config import settings

        assert hasattr(settings, 'nestjs_backend_url')
        assert "localhost:3001" in settings.nestjs_backend_url or settings.nestjs_backend_url != ""


# =============================================================================
# Connections Module Tests
# =============================================================================

class TestConnectionsModule:
    """Tests for the Connections module."""

    def test_connections_api_client_init(self):
        """Test ConnectionsApiClient initialization."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert client.base_url == "http://localhost:3001/api"
        assert client.api_key == "test-key"
        assert "X-API-Key" in client.headers

    def test_connections_api_client_list_connections_mock(self):
        """Test list_connections with mocked response."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {
                "data": [
                    {"id": "conn-1", "name": "Test Connection", "type": "snowflake"},
                    {"id": "conn-2", "name": "Another Connection", "type": "mysql"}
                ]
            }

            connections = client.list_connections()

            assert len(connections) == 2
            assert connections[0]["id"] == "conn-1"
            mock_request.assert_called_once_with("GET", "/connections")

    def test_connections_api_client_get_databases_mock(self):
        """Test get_databases with mocked response."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {
                "data": ["DATABASE_A", "DATABASE_B", "DATABASE_C"]
            }

            databases = client.get_databases("conn-1")

            assert len(databases) == 3
            assert "DATABASE_A" in databases
            mock_request.assert_called_once()

    def test_connections_api_client_error_handling(self):
        """Test error handling when backend is unreachable."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:9999/api",  # Non-existent port
            api_key="test-key",
            timeout=1
        )

        # This should return empty list due to connection error
        connections = client.list_connections()
        assert connections == []

    def test_mcp_tools_registration(self):
        """Test that connection MCP tools can be registered."""
        from connections.mcp_tools import register_connection_tools

        # Create a mock MCP server
        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        # Register tools
        client = register_connection_tools(
            mock_mcp,
            "http://localhost:3001/api",
            "test-key"
        )

        # Verify tools were registered (mock.tool was called)
        assert mock_mcp.tool.called


# =============================================================================
# Schema Matcher Module Tests
# =============================================================================

class TestSchemaMatcherModule:
    """Tests for the Schema Matcher module."""

    def test_schema_matcher_api_client_init(self):
        """Test SchemaMatcherApiClient initialization."""
        from schema_matcher.api_client import SchemaMatcherApiClient

        client = SchemaMatcherApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert client.base_url == "http://localhost:3001/api"
        assert client.timeout == 60  # Longer timeout for schema ops

    def test_schema_matcher_compare_schemas_mock(self):
        """Test compare_schemas with mocked response."""
        from schema_matcher.api_client import SchemaMatcherApiClient

        client = SchemaMatcherApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {
                "data": {
                    "columns_added": ["new_col"],
                    "columns_removed": ["old_col"],
                    "type_changes": {}
                }
            }

            result = client.compare_schemas(
                source_connection_id="conn-1",
                source_database="DB_A",
                source_schema="SCHEMA_A",
                source_table="TABLE_A",
                target_connection_id="conn-2",
                target_database="DB_B",
                target_schema="SCHEMA_B",
                target_table="TABLE_B"
            )

            assert "data" in result
            mock_request.assert_called_once()

    def test_schema_matcher_generate_merge_script_mock(self):
        """Test generate_merge_script with mocked response."""
        from schema_matcher.api_client import SchemaMatcherApiClient

        client = SchemaMatcherApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {
                "script": "MERGE INTO target USING source ON ..."
            }

            result = client.generate_merge_script(
                source_connection_id="conn-1",
                source_database="DB_A",
                source_schema="SCHEMA_A",
                source_table="TABLE_A",
                target_connection_id="conn-2",
                target_database="DB_B",
                target_schema="SCHEMA_B",
                target_table="TABLE_B",
                key_columns=["id"],
                script_type="MERGE"
            )

            assert "script" in result

    def test_mcp_schema_matcher_tools_registration(self):
        """Test that schema matcher MCP tools can be registered."""
        from schema_matcher.mcp_tools import register_schema_matcher_tools

        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        client = register_schema_matcher_tools(
            mock_mcp,
            "http://localhost:3001/api",
            "test-key"
        )

        assert mock_mcp.tool.called


# =============================================================================
# Data Matcher Module Tests
# =============================================================================

class TestDataMatcherModule:
    """Tests for the Data Matcher module."""

    def test_data_matcher_api_client_init(self):
        """Test DataMatcherApiClient initialization."""
        from data_matcher.api_client import DataMatcherApiClient

        client = DataMatcherApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert client.base_url == "http://localhost:3001/api"
        assert client.timeout == 120  # Longer timeout for data comparison

    def test_data_matcher_compare_data_mock(self):
        """Test compare_data with mocked response."""
        from data_matcher.api_client import DataMatcherApiClient

        client = DataMatcherApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {
                "data": {
                    "matches": 100,
                    "orphans_source": 5,
                    "orphans_target": 3,
                    "conflicts": 2
                }
            }

            result = client.compare_data(
                source_connection_id="conn-1",
                source_database="DB_A",
                source_schema="SCHEMA_A",
                source_table="TABLE_A",
                target_connection_id="conn-2",
                target_database="DB_B",
                target_schema="SCHEMA_B",
                target_table="TABLE_B",
                key_columns=["id"]
            )

            assert "data" in result

    def test_data_matcher_get_table_statistics_mock(self):
        """Test get_table_statistics with mocked response."""
        from data_matcher.api_client import DataMatcherApiClient

        client = DataMatcherApiClient(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {
                "row_count": 10000,
                "columns": [
                    {"name": "id", "distinct_count": 10000, "null_count": 0},
                    {"name": "name", "distinct_count": 9500, "null_count": 50}
                ]
            }

            result = client.get_table_statistics(
                connection_id="conn-1",
                database="DB_A",
                schema="SCHEMA_A",
                table="TABLE_A"
            )

            assert "row_count" in result

    def test_mcp_data_matcher_tools_registration(self):
        """Test that data matcher MCP tools can be registered."""
        from data_matcher.mcp_tools import register_data_matcher_tools

        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        client = register_data_matcher_tools(
            mock_mcp,
            "http://localhost:3001/api",
            "test-key"
        )

        assert mock_mcp.tool.called


# =============================================================================
# Enhanced Hierarchy API Sync Tests
# =============================================================================

class TestEnhancedHierarchyApiSync:
    """Tests for enhanced Hierarchy API Sync methods."""

    def test_api_sync_has_dashboard_methods(self):
        """Test that HierarchyApiSync has new dashboard methods."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert hasattr(sync, 'get_dashboard_stats')
        assert hasattr(sync, 'get_dashboard_activities')
        assert callable(sync.get_dashboard_stats)
        assert callable(sync.get_dashboard_activities)

    def test_api_sync_has_deployment_methods(self):
        """Test that HierarchyApiSync has deployment methods."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert hasattr(sync, 'generate_deployment_scripts')
        assert hasattr(sync, 'push_to_snowflake')
        assert hasattr(sync, 'get_deployment_history')

    def test_api_sync_has_filter_methods(self):
        """Test that HierarchyApiSync has filter group methods."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert hasattr(sync, 'create_filter_group')
        assert hasattr(sync, 'list_filter_groups')

    def test_api_sync_has_search_method(self):
        """Test that HierarchyApiSync has search method."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert hasattr(sync, 'search_hierarchies')

    def test_get_dashboard_stats_mock(self):
        """Test get_dashboard_stats with mocked response."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(sync, '_request') as mock_request:
            mock_request.return_value = {
                "projects": 5,
                "hierarchies": 50,
                "deployments": 10
            }

            result = sync.get_dashboard_stats()

            assert "projects" in result
            mock_request.assert_called_once()


# =============================================================================
# Enhanced Hierarchy MCP Tools Tests
# =============================================================================

class TestEnhancedHierarchyTools:
    """Tests for enhanced Hierarchy MCP tools."""

    def test_hierarchy_api_sync_module_exists(self):
        """Test that hierarchy api_sync module has required methods."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        # Verify new methods exist
        assert hasattr(sync, 'get_dashboard_stats')
        assert hasattr(sync, 'get_dashboard_activities')
        assert hasattr(sync, 'generate_deployment_scripts')
        assert hasattr(sync, 'push_to_snowflake')
        assert hasattr(sync, 'get_deployment_history')
        assert hasattr(sync, 'export_hierarchy_csv_backend')
        assert hasattr(sync, 'import_hierarchy_csv_backend')
        assert hasattr(sync, 'create_filter_group')
        assert hasattr(sync, 'list_filter_groups')
        assert hasattr(sync, 'search_hierarchies')

    def test_hierarchy_api_sync_dashboard_activities_mock(self):
        """Test get_dashboard_activities with mocked response."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        with patch.object(sync, '_request') as mock_request:
            mock_request.return_value = {
                "data": [
                    {"action": "created", "timestamp": "2024-01-01"},
                    {"action": "updated", "timestamp": "2024-01-02"}
                ]
            }

            activities = sync.get_dashboard_activities(limit=10)

            assert len(activities) == 2
            mock_request.assert_called_once()


# =============================================================================
# Server Integration Tests
# =============================================================================

class TestServerIntegration:
    """Tests for server module registration."""

    def test_server_imports_new_modules(self):
        """Test that server can import new modules."""
        # These should not raise ImportError
        try:
            from connections.mcp_tools import register_connection_tools
            from schema_matcher.mcp_tools import register_schema_matcher_tools
            from data_matcher.mcp_tools import register_data_matcher_tools
        except ImportError as e:
            pytest.fail(f"Failed to import module: {e}")

    def test_server_module_structure(self):
        """Test that all new modules have correct structure."""
        from connections import ConnectionsApiClient, register_connection_tools
        from schema_matcher import SchemaMatcherApiClient, register_schema_matcher_tools
        from data_matcher import DataMatcherApiClient, register_data_matcher_tools

        # All modules should export their API client and registration function
        assert ConnectionsApiClient is not None
        assert register_connection_tools is not None
        assert SchemaMatcherApiClient is not None
        assert register_schema_matcher_tools is not None
        assert DataMatcherApiClient is not None
        assert register_data_matcher_tools is not None


# =============================================================================
# Tool JSON Response Tests
# =============================================================================

class TestToolJsonResponses:
    """Tests for tool JSON response formatting."""

    def test_connections_tool_returns_json(self):
        """Test that connection tools return valid JSON."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:9999/api",
            api_key="test-key",
            timeout=1
        )

        # Even on error, should return empty list (not raise exception)
        result = client.list_connections()
        assert isinstance(result, list)

    def test_error_response_format(self):
        """Test error response format."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:9999/api",
            api_key="test-key",
            timeout=1
        )

        # Test connection returns error dict
        result = client.test_connection("non-existent-id")
        assert "error" in result


# =============================================================================
# Auto-Sync Tests
# =============================================================================

class TestAutoSync:
    """Tests for the automatic synchronization feature."""

    def test_auto_sync_manager_exists(self):
        """Test that AutoSyncManager class exists."""
        from hierarchy.api_sync import AutoSyncManager

        # Should be importable
        assert AutoSyncManager is not None

    def test_hierarchy_api_sync_has_auto_sync(self):
        """Test that HierarchyApiSync has auto-sync attributes."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key",
            auto_sync=True
        )

        assert hasattr(sync, 'auto_sync_enabled')
        assert hasattr(sync, 'auto_sync_manager')
        assert hasattr(sync, 'enable_auto_sync')
        assert hasattr(sync, 'disable_auto_sync')
        assert hasattr(sync, 'get_sync_status')

    def test_auto_sync_enabled_by_default(self):
        """Test that auto-sync is enabled by default."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key"
        )

        assert sync.auto_sync_enabled is True
        assert sync.auto_sync_manager is not None

    def test_auto_sync_can_be_disabled(self):
        """Test that auto-sync can be disabled."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key",
            auto_sync=False
        )

        assert sync.auto_sync_enabled is False

    def test_get_sync_status(self):
        """Test get_sync_status returns proper format."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key",
            auto_sync=True
        )

        status = sync.get_sync_status()

        assert "auto_sync_enabled" in status
        assert "sync_mode" in status
        assert "backend_url" in status
        assert "description" in status
        assert status["sync_mode"] in ["automatic", "manual"]

    def test_health_check_includes_sync_status(self):
        """Test that health_check includes auto-sync information."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="test-key",
            auto_sync=True
        )

        with patch.object(sync, '_request') as mock_request:
            mock_request.return_value = {"status": "ok"}

            result = sync.health_check()

            assert "auto_sync_enabled" in result
            assert "sync_mode" in result

    def test_configure_auto_sync_tool_exists(self):
        """Test that configure_auto_sync tool is registered."""
        from src.server import mcp

        # Get tool names
        tool_names = [tool.name for tool in mcp._tool_manager._tools.values()]

        assert "configure_auto_sync" in tool_names


# =============================================================================
# Backend Integration Tests (require running backend)
# =============================================================================

@pytest.mark.backend
class TestBackendIntegration:
    """Integration tests that require NestJS backend running at localhost:3001.

    These tests are skipped by default. Run with: pytest -m backend
    """

    def test_backend_health_check(self):
        """Test connection to NestJS backend."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="dev-key-1"
        )

        result = sync.health_check()
        assert result["connected"] is True

    def test_list_connections_from_backend(self):
        """Test listing connections from live backend."""
        from connections.api_client import ConnectionsApiClient

        client = ConnectionsApiClient(
            base_url="http://localhost:3001/api",
            api_key="dev-key-1"
        )

        connections = client.list_connections()
        # Should return list (may be empty if no connections configured)
        assert isinstance(connections, list)

    def test_list_projects_from_backend(self):
        """Test listing projects from live backend."""
        from hierarchy.api_sync import HierarchyApiSync

        sync = HierarchyApiSync(
            base_url="http://localhost:3001/api",
            api_key="dev-key-1"
        )

        projects = sync.list_projects()
        assert isinstance(projects, list)


# =============================================================================
# Pytest Configuration
# =============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "backend: marks tests as requiring backend (skip with -m 'not backend')"
    )
