"""
Tests for Librarian Hierarchy Client.

Tests the integration client for connecting to Librarian Hierarchy Builder.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.integration.librarian_client import (
    LibrarianHierarchyClient,
    LibrarianConnectionMode,
    LibrarianProject,
    LibrarianHierarchy,
    LibrarianMapping,
    LibrarianClientResult,
)


class TestLibrarianProject:
    """Tests for LibrarianProject dataclass."""

    def test_librarian_project_creation(self):
        """Test creating a Librarian project."""
        project = LibrarianProject(
            id="test-123",
            name="Test Project",
            description="A test project",
        )

        assert project.id == "test-123"
        assert project.name == "Test Project"
        assert project.description == "A test project"

    def test_librarian_project_to_dict(self):
        """Test Librarian project to dictionary conversion."""
        project = LibrarianProject(
            id="test-123",
            name="Test Project",
            description="Test description",
            created_at="2024-01-01T12:00:00",
        )

        result = project.to_dict()

        assert result["id"] == "test-123"
        assert result["name"] == "Test Project"
        assert result["description"] == "Test description"
        assert "created_at" in result

    def test_librarian_project_defaults(self):
        """Test Librarian project default values."""
        project = LibrarianProject(
            id="p1",
            name="P1",
        )

        assert project.description == ""
        assert project.hierarchy_count == 0
        assert project.client_name == ""


class TestLibrarianHierarchy:
    """Tests for LibrarianHierarchy dataclass."""

    def test_librarian_hierarchy_creation(self):
        """Test creating a Librarian hierarchy."""
        hierarchy = LibrarianHierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Test Hierarchy",
            levels={"level_1": "Root", "level_2": "Branch"},
        )

        assert hierarchy.hierarchy_id == "h-123"
        assert hierarchy.project_id == "p-123"
        assert hierarchy.hierarchy_name == "Test Hierarchy"
        assert hierarchy.levels["level_1"] == "Root"

    def test_librarian_hierarchy_get_depth(self):
        """Test getting hierarchy depth."""
        hierarchy = LibrarianHierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Test",
            levels={
                "level_1": "A",
                "level_2": "B",
                "level_3": "C",
            },
        )

        assert hierarchy.get_depth() == 3

    def test_librarian_hierarchy_get_depth_empty(self):
        """Test getting depth with empty levels."""
        hierarchy = LibrarianHierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Test",
            levels={},
        )

        assert hierarchy.get_depth() == 0

    def test_librarian_hierarchy_to_dict(self):
        """Test hierarchy to dictionary conversion."""
        hierarchy = LibrarianHierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Test",
            parent_id="h-parent",
            levels={"level_1": "Root"},
            sort_order=5,
        )

        result = hierarchy.to_dict()

        assert result["hierarchy_id"] == "h-123"
        assert result["parent_id"] == "h-parent"
        assert result["sort_order"] == 5

    def test_librarian_hierarchy_get_level_path(self):
        """Test getting level path."""
        hierarchy = LibrarianHierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Test",
            levels={"level_1": "Root", "level_2": "Child", "level_3": "Leaf"},
        )

        path = hierarchy.get_level_path()

        assert path == "Root > Child > Leaf"

    def test_librarian_hierarchy_get_level_path_custom_separator(self):
        """Test getting level path with custom separator."""
        hierarchy = LibrarianHierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Test",
            levels={"level_1": "A", "level_2": "B"},
        )

        path = hierarchy.get_level_path(" / ")

        assert path == "A / B"


class TestLibrarianMapping:
    """Tests for LibrarianMapping dataclass."""

    def test_librarian_mapping_creation(self):
        """Test creating a Librarian mapping."""
        mapping = LibrarianMapping(
            hierarchy_id="h-123",
            mapping_index=0,
            source_database="prod_db",
            source_schema="finance",
            source_table="gl_accounts",
            source_column="account_id",
        )

        assert mapping.hierarchy_id == "h-123"
        assert mapping.mapping_index == 0
        assert mapping.source_database == "prod_db"

    def test_librarian_mapping_get_full_path(self):
        """Test getting full source path."""
        mapping = LibrarianMapping(
            hierarchy_id="h-123",
            mapping_index=0,
            source_database="prod_db",
            source_schema="finance",
            source_table="gl_accounts",
            source_column="account_id",
        )

        path = mapping.get_full_path()

        assert path == "prod_db.finance.gl_accounts.account_id"

    def test_librarian_mapping_to_dict(self):
        """Test mapping to dictionary conversion."""
        mapping = LibrarianMapping(
            hierarchy_id="h-123",
            mapping_index=1,
            source_database="db",
            source_schema="schema",
            source_table="table",
            source_column="col",
            precedence_group=2,
        )

        result = mapping.to_dict()

        assert result["hierarchy_id"] == "h-123"
        assert result["mapping_index"] == 1
        assert result["precedence_group"] == 2


class TestLibrarianClientResult:
    """Tests for LibrarianClientResult dataclass."""

    def test_result_success(self):
        """Test successful result."""
        result = LibrarianClientResult(
            success=True,
            message="Operation completed",
        )

        assert result.success is True
        assert result.message == "Operation completed"
        assert result.errors == []

    def test_result_failure(self):
        """Test failed result."""
        result = LibrarianClientResult(
            success=False,
            message="Operation failed",
            errors=["Error 1", "Error 2"],
        )

        assert result.success is False
        assert len(result.errors) == 2

    def test_result_to_dict(self):
        """Test result to dictionary conversion."""
        result = LibrarianClientResult(
            success=True,
            message="Done",
            data={"key": "value"},
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["data"]["key"] == "value"

    def test_result_with_data(self):
        """Test result with data."""
        project_data = {"id": "p1", "name": "Test"}
        result = LibrarianClientResult(
            success=True,
            data=project_data,
        )

        assert result.data["id"] == "p1"


class TestLibrarianHierarchyClient:
    """Tests for LibrarianHierarchyClient."""

    def test_client_initialization_http_mode(self):
        """Test client initialization in HTTP mode."""
        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        assert client.mode == LibrarianConnectionMode.HTTP
        assert client.base_url == "http://localhost:8000"

    def test_client_initialization_direct_mode(self):
        """Test client initialization in direct mode."""
        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.DIRECT,
        )

        assert client.mode == LibrarianConnectionMode.DIRECT

    def test_client_defaults(self):
        """Test client default values."""
        client = LibrarianHierarchyClient()

        assert client.base_url == "http://localhost:8000"
        assert client.mode == LibrarianConnectionMode.HTTP
        assert client.timeout == 30.0
        assert client.cache_enabled is True

    @patch("src.integration.librarian_client.httpx.Client")
    def test_list_projects_http(self, mock_client_class):
        """Test listing projects via HTTP."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        # Implementation expects {"projects": [...]} format
        mock_response.json.return_value = {
            "projects": [
                {"id": "p1", "name": "Project 1"},
                {"id": "p2", "name": "Project 2"},
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.list_projects()

        assert result.success is True
        assert len(result.data) == 2

    @patch("src.integration.librarian_client.httpx.Client")
    def test_list_projects_http_error(self, mock_client_class):
        """Test handling HTTP error when listing projects."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get.side_effect = Exception("Connection refused")

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.list_projects()

        assert result.success is False
        assert len(result.errors) > 0

    @patch("src.integration.librarian_client.httpx.Client")
    def test_get_project_http(self, mock_client_class):
        """Test getting a project via HTTP."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "p1",
            "name": "Project 1",
            "description": "Test project",
        }
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.get_project("p1")

        assert result.success is True
        assert result.data["id"] == "p1"
        assert result.data["name"] == "Project 1"

    @patch("src.integration.librarian_client.httpx.Client")
    def test_list_hierarchies_http(self, mock_client_class):
        """Test listing hierarchies via HTTP."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        # Implementation expects {"hierarchies": [...]} format
        mock_response.json.return_value = {
            "hierarchies": [
                {"hierarchy_id": "h1", "hierarchy_name": "H1", "project_id": "p1"},
                {"hierarchy_id": "h2", "hierarchy_name": "H2", "project_id": "p1"},
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.list_hierarchies("p1")

        assert result.success is True
        assert len(result.data) == 2

    @patch("src.integration.librarian_client.httpx.Client")
    def test_get_hierarchy_http(self, mock_client_class):
        """Test getting a hierarchy via HTTP."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        mock_response.json.return_value = {
            "hierarchy_id": "h1",
            "hierarchy_name": "Hierarchy 1",
            "project_id": "p1",
            "level_1": "Root",
        }
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.get_hierarchy("h1")

        assert result.success is True
        assert result.data["hierarchy_id"] == "h1"

    @patch("src.integration.librarian_client.httpx.Client")
    def test_get_hierarchy_tree_http(self, mock_client_class):
        """Test getting hierarchy tree via HTTP."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        mock_response.json.return_value = {
            "tree": [
                {"hierarchy_id": "h1", "children": [{"hierarchy_id": "h2"}]},
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.get_hierarchy_tree("p1")

        assert result.success is True

    @patch("src.integration.librarian_client.httpx.Client")
    def test_get_mappings_http(self, mock_client_class):
        """Test getting mappings via HTTP."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        # Implementation expects {"mappings": [...]} format
        mock_response.json.return_value = {
            "mappings": [
                {
                    "hierarchy_id": "h1",
                    "mapping_index": 0,
                    "source_database": "db",
                    "source_schema": "schema",
                    "source_table": "table",
                    "source_column": "col",
                },
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.get_mappings("h1")

        assert result.success is True
        assert len(result.data) == 1

    def test_caching_enabled(self):
        """Test that caching is enabled by default."""
        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
            cache_enabled=True,
        )

        assert client.cache_enabled is True

    @patch("src.integration.librarian_client.httpx.Client")
    def test_response_caching(self, mock_client_class):
        """Test that responses are cached."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        mock_response.json.return_value = {"id": "p1", "name": "Project 1"}
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
            cache_enabled=True,
        )

        # First call
        result1 = client.get_project("p1")
        # Second call - should use cache
        result2 = client.get_project("p1")

        # Both should succeed
        assert result1.success is True
        assert result2.success is True

        # HTTP should only be called once due to caching
        assert mock_client.get.call_count == 1

    @patch("src.integration.librarian_client.httpx.Client")
    def test_connection_error_handling(self, mock_client_class):
        """Test handling connection errors."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get.side_effect = Exception("Connection refused")

        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.HTTP,
            base_url="http://localhost:8000",
        )

        result = client.list_projects()

        assert result.success is False
        assert "Connection refused" in str(result.errors)

    def test_clear_cache(self):
        """Test clearing the cache."""
        client = LibrarianHierarchyClient()
        client._cache["test_key"] = "test_value"

        client.clear_cache()

        assert len(client._cache) == 0

    def test_cache_key_generation(self):
        """Test cache key generation."""
        client = LibrarianHierarchyClient()

        key = client._cache_key("operation", param1="value1", param2="value2")

        assert "operation" in key
        assert "param1=value1" in key
        assert "param2=value2" in key


class TestLibrarianHierarchyClientDirectMode:
    """Tests for LibrarianHierarchyClient in direct mode."""

    def test_direct_mode_returns_error(self):
        """Test that direct mode methods return appropriate error messages."""
        client = LibrarianHierarchyClient(
            mode=LibrarianConnectionMode.DIRECT,
        )

        # Direct mode should return error since it's not implemented
        result = client.list_projects()

        assert result.success is False
        assert "Direct mode not implemented" in result.errors[0]

    def test_direct_mode_get_project(self):
        """Test get_project in direct mode."""
        client = LibrarianHierarchyClient(mode=LibrarianConnectionMode.DIRECT)

        result = client.get_project("p1")

        assert result.success is False

    def test_direct_mode_list_hierarchies(self):
        """Test list_hierarchies in direct mode."""
        client = LibrarianHierarchyClient(mode=LibrarianConnectionMode.DIRECT)

        result = client.list_hierarchies("p1")

        assert result.success is False
