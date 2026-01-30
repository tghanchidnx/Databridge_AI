"""
Unit tests for MCP tools.

These tests use the actual database without the transaction fixture since
MCP tools create their own sessions internally.
"""

import pytest
import uuid
import os

# Set test database and disable audit before imports
os.environ["DATABRIDGE_DATABASE_URL"] = "sqlite:///./test_mcp_tools.db"
os.environ["DATABRIDGE_DISABLE_AUDIT"] = "true"


class TestProjectTools:
    """Tests for project MCP tools."""

    @pytest.fixture(autouse=True)
    def setup_database(self):
        """Setup and cleanup test database."""
        from src.core.database import init_database, reset_database
        reset_database()
        yield
        # Cleanup handled by reset on next run

    def test_create_and_get_project(self):
        """Test creating and getting a project through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.project import register_project_tools

        mcp = FastMCP("test")
        register_project_tools(mcp)
        tools = mcp._tool_manager._tools

        # Test create
        create_tool = tools["create_hierarchy_project"]
        unique_name = f"Test Project {uuid.uuid4().hex[:8]}"
        result = create_tool.fn(
            name=unique_name,
            description="Test description",
            industry="Manufacturing",
        )

        assert result["success"] is True
        assert result["project"]["name"] == unique_name
        project_id = result["project"]["id"]

        # Test get
        get_tool = tools["get_hierarchy_project"]
        result = get_tool.fn(project_id=project_id[:8])

        assert result["success"] is True
        assert result["project"]["name"] == unique_name

    def test_list_projects(self):
        """Test listing projects through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.project import register_project_tools

        mcp = FastMCP("test")
        register_project_tools(mcp)
        tools = mcp._tool_manager._tools

        # Create some projects
        create_tool = tools["create_hierarchy_project"]
        unique = uuid.uuid4().hex[:8]
        create_tool.fn(name=f"List Test 1 {unique}")
        create_tool.fn(name=f"List Test 2 {unique}")

        # Test list
        list_tool = tools["list_hierarchy_projects"]
        result = list_tool.fn(search=unique)

        assert result["success"] is True
        assert result["count"] >= 2

    def test_update_project(self):
        """Test updating a project through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.project import register_project_tools

        mcp = FastMCP("test")
        register_project_tools(mcp)
        tools = mcp._tool_manager._tools

        # Create project
        create_tool = tools["create_hierarchy_project"]
        unique_name = f"Update Test {uuid.uuid4().hex[:8]}"
        result = create_tool.fn(name=unique_name, industry="Manufacturing")
        project_id = result["project"]["id"]

        # Update project
        update_tool = tools["update_hierarchy_project"]
        result = update_tool.fn(
            project_id=project_id,
            industry="Retail",
        )

        assert result["success"] is True
        assert result["project"]["industry"] == "Retail"

    def test_delete_project(self):
        """Test deleting a project through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.project import register_project_tools

        mcp = FastMCP("test")
        register_project_tools(mcp)
        tools = mcp._tool_manager._tools

        # Create project
        create_tool = tools["create_hierarchy_project"]
        unique_name = f"Delete Test {uuid.uuid4().hex[:8]}"
        result = create_tool.fn(name=unique_name)
        project_id = result["project"]["id"]

        # Delete project
        delete_tool = tools["delete_hierarchy_project"]
        result = delete_tool.fn(project_id=project_id)

        assert result["success"] is True

        # Verify deleted
        get_tool = tools["get_hierarchy_project"]
        result = get_tool.fn(project_id=project_id)
        assert result["success"] is False


class TestHierarchyTools:
    """Tests for hierarchy MCP tools."""

    @pytest.fixture(autouse=True)
    def setup_database(self):
        """Setup and cleanup test database."""
        from src.core.database import init_database, reset_database
        reset_database()
        yield

    @pytest.fixture
    def test_project(self):
        """Create a test project."""
        from fastmcp import FastMCP
        from src.mcp.tools.project import register_project_tools

        mcp = FastMCP("test")
        register_project_tools(mcp)
        tools = mcp._tool_manager._tools
        create_tool = tools["create_hierarchy_project"]
        result = create_tool.fn(name=f"Hierarchy Test {uuid.uuid4().hex[:8]}")
        return result["project"]

    def test_create_hierarchy(self, test_project):
        """Test creating a hierarchy through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.hierarchy import register_hierarchy_tools

        mcp = FastMCP("test")
        register_hierarchy_tools(mcp)
        tools = mcp._tool_manager._tools

        create_tool = tools["create_hierarchy"]
        result = create_tool.fn(
            project_id=test_project["id"],
            hierarchy_id=f"HIER-{uuid.uuid4().hex[:8]}",
            hierarchy_name="Test Hierarchy",
            levels={"level_1": "Revenue", "level_2": "Product"},
        )

        assert result["success"] is True
        assert result["hierarchy"]["hierarchy_name"] == "Test Hierarchy"
        assert result["hierarchy"]["level_1"] == "Revenue"

    def test_get_hierarchy_tree(self, test_project):
        """Test getting hierarchy tree through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.hierarchy import register_hierarchy_tools

        mcp = FastMCP("test")
        register_hierarchy_tools(mcp)
        tools = mcp._tool_manager._tools

        # Create hierarchies
        create_tool = tools["create_hierarchy"]
        unique = uuid.uuid4().hex[:8]
        root_result = create_tool.fn(
            project_id=test_project["id"],
            hierarchy_id=f"ROOT-{unique}",
            hierarchy_name="Root",
        )
        create_tool.fn(
            project_id=test_project["id"],
            hierarchy_id=f"CHILD-{unique}",
            hierarchy_name="Child",
            parent_id=root_result["hierarchy"]["hierarchy_id"],
        )

        tree_tool = tools["get_hierarchy_tree"]
        result = tree_tool.fn(project_id=test_project["id"])

        assert result["success"] is True
        assert result["stats"]["total_nodes"] >= 2

    def test_add_source_mapping(self, test_project):
        """Test adding source mapping through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.hierarchy import register_hierarchy_tools

        mcp = FastMCP("test")
        register_hierarchy_tools(mcp)
        tools = mcp._tool_manager._tools

        # Create hierarchy
        create_tool = tools["create_hierarchy"]
        hier_id = f"MAP-{uuid.uuid4().hex[:8]}"
        create_tool.fn(
            project_id=test_project["id"],
            hierarchy_id=hier_id,
            hierarchy_name="Mapping Test",
        )

        add_mapping_tool = tools["add_source_mapping"]
        result = add_mapping_tool.fn(
            hierarchy_id=hier_id,
            source_database="ANALYTICS",
            source_schema="PUBLIC",
            source_table="FACT_SALES",
            source_column="AMOUNT",
        )

        assert result["success"] is True
        assert "ANALYTICS" in result["mapping"]["source_path"]

    def test_create_formula_group(self, test_project):
        """Test creating formula group through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.hierarchy import register_hierarchy_tools

        mcp = FastMCP("test")
        register_hierarchy_tools(mcp)
        tools = mcp._tool_manager._tools

        create_group_tool = tools["create_formula_group"]
        result = create_group_tool.fn(
            project_id=test_project["id"],
            name="Revenue Calculations",
            description="Sum revenue components",
        )

        assert result["success"] is True
        assert result["formula_group"]["name"] == "Revenue Calculations"

    def test_export_hierarchy_csv(self, test_project):
        """Test exporting hierarchy to CSV through MCP tools."""
        from fastmcp import FastMCP
        from src.mcp.tools.hierarchy import register_hierarchy_tools

        mcp = FastMCP("test")
        register_hierarchy_tools(mcp)
        tools = mcp._tool_manager._tools

        # Create hierarchy
        create_tool = tools["create_hierarchy"]
        create_tool.fn(
            project_id=test_project["id"],
            hierarchy_id=f"EXP-{uuid.uuid4().hex[:8]}",
            hierarchy_name="Export Test",
            levels={"level_1": "Revenue"},
        )

        export_tool = tools["export_hierarchy_csv"]
        result = export_tool.fn(project_id=test_project["id"])

        assert result["success"] is True
        assert "HIERARCHY_ID" in result["csv_content"]
        assert result["rows_exported"] >= 1
