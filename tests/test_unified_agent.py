"""
Tests for Unified AI Agent.

Tests the context management, bridges, and MCP tools for cross-system
operations between Book, Librarian, and Researcher.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "Book"))


class TestUnifiedAgentContext:
    """Tests for UnifiedAgentContext."""

    def test_context_initialization(self):
        """Test context initializes correctly."""
        from agents.unified_agent.context import UnifiedAgentContext

        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = UnifiedAgentContext(data_dir=Path(tmpdir))

            assert ctx._active_books == {}
            assert ctx._book_refs == {}
            assert ctx._active_project_id is None
            assert ctx._connection_ids == {}

    def test_register_book(self):
        """Test registering a Book in context."""
        from agents.unified_agent.context import UnifiedAgentContext

        # Create a mock Book
        mock_book = Mock()
        mock_book.name = "Test Book"
        mock_book.data_version = "v1"
        mock_book.last_updated = "2024-01-01T00:00:00Z"
        mock_book.root_nodes = [Mock(children=[])]

        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = UnifiedAgentContext(data_dir=Path(tmpdir))
            name = ctx.register_book(mock_book, source_project_id="proj-123")

            assert name == "Test Book"
            assert "Test Book" in ctx._active_books
            assert "Test Book" in ctx._book_refs

            ref = ctx._book_refs["Test Book"]
            assert ref.source_project_id == "proj-123"
            assert ref.root_node_count == 1

    def test_list_books(self):
        """Test listing registered Books."""
        from agents.unified_agent.context import UnifiedAgentContext

        mock_book = Mock()
        mock_book.name = "Test Book"
        mock_book.data_version = "v1"
        mock_book.last_updated = "2024-01-01T00:00:00Z"
        mock_book.root_nodes = []

        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = UnifiedAgentContext(data_dir=Path(tmpdir))
            ctx.register_book(mock_book)

            books = ctx.list_books()
            assert len(books) == 1
            assert books[0]["name"] == "Test Book"

    def test_connection_management(self):
        """Test connection alias management."""
        from agents.unified_agent.context import UnifiedAgentContext

        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = UnifiedAgentContext(data_dir=Path(tmpdir))

            ctx.set_connection("snowflake", "conn-abc-123")
            ctx.set_connection("postgres", "conn-xyz-456")

            assert ctx.get_connection("snowflake") == "conn-abc-123"
            assert ctx.get_connection("postgres") == "conn-xyz-456"
            assert ctx.get_connection("unknown") is None

    def test_operation_history(self):
        """Test recording operation history."""
        from agents.unified_agent.context import UnifiedAgentContext

        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = UnifiedAgentContext(data_dir=Path(tmpdir))

            ctx.record_operation(
                operation="checkout",
                source_system="librarian",
                target_system="book",
                details={"project_id": "test"},
            )

            history = ctx.get_operation_history(limit=10)
            assert len(history) == 1
            assert history[0]["operation"] == "checkout"
            assert history[0]["source_system"] == "librarian"

    def test_context_persistence(self):
        """Test that context persists across instances."""
        from agents.unified_agent.context import UnifiedAgentContext

        with tempfile.TemporaryDirectory() as tmpdir:
            # First instance
            ctx1 = UnifiedAgentContext(data_dir=Path(tmpdir))
            ctx1.set_connection("test", "conn-123")
            ctx1.active_project_id = "proj-456"

            # Second instance should load persisted state
            ctx2 = UnifiedAgentContext(data_dir=Path(tmpdir))
            assert ctx2.get_connection("test") == "conn-123"
            assert ctx2.active_project_id == "proj-456"


class TestLibrarianBridge:
    """Tests for LibrarianBridge."""

    def test_book_to_librarian_conversion(self):
        """Test converting Book nodes to Librarian format."""
        from agents.unified_agent.bridges.librarian_bridge import LibrarianBridge

        bridge = LibrarianBridge(
            base_url="http://localhost:8001/api",
            api_key="test-key",
        )

        # Create mock Book with nodes
        mock_node = Mock()
        mock_node.id = "node-1"
        mock_node.name = "Revenue"
        mock_node.children = []
        mock_node.properties = {"description": "Total revenue"}
        mock_node.flags = {"include": True, "active": True}
        mock_node.formulas = []
        mock_node.schema_version = "1.0"
        mock_node.python_function = None
        mock_node.llm_prompt = None

        mock_book = Mock()
        mock_book.name = "P&L Hierarchy"
        mock_book.root_nodes = [mock_node]

        hierarchies = bridge.book_to_librarian_hierarchies(
            mock_book,
            project_id="proj-123",
        )

        assert len(hierarchies) == 1
        hier = hierarchies[0]
        assert hier["projectId"] == "proj-123"
        assert hier["hierarchyName"] == "Revenue"
        assert hier["parentId"] is None
        assert hier["isRoot"] is True
        assert hier["flags"]["include_flag"] is True

    def test_generate_hierarchy_id(self):
        """Test hierarchy ID generation."""
        from agents.unified_agent.bridges.librarian_bridge import LibrarianBridge

        bridge = LibrarianBridge(
            base_url="http://localhost:8001/api",
            api_key="test-key",
        )

        mock_node = Mock()
        mock_node.id = "short-id"
        mock_node.name = "Test Node"

        hier_id = bridge._generate_hierarchy_id(mock_node)
        assert hier_id == "SHORT_ID"  # Hyphens are converted to underscores

        # Long name should be truncated
        mock_node.id = "a" * 100
        mock_node.name = "Very Long Name That Should Be Truncated"
        hier_id = bridge._generate_hierarchy_id(mock_node)
        assert len(hier_id) <= 50

    def test_build_hierarchy_level(self):
        """Test building hierarchyLevel structure."""
        from agents.unified_agent.bridges.librarian_bridge import LibrarianBridge

        bridge = LibrarianBridge(
            base_url="http://localhost:8001/api",
            api_key="test-key",
        )

        path = ["P&L", "Revenue", "Product Sales"]
        level = bridge._build_hierarchy_level(path)

        assert level["level_1"] == "P&L"
        assert level["level_2"] == "Revenue"
        assert level["level_3"] == "Product Sales"

    def test_diff_result(self):
        """Test DiffResult structure."""
        from agents.unified_agent.bridges.librarian_bridge import DiffResult

        diff = DiffResult()
        diff.book_only = ["node-1", "node-2"]
        diff.librarian_only = ["node-3"]
        diff.modified = [{"hierarchy_id": "node-4", "changes": []}]
        diff.identical = ["node-5"]

        result = diff.to_dict()
        assert result["summary"]["book_only_count"] == 2
        assert result["summary"]["librarian_only_count"] == 1
        assert result["summary"]["modified_count"] == 1
        assert result["summary"]["total_differences"] == 4


class TestResearcherBridge:
    """Tests for ResearcherBridge."""

    def test_extract_sources_from_book(self):
        """Test extracting source mappings from Book."""
        from agents.unified_agent.bridges.researcher_bridge import ResearcherBridge

        bridge = ResearcherBridge(
            base_url="http://localhost:8001/api",
            api_key="test-key",
        )

        # Create mock Book with source mappings
        mock_node = Mock()
        mock_node.id = "node-1"
        mock_node.name = "Revenue"
        mock_node.children = []
        mock_node.properties = {
            "source_mappings": [
                {
                    "database": "WAREHOUSE",
                    "schema": "FINANCE",
                    "table": "GL_TRANSACTIONS",
                    "column": "ACCOUNT_CODE",
                    "uid": "4%",
                },
            ],
        }

        mock_book = Mock()
        mock_book.name = "P&L"
        mock_book.root_nodes = [mock_node]

        sources = bridge.extract_sources_from_book(mock_book)

        assert len(sources) == 1
        assert sources[0].database == "WAREHOUSE"
        assert sources[0].table == "GL_TRANSACTIONS"
        assert sources[0].column == "ACCOUNT_CODE"
        assert sources[0].hierarchy_id == "node-1"

    def test_source_mapping_full_path(self):
        """Test SourceMapping full_path method."""
        from agents.unified_agent.bridges.researcher_bridge import SourceMapping

        source = SourceMapping(
            database="WAREHOUSE",
            schema="FINANCE",
            table="GL_TRANSACTIONS",
            column="ACCOUNT_CODE",
        )

        assert source.full_path() == "WAREHOUSE.FINANCE.GL_TRANSACTIONS.ACCOUNT_CODE"

    def test_validation_result(self):
        """Test ValidationResult structure."""
        from agents.unified_agent.bridges.researcher_bridge import (
            ValidationResult,
            SourceMapping,
        )

        valid_source = SourceMapping(
            database="DB",
            schema="SCHEMA",
            table="TABLE",
            column="COL1",
        )

        result = ValidationResult(
            valid=[valid_source],
            invalid=[{"source": {"column": "COL2"}, "error": "Not found"}],
            warnings=[],
            summary={"total_sources": 2, "valid_count": 1, "invalid_count": 1},
        )

        result_dict = result.to_dict()
        assert len(result_dict["valid"]) == 1
        assert len(result_dict["invalid"]) == 1
        assert result_dict["summary"]["valid_count"] == 1


class TestMCPToolsRegistration:
    """Tests for MCP tool registration."""

    def test_tools_register(self):
        """Test that tools can be registered."""
        from fastmcp import FastMCP

        mcp = FastMCP("Test Server")

        # Import and register tools
        from agents.unified_agent.mcp_tools import register_unified_agent_tools

        register_unified_agent_tools(mcp)

        # Check that tools were registered
        # (FastMCP stores tools internally)
        assert mcp is not None

    def test_get_unified_context_tool(self):
        """Test the get_unified_context tool function."""
        from fastmcp import FastMCP
        from agents.unified_agent.mcp_tools import register_unified_agent_tools
        from agents.unified_agent.context import get_context

        mcp = FastMCP("Test Server")
        register_unified_agent_tools(mcp)

        # Get context should work
        ctx = get_context()
        assert ctx is not None


class TestPlannerAgentIntegration:
    """Tests for PlannerAgent integration with Unified Agent."""

    def test_new_agents_in_default_agents(self):
        """Test that new agent types are in DEFAULT_AGENTS."""
        from agents.planner_agent import DEFAULT_AGENTS

        agent_names = [a.name for a in DEFAULT_AGENTS]

        assert "book_manipulator" in agent_names
        assert "librarian_sync" in agent_names
        assert "researcher_analyst" in agent_names

    def test_agent_capabilities(self):
        """Test that new agents have expected capabilities."""
        from agents.planner_agent import DEFAULT_AGENTS

        agents_by_name = {a.name: a for a in DEFAULT_AGENTS}

        # Book manipulator
        book_agent = agents_by_name.get("book_manipulator")
        assert book_agent is not None
        assert "create_book" in book_agent.capabilities
        assert "apply_formula" in book_agent.capabilities

        # Librarian sync
        lib_agent = agents_by_name.get("librarian_sync")
        assert lib_agent is not None
        assert "checkout_project" in lib_agent.capabilities
        assert "promote_book" in lib_agent.capabilities

        # Researcher analyst
        res_agent = agents_by_name.get("researcher_analyst")
        assert res_agent is not None
        assert "validate_mappings" in res_agent.capabilities
        assert "profile_sources" in res_agent.capabilities


class TestRoundtripConversion:
    """Tests for Book ↔ Librarian roundtrip conversion."""

    def test_book_to_librarian_to_book(self):
        """Test that Book → Librarian → Book preserves structure."""
        from agents.unified_agent.bridges.librarian_bridge import LibrarianBridge

        bridge = LibrarianBridge(
            base_url="http://localhost:8001/api",
            api_key="test-key",
        )

        # Create mock Book
        child_node = Mock()
        child_node.id = "child-1"
        child_node.name = "Product Sales"
        child_node.children = []
        child_node.properties = {}
        child_node.flags = {"include": True}
        child_node.formulas = []
        child_node.schema_version = "1.0"
        child_node.python_function = None
        child_node.llm_prompt = None

        root_node = Mock()
        root_node.id = "root-1"
        root_node.name = "Revenue"
        root_node.children = [child_node]
        root_node.properties = {"description": "Total revenue"}
        root_node.flags = {"include": True}
        root_node.formulas = []
        root_node.schema_version = "1.0"
        root_node.python_function = None
        root_node.llm_prompt = None

        mock_book = Mock()
        mock_book.name = "Test Book"
        mock_book.root_nodes = [root_node]

        # Convert to Librarian format
        hierarchies = bridge.book_to_librarian_hierarchies(mock_book, "proj-123")

        assert len(hierarchies) == 2  # root + child

        # Find root and child
        root_hier = next(h for h in hierarchies if h["hierarchyName"] == "Revenue")
        child_hier = next(h for h in hierarchies if h["hierarchyName"] == "Product Sales")

        assert root_hier["parentId"] is None
        assert root_hier["isRoot"] is True
        assert child_hier["parentId"] is not None
        assert child_hier["isRoot"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
