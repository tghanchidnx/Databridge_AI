"""
MCP Server for DataBridge AI V3 - Hierarchy Builder.

This server provides 92+ MCP tools across major modules:
- Data Reconciliation (20 tools)
- Hierarchy Knowledge Base (20 tools)
- Vector Store & RAG (16 tools)
- Templates, Skills & Knowledge Base (additional tools)

Current implementation:
- Phase 2: Core hierarchy tools (20 tools)
- Phase 3: Reconciliation tools (20 tools)
- Phase 5: Vector and RAG tools (16 tools)
"""

from fastmcp import FastMCP

from .tools.project import register_project_tools
from .tools.hierarchy import register_hierarchy_tools
from .tools.reconciliation import register_reconciliation_tools
from .tools.vectors import register_vector_tools

# Create the MCP server
mcp = FastMCP("databridge-v3")


def register_all_tools() -> None:
    """Register all MCP tools."""
    # Phase 2: Core hierarchy tools (20 tools)
    register_project_tools(mcp)
    register_hierarchy_tools(mcp)

    # Phase 3: Reconciliation tools (20 tools)
    register_reconciliation_tools(mcp)

    # Phase 5: Vector store and RAG tools (16 tools)
    register_vector_tools(mcp)


# Register tools on module import
register_all_tools()


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
