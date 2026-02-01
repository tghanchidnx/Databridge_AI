"""
MCP Server for DataBridge AI V4 - Analytics Engine.

This server provides 37 MCP tools for:
- Query Building (10 tools)
- Insights and Analysis (8 tools)
- Knowledge Base (7 tools)
- FP&A Workflows (12 tools)
"""

from fastmcp import FastMCP

from .tools.query import register_query_tools
from .tools.insights import register_insights_tools
from .tools.knowledgebase import register_knowledgebase_tools
from .tools.fpa import register_fpa_tools

# Create the MCP server
mcp = FastMCP("databridge-analytics-v4")


def register_all_tools() -> None:
    """Register all MCP tools."""
    # Query tools (10 tools)
    register_query_tools(mcp)

    # Insights tools (8 tools)
    register_insights_tools(mcp)

    # Knowledge Base tools (7 tools)
    register_knowledgebase_tools(mcp)

    # FP&A tools (12 tools)
    register_fpa_tools(mcp)


# Register tools on module import
register_all_tools()


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
