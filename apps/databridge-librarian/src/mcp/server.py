"""
MCP Server for DataBridge AI Librarian - Hierarchy Builder.

This server provides 100+ MCP tools across major modules:
- Data Reconciliation (20 tools)
- Hierarchy Knowledge Base (20 tools)
- Vector Store & RAG (16 tools)
- Document Extraction - PDF/OCR (8 tools)
- Source Intelligence (22 tools)
- DDL/dbt Generation (7 tools)
- Deployment & Execution (6 tools)
- Health & Readiness (6 tools)
- Templates, Skills & Knowledge Base (additional tools)

Current implementation:
- Phase 1: Source Intelligence (extraction + source tools)
- Phase 2: Core hierarchy tools + DDL/dbt generation
- Phase 3: Deployment & Execution Engine (6 tools)
- Phase 3b: Reconciliation tools (20 tools)
- Phase 4: Health & Service Integration (6 tools)
- Phase 5: Vector and RAG tools (16 tools)
"""

from fastmcp import FastMCP

from .tools.project import register_project_tools
from .tools.hierarchy import register_hierarchy_tools
from .tools.reconciliation import register_reconciliation_tools
from .tools.vectors import register_vector_tools
from .tools.extraction import register_extraction_tools
from .tools.source import register_source_tools
from .tools.generation import register_generation_tools
from .tools.deployment import register_deployment_tools
from .tools.health import register_health_tools

# Create the MCP server
mcp = FastMCP("databridge-librarian")


def register_all_tools() -> None:
    """Register all MCP tools."""
    # Phase 1: Source Intelligence
    register_extraction_tools(mcp)  # PDF/OCR extraction (8 tools)
    register_source_tools(mcp)  # Source model management (22 tools)

    # Phase 2: Core hierarchy tools + Generation
    register_project_tools(mcp)
    register_hierarchy_tools(mcp)
    register_generation_tools(mcp)  # DDL/dbt generation (6 tools)

    # Phase 3: Deployment & Execution Engine (6 tools)
    register_deployment_tools(mcp)

    # Phase 3b: Reconciliation tools (20 tools)
    register_reconciliation_tools(mcp)

    # Phase 4: Health & Service Integration (6 tools)
    register_health_tools(mcp)

    # Phase 5: Vector store and RAG tools (16 tools)
    register_vector_tools(mcp)


# Register tools on module import
register_all_tools()


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
