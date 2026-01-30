"""
CLI module for DataBridge Analytics V4.

Typer-based command line interface with commands:
- connection: Manage data warehouse connections
- catalog: Browse metadata catalog
- query: Execute SQL and NL queries
- insights: Generate analysis and reports
- workflow: Run FP&A workflows
- mcp: Start MCP server
"""

__all__ = [
    "app",
    "main_cli",
]
