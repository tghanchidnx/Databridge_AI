"""MCP (Model Context Protocol) server module with 92 tools."""

from .server import mcp, register_all_tools

__all__ = [
    "mcp",
    "register_all_tools",
]
