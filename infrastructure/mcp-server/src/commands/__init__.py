"""
Slash Commands Module for DataBridge AI MCP Server.

This module provides slash command functionality that integrates with the frontend.
"""
from .slash_commands import register_slash_command_tools, SLASH_COMMANDS, TEMPLATES

__all__ = ["register_slash_command_tools", "SLASH_COMMANDS", "TEMPLATES"]
