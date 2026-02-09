"""Advanced Hierarchy Management for DataBridge AI Pro.

Extended hierarchy features including flexible import, enterprise formats,
and Snowflake deployment.
"""
from typing import Any


def register_hierarchy_tools(mcp_instance: Any) -> None:
    """Register Advanced Hierarchy tools with the MCP server."""
    try:
        from src.hierarchy.mcp_tools import register_hierarchy_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def import_flexible_hierarchy(file_path: str, format: str = "auto") -> str:
            """[Pro] Import hierarchy from flexible format.

            Args:
                file_path: Path to hierarchy file
                format: Format (auto, csv, json, excel)

            Returns:
                Import result
            """
            return '{"error": "Advanced Hierarchy tools require full Pro installation"}'

        @mcp_instance.tool()
        def push_hierarchy_to_snowflake(hierarchy_id: str, connection: str) -> str:
            """[Pro] Deploy hierarchy to Snowflake.

            Args:
                hierarchy_id: Hierarchy identifier
                connection: Snowflake connection ID

            Returns:
                Deployment result
            """
            return '{"error": "Advanced Hierarchy tools require full Pro installation"}'


__all__ = ['register_hierarchy_tools']
