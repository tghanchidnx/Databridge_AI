"""Wright Pipeline - 4-object data mart factory for DataBridge AI Pro.

This module provides automated data mart generation with:
- Source staging
- Dimension tables
- Fact tables
- Mart aggregation views
"""
from typing import Any


def register_wright_tools(mcp_instance: Any) -> None:
    """Register Wright Pipeline tools with the MCP server.

    Note: This is a placeholder. The actual implementation should copy
    the tools from src/wright/mcp_tools.py in the main repository.
    """
    try:
        from src.wright.mcp_tools import register_wright_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def create_mart_config(name: str, source_table: str, grain: str) -> str:
            """[Pro] Create a data mart configuration.

            Args:
                name: Mart name
                source_table: Source table reference
                grain: Grain columns (comma-separated)

            Returns:
                Mart configuration
            """
            return '{"error": "Wright tools require full Pro installation"}'

        @mcp_instance.tool()
        def generate_mart_pipeline(mart_name: str) -> str:
            """[Pro] Generate complete mart pipeline.

            Args:
                mart_name: Name of the mart to generate

            Returns:
                Generated pipeline details
            """
            return '{"error": "Wright tools require full Pro installation"}'


__all__ = ['register_wright_tools']
