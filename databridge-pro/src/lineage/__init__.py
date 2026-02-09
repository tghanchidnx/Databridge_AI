"""Column Lineage - Track data flow from source to destination for DataBridge AI Pro."""
from typing import Any


def register_lineage_tools(mcp_instance: Any) -> None:
    """Register Column Lineage tools with the MCP server."""
    try:
        from src.lineage.mcp_tools import register_lineage_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def track_column_lineage(table: str, column: str) -> str:
            """[Pro] Track lineage for a column.

            Args:
                table: Table name
                column: Column name

            Returns:
                Lineage chain
            """
            return '{"error": "Lineage tools require full Pro installation"}'

        @mcp_instance.tool()
        def analyze_change_impact(table: str, column: str) -> str:
            """[Pro] Analyze impact of changing a column.

            Args:
                table: Table name
                column: Column name

            Returns:
                Impact analysis
            """
            return '{"error": "Lineage tools require full Pro installation"}'


__all__ = ['register_lineage_tools']
