"""Data Catalog - Metadata registry with automatic lineage for DataBridge AI Pro."""
from typing import Any


def register_catalog_tools(mcp_instance: Any) -> None:
    """Register Data Catalog tools with the MCP server."""
    try:
        from src.data_catalog.mcp_tools import register_catalog_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def catalog_scan_connection(connection_id: str) -> str:
            """[Pro] Scan a database connection for metadata.

            Args:
                connection_id: Connection identifier

            Returns:
                Scan results
            """
            return '{"error": "Catalog tools require full Pro installation"}'

        @mcp_instance.tool()
        def catalog_search(query: str) -> str:
            """[Pro] Search the data catalog.

            Args:
                query: Search query

            Returns:
                Matching catalog entries
            """
            return '{"error": "Catalog tools require full Pro installation"}'

        @mcp_instance.tool()
        def catalog_auto_lineage_from_sql(sql: str) -> str:
            """[Pro] Extract lineage from SQL statement.

            Args:
                sql: SQL statement to analyze

            Returns:
                Lineage graph
            """
            return '{"error": "Catalog tools require full Pro installation"}'


__all__ = ['register_catalog_tools']
