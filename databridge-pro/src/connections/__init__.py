"""Backend Connections - Database connection management for DataBridge AI Pro."""
from typing import Any


def register_connections_tools(mcp_instance: Any) -> None:
    """Register Backend Connections tools with the MCP server."""
    try:
        from src.connections.mcp_tools import register_connections_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def list_backend_connections() -> str:
            """[Pro] List all configured backend connections.

            Returns:
                List of connections
            """
            return '{"error": "Connection tools require full Pro installation"}'

        @mcp_instance.tool()
        def test_backend_connection(connection_id: str) -> str:
            """[Pro] Test a backend connection.

            Args:
                connection_id: Connection identifier

            Returns:
                Connection test result
            """
            return '{"error": "Connection tools require full Pro installation"}'


__all__ = ['register_connections_tools']
