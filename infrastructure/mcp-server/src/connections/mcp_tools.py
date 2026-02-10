"""MCP Tools for database connections management."""
import json
from typing import Optional

from .api_client import ConnectionsApiClient


def register_connection_tools(mcp, base_url: str, api_key: str):
    """Register all connection MCP tools with the server."""

    client = ConnectionsApiClient(base_url=base_url, api_key=api_key)

    # =========================================================================
    # Connection Management Tools
    # =========================================================================

    @mcp.tool()
    def list_backend_connections() -> str:
        """
        List all database connections from the NestJS backend.

        Returns:
            JSON array of connections with their configuration details.
            Each connection includes: id, name, type, host, port, database, status.
        """
        try:
            connections = client.list_connections()
            return json.dumps({
                "source": "backend",
                "total": len(connections),
                "connections": connections,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_backend_connection(connection_id: str) -> str:
        """
        Get detailed information about a specific database connection.

        Args:
            connection_id: Connection UUID

        Returns:
            JSON with connection details including configuration and metadata.
        """
        try:
            connection = client.get_connection(connection_id)
            if not connection:
                return json.dumps({"error": f"Connection '{connection_id}' not found"})
            return json.dumps({
                "source": "backend",
                "connection": connection,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def test_backend_connection(connection_id: str) -> str:
        """
        Test a database connection's health and connectivity.

        Args:
            connection_id: Connection UUID to test

        Returns:
            JSON with test results including success status, latency, and any error messages.
        """
        try:
            result = client.test_connection(connection_id)
            return json.dumps({
                "connection_id": connection_id,
                "test_result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Database Metadata Tools
    # =========================================================================

    @mcp.tool()
    def get_connection_databases(connection_id: str) -> str:
        """
        List all databases available in a connection.

        Args:
            connection_id: Connection UUID

        Returns:
            JSON array of database names available in the connection.
        """
        try:
            databases = client.get_databases(connection_id)
            return json.dumps({
                "connection_id": connection_id,
                "total": len(databases),
                "databases": databases,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_connection_schemas(connection_id: str, database: str) -> str:
        """
        List all schemas in a database.

        Args:
            connection_id: Connection UUID
            database: Database name

        Returns:
            JSON array of schema names in the specified database.
        """
        try:
            schemas = client.get_schemas(connection_id, database)
            return json.dumps({
                "connection_id": connection_id,
                "database": database,
                "total": len(schemas),
                "schemas": schemas,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_connection_tables(
        connection_id: str,
        database: str,
        schema: str
    ) -> str:
        """
        List all tables in a schema.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name

        Returns:
            JSON array of table names in the specified schema.
        """
        try:
            tables = client.get_tables(connection_id, database, schema)
            return json.dumps({
                "connection_id": connection_id,
                "database": database,
                "schema": schema,
                "total": len(tables),
                "tables": tables,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_connection_columns(
        connection_id: str,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Get column details for a table.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            JSON array of columns with name, data type, nullable, and other metadata.
        """
        try:
            columns = client.get_columns(connection_id, database, schema, table)
            return json.dumps({
                "connection_id": connection_id,
                "database": database,
                "schema": schema,
                "table": table,
                "total": len(columns),
                "columns": columns,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_column_distinct_values(
        connection_id: str,
        database: str,
        schema: str,
        table: str,
        column: str,
        limit: int = 100
    ) -> str:
        """
        Get distinct values from a specific column.

        Useful for understanding data distribution and selecting values for hierarchy mappings.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name
            column: Column name to get values from
            limit: Maximum number of distinct values to return (default: 100)

        Returns:
            JSON array of distinct values from the column.
        """
        try:
            # Limit to max 100 to respect context sensitivity
            limit = min(limit, 100)
            values = client.get_column_values(
                connection_id, database, schema, table, column, limit
            )
            return json.dumps({
                "connection_id": connection_id,
                "database": database,
                "schema": schema,
                "table": table,
                "column": column,
                "total": len(values),
                "values": values,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return client  # Return client for potential direct use
