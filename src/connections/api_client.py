"""HTTP client for NestJS connections API."""
import json
import requests
from typing import Dict, Any, Optional, List


class ConnectionsApiClient:
    """HTTP client for managing database connections via NestJS backend."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        """
        Initialize the connections API client.

        Args:
            base_url: NestJS backend URL (e.g., 'http://localhost:3001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json',
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request to the backend."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=self.timeout,
            )

            if response.status_code >= 400:
                return {
                    "error": True,
                    "status_code": response.status_code,
                    "message": response.text,
                }

            return response.json() if response.text else {"success": True}

        except requests.exceptions.ConnectionError:
            return {"error": True, "message": "Backend not reachable"}
        except requests.exceptions.Timeout:
            return {"error": True, "message": "Request timed out"}
        except Exception as e:
            return {"error": True, "message": str(e)}

    # =========================================================================
    # Connection CRUD Operations
    # =========================================================================

    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all database connections.

        Returns:
            List of connection objects
        """
        result = self._request("GET", "/connections")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a connection by ID.

        Args:
            connection_id: Connection UUID

        Returns:
            Connection object or None
        """
        result = self._request("GET", f"/connections/{connection_id}")
        if result.get("error"):
            return None
        return result.get("data") if "data" in result else result

    def test_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Test a connection's health.

        Args:
            connection_id: Connection UUID

        Returns:
            Connection test result
        """
        result = self._request("GET", f"/connections/{connection_id}/test")
        return result

    # =========================================================================
    # Database Metadata Operations
    # =========================================================================

    def get_databases(self, connection_id: str) -> List[str]:
        """
        Get list of databases for a connection.

        Args:
            connection_id: Connection UUID

        Returns:
            List of database names
        """
        result = self._request("POST", "/connections/databases", {
            "connectionId": connection_id
        })
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_schemas(self, connection_id: str, database: str) -> List[str]:
        """
        Get list of schemas in a database.

        Args:
            connection_id: Connection UUID
            database: Database name

        Returns:
            List of schema names
        """
        result = self._request("POST", "/connections/schemas", {
            "connectionId": connection_id,
            "database": database
        })
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_tables(
        self,
        connection_id: str,
        database: str,
        schema: str
    ) -> List[str]:
        """
        Get list of tables in a schema.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name

        Returns:
            List of table names
        """
        result = self._request("POST", "/connections/tables", {
            "connectionId": connection_id,
            "database": database,
            "schema": schema
        })
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_columns(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str
    ) -> List[Dict[str, Any]]:
        """
        Get columns for a table.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            List of column objects with name, type, nullable, etc.
        """
        result = self._request("POST", "/connections/columns", {
            "connectionId": connection_id,
            "database": database,
            "schema": schema,
            "table": table
        })
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_column_values(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str,
        column: str,
        limit: int = 100
    ) -> List[Any]:
        """
        Get distinct values from a column.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name
            column: Column name
            limit: Maximum number of values to return

        Returns:
            List of distinct values
        """
        result = self._request("POST", "/connections/column-data", {
            "connectionId": connection_id,
            "database": database,
            "schema": schema,
            "table": table,
            "column": column,
            "limit": limit
        })
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []
