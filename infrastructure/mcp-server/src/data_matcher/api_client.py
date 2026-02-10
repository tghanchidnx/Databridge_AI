"""HTTP client for NestJS data matcher API."""
import json
import requests
from typing import Dict, Any, Optional, List


class DataMatcherApiClient:
    """HTTP client for data comparison operations via NestJS backend."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 120):
        """
        Initialize the data matcher API client.

        Args:
            base_url: NestJS backend URL (e.g., 'http://localhost:3001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds (longer for data comparison)
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
    # Data Comparison Operations
    # =========================================================================

    def compare_data(
        self,
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Compare data between two tables row by row.

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name
            key_columns: List of columns to use as keys
            compare_columns: Optional list of columns to compare (default: all)

        Returns:
            Comparison result with matches, orphans, and conflicts
        """
        payload = {
            "source": {
                "connectionId": source_connection_id,
                "database": source_database,
                "schema": source_schema,
                "table": source_table
            },
            "target": {
                "connectionId": target_connection_id,
                "database": target_database,
                "schema": target_schema,
                "table": target_table
            },
            "keyColumns": key_columns,
        }
        if compare_columns:
            payload["compareColumns"] = compare_columns

        return self._request("POST", "/data-matcher/compare", payload)

    def get_comparison_summary(
        self,
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        key_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Get a statistical summary of data comparison without full details.

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name
            key_columns: List of columns to use as keys

        Returns:
            Summary with counts of matches, orphans, and conflicts
        """
        return self._request("POST", "/data-matcher/compare-summary", {
            "source": {
                "connectionId": source_connection_id,
                "database": source_database,
                "schema": source_schema,
                "table": source_table
            },
            "target": {
                "connectionId": target_connection_id,
                "database": target_database,
                "schema": target_schema,
                "table": target_table
            },
            "keyColumns": key_columns,
        })

    def get_table_statistics(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """
        Get profiling statistics for a table.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            Table statistics including row count, column stats, etc.
        """
        return self._request("GET", "/data-matcher/table-statistics", params={
            "connectionId": connection_id,
            "database": database,
            "schema": schema,
            "table": table
        })
