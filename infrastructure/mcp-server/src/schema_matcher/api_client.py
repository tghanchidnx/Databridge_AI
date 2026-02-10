"""HTTP client for NestJS schema matcher API."""
import json
import requests
from typing import Dict, Any, Optional, List


class SchemaMatcherApiClient:
    """HTTP client for schema comparison operations via NestJS backend."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 60):
        """
        Initialize the schema matcher API client.

        Args:
            base_url: NestJS backend URL (e.g., 'http://localhost:3001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds (longer for schema ops)
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
    # Schema Comparison Operations
    # =========================================================================

    def compare_schemas(
        self,
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str
    ) -> Dict[str, Any]:
        """
        Compare schemas between two tables.

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name

        Returns:
            Comparison result with column differences
        """
        return self._request("POST", "/schema-matcher/compare", {
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
            }
        })

    def get_comparison_job(self, job_id: str) -> Dict[str, Any]:
        """
        Get the result of a schema comparison job.

        Args:
            job_id: Comparison job UUID

        Returns:
            Job result with comparison details
        """
        result = self._request("GET", f"/schema-matcher/jobs/{job_id}")
        return result

    def list_comparison_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        List all schema comparison jobs.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of comparison job summaries
        """
        result = self._request("GET", "/schema-matcher/jobs", params={"limit": limit})
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def generate_merge_script(
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
        script_type: str = "MERGE"
    ) -> Dict[str, Any]:
        """
        Generate a MERGE SQL script for synchronizing two tables.

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
            script_type: Type of script (MERGE, INSERT, UPDATE)

        Returns:
            Generated SQL script
        """
        return self._request("POST", "/schema-matcher/merge-tables", {
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
            "scriptType": script_type
        })
