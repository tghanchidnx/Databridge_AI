"""MCP Tools for schema comparison operations."""
import json
from typing import Optional

from .api_client import SchemaMatcherApiClient


def register_schema_matcher_tools(mcp, base_url: str, api_key: str):
    """Register all schema matcher MCP tools with the server."""

    client = SchemaMatcherApiClient(base_url=base_url, api_key=api_key)

    # =========================================================================
    # Schema Comparison Tools
    # =========================================================================

    @mcp.tool()
    def compare_database_schemas(
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str
    ) -> str:
        """
        Compare schemas between two tables from different database connections.

        Identifies:
        - Columns present in source but not in target
        - Columns present in target but not in source
        - Columns with different data types
        - Columns with different nullability

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
            JSON with detailed schema comparison results including column differences.
        """
        try:
            result = client.compare_schemas(
                source_connection_id=source_connection_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                target_connection_id=target_connection_id,
                target_database=target_database,
                target_schema=target_schema,
                target_table=target_table
            )
            return json.dumps({
                "operation": "schema_comparison",
                "source": {
                    "connection_id": source_connection_id,
                    "fully_qualified": f"{source_database}.{source_schema}.{source_table}"
                },
                "target": {
                    "connection_id": target_connection_id,
                    "fully_qualified": f"{target_database}.{target_schema}.{target_table}"
                },
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_schema_comparison_result(job_id: str) -> str:
        """
        Get the result of a previously executed schema comparison job.

        Args:
            job_id: Comparison job UUID

        Returns:
            JSON with full comparison results including column mappings and differences.
        """
        try:
            result = client.get_comparison_job(job_id)
            return json.dumps({
                "job_id": job_id,
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_schema_comparisons(limit: int = 50) -> str:
        """
        List all schema comparison jobs.

        Args:
            limit: Maximum number of jobs to return (default: 50)

        Returns:
            JSON array of comparison job summaries with status and timestamps.
        """
        try:
            jobs = client.list_comparison_jobs(limit=limit)
            return json.dumps({
                "total": len(jobs),
                "jobs": jobs,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def generate_merge_sql_script(
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        key_columns: str,
        script_type: str = "MERGE"
    ) -> str:
        """
        Generate a MERGE SQL script for synchronizing data between two tables.

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name
            key_columns: Comma-separated list of key columns for matching rows
            script_type: Type of script to generate: MERGE, INSERT, UPDATE, DELETE

        Returns:
            JSON with generated SQL script ready for execution or review.
        """
        try:
            keys = [k.strip() for k in key_columns.split(",")]
            result = client.generate_merge_script(
                source_connection_id=source_connection_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                target_connection_id=target_connection_id,
                target_database=target_database,
                target_schema=target_schema,
                target_table=target_table,
                key_columns=keys,
                script_type=script_type
            )
            return json.dumps({
                "operation": "generate_merge_script",
                "script_type": script_type,
                "key_columns": keys,
                "source": f"{source_database}.{source_schema}.{source_table}",
                "target": f"{target_database}.{target_schema}.{target_table}",
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return client  # Return client for potential direct use
