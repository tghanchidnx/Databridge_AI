"""MCP Tools for data comparison operations."""
import json
from typing import Optional

from .api_client import DataMatcherApiClient


def register_data_matcher_tools(mcp, base_url: str, api_key: str):
    """Register all data matcher MCP tools with the server."""

    client = DataMatcherApiClient(base_url=base_url, api_key=api_key)

    # =========================================================================
    # Data Comparison Tools
    # =========================================================================

    @mcp.tool()
    def compare_table_data(
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        key_columns: str,
        compare_columns: str = ""
    ) -> str:
        """
        Compare data between two tables at the row level.

        Identifies:
        - Rows in source but not in target (orphans)
        - Rows in target but not in source (orphans)
        - Rows with same key but different values (conflicts)
        - Rows that match exactly

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
            compare_columns: Optional comma-separated list of columns to compare (default: all)

        Returns:
            JSON with comparison results including orphans, conflicts, and match statistics.
        """
        try:
            keys = [k.strip() for k in key_columns.split(",")]
            compare_cols = None
            if compare_columns:
                compare_cols = [c.strip() for c in compare_columns.split(",")]

            result = client.compare_data(
                source_connection_id=source_connection_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                target_connection_id=target_connection_id,
                target_database=target_database,
                target_schema=target_schema,
                target_table=target_table,
                key_columns=keys,
                compare_columns=compare_cols
            )
            return json.dumps({
                "operation": "data_comparison",
                "source": f"{source_database}.{source_schema}.{source_table}",
                "target": f"{target_database}.{target_schema}.{target_table}",
                "key_columns": keys,
                "compare_columns": compare_cols or "all",
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_data_comparison_summary(
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        key_columns: str
    ) -> str:
        """
        Get a statistical summary of data comparison between two tables.

        Faster than full comparison, returns only counts without row details.

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

        Returns:
            JSON with summary statistics: total rows, matches, orphans, conflicts.
        """
        try:
            keys = [k.strip() for k in key_columns.split(",")]
            result = client.get_comparison_summary(
                source_connection_id=source_connection_id,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                target_connection_id=target_connection_id,
                target_database=target_database,
                target_schema=target_schema,
                target_table=target_table,
                key_columns=keys
            )
            return json.dumps({
                "operation": "data_comparison_summary",
                "source": f"{source_database}.{source_schema}.{source_table}",
                "target": f"{target_database}.{target_schema}.{target_table}",
                "key_columns": keys,
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_backend_table_statistics(
        connection_id: str,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Get profiling statistics for a table from the backend.

        Returns detailed statistics including:
        - Row count
        - Column statistics (min, max, distinct count, null count)
        - Data distribution information

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            JSON with comprehensive table statistics.
        """
        try:
            result = client.get_table_statistics(
                connection_id=connection_id,
                database=database,
                schema=schema,
                table=table
            )
            return json.dumps({
                "operation": "table_statistics",
                "connection_id": connection_id,
                "fully_qualified": f"{database}.{schema}.{table}",
                "result": result,
            }, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return client  # Return client for potential direct use
