"""
MCP Tools for Data Reconciliation in DataBridge AI V3.

Provides 20 tools for loading, profiling, comparing, and reconciling data.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP

from ...reconciliation import (
    DataLoader,
    DataProfiler,
    HashComparer,
    FuzzyMatcher,
)
from ...reconciliation.fuzzy import MatchMethod


def register_reconciliation_tools(mcp: FastMCP) -> None:
    """Register all reconciliation MCP tools."""

    # Workflow state storage
    _workflow_steps: List[Dict[str, Any]] = []
    _data_cache: Dict[str, Any] = {}

    # ==================== Data Loading Tools ====================

    @mcp.tool()
    def load_csv(
        file_path: str,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        skip_rows: int = 0,
        columns: Optional[List[str]] = None,
        max_rows: Optional[int] = None,
        cache_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Load data from a CSV file.

        Args:
            file_path: Path to the CSV file.
            delimiter: Column delimiter (auto-detected if None).
            has_header: Whether the file has a header row.
            skip_rows: Number of rows to skip at the start.
            columns: Specific columns to load (None for all).
            max_rows: Maximum rows to load (None for unlimited).
            cache_key: Key to cache the loaded data for later use.

        Returns:
            Dictionary with load results including row count, columns, and schema.
        """
        loader = DataLoader(max_rows=max_rows)
        result = loader.load_csv(
            path=file_path,
            delimiter=delimiter,
            has_header=has_header,
            skip_rows=skip_rows,
            columns=columns,
        )

        if result.success and cache_key and result.data is not None:
            _data_cache[cache_key] = result.data

        response = result.to_dict()
        if result.success and result.data is not None:
            response["preview"] = result.data.head(5).to_dict(orient="records")
        return response

    @mcp.tool()
    def load_json(
        file_path: str,
        json_format: str = "auto",
        record_path: Optional[str] = None,
        max_rows: Optional[int] = None,
        cache_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Load data from a JSON file.

        Args:
            file_path: Path to the JSON file.
            json_format: Format type ('records', 'lines', 'nested', or 'auto').
            record_path: JSON path to records for nested format (e.g., 'data.items').
            max_rows: Maximum rows to load.
            cache_key: Key to cache the loaded data.

        Returns:
            Dictionary with load results including row count, columns, and schema.
        """
        loader = DataLoader(max_rows=max_rows)
        result = loader.load_json(
            path=file_path,
            json_format=json_format,
            record_path=record_path,
        )

        if result.success and cache_key and result.data is not None:
            _data_cache[cache_key] = result.data

        response = result.to_dict()
        if result.success and result.data is not None:
            response["preview"] = result.data.head(5).to_dict(orient="records")
        return response

    @mcp.tool()
    def query_database(
        query: str,
        connection_string: str,
        params: Optional[Dict[str, Any]] = None,
        max_rows: Optional[int] = None,
        cache_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Load data from a SQL database query.

        Args:
            query: SQL query to execute.
            connection_string: SQLAlchemy connection string.
            params: Query parameters for parameterized queries.
            max_rows: Maximum rows to load.
            cache_key: Key to cache the loaded data.

        Returns:
            Dictionary with load results including row count, columns, and schema.
        """
        loader = DataLoader(max_rows=max_rows)
        result = loader.load_sql(
            query=query,
            connection_string=connection_string,
            params=params,
        )

        if result.success and cache_key and result.data is not None:
            _data_cache[cache_key] = result.data

        response = result.to_dict()
        if result.success and result.data is not None:
            response["preview"] = result.data.head(5).to_dict(orient="records")
        return response

    @mcp.tool()
    def load_excel(
        file_path: str,
        sheet_name: str = "0",
        has_header: bool = True,
        skip_rows: int = 0,
        columns: Optional[List[str]] = None,
        max_rows: Optional[int] = None,
        cache_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Load data from an Excel file.

        Args:
            file_path: Path to the Excel file (.xlsx or .xls).
            sheet_name: Sheet name or index (as string, default "0" for first sheet).
            has_header: Whether the sheet has a header row.
            skip_rows: Number of rows to skip at the start.
            columns: Specific columns to load (None for all).
            max_rows: Maximum rows to load.
            cache_key: Key to cache the loaded data.

        Returns:
            Dictionary with load results including row count, columns, and schema.
        """
        loader = DataLoader(max_rows=max_rows)

        # Convert sheet_name to int if it's a digit string
        sheet = int(sheet_name) if sheet_name.isdigit() else sheet_name

        result = loader.load_excel(
            path=file_path,
            sheet_name=sheet,
            has_header=has_header,
            skip_rows=skip_rows,
            columns=columns,
        )

        if result.success and cache_key and result.data is not None:
            _data_cache[cache_key] = result.data

        response = result.to_dict()
        if result.success and result.data is not None:
            response["preview"] = result.data.head(5).to_dict(orient="records")
        return response

    # ==================== Profiling Tools ====================

    @mcp.tool()
    def profile_data(
        cache_key: str,
        top_n: int = 10,
        detect_patterns: bool = True,
    ) -> Dict[str, Any]:
        """
        Profile a cached dataset to understand its structure and quality.

        Args:
            cache_key: Key of the cached data to profile.
            top_n: Number of top values to include per column.
            detect_patterns: Whether to detect common patterns (email, phone, etc.).

        Returns:
            Dictionary with profiling results including column stats and quality scores.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]
        profiler = DataProfiler(top_n=top_n, detect_patterns=detect_patterns)
        result = profiler.profile(df)
        return result.to_dict()

    @mcp.tool()
    def detect_schema_drift(
        baseline_cache_key: str,
        current_cache_key: str,
        null_increase_threshold: float = 10.0,
        cardinality_change_threshold: float = 0.2,
    ) -> Dict[str, Any]:
        """
        Detect schema drift between a baseline and current dataset.

        Args:
            baseline_cache_key: Key of the cached baseline data.
            current_cache_key: Key of the cached current data.
            null_increase_threshold: Percentage point increase to flag (default 10).
            cardinality_change_threshold: Relative change to flag (default 0.2 = 20%).

        Returns:
            Dictionary with drift detection results.
        """
        if baseline_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {baseline_cache_key}"]}
        if current_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {current_cache_key}"]}

        baseline_df = _data_cache[baseline_cache_key]
        current_df = _data_cache[current_cache_key]

        profiler = DataProfiler()
        baseline_profile = profiler.profile(baseline_df)
        current_profile = profiler.profile(current_df)

        drift_result = profiler.detect_schema_drift(
            baseline=baseline_profile,
            current=current_profile,
            thresholds={
                "null_increase": null_increase_threshold,
                "cardinality_change": cardinality_change_threshold,
                "type_change": True,
            },
        )

        return {
            "success": True,
            **drift_result,
        }

    # ==================== Comparison Tools ====================

    @mcp.tool()
    def compare_hashes(
        source_cache_key: str,
        target_cache_key: str,
        key_columns: List[str],
        value_columns: Optional[List[str]] = None,
        include_details: bool = True,
        max_details: int = 100,
    ) -> Dict[str, Any]:
        """
        Compare two datasets using hash-based comparison.

        Args:
            source_cache_key: Key of the cached source data.
            target_cache_key: Key of the cached target data.
            key_columns: Columns that uniquely identify a record.
            value_columns: Columns to compare for conflicts (None = all non-key columns).
            include_details: Whether to include detailed match records.
            max_details: Maximum number of detail records to return.

        Returns:
            Dictionary with comparison results including matches, orphans, and conflicts.
        """
        if source_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {source_cache_key}"]}
        if target_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {target_cache_key}"]}

        source_df = _data_cache[source_cache_key]
        target_df = _data_cache[target_cache_key]

        comparer = HashComparer()
        result = comparer.compare(
            source_df=source_df,
            target_df=target_df,
            key_columns=key_columns,
            value_columns=value_columns,
            include_details=include_details,
            max_details=max_details,
        )

        response = result.to_dict()
        if include_details:
            response["details"] = {
                "matches": [m.to_dict() for m in result.get_matches()[:max_details]],
                "orphans_source": [m.to_dict() for m in result.get_orphans_source()[:max_details]],
                "orphans_target": [m.to_dict() for m in result.get_orphans_target()[:max_details]],
                "conflicts": [m.to_dict() for m in result.get_conflicts()[:max_details]],
            }
        return response

    @mcp.tool()
    def get_orphan_details(
        source_cache_key: str,
        target_cache_key: str,
        key_columns: List[str],
        orphan_type: str = "source",
        max_records: int = 100,
    ) -> Dict[str, Any]:
        """
        Get detailed information about orphan records (records in one dataset but not the other).

        Args:
            source_cache_key: Key of the cached source data.
            target_cache_key: Key of the cached target data.
            key_columns: Columns that uniquely identify a record.
            orphan_type: Type of orphans ('source' or 'target').
            max_records: Maximum number of orphan records to return.

        Returns:
            Dictionary with orphan record details.
        """
        if source_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {source_cache_key}"]}
        if target_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {target_cache_key}"]}

        source_df = _data_cache[source_cache_key]
        target_df = _data_cache[target_cache_key]

        comparer = HashComparer()
        result = comparer.compare(
            source_df=source_df,
            target_df=target_df,
            key_columns=key_columns,
            include_details=True,
            max_details=max_records,
        )

        if orphan_type == "source":
            orphans = result.get_orphans_source()
        else:
            orphans = result.get_orphans_target()

        return {
            "success": True,
            "orphan_type": orphan_type,
            "count": len(orphans),
            "records": [
                {
                    "key_values": o.key_values,
                    "values": o.source_values if orphan_type == "source" else o.target_values,
                }
                for o in orphans[:max_records]
            ],
        }

    @mcp.tool()
    def get_conflict_details(
        source_cache_key: str,
        target_cache_key: str,
        key_columns: List[str],
        value_columns: Optional[List[str]] = None,
        max_records: int = 100,
    ) -> Dict[str, Any]:
        """
        Get detailed information about conflict records (same key, different values).

        Args:
            source_cache_key: Key of the cached source data.
            target_cache_key: Key of the cached target data.
            key_columns: Columns that uniquely identify a record.
            value_columns: Columns to compare for conflicts.
            max_records: Maximum number of conflict records to return.

        Returns:
            Dictionary with conflict record details.
        """
        if source_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {source_cache_key}"]}
        if target_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {target_cache_key}"]}

        source_df = _data_cache[source_cache_key]
        target_df = _data_cache[target_cache_key]

        comparer = HashComparer()
        result = comparer.compare(
            source_df=source_df,
            target_df=target_df,
            key_columns=key_columns,
            value_columns=value_columns,
            include_details=True,
            max_details=max_records,
        )

        conflicts = result.get_conflicts()

        return {
            "success": True,
            "count": len(conflicts),
            "records": [
                {
                    "key_values": c.key_values,
                    "diff_columns": c.diff_columns,
                    "source_values": c.source_values,
                    "target_values": c.target_values,
                }
                for c in conflicts[:max_records]
            ],
        }

    # ==================== Fuzzy Matching Tools ====================

    @mcp.tool()
    def fuzzy_match_columns(
        source_cache_key: str,
        target_cache_key: str,
        source_column: str,
        target_column: str,
        threshold: float = 80.0,
        method: str = "weighted_ratio",
        include_unmatched: bool = True,
    ) -> Dict[str, Any]:
        """
        Fuzzy match records between two datasets based on a column.

        Args:
            source_cache_key: Key of the cached source data.
            target_cache_key: Key of the cached target data.
            source_column: Column name in source to match on.
            target_column: Column name in target to match on.
            threshold: Minimum score (0-100) to consider a match.
            method: Matching method ('ratio', 'partial_ratio', 'token_sort_ratio', 'token_set_ratio', 'weighted_ratio').
            include_unmatched: Whether to include unmatched source records.

        Returns:
            Dictionary with fuzzy matching results.
        """
        if source_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {source_cache_key}"]}
        if target_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {target_cache_key}"]}

        source_df = _data_cache[source_cache_key]
        target_df = _data_cache[target_cache_key]

        # Map method string to enum
        method_map = {
            "ratio": MatchMethod.RATIO,
            "partial_ratio": MatchMethod.PARTIAL_RATIO,
            "token_sort_ratio": MatchMethod.TOKEN_SORT,
            "token_set_ratio": MatchMethod.TOKEN_SET,
            "weighted_ratio": MatchMethod.WEIGHTED,
            "levenshtein": MatchMethod.LEVENSHTEIN,
        }
        match_method = method_map.get(method, MatchMethod.WEIGHTED)

        matcher = FuzzyMatcher(threshold=threshold, method=match_method)
        result = matcher.match(
            source=source_df[source_column],
            target=target_df[target_column],
        )

        response = result.to_dict()
        response["matches"] = [m.to_dict() for m in result.matches[:100]]
        return response

    @mcp.tool()
    def fuzzy_deduplicate(
        cache_key: str,
        column: str,
        threshold: float = 80.0,
        keep: str = "first",
        save_to_cache: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Find and remove duplicate records using fuzzy matching.

        Args:
            cache_key: Key of the cached data to deduplicate.
            column: Column name to check for duplicates.
            threshold: Minimum score (0-100) to consider a duplicate.
            keep: Which record to keep ('first', 'last', or 'best').
            save_to_cache: Key to cache the deduplicated data (optional).

        Returns:
            Dictionary with deduplication results.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        matcher = FuzzyMatcher(threshold=threshold)
        deduped_df, result = matcher.deduplicate_dataframe(df, column, keep=keep)

        if save_to_cache:
            _data_cache[save_to_cache] = deduped_df

        response = result.to_dict()
        response["deduplicated_rows"] = len(deduped_df)
        if save_to_cache:
            response["saved_to_cache"] = save_to_cache
        return response

    # ==================== Transform Tools ====================

    @mcp.tool()
    def transform_column(
        cache_key: str,
        column: str,
        operation: str,
        new_column: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Transform a column in a cached dataset.

        Args:
            cache_key: Key of the cached data.
            column: Column name to transform.
            operation: Transformation operation ('upper', 'lower', 'trim', 'strip_chars', 'replace', 'split', 'round', 'abs', 'fillna').
            new_column: Name for the new column (optional, modifies in place if not provided).
            params: Parameters for the operation (e.g., {'chars': '-'} for strip_chars, {'old': 'a', 'new': 'b'} for replace).

        Returns:
            Dictionary with transformation results.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]
        params = params or {}

        target_col = new_column or column
        try:
            if operation == "upper":
                df[target_col] = df[column].astype(str).str.upper()
            elif operation == "lower":
                df[target_col] = df[column].astype(str).str.lower()
            elif operation == "trim":
                df[target_col] = df[column].astype(str).str.strip()
            elif operation == "strip_chars":
                chars = params.get("chars", " ")
                df[target_col] = df[column].astype(str).str.strip(chars)
            elif operation == "replace":
                old = params.get("old", "")
                new = params.get("new", "")
                df[target_col] = df[column].astype(str).str.replace(old, new, regex=False)
            elif operation == "split":
                delimiter = params.get("delimiter", ",")
                index = params.get("index", 0)
                df[target_col] = df[column].astype(str).str.split(delimiter).str[index]
            elif operation == "round":
                decimals = params.get("decimals", 2)
                df[target_col] = df[column].round(decimals)
            elif operation == "abs":
                df[target_col] = df[column].abs()
            elif operation == "fillna":
                value = params.get("value", "")
                df[target_col] = df[column].fillna(value)
            else:
                return {"success": False, "errors": [f"Unknown operation: {operation}"]}

            _data_cache[cache_key] = df
            return {
                "success": True,
                "column": target_col,
                "operation": operation,
                "rows_affected": len(df),
            }

        except Exception as e:
            return {"success": False, "errors": [str(e)]}

    @mcp.tool()
    def merge_sources(
        left_cache_key: str,
        right_cache_key: str,
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
        how: str = "inner",
        save_to_cache: str = "merged",
    ) -> Dict[str, Any]:
        """
        Merge two cached datasets.

        Args:
            left_cache_key: Key of the left cached data.
            right_cache_key: Key of the right cached data.
            on: Column(s) to merge on (if same in both).
            left_on: Column(s) from left to merge on.
            right_on: Column(s) from right to merge on.
            how: Type of merge ('inner', 'left', 'right', 'outer').
            save_to_cache: Key to cache the merged data.

        Returns:
            Dictionary with merge results.
        """
        if left_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {left_cache_key}"]}
        if right_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {right_cache_key}"]}

        left_df = _data_cache[left_cache_key]
        right_df = _data_cache[right_cache_key]

        try:
            if on:
                merged_df = left_df.merge(right_df, on=on, how=how)
            else:
                merged_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)

            _data_cache[save_to_cache] = merged_df

            return {
                "success": True,
                "left_rows": len(left_df),
                "right_rows": len(right_df),
                "merged_rows": len(merged_df),
                "columns": list(merged_df.columns),
                "saved_to_cache": save_to_cache,
            }

        except Exception as e:
            return {"success": False, "errors": [str(e)]}

    # ==================== Workflow Tools ====================

    @mcp.tool()
    def save_workflow_step(
        step_name: str,
        tool_name: str,
        parameters: Dict[str, Any],
        result_summary: str,
    ) -> Dict[str, Any]:
        """
        Save a workflow step for audit and replay.

        Args:
            step_name: Name/description of the step.
            tool_name: Name of the tool that was executed.
            parameters: Parameters that were passed to the tool.
            result_summary: Summary of the result.

        Returns:
            Dictionary confirming the step was saved.
        """
        step = {
            "step_number": len(_workflow_steps) + 1,
            "step_name": step_name,
            "tool_name": tool_name,
            "parameters": parameters,
            "result_summary": result_summary,
            "timestamp": datetime.now().isoformat(),
        }
        _workflow_steps.append(step)

        return {
            "success": True,
            "step_number": step["step_number"],
            "message": f"Step '{step_name}' saved to workflow",
        }

    @mcp.tool()
    def get_workflow() -> Dict[str, Any]:
        """
        Get the current workflow steps.

        Returns:
            Dictionary with all workflow steps.
        """
        return {
            "success": True,
            "total_steps": len(_workflow_steps),
            "steps": _workflow_steps,
        }

    @mcp.tool()
    def clear_workflow() -> Dict[str, Any]:
        """
        Clear all workflow steps.

        Returns:
            Dictionary confirming the workflow was cleared.
        """
        cleared_count = len(_workflow_steps)
        _workflow_steps.clear()

        return {
            "success": True,
            "cleared_steps": cleared_count,
            "message": "Workflow cleared",
        }

    @mcp.tool()
    def get_cached_data_keys() -> Dict[str, Any]:
        """
        Get list of all cached data keys.

        Returns:
            Dictionary with cached data information.
        """
        cache_info = {}
        for key, df in _data_cache.items():
            cache_info[key] = {
                "rows": len(df),
                "columns": list(df.columns),
            }

        return {
            "success": True,
            "cached_datasets": len(_data_cache),
            "datasets": cache_info,
        }

    @mcp.tool()
    def clear_cache(cache_key: Optional[str] = None) -> Dict[str, Any]:
        """
        Clear cached data.

        Args:
            cache_key: Specific key to clear (None to clear all).

        Returns:
            Dictionary confirming the cache was cleared.
        """
        if cache_key:
            if cache_key in _data_cache:
                del _data_cache[cache_key]
                return {"success": True, "message": f"Cleared cache key: {cache_key}"}
            else:
                return {"success": False, "errors": [f"Cache key not found: {cache_key}"]}
        else:
            cleared_count = len(_data_cache)
            _data_cache.clear()
            return {"success": True, "message": f"Cleared {cleared_count} cached datasets"}

    @mcp.tool()
    def export_comparison_report(
        source_cache_key: str,
        target_cache_key: str,
        key_columns: List[str],
        output_path: str,
        format: str = "csv",
    ) -> Dict[str, Any]:
        """
        Export a comparison report to a file.

        Args:
            source_cache_key: Key of the cached source data.
            target_cache_key: Key of the cached target data.
            key_columns: Columns that uniquely identify a record.
            output_path: Path to save the report.
            format: Output format ('csv' or 'json').

        Returns:
            Dictionary with export results.
        """
        if source_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {source_cache_key}"]}
        if target_cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {target_cache_key}"]}

        source_df = _data_cache[source_cache_key]
        target_df = _data_cache[target_cache_key]

        comparer = HashComparer()
        report_df = comparer.reconcile(source_df, target_df, key_columns)

        try:
            if format == "csv":
                report_df.to_csv(output_path, index=False)
            elif format == "json":
                report_df.to_json(output_path, orient="records", indent=2)
            else:
                return {"success": False, "errors": [f"Unsupported format: {format}"]}

            return {
                "success": True,
                "output_path": output_path,
                "format": format,
                "rows_exported": len(report_df),
            }

        except Exception as e:
            return {"success": False, "errors": [str(e)]}
