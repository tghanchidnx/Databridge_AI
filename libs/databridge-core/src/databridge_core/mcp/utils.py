"""
MCP server utilities for DataBridge AI platform.

Provides common utilities for building FastMCP-based MCP servers.
"""

from typing import Any, Dict, List, Optional, Union
import json


def create_mcp_server(name: str, description: str = ""):
    """
    Create a FastMCP server instance.

    This is a helper function that creates a properly configured
    FastMCP server with common settings.

    Args:
        name: Server name.
        description: Server description.

    Returns:
        FastMCP: Configured MCP server instance.
    """
    try:
        from fastmcp import FastMCP
        return FastMCP(name=name, description=description)
    except ImportError:
        raise ImportError("FastMCP is required. Install with: pip install fastmcp")


def truncate_for_llm(
    data: Any,
    max_rows: int = 10,
    max_length: int = 4000,
    include_summary: bool = True,
) -> Union[str, Dict[str, Any], List[Any]]:
    """
    Truncate data for LLM consumption.

    Prevents context overflow by limiting the size of data returned
    to the LLM.

    Args:
        data: Data to truncate (can be list, dict, or other).
        max_rows: Maximum number of rows for list data.
        max_length: Maximum character length for string data.
        include_summary: Include a summary of truncated data.

    Returns:
        Truncated data suitable for LLM context.
    """
    if data is None:
        return None

    if isinstance(data, str):
        if len(data) > max_length:
            truncated = data[:max_length]
            if include_summary:
                return f"{truncated}... [truncated, {len(data)} total characters]"
            return truncated
        return data

    if isinstance(data, list):
        if len(data) > max_rows:
            truncated = data[:max_rows]
            if include_summary:
                return {
                    "data": truncated,
                    "_truncated": True,
                    "_total_rows": len(data),
                    "_shown_rows": max_rows,
                }
            return truncated
        return data

    if isinstance(data, dict):
        # Recursively truncate dict values
        truncated = {}
        for key, value in data.items():
            truncated[key] = truncate_for_llm(value, max_rows, max_length, include_summary)
        return truncated

    return data


def format_tool_response(
    success: bool,
    data: Any = None,
    message: Optional[str] = None,
    error: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Format a standardized tool response.

    Provides a consistent response format across all MCP tools.

    Args:
        success: Whether the operation succeeded.
        data: Response data.
        message: Optional success/info message.
        error: Optional error message (for failures).
        metadata: Optional additional metadata.

    Returns:
        Standardized response dictionary.
    """
    response = {"success": success}

    if data is not None:
        response["data"] = data

    if message:
        response["message"] = message

    if error:
        response["error"] = error

    if metadata:
        response["metadata"] = metadata

    return response


def json_serializable(obj: Any) -> Any:
    """
    Convert an object to JSON-serializable format.

    Handles common non-serializable types like datetime.

    Args:
        obj: Object to convert.

    Returns:
        JSON-serializable version of the object.
    """
    from datetime import datetime, date

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "__dict__"):
        return {k: json_serializable(v) for k, v in obj.__dict__.items() if not k.startswith("_")}
    if isinstance(obj, dict):
        return {k: json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_serializable(v) for v in obj]
    if hasattr(obj, "model_dump"):  # Pydantic v2
        return obj.model_dump()
    if hasattr(obj, "dict"):  # Pydantic v1
        return obj.dict()
    return obj


def safe_json_dumps(obj: Any, **kwargs) -> str:
    """
    Safely serialize an object to JSON.

    Handles non-serializable types gracefully.

    Args:
        obj: Object to serialize.
        **kwargs: Additional arguments for json.dumps.

    Returns:
        JSON string.
    """
    return json.dumps(json_serializable(obj), **kwargs)
