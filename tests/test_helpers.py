"""Helper module to extract callable functions from FastMCP tool wrappers.

FastMCP wraps functions with @mcp.tool() decorator as FunctionTool objects.
This module provides access to the underlying callable functions for testing.
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Import the server module to get access to the mcp instance and helper functions
from server import mcp, compute_row_hash, log_action, truncate_dataframe

# Export the internal helper functions directly (they are not wrapped)
__all__ = [
    'compute_row_hash',
    'log_action',
    'truncate_dataframe',
    'get_tool_function',
    # Tool functions
    'load_csv',
    'load_json',
    'query_database',
    'profile_data',
    'detect_schema_drift',
    'compare_hashes',
    'get_orphan_details',
    'get_conflict_details',
    'fuzzy_match_columns',
    'fuzzy_deduplicate',
    'extract_text_from_pdf',
    'ocr_image',
    'parse_table_from_text',
    'save_workflow_step',
    'get_workflow',
    'clear_workflow',
    'get_audit_log',
    'transform_column',
    'merge_sources',
    'update_manifest',
]


def get_tool_function(tool_name: str):
    """Get the underlying callable function from an MCP tool by name.

    Args:
        tool_name: The name of the MCP tool.

    Returns:
        The underlying callable function.

    Raises:
        KeyError: If the tool is not found.
    """
    tools = mcp._tool_manager._tools
    if tool_name not in tools:
        raise KeyError(f"Tool '{tool_name}' not found. Available: {list(tools.keys())}")
    return tools[tool_name].fn


# Extract all tool functions for direct import
def _extract_tool_functions():
    """Extract all tool functions from the MCP server."""
    functions = {}
    try:
        tools = mcp._tool_manager._tools
        for name, tool in tools.items():
            functions[name] = tool.fn
    except Exception as e:
        print(f"Warning: Could not extract tool functions: {e}")
    return functions


# Auto-extract and make available as module-level functions
_tool_functions = _extract_tool_functions()

# Data Loading
load_csv = _tool_functions.get('load_csv')
load_json = _tool_functions.get('load_json')
query_database = _tool_functions.get('query_database')

# Data Profiling
profile_data = _tool_functions.get('profile_data')
detect_schema_drift = _tool_functions.get('detect_schema_drift')

# Comparison Engine
compare_hashes = _tool_functions.get('compare_hashes')
get_orphan_details = _tool_functions.get('get_orphan_details')
get_conflict_details = _tool_functions.get('get_conflict_details')

# Fuzzy Matching
fuzzy_match_columns = _tool_functions.get('fuzzy_match_columns')
fuzzy_deduplicate = _tool_functions.get('fuzzy_deduplicate')

# PDF/OCR
extract_text_from_pdf = _tool_functions.get('extract_text_from_pdf')
ocr_image = _tool_functions.get('ocr_image')
parse_table_from_text = _tool_functions.get('parse_table_from_text')

# Workflow Management
save_workflow_step = _tool_functions.get('save_workflow_step')
get_workflow = _tool_functions.get('get_workflow')
clear_workflow = _tool_functions.get('clear_workflow')
get_audit_log = _tool_functions.get('get_audit_log')

# Data Transformation
transform_column = _tool_functions.get('transform_column')
merge_sources = _tool_functions.get('merge_sources')

# Documentation
update_manifest = _tool_functions.get('update_manifest')
