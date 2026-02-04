"""
DataBridge AI Diff Utilities Module

Comprehensive text/data comparison capabilities using Python's difflib module.
Provides character-level diff analysis for AI agents and enhances existing comparison tools.
"""

from .types import (
    DiffOpcode,
    MatchingBlock,
    TextDiffResult,
    ListDiffResult,
    DictDiffResult,
    SimilarStringMatch,
    PatchResult,
)

from .core import (
    compute_similarity,
    get_matching_blocks,
    get_opcodes,
    unified_diff,
    ndiff_text,
    diff_lists,
    diff_dicts,
    diff_values_paired,
    explain_diff_human_readable,
    find_close_matches,
    context_diff,
)

from .formatters import (
    format_opcodes_json,
    format_diff_html,
    format_diff_text,
    format_explanation,
)

from .mcp_tools import register_diff_tools

__all__ = [
    # Types
    "DiffOpcode",
    "MatchingBlock",
    "TextDiffResult",
    "ListDiffResult",
    "DictDiffResult",
    "SimilarStringMatch",
    "PatchResult",
    # Core functions
    "compute_similarity",
    "get_matching_blocks",
    "get_opcodes",
    "unified_diff",
    "ndiff_text",
    "diff_lists",
    "diff_dicts",
    "diff_values_paired",
    "explain_diff_human_readable",
    "find_close_matches",
    "context_diff",
    # Formatters
    "format_opcodes_json",
    "format_diff_html",
    "format_diff_text",
    "format_explanation",
    # MCP registration
    "register_diff_tools",
]
