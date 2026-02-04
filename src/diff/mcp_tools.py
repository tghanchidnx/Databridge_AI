"""
MCP tools for diff utilities.

Provides 6 new MCP tools for text/data comparison:
1. diff_text - Compare two strings
2. diff_lists - Compare two lists
3. diff_dicts - Compare two dictionaries
4. find_similar_strings - Find similar strings from candidates
5. explain_diff - Human-readable diff explanation
6. generate_patch - Create patch format output
"""

from typing import List, Dict, Any, Optional, Literal
from .core import (
    compute_similarity,
    get_matching_blocks,
    get_opcodes,
    unified_diff,
    context_diff,
    ndiff_text,
    diff_lists as core_diff_lists,
    diff_dicts as core_diff_dicts,
    explain_diff_human_readable,
    find_close_matches,
)
from .types import TextDiffResult, PatchResult
from .formatters import (
    format_opcodes_json,
    format_opcodes_compact,
    format_diff_html,
    format_diff_text,
    format_explanation,
)


def register_diff_tools(mcp):
    """
    Register all diff utility tools with the MCP server.

    Args:
        mcp: The FastMCP server instance
    """

    @mcp.tool()
    def diff_text(
        text_a: str,
        text_b: str,
        detail_level: Literal["basic", "standard", "detailed"] = "standard",
        include_html: bool = False
    ) -> Dict[str, Any]:
        """
        Compare two text strings with similarity scores and detailed diff analysis.

        Provides character-level comparison using Python's difflib module.
        Useful for comparing names, descriptions, or any text fields where
        you need to understand exactly what changed.

        Args:
            text_a: First text string to compare
            text_b: Second text string to compare
            detail_level: Amount of detail in response
                - "basic": Just similarity score and is_identical flag
                - "standard": Add opcodes and explanation
                - "detailed": Add matching blocks, unified diff, and ndiff
            include_html: Include HTML-formatted diff output

        Returns:
            Dictionary with comparison results including:
            - similarity: Float from 0.0 to 1.0
            - similarity_percent: String like "72.5%"
            - is_identical: Boolean
            - opcodes: List of operations (replace/delete/insert/equal)
            - explanation: Human-readable description of changes
            - matching_blocks: Where the strings match (detailed only)
            - unified_diff: Standard patch format (detailed only)
            - ndiff: Character-level +/-/? markers (detailed only)
            - html: HTML formatted diff (if include_html=True)

        Example:
            >>> diff_text("John Smith", "Jon Smyth")
            {
                "similarity": 0.7273,
                "similarity_percent": "72.7%",
                "is_identical": false,
                "opcodes": [
                    {"operation": "equal", "a_content": "Jo", "b_content": "Jo"},
                    {"operation": "replace", "a_content": "hn", "b_content": "n"},
                    ...
                ],
                "explanation": "Similarity: 72.7%\\n  Changed: 'hn' -> 'n'\\n  Changed: 'i' -> 'y'"
            }
        """
        similarity = compute_similarity(text_a, text_b)
        is_identical = text_a == text_b

        result = {
            "text_a": text_a,
            "text_b": text_b,
            "similarity": round(similarity, 4),
            "similarity_percent": f"{similarity * 100:.1f}%",
            "is_identical": is_identical,
        }

        if detail_level in ("standard", "detailed"):
            opcodes = get_opcodes(text_a, text_b)
            result["opcodes"] = format_opcodes_compact(opcodes)
            result["explanation"] = explain_diff_human_readable(text_a, text_b)

        if detail_level == "detailed":
            result["matching_blocks"] = [
                {
                    "a_start": b.a_start,
                    "b_start": b.b_start,
                    "size": b.size,
                    "content": b.content
                }
                for b in get_matching_blocks(text_a, text_b)
            ]
            result["unified_diff"] = unified_diff(text_a, text_b, "text_a", "text_b")
            result["ndiff"] = ndiff_text(text_a, text_b)

        if include_html:
            opcodes = get_opcodes(text_a, text_b)
            result["html"] = format_diff_html(text_a, text_b, opcodes)

        return result

    @mcp.tool()
    def diff_lists(
        list_a: List[Any],
        list_b: List[Any],
        max_items: int = 100
    ) -> Dict[str, Any]:
        """
        Compare two lists and identify added, removed, and common items.

        Computes both Jaccard similarity (set-based) and sequence similarity.
        Useful for comparing column values, categories, or any ordered/unordered lists.

        Args:
            list_a: First list to compare
            list_b: Second list to compare
            max_items: Maximum items to show in added/removed/common lists

        Returns:
            Dictionary with:
            - list_a_count, list_b_count: Sizes of input lists
            - added: Items in B but not in A
            - removed: Items in A but not in B
            - common: Items in both lists
            - added_count, removed_count, common_count: Counts
            - jaccard_similarity: |A ∩ B| / |A ∪ B| (0.0 to 1.0)
            - jaccard_percent: Jaccard as percentage string
            - sequence_similarity: Order-aware similarity

        Example:
            >>> diff_lists(["a", "b", "c"], ["b", "c", "d"])
            {
                "added": ["d"],
                "removed": ["a"],
                "common": ["b", "c"],
                "jaccard_similarity": 0.5,
                "jaccard_percent": "50.0%"
            }
        """
        result = core_diff_lists(list_a, list_b)

        # Truncate long lists
        return {
            "list_a_count": result.list_a_count,
            "list_b_count": result.list_b_count,
            "added": result.added[:max_items],
            "removed": result.removed[:max_items],
            "common": result.common[:max_items],
            "added_count": result.added_count,
            "removed_count": result.removed_count,
            "common_count": result.common_count,
            "jaccard_similarity": round(result.jaccard_similarity, 4),
            "jaccard_percent": result.jaccard_percent,
            "sequence_similarity": round(result.sequence_similarity, 4),
            "truncated": len(result.added) > max_items or len(result.removed) > max_items or len(result.common) > max_items
        }

    @mcp.tool()
    def diff_dicts(
        dict_a: Dict[str, Any],
        dict_b: Dict[str, Any],
        include_unchanged: bool = False
    ) -> Dict[str, Any]:
        """
        Compare two dictionaries with value-level character diffs.

        For string values, provides character-level diff analysis showing
        exactly what changed. Useful for comparing record fields, configurations,
        or any key-value data structures.

        Args:
            dict_a: First dictionary to compare
            dict_b: Second dictionary to compare
            include_unchanged: Whether to include unchanged keys in differences list

        Returns:
            Dictionary with:
            - dict_a_keys, dict_b_keys: Key counts
            - added_keys: Keys in B but not in A
            - removed_keys: Keys in A but not in B
            - common_keys: Keys in both
            - changed_keys: Common keys with different values
            - unchanged_keys: Common keys with same values
            - differences: Detailed per-key comparison with:
                - key, value_a, value_b, status
                - similarity, opcodes (for string values)
            - overall_similarity: Weighted similarity score

        Example:
            >>> diff_dicts(
            ...     {"name": "John Smith", "age": 30},
            ...     {"name": "Jon Smyth", "age": 30, "city": "NYC"}
            ... )
            {
                "added_keys": ["city"],
                "changed_keys": ["name"],
                "unchanged_keys": ["age"],
                "differences": [
                    {
                        "key": "name",
                        "value_a": "John Smith",
                        "value_b": "Jon Smyth",
                        "status": "changed",
                        "similarity": 0.7273,
                        "opcodes": [...]
                    },
                    ...
                ]
            }
        """
        result = core_diff_dicts(dict_a, dict_b)

        differences = []
        for diff in result.differences:
            if not include_unchanged and diff.status == "unchanged":
                continue

            item = {
                "key": diff.key,
                "value_a": diff.value_a,
                "value_b": diff.value_b,
                "status": diff.status,
            }

            if diff.similarity is not None:
                item["similarity"] = round(diff.similarity, 4)

            if diff.opcodes:
                item["opcodes"] = format_opcodes_compact(diff.opcodes)

            differences.append(item)

        return {
            "dict_a_keys": result.dict_a_keys,
            "dict_b_keys": result.dict_b_keys,
            "added_keys": result.added_keys,
            "removed_keys": result.removed_keys,
            "common_keys": result.common_keys,
            "changed_keys": result.changed_keys,
            "unchanged_keys": result.unchanged_keys,
            "differences": differences,
            "overall_similarity": round(result.overall_similarity, 4)
        }

    @mcp.tool()
    def find_similar_strings(
        target: str,
        candidates: List[str],
        max_results: int = 5,
        min_similarity: float = 0.6
    ) -> Dict[str, Any]:
        """
        Find strings similar to a target from a list of candidates.

        Uses difflib's get_close_matches with exact similarity scoring.
        Useful for fuzzy lookups, typo correction, or finding related entries.

        Args:
            target: The string to match against
            candidates: List of candidate strings to search
            max_results: Maximum number of matches to return (default: 5)
            min_similarity: Minimum similarity threshold 0.0-1.0 (default: 0.6)

        Returns:
            Dictionary with:
            - target: The search string
            - candidates_searched: Number of candidates searched
            - matches: List of matches sorted by similarity, each with:
                - candidate: The matching string
                - similarity: Float similarity score
                - similarity_percent: Percentage string
                - rank: 1 = best match

        Example:
            >>> find_similar_strings("Revenue", ["Revnue", "Expenses", "Revenue Total", "Rev"])
            {
                "target": "Revenue",
                "matches": [
                    {"candidate": "Revenue Total", "similarity": 0.8571, "rank": 1},
                    {"candidate": "Revnue", "similarity": 0.8571, "rank": 2},
                    {"candidate": "Rev", "similarity": 0.6667, "rank": 3}
                ]
            }
        """
        matches = find_close_matches(target, candidates, n=max_results, cutoff=min_similarity)

        return {
            "target": target,
            "candidates_searched": len(candidates),
            "matches_found": len(matches),
            "min_similarity_threshold": min_similarity,
            "matches": [
                {
                    "candidate": m.candidate,
                    "similarity": round(m.similarity, 4),
                    "similarity_percent": m.similarity_percent,
                    "rank": m.rank
                }
                for m in matches
            ]
        }

    @mcp.tool()
    def explain_diff(
        text_a: str,
        text_b: str,
        context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a human-readable explanation of differences between two texts.

        Designed for non-technical users or for displaying to end users.
        Provides a natural language description of what changed.

        Args:
            text_a: First text (original/before)
            text_b: Second text (modified/after)
            context: Optional context for the comparison (e.g., "account name", "description")

        Returns:
            Dictionary with:
            - similarity: Float similarity score
            - similarity_percent: Percentage string
            - is_identical: Boolean
            - summary: One-line summary
            - explanation: Detailed natural language explanation
            - change_count: Number of distinct changes

        Example:
            >>> explain_diff("John Smith", "Jon Smyth", context="customer name")
            {
                "similarity": 0.7273,
                "is_identical": false,
                "summary": "The customer name values are 72.7% similar with 2 changes",
                "explanation": "Similarity: 72.7%\\nChanges:\\n  - Changed 'hn' to 'n'\\n  - Changed 'i' to 'y'"
            }
        """
        similarity = compute_similarity(text_a, text_b)
        is_identical = text_a == text_b
        opcodes = get_opcodes(text_a, text_b)

        # Count changes
        change_count = sum(1 for op in opcodes if op.operation != "equal")

        # Build summary
        context_str = f"The {context} values" if context else "The values"
        if is_identical:
            summary = f"{context_str} are identical"
        elif similarity == 0:
            summary = f"{context_str} are completely different"
        else:
            summary = f"{context_str} are {similarity * 100:.1f}% similar with {change_count} change(s)"

        # Build detailed explanation
        explanation = format_explanation(similarity, opcodes)

        return {
            "text_a": text_a,
            "text_b": text_b,
            "similarity": round(similarity, 4),
            "similarity_percent": f"{similarity * 100:.1f}%",
            "is_identical": is_identical,
            "summary": summary,
            "explanation": explanation,
            "change_count": change_count
        }

    @mcp.tool()
    def generate_patch(
        text_a: str,
        text_b: str,
        format: Literal["unified", "context", "ndiff"] = "unified",
        from_label: str = "original",
        to_label: str = "modified",
        context_lines: int = 3
    ) -> Dict[str, Any]:
        """
        Generate a patch in unified, context, or ndiff format.

        Creates standard patch output that can be used with patch tools
        or for displaying changes in a familiar format.

        Args:
            text_a: Original text (before)
            text_b: Modified text (after)
            format: Patch format
                - "unified": Standard unified diff (default)
                - "context": Context diff format
                - "ndiff": Character-level with +/-/? markers
            from_label: Label for the original file/text
            to_label: Label for the modified file/text
            context_lines: Number of context lines (unified/context only)

        Returns:
            Dictionary with:
            - format: The format used
            - from_label, to_label: Labels used
            - patch: The patch content
            - line_count: Number of lines in patch
            - has_changes: Boolean indicating if there are differences

        Example:
            >>> generate_patch("line1\\nline2", "line1\\nline2 modified", format="unified")
            {
                "format": "unified",
                "patch": "--- original\\n+++ modified\\n@@ -1,2 +1,2 @@\\n line1\\n-line2\\n+line2 modified",
                "has_changes": true
            }
        """
        if format == "unified":
            patch = unified_diff(text_a, text_b, from_label, to_label, context_lines)
        elif format == "context":
            patch = context_diff(text_a, text_b, from_label, to_label, context_lines)
        else:  # ndiff
            patch = ndiff_text(text_a, text_b)

        lines = patch.split('\n') if patch else []
        has_changes = bool(patch.strip())

        return {
            "format": format,
            "from_label": from_label,
            "to_label": to_label,
            "patch": patch,
            "line_count": len(lines),
            "has_changes": has_changes
        }
