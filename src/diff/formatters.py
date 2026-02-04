"""
Output formatters for diff results.

Provides formatting utilities for JSON, HTML, and plain text output.
"""

from typing import List, Dict, Any, Optional
from .types import DiffOpcode, MatchingBlock, TextDiffResult


def format_opcodes_json(opcodes: List[DiffOpcode]) -> List[Dict[str, Any]]:
    """
    Format opcodes as JSON-serializable dictionaries.

    Args:
        opcodes: List of DiffOpcode objects

    Returns:
        List of dictionaries suitable for JSON output
    """
    result = []
    for op in opcodes:
        item = {
            "operation": op.operation,
            "a_range": [op.a_start, op.a_end],
            "b_range": [op.b_start, op.b_end],
        }

        if op.a_content is not None:
            item["a_content"] = op.a_content
        if op.b_content is not None:
            item["b_content"] = op.b_content

        result.append(item)

    return result


def format_opcodes_compact(opcodes: List[DiffOpcode]) -> List[Dict[str, Any]]:
    """
    Format opcodes in a compact form for AI agents.

    Only includes non-equal operations with content.

    Args:
        opcodes: List of DiffOpcode objects

    Returns:
        Compact list of changes
    """
    result = []
    for op in opcodes:
        if op.operation == "equal":
            continue

        if op.operation == "replace":
            result.append({
                "operation": "replace",
                "from": op.a_content,
                "to": op.b_content
            })
        elif op.operation == "delete":
            result.append({
                "operation": "delete",
                "content": op.a_content
            })
        elif op.operation == "insert":
            result.append({
                "operation": "insert",
                "content": op.b_content
            })

    return result


def format_diff_html(
    a: str,
    b: str,
    opcodes: List[DiffOpcode],
    inline: bool = True
) -> str:
    """
    Format diff as HTML with color highlighting.

    Args:
        a: First string
        b: Second string
        opcodes: List of DiffOpcode objects
        inline: If True, show changes inline; if False, show side-by-side

    Returns:
        HTML string with styled diff
    """
    if inline:
        return _format_html_inline(a, b, opcodes)
    else:
        return _format_html_side_by_side(a, b, opcodes)


def _format_html_inline(a: str, b: str, opcodes: List[DiffOpcode]) -> str:
    """Generate inline HTML diff."""
    html_parts = ['<div class="diff-inline">']

    for op in opcodes:
        if op.operation == "equal":
            html_parts.append(f'<span class="equal">{_escape_html(op.a_content)}</span>')
        elif op.operation == "replace":
            html_parts.append(f'<del class="removed">{_escape_html(op.a_content)}</del>')
            html_parts.append(f'<ins class="added">{_escape_html(op.b_content)}</ins>')
        elif op.operation == "delete":
            html_parts.append(f'<del class="removed">{_escape_html(op.a_content)}</del>')
        elif op.operation == "insert":
            html_parts.append(f'<ins class="added">{_escape_html(op.b_content)}</ins>')

    html_parts.append('</div>')

    # Add default styles
    styles = """
<style>
.diff-inline { font-family: monospace; white-space: pre-wrap; }
.diff-inline .equal { color: inherit; }
.diff-inline .removed { background-color: #ffcccc; text-decoration: line-through; color: #990000; }
.diff-inline .added { background-color: #ccffcc; text-decoration: none; color: #006600; }
</style>
"""
    return styles + '\n'.join(html_parts)


def _format_html_side_by_side(a: str, b: str, opcodes: List[DiffOpcode]) -> str:
    """Generate side-by-side HTML diff."""
    left_parts = []
    right_parts = []

    for op in opcodes:
        if op.operation == "equal":
            left_parts.append(f'<span class="equal">{_escape_html(op.a_content)}</span>')
            right_parts.append(f'<span class="equal">{_escape_html(op.b_content)}</span>')
        elif op.operation == "replace":
            left_parts.append(f'<span class="removed">{_escape_html(op.a_content)}</span>')
            right_parts.append(f'<span class="added">{_escape_html(op.b_content)}</span>')
        elif op.operation == "delete":
            left_parts.append(f'<span class="removed">{_escape_html(op.a_content)}</span>')
        elif op.operation == "insert":
            right_parts.append(f'<span class="added">{_escape_html(op.b_content)}</span>')

    styles = """
<style>
.diff-side-by-side { display: flex; gap: 20px; font-family: monospace; }
.diff-side-by-side > div { flex: 1; white-space: pre-wrap; padding: 10px; border: 1px solid #ddd; }
.diff-side-by-side .equal { color: inherit; }
.diff-side-by-side .removed { background-color: #ffcccc; color: #990000; }
.diff-side-by-side .added { background-color: #ccffcc; color: #006600; }
</style>
"""

    html = f"""
{styles}
<div class="diff-side-by-side">
    <div class="left">{''.join(left_parts)}</div>
    <div class="right">{''.join(right_parts)}</div>
</div>
"""
    return html


def _escape_html(text: Optional[str]) -> str:
    """Escape HTML special characters."""
    if text is None:
        return ""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))


def format_diff_text(
    opcodes: List[DiffOpcode],
    context_chars: int = 10
) -> str:
    """
    Format diff as plain text with markers.

    Args:
        opcodes: List of DiffOpcode objects
        context_chars: Number of context characters to show

    Returns:
        Plain text diff representation
    """
    lines = []

    for op in opcodes:
        if op.operation == "equal":
            content = op.a_content or ""
            if len(content) > context_chars * 2:
                # Show abbreviated equal content
                lines.append(f"  {content[:context_chars]}...{content[-context_chars:]}")
            else:
                lines.append(f"  {content}")
        elif op.operation == "replace":
            lines.append(f"- {op.a_content}")
            lines.append(f"+ {op.b_content}")
        elif op.operation == "delete":
            lines.append(f"- {op.a_content}")
        elif op.operation == "insert":
            lines.append(f"+ {op.b_content}")

    return "\n".join(lines)


def format_explanation(
    similarity: float,
    opcodes: List[DiffOpcode],
    max_changes: int = 5
) -> str:
    """
    Format a human-readable explanation of differences.

    Args:
        similarity: Similarity ratio (0.0 to 1.0)
        opcodes: List of DiffOpcode objects
        max_changes: Maximum number of changes to list

    Returns:
        Natural language explanation
    """
    if similarity == 1.0:
        return "The texts are identical."

    if similarity == 0.0:
        return "The texts are completely different."

    lines = [f"Similarity: {similarity * 100:.1f}%"]

    changes = []
    for op in opcodes:
        if op.operation == "equal":
            continue

        if op.operation == "replace":
            changes.append(f"  - Changed '{_truncate(op.a_content, 20)}' to '{_truncate(op.b_content, 20)}'")
        elif op.operation == "delete":
            changes.append(f"  - Removed '{_truncate(op.a_content, 20)}'")
        elif op.operation == "insert":
            changes.append(f"  - Added '{_truncate(op.b_content, 20)}'")

    if len(changes) > max_changes:
        lines.append(f"Changes ({len(changes)} total, showing first {max_changes}):")
        lines.extend(changes[:max_changes])
        lines.append(f"  ... and {len(changes) - max_changes} more")
    elif changes:
        lines.append("Changes:")
        lines.extend(changes)
    else:
        lines.append("No visible character changes detected.")

    return "\n".join(lines)


def _truncate(text: Optional[str], max_len: int) -> str:
    """Truncate text with ellipsis."""
    if text is None:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def format_matching_blocks(blocks: List[MatchingBlock]) -> str:
    """
    Format matching blocks as readable text.

    Args:
        blocks: List of MatchingBlock objects

    Returns:
        Formatted string describing matches
    """
    if not blocks:
        return "No matching blocks found."

    lines = [f"Found {len(blocks)} matching block(s):"]

    for i, block in enumerate(blocks, 1):
        content_preview = _truncate(block.content, 30) if block.content else "(content not captured)"
        lines.append(
            f"  {i}. Position {block.a_start}-{block.a_start + block.size} in A, "
            f"{block.b_start}-{block.b_start + block.size} in B: "
            f"'{content_preview}' ({block.size} chars)"
        )

    return "\n".join(lines)
