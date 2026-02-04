"""
Core difflib wrapper functions.

Provides a clean interface to Python's difflib module with
structured output suitable for MCP tools and AI agents.
"""

import difflib
from typing import List, Dict, Any, Optional, Tuple, Sequence
from .types import (
    DiffOpcode,
    MatchingBlock,
    TextDiffResult,
    ListDiffResult,
    DictDiffResult,
    DictValueDiff,
    SimilarStringMatch,
    TransformDiff,
)


def compute_similarity(a: str, b: str) -> float:
    """
    Compute similarity ratio between two strings.

    Args:
        a: First string
        b: Second string

    Returns:
        Similarity ratio from 0.0 (completely different) to 1.0 (identical)
    """
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0

    matcher = difflib.SequenceMatcher(None, a, b)
    return matcher.ratio()


def get_matching_blocks(a: str, b: str, include_content: bool = True) -> List[MatchingBlock]:
    """
    Find all matching blocks between two strings.

    Args:
        a: First string
        b: Second string
        include_content: Whether to include the matched content

    Returns:
        List of MatchingBlock objects
    """
    matcher = difflib.SequenceMatcher(None, a, b)
    blocks = []

    for block in matcher.get_matching_blocks():
        if block.size > 0:  # Skip the final dummy block
            content = a[block.a:block.a + block.size] if include_content else None
            blocks.append(MatchingBlock(
                a_start=block.a,
                b_start=block.b,
                size=block.size,
                content=content
            ))

    return blocks


def get_opcodes(a: str, b: str, include_content: bool = True) -> List[DiffOpcode]:
    """
    Get the sequence of operations to transform string a into string b.

    Args:
        a: First string
        b: Second string
        include_content: Whether to include the affected content

    Returns:
        List of DiffOpcode objects describing the transformation
    """
    matcher = difflib.SequenceMatcher(None, a, b)
    opcodes = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        a_content = a[i1:i2] if include_content else None
        b_content = b[j1:j2] if include_content else None

        opcodes.append(DiffOpcode(
            operation=tag,
            a_start=i1,
            a_end=i2,
            b_start=j1,
            b_end=j2,
            a_content=a_content,
            b_content=b_content
        ))

    return opcodes


def unified_diff(
    a: str,
    b: str,
    from_label: str = "a",
    to_label: str = "b",
    context_lines: int = 3
) -> str:
    """
    Generate unified diff format between two texts.

    Args:
        a: First text (can be multiline)
        b: Second text (can be multiline)
        from_label: Label for the source
        to_label: Label for the target
        context_lines: Number of context lines

    Returns:
        Unified diff as a string
    """
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)

    # Ensure lines end with newline for proper diff format
    if a_lines and not a_lines[-1].endswith('\n'):
        a_lines[-1] += '\n'
    if b_lines and not b_lines[-1].endswith('\n'):
        b_lines[-1] += '\n'

    diff = difflib.unified_diff(
        a_lines,
        b_lines,
        fromfile=from_label,
        tofile=to_label,
        n=context_lines
    )

    return ''.join(diff)


def context_diff(
    a: str,
    b: str,
    from_label: str = "a",
    to_label: str = "b",
    context_lines: int = 3
) -> str:
    """
    Generate context diff format between two texts.

    Args:
        a: First text (can be multiline)
        b: Second text (can be multiline)
        from_label: Label for the source
        to_label: Label for the target
        context_lines: Number of context lines

    Returns:
        Context diff as a string
    """
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)

    if a_lines and not a_lines[-1].endswith('\n'):
        a_lines[-1] += '\n'
    if b_lines and not b_lines[-1].endswith('\n'):
        b_lines[-1] += '\n'

    diff = difflib.context_diff(
        a_lines,
        b_lines,
        fromfile=from_label,
        tofile=to_label,
        n=context_lines
    )

    return ''.join(diff)


def ndiff_text(a: str, b: str) -> str:
    """
    Generate character-level diff with +/-/? markers.

    This provides a detailed character-by-character comparison
    showing insertions (+), deletions (-), and change indicators (?).

    Args:
        a: First string
        b: Second string

    Returns:
        ndiff formatted string
    """
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)

    # For single-line strings, treat as character sequences
    if len(a_lines) <= 1 and len(b_lines) <= 1:
        # Split into characters for character-level diff
        result = list(difflib.ndiff(list(a), list(b)))
        return ''.join(result)

    result = list(difflib.ndiff(a_lines, b_lines))
    return ''.join(result)


def diff_lists(a: List[Any], b: List[Any]) -> ListDiffResult:
    """
    Compare two lists and compute various similarity metrics.

    Args:
        a: First list
        b: Second list

    Returns:
        ListDiffResult with added, removed, common items and similarity scores
    """
    set_a = set(str(item) for item in a)
    set_b = set(str(item) for item in b)

    added = [item for item in b if str(item) not in set_a]
    removed = [item for item in a if str(item) not in set_b]
    common = [item for item in a if str(item) in set_b]

    # Jaccard similarity: |A ∩ B| / |A ∪ B|
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    jaccard = intersection / union if union > 0 else 1.0

    # Sequence similarity using SequenceMatcher
    str_a = [str(item) for item in a]
    str_b = [str(item) for item in b]
    matcher = difflib.SequenceMatcher(None, str_a, str_b)
    sequence_similarity = matcher.ratio()

    return ListDiffResult(
        list_a_count=len(a),
        list_b_count=len(b),
        added=added,
        removed=removed,
        common=common,
        added_count=len(added),
        removed_count=len(removed),
        common_count=len(common),
        jaccard_similarity=jaccard,
        jaccard_percent=f"{jaccard * 100:.1f}%",
        sequence_similarity=sequence_similarity
    )


def diff_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> DictDiffResult:
    """
    Compare two dictionaries with value-level character diffs.

    Args:
        a: First dictionary
        b: Second dictionary

    Returns:
        DictDiffResult with detailed per-key comparison
    """
    keys_a = set(a.keys())
    keys_b = set(b.keys())

    added_keys = list(keys_b - keys_a)
    removed_keys = list(keys_a - keys_b)
    common_keys = list(keys_a & keys_b)

    changed_keys = []
    unchanged_keys = []
    differences = []

    # Process removed keys
    for key in removed_keys:
        differences.append(DictValueDiff(
            key=key,
            value_a=a[key],
            value_b=None,
            status="removed"
        ))

    # Process added keys
    for key in added_keys:
        differences.append(DictValueDiff(
            key=key,
            value_a=None,
            value_b=b[key],
            status="added"
        ))

    # Process common keys
    for key in common_keys:
        val_a = a[key]
        val_b = b[key]

        if val_a == val_b:
            unchanged_keys.append(key)
            differences.append(DictValueDiff(
                key=key,
                value_a=val_a,
                value_b=val_b,
                status="unchanged"
            ))
        else:
            changed_keys.append(key)

            # If both are strings, compute detailed diff
            similarity = None
            opcodes = None
            if isinstance(val_a, str) and isinstance(val_b, str):
                similarity = compute_similarity(val_a, val_b)
                opcodes = get_opcodes(val_a, val_b)

            differences.append(DictValueDiff(
                key=key,
                value_a=val_a,
                value_b=val_b,
                status="changed",
                similarity=similarity,
                opcodes=opcodes
            ))

    # Compute overall similarity
    total_keys = len(keys_a | keys_b)
    if total_keys == 0:
        overall_similarity = 1.0
    else:
        # Weight: unchanged = 1.0, changed = avg string similarity, added/removed = 0
        score = len(unchanged_keys)
        for diff in differences:
            if diff.status == "changed" and diff.similarity is not None:
                score += diff.similarity
        overall_similarity = score / total_keys

    return DictDiffResult(
        dict_a_keys=len(keys_a),
        dict_b_keys=len(keys_b),
        added_keys=added_keys,
        removed_keys=removed_keys,
        common_keys=common_keys,
        changed_keys=changed_keys,
        unchanged_keys=unchanged_keys,
        differences=differences,
        overall_similarity=overall_similarity
    )


def diff_values_paired(
    before_values: List[Any],
    after_values: List[Any]
) -> List[TransformDiff]:
    """
    Compare paired before/after values for transform_column enhancement.

    Args:
        before_values: List of original values
        after_values: List of transformed values

    Returns:
        List of TransformDiff objects with character-level analysis
    """
    results = []

    for i, (before, after) in enumerate(zip(before_values, after_values)):
        before_str = str(before) if before is not None else ""
        after_str = str(after) if after is not None else ""

        similarity = compute_similarity(before_str, after_str)
        opcodes = get_opcodes(before_str, after_str)
        explanation = explain_diff_human_readable(before_str, after_str)

        results.append(TransformDiff(
            index=i,
            before=before_str,
            after=after_str,
            similarity=similarity,
            opcodes=opcodes,
            explanation=explanation
        ))

    return results


def explain_diff_human_readable(a: str, b: str) -> str:
    """
    Generate a human-readable explanation of differences.

    Args:
        a: First string
        b: Second string

    Returns:
        Natural language description of the differences
    """
    if a == b:
        return "Identical - no changes"

    if not a:
        return f"Added: '{b}'"

    if not b:
        return f"Removed: '{a}'"

    similarity = compute_similarity(a, b)
    opcodes = get_opcodes(a, b)

    lines = [f"Similarity: {similarity * 100:.1f}%"]

    changes = []
    for op in opcodes:
        if op.operation == "replace":
            changes.append(f"  Changed: '{op.a_content}' -> '{op.b_content}'")
        elif op.operation == "delete":
            changes.append(f"  Removed: '{op.a_content}'")
        elif op.operation == "insert":
            changes.append(f"  Added: '{op.b_content}'")

    if changes:
        lines.extend(changes)
    else:
        lines.append("  (whitespace or formatting changes only)")

    return "\n".join(lines)


def find_close_matches(
    word: str,
    candidates: List[str],
    n: int = 5,
    cutoff: float = 0.6
) -> List[SimilarStringMatch]:
    """
    Find similar strings from a list of candidates.

    Args:
        word: The target word to match
        candidates: List of candidate strings
        n: Maximum number of matches to return
        cutoff: Minimum similarity threshold (0.0 to 1.0)

    Returns:
        List of SimilarStringMatch objects sorted by similarity
    """
    # Get close matches using difflib
    matches = difflib.get_close_matches(word, candidates, n=n, cutoff=cutoff)

    # Compute exact similarity scores for ranking
    results = []
    for i, match in enumerate(matches):
        similarity = compute_similarity(word, match)
        results.append(SimilarStringMatch(
            candidate=match,
            similarity=similarity,
            similarity_percent=f"{similarity * 100:.1f}%",
            rank=i + 1
        ))

    return results


def quick_ratio(a: str, b: str) -> float:
    """
    Compute a quick (upper bound) similarity estimate.

    Faster than compute_similarity() but may overestimate.

    Args:
        a: First string
        b: Second string

    Returns:
        Upper bound similarity ratio
    """
    matcher = difflib.SequenceMatcher(None, a, b)
    return matcher.quick_ratio()


def real_quick_ratio(a: str, b: str) -> float:
    """
    Compute the fastest possible similarity estimate.

    Very fast but roughest estimate. Good for filtering.

    Args:
        a: First string
        b: Second string

    Returns:
        Upper bound similarity ratio
    """
    matcher = difflib.SequenceMatcher(None, a, b)
    return matcher.real_quick_ratio()
