"""
Tests for the diff utilities module.

Tests core difflib wrapper functions and MCP tools.
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from diff.core import (
    compute_similarity,
    get_matching_blocks,
    get_opcodes,
    unified_diff,
    context_diff,
    ndiff_text,
    diff_lists,
    diff_dicts,
    diff_values_paired,
    explain_diff_human_readable,
    find_close_matches,
)
from diff.types import (
    DiffOpcode,
    MatchingBlock,
    TextDiffResult,
    ListDiffResult,
    DictDiffResult,
    SimilarStringMatch,
)
from diff.formatters import (
    format_opcodes_json,
    format_opcodes_compact,
    format_diff_html,
    format_diff_text,
    format_explanation,
)


class TestComputeSimilarity:
    """Tests for compute_similarity function."""

    def test_identical_strings(self):
        assert compute_similarity("hello", "hello") == 1.0

    def test_completely_different(self):
        assert compute_similarity("abc", "xyz") == 0.0

    def test_partial_match(self):
        similarity = compute_similarity("John Smith", "Jon Smyth")
        assert 0.7 < similarity < 0.9  # difflib gives ~0.84 similarity

    def test_empty_strings(self):
        assert compute_similarity("", "") == 1.0
        assert compute_similarity("hello", "") == 0.0
        assert compute_similarity("", "hello") == 0.0

    def test_case_sensitive(self):
        # difflib is case-sensitive
        similarity = compute_similarity("Hello", "hello")
        assert similarity < 1.0


class TestGetMatchingBlocks:
    """Tests for get_matching_blocks function."""

    def test_identical_strings(self):
        blocks = get_matching_blocks("hello", "hello")
        assert len(blocks) == 1
        assert blocks[0].content == "hello"
        assert blocks[0].size == 5

    def test_partial_match(self):
        blocks = get_matching_blocks("John Smith", "Jon Smyth")
        # Should find "Jo", " Sm", "th" as matching blocks
        assert len(blocks) >= 3

    def test_no_match(self):
        blocks = get_matching_blocks("abc", "xyz")
        # No matching blocks (except possible empty final block)
        assert all(b.size == 0 for b in blocks) or len(blocks) == 0

    def test_content_included(self):
        blocks = get_matching_blocks("hello world", "hello there", include_content=True)
        assert any(b.content == "hello " for b in blocks)


class TestGetOpcodes:
    """Tests for get_opcodes function."""

    def test_identical_strings(self):
        opcodes = get_opcodes("hello", "hello")
        assert len(opcodes) == 1
        assert opcodes[0].operation == "equal"

    def test_replacement(self):
        opcodes = get_opcodes("cat", "hat")
        # Should have replace for 'c' -> 'h' and equal for 'at'
        operations = [op.operation for op in opcodes]
        assert "replace" in operations
        assert "equal" in operations

    def test_insertion(self):
        opcodes = get_opcodes("helo", "hello")
        operations = [op.operation for op in opcodes]
        assert "insert" in operations

    def test_deletion(self):
        opcodes = get_opcodes("hello", "helo")
        operations = [op.operation for op in opcodes]
        assert "delete" in operations

    def test_content_included(self):
        opcodes = get_opcodes("abc", "adc", include_content=True)
        for op in opcodes:
            if op.operation == "replace":
                assert op.a_content == "b"
                assert op.b_content == "d"


class TestUnifiedDiff:
    """Tests for unified_diff function."""

    def test_single_line_diff(self):
        diff = unified_diff("hello", "world", "a", "b")
        assert "---" in diff
        assert "+++" in diff
        assert "-hello" in diff
        assert "+world" in diff

    def test_multiline_diff(self):
        a = "line1\nline2\nline3"
        b = "line1\nmodified\nline3"
        diff = unified_diff(a, b)
        assert "-line2" in diff
        assert "+modified" in diff

    def test_identical_no_diff(self):
        diff = unified_diff("hello", "hello")
        assert diff == ""


class TestNdiffText:
    """Tests for ndiff_text function."""

    def test_character_diff(self):
        diff = ndiff_text("cat", "hat")
        assert "-" in diff or "+" in diff

    def test_identical(self):
        diff = ndiff_text("hello", "hello")
        # Should only have spaces (equal) markers
        assert "-" not in diff.replace("- ", "") or diff.count("-") == diff.count("- ")


class TestDiffLists:
    """Tests for diff_lists function."""

    def test_identical_lists(self):
        result = diff_lists([1, 2, 3], [1, 2, 3])
        assert result.added_count == 0
        assert result.removed_count == 0
        assert result.common_count == 3
        assert result.jaccard_similarity == 1.0

    def test_completely_different(self):
        result = diff_lists([1, 2], [3, 4])
        assert result.added_count == 2
        assert result.removed_count == 2
        assert result.common_count == 0
        assert result.jaccard_similarity == 0.0

    def test_partial_overlap(self):
        result = diff_lists(["a", "b", "c"], ["b", "c", "d"])
        assert "a" in result.removed
        assert "d" in result.added
        assert "b" in result.common
        assert "c" in result.common
        # Jaccard: 2/4 = 0.5
        assert result.jaccard_similarity == 0.5

    def test_empty_lists(self):
        result = diff_lists([], [])
        assert result.jaccard_similarity == 1.0


class TestDiffDicts:
    """Tests for diff_dicts function."""

    def test_identical_dicts(self):
        result = diff_dicts({"a": 1, "b": 2}, {"a": 1, "b": 2})
        assert len(result.added_keys) == 0
        assert len(result.removed_keys) == 0
        assert len(result.changed_keys) == 0
        assert result.overall_similarity == 1.0

    def test_added_key(self):
        result = diff_dicts({"a": 1}, {"a": 1, "b": 2})
        assert "b" in result.added_keys
        assert len(result.removed_keys) == 0

    def test_removed_key(self):
        result = diff_dicts({"a": 1, "b": 2}, {"a": 1})
        assert "b" in result.removed_keys
        assert len(result.added_keys) == 0

    def test_changed_value(self):
        result = diff_dicts({"name": "John"}, {"name": "Jane"})
        assert "name" in result.changed_keys
        # Find the diff for name
        name_diff = next(d for d in result.differences if d.key == "name")
        assert name_diff.status == "changed"
        assert name_diff.similarity is not None

    def test_string_value_diff(self):
        result = diff_dicts(
            {"name": "John Smith"},
            {"name": "Jon Smyth"}
        )
        name_diff = next(d for d in result.differences if d.key == "name")
        assert name_diff.opcodes is not None
        assert len(name_diff.opcodes) > 0


class TestDiffValuesPaired:
    """Tests for diff_values_paired function."""

    def test_basic_transform(self):
        before = ["hello", "world"]
        after = ["HELLO", "WORLD"]
        results = diff_values_paired(before, after)
        assert len(results) == 2
        assert results[0].before == "hello"
        assert results[0].after == "HELLO"
        assert results[0].similarity < 1.0

    def test_identical_values(self):
        before = ["test"]
        after = ["test"]
        results = diff_values_paired(before, after)
        assert results[0].similarity == 1.0


class TestExplainDiffHumanReadable:
    """Tests for explain_diff_human_readable function."""

    def test_identical(self):
        explanation = explain_diff_human_readable("hello", "hello")
        assert "Identical" in explanation or "no changes" in explanation.lower()

    def test_added(self):
        explanation = explain_diff_human_readable("", "hello")
        assert "Added" in explanation

    def test_removed(self):
        explanation = explain_diff_human_readable("hello", "")
        assert "Removed" in explanation

    def test_changed(self):
        explanation = explain_diff_human_readable("cat", "hat")
        assert "Changed" in explanation or "Similarity" in explanation


class TestFindCloseMatches:
    """Tests for find_close_matches function."""

    def test_exact_match(self):
        matches = find_close_matches("hello", ["hello", "world", "help"])
        assert len(matches) >= 1
        assert matches[0].candidate == "hello"
        assert matches[0].similarity == 1.0

    def test_close_match(self):
        matches = find_close_matches("helo", ["hello", "world", "help"])
        # "hello" should be a close match
        assert any(m.candidate == "hello" for m in matches)

    def test_no_match(self):
        matches = find_close_matches("xyz", ["hello", "world"], cutoff=0.9)
        assert len(matches) == 0

    def test_ranking(self):
        matches = find_close_matches("Revenue", ["Revnue", "Revenue Total", "Expenses"])
        # Matches should be ranked by similarity
        for i in range(len(matches) - 1):
            assert matches[i].similarity >= matches[i + 1].similarity
        # Ranks should be sequential
        for i, m in enumerate(matches):
            assert m.rank == i + 1


class TestFormatters:
    """Tests for formatters module."""

    def test_format_opcodes_json(self):
        opcodes = [
            DiffOpcode(
                operation="replace",
                a_start=0,
                a_end=1,
                b_start=0,
                b_end=1,
                a_content="a",
                b_content="b"
            )
        ]
        result = format_opcodes_json(opcodes)
        assert len(result) == 1
        assert result[0]["operation"] == "replace"

    def test_format_opcodes_compact(self):
        opcodes = [
            DiffOpcode(operation="equal", a_start=0, a_end=2, b_start=0, b_end=2, a_content="ab", b_content="ab"),
            DiffOpcode(operation="replace", a_start=2, a_end=3, b_start=2, b_end=3, a_content="c", b_content="d"),
        ]
        result = format_opcodes_compact(opcodes)
        # Should only include non-equal operations
        assert len(result) == 1
        assert result[0]["operation"] == "replace"

    def test_format_diff_html(self):
        opcodes = get_opcodes("abc", "adc")
        html = format_diff_html("abc", "adc", opcodes)
        assert "<del" in html or "<ins" in html
        assert "class=" in html

    def test_format_diff_text(self):
        opcodes = get_opcodes("abc", "adc")
        text = format_diff_text(opcodes)
        assert "-" in text or "+" in text


class TestMCPToolsIntegration:
    """Integration tests for MCP tools (if server is importable)."""

    @pytest.fixture
    def mock_mcp(self):
        """Create a mock MCP server for testing."""
        class MockTool:
            def __init__(self):
                self.tools = {}

            def tool(self):
                def decorator(func):
                    self.tools[func.__name__] = func
                    return func
                return decorator

        return MockTool()

    def test_register_diff_tools(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        # Should register 6 tools
        assert len(mock_mcp.tools) == 6
        assert "diff_text" in mock_mcp.tools
        assert "diff_lists" in mock_mcp.tools
        assert "diff_dicts" in mock_mcp.tools
        assert "find_similar_strings" in mock_mcp.tools
        assert "explain_diff" in mock_mcp.tools
        assert "generate_patch" in mock_mcp.tools

    def test_diff_text_tool(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        result = mock_mcp.tools["diff_text"]("John Smith", "Jon Smyth")
        assert "similarity" in result
        assert result["similarity"] > 0.7
        assert result["is_identical"] is False

    def test_diff_lists_tool(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        result = mock_mcp.tools["diff_lists"](["a", "b"], ["b", "c"])
        assert result["added"] == ["c"]
        assert result["removed"] == ["a"]
        assert result["common"] == ["b"]

    def test_diff_dicts_tool(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        result = mock_mcp.tools["diff_dicts"](
            {"name": "John"},
            {"name": "Jane", "age": 30}
        )
        assert "age" in result["added_keys"]
        assert "name" in result["changed_keys"]

    def test_find_similar_strings_tool(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        result = mock_mcp.tools["find_similar_strings"](
            "Revenue",
            ["Revnue", "Expenses", "Rev"]
        )
        assert result["matches_found"] >= 1
        assert result["matches"][0]["candidate"] == "Revnue"

    def test_explain_diff_tool(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        result = mock_mcp.tools["explain_diff"](
            "hello",
            "hallo",
            context="greeting"
        )
        assert "summary" in result
        assert "explanation" in result
        assert "greeting" in result["summary"]

    def test_generate_patch_tool(self, mock_mcp):
        from diff.mcp_tools import register_diff_tools
        register_diff_tools(mock_mcp)

        result = mock_mcp.tools["generate_patch"](
            "line1\nline2",
            "line1\nmodified"
        )
        assert result["format"] == "unified"
        assert result["has_changes"] is True
        assert "-line2" in result["patch"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
