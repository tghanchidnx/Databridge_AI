"""Tests for fuzzy matching functionality."""
import pytest
import json
import os
import pandas as pd

# Check if rapidfuzz is available
try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

# Import callable functions from test helpers (extracts underlying functions from MCP tools)
from test_helpers import fuzzy_match_columns, fuzzy_deduplicate


@pytest.mark.skipif(not RAPIDFUZZ_AVAILABLE, reason="RapidFuzz not installed")
class TestFuzzyMatchColumns:
    """Tests for the fuzzy_match_columns tool."""

    def test_fuzzy_match_similar_companies(self, fuzzy_csv, fuzzy_csv_variants):
        """Should find fuzzy matches between similar company names."""
        result = json.loads(fuzzy_match_columns(
            fuzzy_csv,
            fuzzy_csv_variants,
            "company",
            "company",
            threshold=60
        ))

        assert "error" not in result
        assert result["total_matches"] >= 1
        assert "top_matches" in result

    def test_fuzzy_match_threshold(self, fuzzy_csv, fuzzy_csv_variants):
        """Higher threshold should return fewer matches."""
        result_low = json.loads(fuzzy_match_columns(
            fuzzy_csv, fuzzy_csv_variants, "company", "company", threshold=50
        ))
        result_high = json.loads(fuzzy_match_columns(
            fuzzy_csv, fuzzy_csv_variants, "company", "company", threshold=90
        ))

        assert result_low["total_matches"] >= result_high["total_matches"]

    def test_fuzzy_match_returns_similarity_score(self, fuzzy_csv, fuzzy_csv_variants):
        """Match results should include similarity scores."""
        result = json.loads(fuzzy_match_columns(
            fuzzy_csv, fuzzy_csv_variants, "company", "company", threshold=50
        ))

        if result.get("top_matches"):
            for match in result["top_matches"]:
                assert "similarity" in match
                assert 0 <= match["similarity"] <= 100


@pytest.mark.skipif(not RAPIDFUZZ_AVAILABLE, reason="RapidFuzz not installed")
class TestFuzzyDeduplicate:
    """Tests for the fuzzy_deduplicate tool."""

    def test_deduplicate_finds_similar(self, temp_dir):
        """Should find similar values within same column."""
        # Create CSV with near-duplicates
        df = pd.DataFrame({
            "name": [
                "John Smith",
                "Jon Smith",
                "John Smyth",
                "Jane Doe",
                "Mary Johnson"
            ]
        })
        path = os.path.join(temp_dir, "dupes.csv")
        df.to_csv(path, index=False)

        result = json.loads(fuzzy_deduplicate(path, "name", threshold=80))

        assert "error" not in result
        # Should find John variations as duplicates
        assert result["total_groups"] >= 1

    def test_deduplicate_respects_threshold(self, temp_dir):
        """Higher threshold should find fewer duplicates."""
        df = pd.DataFrame({
            "name": ["Apple Inc", "Apple Incorporated", "Apple", "Microsoft"]
        })
        path = os.path.join(temp_dir, "threshold_test.csv")
        df.to_csv(path, index=False)

        result_low = json.loads(fuzzy_deduplicate(path, "name", threshold=50))
        result_high = json.loads(fuzzy_deduplicate(path, "name", threshold=95))

        assert result_low["total_groups"] >= result_high["total_groups"]
