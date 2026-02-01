"""Tests for the hashing and comparison engine."""
import pytest
import json
import os
import pandas as pd

# Import callable functions from test helpers (extracts underlying functions from MCP tools)
from test_helpers import compute_row_hash, compare_hashes, get_orphan_details, get_conflict_details


class TestRowHashing:
    """Tests for the compute_row_hash function."""

    def test_hash_consistency(self):
        """Same data should produce same hash."""
        row = pd.Series({"name": "Alice", "amount": 100})
        hash1 = compute_row_hash(row, ["name", "amount"])
        hash2 = compute_row_hash(row, ["name", "amount"])
        assert hash1 == hash2

    def test_hash_different_data(self):
        """Different data should produce different hash."""
        row1 = pd.Series({"name": "Alice", "amount": 100})
        row2 = pd.Series({"name": "Bob", "amount": 100})
        hash1 = compute_row_hash(row1, ["name", "amount"])
        hash2 = compute_row_hash(row2, ["name", "amount"])
        assert hash1 != hash2

    def test_hash_column_order_matters(self):
        """Column order should affect hash (deterministic)."""
        row = pd.Series({"name": "Alice", "amount": 100})
        hash1 = compute_row_hash(row, ["name", "amount"])
        hash2 = compute_row_hash(row, ["amount", "name"])
        assert hash1 != hash2

    def test_hash_length(self):
        """Hash should be truncated to 16 characters."""
        row = pd.Series({"name": "Alice", "amount": 100})
        hash_val = compute_row_hash(row, ["name", "amount"])
        assert len(hash_val) == 16


class TestCompareHashes:
    """Tests for the compare_hashes tool."""

    def test_compare_identical_files(self, sample_csv_a, temp_dir):
        """Comparing a file with itself should show 100% match."""
        result = json.loads(compare_hashes(sample_csv_a, sample_csv_a, "id"))

        assert "error" not in result
        assert result["statistics"]["exact_matches"] == 5
        assert result["statistics"]["conflicts"] == 0
        assert result["statistics"]["total_orphans"] == 0
        assert result["statistics"]["match_rate_percent"] == 100.0

    def test_compare_with_orphans(self, sample_csv_a, sample_csv_b):
        """Should detect orphans in both sources."""
        result = json.loads(compare_hashes(sample_csv_a, sample_csv_b, "id"))

        assert "error" not in result
        # IDs 4,5 only in A; IDs 6,7 only in B
        assert result["statistics"]["orphans_only_in_source_a"] == 2
        assert result["statistics"]["orphans_only_in_source_b"] == 2
        assert result["statistics"]["total_orphans"] == 4

    def test_compare_with_conflicts(self, sample_csv_a, sample_csv_b):
        """Should detect conflicts where keys match but values differ."""
        result = json.loads(compare_hashes(sample_csv_a, sample_csv_b, "id", "name,amount"))

        assert "error" not in result
        # ID 2 has different name (Bob vs Bobby) and amount (200 vs 250)
        assert result["statistics"]["conflicts"] >= 1

    def test_compare_invalid_column(self, sample_csv_a, sample_csv_b):
        """Should return error for invalid column name."""
        result = json.loads(compare_hashes(sample_csv_a, sample_csv_b, "nonexistent"))

        assert "error" in result

    def test_compare_missing_file(self):
        """Should return error for missing file."""
        result = json.loads(compare_hashes("missing_a.csv", "missing_b.csv", "id"))

        assert "error" in result


class TestOrphanDetails:
    """Tests for get_orphan_details tool."""

    def test_get_orphans_in_source_a(self, sample_csv_a, sample_csv_b):
        """Should return orphan details from source A."""
        result = json.loads(get_orphan_details(sample_csv_a, sample_csv_b, "id", "a"))

        assert "error" not in result
        assert "orphans_in_a" in result
        assert result["orphans_in_a"]["total"] == 2

    def test_get_orphans_in_both(self, sample_csv_a, sample_csv_b):
        """Should return orphan details from both sources."""
        result = json.loads(get_orphan_details(sample_csv_a, sample_csv_b, "id", "both"))

        assert "error" not in result
        assert "orphans_in_a" in result
        assert "orphans_in_b" in result

    def test_orphan_limit(self, sample_csv_a, sample_csv_b):
        """Should respect the limit parameter."""
        result = json.loads(get_orphan_details(sample_csv_a, sample_csv_b, "id", "both", limit=1))

        assert "error" not in result
        assert len(result["orphans_in_a"]["sample"]) <= 1


class TestConflictDetails:
    """Tests for get_conflict_details tool."""

    def test_get_conflicts(self, sample_csv_a, sample_csv_b):
        """Should return conflict details with differences."""
        result = json.loads(get_conflict_details(sample_csv_a, sample_csv_b, "id", "name,amount"))

        assert "error" not in result
        assert result["total_conflicts"] >= 1

        if result["conflicts"]:
            conflict = result["conflicts"][0]
            assert "key" in conflict
            assert "differences" in conflict

    def test_conflict_shows_both_values(self, sample_csv_a, sample_csv_b):
        """Conflict details should show value_a and value_b."""
        result = json.loads(get_conflict_details(sample_csv_a, sample_csv_b, "id", "name,amount"))

        if result.get("conflicts"):
            for conflict in result["conflicts"]:
                for diff in conflict["differences"]:
                    assert "value_a" in diff
                    assert "value_b" in diff
                    assert "column" in diff
