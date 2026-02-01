"""Tests for data loading functionality."""
import pytest
import json
import os

# Import callable functions from test helpers (extracts underlying functions from MCP tools)
from test_helpers import load_csv, load_json, profile_data, detect_schema_drift


class TestLoadCSV:
    """Tests for the load_csv tool."""

    def test_load_csv_success(self, sample_csv_a):
        """Should successfully load a CSV file."""
        result = json.loads(load_csv(sample_csv_a))

        assert "error" not in result
        assert result["rows"] == 5
        assert "id" in result["columns"]
        assert "name" in result["columns"]

    def test_load_csv_preview_limit(self, sample_csv_a):
        """Preview should respect row limit."""
        result = json.loads(load_csv(sample_csv_a, preview_rows=2))

        assert "error" not in result
        assert len(result["preview"]) == 2

    def test_load_csv_missing_file(self):
        """Should return error for missing file."""
        result = json.loads(load_csv("nonexistent.csv"))

        assert "error" in result

    def test_load_csv_includes_null_counts(self, sample_csv_a):
        """Should include null count information."""
        result = json.loads(load_csv(sample_csv_a))

        assert "null_counts" in result


class TestLoadJSON:
    """Tests for the load_json tool."""

    def test_load_json_array(self, sample_json):
        """Should successfully load a JSON array file."""
        result = json.loads(load_json(sample_json))

        assert "error" not in result
        assert result["rows"] == 3
        assert "id" in result["columns"]

    def test_load_json_preview_limit(self, sample_json):
        """Preview should respect row limit."""
        result = json.loads(load_json(sample_json, preview_rows=1))

        assert "error" not in result
        assert len(result["preview"]) == 1


class TestProfileData:
    """Tests for the profile_data tool."""

    def test_profile_returns_statistics(self, sample_csv_a):
        """Should return comprehensive statistics."""
        result = json.loads(profile_data(sample_csv_a))

        assert "error" not in result
        assert "rows" in result
        assert "columns" in result
        assert "structure_type" in result
        assert "data_quality" in result

    def test_profile_detects_potential_keys(self, sample_csv_a):
        """Should identify potential key columns."""
        result = json.loads(profile_data(sample_csv_a))

        assert "potential_key_columns" in result
        # ID should be high cardinality
        assert "id" in result["potential_key_columns"]

    def test_profile_includes_null_percentage(self, sample_csv_a):
        """Should include null percentage in data quality."""
        result = json.loads(profile_data(sample_csv_a))

        assert "null_percentage" in result["data_quality"]


class TestDetectSchemaDrift:
    """Tests for the detect_schema_drift tool."""

    def test_detect_no_drift_same_file(self, sample_csv_a):
        """Same file should show no drift."""
        result = json.loads(detect_schema_drift(sample_csv_a, sample_csv_a))

        assert "error" not in result
        assert result["has_drift"] is False
        assert len(result["columns_added"]) == 0
        assert len(result["columns_removed"]) == 0

    def test_detect_drift_different_schemas(self, sample_csv_a, temp_dir):
        """Should detect added and removed columns."""
        import pandas as pd

        # Create CSV with different schema
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "new_column": [1, 2, 3]
            # 'amount' and 'date' removed
        })
        path_b = os.path.join(temp_dir, "different_schema.csv")
        df.to_csv(path_b, index=False)

        result = json.loads(detect_schema_drift(sample_csv_a, path_b))

        assert "error" not in result
        assert result["has_drift"] is True
        assert "new_column" in result["columns_added"]
        assert "amount" in result["columns_removed"]
