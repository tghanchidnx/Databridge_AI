"""Unit tests for the DataProfiler class."""

import pandas as pd
import numpy as np
import pytest

from src.reconciliation.profiler import DataProfiler, ProfileResult, ColumnProfile


class TestDataProfiler:
    """Tests for DataProfiler."""

    @pytest.fixture
    def profiler(self) -> DataProfiler:
        """Create a DataProfiler instance."""
        return DataProfiler()

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "amount": [100.50, 200.75, 300.25, 150.00, 250.00],
            "category": ["A", "B", "A", "B", "A"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "david@example.com",
                "eve@example.com",
            ],
        })

    @pytest.fixture
    def df_with_nulls(self) -> pd.DataFrame:
        """Create a DataFrame with null values."""
        return pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", None, "Charlie", "David", None],
            "amount": [100.0, None, 300.0, None, 250.0],
        })

    # ==================== Profile Tests ====================

    def test_profile_success(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test successful profiling."""
        result = profiler.profile(sample_df)

        assert result.success is True
        assert result.row_count == 5
        assert result.column_count == 5
        assert len(result.columns) == 5

    def test_profile_column_count(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test that all columns are profiled."""
        result = profiler.profile(sample_df)

        column_names = [c.name for c in result.columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "amount" in column_names
        assert "category" in column_names
        assert "email" in column_names

    def test_profile_numeric_stats(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test numeric column statistics."""
        result = profiler.profile(sample_df)
        amount_profile = result.get_column("amount")

        assert amount_profile is not None
        assert amount_profile.min_value == 100.50
        assert amount_profile.max_value == 300.25
        assert amount_profile.mean_value is not None
        assert amount_profile.median_value is not None
        assert amount_profile.std_value is not None

    def test_profile_string_stats(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test string column statistics."""
        result = profiler.profile(sample_df)
        name_profile = result.get_column("name")

        assert name_profile is not None
        assert name_profile.min_length is not None
        assert name_profile.max_length is not None
        assert name_profile.avg_length is not None

    def test_profile_null_detection(self, profiler: DataProfiler, df_with_nulls: pd.DataFrame):
        """Test null value detection."""
        result = profiler.profile(df_with_nulls)
        name_profile = result.get_column("name")

        assert name_profile is not None
        assert name_profile.null_count == 2
        assert name_profile.null_percentage == 40.0

    def test_profile_cardinality(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test cardinality calculation."""
        result = profiler.profile(sample_df)

        # id column should have cardinality of 1.0 (all unique)
        id_profile = result.get_column("id")
        assert id_profile is not None
        assert id_profile.cardinality == 1.0
        assert id_profile.is_unique == True  # Use == instead of 'is' for numpy bool

        # category column should have lower cardinality
        cat_profile = result.get_column("category")
        assert cat_profile is not None
        assert cat_profile.cardinality < 1.0
        assert cat_profile.is_unique == False  # Use == instead of 'is' for numpy bool

    def test_profile_constant_column(self, profiler: DataProfiler):
        """Test constant column detection."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "status": ["active", "active", "active"],
        })
        result = profiler.profile(df)
        status_profile = result.get_column("status")

        assert status_profile is not None
        assert status_profile.is_constant is True

    def test_profile_top_values(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test top values extraction."""
        result = profiler.profile(sample_df)
        cat_profile = result.get_column("category")

        assert cat_profile is not None
        assert len(cat_profile.top_values) > 0
        assert "value" in cat_profile.top_values[0]
        assert "count" in cat_profile.top_values[0]
        assert "percentage" in cat_profile.top_values[0]

    # ==================== Pattern Detection Tests ====================

    def test_detect_email_pattern(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test email pattern detection."""
        result = profiler.profile(sample_df)
        email_profile = result.get_column("email")

        assert email_profile is not None
        assert "email" in email_profile.patterns

    def test_detect_phone_pattern(self, profiler: DataProfiler):
        """Test phone pattern detection."""
        df = pd.DataFrame({
            "phone": ["123-456-7890", "234-567-8901", "345-678-9012"],
        })
        result = profiler.profile(df)
        phone_profile = result.get_column("phone")

        assert phone_profile is not None
        assert "phone" in phone_profile.patterns

    def test_detect_date_pattern(self, profiler: DataProfiler):
        """Test date pattern detection."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        })
        result = profiler.profile(df)
        date_profile = result.get_column("date")

        assert date_profile is not None
        assert "date_iso" in date_profile.patterns

    def test_detect_uuid_pattern(self, profiler: DataProfiler):
        """Test UUID pattern detection."""
        df = pd.DataFrame({
            "uuid": [
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
            ],
        })
        result = profiler.profile(df)
        uuid_profile = result.get_column("uuid")

        assert uuid_profile is not None
        assert "uuid" in uuid_profile.patterns

    def test_no_pattern_detection_option(self):
        """Test disabling pattern detection."""
        df = pd.DataFrame({
            "email": ["alice@example.com", "bob@example.com"],
        })
        profiler = DataProfiler(detect_patterns=False)
        result = profiler.profile(df)
        email_profile = result.get_column("email")

        assert email_profile is not None
        assert len(email_profile.patterns) == 0

    # ==================== Quality Score Tests ====================

    def test_quality_score_perfect(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test quality score for clean data."""
        result = profiler.profile(sample_df)

        # Clean data should have high quality scores
        assert result.overall_quality_score >= 90.0

    def test_quality_score_with_nulls(self, profiler: DataProfiler, df_with_nulls: pd.DataFrame):
        """Test quality score with null values."""
        result = profiler.profile(df_with_nulls)

        # Data with 40% nulls should have lower quality score
        assert result.overall_quality_score < 100.0

    def test_quality_score_with_duplicates(self, profiler: DataProfiler):
        """Test quality score with duplicate rows."""
        df = pd.DataFrame({
            "id": [1, 1, 2, 2, 3],
            "name": ["A", "A", "B", "B", "C"],
        })
        result = profiler.profile(df)

        # Duplicate rows should reduce quality score
        assert result.duplicate_rows == 2

    # ==================== Schema Comparison Tests ====================

    def test_compare_schemas_match(self, profiler: DataProfiler):
        """Test schema comparison with matching schemas."""
        df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        df2 = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})

        profile1 = profiler.profile(df1)
        profile2 = profiler.profile(df2)

        diff = profiler.compare_schemas(profile1, profile2)

        assert diff["schemas_match"] is True
        assert len(diff["only_in_first"]) == 0
        assert len(diff["only_in_second"]) == 0

    def test_compare_schemas_different_columns(self, profiler: DataProfiler):
        """Test schema comparison with different columns."""
        df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        df2 = pd.DataFrame({"id": [3, 4], "email": ["a@b.com", "c@d.com"]})

        profile1 = profiler.profile(df1)
        profile2 = profiler.profile(df2)

        diff = profiler.compare_schemas(profile1, profile2)

        assert diff["schemas_match"] is False
        assert "name" in diff["only_in_first"]
        assert "email" in diff["only_in_second"]

    def test_compare_schemas_type_mismatch(self, profiler: DataProfiler):
        """Test schema comparison with type mismatches."""
        df1 = pd.DataFrame({"value": [1, 2, 3]})
        df2 = pd.DataFrame({"value": ["1", "2", "3"]})

        profile1 = profiler.profile(df1)
        profile2 = profiler.profile(df2)

        diff = profiler.compare_schemas(profile1, profile2)

        assert len(diff["type_mismatches"]) > 0

    # ==================== Schema Drift Tests ====================

    def test_detect_drift_no_drift(self, profiler: DataProfiler):
        """Test drift detection with no drift."""
        df1 = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        df2 = pd.DataFrame({"id": [4, 5, 6], "name": ["D", "E", "F"]})

        baseline = profiler.profile(df1)
        current = profiler.profile(df2)

        drift = profiler.detect_schema_drift(baseline, current)

        assert drift["has_drift"] is False

    def test_detect_drift_null_increase(self, profiler: DataProfiler):
        """Test drift detection with null increase."""
        df1 = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})
        df2 = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["A", None, None, None, None]})

        baseline = profiler.profile(df1)
        current = profiler.profile(df2)

        drift = profiler.detect_schema_drift(baseline, current, thresholds={"null_increase": 10.0, "cardinality_change": 0.2, "type_change": True})

        assert drift["has_drift"] is True
        assert any(d["column"] == "name" for d in drift["column_drifts"])

    def test_detect_drift_column_added(self, profiler: DataProfiler):
        """Test drift detection with new column."""
        df1 = pd.DataFrame({"id": [1, 2, 3]})
        df2 = pd.DataFrame({"id": [1, 2, 3], "new_col": ["A", "B", "C"]})

        baseline = profiler.profile(df1)
        current = profiler.profile(df2)

        drift = profiler.detect_schema_drift(baseline, current)

        assert drift["has_drift"] is True
        assert "new_col" in drift["schema_changes"]["only_in_second"]

    # ==================== Result Methods Tests ====================

    def test_profile_result_to_dict(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test ProfileResult to_dict method."""
        result = profiler.profile(sample_df)
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "row_count" in result_dict
        assert "column_count" in result_dict
        assert "columns" in result_dict
        assert "overall_quality_score" in result_dict
        assert "memory_usage_mb" in result_dict

    def test_column_profile_to_dict(self, profiler: DataProfiler, sample_df: pd.DataFrame):
        """Test ColumnProfile to_dict method."""
        result = profiler.profile(sample_df)
        col_dict = result.columns[0].to_dict()

        assert "name" in col_dict
        assert "dtype" in col_dict
        assert "total_count" in col_dict
        assert "null_count" in col_dict
        assert "null_percentage" in col_dict
        assert "cardinality" in col_dict
        assert "data_quality_score" in col_dict


class TestDataProfilerEdgeCases:
    """Edge case tests for DataProfiler."""

    def test_empty_dataframe(self):
        """Test profiling an empty DataFrame."""
        df = pd.DataFrame()
        profiler = DataProfiler()
        result = profiler.profile(df)

        assert result.success is True
        assert result.row_count == 0
        assert result.column_count == 0

    def test_single_row_dataframe(self):
        """Test profiling a single-row DataFrame."""
        df = pd.DataFrame({"id": [1], "name": ["Test"]})
        profiler = DataProfiler()
        result = profiler.profile(df)

        assert result.success is True
        assert result.row_count == 1

    def test_all_null_column(self):
        """Test profiling a column with all null values."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "empty": [None, None, None],
        })
        profiler = DataProfiler()
        result = profiler.profile(df)
        empty_profile = result.get_column("empty")

        assert empty_profile is not None
        assert empty_profile.null_percentage == 100.0
        assert empty_profile.unique_count == 0

    def test_mixed_types_column(self):
        """Test profiling a column with mixed types."""
        df = pd.DataFrame({
            "mixed": [1, "two", 3.0, None],
        })
        profiler = DataProfiler()
        result = profiler.profile(df)

        assert result.success is True

    def test_large_dataframe_sampling(self):
        """Test that large DataFrames are sampled for pattern detection."""
        # Create a DataFrame larger than sample_size
        df = pd.DataFrame({
            "email": [f"user{i}@example.com" for i in range(15000)],
        })
        profiler = DataProfiler(sample_size=10000)
        result = profiler.profile(df)

        assert result.success is True
        email_profile = result.get_column("email")
        assert email_profile is not None
