"""Unit tests for the HashComparer class."""

import pandas as pd
import numpy as np
import pytest

from src.reconciliation.hasher import (
    HashComparer,
    CompareResult,
    RecordMatch,
    MatchStatus,
)


class TestHashComparer:
    """Tests for HashComparer."""

    @pytest.fixture
    def comparer(self) -> HashComparer:
        """Create a HashComparer instance."""
        return HashComparer()

    @pytest.fixture
    def source_df(self) -> pd.DataFrame:
        """Create a source DataFrame."""
        return pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "amount": [100.0, 200.0, 300.0, 400.0, 500.0],
        })

    @pytest.fixture
    def target_df(self) -> pd.DataFrame:
        """Create a target DataFrame matching source."""
        return pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "amount": [100.0, 200.0, 300.0, 400.0, 500.0],
        })

    # ==================== Basic Comparison Tests ====================

    def test_compare_identical_datasets(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test comparison of identical datasets."""
        result = comparer.compare(source_df, source_df.copy(), key_columns=["id"])

        assert result.success is True
        assert result.total_source == 5
        assert result.total_target == 5
        assert result.matched == 5
        assert result.orphan_source == 0
        assert result.orphan_target == 0
        assert result.conflicts == 0

    def test_compare_orphan_source(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test detection of orphan records in source."""
        target_df = source_df.iloc[:3].copy()  # Only first 3 records

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.success is True
        assert result.matched == 3
        assert result.orphan_source == 2  # Records 4 and 5 missing from target
        assert result.orphan_target == 0

    def test_compare_orphan_target(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test detection of orphan records in target."""
        target_df = pd.concat([
            source_df,
            pd.DataFrame({"id": [6, 7], "name": ["Frank", "Grace"], "amount": [600.0, 700.0]}),
        ], ignore_index=True)

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.success is True
        assert result.matched == 5
        assert result.orphan_source == 0
        assert result.orphan_target == 2  # Records 6 and 7 missing from source

    def test_compare_conflicts(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test detection of conflict records (same key, different values)."""
        target_df = source_df.copy()
        target_df.loc[target_df["id"] == 2, "amount"] = 999.0  # Change Bob's amount

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.success is True
        assert result.matched == 4
        assert result.conflicts == 1

    def test_compare_mixed_results(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test comparison with matches, orphans, and conflicts."""
        target_df = pd.DataFrame({
            "id": [1, 2, 6],  # 1 matches, 2 conflicts, 6 is new
            "name": ["Alice", "Bob", "Frank"],
            "amount": [100.0, 999.0, 600.0],  # Bob's amount changed
        })

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.success is True
        assert result.matched == 1  # Only Alice matches
        assert result.conflicts == 1  # Bob has conflict
        assert result.orphan_source == 3  # Charlie, David, Eve missing from target
        assert result.orphan_target == 1  # Frank missing from source

    # ==================== Key Column Tests ====================

    def test_compare_composite_key(self, comparer: HashComparer):
        """Test comparison with composite key."""
        source_df = pd.DataFrame({
            "dept": ["A", "A", "B", "B"],
            "emp_id": [1, 2, 1, 2],
            "salary": [1000, 2000, 3000, 4000],
        })
        target_df = source_df.copy()

        result = comparer.compare(source_df, target_df, key_columns=["dept", "emp_id"])

        assert result.success is True
        assert result.matched == 4

    def test_compare_missing_key_column(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test error when key column is missing."""
        result = comparer.compare(
            source_df,
            source_df.copy(),
            key_columns=["nonexistent"],
        )

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    # ==================== Value Column Tests ====================

    def test_compare_specific_value_columns(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test comparison with specific value columns."""
        target_df = source_df.copy()
        target_df.loc[target_df["id"] == 2, "amount"] = 999.0  # Change amount (monitored)
        target_df.loc[target_df["id"] == 2, "name"] = "Bobby"  # Change name (not monitored)

        result = comparer.compare(
            source_df,
            target_df,
            key_columns=["id"],
            value_columns=["amount"],  # Only monitor amount
        )

        assert result.conflicts == 1  # Bob has conflict in amount

    def test_compare_auto_value_columns(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test that value columns are auto-detected."""
        target_df = source_df.copy()
        target_df.loc[target_df["id"] == 2, "name"] = "Bobby"

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        # Should detect conflict in name column
        assert result.conflicts == 1

    # ==================== Configuration Tests ====================

    def test_compare_case_insensitive(self):
        """Test case-insensitive comparison."""
        comparer = HashComparer(case_sensitive=False)

        source_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target_df = pd.DataFrame({"id": [1], "name": ["ALICE"]})

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 1  # Should match despite case difference

    def test_compare_case_sensitive(self):
        """Test case-sensitive comparison."""
        comparer = HashComparer(case_sensitive=True)

        source_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target_df = pd.DataFrame({"id": [1], "name": ["ALICE"]})

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.conflicts == 1  # Should conflict due to case difference

    def test_compare_trim_whitespace(self):
        """Test whitespace trimming."""
        comparer = HashComparer(trim_whitespace=True)

        source_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target_df = pd.DataFrame({"id": [1], "name": ["  Alice  "]})

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 1  # Should match after trimming

    def test_compare_no_trim_whitespace(self):
        """Test without whitespace trimming."""
        comparer = HashComparer(trim_whitespace=False)

        source_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target_df = pd.DataFrame({"id": [1], "name": ["  Alice  "]})

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.conflicts == 1  # Should conflict due to whitespace

    def test_compare_different_algorithms(self):
        """Test different hash algorithms."""
        source_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        target_df = source_df.copy()

        for algorithm in ["md5", "sha1", "sha256"]:
            comparer = HashComparer(hash_algorithm=algorithm)
            result = comparer.compare(source_df, target_df, key_columns=["id"])
            assert result.matched == 1

    # ==================== Detail Tests ====================

    def test_compare_include_details(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test that details are included."""
        result = comparer.compare(
            source_df,
            source_df.copy(),
            key_columns=["id"],
            include_details=True,
        )

        assert len(result.matches) > 0
        assert all(m.status == MatchStatus.MATCH for m in result.matches)

    def test_compare_exclude_details(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test excluding details."""
        result = comparer.compare(
            source_df,
            source_df.copy(),
            key_columns=["id"],
            include_details=False,
        )

        # Details might be empty or limited
        # The counts should still be correct
        assert result.matched == 5

    def test_compare_max_details(self, comparer: HashComparer):
        """Test max_details limit."""
        source_df = pd.DataFrame({
            "id": list(range(100)),
            "value": list(range(100)),
        })

        result = comparer.compare(
            source_df,
            source_df.copy(),
            key_columns=["id"],
            include_details=True,
            max_details=10,
        )

        assert len(result.matches) <= 10

    def test_conflict_diff_columns(self, comparer: HashComparer):
        """Test that diff columns are identified."""
        source_df = pd.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "amount": [100.0],
            "status": ["active"],
        })
        target_df = pd.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "amount": [200.0],  # Changed
            "status": ["inactive"],  # Changed
        })

        result = comparer.compare(source_df, target_df, key_columns=["id"])
        conflicts = result.get_conflicts()

        assert len(conflicts) == 1
        assert "amount" in conflicts[0].diff_columns
        assert "status" in conflicts[0].diff_columns
        assert "name" not in conflicts[0].diff_columns

    # ==================== Null Handling Tests ====================

    def test_compare_with_nulls_in_key(self, comparer: HashComparer):
        """Test comparison with null values in key columns."""
        source_df = pd.DataFrame({
            "id": [1, None, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        target_df = pd.DataFrame({
            "id": [1, None, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.success is True

    def test_compare_with_nulls_in_values(self, comparer: HashComparer):
        """Test comparison with null values in value columns."""
        source_df = pd.DataFrame({
            "id": [1, 2],
            "value": [100.0, None],
        })
        target_df = pd.DataFrame({
            "id": [1, 2],
            "value": [100.0, None],
        })

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 2  # Both should match (null == null)

    def test_compare_null_vs_value(self, comparer: HashComparer):
        """Test comparison of null vs non-null values."""
        source_df = pd.DataFrame({
            "id": [1],
            "value": [None],
        })
        target_df = pd.DataFrame({
            "id": [1],
            "value": [0],
        })

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.conflicts == 1  # None != 0

    # ==================== Reconcile Method Tests ====================

    def test_reconcile_report(self, comparer: HashComparer):
        """Test reconcile method generates a report DataFrame."""
        source_df = pd.DataFrame({
            "id": [1, 2, 3],
            "amount": [100.0, 200.0, 300.0],
        })
        target_df = pd.DataFrame({
            "id": [1, 2, 4],
            "amount": [100.0, 250.0, 400.0],  # 2 conflicts
        })

        report_df = comparer.reconcile(source_df, target_df, key_columns=["id"])

        assert isinstance(report_df, pd.DataFrame)
        assert "status" in report_df.columns
        assert "key_id" in report_df.columns

    # ==================== Result Methods Tests ====================

    def test_compare_result_to_dict(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test CompareResult to_dict method."""
        result = comparer.compare(source_df, source_df.copy(), key_columns=["id"])
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "summary" in result_dict
        assert "match_rate" in result_dict
        assert result_dict["match_rate"] == 100.0

    def test_get_orphans_source(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test getting source orphans."""
        target_df = source_df.iloc[:3].copy()

        result = comparer.compare(source_df, target_df, key_columns=["id"])
        orphans = result.get_orphans_source()

        assert len(orphans) == 2
        assert all(o.status == MatchStatus.ORPHAN_SOURCE for o in orphans)

    def test_get_orphans_target(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test getting target orphans."""
        target_df = pd.concat([
            source_df,
            pd.DataFrame({"id": [6], "name": ["Frank"], "amount": [600.0]}),
        ], ignore_index=True)

        result = comparer.compare(source_df, target_df, key_columns=["id"])
        orphans = result.get_orphans_target()

        assert len(orphans) == 1
        assert orphans[0].key_values["id"] == 6

    def test_get_conflicts(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test getting conflict records."""
        target_df = source_df.copy()
        target_df.loc[target_df["id"] == 2, "amount"] = 999.0

        result = comparer.compare(source_df, target_df, key_columns=["id"])
        conflicts = result.get_conflicts()

        assert len(conflicts) == 1
        assert conflicts[0].key_values["id"] == 2

    def test_get_matches(self, comparer: HashComparer, source_df: pd.DataFrame):
        """Test getting matched records."""
        result = comparer.compare(source_df, source_df.copy(), key_columns=["id"])
        matches = result.get_matches()

        assert len(matches) == 5
        assert all(m.status == MatchStatus.MATCH for m in matches)


class TestHashComparerEdgeCases:
    """Edge case tests for HashComparer."""

    def test_empty_dataframes(self):
        """Test comparison of empty DataFrames."""
        comparer = HashComparer()
        source_df = pd.DataFrame(columns=["id", "name"])
        target_df = pd.DataFrame(columns=["id", "name"])

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.success is True
        assert result.matched == 0

    def test_single_row_dataframes(self):
        """Test comparison of single-row DataFrames."""
        comparer = HashComparer()
        source_df = pd.DataFrame({"id": [1], "name": ["Test"]})
        target_df = pd.DataFrame({"id": [1], "name": ["Test"]})

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 1

    def test_large_values(self):
        """Test comparison with very large values."""
        comparer = HashComparer()
        large_value = "A" * 10000

        source_df = pd.DataFrame({"id": [1], "text": [large_value]})
        target_df = source_df.copy()

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 1

    def test_numeric_types(self):
        """Test comparison with various numeric types."""
        comparer = HashComparer()

        source_df = pd.DataFrame({
            "id": [1],
            "int_val": [100],
            "float_val": [100.5],
        })
        target_df = pd.DataFrame({
            "id": [1],
            "int_val": [100],
            "float_val": [100.5],
        })

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 1

    def test_boolean_values(self):
        """Test comparison with boolean values."""
        comparer = HashComparer()

        source_df = pd.DataFrame({
            "id": [1, 2],
            "flag": [True, False],
        })
        target_df = source_df.copy()

        result = comparer.compare(source_df, target_df, key_columns=["id"])

        assert result.matched == 2
