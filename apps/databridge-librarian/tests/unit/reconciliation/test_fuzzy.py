"""Unit tests for the FuzzyMatcher class."""

import pandas as pd
import pytest

from src.reconciliation.fuzzy import (
    FuzzyMatcher,
    MatchResult,
    DedupeResult,
    MatchCandidate,
    MatchMethod,
    match_columns,
)


class TestFuzzyMatcher:
    """Tests for FuzzyMatcher."""

    @pytest.fixture
    def matcher(self) -> FuzzyMatcher:
        """Create a FuzzyMatcher instance."""
        return FuzzyMatcher()

    # ==================== Basic Match Tests ====================

    def test_match_exact_values(self, matcher: FuzzyMatcher):
        """Test matching exact values."""
        source = ["Alice", "Bob", "Charlie"]
        target = ["Alice", "Bob", "Charlie"]

        result = matcher.match(source, target)

        assert result.success is True
        assert result.matched == 3
        assert all(m.score == 100.0 for m in result.matches)

    def test_match_similar_values(self, matcher: FuzzyMatcher):
        """Test matching similar values."""
        source = ["Alice Johnson", "Robert Smith"]
        target = ["Alice Johnsen", "Bob Smith"]  # Slight variations

        result = matcher.match(source, target)

        assert result.success is True
        # Alice Johnson should match Alice Johnsen
        assert result.matched >= 1

    def test_match_no_matches(self, matcher: FuzzyMatcher):
        """Test when no matches are found."""
        source = ["Alice", "Bob", "Charlie"]
        target = ["Xyz123", "Abc456", "Qwe789"]

        result = matcher.match(source, target)

        assert result.success is True
        assert result.matched == 0
        assert result.unmatched_source == 3

    def test_match_partial_matches(self, matcher: FuzzyMatcher):
        """Test partial matching."""
        matcher = FuzzyMatcher(threshold=70.0)

        source = ["Microsoft Corporation", "Apple Inc"]
        target = ["Microsoft Corp", "Apple Incorporated"]

        result = matcher.match(source, target)

        assert result.success is True
        assert result.matched >= 1

    def test_match_with_threshold(self):
        """Test matching with custom threshold."""
        # High threshold - only exact matches
        matcher_strict = FuzzyMatcher(threshold=95.0)
        source = ["Alice", "Bob"]
        target = ["Alice", "Bobby"]

        result_strict = matcher_strict.match(source, target)

        # Low threshold - more permissive
        matcher_loose = FuzzyMatcher(threshold=50.0)
        result_loose = matcher_loose.match(source, target)

        # Loose should match more
        assert result_loose.matched >= result_strict.matched

    # ==================== Match Method Tests ====================

    def test_match_method_ratio(self):
        """Test simple ratio matching."""
        matcher = FuzzyMatcher(method=MatchMethod.RATIO)
        source = ["hello world"]
        target = ["hello world", "goodbye world"]

        result = matcher.match(source, target)

        assert result.success is True
        assert result.matches[0].method == MatchMethod.RATIO

    def test_match_method_partial_ratio(self):
        """Test partial ratio matching."""
        matcher = FuzzyMatcher(method=MatchMethod.PARTIAL_RATIO, threshold=80.0)
        source = ["world"]
        target = ["hello world is great", "goodbye"]

        result = matcher.match(source, target)

        # Should find partial match
        assert result.success is True
        assert result.matched >= 1

    def test_match_method_token_sort(self):
        """Test token sort ratio matching."""
        matcher = FuzzyMatcher(method=MatchMethod.TOKEN_SORT, threshold=90.0)
        source = ["John Smith"]
        target = ["Smith John"]  # Same tokens, different order

        result = matcher.match(source, target)

        assert result.success is True
        assert result.matched == 1
        assert result.matches[0].score >= 90.0

    def test_match_method_token_set(self):
        """Test token set ratio matching."""
        matcher = FuzzyMatcher(method=MatchMethod.TOKEN_SET, threshold=80.0)
        source = ["New York City"]
        target = ["City of New York"]

        result = matcher.match(source, target)

        assert result.success is True
        assert result.matched == 1

    def test_match_method_weighted(self):
        """Test weighted ratio matching (default)."""
        matcher = FuzzyMatcher(method=MatchMethod.WEIGHTED)
        source = ["test value"]
        target = ["test value"]

        result = matcher.match(source, target)

        assert result.matches[0].method == MatchMethod.WEIGHTED

    # ==================== Configuration Tests ====================

    def test_match_case_sensitive(self):
        """Test case-sensitive matching."""
        matcher_sensitive = FuzzyMatcher(case_sensitive=True)
        matcher_insensitive = FuzzyMatcher(case_sensitive=False)

        source = ["ALICE"]
        target = ["alice"]

        result_sensitive = matcher_sensitive.match(source, target)
        result_insensitive = matcher_insensitive.match(source, target)

        # Case-insensitive should match better
        if result_sensitive.matched > 0 and result_insensitive.matched > 0:
            assert result_insensitive.matches[0].score >= result_sensitive.matches[0].score

    def test_match_trim_whitespace(self):
        """Test whitespace trimming."""
        matcher = FuzzyMatcher(trim_whitespace=True, threshold=100.0)
        source = ["  Alice  "]
        target = ["Alice"]

        result = matcher.match(source, target)

        assert result.matched == 1
        assert result.matches[0].score == 100.0

    def test_match_remove_punctuation(self):
        """Test punctuation removal."""
        matcher = FuzzyMatcher(remove_punctuation=True, threshold=100.0)
        source = ["Hello, World!"]
        target = ["Hello World"]

        result = matcher.match(source, target)

        assert result.matched == 1

    # ==================== DataFrame Match Tests ====================

    def test_match_dataframes(self, matcher: FuzzyMatcher):
        """Test matching DataFrames."""
        source_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice Johnson", "Bob Smith", "Charlie Brown"],
        })
        target_df = pd.DataFrame({
            "id": [10, 20, 30],
            "name": ["Alice Johnsen", "Robert Smith", "Charles Brown"],
        })

        result_df = matcher.match_dataframes(
            source_df,
            target_df,
            source_column="name",
            target_column="name",
        )

        assert isinstance(result_df, pd.DataFrame)
        assert "match_score" in result_df.columns
        assert "source_name" in result_df.columns
        assert "target_name" in result_df.columns

    def test_match_dataframes_include_unmatched(self, matcher: FuzzyMatcher):
        """Test DataFrame matching with unmatched records."""
        matcher = FuzzyMatcher(threshold=90.0)

        source_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Xyz123"],
        })
        target_df = pd.DataFrame({
            "id": [10],
            "name": ["Alice"],
        })

        result_df = matcher.match_dataframes(
            source_df,
            target_df,
            source_column="name",
            target_column="name",
            include_all_source=True,
        )

        # Should include all source records
        assert len(result_df) == 2
        # One should have null match_score
        assert result_df["match_score"].isna().sum() == 1

    # ==================== Deduplicate Tests ====================

    def test_deduplicate_exact(self, matcher: FuzzyMatcher):
        """Test deduplication with exact duplicates."""
        values = ["Alice", "Bob", "Alice", "Charlie", "Bob"]

        result = matcher.deduplicate(values)

        assert result.success is True
        assert result.duplicate_groups == 2  # Alice and Bob groups
        assert result.total_records == 5

    def test_deduplicate_fuzzy(self):
        """Test deduplication with fuzzy matches."""
        matcher = FuzzyMatcher(threshold=80.0)
        values = ["Alice Johnson", "Alice Johnsen", "Bob Smith", "Robert Smith"]

        result = matcher.deduplicate(values)

        assert result.success is True
        # Should find at least one duplicate group
        assert result.duplicate_groups >= 1

    def test_deduplicate_no_duplicates(self, matcher: FuzzyMatcher):
        """Test deduplication with no duplicates."""
        values = ["Alice", "Bob", "Charlie"]

        result = matcher.deduplicate(values)

        assert result.success is True
        assert result.duplicate_groups == 0
        assert result.unique_records == 3

    def test_deduplicate_dataframe(self, matcher: FuzzyMatcher):
        """Test DataFrame deduplication."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Alice", "Bob", "Charlie"],
        })

        deduped_df, result = matcher.deduplicate_dataframe(df, column="name", keep="first")

        assert result.success is True
        assert len(deduped_df) < len(df)
        assert "Alice" in deduped_df["name"].values

    def test_deduplicate_dataframe_keep_last(self, matcher: FuzzyMatcher):
        """Test DataFrame deduplication keeping last."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Alice"],
        })

        deduped_df, result = matcher.deduplicate_dataframe(df, column="name", keep="last")

        assert result.success is True
        # Should keep the last Alice (id=3)
        alice_row = deduped_df[deduped_df["name"] == "Alice"]
        assert alice_row["id"].values[0] == 3

    # ==================== Score Tests ====================

    def test_score_exact(self, matcher: FuzzyMatcher):
        """Test scoring exact match."""
        score = matcher.score("Alice", "Alice")

        assert score == 100.0

    def test_score_similar(self, matcher: FuzzyMatcher):
        """Test scoring similar strings."""
        score = matcher.score("Alice Johnson", "Alice Johnsen")

        assert 80.0 <= score <= 100.0

    def test_score_different(self, matcher: FuzzyMatcher):
        """Test scoring different strings."""
        score = matcher.score("Alice", "Xyz123")

        assert score < 50.0

    # ==================== Find Best Match Tests ====================

    def test_find_best_match(self, matcher: FuzzyMatcher):
        """Test finding best match."""
        value = "Alice Johnson"
        candidates = ["Alice Johnsen", "Bob Smith", "Charlie Brown"]

        match = matcher.find_best_match(value, candidates)

        assert match is not None
        assert match.target_value == "Alice Johnsen"
        assert match.score > 80.0

    def test_find_best_match_no_match(self, matcher: FuzzyMatcher):
        """Test finding best match with no match above threshold."""
        matcher = FuzzyMatcher(threshold=95.0)
        value = "Alice"
        candidates = ["Bob", "Charlie", "David"]

        match = matcher.find_best_match(value, candidates)

        assert match is None

    def test_find_best_match_with_series(self, matcher: FuzzyMatcher):
        """Test finding best match with pandas Series."""
        value = "Alice"
        candidates = pd.Series(["Alice", "Bob", "Charlie"])

        match = matcher.find_best_match(value, candidates)

        assert match is not None
        assert match.score == 100.0

    # ==================== Result Tests ====================

    def test_match_result_to_dict(self, matcher: FuzzyMatcher):
        """Test MatchResult to_dict method."""
        result = matcher.match(["Alice"], ["Alice"])
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "summary" in result_dict
        assert "match_rate" in result_dict
        assert "threshold" in result_dict
        assert "method" in result_dict

    def test_match_candidate_to_dict(self, matcher: FuzzyMatcher):
        """Test MatchCandidate to_dict method."""
        result = matcher.match(["Alice"], ["Alice"])
        match_dict = result.matches[0].to_dict()

        assert "source_value" in match_dict
        assert "target_value" in match_dict
        assert "score" in match_dict
        assert "method" in match_dict
        assert "additional_scores" in match_dict

    def test_match_result_get_matched_pairs(self, matcher: FuzzyMatcher):
        """Test getting matched pairs from result."""
        result = matcher.match(["Alice", "Bob"], ["Alice", "Bob"])
        pairs = result.get_matched_pairs()

        assert len(pairs) == 2
        assert all(isinstance(p, tuple) and len(p) == 2 for p in pairs)

    def test_match_result_get_unmatched_indices(self, matcher: FuzzyMatcher):
        """Test getting unmatched indices."""
        matcher = FuzzyMatcher(threshold=95.0)
        result = matcher.match(["Alice", "Xyz"], ["Alice", "Abc"])

        unmatched_source = result.get_unmatched_source_indices(2)
        unmatched_target = result.get_unmatched_target_indices(2)

        assert 1 in unmatched_source  # "Xyz" not matched
        assert 1 in unmatched_target  # "Abc" not matched

    def test_dedupe_result_to_dict(self, matcher: FuzzyMatcher):
        """Test DedupeResult to_dict method."""
        result = matcher.deduplicate(["Alice", "Alice", "Bob"])
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "summary" in result_dict
        assert "dedupe_rate" in result_dict
        assert "groups" in result_dict


class TestMatchColumnsFunction:
    """Tests for match_columns helper function."""

    def test_match_columns_exact(self):
        """Test matching exact column names."""
        source_df = pd.DataFrame(columns=["id", "name", "value"])
        target_df = pd.DataFrame(columns=["id", "name", "value"])

        mapping = match_columns(source_df, target_df)

        assert mapping["id"] == "id"
        assert mapping["name"] == "name"
        assert mapping["value"] == "value"

    def test_match_columns_similar(self):
        """Test matching similar column names."""
        source_df = pd.DataFrame(columns=["customer_id", "customer_name"])
        target_df = pd.DataFrame(columns=["cust_id", "cust_name"])

        mapping = match_columns(source_df, target_df, threshold=70.0)

        # Should find matches despite naming differences
        assert len(mapping) >= 1


class TestFuzzyMatcherEdgeCases:
    """Edge case tests for FuzzyMatcher."""

    def test_empty_lists(self):
        """Test matching empty lists."""
        matcher = FuzzyMatcher()
        result = matcher.match([], [])

        assert result.success is True
        assert result.matched == 0

    def test_single_value(self):
        """Test matching single value."""
        matcher = FuzzyMatcher()
        result = matcher.match(["Alice"], ["Alice"])

        assert result.matched == 1

    def test_with_none_values(self):
        """Test handling None values."""
        matcher = FuzzyMatcher()
        result = matcher.match(["Alice", None, "Bob"], ["Alice", "Bob"])

        # Should handle None gracefully
        assert result.success is True

    def test_with_pandas_series(self):
        """Test matching with pandas Series."""
        matcher = FuzzyMatcher()
        source = pd.Series(["Alice", "Bob"])
        target = pd.Series(["Alice", "Bob"])

        result = matcher.match(source, target)

        assert result.matched == 2

    def test_with_custom_indices(self):
        """Test matching with custom indices."""
        matcher = FuzzyMatcher()
        source = ["Alice", "Bob"]
        target = ["Alice", "Bob"]

        result = matcher.match(
            source,
            target,
            source_indices=[100, 101],
            target_indices=[200, 201],
        )

        assert result.success is True
        assert result.matches[0].source_index in [100, 101]
        assert result.matches[0].target_index in [200, 201]

    def test_special_characters(self):
        """Test matching with special characters."""
        matcher = FuzzyMatcher()
        source = ["Test (123)", "Example [456]"]
        target = ["Test (123)", "Example [456]"]

        result = matcher.match(source, target)

        assert result.matched == 2

    def test_unicode_characters(self):
        """Test matching with unicode characters."""
        matcher = FuzzyMatcher()
        source = ["Héllo Wörld", "日本語テスト"]
        target = ["Héllo Wörld", "日本語テスト"]

        result = matcher.match(source, target)

        assert result.matched == 2

    def test_very_long_strings(self):
        """Test matching very long strings."""
        matcher = FuzzyMatcher()
        long_string = "A" * 10000
        source = [long_string]
        target = [long_string]

        result = matcher.match(source, target)

        assert result.matched == 1
        assert result.matches[0].score == 100.0
