"""
Unit tests for V3 industry patterns.

Tests pre-built hierarchy patterns for various industries.
"""

import pytest
from unittest.mock import MagicMock

from src.vectors.industry_patterns import (
    INDUSTRIES,
    INDUSTRY_PATTERNS,
    get_all_patterns,
    get_patterns_by_industry,
    get_pattern_by_id,
    get_industries,
    get_industry_info,
    IndustryPatternLoader,
)


class TestIndustries:
    """Tests for INDUSTRIES constant."""

    def test_industries_not_empty(self):
        """Test industries list is not empty."""
        assert len(INDUSTRIES) > 0

    def test_industries_have_required_fields(self):
        """Test each industry has required fields."""
        for industry_id, info in INDUSTRIES.items():
            assert "name" in info, f"Missing 'name' in {industry_id}"
            assert "description" in info, f"Missing 'description' in {industry_id}"

    def test_expected_industries_exist(self):
        """Test expected industries are present."""
        expected = ["oil_gas", "manufacturing", "healthcare", "private_equity", "retail", "construction"]
        for industry in expected:
            assert industry in INDUSTRIES, f"Expected industry '{industry}' not found"


class TestIndustryPatterns:
    """Tests for INDUSTRY_PATTERNS constant."""

    def test_patterns_not_empty(self):
        """Test patterns list is not empty."""
        assert len(INDUSTRY_PATTERNS) > 0

    def test_patterns_have_required_fields(self):
        """Test each pattern has required fields."""
        required_fields = ["id", "industry", "name", "hierarchy_type", "description"]
        for pattern in INDUSTRY_PATTERNS:
            for field in required_fields:
                assert field in pattern, f"Missing '{field}' in pattern {pattern.get('id')}"

    def test_patterns_have_valid_industry(self):
        """Test all patterns reference valid industries."""
        valid_industries = set(INDUSTRIES.keys())
        for pattern in INDUSTRY_PATTERNS:
            assert pattern["industry"] in valid_industries, (
                f"Invalid industry '{pattern['industry']}' in pattern {pattern['id']}"
            )

    def test_pattern_ids_unique(self):
        """Test pattern IDs are unique."""
        ids = [p["id"] for p in INDUSTRY_PATTERNS]
        assert len(ids) == len(set(ids)), "Duplicate pattern IDs found"

    def test_patterns_have_typical_levels(self):
        """Test patterns include typical hierarchy levels."""
        for pattern in INDUSTRY_PATTERNS:
            assert "typical_levels" in pattern, f"Missing 'typical_levels' in {pattern['id']}"
            assert len(pattern["typical_levels"]) > 0, f"Empty typical_levels in {pattern['id']}"


class TestGetAllPatterns:
    """Tests for get_all_patterns function."""

    def test_returns_all_patterns(self):
        """Test returns all patterns."""
        patterns = get_all_patterns()
        assert len(patterns) == len(INDUSTRY_PATTERNS)

    def test_returns_copy(self):
        """Test returns a copy, not original."""
        patterns1 = get_all_patterns()
        patterns2 = get_all_patterns()

        assert patterns1 == patterns2
        assert patterns1 is not patterns2

        # Modifying shouldn't affect original
        patterns1.append({"id": "test"})
        assert len(get_all_patterns()) == len(INDUSTRY_PATTERNS)


class TestGetPatternsByIndustry:
    """Tests for get_patterns_by_industry function."""

    def test_filters_by_industry(self):
        """Test filters patterns by industry."""
        oil_gas_patterns = get_patterns_by_industry("oil_gas")

        for pattern in oil_gas_patterns:
            assert pattern["industry"] == "oil_gas"

    def test_returns_empty_for_invalid_industry(self):
        """Test returns empty list for invalid industry."""
        patterns = get_patterns_by_industry("nonexistent_industry")
        assert patterns == []

    def test_manufacturing_patterns(self):
        """Test manufacturing patterns exist."""
        patterns = get_patterns_by_industry("manufacturing")
        assert len(patterns) > 0

    def test_healthcare_patterns(self):
        """Test healthcare patterns exist."""
        patterns = get_patterns_by_industry("healthcare")
        assert len(patterns) > 0


class TestGetPatternById:
    """Tests for get_pattern_by_id function."""

    def test_returns_pattern_by_id(self):
        """Test returns correct pattern."""
        # Get first pattern's ID
        first_pattern = INDUSTRY_PATTERNS[0]
        pattern = get_pattern_by_id(first_pattern["id"])

        assert pattern is not None
        assert pattern["id"] == first_pattern["id"]

    def test_returns_empty_for_invalid_id(self):
        """Test returns empty dict for invalid ID."""
        pattern = get_pattern_by_id("nonexistent_id")
        assert pattern == {}


class TestGetIndustries:
    """Tests for get_industries function."""

    def test_returns_industry_dict(self):
        """Test returns dict of industries."""
        industries = get_industries()
        assert isinstance(industries, dict)
        assert len(industries) > 0

    def test_industry_entries_have_name_and_description(self):
        """Test each entry has name and description."""
        industries = get_industries()
        for industry_id, info in industries.items():
            assert "name" in info
            assert "description" in info

    def test_includes_pattern_count(self):
        """Test includes pattern count."""
        industries = get_industries()
        for industry_id, info in industries.items():
            assert "pattern_count" in info
            assert info["pattern_count"] >= 0


class TestGetIndustryInfo:
    """Tests for get_industry_info function."""

    def test_returns_info_for_valid_industry(self):
        """Test returns info for valid industry."""
        info = get_industry_info("oil_gas")
        assert info is not None
        assert "name" in info
        assert "description" in info

    def test_returns_empty_for_invalid_industry(self):
        """Test returns empty dict for invalid industry."""
        info = get_industry_info("invalid")
        assert info == {}


class TestIndustryPatternLoader:
    """Tests for IndustryPatternLoader class."""

    @pytest.fixture
    def mock_rag(self):
        """Create mock RAG pipeline."""
        mock = MagicMock()
        mock.index_industry_pattern.return_value = MagicMock(success=True)
        return mock

    def test_load_all_patterns(self, mock_rag):
        """Test loading all patterns."""
        loader = IndustryPatternLoader(mock_rag)
        result = loader.load_all_patterns()

        assert result["total"] == len(INDUSTRY_PATTERNS)
        assert result["success"] == len(INDUSTRY_PATTERNS)
        assert mock_rag.index_industry_pattern.call_count == len(INDUSTRY_PATTERNS)

    def test_load_patterns_by_industry(self, mock_rag):
        """Test loading patterns for specific industry."""
        loader = IndustryPatternLoader(mock_rag)
        result = loader.load_patterns_by_industry("oil_gas")

        expected_count = len(get_patterns_by_industry("oil_gas"))
        assert result["success"] == expected_count
        assert result["industry"] == "oil_gas"

    def test_load_patterns_invalid_industry(self, mock_rag):
        """Test loading patterns for invalid industry."""
        loader = IndustryPatternLoader(mock_rag)
        result = loader.load_patterns_by_industry("invalid")

        assert result["total"] == 0
        assert result["success"] == 0

    def test_load_tracks_industries(self, mock_rag):
        """Test load_all_patterns tracks industries loaded."""
        loader = IndustryPatternLoader(mock_rag)
        result = loader.load_all_patterns()

        assert "by_industry" in result
        assert isinstance(result["by_industry"], dict)

    def test_handles_indexing_failure(self, mock_rag):
        """Test handles indexing failures gracefully."""
        mock_rag.index_industry_pattern.return_value = MagicMock(success=False)

        loader = IndustryPatternLoader(mock_rag)
        result = loader.load_all_patterns()

        # Should still complete, but with failures counted
        assert result["failed"] == len(INDUSTRY_PATTERNS)
        assert result["success"] == 0


class TestPatternContent:
    """Tests for pattern content quality."""

    def test_oil_gas_patterns_have_relevant_content(self):
        """Test oil & gas patterns have industry-relevant content."""
        patterns = get_patterns_by_industry("oil_gas")

        for pattern in patterns:
            content = pattern["description"].lower()
            # Should contain industry-relevant terms
            relevant_terms = ["oil", "gas", "production", "upstream", "lease", "well", "basin"]
            has_relevant = any(term in content for term in relevant_terms)
            assert has_relevant, f"Pattern {pattern['id']} lacks oil & gas content"

    def test_manufacturing_patterns_have_relevant_content(self):
        """Test manufacturing patterns have industry-relevant content."""
        patterns = get_patterns_by_industry("manufacturing")

        for pattern in patterns:
            content = pattern["description"].lower()
            relevant_terms = ["manufacturing", "production", "plant", "factory", "cost", "inventory"]
            has_relevant = any(term in content for term in relevant_terms)
            assert has_relevant, f"Pattern {pattern['id']} lacks manufacturing content"

    def test_patterns_have_meaningful_descriptions(self):
        """Test all patterns have meaningful descriptions."""
        for pattern in INDUSTRY_PATTERNS:
            desc = pattern["description"]
            assert len(desc) > 50, f"Pattern {pattern['id']} has too short description"
