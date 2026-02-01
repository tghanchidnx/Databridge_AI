"""
Unit tests for V4 glossary module.

Tests business glossary, metric definitions, and FP&A concepts.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.knowledgebase.glossary import (
    BUSINESS_GLOSSARY,
    METRIC_DEFINITIONS,
    FPA_CONCEPTS,
    GlossaryLoader,
    get_glossary_terms,
    get_metric_definitions,
    get_fpa_concepts,
)


class TestBusinessGlossary:
    """Tests for BUSINESS_GLOSSARY data."""

    def test_glossary_not_empty(self):
        """Test glossary has terms."""
        assert len(BUSINESS_GLOSSARY) > 0

    def test_glossary_has_required_fields(self):
        """Test each term has required fields."""
        for term in BUSINESS_GLOSSARY:
            assert "term" in term, f"Missing 'term' field"
            assert "category" in term, f"Missing 'category' in {term.get('term')}"
            assert "definition" in term, f"Missing 'definition' in {term.get('term')}"

    def test_glossary_categories(self):
        """Test valid categories exist."""
        categories = {term["category"] for term in BUSINESS_GLOSSARY}
        expected_categories = {"accounting", "finance", "fpa", "analytics"}
        assert categories.issubset(expected_categories) or categories == expected_categories

    def test_glossary_terms_unique(self):
        """Test term names are unique."""
        terms = [term["term"] for term in BUSINESS_GLOSSARY]
        assert len(terms) == len(set(terms)), "Duplicate terms found"

    def test_known_terms_exist(self):
        """Test some expected terms exist."""
        term_names = {term["term"] for term in BUSINESS_GLOSSARY}
        expected_terms = ["GAAP", "EBITDA", "Working Capital", "KPI"]

        for expected in expected_terms:
            assert expected in term_names, f"Expected term '{expected}' not found"


class TestMetricDefinitions:
    """Tests for METRIC_DEFINITIONS data."""

    def test_metrics_not_empty(self):
        """Test metrics list has items."""
        assert len(METRIC_DEFINITIONS) > 0

    def test_metrics_have_required_fields(self):
        """Test each metric has required fields."""
        for metric in METRIC_DEFINITIONS:
            assert "name" in metric, f"Missing 'name' field"
            assert "category" in metric, f"Missing 'category' in {metric.get('name')}"
            assert "definition" in metric, f"Missing 'definition' in {metric.get('name')}"
            assert "formula" in metric, f"Missing 'formula' in {metric.get('name')}"
            assert "unit" in metric, f"Missing 'unit' in {metric.get('name')}"

    def test_metric_categories(self):
        """Test valid categories exist."""
        categories = {metric["category"] for metric in METRIC_DEFINITIONS}
        expected_categories = {"revenue", "profitability", "efficiency", "liquidity", "growth"}
        assert categories.issubset(expected_categories) or categories == expected_categories

    def test_metric_units(self):
        """Test valid units used."""
        units = {metric["unit"] for metric in METRIC_DEFINITIONS}
        valid_units = {"currency", "percentage", "ratio", "days"}
        assert units.issubset(valid_units), f"Unknown units found: {units - valid_units}"

    def test_metrics_unique(self):
        """Test metric names are unique."""
        names = [metric["name"] for metric in METRIC_DEFINITIONS]
        assert len(names) == len(set(names)), "Duplicate metrics found"

    def test_known_metrics_exist(self):
        """Test some expected metrics exist."""
        metric_names = {metric["name"] for metric in METRIC_DEFINITIONS}
        expected_metrics = ["Revenue", "Gross Margin", "EBITDA Margin", "DSO", "Current Ratio"]

        for expected in expected_metrics:
            assert expected in metric_names, f"Expected metric '{expected}' not found"


class TestFPAConcepts:
    """Tests for FPA_CONCEPTS data."""

    def test_concepts_not_empty(self):
        """Test concepts list has items."""
        assert len(FPA_CONCEPTS) > 0

    def test_concepts_have_required_fields(self):
        """Test each concept has required fields."""
        for concept in FPA_CONCEPTS:
            assert "concept" in concept, f"Missing 'concept' field"
            assert "category" in concept, f"Missing 'category' in {concept.get('concept')}"
            assert "description" in concept, f"Missing 'description' in {concept.get('concept')}"

    def test_concepts_have_use_cases(self):
        """Test concepts have use cases."""
        for concept in FPA_CONCEPTS:
            assert "use_cases" in concept, f"Missing 'use_cases' in {concept.get('concept')}"
            assert len(concept["use_cases"]) > 0, f"Empty use_cases in {concept.get('concept')}"

    def test_known_concepts_exist(self):
        """Test expected concepts exist."""
        concept_names = {c["concept"] for c in FPA_CONCEPTS}
        expected = ["Zero-Based Budgeting", "Three Statement Model"]

        for expected_concept in expected:
            assert expected_concept in concept_names, f"Expected '{expected_concept}' not found"


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_get_glossary_terms_returns_copy(self):
        """Test get_glossary_terms returns a copy."""
        terms1 = get_glossary_terms()
        terms2 = get_glossary_terms()

        # Should be equal
        assert terms1 == terms2

        # Should be different objects (copies)
        assert terms1 is not terms2

        # Modifying one shouldn't affect the other
        terms1.append({"term": "Test"})
        assert len(terms1) != len(terms2)

    def test_get_metric_definitions_returns_copy(self):
        """Test get_metric_definitions returns a copy."""
        metrics1 = get_metric_definitions()
        metrics2 = get_metric_definitions()

        assert metrics1 == metrics2
        assert metrics1 is not metrics2

    def test_get_fpa_concepts_returns_copy(self):
        """Test get_fpa_concepts returns a copy."""
        concepts1 = get_fpa_concepts()
        concepts2 = get_fpa_concepts()

        assert concepts1 == concepts2
        assert concepts1 is not concepts2


class TestGlossaryLoader:
    """Tests for GlossaryLoader class."""

    @pytest.fixture
    def mock_kb_store(self):
        """Create a mock knowledge base store."""
        mock_store = MagicMock()
        mock_store.add_documents.return_value = MagicMock(success=True)
        return mock_store

    def test_load_business_glossary(self, mock_kb_store):
        """Test loading business glossary."""
        loader = GlossaryLoader(mock_kb_store)
        result = loader.load_business_glossary()

        assert result["type"] == "business_glossary"
        assert result["loaded"] > 0
        assert result["success"] is True

        # Verify add_documents was called
        mock_kb_store.add_documents.assert_called_once()

    def test_load_metric_definitions(self, mock_kb_store):
        """Test loading metric definitions."""
        loader = GlossaryLoader(mock_kb_store)
        result = loader.load_metric_definitions()

        assert result["type"] == "metric_definitions"
        assert result["loaded"] > 0
        assert result["success"] is True

    def test_load_fpa_concepts(self, mock_kb_store):
        """Test loading FP&A concepts."""
        loader = GlossaryLoader(mock_kb_store)
        result = loader.load_fpa_concepts()

        assert result["type"] == "fpa_concepts"
        assert result["loaded"] > 0
        assert result["success"] is True

    def test_load_all(self, mock_kb_store):
        """Test loading all content."""
        loader = GlossaryLoader(mock_kb_store)
        result = loader.load_all()

        assert result["success"] is True
        assert result["total_loaded"] > 0
        assert "glossary" in result["details"]
        assert "metrics" in result["details"]
        assert "concepts" in result["details"]

        # Should have called add_documents 3 times
        assert mock_kb_store.add_documents.call_count == 3

    def test_load_all_with_failure(self, mock_kb_store):
        """Test load_all when one load fails."""
        mock_kb_store.add_documents.side_effect = [
            MagicMock(success=True),
            MagicMock(success=False),  # metrics fails
            MagicMock(success=True),
        ]

        loader = GlossaryLoader(mock_kb_store)
        result = loader.load_all()

        # Overall success should be False
        assert result["success"] is False

    def test_document_content_format(self, mock_kb_store):
        """Test document content is properly formatted."""
        loader = GlossaryLoader(mock_kb_store)
        loader.load_business_glossary()

        # Get the documents that were passed to add_documents
        call_args = mock_kb_store.add_documents.call_args
        documents = call_args[0][1]  # Second positional arg

        # Check first document
        doc = documents[0]
        assert doc.content  # Should have content
        assert doc.category  # Should have category
        assert doc.title  # Should have title

    def test_metric_document_has_formula(self, mock_kb_store):
        """Test metric documents include formula in content."""
        loader = GlossaryLoader(mock_kb_store)
        loader.load_metric_definitions()

        call_args = mock_kb_store.add_documents.call_args
        documents = call_args[0][1]

        # Check that formulas are included
        doc = documents[0]
        assert "Formula:" in doc.content or doc.metadata.get("formula")

    def test_document_ids_are_deterministic(self, mock_kb_store):
        """Test document IDs are based on content."""
        loader = GlossaryLoader(mock_kb_store)
        loader.load_business_glossary()

        call_args = mock_kb_store.add_documents.call_args
        documents = call_args[0][1]

        # IDs should be based on term name
        first_doc = documents[0]
        assert first_doc.id.startswith("term_")
