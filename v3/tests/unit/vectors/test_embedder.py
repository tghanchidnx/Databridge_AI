"""
Unit tests for V3 Embedder classes.

Tests embedding generation and hierarchy text conversion.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np

from src.vectors.embedder import (
    Embedder,
    HierarchyEmbedder,
    ConceptEmbedder,
    EmbeddingProvider,
    EmbeddingResult,
)


class TestEmbeddingResult:
    """Tests for EmbeddingResult dataclass."""

    def test_create_result(self):
        """Test creating an embedding result."""
        embeddings = [[0.1, 0.2, 0.3]]
        result = EmbeddingResult(
            success=True,
            embeddings=embeddings,
            dimension=3,
            model="test-model",
            count=1,
        )
        assert result.success is True
        assert result.embeddings == embeddings
        assert result.dimension == 3
        assert result.count == 1

    def test_result_to_dict(self):
        """Test converting to dictionary."""
        result = EmbeddingResult(
            success=True,
            embeddings=[[0.1, 0.2]],
            dimension=2,
            model="test",
            count=1,
        )
        d = result.to_dict()
        assert "success" in d
        assert "dimension" in d
        assert "model" in d
        assert "count" in d

    def test_failed_result(self):
        """Test failed embedding result."""
        result = EmbeddingResult(
            success=False,
            errors=["Failed to embed"],
        )
        assert result.success is False
        assert len(result.errors) == 1


class TestEmbeddingProvider:
    """Tests for EmbeddingProvider enum."""

    def test_providers_exist(self):
        """Test all providers exist."""
        assert EmbeddingProvider.SENTENCE_TRANSFORMERS.value == "sentence-transformers"
        assert EmbeddingProvider.OPENAI.value == "openai"


class TestEmbedder:
    """Tests for base Embedder class."""

    def test_embed_single_text(self):
        """Test embedding a single text."""
        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_model = MagicMock()
                mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3]])
                mock_st.return_value = mock_model

                embedder = Embedder()
                result = embedder.embed("test text")

                assert result.success is True
                assert len(result.embeddings) == 1
                assert len(result.embeddings[0]) == 3

    def test_embed_multiple_texts(self):
        """Test embedding multiple texts."""
        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_model = MagicMock()
                mock_model.encode.return_value = np.array([
                    [0.1, 0.2, 0.3],
                    [0.4, 0.5, 0.6],
                ])
                mock_st.return_value = mock_model

                embedder = Embedder()
                result = embedder.embed(["text 1", "text 2"])

                assert result.success is True
                assert len(result.embeddings) == 2

    def test_embedder_without_sentence_transformers(self):
        """Test error when sentence-transformers not available."""
        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", False):
            with pytest.raises(ImportError):
                Embedder()


class TestHierarchyEmbedder:
    """Tests for HierarchyEmbedder class."""

    def test_hierarchy_to_text(self):
        """Test converting hierarchy to text."""
        hierarchy = {
            "hierarchy_name": "Revenue",
            "description": "Total revenue by region",
            "level_1": "Americas",
            "level_2": "North America",
            "level_3": "USA",
            "formula_group": "SUM",
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_st.return_value = MagicMock()
                embedder = HierarchyEmbedder()
                text = embedder.hierarchy_to_text(hierarchy)

                assert "Revenue" in text
                assert "Americas" in text
                assert "North America" in text
                assert "USA" in text

    def test_hierarchy_to_text_minimal(self):
        """Test hierarchy to text with minimal fields."""
        hierarchy = {
            "hierarchy_name": "Simple",
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_st.return_value = MagicMock()
                embedder = HierarchyEmbedder()
                text = embedder.hierarchy_to_text(hierarchy)

                assert "Simple" in text

    def test_tree_to_text(self):
        """Test converting tree structure to text."""
        tree = {
            "name": "Root",
            "children": [
                {"name": "Child 1", "children": []},
                {"name": "Child 2", "children": [
                    {"name": "Grandchild", "children": []}
                ]},
            ],
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_st.return_value = MagicMock()
                embedder = HierarchyEmbedder()
                text = embedder.tree_to_text(tree)

                assert "Root" in text
                assert "Child 1" in text
                assert "Child 2" in text
                assert "Grandchild" in text

    def test_embed_hierarchy(self):
        """Test embedding a hierarchy."""
        hierarchy = {
            "hierarchy_name": "Revenue",
            "level_1": "Americas",
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_model = MagicMock()
                mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3]])
                mock_st.return_value = mock_model

                embedder = HierarchyEmbedder()
                result = embedder.embed_hierarchy(hierarchy)

                assert result.success is True
                assert len(result.embeddings) == 1


class TestConceptEmbedder:
    """Tests for ConceptEmbedder class."""

    def test_concept_to_text(self):
        """Test converting concept to text."""
        concept = {
            "title": "EBITDA",
            "definition": "Earnings Before Interest, Taxes, Depreciation, and Amortization",
            "category": "finance",
            "examples": ["EBITDA margin", "Adjusted EBITDA"],
            "related": ["Operating Income", "Cash Flow"],
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_st.return_value = MagicMock()
                embedder = ConceptEmbedder()
                text = embedder.concept_to_text(concept)

                assert "EBITDA" in text
                assert "Earnings Before Interest" in text
                assert "finance" in text

    def test_industry_pattern_to_text(self):
        """Test converting industry pattern to text."""
        pattern = {
            "name": "Oil & Gas P&L",
            "description": "Income statement for upstream operations",
            "industry": "oil_gas",
            "hierarchy_type": "accounting",
            "typical_levels": ["Company", "Region", "Asset"],
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_st.return_value = MagicMock()
                embedder = ConceptEmbedder()
                text = embedder.industry_pattern_to_text(pattern)

                assert "Oil & Gas P&L" in text
                assert "upstream operations" in text
                assert "oil_gas" in text

    def test_concept_to_text_minimal(self):
        """Test concept to text with minimal fields."""
        item = {
            "name": "Test Concept",
            "description": "A test description",
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_st.return_value = MagicMock()
                embedder = ConceptEmbedder()
                text = embedder.concept_to_text(item)

                assert "Test Concept" in text
                assert "test description" in text

    def test_embed_concept(self):
        """Test embedding a concept."""
        concept = {
            "title": "EBITDA",
            "definition": "Earnings metric",
            "category": "finance",
        }

        with patch("src.vectors.embedder.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.vectors.embedder.SentenceTransformer") as mock_st:
                mock_model = MagicMock()
                mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3]])
                mock_st.return_value = mock_model

                embedder = ConceptEmbedder()
                result = embedder.embed_concept(concept)

                assert result.success is True
                assert len(result.embeddings) == 1
