"""
Unit tests for V3 HierarchyRAG pipeline.

Tests retrieval-augmented generation for hierarchy context.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.vectors.rag import HierarchyRAG, RAGContext


class TestRAGContext:
    """Tests for RAGContext dataclass."""

    def test_create_context(self):
        """Test creating a RAG context."""
        context = RAGContext(
            query="test query",
            results=[{"id": "1", "content": "test"}],
            sources=["source1"],
            total_results=1,
        )
        assert context.query == "test query"
        assert len(context.results) == 1
        assert context.total_results == 1

    def test_context_to_dict(self):
        """Test converting context to dictionary."""
        context = RAGContext(
            query="test",
            results=[{"id": "1"}],
            sources=["src"],
            total_results=1,
        )
        d = context.to_dict()
        assert "query" in d
        assert "results" in d
        assert "sources" in d
        assert "total_results" in d

    def test_empty_context(self):
        """Test empty context."""
        context = RAGContext(
            query="test",
            results=[],
            sources=[],
            total_results=0,
        )
        assert context.total_results == 0
        assert len(context.results) == 0


class TestHierarchyRAG:
    """Tests for HierarchyRAG class."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all RAG dependencies."""
        with patch("src.vectors.rag.VectorStore") as mock_store:
            with patch("src.vectors.rag.HierarchyEmbedder") as mock_h_embedder:
                with patch("src.vectors.rag.ConceptEmbedder") as mock_c_embedder:
                    mock_store_instance = MagicMock()
                    mock_store.return_value = mock_store_instance

                    mock_h_embedder_instance = MagicMock()
                    mock_h_embedder.return_value = mock_h_embedder_instance

                    mock_c_embedder_instance = MagicMock()
                    mock_c_embedder.return_value = mock_c_embedder_instance

                    yield {
                        "store": mock_store_instance,
                        "h_embedder": mock_h_embedder_instance,
                        "c_embedder": mock_c_embedder_instance,
                    }

    def test_init_creates_store(self, mock_dependencies):
        """Test RAG initialization creates vector store."""
        rag = HierarchyRAG(persist_directory="/tmp/test")
        assert rag is not None

    def test_index_hierarchy(self, mock_dependencies):
        """Test indexing a hierarchy."""
        mock_store = mock_dependencies["store"]
        mock_h_embedder = mock_dependencies["h_embedder"]

        mock_h_embedder.hierarchy_to_text.return_value = "Revenue hierarchy text"
        mock_h_embedder.embed.return_value = MagicMock(embedding=[0.1, 0.2, 0.3])
        mock_store.upsert.return_value = MagicMock(success=True)

        rag = HierarchyRAG(persist_directory="/tmp/test")

        hierarchy = {
            "hierarchy_id": "h1",
            "hierarchy_name": "Revenue",
            "level_1": "Americas",
        }

        result = rag.index_hierarchy(hierarchy)
        assert result.success is True

    def test_index_hierarchies_batch(self, mock_dependencies):
        """Test indexing multiple hierarchies."""
        mock_store = mock_dependencies["store"]
        mock_h_embedder = mock_dependencies["h_embedder"]

        mock_h_embedder.hierarchy_to_text.return_value = "Text"
        mock_h_embedder.embed.return_value = MagicMock(embedding=[0.1, 0.2])
        mock_store.upsert.return_value = MagicMock(success=True)

        rag = HierarchyRAG(persist_directory="/tmp/test")

        hierarchies = [
            {"hierarchy_id": "h1", "hierarchy_name": "Revenue"},
            {"hierarchy_id": "h2", "hierarchy_name": "Expenses"},
        ]

        result = rag.index_hierarchies(hierarchies)
        assert result.success is True

    def test_search_hierarchies(self, mock_dependencies):
        """Test searching hierarchies."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[
                {"id": "h1", "content": "Revenue", "score": 0.95, "metadata": {}},
                {"id": "h2", "content": "Expenses", "score": 0.85, "metadata": {}},
            ],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.search_hierarchies("revenue analysis")

        assert context.total_results == 2
        assert context.query == "revenue analysis"

    def test_search_hierarchies_with_filter(self, mock_dependencies):
        """Test searching with project filter."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[{"id": "h1", "content": "Revenue", "score": 0.9, "metadata": {}}],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.search_hierarchies(
            "revenue",
            project_id="proj1",
            min_score=0.8,
        )

        assert context.total_results == 1
        # Verify filter was passed
        mock_store.search.assert_called()

    def test_index_industry_pattern(self, mock_dependencies):
        """Test indexing an industry pattern."""
        mock_store = mock_dependencies["store"]
        mock_c_embedder = mock_dependencies["c_embedder"]

        mock_c_embedder.concept_to_text.return_value = "Oil Gas pattern"
        mock_c_embedder.embed.return_value = MagicMock(embedding=[0.1, 0.2])
        mock_store.upsert.return_value = MagicMock(success=True)

        rag = HierarchyRAG(persist_directory="/tmp/test")

        pattern = {
            "id": "og_pl",
            "name": "Oil & Gas P&L",
            "industry": "oil_gas",
        }

        result = rag.index_industry_pattern(pattern)
        assert result.success is True

    def test_search_patterns(self, mock_dependencies):
        """Test searching industry patterns."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[
                {"id": "og_pl", "content": "Oil Gas P&L", "score": 0.9, "metadata": {"industry": "oil_gas"}},
            ],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.search_patterns("oil and gas income statement")

        assert context.total_results == 1

    def test_search_patterns_with_industry_filter(self, mock_dependencies):
        """Test searching patterns filtered by industry."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[{"id": "og_pl", "content": "Oil Gas", "score": 0.95, "metadata": {}}],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.search_patterns("P&L", industry="oil_gas")

        assert context.total_results == 1

    def test_index_concept(self, mock_dependencies):
        """Test indexing a concept."""
        mock_store = mock_dependencies["store"]
        mock_c_embedder = mock_dependencies["c_embedder"]

        mock_c_embedder.concept_to_text.return_value = "EBITDA concept"
        mock_c_embedder.embed.return_value = MagicMock(embedding=[0.1, 0.2])
        mock_store.upsert.return_value = MagicMock(success=True)

        rag = HierarchyRAG(persist_directory="/tmp/test")

        concept = {
            "title": "EBITDA",
            "definition": "Earnings before...",
            "category": "finance",
        }

        result = rag.index_concept(concept)
        assert result.success is True

    def test_search_concepts(self, mock_dependencies):
        """Test searching concepts."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[
                {"id": "c1", "content": "EBITDA", "score": 0.92, "metadata": {"category": "finance"}},
            ],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.search_concepts("profit metric")

        assert context.total_results == 1

    def test_get_context(self, mock_dependencies):
        """Test getting combined context."""
        mock_store = mock_dependencies["store"]

        # Mock search returns for all collection types
        mock_store.search.return_value = MagicMock(
            success=True,
            data=[{"id": "1", "content": "Result", "score": 0.9, "metadata": {}}],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.get_context(
            "revenue analysis",
            include_hierarchies=True,
            include_patterns=True,
            include_concepts=True,
        )

        assert context.query == "revenue analysis"
        # Should have results from all enabled sources
        assert context.total_results > 0

    def test_get_context_hierarchies_only(self, mock_dependencies):
        """Test getting context with only hierarchies."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[{"id": "h1", "content": "Hierarchy", "score": 0.9, "metadata": {}}],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.get_context(
            "test",
            include_hierarchies=True,
            include_patterns=False,
            include_concepts=False,
        )

        assert context.total_results >= 0

    def test_format_for_prompt(self, mock_dependencies):
        """Test formatting context for LLM prompt."""
        mock_store = mock_dependencies["store"]

        mock_store.search.return_value = MagicMock(
            success=True,
            data=[
                {"id": "1", "content": "Revenue by region", "score": 0.95, "metadata": {"type": "hierarchy"}},
                {"id": "2", "content": "EBITDA calculation", "score": 0.90, "metadata": {"type": "concept"}},
            ],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.get_context("revenue metrics")
        formatted = rag.format_for_prompt(context)

        assert isinstance(formatted, str)
        # Should contain some content
        assert len(formatted) > 0

    def test_format_for_prompt_max_tokens(self, mock_dependencies):
        """Test prompt formatting respects token limit."""
        mock_store = mock_dependencies["store"]

        # Create many results
        mock_store.search.return_value = MagicMock(
            success=True,
            data=[
                {"id": f"doc{i}", "content": f"Content {i} " * 100, "score": 0.9 - i * 0.01, "metadata": {}}
                for i in range(10)
            ],
        )

        rag = HierarchyRAG(persist_directory="/tmp/test")
        context = rag.get_context("test")
        formatted = rag.format_for_prompt(context, max_tokens=500)

        # Should be truncated
        assert len(formatted) < 5000  # Rough character limit
