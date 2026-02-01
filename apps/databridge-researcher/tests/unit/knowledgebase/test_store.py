"""
Unit tests for V4 KnowledgeBaseStore.

Tests knowledge base storage operations using ChromaDB.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import numpy as np

from src.knowledgebase.store import (
    KnowledgeBaseStore,
    KBCollectionType,
    KBDocument,
    KBSearchResult,
    KBResult,
    CHROMADB_AVAILABLE,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test persistence."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_chromadb():
    """Mock ChromaDB for tests that don't need real DB."""
    with patch("src.knowledgebase.store.CHROMADB_AVAILABLE", True):
        with patch("src.knowledgebase.store.SENTENCE_TRANSFORMERS_AVAILABLE", True):
            with patch("src.knowledgebase.store.chromadb") as mock_chroma:
                with patch("src.knowledgebase.store.SentenceTransformer") as mock_st:
                    mock_client = MagicMock()
                    mock_chroma.PersistentClient.return_value = mock_client

                    mock_model = MagicMock()
                    # Return numpy array so .tolist() works
                    mock_model.encode.return_value = np.array([0.1, 0.2, 0.3])
                    mock_st.return_value = mock_model

                    yield mock_chroma, mock_client, mock_st


class TestKBDocument:
    """Tests for KBDocument dataclass."""

    def test_create_document(self):
        """Test creating a document."""
        doc = KBDocument(
            id="test-id",
            content="Test content",
            category="accounting",
            title="Test Term",
            metadata={"related_terms": ["Term1", "Term2"]},
        )
        assert doc.id == "test-id"
        assert doc.content == "Test content"
        assert doc.category == "accounting"
        assert doc.title == "Test Term"

    def test_document_to_dict(self):
        """Test converting document to dictionary."""
        doc = KBDocument(
            id="test-id",
            content="Content",
            category="finance",
            title="Title",
        )
        result = doc.to_dict()
        assert result["id"] == "test-id"
        assert result["content"] == "Content"
        assert result["category"] == "finance"

    def test_document_default_values(self):
        """Test document with default values."""
        doc = KBDocument(id="id", content="content")
        assert doc.category == ""
        assert doc.title == ""
        assert doc.metadata == {}


class TestKBSearchResult:
    """Tests for KBSearchResult dataclass."""

    def test_create_search_result(self):
        """Test creating a search result."""
        result = KBSearchResult(
            id="doc-1",
            content="Matching content",
            score=0.95,
            category="accounting",
            title="GAAP",
            metadata={"related_terms": ["IFRS"]},
        )
        assert result.id == "doc-1"
        assert result.score == 0.95
        assert result.category == "accounting"

    def test_search_result_to_dict(self):
        """Test converting search result to dictionary."""
        result = KBSearchResult(
            id="doc-1",
            content="Content",
            score=0.8765,
            category="finance",
            title="Revenue",
        )
        dict_result = result.to_dict()
        assert dict_result["score"] == 0.8765  # Rounded to 4 decimals
        assert dict_result["title"] == "Revenue"


class TestKBResult:
    """Tests for KBResult dataclass."""

    def test_success_result(self):
        """Test successful result."""
        result = KBResult(
            success=True,
            message="Operation completed",
            data={"count": 10},
        )
        assert result.success is True
        assert result.message == "Operation completed"

    def test_failure_result(self):
        """Test failure result with errors."""
        result = KBResult(
            success=False,
            message="Operation failed",
            errors=["Error 1"],
        )
        assert result.success is False
        assert len(result.errors) == 1

    def test_result_to_dict(self):
        """Test converting result to dictionary."""
        result = KBResult(success=True, message="OK")
        dict_result = result.to_dict()
        assert "success" in dict_result
        assert "message" in dict_result
        assert "errors" in dict_result


class TestKBCollectionType:
    """Tests for KBCollectionType enum."""

    def test_collection_types_exist(self):
        """Test all expected collection types exist."""
        assert KBCollectionType.BUSINESS_GLOSSARY.value == "business_glossary"
        assert KBCollectionType.METRIC_DEFINITIONS.value == "metric_definitions"
        assert KBCollectionType.QUERY_PATTERNS.value == "query_patterns"
        assert KBCollectionType.INDUSTRY_TERMINOLOGY.value == "industry_terminology"
        assert KBCollectionType.FPA_CONCEPTS.value == "fpa_concepts"


class TestKnowledgeBaseStoreInit:
    """Tests for KnowledgeBaseStore initialization."""

    def test_init_without_chromadb(self):
        """Test init fails when ChromaDB not available."""
        with patch("src.knowledgebase.store.CHROMADB_AVAILABLE", False):
            with pytest.raises(ImportError) as exc_info:
                KnowledgeBaseStore()
            assert "chromadb is required" in str(exc_info.value)

    def test_collection_name_prefix(self, mock_chromadb):
        """Test collection name generation."""
        _, mock_client, _ = mock_chromadb
        store = KnowledgeBaseStore(persist_directory="/tmp/test")

        name = store._get_collection_name(KBCollectionType.BUSINESS_GLOSSARY)
        assert name == "databridge_v4_kb_business_glossary"

    def test_generate_id_deterministic(self, mock_chromadb):
        """Test ID generation is deterministic."""
        _, _, _ = mock_chromadb
        store = KnowledgeBaseStore(persist_directory="/tmp/test")

        id1 = store._generate_id("same content", "accounting")
        id2 = store._generate_id("same content", "accounting")
        assert id1 == id2

        id3 = store._generate_id("different content", "accounting")
        assert id1 != id3


class TestKnowledgeBaseStoreOperations:
    """Tests for KnowledgeBaseStore operations with mocked ChromaDB."""

    def test_add_document(self, mock_chromadb):
        """Test adding a single document."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.add_document(
            KBCollectionType.BUSINESS_GLOSSARY,
            content="GAAP: Generally Accepted Accounting Principles",
            category="accounting",
            title="GAAP",
            metadata={"related_terms": ["IFRS", "SEC"]},
        )

        assert result.success is True
        assert "id" in result.data

    def test_add_documents_batch(self, mock_chromadb):
        """Test adding multiple documents."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")

        docs = [
            KBDocument(
                id="term_gaap",
                content="GAAP definition",
                category="accounting",
                title="GAAP",
            ),
            KBDocument(
                id="term_ifrs",
                content="IFRS definition",
                category="accounting",
                title="IFRS",
            ),
        ]

        result = store.add_documents(KBCollectionType.BUSINESS_GLOSSARY, docs)

        assert result.success is True
        assert result.data["count"] == 2

    def test_search_documents(self, mock_chromadb):
        """Test searching documents."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.query.return_value = {
            "ids": [["term_gaap", "term_ifrs"]],
            "documents": [["GAAP definition", "IFRS definition"]],
            "metadatas": [[
                {"category": "accounting", "title": "GAAP"},
                {"category": "accounting", "title": "IFRS"},
            ]],
            "distances": [[0.1, 0.2]],
        }
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.search(
            KBCollectionType.BUSINESS_GLOSSARY,
            query="accounting standards",
            n_results=5,
        )

        assert result.success is True
        assert len(result.data) == 2
        # Score should be 1 - distance
        assert result.data[0]["score"] == 0.9  # 1 - 0.1

    def test_search_with_category_filter(self, mock_chromadb):
        """Test searching with category filter."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.query.return_value = {
            "ids": [["term_1"]],
            "documents": [["Finance term"]],
            "metadatas": [[{"category": "finance", "title": "Term"}]],
            "distances": [[0.15]],
        }
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.search(
            KBCollectionType.BUSINESS_GLOSSARY,
            query="test",
            category="finance",
        )

        assert result.success is True
        # Verify where clause was passed
        mock_collection.query.assert_called_once()
        call_args = mock_collection.query.call_args
        assert call_args.kwargs.get("where") == {"category": "finance"}

    def test_get_by_id(self, mock_chromadb):
        """Test getting document by ID."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.get.return_value = {
            "ids": ["term_gaap"],
            "documents": ["GAAP definition"],
            "metadatas": [{"category": "accounting", "title": "GAAP"}],
        }
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.get_by_id(
            KBCollectionType.BUSINESS_GLOSSARY,
            "term_gaap",
        )

        assert result.success is True
        assert result.data["id"] == "term_gaap"
        assert result.data["title"] == "GAAP"

    def test_get_by_id_not_found(self, mock_chromadb):
        """Test getting non-existent document."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.get.return_value = {
            "ids": [],
            "documents": [],
            "metadatas": [],
        }
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.get_by_id(
            KBCollectionType.BUSINESS_GLOSSARY,
            "nonexistent",
        )

        assert result.success is False
        assert "not found" in result.message

    def test_delete_document(self, mock_chromadb):
        """Test deleting a document."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.delete_document(
            KBCollectionType.BUSINESS_GLOSSARY,
            "term_gaap",
        )

        assert result.success is True
        mock_collection.delete.assert_called_once_with(ids=["term_gaap"])

    def test_get_collection_stats(self, mock_chromadb):
        """Test getting collection statistics."""
        _, mock_client, _ = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.count.return_value = 50
        mock_client.get_or_create_collection.return_value = mock_collection

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.get_collection_stats(KBCollectionType.BUSINESS_GLOSSARY)

        assert result.success is True
        assert result.data["count"] == 50
        assert result.data["collection"] == "business_glossary"

    def test_list_collections(self, mock_chromadb):
        """Test listing collections."""
        _, mock_client, _ = mock_chromadb

        mock_collection1 = MagicMock()
        mock_collection1.name = "databridge_v4_kb_business_glossary"
        mock_collection1.count.return_value = 20

        mock_collection2 = MagicMock()
        mock_collection2.name = "databridge_v4_kb_metric_definitions"
        mock_collection2.count.return_value = 15

        mock_client.list_collections.return_value = [mock_collection1, mock_collection2]

        store = KnowledgeBaseStore(persist_directory="/tmp/test")
        result = store.list_collections()

        assert result.success is True
        assert len(result.data) == 2


@pytest.mark.skipif(not CHROMADB_AVAILABLE, reason="ChromaDB not installed")
class TestKnowledgeBaseStoreIntegration:
    """Integration tests with real ChromaDB (skipped if not installed)."""

    def test_full_workflow(self, temp_dir):
        """Test complete workflow with real ChromaDB."""
        store = KnowledgeBaseStore(persist_directory=temp_dir)

        # Add documents
        result = store.add_document(
            KBCollectionType.BUSINESS_GLOSSARY,
            content="GAAP: Generally Accepted Accounting Principles",
            category="accounting",
            title="GAAP",
            document_id="term_gaap",
        )
        assert result.success is True

        result = store.add_document(
            KBCollectionType.BUSINESS_GLOSSARY,
            content="EBITDA: Earnings Before Interest, Taxes, Depreciation, and Amortization",
            category="finance",
            title="EBITDA",
            document_id="term_ebitda",
        )
        assert result.success is True

        # Get stats
        result = store.get_collection_stats(KBCollectionType.BUSINESS_GLOSSARY)
        assert result.success is True
        assert result.data["count"] == 2

        # Get by ID
        result = store.get_by_id(KBCollectionType.BUSINESS_GLOSSARY, "term_gaap")
        assert result.success is True
        assert "GAAP" in result.data["content"]

        # Delete
        result = store.delete_document(KBCollectionType.BUSINESS_GLOSSARY, "term_gaap")
        assert result.success is True

        # Verify deletion
        result = store.get_collection_stats(KBCollectionType.BUSINESS_GLOSSARY)
        assert result.data["count"] == 1
