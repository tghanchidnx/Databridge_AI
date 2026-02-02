"""
Unit tests for Librarian VectorStore.

Tests vector storage operations using ChromaDB.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
from pathlib import Path

from src.vectors.store import (
    VectorStore,
    CollectionType,
    Document,
    SearchResult,
    VectorStoreResult,
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
    with patch("src.vectors.store.CHROMADB_AVAILABLE", True):
        with patch("src.vectors.store.chromadb") as mock_chroma:
            mock_client = MagicMock()
            mock_chroma.PersistentClient.return_value = mock_client
            yield mock_chroma, mock_client


class TestDocument:
    """Tests for Document dataclass."""

    def test_create_document(self):
        """Test creating a document."""
        doc = Document(
            id="test-id",
            content="Test content",
            metadata={"category": "test"},
        )
        assert doc.id == "test-id"
        assert doc.content == "Test content"
        assert doc.metadata == {"category": "test"}

    def test_document_to_dict(self):
        """Test converting document to dictionary."""
        doc = Document(
            id="test-id",
            content="Test content",
            metadata={"key": "value"},
        )
        result = doc.to_dict()
        assert result["id"] == "test-id"
        assert result["content"] == "Test content"
        assert result["metadata"] == {"key": "value"}

    def test_document_default_metadata(self):
        """Test document with default empty metadata."""
        doc = Document(id="id", content="content")
        assert doc.metadata == {}


class TestSearchResult:
    """Tests for SearchResult dataclass."""

    def test_create_search_result(self):
        """Test creating a search result."""
        result = SearchResult(
            id="doc-1",
            content="Matching content",
            score=0.95,
            metadata={"type": "test"},
        )
        assert result.id == "doc-1"
        assert result.content == "Matching content"
        assert result.score == 0.95

    def test_search_result_to_dict(self):
        """Test converting search result to dictionary."""
        result = SearchResult(
            id="doc-1",
            content="Content",
            score=0.8765,
            metadata={},
        )
        dict_result = result.to_dict()
        assert dict_result["score"] == 0.8765  # Rounded to 4 decimals


class TestVectorStoreResult:
    """Tests for VectorStoreResult dataclass."""

    def test_success_result(self):
        """Test successful result."""
        result = VectorStoreResult(
            success=True,
            message="Operation completed",
            data={"count": 10},
        )
        assert result.success is True
        assert result.message == "Operation completed"
        assert result.data == {"count": 10}

    def test_failure_result(self):
        """Test failure result with errors."""
        result = VectorStoreResult(
            success=False,
            message="Operation failed",
            errors=["Error 1", "Error 2"],
        )
        assert result.success is False
        assert len(result.errors) == 2

    def test_result_to_dict(self):
        """Test converting result to dictionary."""
        result = VectorStoreResult(success=True, message="OK")
        dict_result = result.to_dict()
        assert "success" in dict_result
        assert "message" in dict_result
        assert "data" in dict_result
        assert "errors" in dict_result


class TestCollectionType:
    """Tests for CollectionType enum."""

    def test_collection_types_exist(self):
        """Test all expected collection types exist."""
        assert CollectionType.HIERARCHIES.value == "hierarchies"
        assert CollectionType.MAPPINGS.value == "mappings"
        assert CollectionType.FORMULAS.value == "formulas"
        assert CollectionType.INDUSTRY_PATTERNS.value == "industry_patterns"
        assert CollectionType.WHITEPAPER_CONCEPTS.value == "whitepaper_concepts"
        assert CollectionType.CLIENT_KNOWLEDGE.value == "client_knowledge"


class TestVectorStoreInit:
    """Tests for VectorStore initialization."""

    def test_init_without_chromadb(self):
        """Test init fails when ChromaDB not available."""
        with patch("src.vectors.store.CHROMADB_AVAILABLE", False):
            with pytest.raises(ImportError) as exc_info:
                VectorStore()
            assert "chromadb is required" in str(exc_info.value)

    def test_collection_name_prefix(self, mock_chromadb):
        """Test collection name generation."""
        _, mock_client = mock_chromadb
        store = VectorStore(persist_directory="/tmp/test")

        name = store._get_collection_name(CollectionType.HIERARCHIES)
        assert name == "databridge_librarian_hierarchies"

        name = store._get_collection_name("custom")
        assert name == "databridge_librarian_custom"

    def test_generate_id_deterministic(self, mock_chromadb):
        """Test ID generation is deterministic."""
        _, _ = mock_chromadb
        store = VectorStore(persist_directory="/tmp/test")

        id1 = store._generate_id("same content", {"key": "value"})
        id2 = store._generate_id("same content", {"key": "value"})
        assert id1 == id2

        id3 = store._generate_id("different content", {"key": "value"})
        assert id1 != id3


class TestVectorStoreOperations:
    """Tests for VectorStore operations with mocked ChromaDB."""

    def test_create_collection(self, mock_chromadb):
        """Test creating a collection."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.count.return_value = 0
        mock_client.get_or_create_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        result = store.create_collection(CollectionType.HIERARCHIES)

        assert result.success is True
        assert "created/loaded" in result.message

    def test_list_collections(self, mock_chromadb):
        """Test listing collections."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.name = "databridge_librarian_hierarchies"
        mock_collection.count.return_value = 10
        mock_collection.metadata = {}
        mock_client.list_collections.return_value = [mock_collection]

        store = VectorStore(persist_directory="/tmp/test")
        result = store.list_collections()

        assert result.success is True
        assert len(result.data) == 1
        assert result.data[0]["name"] == "databridge_librarian_hierarchies"

    def test_upsert_documents(self, mock_chromadb):
        """Test upserting documents."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.count.return_value = 0
        mock_client.get_or_create_collection.return_value = mock_collection
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store.create_collection(CollectionType.HIERARCHIES)

        docs = [
            Document(id="doc1", content="Content 1", metadata={"type": "test"}),
            Document(id="doc2", content="Content 2", metadata={"type": "test"}),
        ]

        result = store.upsert(CollectionType.HIERARCHIES, docs)

        assert result.success is True
        assert result.data["count"] == 2

    def test_search_documents(self, mock_chromadb):
        """Test searching documents."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.query.return_value = {
            "ids": [["doc1", "doc2"]],
            "documents": [["Content 1", "Content 2"]],
            "metadatas": [[{"type": "test"}, {"type": "test"}]],
            "distances": [[0.1, 0.2]],
        }
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = mock_collection

        result = store.search(
            CollectionType.HIERARCHIES,
            query="test query",
            n_results=5,
        )

        assert result.success is True
        assert len(result.data) == 2
        # Score should be 1 - distance
        assert result.data[0]["score"] == 0.9  # 1 - 0.1

    def test_search_collection_not_found(self, mock_chromadb):
        """Test search when collection doesn't exist."""
        _, mock_client = mock_chromadb
        mock_client.get_collection.side_effect = Exception("not found")

        store = VectorStore(persist_directory="/tmp/test")
        result = store.search(CollectionType.HIERARCHIES, query="test")

        assert result.success is False
        assert "not found" in result.message

    def test_get_by_id(self, mock_chromadb):
        """Test getting documents by ID."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.get.return_value = {
            "ids": ["doc1"],
            "documents": ["Content 1"],
            "metadatas": [{"type": "test"}],
        }
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = mock_collection

        result = store.get_by_id(CollectionType.HIERARCHIES, ["doc1"])

        assert result.success is True
        assert len(result.data) == 1
        assert result.data[0]["id"] == "doc1"

    def test_delete_documents_by_id(self, mock_chromadb):
        """Test deleting documents by ID."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = mock_collection

        result = store.delete_documents(
            CollectionType.HIERARCHIES,
            ids=["doc1", "doc2"],
        )

        assert result.success is True
        mock_collection.delete.assert_called_once_with(ids=["doc1", "doc2"])

    def test_delete_documents_by_filter(self, mock_chromadb):
        """Test deleting documents by metadata filter."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = mock_collection

        result = store.delete_documents(
            CollectionType.HIERARCHIES,
            where={"type": "old"},
        )

        assert result.success is True
        mock_collection.delete.assert_called_once_with(where={"type": "old"})

    def test_delete_documents_no_criteria(self, mock_chromadb):
        """Test delete fails without criteria."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = mock_collection

        result = store.delete_documents(CollectionType.HIERARCHIES)

        assert result.success is False
        assert "must be provided" in result.message

    def test_get_collection_stats(self, mock_chromadb):
        """Test getting collection statistics."""
        _, mock_client = mock_chromadb

        mock_collection = MagicMock()
        mock_collection.name = "databridge_librarian_hierarchies"
        mock_collection.count.return_value = 100
        mock_collection.metadata = {"description": "Test"}
        mock_client.get_collection.return_value = mock_collection

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = mock_collection

        result = store.get_collection_stats(CollectionType.HIERARCHIES)

        assert result.success is True
        assert result.data["count"] == 100

    def test_delete_collection(self, mock_chromadb):
        """Test deleting a collection."""
        _, mock_client = mock_chromadb

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["databridge_librarian_hierarchies"] = MagicMock()

        result = store.delete_collection(CollectionType.HIERARCHIES)

        assert result.success is True
        assert "databridge_librarian_hierarchies" not in store._collections

    def test_reset_store(self, mock_chromadb):
        """Test resetting the entire store."""
        _, mock_client = mock_chromadb

        store = VectorStore(persist_directory="/tmp/test")
        store._collections["test"] = MagicMock()

        result = store.reset()

        assert result.success is True
        assert len(store._collections) == 0
        mock_client.reset.assert_called_once()


@pytest.mark.skipif(not CHROMADB_AVAILABLE, reason="ChromaDB not installed")
class TestVectorStoreIntegration:
    """Integration tests with real ChromaDB (skipped if not installed)."""

    def test_full_workflow(self, temp_dir):
        """Test complete workflow with real ChromaDB."""
        store = VectorStore(persist_directory=temp_dir)

        # Create collection with metadata (ChromaDB requires non-empty metadata)
        result = store.create_collection(
            CollectionType.HIERARCHIES,
            metadata={"description": "Test hierarchies"}
        )
        assert result.success is True

        # Add documents
        docs = [
            Document(
                id="h1",
                content="Revenue by Region",
                metadata={"type": "hierarchy", "level": 1},
            ),
            Document(
                id="h2",
                content="Revenue by Product",
                metadata={"type": "hierarchy", "level": 1},
            ),
            Document(
                id="h3",
                content="Cost of Goods Sold",
                metadata={"type": "hierarchy", "level": 1},
            ),
        ]
        result = store.upsert(CollectionType.HIERARCHIES, docs)
        assert result.success is True

        # Get stats
        result = store.get_collection_stats(CollectionType.HIERARCHIES)
        assert result.success is True
        assert result.data["count"] == 3

        # Get by ID
        result = store.get_by_id(CollectionType.HIERARCHIES, ["h1"])
        assert result.success is True
        assert len(result.data) == 1

        # Delete document
        result = store.delete_documents(CollectionType.HIERARCHIES, ids=["h1"])
        assert result.success is True

        # Verify deletion
        result = store.get_collection_stats(CollectionType.HIERARCHIES)
        assert result.data["count"] == 2
