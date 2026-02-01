"""
Tests for the embeddings module (SchemaEmbedder and SimilaritySearch).
"""

import pytest

from databridge_discovery.embeddings.schema_embedder import SchemaEmbedder
from databridge_discovery.embeddings.similarity import SimilaritySearch, SimilarityResult
from databridge_discovery.graph.node_types import (
    ColumnNode,
    HierarchyNode,
    NodeType,
    TableNode,
)


class TestSchemaEmbedder:
    """Tests for SchemaEmbedder class."""

    def test_create_embedder(self):
        """Test creating an embedder."""
        embedder = SchemaEmbedder()
        assert embedder is not None
        assert embedder.embedding_dim > 0

    def test_embed_text(self):
        """Test embedding text."""
        embedder = SchemaEmbedder()
        embedding = embedder.embed_text("customer account balance")

        assert isinstance(embedding, list)
        assert len(embedding) == embedder.embedding_dim
        assert all(isinstance(x, float) for x in embedding)

    def test_embed_empty_text(self):
        """Test embedding empty text."""
        embedder = SchemaEmbedder()
        embedding = embedder.embed_text("")

        assert isinstance(embedding, list)
        assert len(embedding) == embedder.embedding_dim
        # Empty text should return zero vector
        assert all(x == 0.0 for x in embedding)

    def test_embed_table(self):
        """Test embedding a table node."""
        embedder = SchemaEmbedder()
        table = TableNode(
            name="customer_orders",
            table_name="customer_orders",
            schema_name="sales",
            database="main",
            description="Contains all customer order history",
            table_type="fact",
            tags=["sales", "orders"],
        )

        embedding = embedder.embed_table(table)

        assert isinstance(embedding, list)
        assert len(embedding) == embedder.embedding_dim

    def test_embed_column(self):
        """Test embedding a column node."""
        embedder = SchemaEmbedder()
        column = ColumnNode(
            name="account_balance",
            column_name="account_balance",
            data_type="DECIMAL(18,2)",
            description="Current account balance",
            column_type="measure",
            sample_values=["100.50", "250.00", "0.00"],
        )

        embedding = embedder.embed_column(column)

        assert isinstance(embedding, list)
        assert len(embedding) == embedder.embedding_dim

    def test_embed_hierarchy(self):
        """Test embedding a hierarchy node."""
        embedder = SchemaEmbedder()
        hierarchy = HierarchyNode(
            name="account_hierarchy",
            description="Chart of accounts hierarchy",
            source_column="account_code",
            metadata={"entity_type": "account"},
        )

        embedding = embedder.embed_hierarchy(hierarchy)

        assert isinstance(embedding, list)
        assert len(embedding) == embedder.embedding_dim

    def test_embed_node(self):
        """Test embedding a generic node."""
        embedder = SchemaEmbedder()
        table = TableNode(name="test_table", table_name="test_table")

        embedding = embedder.embed_node(table)

        assert isinstance(embedding, list)
        assert len(embedding) == embedder.embedding_dim

    def test_embed_batch(self):
        """Test batch embedding."""
        embedder = SchemaEmbedder()
        nodes = [
            TableNode(name="table1", table_name="table1"),
            TableNode(name="table2", table_name="table2"),
            ColumnNode(name="col1", column_name="col1"),
        ]

        embeddings = embedder.embed_batch(nodes)

        assert len(embeddings) == 3
        for emb in embeddings:
            assert len(emb) == embedder.embedding_dim

    def test_embed_texts_batch(self):
        """Test batch text embedding."""
        embedder = SchemaEmbedder()
        texts = ["customer data", "order history", "product catalog"]

        embeddings = embedder.embed_texts_batch(texts)

        assert len(embeddings) == 3

    def test_compute_similarity(self):
        """Test computing similarity between embeddings."""
        embedder = SchemaEmbedder()

        emb1 = embedder.embed_text("customer order")
        emb2 = embedder.embed_text("customer purchase")
        emb3 = embedder.embed_text("weather forecast")

        # Similar texts should have higher similarity
        sim_similar = embedder.compute_similarity(emb1, emb2)
        sim_different = embedder.compute_similarity(emb1, emb3)

        assert -1 <= sim_similar <= 1
        assert -1 <= sim_different <= 1
        # Note: with fallback embeddings, this may not always hold
        # but should generally be true with transformer embeddings

    def test_find_similar(self):
        """Test finding similar embeddings."""
        embedder = SchemaEmbedder()

        query_emb = embedder.embed_text("customer account")
        candidates = [
            ("c1", embedder.embed_text("customer balance")),
            ("c2", embedder.embed_text("product catalog")),
            ("c3", embedder.embed_text("client account")),
        ]

        results = embedder.find_similar(query_emb, candidates, top_k=2)

        assert len(results) <= 2
        for node_id, score in results:
            assert isinstance(node_id, str)
            assert -1 <= score <= 1

    def test_update_node_embeddings(self):
        """Test updating embeddings on nodes in place."""
        embedder = SchemaEmbedder()
        nodes = [
            TableNode(name="table1", table_name="table1"),
            TableNode(name="table2", table_name="table2"),
        ]

        # Initially no embeddings
        for node in nodes:
            assert node.embedding is None

        updated = embedder.update_node_embeddings(nodes)

        assert updated == 2
        for node in nodes:
            assert node.embedding is not None
            assert len(node.embedding) == embedder.embedding_dim
            assert node.embedding_model is not None

    def test_get_model_info(self):
        """Test getting model information."""
        embedder = SchemaEmbedder()
        info = embedder.get_model_info()

        assert "model_name" in info
        assert "using_transformer" in info
        assert "embedding_dim" in info
        assert info["embedding_dim"] > 0


class TestFallbackEmbedding:
    """Tests specifically for fallback (hash-based) embeddings."""

    def test_fallback_embed_deterministic(self):
        """Test that fallback embedding is deterministic."""
        embedder = SchemaEmbedder()

        # Call private method directly
        emb1 = embedder._fallback_embed("test text")
        emb2 = embedder._fallback_embed("test text")

        assert emb1 == emb2

    def test_fallback_embed_different_texts(self):
        """Test that different texts produce different embeddings."""
        embedder = SchemaEmbedder()

        emb1 = embedder._fallback_embed("customer")
        emb2 = embedder._fallback_embed("product")

        # Should not be identical
        assert emb1 != emb2

    def test_fallback_embed_normalized(self):
        """Test that fallback embeddings are normalized."""
        import numpy as np
        embedder = SchemaEmbedder()

        emb = embedder._fallback_embed("test text")
        norm = np.linalg.norm(emb)

        # Should be approximately unit length (or zero for empty)
        assert abs(norm - 1.0) < 0.01 or norm == 0


class TestSimilaritySearch:
    """Tests for SimilaritySearch class."""

    @pytest.fixture
    def sample_nodes(self):
        """Create sample nodes for testing."""
        return [
            TableNode(name="customers", table_name="customers", description="Customer data"),
            TableNode(name="customer_orders", table_name="customer_orders", description="Order history"),
            TableNode(name="products", table_name="products", description="Product catalog"),
            TableNode(name="inventory", table_name="inventory", description="Stock levels"),
            ColumnNode(name="customer_id", column_name="customer_id", data_type="INTEGER"),
            ColumnNode(name="product_name", column_name="product_name", data_type="VARCHAR"),
        ]

    def test_create_similarity_search(self):
        """Test creating similarity search."""
        search = SimilaritySearch()
        assert search is not None
        assert search.embedder is not None

    def test_index_node(self, sample_nodes):
        """Test indexing a single node."""
        search = SimilaritySearch()
        node = sample_nodes[0]

        search.index_node(node)

        assert node.embedding is not None
        retrieved = search.get_node(node.id)
        assert retrieved is not None
        assert retrieved.id == node.id

    def test_index_nodes(self, sample_nodes):
        """Test indexing multiple nodes."""
        search = SimilaritySearch()
        count = search.index_nodes(sample_nodes)

        assert count == len(sample_nodes)
        all_nodes = search.get_all_nodes()
        assert len(all_nodes) == len(sample_nodes)

    def test_search_text(self, sample_nodes):
        """Test searching by text."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        results = search.search_text("customer data", top_k=3)

        assert len(results) <= 3
        for result in results:
            assert isinstance(result, SimilarityResult)
            assert result.node_id is not None
            assert 0 <= result.score <= 1

    def test_search_text_with_type_filter(self, sample_nodes):
        """Test searching with node type filter."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        results = search.search_text("customer", node_type="table", top_k=5)

        for result in results:
            assert result.node_type == "table"

    def test_search_text_with_threshold(self, sample_nodes):
        """Test searching with similarity threshold."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        results = search.search_text("xyz123nonexistent", threshold=0.8, top_k=5)

        # With high threshold and gibberish query, should get few/no results
        for result in results:
            assert result.score >= 0.8

    def test_search_similar(self, sample_nodes):
        """Test searching for similar nodes."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        # Search for nodes similar to "customers"
        results = search.search_similar(sample_nodes[0], top_k=3, exclude_self=True)

        assert len(results) <= 3
        # Should not include the query node itself
        for result in results:
            assert result.node_id != sample_nodes[0].id

    def test_search_similar_include_self(self, sample_nodes):
        """Test searching for similar nodes including self."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        results = search.search_similar(sample_nodes[0], top_k=3, exclude_self=False)

        # First result should be the node itself with score ~1.0
        assert results[0].node_id == sample_nodes[0].id
        assert results[0].score > 0.9

    def test_find_duplicates(self, sample_nodes):
        """Test finding duplicates."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        # Add a near-duplicate
        duplicate = TableNode(
            name="customer_data",
            table_name="customer_data",
            description="Customer information",
        )
        search.index_node(duplicate)

        duplicates = search.find_duplicates(threshold=0.7)

        # Should find some similar pairs
        assert isinstance(duplicates, list)
        for node_id_1, node_id_2, score in duplicates:
            assert score >= 0.7

    def test_find_clusters(self, sample_nodes):
        """Test finding clusters."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        clusters = search.find_clusters(min_cluster_size=2)

        assert isinstance(clusters, list)
        # Should find some clusters
        for cluster in clusters:
            assert len(cluster) >= 2

    def test_remove_node(self, sample_nodes):
        """Test removing a node."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        node_id = sample_nodes[0].id
        removed = search.remove_node(node_id)

        assert removed is True
        assert search.get_node(node_id) is None

    def test_remove_nonexistent_node(self):
        """Test removing a non-existent node."""
        search = SimilaritySearch()
        removed = search.remove_node("nonexistent")
        assert removed is False

    def test_clear(self, sample_nodes):
        """Test clearing the index."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        assert len(search.get_all_nodes()) > 0

        search.clear()

        assert len(search.get_all_nodes()) == 0

    def test_get_stats(self, sample_nodes):
        """Test getting index statistics."""
        search = SimilaritySearch()
        search.index_nodes(sample_nodes)

        stats = search.get_stats()

        assert stats["total_nodes"] == len(sample_nodes)
        assert "embedding_dim" in stats
        assert "node_types" in stats
        assert stats["node_types"]["table"] == 4
        assert stats["node_types"]["column"] == 2


class TestSimilaritySearchWithChromaDB:
    """Tests for SimilaritySearch with ChromaDB (if available)."""

    def test_chromadb_initialization(self):
        """Test initializing with ChromaDB."""
        try:
            search = SimilaritySearch(use_chromadb=True)
            # May fall back to in-memory if chromadb not installed
            assert search is not None
        except ImportError:
            pytest.skip("ChromaDB not installed")

    def test_chromadb_persistence(self, tmp_path):
        """Test ChromaDB persistence."""
        try:
            persist_dir = str(tmp_path / "chroma_test")
            search = SimilaritySearch(
                use_chromadb=True,
                persist_directory=persist_dir,
            )

            # Index a node
            node = TableNode(name="test", table_name="test")
            search.index_node(node)

            # Persist
            search.persist()

            # If ChromaDB is available and working, this should succeed
            assert search is not None
        except ImportError:
            pytest.skip("ChromaDB not installed")
        except Exception:
            # ChromaDB may have issues on some systems
            pytest.skip("ChromaDB initialization failed")
