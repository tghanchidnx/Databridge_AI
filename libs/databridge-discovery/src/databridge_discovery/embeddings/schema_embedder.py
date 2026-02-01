"""
Schema Embedder for generating vector embeddings of database elements.

This module provides embedding generation for tables, columns, and hierarchies
using sentence-transformers or simpler fallback methods.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import numpy as np

from databridge_discovery.graph.node_types import (
    ColumnNode,
    GraphNode,
    HierarchyNode,
    TableNode,
)


class SchemaEmbedder:
    """
    Generates vector embeddings for schema elements.

    Uses sentence-transformers when available, with fallback to
    simpler TF-IDF or hash-based embeddings.

    Example:
        embedder = SchemaEmbedder()

        # Embed a table
        embedding = embedder.embed_table(table_node)

        # Embed text description
        embedding = embedder.embed_text("user account information")

        # Batch embed
        embeddings = embedder.embed_batch([node1, node2, node3])
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        use_gpu: bool = False,
        fallback_dim: int = 384,
    ):
        """
        Initialize the schema embedder.

        Args:
            model_name: Sentence transformer model name
            use_gpu: Whether to use GPU for embeddings
            fallback_dim: Embedding dimension for fallback method
        """
        self.model_name = model_name
        self.fallback_dim = fallback_dim
        self._model = None
        self._use_transformer = False

        # Try to load sentence-transformers
        try:
            from sentence_transformers import SentenceTransformer
            device = "cuda" if use_gpu else "cpu"
            self._model = SentenceTransformer(model_name, device=device)
            self._use_transformer = True
            self.embedding_dim = self._model.get_sentence_embedding_dimension()
        except ImportError:
            self._use_transformer = False
            self.embedding_dim = fallback_dim

    def embed_text(self, text: str) -> list[float]:
        """
        Generate embedding for text.

        Args:
            text: Text to embed

        Returns:
            Embedding vector as list of floats
        """
        if not text:
            return [0.0] * self.embedding_dim

        if self._use_transformer and self._model:
            embedding = self._model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        else:
            return self._fallback_embed(text)

    def embed_node(self, node: GraphNode) -> list[float]:
        """
        Generate embedding for a graph node.

        Args:
            node: GraphNode to embed

        Returns:
            Embedding vector
        """
        text = self._node_to_text(node)
        return self.embed_text(text)

    def embed_table(self, table: TableNode) -> list[float]:
        """
        Generate embedding for a table node.

        Args:
            table: TableNode to embed

        Returns:
            Embedding vector
        """
        # Build rich text representation
        parts = [
            f"table {table.table_name}",
            f"in schema {table.schema_name}" if table.schema_name else "",
            f"in database {table.database}" if table.database else "",
            table.description or "",
            f"type {table.table_type}" if table.table_type != "unknown" else "",
        ]

        # Add tags
        if table.tags:
            parts.extend(table.tags)

        text = " ".join(p for p in parts if p)
        return self.embed_text(text)

    def embed_column(self, column: ColumnNode) -> list[float]:
        """
        Generate embedding for a column node.

        Args:
            column: ColumnNode to embed

        Returns:
            Embedding vector
        """
        # Build text representation
        parts = [
            f"column {column.column_name}",
            f"type {column.data_type}" if column.data_type != "unknown" else "",
            column.description or "",
            f"classification {column.column_type}" if column.column_type != "unknown" else "",
        ]

        # Add sample values if available
        if column.sample_values:
            parts.append(f"examples: {', '.join(column.sample_values[:5])}")

        text = " ".join(p for p in parts if p)
        return self.embed_text(text)

    def embed_hierarchy(self, hierarchy: HierarchyNode) -> list[float]:
        """
        Generate embedding for a hierarchy node.

        Args:
            hierarchy: HierarchyNode to embed

        Returns:
            Embedding vector
        """
        # Build text representation
        parts = [
            f"hierarchy {hierarchy.name}",
            hierarchy.description or "",
            f"source column {hierarchy.source_column}" if hierarchy.source_column else "",
            f"value {hierarchy.value}" if hierarchy.value else "",
        ]

        # Add metadata
        if hierarchy.metadata:
            entity_type = hierarchy.metadata.get("entity_type")
            if entity_type:
                parts.append(f"entity type {entity_type}")

        text = " ".join(p for p in parts if p)
        return self.embed_text(text)

    def embed_batch(self, nodes: list[GraphNode]) -> list[list[float]]:
        """
        Generate embeddings for multiple nodes.

        Args:
            nodes: List of GraphNodes

        Returns:
            List of embedding vectors
        """
        if not nodes:
            return []

        texts = [self._node_to_text(node) for node in nodes]

        if self._use_transformer and self._model:
            embeddings = self._model.encode(texts, convert_to_numpy=True)
            return embeddings.tolist()
        else:
            return [self._fallback_embed(text) for text in texts]

    def embed_texts_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Generate embeddings for multiple texts.

        Args:
            texts: List of texts

        Returns:
            List of embedding vectors
        """
        if not texts:
            return []

        if self._use_transformer and self._model:
            embeddings = self._model.encode(texts, convert_to_numpy=True)
            return embeddings.tolist()
        else:
            return [self._fallback_embed(text) for text in texts]

    def _node_to_text(self, node: GraphNode) -> str:
        """Convert a node to text representation for embedding."""
        if isinstance(node, TableNode):
            parts = [
                f"table {node.table_name}",
                node.schema_name or "",
                node.description or "",
            ]
        elif isinstance(node, ColumnNode):
            parts = [
                f"column {node.column_name}",
                node.data_type if node.data_type != "unknown" else "",
                node.column_type if node.column_type != "unknown" else "",
            ]
        elif isinstance(node, HierarchyNode):
            parts = [
                f"hierarchy {node.name}",
                node.source_column or "",
                node.value or "",
            ]
        else:
            parts = [
                node.name,
                node.description or "",
            ]

        # Add tags
        if node.tags:
            parts.extend(node.tags)

        return " ".join(p for p in parts if p)

    def _fallback_embed(self, text: str) -> list[float]:
        """
        Generate embedding using hash-based fallback method.

        This provides consistent embeddings without requiring
        sentence-transformers, suitable for exact matching.

        Args:
            text: Text to embed

        Returns:
            Embedding vector
        """
        # Normalize text
        text = text.lower().strip()
        text = re.sub(r'[^\w\s]', ' ', text)
        words = text.split()

        # Create embedding using word hashes
        embedding = np.zeros(self.fallback_dim)

        for word in words:
            # Hash the word
            word_hash = hashlib.md5(word.encode()).digest()

            # Convert to floats
            for i, byte in enumerate(word_hash):
                idx = i % self.fallback_dim
                embedding[idx] += (byte - 128) / 128.0

        # Normalize
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm

        return embedding.tolist()

    def compute_similarity(
        self,
        embedding1: list[float],
        embedding2: list[float],
    ) -> float:
        """
        Compute cosine similarity between two embeddings.

        Args:
            embedding1: First embedding
            embedding2: Second embedding

        Returns:
            Cosine similarity score (-1 to 1)
        """
        vec1 = np.array(embedding1)
        vec2 = np.array(embedding2)

        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(np.dot(vec1, vec2) / (norm1 * norm2))

    def find_similar(
        self,
        query_embedding: list[float],
        candidate_embeddings: list[tuple[str, list[float]]],
        top_k: int = 10,
        threshold: float = 0.0,
    ) -> list[tuple[str, float]]:
        """
        Find most similar embeddings to a query.

        Args:
            query_embedding: Query embedding
            candidate_embeddings: List of (id, embedding) tuples
            top_k: Number of results to return
            threshold: Minimum similarity threshold

        Returns:
            List of (id, similarity) tuples
        """
        results = []

        for cand_id, cand_embedding in candidate_embeddings:
            similarity = self.compute_similarity(query_embedding, cand_embedding)
            if similarity >= threshold:
                results.append((cand_id, similarity))

        # Sort by similarity descending
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    def update_node_embeddings(self, nodes: list[GraphNode]) -> int:
        """
        Update embeddings for nodes in place.

        Args:
            nodes: List of nodes to update

        Returns:
            Number of nodes updated
        """
        updated = 0

        for node in nodes:
            embedding = self.embed_node(node)
            node.embedding = embedding
            node.embedding_model = self.model_name if self._use_transformer else "hash"
            updated += 1

        return updated

    def get_model_info(self) -> dict[str, Any]:
        """
        Get information about the embedding model.

        Returns:
            Dictionary with model info
        """
        return {
            "model_name": self.model_name,
            "using_transformer": self._use_transformer,
            "embedding_dim": self.embedding_dim,
            "device": "cpu" if not self._use_transformer else (
                str(self._model.device) if self._model else "unknown"
            ),
        }
