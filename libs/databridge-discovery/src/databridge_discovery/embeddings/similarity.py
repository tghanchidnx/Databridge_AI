"""
Similarity Search for semantic matching across schema elements.

This module provides similarity search functionality using vector embeddings,
with optional ChromaDB integration for persistent vector storage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from databridge_discovery.embeddings.schema_embedder import SchemaEmbedder
from databridge_discovery.graph.node_types import GraphNode


@dataclass
class SimilarityResult:
    """Result from similarity search."""

    node_id: str
    node_name: str
    node_type: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


class SimilaritySearch:
    """
    Similarity search engine for schema elements.

    Provides semantic similarity search using vector embeddings,
    with optional ChromaDB integration for scalable vector storage.

    Example:
        search = SimilaritySearch()

        # Index nodes
        search.index_nodes(nodes)

        # Search by text
        results = search.search_text("customer account balance", top_k=10)

        # Search by node
        results = search.search_similar(node, top_k=5)

        # Find duplicates
        duplicates = search.find_duplicates(threshold=0.9)
    """

    def __init__(
        self,
        embedder: SchemaEmbedder | None = None,
        use_chromadb: bool = False,
        collection_name: str = "schema_elements",
        persist_directory: str | None = None,
    ):
        """
        Initialize similarity search.

        Args:
            embedder: SchemaEmbedder instance (creates one if not provided)
            use_chromadb: Whether to use ChromaDB for storage
            collection_name: ChromaDB collection name
            persist_directory: Directory for ChromaDB persistence
        """
        self.embedder = embedder or SchemaEmbedder()
        self._use_chromadb = use_chromadb
        self._collection_name = collection_name
        self._persist_directory = persist_directory

        # In-memory storage
        self._nodes: dict[str, GraphNode] = {}
        self._embeddings: dict[str, list[float]] = {}

        # ChromaDB client (lazy loaded)
        self._chroma_client = None
        self._collection = None

        if use_chromadb:
            self._init_chromadb()

    def _init_chromadb(self) -> None:
        """Initialize ChromaDB client and collection."""
        try:
            import chromadb
            from chromadb.config import Settings

            if self._persist_directory:
                self._chroma_client = chromadb.Client(Settings(
                    chroma_db_impl="duckdb+parquet",
                    persist_directory=self._persist_directory,
                    anonymized_telemetry=False,
                ))
            else:
                self._chroma_client = chromadb.Client(Settings(
                    chroma_db_impl="duckdb",
                    anonymized_telemetry=False,
                ))

            self._collection = self._chroma_client.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        except ImportError:
            self._use_chromadb = False
        except Exception:
            # ChromaDB initialization failed, fall back to in-memory
            self._use_chromadb = False

    def index_node(self, node: GraphNode) -> None:
        """
        Index a single node for similarity search.

        Args:
            node: GraphNode to index
        """
        # Generate embedding if not present
        if node.embedding:
            embedding = node.embedding
        else:
            embedding = self.embedder.embed_node(node)
            node.embedding = embedding

        # Store in memory
        self._nodes[node.id] = node
        self._embeddings[node.id] = embedding

        # Store in ChromaDB if available
        if self._use_chromadb and self._collection:
            self._collection.upsert(
                ids=[node.id],
                embeddings=[embedding],
                metadatas=[{
                    "name": node.name,
                    "node_type": node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type),
                    "description": node.description or "",
                }],
                documents=[self._node_to_document(node)],
            )

    def index_nodes(self, nodes: list[GraphNode]) -> int:
        """
        Index multiple nodes for similarity search.

        Args:
            nodes: List of GraphNodes to index

        Returns:
            Number of nodes indexed
        """
        if not nodes:
            return 0

        # Generate embeddings in batch for efficiency
        nodes_to_embed = [n for n in nodes if not n.embedding]
        if nodes_to_embed:
            embeddings = self.embedder.embed_batch(nodes_to_embed)
            for node, embedding in zip(nodes_to_embed, embeddings):
                node.embedding = embedding

        # Store all nodes
        for node in nodes:
            self._nodes[node.id] = node
            self._embeddings[node.id] = node.embedding  # type: ignore

        # Batch insert to ChromaDB
        if self._use_chromadb and self._collection:
            self._collection.upsert(
                ids=[n.id for n in nodes],
                embeddings=[n.embedding for n in nodes],  # type: ignore
                metadatas=[{
                    "name": n.name,
                    "node_type": n.node_type.value if hasattr(n.node_type, 'value') else str(n.node_type),
                    "description": n.description or "",
                } for n in nodes],
                documents=[self._node_to_document(n) for n in nodes],
            )

        return len(nodes)

    def search_text(
        self,
        query: str,
        top_k: int = 10,
        node_type: str | None = None,
        threshold: float = 0.0,
    ) -> list[SimilarityResult]:
        """
        Search for nodes similar to a text query.

        Args:
            query: Text query
            top_k: Number of results to return
            node_type: Filter by node type (optional)
            threshold: Minimum similarity threshold

        Returns:
            List of SimilarityResult ordered by similarity
        """
        # Generate query embedding
        query_embedding = self.embedder.embed_text(query)

        return self._search_by_embedding(
            query_embedding,
            top_k=top_k,
            node_type=node_type,
            threshold=threshold,
        )

    def search_similar(
        self,
        node: GraphNode,
        top_k: int = 10,
        node_type: str | None = None,
        threshold: float = 0.0,
        exclude_self: bool = True,
    ) -> list[SimilarityResult]:
        """
        Search for nodes similar to a given node.

        Args:
            node: Reference node
            top_k: Number of results to return
            node_type: Filter by node type (optional)
            threshold: Minimum similarity threshold
            exclude_self: Whether to exclude the query node from results

        Returns:
            List of SimilarityResult ordered by similarity
        """
        # Get or generate embedding
        if node.embedding:
            embedding = node.embedding
        else:
            embedding = self.embedder.embed_node(node)

        results = self._search_by_embedding(
            embedding,
            top_k=top_k + (1 if exclude_self else 0),
            node_type=node_type,
            threshold=threshold,
        )

        # Exclude self if requested
        if exclude_self:
            results = [r for r in results if r.node_id != node.id][:top_k]

        return results

    def _search_by_embedding(
        self,
        embedding: list[float],
        top_k: int = 10,
        node_type: str | None = None,
        threshold: float = 0.0,
    ) -> list[SimilarityResult]:
        """Search by embedding vector."""
        # Use ChromaDB if available
        if self._use_chromadb and self._collection:
            return self._chromadb_search(
                embedding,
                top_k=top_k,
                node_type=node_type,
                threshold=threshold,
            )

        # Fall back to in-memory search
        return self._memory_search(
            embedding,
            top_k=top_k,
            node_type=node_type,
            threshold=threshold,
        )

    def _memory_search(
        self,
        embedding: list[float],
        top_k: int,
        node_type: str | None,
        threshold: float,
    ) -> list[SimilarityResult]:
        """In-memory similarity search."""
        results = []

        for node_id, node_embedding in self._embeddings.items():
            node = self._nodes.get(node_id)
            if not node:
                continue

            # Filter by type
            if node_type:
                current_type = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)
                if current_type != node_type:
                    continue

            # Calculate similarity
            similarity = self.embedder.compute_similarity(embedding, node_embedding)

            if similarity >= threshold:
                results.append(SimilarityResult(
                    node_id=node_id,
                    node_name=node.name,
                    node_type=node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type),
                    score=similarity,
                    metadata={
                        "description": node.description,
                        "tags": node.tags,
                    },
                ))

        # Sort by score descending
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]

    def _chromadb_search(
        self,
        embedding: list[float],
        top_k: int,
        node_type: str | None,
        threshold: float,
    ) -> list[SimilarityResult]:
        """ChromaDB-based similarity search."""
        if not self._collection:
            return []

        # Build where clause for filtering
        where = None
        if node_type:
            where = {"node_type": node_type}

        # Query ChromaDB
        results = self._collection.query(
            query_embeddings=[embedding],
            n_results=top_k,
            where=where,
            include=["metadatas", "distances"],
        )

        # Convert to SimilarityResult
        similarity_results = []

        if results["ids"] and results["ids"][0]:
            ids = results["ids"][0]
            distances = results["distances"][0] if results["distances"] else []
            metadatas = results["metadatas"][0] if results["metadatas"] else []

            for i, node_id in enumerate(ids):
                # ChromaDB returns distances, convert to similarity
                distance = distances[i] if i < len(distances) else 0
                similarity = 1 - distance  # Cosine distance to similarity

                if similarity >= threshold:
                    metadata = metadatas[i] if i < len(metadatas) else {}
                    similarity_results.append(SimilarityResult(
                        node_id=node_id,
                        node_name=metadata.get("name", node_id),
                        node_type=metadata.get("node_type", "unknown"),
                        score=similarity,
                        metadata=metadata,
                    ))

        return similarity_results

    def find_duplicates(
        self,
        threshold: float = 0.9,
        node_type: str | None = None,
    ) -> list[tuple[str, str, float]]:
        """
        Find potential duplicate nodes based on similarity.

        Args:
            threshold: Minimum similarity to consider as duplicate
            node_type: Filter by node type (optional)

        Returns:
            List of (node_id_1, node_id_2, similarity) tuples
        """
        duplicates = []
        processed = set()

        node_ids = list(self._embeddings.keys())

        for i, node_id_1 in enumerate(node_ids):
            node_1 = self._nodes.get(node_id_1)
            if not node_1:
                continue

            # Filter by type
            if node_type:
                type_1 = node_1.node_type.value if hasattr(node_1.node_type, 'value') else str(node_1.node_type)
                if type_1 != node_type:
                    continue

            embedding_1 = self._embeddings[node_id_1]

            for node_id_2 in node_ids[i + 1:]:
                if (node_id_1, node_id_2) in processed:
                    continue

                node_2 = self._nodes.get(node_id_2)
                if not node_2:
                    continue

                # Filter by type
                if node_type:
                    type_2 = node_2.node_type.value if hasattr(node_2.node_type, 'value') else str(node_2.node_type)
                    if type_2 != node_type:
                        continue

                embedding_2 = self._embeddings[node_id_2]
                similarity = self.embedder.compute_similarity(embedding_1, embedding_2)

                if similarity >= threshold:
                    duplicates.append((node_id_1, node_id_2, similarity))
                    processed.add((node_id_1, node_id_2))

        # Sort by similarity descending
        duplicates.sort(key=lambda x: x[2], reverse=True)
        return duplicates

    def find_clusters(
        self,
        n_clusters: int | None = None,
        min_cluster_size: int = 2,
    ) -> list[list[str]]:
        """
        Cluster nodes by embedding similarity.

        Args:
            n_clusters: Number of clusters (auto-detected if None)
            min_cluster_size: Minimum nodes per cluster

        Returns:
            List of clusters (each cluster is a list of node IDs)
        """
        if len(self._embeddings) < min_cluster_size:
            return []

        # Convert to numpy array
        node_ids = list(self._embeddings.keys())
        embeddings = np.array([self._embeddings[nid] for nid in node_ids])

        try:
            from sklearn.cluster import KMeans, DBSCAN

            if n_clusters:
                # Use K-means if cluster count specified
                n_clusters = min(n_clusters, len(node_ids))
                kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                labels = kmeans.fit_predict(embeddings)
            else:
                # Use DBSCAN for automatic cluster detection
                dbscan = DBSCAN(eps=0.5, min_samples=min_cluster_size, metric='cosine')
                labels = dbscan.fit_predict(embeddings)

        except ImportError:
            # Fall back to simple similarity-based clustering
            return self._simple_clustering(node_ids, min_cluster_size)

        # Group by cluster label
        clusters: dict[int, list[str]] = {}
        for node_id, label in zip(node_ids, labels):
            if label == -1:  # DBSCAN noise
                continue
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(node_id)

        # Filter by minimum size and sort by size
        result = [c for c in clusters.values() if len(c) >= min_cluster_size]
        result.sort(key=len, reverse=True)
        return result

    def _simple_clustering(
        self,
        node_ids: list[str],
        min_cluster_size: int,
    ) -> list[list[str]]:
        """Simple similarity-based clustering without sklearn."""
        clusters = []
        assigned = set()

        for node_id in node_ids:
            if node_id in assigned:
                continue

            cluster = [node_id]
            assigned.add(node_id)
            embedding = self._embeddings[node_id]

            # Find similar nodes
            for other_id in node_ids:
                if other_id in assigned:
                    continue

                other_embedding = self._embeddings[other_id]
                similarity = self.embedder.compute_similarity(embedding, other_embedding)

                if similarity >= 0.7:  # Threshold for clustering
                    cluster.append(other_id)
                    assigned.add(other_id)

            if len(cluster) >= min_cluster_size:
                clusters.append(cluster)

        clusters.sort(key=len, reverse=True)
        return clusters

    def get_node(self, node_id: str) -> GraphNode | None:
        """
        Get a node by ID.

        Args:
            node_id: Node ID

        Returns:
            GraphNode or None if not found
        """
        return self._nodes.get(node_id)

    def get_all_nodes(self) -> list[GraphNode]:
        """
        Get all indexed nodes.

        Returns:
            List of all GraphNodes
        """
        return list(self._nodes.values())

    def remove_node(self, node_id: str) -> bool:
        """
        Remove a node from the index.

        Args:
            node_id: Node ID to remove

        Returns:
            True if removed, False if not found
        """
        if node_id not in self._nodes:
            return False

        del self._nodes[node_id]
        del self._embeddings[node_id]

        if self._use_chromadb and self._collection:
            self._collection.delete(ids=[node_id])

        return True

    def clear(self) -> None:
        """Clear all indexed nodes."""
        self._nodes.clear()
        self._embeddings.clear()

        if self._use_chromadb and self._collection:
            # Delete and recreate collection
            if self._chroma_client:
                self._chroma_client.delete_collection(self._collection_name)
                self._collection = self._chroma_client.create_collection(
                    name=self._collection_name,
                    metadata={"hnsw:space": "cosine"},
                )

    def get_stats(self) -> dict[str, Any]:
        """
        Get statistics about the index.

        Returns:
            Dictionary with index statistics
        """
        type_counts: dict[str, int] = {}
        for node in self._nodes.values():
            node_type = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)
            type_counts[node_type] = type_counts.get(node_type, 0) + 1

        return {
            "total_nodes": len(self._nodes),
            "using_chromadb": self._use_chromadb,
            "embedding_dim": self.embedder.embedding_dim,
            "node_types": type_counts,
        }

    def _node_to_document(self, node: GraphNode) -> str:
        """Convert a node to a searchable document string."""
        parts = [node.name]

        if node.description:
            parts.append(node.description)

        if node.tags:
            parts.extend(node.tags)

        return " ".join(parts)

    def persist(self) -> None:
        """Persist ChromaDB data to disk if configured."""
        if self._use_chromadb and self._chroma_client and self._persist_directory:
            self._chroma_client.persist()
