"""
Vector Store - Abstraction layer for vector databases.

Supports:
- SQLite (lightweight, no external dependencies)
- ChromaDB (local, feature-rich)
- Pinecone (cloud, production-ready)

The SQLite implementation uses numpy for similarity calculations,
making it suitable for development and small-scale deployments.
"""
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from .types import VectorStoreType, IndexStats, IndexedDocument

logger = logging.getLogger(__name__)


class BaseVectorStore(ABC):
    """Abstract base class for vector stores."""

    @abstractmethod
    def upsert(
        self,
        id: str,
        embedding: List[float],
        content: str,
        metadata: Dict[str, Any],
    ) -> bool:
        """Insert or update a vector with content and metadata."""
        pass

    @abstractmethod
    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        threshold: float = 0.0,
    ) -> List[Tuple[str, float, str, Dict[str, Any]]]:
        """
        Search for similar vectors.

        Returns: [(id, score, content, metadata), ...]
        """
        pass

    @abstractmethod
    def get(self, id: str) -> Optional[Tuple[List[float], str, Dict[str, Any]]]:
        """Get a document by ID. Returns (embedding, content, metadata) or None."""
        pass

    @abstractmethod
    def delete(self, id: str) -> bool:
        """Delete a vector by ID."""
        pass

    @abstractmethod
    def delete_by_filter(self, filter: Dict[str, Any]) -> int:
        """Delete vectors matching filter. Returns count deleted."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Return total number of vectors."""
        pass

    @abstractmethod
    def stats(self) -> IndexStats:
        """Return index statistics."""
        pass

    @abstractmethod
    def clear(self) -> int:
        """Clear all vectors. Returns count deleted."""
        pass


class SQLiteVectorStore(BaseVectorStore):
    """
    Lightweight vector store using SQLite + numpy.

    No external dependencies beyond numpy. Suitable for development
    and deployments with < 100k vectors.
    """

    def __init__(
        self,
        db_path: str = "data/graphrag/vectors.db",
        dimension: int = 1536,
    ):
        import sqlite3

        self.db_path = db_path
        self.dimension = dimension

        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Initialize database
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS vectors (
                id TEXT PRIMARY KEY,
                embedding BLOB NOT NULL,
                content TEXT,
                metadata TEXT,
                source_type TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_source_type ON vectors(source_type)
        """)
        self.conn.commit()

    def _serialize_embedding(self, embedding: List[float]) -> bytes:
        """Serialize embedding to bytes."""
        import numpy as np
        return np.array(embedding, dtype=np.float32).tobytes()

    def _deserialize_embedding(self, data: bytes) -> List[float]:
        """Deserialize embedding from bytes."""
        import numpy as np
        return np.frombuffer(data, dtype=np.float32).tolist()

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        import numpy as np
        a_np = np.array(a, dtype=np.float32)
        b_np = np.array(b, dtype=np.float32)

        dot = np.dot(a_np, b_np)
        norm_a = np.linalg.norm(a_np)
        norm_b = np.linalg.norm(b_np)

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return float(dot / (norm_a * norm_b))

    def upsert(
        self,
        id: str,
        embedding: List[float],
        content: str,
        metadata: Dict[str, Any],
    ) -> bool:
        """Insert or update a vector."""
        try:
            emb_bytes = self._serialize_embedding(embedding)
            meta_str = json.dumps(metadata)
            source_type = metadata.get("source_type", "unknown")

            self.conn.execute("""
                INSERT OR REPLACE INTO vectors
                (id, embedding, content, metadata, source_type, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (id, emb_bytes, content, meta_str, source_type, datetime.utcnow().isoformat()))

            self.conn.commit()
            return True

        except Exception as e:
            logger.error(f"SQLite upsert failed: {e}")
            return False

    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        threshold: float = 0.0,
    ) -> List[Tuple[str, float, str, Dict[str, Any]]]:
        """Search for similar vectors using cosine similarity."""
        try:
            # Build query
            sql = "SELECT id, embedding, content, metadata FROM vectors"
            params = []

            if filter:
                conditions = []
                for key, value in filter.items():
                    if key == "source_type":
                        conditions.append("source_type = ?")
                        params.append(value)
                    else:
                        # JSON filter
                        conditions.append(f"json_extract(metadata, '$.{key}') = ?")
                        params.append(json.dumps(value) if isinstance(value, (dict, list)) else str(value))

                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)

            cursor = self.conn.execute(sql, params)
            results = []

            for row in cursor:
                id, emb_bytes, content, meta_str = row
                embedding = self._deserialize_embedding(emb_bytes)
                score = self._cosine_similarity(query_embedding, embedding)

                if score >= threshold:
                    metadata = json.loads(meta_str) if meta_str else {}
                    results.append((id, score, content or "", metadata))

            # Sort by score descending and limit
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]

        except Exception as e:
            logger.error(f"SQLite search failed: {e}")
            return []

    def get(self, id: str) -> Optional[Tuple[List[float], str, Dict[str, Any]]]:
        """Get a document by ID."""
        try:
            cursor = self.conn.execute(
                "SELECT embedding, content, metadata FROM vectors WHERE id = ?",
                (id,)
            )
            row = cursor.fetchone()

            if row:
                emb_bytes, content, meta_str = row
                embedding = self._deserialize_embedding(emb_bytes)
                metadata = json.loads(meta_str) if meta_str else {}
                return (embedding, content or "", metadata)

            return None

        except Exception as e:
            logger.error(f"SQLite get failed: {e}")
            return None

    def delete(self, id: str) -> bool:
        """Delete a vector by ID."""
        try:
            cursor = self.conn.execute("DELETE FROM vectors WHERE id = ?", (id,))
            self.conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"SQLite delete failed: {e}")
            return False

    def delete_by_filter(self, filter: Dict[str, Any]) -> int:
        """Delete vectors matching filter."""
        try:
            conditions = []
            params = []

            for key, value in filter.items():
                if key == "source_type":
                    conditions.append("source_type = ?")
                    params.append(value)
                else:
                    conditions.append(f"json_extract(metadata, '$.{key}') = ?")
                    params.append(str(value))

            if not conditions:
                return 0

            sql = "DELETE FROM vectors WHERE " + " AND ".join(conditions)
            cursor = self.conn.execute(sql, params)
            self.conn.commit()
            return cursor.rowcount

        except Exception as e:
            logger.error(f"SQLite delete_by_filter failed: {e}")
            return 0

    def count(self) -> int:
        """Return total count."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM vectors")
        return cursor.fetchone()[0]

    def stats(self) -> IndexStats:
        """Return index statistics."""
        try:
            # Total count
            total = self.count()

            # Count by source type
            cursor = self.conn.execute(
                "SELECT source_type, COUNT(*) FROM vectors GROUP BY source_type"
            )
            by_source = {row[0] or "unknown": row[1] for row in cursor}

            # Last indexed
            cursor = self.conn.execute(
                "SELECT MAX(updated_at) FROM vectors"
            )
            last = cursor.fetchone()[0]
            last_indexed = datetime.fromisoformat(last) if last else None

            return IndexStats(
                total_documents=total,
                by_source=by_source,
                last_indexed=last_indexed,
                vector_dimension=self.dimension,
            )

        except Exception as e:
            logger.error(f"SQLite stats failed: {e}")
            return IndexStats()

    def clear(self) -> int:
        """Clear all vectors."""
        try:
            cursor = self.conn.execute("DELETE FROM vectors")
            self.conn.commit()
            return cursor.rowcount

        except Exception as e:
            logger.error(f"SQLite clear failed: {e}")
            return 0

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


class ChromaVectorStore(BaseVectorStore):
    """
    ChromaDB vector store for local deployments.

    Provides better performance than SQLite for larger datasets
    with built-in HNSW indexing.
    """

    def __init__(
        self,
        collection_name: str = "databridge_graphrag",
        persist_dir: str = "data/graphrag/chroma",
        dimension: int = 1536,
    ):
        self.collection_name = collection_name
        self.persist_dir = persist_dir
        self.dimension = dimension
        self._client = None
        self._collection = None

    def _init_client(self):
        """Lazy initialize ChromaDB client."""
        if self._client is None:
            try:
                import chromadb
                from chromadb.config import Settings

                Path(self.persist_dir).mkdir(parents=True, exist_ok=True)

                self._client = chromadb.PersistentClient(
                    path=self.persist_dir,
                    settings=Settings(
                        anonymized_telemetry=False,
                        allow_reset=True,
                    ),
                )

                self._collection = self._client.get_or_create_collection(
                    name=self.collection_name,
                    metadata={"hnsw:space": "cosine"},
                )

                logger.info(f"ChromaDB initialized: {self.collection_name}")

            except ImportError:
                raise ImportError("chromadb not installed. Run: pip install chromadb")

    def upsert(
        self,
        id: str,
        embedding: List[float],
        content: str,
        metadata: Dict[str, Any],
    ) -> bool:
        """Insert or update a vector."""
        self._init_client()

        try:
            # ChromaDB requires string metadata values
            safe_metadata = {}
            for k, v in metadata.items():
                if isinstance(v, (str, int, float, bool)):
                    safe_metadata[k] = v
                elif v is None:
                    safe_metadata[k] = ""
                else:
                    safe_metadata[k] = json.dumps(v)

            self._collection.upsert(
                ids=[id],
                embeddings=[embedding],
                documents=[content],
                metadatas=[safe_metadata],
            )
            return True

        except Exception as e:
            logger.error(f"ChromaDB upsert failed: {e}")
            return False

    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
        threshold: float = 0.0,
    ) -> List[Tuple[str, float, str, Dict[str, Any]]]:
        """Search for similar vectors."""
        self._init_client()

        try:
            # Convert filter to ChromaDB where clause
            where = None
            if filter:
                where = {}
                for k, v in filter.items():
                    where[k] = {"$eq": v}

            results = self._collection.query(
                query_embeddings=[query_embedding],
                n_results=top_k,
                where=where,
            )

            output = []
            if results["ids"] and results["ids"][0]:
                for i, id in enumerate(results["ids"][0]):
                    # ChromaDB returns distance, convert to similarity
                    distance = results["distances"][0][i] if results["distances"] else 0
                    score = 1.0 - distance  # Cosine distance to similarity

                    if score >= threshold:
                        content = results["documents"][0][i] if results["documents"] else ""
                        metadata = results["metadatas"][0][i] if results["metadatas"] else {}
                        output.append((id, score, content, metadata))

            return output

        except Exception as e:
            logger.error(f"ChromaDB search failed: {e}")
            return []

    def get(self, id: str) -> Optional[Tuple[List[float], str, Dict[str, Any]]]:
        """Get a document by ID."""
        self._init_client()

        try:
            result = self._collection.get(
                ids=[id],
                include=["embeddings", "documents", "metadatas"],
            )

            if result["ids"]:
                embedding = result["embeddings"][0] if result["embeddings"] else []
                content = result["documents"][0] if result["documents"] else ""
                metadata = result["metadatas"][0] if result["metadatas"] else {}
                return (embedding, content, metadata)

            return None

        except Exception as e:
            logger.error(f"ChromaDB get failed: {e}")
            return None

    def delete(self, id: str) -> bool:
        """Delete a vector by ID."""
        self._init_client()

        try:
            self._collection.delete(ids=[id])
            return True

        except Exception as e:
            logger.error(f"ChromaDB delete failed: {e}")
            return False

    def delete_by_filter(self, filter: Dict[str, Any]) -> int:
        """Delete vectors matching filter."""
        self._init_client()

        try:
            where = {k: {"$eq": v} for k, v in filter.items()}

            # Get IDs matching filter
            results = self._collection.get(where=where)
            ids = results["ids"] if results["ids"] else []

            if ids:
                self._collection.delete(ids=ids)

            return len(ids)

        except Exception as e:
            logger.error(f"ChromaDB delete_by_filter failed: {e}")
            return 0

    def count(self) -> int:
        """Return total count."""
        self._init_client()
        return self._collection.count()

    def stats(self) -> IndexStats:
        """Return index statistics."""
        self._init_client()

        try:
            total = self._collection.count()

            # Get source type distribution
            by_source = {}
            for source_type in ["catalog", "hierarchy", "template", "skill", "knowledge"]:
                try:
                    results = self._collection.get(
                        where={"source_type": {"$eq": source_type}},
                        limit=1,
                    )
                    if results["ids"]:
                        count = len(self._collection.get(
                            where={"source_type": {"$eq": source_type}},
                        )["ids"])
                        by_source[source_type] = count
                except Exception:
                    pass

            return IndexStats(
                total_documents=total,
                by_source=by_source,
                vector_dimension=self.dimension,
            )

        except Exception as e:
            logger.error(f"ChromaDB stats failed: {e}")
            return IndexStats()

    def clear(self) -> int:
        """Clear all vectors."""
        self._init_client()

        try:
            count = self._collection.count()
            # Reset collection
            self._client.delete_collection(self.collection_name)
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
            return count

        except Exception as e:
            logger.error(f"ChromaDB clear failed: {e}")
            return 0


def get_vector_store(
    store_type: VectorStoreType,
    **kwargs,
) -> BaseVectorStore:
    """
    Factory function to get a vector store.

    Args:
        store_type: The vector store type
        **kwargs: Store-specific configuration

    Returns:
        Configured vector store instance
    """
    if store_type == VectorStoreType.SQLITE:
        return SQLiteVectorStore(**kwargs)

    elif store_type == VectorStoreType.CHROMA:
        return ChromaVectorStore(**kwargs)

    elif store_type == VectorStoreType.PINECONE:
        raise NotImplementedError("Pinecone support coming in Phase 31b")

    elif store_type == VectorStoreType.WEAVIATE:
        raise NotImplementedError("Weaviate support coming in Phase 31b")

    else:
        raise ValueError(f"Unsupported vector store type: {store_type}")
