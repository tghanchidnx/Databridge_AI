"""
Vector Store implementation using ChromaDB for DataBridge AI Librarian.

Provides persistent vector storage for hierarchies, mappings, and concepts.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from enum import Enum
import hashlib
import json

try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False


class CollectionType(str, Enum):
    """Types of vector collections."""
    HIERARCHIES = "hierarchies"
    MAPPINGS = "mappings"
    FORMULAS = "formulas"
    INDUSTRY_PATTERNS = "industry_patterns"
    WHITEPAPER_CONCEPTS = "whitepaper_concepts"
    CLIENT_KNOWLEDGE = "client_knowledge"


@dataclass
class Document:
    """Represents a document to be stored in the vector store."""

    id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "metadata": self.metadata,
        }


@dataclass
class SearchResult:
    """Result from a similarity search."""

    id: str
    content: str
    score: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "score": round(self.score, 4),
            "metadata": self.metadata,
        }


@dataclass
class VectorStoreResult:
    """Result from vector store operations."""

    success: bool
    message: str = ""
    data: Any = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "errors": self.errors,
        }


class VectorStore:
    """
    ChromaDB-based vector store for DataBridge AI.

    Provides:
    - Persistent local vector storage
    - Collection management
    - Document upsert with embeddings
    - Similarity search with filters
    - Metadata-based filtering
    """

    # Default collection prefix
    COLLECTION_PREFIX = "databridge_librarian_"

    def __init__(
        self,
        persist_directory: Optional[Union[str, Path]] = None,
        embedding_function: Optional[Any] = None,
    ):
        """
        Initialize the vector store.

        Args:
            persist_directory: Directory for persistent storage.
            embedding_function: ChromaDB embedding function to use.
        """
        if not CHROMADB_AVAILABLE:
            raise ImportError(
                "chromadb is required for VectorStore. "
                "Install with: pip install chromadb"
            )

        self.persist_directory = Path(persist_directory) if persist_directory else Path("data/vectors")
        self.persist_directory.mkdir(parents=True, exist_ok=True)

        # Initialize ChromaDB client with persistent storage
        self._client = chromadb.PersistentClient(
            path=str(self.persist_directory),
            settings=ChromaSettings(
                anonymized_telemetry=False,
                allow_reset=True,
            ),
        )

        self._embedding_function = embedding_function
        self._collections: Dict[str, Any] = {}

    def _get_collection_name(self, collection_type: Union[CollectionType, str]) -> str:
        """Get the full collection name with prefix."""
        if isinstance(collection_type, CollectionType):
            return f"{self.COLLECTION_PREFIX}{collection_type.value}"
        return f"{self.COLLECTION_PREFIX}{collection_type}"

    def _generate_id(self, content: str, metadata: Optional[Dict] = None) -> str:
        """Generate a deterministic ID for a document."""
        hash_input = content
        if metadata:
            hash_input += json.dumps(metadata, sort_keys=True)
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    def create_collection(
        self,
        collection_type: Union[CollectionType, str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> VectorStoreResult:
        """
        Create a new collection.

        Args:
            collection_type: Type of collection to create.
            metadata: Optional collection metadata.

        Returns:
            VectorStoreResult with operation status.
        """
        try:
            name = self._get_collection_name(collection_type)

            collection = self._client.get_or_create_collection(
                name=name,
                metadata=metadata or {},
                embedding_function=self._embedding_function,
            )

            self._collections[name] = collection

            return VectorStoreResult(
                success=True,
                message=f"Collection '{name}' created/loaded successfully",
                data={"name": name, "count": collection.count()},
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to create collection: {str(e)}",
                errors=[str(e)],
            )

    def get_collection(
        self,
        collection_type: Union[CollectionType, str],
    ) -> Optional[Any]:
        """
        Get a collection by type.

        Args:
            collection_type: Type of collection to get.

        Returns:
            ChromaDB collection or None.
        """
        name = self._get_collection_name(collection_type)

        if name not in self._collections:
            try:
                self._collections[name] = self._client.get_collection(
                    name=name,
                    embedding_function=self._embedding_function,
                )
            except Exception:
                return None

        return self._collections.get(name)

    def list_collections(self) -> VectorStoreResult:
        """
        List all collections.

        Returns:
            VectorStoreResult with list of collection names.
        """
        try:
            collections = self._client.list_collections()
            collection_info = [
                {
                    "name": c.name,
                    "count": c.count(),
                    "metadata": c.metadata,
                }
                for c in collections
            ]

            return VectorStoreResult(
                success=True,
                message=f"Found {len(collections)} collections",
                data=collection_info,
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to list collections: {str(e)}",
                errors=[str(e)],
            )

    def delete_collection(
        self,
        collection_type: Union[CollectionType, str],
    ) -> VectorStoreResult:
        """
        Delete a collection.

        Args:
            collection_type: Type of collection to delete.

        Returns:
            VectorStoreResult with operation status.
        """
        try:
            name = self._get_collection_name(collection_type)
            self._client.delete_collection(name=name)

            if name in self._collections:
                del self._collections[name]

            return VectorStoreResult(
                success=True,
                message=f"Collection '{name}' deleted successfully",
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to delete collection: {str(e)}",
                errors=[str(e)],
            )

    def upsert(
        self,
        collection_type: Union[CollectionType, str],
        documents: List[Document],
        batch_size: int = 100,
    ) -> VectorStoreResult:
        """
        Upsert documents into a collection.

        Args:
            collection_type: Collection to upsert into.
            documents: List of documents to upsert.
            batch_size: Batch size for upsert operations.

        Returns:
            VectorStoreResult with operation status.
        """
        try:
            collection = self.get_collection(collection_type)
            if collection is None:
                # Create collection if it doesn't exist
                result = self.create_collection(collection_type)
                if not result.success:
                    return result
                collection = self.get_collection(collection_type)

            # Process in batches
            total_upserted = 0
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]

                ids = []
                contents = []
                metadatas = []
                embeddings = []

                for doc in batch:
                    doc_id = doc.id or self._generate_id(doc.content, doc.metadata)
                    ids.append(doc_id)
                    contents.append(doc.content)
                    metadatas.append(doc.metadata or {})

                    if doc.embedding:
                        embeddings.append(doc.embedding)

                # Upsert with or without pre-computed embeddings
                if embeddings and len(embeddings) == len(ids):
                    collection.upsert(
                        ids=ids,
                        documents=contents,
                        metadatas=metadatas,
                        embeddings=embeddings,
                    )
                else:
                    collection.upsert(
                        ids=ids,
                        documents=contents,
                        metadatas=metadatas,
                    )

                total_upserted += len(batch)

            return VectorStoreResult(
                success=True,
                message=f"Upserted {total_upserted} documents",
                data={"count": total_upserted},
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to upsert documents: {str(e)}",
                errors=[str(e)],
            )

    def search(
        self,
        collection_type: Union[CollectionType, str],
        query: str,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, Any]] = None,
        include: Optional[List[str]] = None,
    ) -> VectorStoreResult:
        """
        Search for similar documents.

        Args:
            collection_type: Collection to search in.
            query: Query text for similarity search.
            n_results: Number of results to return.
            where: Metadata filter conditions.
            where_document: Document content filter conditions.
            include: What to include in results (documents, metadatas, distances).

        Returns:
            VectorStoreResult with search results.
        """
        try:
            collection = self.get_collection(collection_type)
            if collection is None:
                return VectorStoreResult(
                    success=False,
                    message=f"Collection '{collection_type}' not found",
                    errors=["Collection does not exist"],
                )

            include = include or ["documents", "metadatas", "distances"]

            results = collection.query(
                query_texts=[query],
                n_results=n_results,
                where=where,
                where_document=where_document,
                include=include,
            )

            # Convert to SearchResult objects
            search_results = []
            if results["ids"] and results["ids"][0]:
                for i, doc_id in enumerate(results["ids"][0]):
                    content = results["documents"][0][i] if "documents" in include else ""
                    metadata = results["metadatas"][0][i] if "metadatas" in include else {}
                    distance = results["distances"][0][i] if "distances" in include else 0

                    # Convert distance to similarity score (1 - normalized_distance)
                    score = max(0, 1 - distance) if distance else 1.0

                    search_results.append(SearchResult(
                        id=doc_id,
                        content=content,
                        score=score,
                        metadata=metadata,
                    ))

            return VectorStoreResult(
                success=True,
                message=f"Found {len(search_results)} results",
                data=[r.to_dict() for r in search_results],
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Search failed: {str(e)}",
                errors=[str(e)],
            )

    def get_by_id(
        self,
        collection_type: Union[CollectionType, str],
        ids: List[str],
    ) -> VectorStoreResult:
        """
        Get documents by their IDs.

        Args:
            collection_type: Collection to get from.
            ids: List of document IDs.

        Returns:
            VectorStoreResult with documents.
        """
        try:
            collection = self.get_collection(collection_type)
            if collection is None:
                return VectorStoreResult(
                    success=False,
                    message=f"Collection '{collection_type}' not found",
                    errors=["Collection does not exist"],
                )

            results = collection.get(
                ids=ids,
                include=["documents", "metadatas"],
            )

            documents = []
            if results["ids"]:
                for i, doc_id in enumerate(results["ids"]):
                    documents.append({
                        "id": doc_id,
                        "content": results["documents"][i] if results["documents"] else "",
                        "metadata": results["metadatas"][i] if results["metadatas"] else {},
                    })

            return VectorStoreResult(
                success=True,
                message=f"Retrieved {len(documents)} documents",
                data=documents,
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to get documents: {str(e)}",
                errors=[str(e)],
            )

    def delete_documents(
        self,
        collection_type: Union[CollectionType, str],
        ids: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
    ) -> VectorStoreResult:
        """
        Delete documents from a collection.

        Args:
            collection_type: Collection to delete from.
            ids: Document IDs to delete.
            where: Metadata filter for deletion.

        Returns:
            VectorStoreResult with operation status.
        """
        try:
            collection = self.get_collection(collection_type)
            if collection is None:
                return VectorStoreResult(
                    success=False,
                    message=f"Collection '{collection_type}' not found",
                    errors=["Collection does not exist"],
                )

            if ids:
                collection.delete(ids=ids)
            elif where:
                collection.delete(where=where)
            else:
                return VectorStoreResult(
                    success=False,
                    message="Either 'ids' or 'where' must be provided",
                    errors=["Missing deletion criteria"],
                )

            return VectorStoreResult(
                success=True,
                message="Documents deleted successfully",
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to delete documents: {str(e)}",
                errors=[str(e)],
            )

    def get_collection_stats(
        self,
        collection_type: Union[CollectionType, str],
    ) -> VectorStoreResult:
        """
        Get statistics for a collection.

        Args:
            collection_type: Collection to get stats for.

        Returns:
            VectorStoreResult with collection statistics.
        """
        try:
            collection = self.get_collection(collection_type)
            if collection is None:
                return VectorStoreResult(
                    success=False,
                    message=f"Collection '{collection_type}' not found",
                    errors=["Collection does not exist"],
                )

            stats = {
                "name": collection.name,
                "count": collection.count(),
                "metadata": collection.metadata,
            }

            return VectorStoreResult(
                success=True,
                message="Collection stats retrieved",
                data=stats,
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to get collection stats: {str(e)}",
                errors=[str(e)],
            )

    def reset(self) -> VectorStoreResult:
        """
        Reset the entire vector store (delete all collections).

        Returns:
            VectorStoreResult with operation status.
        """
        try:
            self._client.reset()
            self._collections.clear()

            return VectorStoreResult(
                success=True,
                message="Vector store reset successfully",
            )

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to reset vector store: {str(e)}",
                errors=[str(e)],
            )
