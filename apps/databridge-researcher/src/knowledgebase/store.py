"""
Knowledge Base Store for DataBridge Analytics Researcher.

ChromaDB-based storage for business glossary, metrics, and query patterns.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from enum import Enum
import json
import hashlib

try:
    import chromadb
    from chromadb.config import Settings as ChromaSettings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False


class KBCollectionType(str, Enum):
    """Knowledge base collection types."""
    BUSINESS_GLOSSARY = "business_glossary"
    METRIC_DEFINITIONS = "metric_definitions"
    QUERY_PATTERNS = "query_patterns"
    INDUSTRY_TERMINOLOGY = "industry_terminology"
    FPA_CONCEPTS = "fpa_concepts"


@dataclass
class KBDocument:
    """A knowledge base document."""

    id: str
    content: str
    category: str = ""
    title: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "category": self.category,
            "title": self.title,
            "metadata": self.metadata,
        }


@dataclass
class KBSearchResult:
    """Knowledge base search result."""

    id: str
    content: str
    score: float
    category: str = ""
    title: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "score": round(self.score, 4),
            "category": self.category,
            "title": self.title,
            "metadata": self.metadata,
        }


@dataclass
class KBResult:
    """Result from knowledge base operations."""

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


class KnowledgeBaseStore:
    """
    ChromaDB-based knowledge base for DataBridge Analytics Researcher.

    Manages:
    - Business glossary terms
    - Metric definitions
    - Query patterns
    - Industry terminology
    - FP&A concepts
    """

    COLLECTION_PREFIX = "databridge_researcher_kb_"

    def __init__(
        self,
        persist_directory: Optional[Union[str, Path]] = None,
        embedding_model: str = "all-MiniLM-L6-v2",
    ):
        """
        Initialize the knowledge base store.

        Args:
            persist_directory: Directory for persistent storage.
            embedding_model: Sentence-transformers model name.
        """
        if not CHROMADB_AVAILABLE:
            raise ImportError("chromadb is required. Install with: pip install chromadb")

        self.persist_directory = Path(persist_directory) if persist_directory else Path("data/kb")
        self.persist_directory.mkdir(parents=True, exist_ok=True)

        self._client = chromadb.PersistentClient(
            path=str(self.persist_directory),
            settings=ChromaSettings(
                anonymized_telemetry=False,
                allow_reset=True,
            ),
        )

        self._embedder: Optional[SentenceTransformer] = None
        self._embedding_model = embedding_model
        self._collections: Dict[str, Any] = {}

    def _get_embedder(self) -> SentenceTransformer:
        """Get or create the embedder."""
        if self._embedder is None:
            if not SENTENCE_TRANSFORMERS_AVAILABLE:
                raise ImportError("sentence-transformers is required")
            self._embedder = SentenceTransformer(self._embedding_model)
        return self._embedder

    def _get_collection_name(self, collection_type: KBCollectionType) -> str:
        """Get full collection name."""
        return f"{self.COLLECTION_PREFIX}{collection_type.value}"

    def _generate_id(self, content: str, category: str = "") -> str:
        """Generate deterministic document ID."""
        hash_input = f"{category}:{content}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    def _get_embedding(self, text: str) -> List[float]:
        """Generate embedding for text."""
        embedder = self._get_embedder()
        embedding = embedder.encode(text, normalize_embeddings=True)
        return embedding.tolist()

    def get_collection(self, collection_type: KBCollectionType):
        """Get or create a collection."""
        name = self._get_collection_name(collection_type)

        if name not in self._collections:
            self._collections[name] = self._client.get_or_create_collection(
                name=name,
                metadata={"type": collection_type.value},
            )

        return self._collections[name]

    def add_document(
        self,
        collection_type: KBCollectionType,
        content: str,
        category: str = "",
        title: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        document_id: Optional[str] = None,
    ) -> KBResult:
        """
        Add a document to the knowledge base.

        Args:
            collection_type: Type of collection.
            content: Document content.
            category: Document category.
            title: Document title.
            metadata: Additional metadata.
            document_id: Optional custom ID.

        Returns:
            KBResult with operation status.
        """
        try:
            collection = self.get_collection(collection_type)

            doc_id = document_id or self._generate_id(content, category)
            embedding = self._get_embedding(content)

            doc_metadata = {
                "category": category,
                "title": title,
                **(metadata or {}),
            }

            collection.upsert(
                ids=[doc_id],
                documents=[content],
                embeddings=[embedding],
                metadatas=[doc_metadata],
            )

            return KBResult(
                success=True,
                message=f"Document added with ID: {doc_id}",
                data={"id": doc_id},
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Failed to add document: {str(e)}",
                errors=[str(e)],
            )

    def add_documents(
        self,
        collection_type: KBCollectionType,
        documents: List[KBDocument],
        batch_size: int = 100,
    ) -> KBResult:
        """
        Add multiple documents to the knowledge base.

        Args:
            collection_type: Type of collection.
            documents: List of documents.
            batch_size: Batch size for processing.

        Returns:
            KBResult with operation status.
        """
        try:
            collection = self.get_collection(collection_type)
            total_added = 0

            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]

                ids = []
                contents = []
                embeddings = []
                metadatas = []

                for doc in batch:
                    doc_id = doc.id or self._generate_id(doc.content, doc.category)
                    ids.append(doc_id)
                    contents.append(doc.content)
                    embeddings.append(self._get_embedding(doc.content))
                    metadatas.append({
                        "category": doc.category,
                        "title": doc.title,
                        **doc.metadata,
                    })

                collection.upsert(
                    ids=ids,
                    documents=contents,
                    embeddings=embeddings,
                    metadatas=metadatas,
                )
                total_added += len(batch)

            return KBResult(
                success=True,
                message=f"Added {total_added} documents",
                data={"count": total_added},
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Failed to add documents: {str(e)}",
                errors=[str(e)],
            )

    def search(
        self,
        collection_type: KBCollectionType,
        query: str,
        n_results: int = 5,
        category: Optional[str] = None,
    ) -> KBResult:
        """
        Search the knowledge base.

        Args:
            collection_type: Type of collection.
            query: Search query.
            n_results: Number of results.
            category: Optional category filter.

        Returns:
            KBResult with search results.
        """
        try:
            collection = self.get_collection(collection_type)

            where = {"category": category} if category else None

            results = collection.query(
                query_embeddings=[self._get_embedding(query)],
                n_results=n_results,
                where=where,
                include=["documents", "metadatas", "distances"],
            )

            search_results = []
            if results["ids"] and results["ids"][0]:
                for i, doc_id in enumerate(results["ids"][0]):
                    distance = results["distances"][0][i] if results["distances"] else 0
                    score = max(0, 1 - distance)

                    metadata = results["metadatas"][0][i] if results["metadatas"] else {}

                    search_results.append(KBSearchResult(
                        id=doc_id,
                        content=results["documents"][0][i] if results["documents"] else "",
                        score=score,
                        category=metadata.get("category", ""),
                        title=metadata.get("title", ""),
                        metadata=metadata,
                    ))

            return KBResult(
                success=True,
                message=f"Found {len(search_results)} results",
                data=[r.to_dict() for r in search_results],
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Search failed: {str(e)}",
                errors=[str(e)],
            )

    def get_by_id(
        self,
        collection_type: KBCollectionType,
        document_id: str,
    ) -> KBResult:
        """
        Get a document by ID.

        Args:
            collection_type: Type of collection.
            document_id: Document ID.

        Returns:
            KBResult with document.
        """
        try:
            collection = self.get_collection(collection_type)

            results = collection.get(
                ids=[document_id],
                include=["documents", "metadatas"],
            )

            if results["ids"]:
                metadata = results["metadatas"][0] if results["metadatas"] else {}
                return KBResult(
                    success=True,
                    message="Document found",
                    data={
                        "id": results["ids"][0],
                        "content": results["documents"][0] if results["documents"] else "",
                        "category": metadata.get("category", ""),
                        "title": metadata.get("title", ""),
                        "metadata": metadata,
                    },
                )

            return KBResult(
                success=False,
                message="Document not found",
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Failed to get document: {str(e)}",
                errors=[str(e)],
            )

    def delete_document(
        self,
        collection_type: KBCollectionType,
        document_id: str,
    ) -> KBResult:
        """
        Delete a document.

        Args:
            collection_type: Type of collection.
            document_id: Document ID.

        Returns:
            KBResult with operation status.
        """
        try:
            collection = self.get_collection(collection_type)
            collection.delete(ids=[document_id])

            return KBResult(
                success=True,
                message="Document deleted",
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Failed to delete document: {str(e)}",
                errors=[str(e)],
            )

    def get_collection_stats(
        self,
        collection_type: KBCollectionType,
    ) -> KBResult:
        """
        Get collection statistics.

        Args:
            collection_type: Type of collection.

        Returns:
            KBResult with statistics.
        """
        try:
            collection = self.get_collection(collection_type)

            return KBResult(
                success=True,
                message="Stats retrieved",
                data={
                    "collection": collection_type.value,
                    "count": collection.count(),
                },
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Failed to get stats: {str(e)}",
                errors=[str(e)],
            )

    def list_collections(self) -> KBResult:
        """
        List all knowledge base collections.

        Returns:
            KBResult with collection list.
        """
        try:
            collections = self._client.list_collections()

            kb_collections = []
            for c in collections:
                if c.name.startswith(self.COLLECTION_PREFIX):
                    kb_collections.append({
                        "name": c.name.replace(self.COLLECTION_PREFIX, ""),
                        "full_name": c.name,
                        "count": c.count(),
                    })

            return KBResult(
                success=True,
                message=f"Found {len(kb_collections)} collections",
                data=kb_collections,
            )

        except Exception as e:
            return KBResult(
                success=False,
                message=f"Failed to list collections: {str(e)}",
                errors=[str(e)],
            )
