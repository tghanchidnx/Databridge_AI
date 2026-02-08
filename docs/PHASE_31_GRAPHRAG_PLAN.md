# Phase 31: GraphRAG Engine

## Strategic Alignment

This phase implements the "anti-hallucination" layer described in the strategic documents by:
1. **Enforcing Structural Context** - Anchor AI reasoning in verified data hierarchies
2. **Replicating Consultant Memory** - Vector DB for unstructured context retrieval
3. **Proof of Graph** - Verify generated code against knowledge graph structure

---

## Architecture

```
User Query ("Generate a P&L hierarchy for Oil & Gas upstream")
                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                     GraphRAG Engine (Phase 31)                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Query Understanding Layer                       │   │
│  │  - Entity extraction ("P&L", "Oil & Gas", "upstream")        │   │
│  │  - Intent classification (CREATE_HIERARCHY, QUERY_DATA, etc) │   │
│  │  - Domain detection (oil_gas_upstream skill)                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                           ↓                                         │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Hybrid Retrieval Layer                          │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐ │   │
│  │  │ Vector Search│ │ Graph Search │ │ Lexical Search       │ │   │
│  │  │ (embeddings) │ │ (lineage)    │ │ (catalog index)      │ │   │
│  │  └──────────────┘ └──────────────┘ └──────────────────────┘ │   │
│  │        ↓                ↓                    ↓               │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │              Result Fusion & Ranking                  │   │   │
│  │  │  - Reciprocal Rank Fusion (RRF)                       │   │   │
│  │  │  - Re-ranking by relevance + recency                  │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                           ↓                                         │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Context Assembly Layer                          │   │
│  │  - Relevant templates (upstream_oil_gas_pl)                  │   │
│  │  - Skill prompts (fpa-oil-gas-analyst)                       │   │
│  │  - Client knowledge (LOE patterns, JIB rules)                │   │
│  │  - Lineage paths (what tables feed this hierarchy)           │   │
│  │  - Glossary terms (LOE = Lease Operating Expenses)           │   │
│  │  - Similar past projects (vector similarity)                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                           ↓                                         │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Proof of Graph Validator                        │   │
│  │  - Validate generated schema against lineage                 │   │
│  │  - Check hierarchy references exist in catalog               │   │
│  │  - Verify column names match source metadata                 │   │
│  │  - Flag hallucinated table/column names                      │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                           ↓
                    Claude via MCP
                           ↓
            Verified, Context-Anchored Response
```

---

## Module Structure

```
src/graphrag/
├── __init__.py
├── types.py                    # RAGQuery, RAGContext, RAGResult
├── embedding_provider.py       # Unified embedding interface
├── vector_store.py             # Vector DB abstraction (Chroma, Pinecone)
├── knowledge_graph.py          # KG construction from lineage + catalog
├── entity_extractor.py         # Extract entities from queries
├── retriever.py                # Hybrid retrieval (vector + graph + lexical)
├── context_builder.py          # Assemble context for LLM
├── proof_of_graph.py           # Validation layer
└── mcp_tools.py                # 10+ MCP tools

```

---

## Deliverables

### File 1: `src/graphrag/types.py` (~150 lines)

```python
from enum import Enum
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime


class EmbeddingProvider(str, Enum):
    """Supported embedding providers."""
    OPENAI = "openai"           # text-embedding-3-small/large
    ANTHROPIC = "anthropic"     # claude-3 embeddings (when available)
    HUGGINGFACE = "huggingface" # sentence-transformers
    LOCAL = "local"             # Local models via Ollama


class VectorStore(str, Enum):
    """Supported vector databases."""
    CHROMA = "chroma"           # Local, easy setup
    PINECONE = "pinecone"       # Cloud, production-ready
    WEAVIATE = "weaviate"       # Hybrid search native
    SQLITE = "sqlite"           # SQLite with vector extension


class RetrievalSource(str, Enum):
    """Sources for hybrid retrieval."""
    VECTOR = "vector"           # Embedding similarity
    GRAPH = "graph"             # Lineage traversal
    LEXICAL = "lexical"         # Catalog keyword search
    TEMPLATE = "template"       # Template matching
    KNOWLEDGE = "knowledge"     # Knowledge base


class EntityType(str, Enum):
    """Types of entities extracted from queries."""
    TABLE = "table"
    COLUMN = "column"
    HIERARCHY = "hierarchy"
    GLOSSARY_TERM = "term"
    SKILL = "skill"
    TEMPLATE = "template"
    DATABASE = "database"
    SCHEMA = "schema"
    DOMAIN = "domain"
    INDUSTRY = "industry"


class ExtractedEntity(BaseModel):
    """An entity extracted from a query."""
    text: str
    entity_type: EntityType
    confidence: float = 1.0
    linked_id: Optional[str] = None  # ID in catalog/hierarchy
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RAGQuery(BaseModel):
    """A query for the RAG system."""
    query: str
    entities: List[ExtractedEntity] = Field(default_factory=list)
    intent: Optional[str] = None
    domain: Optional[str] = None
    industry: Optional[str] = None
    connection_id: Optional[str] = None  # For live validation
    include_lineage: bool = True
    include_templates: bool = True
    include_knowledge: bool = True
    max_results: int = 10


class RetrievedItem(BaseModel):
    """A single item retrieved by the RAG system."""
    id: str
    source: RetrievalSource
    content: str
    score: float
    metadata: Dict[str, Any] = Field(default_factory=dict)
    embedding: Optional[List[float]] = None


class RAGContext(BaseModel):
    """Assembled context for LLM generation."""
    query: RAGQuery
    retrieved_items: List[RetrievedItem] = Field(default_factory=list)

    # Structured context
    templates: List[Dict[str, Any]] = Field(default_factory=list)
    skills: List[Dict[str, Any]] = Field(default_factory=list)
    knowledge: List[Dict[str, Any]] = Field(default_factory=list)
    lineage_paths: List[Dict[str, Any]] = Field(default_factory=list)
    glossary_terms: List[Dict[str, Any]] = Field(default_factory=list)
    catalog_assets: List[Dict[str, Any]] = Field(default_factory=list)

    # For validation
    available_tables: List[str] = Field(default_factory=list)
    available_columns: Dict[str, List[str]] = Field(default_factory=dict)

    # Context window management
    total_tokens: int = 0
    max_tokens: int = 8000


class ValidationResult(BaseModel):
    """Result of Proof of Graph validation."""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    suggested_fixes: List[str] = Field(default_factory=list)
    referenced_entities: List[str] = Field(default_factory=list)
    missing_entities: List[str] = Field(default_factory=list)


class RAGResult(BaseModel):
    """Final result from the RAG pipeline."""
    query: str
    context: RAGContext
    response: str
    validation: ValidationResult
    sources: List[str] = Field(default_factory=list)
    confidence: float = 0.0
    processing_time_ms: int = 0
```

---

### File 2: `src/graphrag/embedding_provider.py` (~200 lines)

```python
"""
Unified embedding provider interface.
Supports OpenAI, Anthropic (future), HuggingFace, and local models.
"""
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from .types import EmbeddingProvider

logger = logging.getLogger(__name__)


class BaseEmbeddingProvider(ABC):
    """Abstract base for embedding providers."""

    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        pass

    @abstractmethod
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        pass

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return embedding dimension."""
        pass


class EmbeddingCache:
    """Cache embeddings to avoid recomputation."""

    def __init__(self, cache_dir: str = "data/graphrag/cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._memory_cache: Dict[str, List[float]] = {}

    def _hash_text(self, text: str, model: str) -> str:
        return hashlib.md5(f"{model}:{text}".encode()).hexdigest()

    def get(self, text: str, model: str) -> Optional[List[float]]:
        key = self._hash_text(text, model)
        if key in self._memory_cache:
            return self._memory_cache[key]

        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            with open(cache_file) as f:
                embedding = json.load(f)
                self._memory_cache[key] = embedding
                return embedding
        return None

    def set(self, text: str, model: str, embedding: List[float]) -> None:
        key = self._hash_text(text, model)
        self._memory_cache[key] = embedding
        cache_file = self.cache_dir / f"{key}.json"
        with open(cache_file, "w") as f:
            json.dump(embedding, f)


class OpenAIEmbeddings(BaseEmbeddingProvider):
    """OpenAI embedding provider."""

    def __init__(
        self,
        model: str = "text-embedding-3-small",
        api_key: Optional[str] = None,
        cache: Optional[EmbeddingCache] = None,
    ):
        self.model = model
        self.api_key = api_key or self._get_api_key()
        self.cache = cache or EmbeddingCache()
        self._dimension = 1536 if "small" in model else 3072

    def _get_api_key(self) -> str:
        import os
        return os.getenv("OPENAI_API_KEY", "")

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, text: str) -> List[float]:
        cached = self.cache.get(text, self.model)
        if cached:
            return cached

        try:
            import openai
            client = openai.OpenAI(api_key=self.api_key)
            response = client.embeddings.create(
                model=self.model,
                input=text,
            )
            embedding = response.data[0].embedding
            self.cache.set(text, self.model, embedding)
            return embedding
        except ImportError:
            logger.error("openai package not installed")
            return [0.0] * self.dimension
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return [0.0] * self.dimension

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        # Check cache first
        results = []
        uncached_texts = []
        uncached_indices = []

        for i, text in enumerate(texts):
            cached = self.cache.get(text, self.model)
            if cached:
                results.append(cached)
            else:
                results.append(None)
                uncached_texts.append(text)
                uncached_indices.append(i)

        if uncached_texts:
            try:
                import openai
                client = openai.OpenAI(api_key=self.api_key)
                response = client.embeddings.create(
                    model=self.model,
                    input=uncached_texts,
                )
                for j, emb_data in enumerate(response.data):
                    idx = uncached_indices[j]
                    embedding = emb_data.embedding
                    results[idx] = embedding
                    self.cache.set(uncached_texts[j], self.model, embedding)
            except Exception as e:
                logger.error(f"Batch embedding failed: {e}")
                for idx in uncached_indices:
                    results[idx] = [0.0] * self.dimension

        return results


class HuggingFaceEmbeddings(BaseEmbeddingProvider):
    """HuggingFace sentence-transformers provider."""

    def __init__(
        self,
        model: str = "all-MiniLM-L6-v2",
        cache: Optional[EmbeddingCache] = None,
    ):
        self.model_name = model
        self.cache = cache or EmbeddingCache()
        self._model = None
        self._dimension = 384  # Default for MiniLM

    def _load_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name)
                self._dimension = self._model.get_sentence_embedding_dimension()
            except ImportError:
                logger.error("sentence-transformers not installed")

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, text: str) -> List[float]:
        cached = self.cache.get(text, self.model_name)
        if cached:
            return cached

        self._load_model()
        if self._model:
            embedding = self._model.encode(text).tolist()
            self.cache.set(text, self.model_name, embedding)
            return embedding
        return [0.0] * self.dimension

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        self._load_model()
        if self._model:
            embeddings = self._model.encode(texts).tolist()
            for text, emb in zip(texts, embeddings):
                self.cache.set(text, self.model_name, emb)
            return embeddings
        return [[0.0] * self.dimension for _ in texts]


def get_embedding_provider(
    provider: EmbeddingProvider,
    model: Optional[str] = None,
    **kwargs
) -> BaseEmbeddingProvider:
    """Factory to get embedding provider."""
    if provider == EmbeddingProvider.OPENAI:
        return OpenAIEmbeddings(model=model or "text-embedding-3-small", **kwargs)
    elif provider == EmbeddingProvider.HUGGINGFACE:
        return HuggingFaceEmbeddings(model=model or "all-MiniLM-L6-v2", **kwargs)
    else:
        raise ValueError(f"Unsupported provider: {provider}")
```

---

### File 3: `src/graphrag/vector_store.py` (~250 lines)

```python
"""
Vector database abstraction layer.
Supports ChromaDB (local), Pinecone (cloud), and SQLite (lightweight).
"""
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from .types import VectorStore, RetrievedItem, RetrievalSource

logger = logging.getLogger(__name__)


class BaseVectorStore(ABC):
    """Abstract base for vector stores."""

    @abstractmethod
    def upsert(self, id: str, embedding: List[float], metadata: Dict[str, Any]) -> bool:
        """Insert or update a vector."""
        pass

    @abstractmethod
    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[str, float, Dict[str, Any]]]:
        """Search for similar vectors. Returns [(id, score, metadata), ...]"""
        pass

    @abstractmethod
    def delete(self, id: str) -> bool:
        """Delete a vector by ID."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Return total number of vectors."""
        pass


class ChromaVectorStore(BaseVectorStore):
    """ChromaDB vector store (local, easy setup)."""

    def __init__(
        self,
        collection_name: str = "databridge",
        persist_dir: str = "data/graphrag/chroma",
    ):
        self.collection_name = collection_name
        self.persist_dir = persist_dir
        self._client = None
        self._collection = None

    def _init_client(self):
        if self._client is None:
            try:
                import chromadb
                from chromadb.config import Settings

                self._client = chromadb.PersistentClient(
                    path=self.persist_dir,
                    settings=Settings(anonymized_telemetry=False),
                )
                self._collection = self._client.get_or_create_collection(
                    name=self.collection_name,
                    metadata={"hnsw:space": "cosine"},
                )
            except ImportError:
                logger.error("chromadb not installed: pip install chromadb")
                raise

    def upsert(self, id: str, embedding: List[float], metadata: Dict[str, Any]) -> bool:
        self._init_client()
        try:
            # ChromaDB requires string metadata values
            str_metadata = {k: str(v) if not isinstance(v, (str, int, float, bool)) else v
                          for k, v in metadata.items()}

            self._collection.upsert(
                ids=[id],
                embeddings=[embedding],
                metadatas=[str_metadata],
                documents=[metadata.get("content", "")],
            )
            return True
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            return False

    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[str, float, Dict[str, Any]]]:
        self._init_client()
        try:
            results = self._collection.query(
                query_embeddings=[query_embedding],
                n_results=top_k,
                where=filter,
            )

            output = []
            if results["ids"] and results["ids"][0]:
                for i, id in enumerate(results["ids"][0]):
                    score = 1 - results["distances"][0][i]  # Cosine similarity
                    metadata = results["metadatas"][0][i] if results["metadatas"] else {}
                    output.append((id, score, metadata))
            return output
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def delete(self, id: str) -> bool:
        self._init_client()
        try:
            self._collection.delete(ids=[id])
            return True
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return False

    def count(self) -> int:
        self._init_client()
        return self._collection.count()


class SQLiteVectorStore(BaseVectorStore):
    """Lightweight SQLite vector store (no external dependencies)."""

    def __init__(self, db_path: str = "data/graphrag/vectors.db"):
        import sqlite3
        import numpy as np

        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS vectors (
                id TEXT PRIMARY KEY,
                embedding BLOB,
                metadata TEXT,
                content TEXT
            )
        """)
        self.conn.commit()

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        import numpy as np
        a, b = np.array(a), np.array(b)
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))

    def upsert(self, id: str, embedding: List[float], metadata: Dict[str, Any]) -> bool:
        try:
            import numpy as np
            emb_bytes = np.array(embedding, dtype=np.float32).tobytes()
            self.conn.execute(
                "INSERT OR REPLACE INTO vectors (id, embedding, metadata, content) VALUES (?, ?, ?, ?)",
                (id, emb_bytes, json.dumps(metadata), metadata.get("content", "")),
            )
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            return False

    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[str, float, Dict[str, Any]]]:
        import numpy as np

        cursor = self.conn.execute("SELECT id, embedding, metadata FROM vectors")
        results = []

        for row in cursor:
            id, emb_bytes, meta_str = row
            embedding = np.frombuffer(emb_bytes, dtype=np.float32).tolist()
            metadata = json.loads(meta_str)

            # Apply filter
            if filter:
                match = all(metadata.get(k) == v for k, v in filter.items())
                if not match:
                    continue

            score = self._cosine_similarity(query_embedding, embedding)
            results.append((id, score, metadata))

        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    def delete(self, id: str) -> bool:
        try:
            self.conn.execute("DELETE FROM vectors WHERE id = ?", (id,))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return False

    def count(self) -> int:
        cursor = self.conn.execute("SELECT COUNT(*) FROM vectors")
        return cursor.fetchone()[0]


def get_vector_store(
    store_type: VectorStore,
    **kwargs,
) -> BaseVectorStore:
    """Factory to get vector store."""
    if store_type == VectorStore.CHROMA:
        return ChromaVectorStore(**kwargs)
    elif store_type == VectorStore.SQLITE:
        return SQLiteVectorStore(**kwargs)
    else:
        raise ValueError(f"Unsupported vector store: {store_type}")
```

---

### File 4: `src/graphrag/retriever.py` (~300 lines)

```python
"""
Hybrid retrieval combining vector, graph, and lexical search.
Implements Reciprocal Rank Fusion (RRF) for result combination.
"""
import logging
from typing import Dict, List, Optional, Any

from .types import (
    RAGQuery, RetrievedItem, RetrievalSource, ExtractedEntity, EntityType
)
from .embedding_provider import BaseEmbeddingProvider
from .vector_store import BaseVectorStore

logger = logging.getLogger(__name__)


class HybridRetriever:
    """
    Combines multiple retrieval strategies:
    1. Vector search (semantic similarity)
    2. Graph search (lineage traversal)
    3. Lexical search (catalog keyword search)
    4. Template matching (skill/template relevance)
    """

    def __init__(
        self,
        embedding_provider: BaseEmbeddingProvider,
        vector_store: BaseVectorStore,
        catalog_store=None,
        lineage_tracker=None,
        template_service=None,
        knowledge_base=None,
    ):
        self.embedder = embedding_provider
        self.vector_store = vector_store
        self.catalog = catalog_store
        self.lineage = lineage_tracker
        self.templates = template_service
        self.knowledge = knowledge_base

    def retrieve(
        self,
        query: RAGQuery,
        vector_weight: float = 0.4,
        graph_weight: float = 0.3,
        lexical_weight: float = 0.2,
        template_weight: float = 0.1,
    ) -> List[RetrievedItem]:
        """
        Perform hybrid retrieval with weighted fusion.

        Args:
            query: The RAG query with extracted entities
            *_weight: Weights for each retrieval source (should sum to 1.0)

        Returns:
            List of retrieved items, sorted by fused score
        """
        all_results: Dict[str, Dict[str, Any]] = {}

        # 1. Vector Search
        if self.vector_store:
            vector_results = self._vector_search(query)
            for item in vector_results:
                if item.id not in all_results:
                    all_results[item.id] = {"item": item, "scores": {}}
                all_results[item.id]["scores"]["vector"] = item.score

        # 2. Graph Search (Lineage)
        if self.lineage and query.include_lineage:
            graph_results = self._graph_search(query)
            for item in graph_results:
                if item.id not in all_results:
                    all_results[item.id] = {"item": item, "scores": {}}
                all_results[item.id]["scores"]["graph"] = item.score

        # 3. Lexical Search (Catalog)
        if self.catalog:
            lexical_results = self._lexical_search(query)
            for item in lexical_results:
                if item.id not in all_results:
                    all_results[item.id] = {"item": item, "scores": {}}
                all_results[item.id]["scores"]["lexical"] = item.score

        # 4. Template Matching
        if self.templates and query.include_templates:
            template_results = self._template_search(query)
            for item in template_results:
                if item.id not in all_results:
                    all_results[item.id] = {"item": item, "scores": {}}
                all_results[item.id]["scores"]["template"] = item.score

        # Fuse results using Reciprocal Rank Fusion
        fused_results = self._reciprocal_rank_fusion(
            all_results,
            weights={
                "vector": vector_weight,
                "graph": graph_weight,
                "lexical": lexical_weight,
                "template": template_weight,
            }
        )

        return fused_results[:query.max_results]

    def _vector_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search by embedding similarity."""
        try:
            query_embedding = self.embedder.embed(query.query)
            results = self.vector_store.search(
                query_embedding=query_embedding,
                top_k=query.max_results * 2,  # Over-fetch for fusion
            )

            return [
                RetrievedItem(
                    id=id,
                    source=RetrievalSource.VECTOR,
                    content=metadata.get("content", ""),
                    score=score,
                    metadata=metadata,
                )
                for id, score, metadata in results
            ]
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []

    def _graph_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search by lineage graph traversal."""
        results = []

        # Find entities that match lineage nodes
        for entity in query.entities:
            if entity.entity_type in (EntityType.TABLE, EntityType.HIERARCHY):
                # Get upstream and downstream
                try:
                    # Check if entity exists in lineage
                    for graph_name in self.lineage.list_graphs():
                        graph = self.lineage.get_graph(graph_name)
                        if not graph:
                            continue

                        for node in graph.nodes:
                            if entity.text.lower() in node.name.lower():
                                # Found matching node
                                upstream = self.lineage.get_all_upstream(graph_name, node.id)
                                downstream = self.lineage.get_all_downstream(graph_name, node.id)

                                # Score based on distance (closer = higher score)
                                for i, up_node in enumerate(upstream[:5]):
                                    score = 1.0 / (i + 2)  # Decay by distance
                                    results.append(RetrievedItem(
                                        id=f"lineage:{graph_name}:{up_node.id}",
                                        source=RetrievalSource.GRAPH,
                                        content=f"Upstream: {up_node.name} ({up_node.node_type})",
                                        score=score,
                                        metadata={"node": up_node.model_dump(), "direction": "upstream"},
                                    ))

                                for i, down_node in enumerate(downstream[:5]):
                                    score = 1.0 / (i + 2)
                                    results.append(RetrievedItem(
                                        id=f"lineage:{graph_name}:{down_node.id}",
                                        source=RetrievalSource.GRAPH,
                                        content=f"Downstream: {down_node.name} ({down_node.node_type})",
                                        score=score,
                                        metadata={"node": down_node.model_dump(), "direction": "downstream"},
                                    ))
                except Exception as e:
                    logger.debug(f"Lineage search error: {e}")

        return results

    def _lexical_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search catalog using keyword/lexical search."""
        try:
            search_results = self.catalog.search(
                query=query.query,
                limit=query.max_results * 2,
            )

            return [
                RetrievedItem(
                    id=f"catalog:{r['id']}",
                    source=RetrievalSource.LEXICAL,
                    content=f"{r['name']}: {r.get('description', '')}",
                    score=r.get("relevance_score", 0.5),
                    metadata=r,
                )
                for r in search_results.get("results", [])
            ]
        except Exception as e:
            logger.error(f"Lexical search failed: {e}")
            return []

    def _template_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search for matching templates and skills."""
        results = []

        try:
            # Search templates by domain/industry
            if self.templates:
                templates = self.templates.list_templates(
                    domain=query.domain,
                    industry=query.industry,
                )

                for t in templates.get("templates", [])[:5]:
                    # Score based on keyword match
                    score = 0.5
                    query_lower = query.query.lower()
                    if t.get("name", "").lower() in query_lower:
                        score = 0.9
                    elif t.get("domain", "").lower() in query_lower:
                        score = 0.7

                    results.append(RetrievedItem(
                        id=f"template:{t['id']}",
                        source=RetrievalSource.TEMPLATE,
                        content=f"Template: {t['name']} - {t.get('description', '')}",
                        score=score,
                        metadata=t,
                    ))
        except Exception as e:
            logger.debug(f"Template search error: {e}")

        return results

    def _reciprocal_rank_fusion(
        self,
        results: Dict[str, Dict[str, Any]],
        weights: Dict[str, float],
        k: int = 60,
    ) -> List[RetrievedItem]:
        """
        Combine results using Reciprocal Rank Fusion.

        RRF score = sum(weight_i / (k + rank_i)) for each source
        """
        # Sort each source by score to get ranks
        source_ranks: Dict[str, Dict[str, int]] = {}

        for source in ["vector", "graph", "lexical", "template"]:
            source_items = [
                (id, data["scores"].get(source, 0))
                for id, data in results.items()
                if source in data["scores"]
            ]
            source_items.sort(key=lambda x: x[1], reverse=True)
            source_ranks[source] = {id: rank + 1 for rank, (id, _) in enumerate(source_items)}

        # Calculate RRF scores
        fused_scores = {}
        for id, data in results.items():
            rrf_score = 0
            for source, weight in weights.items():
                if source in source_ranks and id in source_ranks[source]:
                    rank = source_ranks[source][id]
                    rrf_score += weight / (k + rank)
            fused_scores[id] = rrf_score

        # Sort by fused score and return
        sorted_items = sorted(fused_scores.items(), key=lambda x: x[1], reverse=True)

        return [
            RetrievedItem(
                id=results[id]["item"].id,
                source=results[id]["item"].source,
                content=results[id]["item"].content,
                score=score,
                metadata=results[id]["item"].metadata,
            )
            for id, score in sorted_items
        ]
```

---

### File 5: `src/graphrag/proof_of_graph.py` (~200 lines)

```python
"""
Proof of Graph - Validation layer to prevent hallucinations.
Validates generated content against the knowledge graph structure.
"""
import logging
import re
from typing import Dict, List, Optional, Set, Any

from .types import ValidationResult, RAGContext

logger = logging.getLogger(__name__)


class ProofOfGraph:
    """
    Validates AI-generated content against known entities.

    Prevents hallucinations by:
    1. Checking table/column references exist in catalog
    2. Verifying hierarchy relationships match lineage
    3. Flagging unknown entity references
    4. Suggesting corrections from known entities
    """

    def __init__(
        self,
        catalog_store=None,
        lineage_tracker=None,
        hierarchy_service=None,
    ):
        self.catalog = catalog_store
        self.lineage = lineage_tracker
        self.hierarchy = hierarchy_service

        # Build entity index
        self._known_tables: Set[str] = set()
        self._known_columns: Dict[str, Set[str]] = {}
        self._known_hierarchies: Set[str] = set()
        self._refresh_index()

    def _refresh_index(self) -> None:
        """Refresh the index of known entities."""
        # Get tables from catalog
        if self.catalog:
            try:
                assets = self.catalog.list_assets(asset_types=["TABLE", "VIEW"])
                for asset in assets.get("assets", []):
                    table_name = asset.get("name", "").upper()
                    self._known_tables.add(table_name)

                    # Index columns
                    for col in asset.get("columns", []):
                        col_name = col.get("name", "").upper()
                        self._known_columns.setdefault(table_name, set()).add(col_name)
            except Exception as e:
                logger.debug(f"Failed to index catalog: {e}")

        # Get hierarchies
        if self.hierarchy:
            try:
                projects = self.hierarchy.list_projects()
                for proj in projects:
                    hierarchies = self.hierarchy.list_hierarchies(proj["id"])
                    for h in hierarchies:
                        self._known_hierarchies.add(h.get("name", "").upper())
            except Exception as e:
                logger.debug(f"Failed to index hierarchies: {e}")

    def validate(
        self,
        content: str,
        context: RAGContext,
        content_type: str = "sql",
    ) -> ValidationResult:
        """
        Validate generated content against knowledge graph.

        Args:
            content: The generated content (SQL, hierarchy definition, etc.)
            context: The RAG context used for generation
            content_type: Type of content ("sql", "hierarchy", "dbt", "yaml")

        Returns:
            ValidationResult with errors, warnings, and suggestions
        """
        errors = []
        warnings = []
        suggestions = []
        referenced = []
        missing = []

        if content_type == "sql":
            self._validate_sql(content, errors, warnings, suggestions, referenced, missing)
        elif content_type == "hierarchy":
            self._validate_hierarchy(content, errors, warnings, suggestions, referenced, missing)
        elif content_type == "dbt":
            self._validate_dbt(content, errors, warnings, suggestions, referenced, missing)

        # Check against context's available entities
        if context.available_tables:
            for table in self._extract_table_references(content):
                if table.upper() not in [t.upper() for t in context.available_tables]:
                    if table.upper() not in self._known_tables:
                        errors.append(f"Unknown table: {table}")
                        missing.append(table)

                        # Suggest similar known tables
                        similar = self._find_similar(table, self._known_tables)
                        if similar:
                            suggestions.append(f"Did you mean '{similar}' instead of '{table}'?")

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            suggested_fixes=suggestions,
            referenced_entities=referenced,
            missing_entities=missing,
        )

    def _validate_sql(
        self,
        sql: str,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str],
        referenced: List[str],
        missing: List[str],
    ) -> None:
        """Validate SQL content."""
        # Extract table references
        tables = self._extract_table_references(sql)
        for table in tables:
            referenced.append(f"TABLE:{table}")
            if table.upper() not in self._known_tables:
                # Check if it's a CTE or subquery alias
                if not self._is_cte_or_alias(sql, table):
                    warnings.append(f"Table '{table}' not found in catalog")
                    missing.append(table)

        # Extract column references
        columns = self._extract_column_references(sql)
        for col in columns:
            referenced.append(f"COLUMN:{col}")

    def _validate_hierarchy(
        self,
        content: str,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str],
        referenced: List[str],
        missing: List[str],
    ) -> None:
        """Validate hierarchy definition content."""
        # Check for hierarchy name references
        hier_pattern = r'hierarchy[_\s]*(?:name|id)?["\']?\s*[:=]\s*["\']?(\w+)'
        matches = re.findall(hier_pattern, content, re.IGNORECASE)
        for match in matches:
            referenced.append(f"HIERARCHY:{match}")

    def _validate_dbt(
        self,
        content: str,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str],
        referenced: List[str],
        missing: List[str],
    ) -> None:
        """Validate dbt model content."""
        # Check ref() and source() calls
        ref_pattern = r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}"
        source_pattern = r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}"

        for match in re.findall(ref_pattern, content):
            referenced.append(f"DBT_REF:{match}")

        for source, table in re.findall(source_pattern, content):
            referenced.append(f"SOURCE:{source}.{table}")

    def _extract_table_references(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        tables = []

        # FROM clause
        from_pattern = r'\bFROM\s+([a-zA-Z_][\w.]*)'
        tables.extend(re.findall(from_pattern, sql, re.IGNORECASE))

        # JOIN clause
        join_pattern = r'\bJOIN\s+([a-zA-Z_][\w.]*)'
        tables.extend(re.findall(join_pattern, sql, re.IGNORECASE))

        return list(set(tables))

    def _extract_column_references(self, sql: str) -> List[str]:
        """Extract column names from SQL."""
        # Simplified column extraction
        col_pattern = r'\b([a-zA-Z_]\w*)\s*(?:=|<|>|LIKE|IN|IS)'
        return list(set(re.findall(col_pattern, sql, re.IGNORECASE)))

    def _is_cte_or_alias(self, sql: str, name: str) -> bool:
        """Check if name is a CTE or subquery alias."""
        cte_pattern = rf'\bWITH\b.*?\b{name}\b\s+AS'
        alias_pattern = rf'\)\s+(?:AS\s+)?{name}\b'
        return bool(re.search(cte_pattern, sql, re.IGNORECASE | re.DOTALL)) or \
               bool(re.search(alias_pattern, sql, re.IGNORECASE))

    def _find_similar(self, name: str, candidates: Set[str], threshold: float = 0.6) -> Optional[str]:
        """Find similar entity name using fuzzy matching."""
        try:
            from rapidfuzz import fuzz
            best_match = None
            best_score = 0

            for candidate in candidates:
                score = fuzz.ratio(name.upper(), candidate.upper()) / 100
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = candidate

            return best_match
        except ImportError:
            return None
```

---

## MCP Tools (10 tools)

| Tool | Description |
|------|-------------|
| `rag_configure` | Configure RAG engine (embedding provider, vector store) |
| `rag_index_catalog` | Index catalog assets into vector store |
| `rag_index_hierarchies` | Index hierarchy structures |
| `rag_search` | Hybrid retrieval search |
| `rag_get_context` | Get assembled context for LLM |
| `rag_validate_output` | Proof of Graph validation |
| `rag_entity_extract` | Extract entities from query |
| `rag_explain_lineage` | Natural language lineage explanation |
| `rag_suggest_schema` | AI-suggested schema based on context |
| `rag_get_stats` | Get RAG engine statistics |

---

## Integration with Cortex Agent

```python
# In cortex_agent/reasoning_loop.py - OBSERVE phase enhancement:

def observe(self, goal: str) -> Dict[str, Any]:
    """Enhanced OBSERVE with RAG context."""

    # Get RAG context
    rag_context = self.rag_engine.get_context(RAGQuery(
        query=goal,
        include_lineage=True,
        include_templates=True,
    ))

    # Include in observation
    return {
        "goal": goal,
        "rag_context": {
            "relevant_tables": [t["name"] for t in rag_context.catalog_assets],
            "lineage_paths": rag_context.lineage_paths,
            "applicable_templates": [t["name"] for t in rag_context.templates],
            "glossary_terms": rag_context.glossary_terms,
        },
        "available_entities": rag_context.available_tables,
        "constraints": "Only use entities listed in available_entities",
    }
```

---

## Tool Count Summary

| Category | Tools |
|----------|-------|
| Phase 31 (GraphRAG) | 10 |
| **Previous Total** | 316 |
| **New Total** | **326** |
