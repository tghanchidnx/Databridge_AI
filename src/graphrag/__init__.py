"""
GraphRAG Module - Phase 31

Anti-hallucination layer using Graph + Vector retrieval-augmented generation.

Key Components:
- EmbeddingProvider: Unified embedding interface (OpenAI, HuggingFace)
- VectorStore: Vector database abstraction (SQLite, ChromaDB)
- EntityExtractor: Extract entities from natural language
- ProofOfGraph: Validate AI outputs against knowledge graph
- HybridRetriever: Combine vector, graph, and lexical search
- MCP Tools: 10 tools for RAG-enhanced AI interactions

Strategic Alignment:
1. Enforces Structural Context - Anchors AI in verified hierarchies
2. Replicates Consultant Memory - Vector DB for context retrieval
3. Proof of Graph - Validates generated content against catalog
"""
from .types import (
    EmbeddingProvider,
    VectorStoreType,
    RetrievalSource,
    EntityType,
    ExtractedEntity,
    RAGQuery,
    RAGContext,
    RAGResult,
    RAGConfig,
    ValidationResult,
    ValidationIssue,
    ValidationSeverity,
    IndexStats,
)
from .embedding_provider import (
    BaseEmbeddingProvider,
    OpenAIEmbeddings,
    HuggingFaceEmbeddings,
    MockEmbeddings,
    EmbeddingCache,
    get_embedding_provider,
)
from .vector_store import (
    BaseVectorStore,
    SQLiteVectorStore,
    ChromaVectorStore,
    get_vector_store,
)
from .entity_extractor import EntityExtractor
from .proof_of_graph import ProofOfGraph
from .retriever import HybridRetriever
from .mcp_tools import register_graphrag_tools

__all__ = [
    # Types
    "EmbeddingProvider",
    "VectorStoreType",
    "RetrievalSource",
    "EntityType",
    "ExtractedEntity",
    "RAGQuery",
    "RAGContext",
    "RAGResult",
    "RAGConfig",
    "ValidationResult",
    "ValidationIssue",
    "ValidationSeverity",
    "IndexStats",
    # Embeddings
    "BaseEmbeddingProvider",
    "OpenAIEmbeddings",
    "HuggingFaceEmbeddings",
    "MockEmbeddings",
    "EmbeddingCache",
    "get_embedding_provider",
    # Vector Store
    "BaseVectorStore",
    "SQLiteVectorStore",
    "ChromaVectorStore",
    "get_vector_store",
    # Core Components
    "EntityExtractor",
    "ProofOfGraph",
    "HybridRetriever",
    # MCP Registration
    "register_graphrag_tools",
]
