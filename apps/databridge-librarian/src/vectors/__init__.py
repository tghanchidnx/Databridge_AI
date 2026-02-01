"""Vector embeddings and RAG module for AI-enhanced hierarchy understanding."""

from .store import (
    VectorStore,
    CollectionType,
    Document,
    SearchResult,
    VectorStoreResult,
)
from .embedder import (
    Embedder,
    HierarchyEmbedder,
    ConceptEmbedder,
    EmbeddingProvider,
    EmbeddingResult,
)
from .rag import (
    HierarchyRAG,
    RAGContext,
)
from .industry_patterns import (
    INDUSTRY_PATTERNS,
    INDUSTRIES,
    get_all_patterns,
    get_patterns_by_industry,
    get_pattern_by_id,
    get_industries,
    get_industry_info,
    IndustryPatternLoader,
)

__all__ = [
    # Store
    "VectorStore",
    "CollectionType",
    "Document",
    "SearchResult",
    "VectorStoreResult",
    # Embedder
    "Embedder",
    "HierarchyEmbedder",
    "ConceptEmbedder",
    "EmbeddingProvider",
    "EmbeddingResult",
    # RAG
    "HierarchyRAG",
    "RAGContext",
    # Industry Patterns
    "INDUSTRY_PATTERNS",
    "INDUSTRIES",
    "get_all_patterns",
    "get_patterns_by_industry",
    "get_pattern_by_id",
    "get_industries",
    "get_industry_info",
    "IndustryPatternLoader",
]
