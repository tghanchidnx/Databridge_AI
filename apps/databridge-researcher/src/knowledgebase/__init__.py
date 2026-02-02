"""
Knowledge Base module for DataBridge Analytics Researcher.

RAG pipeline and vector embeddings:
- ChromaDB vector store integration
- Business glossary embeddings
- Metric definitions
- Query examples and patterns
- Industry-specific terminology
"""

from .store import (
    KnowledgeBaseStore,
    KBCollectionType,
    KBDocument,
    KBSearchResult,
    KBResult,
)

from .glossary import (
    GlossaryLoader,
    BUSINESS_GLOSSARY,
    METRIC_DEFINITIONS,
    FPA_CONCEPTS,
    get_glossary_terms,
    get_metric_definitions,
    get_fpa_concepts,
)

__all__ = [
    # Store classes
    "KnowledgeBaseStore",
    "KBCollectionType",
    "KBDocument",
    "KBSearchResult",
    "KBResult",
    # Glossary classes
    "GlossaryLoader",
    # Data constants
    "BUSINESS_GLOSSARY",
    "METRIC_DEFINITIONS",
    "FPA_CONCEPTS",
    # Utility functions
    "get_glossary_terms",
    "get_metric_definitions",
    "get_fpa_concepts",
]
