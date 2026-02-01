"""
RAG (Retrieval Augmented Generation) pipeline for DataBridge AI V3.

Provides semantic search and context retrieval for hierarchy understanding.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
import logging

from .store import VectorStore, CollectionType, Document, VectorStoreResult
from .embedder import HierarchyEmbedder, ConceptEmbedder, Embedder


logger = logging.getLogger(__name__)


@dataclass
class RAGContext:
    """Context retrieved for RAG."""

    query: str
    results: List[Dict[str, Any]] = field(default_factory=list)
    context_text: str = ""
    sources: List[str] = field(default_factory=list)
    total_results: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "query": self.query,
            "results": self.results,
            "context_text": self.context_text,
            "sources": self.sources,
            "total_results": self.total_results,
        }


class HierarchyRAG:
    """
    RAG pipeline for hierarchy-related queries.

    Provides:
    - Semantic search across hierarchies
    - Context retrieval for LLM prompts
    - Multi-collection search (hierarchies, patterns, concepts)
    - Relevance filtering and ranking
    """

    def __init__(
        self,
        vector_store: Optional[VectorStore] = None,
        embedder: Optional[Embedder] = None,
        persist_directory: Optional[str] = None,
    ):
        """
        Initialize the RAG pipeline.

        Args:
            vector_store: Existing VectorStore instance.
            embedder: Existing Embedder instance.
            persist_directory: Directory for vector storage.
        """
        self.embedder = embedder or Embedder()
        self.hierarchy_embedder = HierarchyEmbedder(embedder=self.embedder)
        self.concept_embedder = ConceptEmbedder(embedder=self.embedder)

        self.vector_store = vector_store or VectorStore(
            persist_directory=persist_directory,
        )

        # Initialize collections
        self._init_collections()

    def _init_collections(self) -> None:
        """Initialize required collections."""
        for collection_type in [
            CollectionType.HIERARCHIES,
            CollectionType.INDUSTRY_PATTERNS,
            CollectionType.WHITEPAPER_CONCEPTS,
        ]:
            self.vector_store.create_collection(collection_type)

    def index_hierarchy(
        self,
        hierarchy: Dict[str, Any],
        project_id: Optional[str] = None,
    ) -> VectorStoreResult:
        """
        Index a hierarchy for semantic search.

        Args:
            hierarchy: Hierarchy data dictionary.
            project_id: Optional project ID for filtering.

        Returns:
            VectorStoreResult with indexing status.
        """
        try:
            # Generate text representation
            text = self.hierarchy_embedder.hierarchy_to_text(hierarchy)

            # Generate embedding
            embed_result = self.embedder.embed(text)
            if not embed_result.success:
                return VectorStoreResult(
                    success=False,
                    message="Failed to generate embedding",
                    errors=embed_result.errors,
                )

            # Create document
            hierarchy_id = hierarchy.get("id") or hierarchy.get("hierarchy_id", "")
            doc = Document(
                id=f"hierarchy_{hierarchy_id}",
                content=text,
                metadata={
                    "hierarchy_id": str(hierarchy_id),
                    "name": hierarchy.get("name") or hierarchy.get("hierarchy_name", ""),
                    "project_id": str(project_id) if project_id else "",
                    "formula_group": hierarchy.get("formula_group", ""),
                    "type": "hierarchy",
                },
                embedding=embed_result.embeddings[0] if embed_result.embeddings else None,
            )

            # Upsert to vector store
            return self.vector_store.upsert(CollectionType.HIERARCHIES, [doc])

        except Exception as e:
            logger.error(f"Failed to index hierarchy: {e}")
            return VectorStoreResult(
                success=False,
                message=f"Failed to index hierarchy: {str(e)}",
                errors=[str(e)],
            )

    def index_hierarchies(
        self,
        hierarchies: List[Dict[str, Any]],
        project_id: Optional[str] = None,
        batch_size: int = 32,
    ) -> VectorStoreResult:
        """
        Index multiple hierarchies.

        Args:
            hierarchies: List of hierarchy data dictionaries.
            project_id: Optional project ID for filtering.
            batch_size: Batch size for processing.

        Returns:
            VectorStoreResult with indexing status.
        """
        try:
            # Generate texts
            texts = [
                self.hierarchy_embedder.hierarchy_to_text(h)
                for h in hierarchies
            ]

            # Generate embeddings in batch
            embed_result = self.embedder.embed(texts, batch_size=batch_size)
            if not embed_result.success:
                return VectorStoreResult(
                    success=False,
                    message="Failed to generate embeddings",
                    errors=embed_result.errors,
                )

            # Create documents
            documents = []
            for i, h in enumerate(hierarchies):
                hierarchy_id = h.get("id") or h.get("hierarchy_id", "")
                doc = Document(
                    id=f"hierarchy_{hierarchy_id}",
                    content=texts[i],
                    metadata={
                        "hierarchy_id": str(hierarchy_id),
                        "name": h.get("name") or h.get("hierarchy_name", ""),
                        "project_id": str(project_id) if project_id else "",
                        "formula_group": h.get("formula_group", ""),
                        "type": "hierarchy",
                    },
                    embedding=embed_result.embeddings[i] if i < len(embed_result.embeddings) else None,
                )
                documents.append(doc)

            # Upsert to vector store
            return self.vector_store.upsert(CollectionType.HIERARCHIES, documents, batch_size)

        except Exception as e:
            logger.error(f"Failed to index hierarchies: {e}")
            return VectorStoreResult(
                success=False,
                message=f"Failed to index hierarchies: {str(e)}",
                errors=[str(e)],
            )

    def index_industry_pattern(
        self,
        pattern: Dict[str, Any],
    ) -> VectorStoreResult:
        """
        Index an industry pattern.

        Args:
            pattern: Industry pattern data dictionary.

        Returns:
            VectorStoreResult with indexing status.
        """
        try:
            text = self.concept_embedder.industry_pattern_to_text(pattern)
            embed_result = self.embedder.embed(text)

            if not embed_result.success:
                return VectorStoreResult(
                    success=False,
                    message="Failed to generate embedding",
                    errors=embed_result.errors,
                )

            pattern_id = pattern.get("id") or pattern.get("name", "").replace(" ", "_").lower()
            doc = Document(
                id=f"pattern_{pattern_id}",
                content=text,
                metadata={
                    "pattern_id": pattern_id,
                    "industry": pattern.get("industry", ""),
                    "name": pattern.get("name", ""),
                    "hierarchy_type": pattern.get("hierarchy_type", ""),
                    "type": "industry_pattern",
                },
                embedding=embed_result.embeddings[0] if embed_result.embeddings else None,
            )

            return self.vector_store.upsert(CollectionType.INDUSTRY_PATTERNS, [doc])

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to index pattern: {str(e)}",
                errors=[str(e)],
            )

    def index_concept(
        self,
        concept: Dict[str, Any],
    ) -> VectorStoreResult:
        """
        Index a whitepaper concept.

        Args:
            concept: Concept data dictionary.

        Returns:
            VectorStoreResult with indexing status.
        """
        try:
            text = self.concept_embedder.concept_to_text(concept)
            embed_result = self.embedder.embed(text)

            if not embed_result.success:
                return VectorStoreResult(
                    success=False,
                    message="Failed to generate embedding",
                    errors=embed_result.errors,
                )

            concept_id = concept.get("id") or concept.get("title", "").replace(" ", "_").lower()
            doc = Document(
                id=f"concept_{concept_id}",
                content=text,
                metadata={
                    "concept_id": concept_id,
                    "title": concept.get("title", ""),
                    "category": concept.get("category", ""),
                    "type": "whitepaper_concept",
                },
                embedding=embed_result.embeddings[0] if embed_result.embeddings else None,
            )

            return self.vector_store.upsert(CollectionType.WHITEPAPER_CONCEPTS, [doc])

        except Exception as e:
            return VectorStoreResult(
                success=False,
                message=f"Failed to index concept: {str(e)}",
                errors=[str(e)],
            )

    def search_hierarchies(
        self,
        query: str,
        project_id: Optional[str] = None,
        n_results: int = 10,
        min_score: float = 0.5,
    ) -> RAGContext:
        """
        Search hierarchies semantically.

        Args:
            query: Search query.
            project_id: Filter by project ID.
            n_results: Maximum results.
            min_score: Minimum similarity score.

        Returns:
            RAGContext with search results.
        """
        where = None
        if project_id:
            where = {"project_id": project_id}

        result = self.vector_store.search(
            CollectionType.HIERARCHIES,
            query=query,
            n_results=n_results,
            where=where,
        )

        if not result.success:
            return RAGContext(query=query)

        # Filter by minimum score
        filtered_results = [
            r for r in result.data
            if r.get("score", 0) >= min_score
        ]

        # Build context text
        context_parts = []
        sources = []
        for r in filtered_results:
            context_parts.append(f"- {r.get('content', '')}")
            sources.append(r.get("metadata", {}).get("name", "Unknown"))

        return RAGContext(
            query=query,
            results=filtered_results,
            context_text="\n".join(context_parts),
            sources=sources,
            total_results=len(filtered_results),
        )

    def search_patterns(
        self,
        query: str,
        industry: Optional[str] = None,
        n_results: int = 5,
    ) -> RAGContext:
        """
        Search industry patterns.

        Args:
            query: Search query.
            industry: Filter by industry.
            n_results: Maximum results.

        Returns:
            RAGContext with search results.
        """
        where = None
        if industry:
            where = {"industry": industry}

        result = self.vector_store.search(
            CollectionType.INDUSTRY_PATTERNS,
            query=query,
            n_results=n_results,
            where=where,
        )

        if not result.success:
            return RAGContext(query=query)

        context_parts = []
        sources = []
        for r in result.data:
            context_parts.append(f"- {r.get('content', '')}")
            sources.append(f"{r.get('metadata', {}).get('industry', '')}: {r.get('metadata', {}).get('name', '')}")

        return RAGContext(
            query=query,
            results=result.data,
            context_text="\n".join(context_parts),
            sources=sources,
            total_results=len(result.data),
        )

    def search_concepts(
        self,
        query: str,
        category: Optional[str] = None,
        n_results: int = 5,
    ) -> RAGContext:
        """
        Search whitepaper concepts.

        Args:
            query: Search query.
            category: Filter by category.
            n_results: Maximum results.

        Returns:
            RAGContext with search results.
        """
        where = None
        if category:
            where = {"category": category}

        result = self.vector_store.search(
            CollectionType.WHITEPAPER_CONCEPTS,
            query=query,
            n_results=n_results,
            where=where,
        )

        if not result.success:
            return RAGContext(query=query)

        context_parts = []
        sources = []
        for r in result.data:
            context_parts.append(f"- {r.get('content', '')}")
            sources.append(r.get("metadata", {}).get("title", "Unknown"))

        return RAGContext(
            query=query,
            results=result.data,
            context_text="\n".join(context_parts),
            sources=sources,
            total_results=len(result.data),
        )

    def get_context(
        self,
        query: str,
        project_id: Optional[str] = None,
        include_hierarchies: bool = True,
        include_patterns: bool = True,
        include_concepts: bool = True,
        max_results: int = 10,
    ) -> RAGContext:
        """
        Get comprehensive context for a query.

        Searches across hierarchies, patterns, and concepts to build
        a rich context for LLM prompts.

        Args:
            query: Search query.
            project_id: Filter hierarchies by project.
            include_hierarchies: Search hierarchies.
            include_patterns: Search industry patterns.
            include_concepts: Search whitepaper concepts.
            max_results: Maximum total results.

        Returns:
            RAGContext with combined results.
        """
        all_results = []
        all_sources = []

        # Distribute results across sources
        per_source = max_results // (
            int(include_hierarchies) + int(include_patterns) + int(include_concepts)
        ) or max_results

        if include_hierarchies:
            hier_context = self.search_hierarchies(
                query, project_id=project_id, n_results=per_source
            )
            all_results.extend(hier_context.results)
            all_sources.extend([f"Hierarchy: {s}" for s in hier_context.sources])

        if include_patterns:
            pattern_context = self.search_patterns(query, n_results=per_source)
            all_results.extend(pattern_context.results)
            all_sources.extend([f"Pattern: {s}" for s in pattern_context.sources])

        if include_concepts:
            concept_context = self.search_concepts(query, n_results=per_source)
            all_results.extend(concept_context.results)
            all_sources.extend([f"Concept: {s}" for s in concept_context.sources])

        # Sort by score and limit
        all_results.sort(key=lambda x: x.get("score", 0), reverse=True)
        all_results = all_results[:max_results]

        # Build combined context
        context_parts = []
        for r in all_results:
            context_parts.append(f"- {r.get('content', '')}")

        return RAGContext(
            query=query,
            results=all_results,
            context_text="\n".join(context_parts),
            sources=all_sources[:max_results],
            total_results=len(all_results),
        )

    def format_for_prompt(
        self,
        context: RAGContext,
        max_tokens: int = 2000,
    ) -> str:
        """
        Format context for inclusion in an LLM prompt.

        Args:
            context: RAGContext to format.
            max_tokens: Approximate maximum tokens (characters / 4).

        Returns:
            Formatted context string.
        """
        max_chars = max_tokens * 4

        if not context.results:
            return "No relevant context found."

        lines = [
            "## Relevant Context",
            "",
            f"Query: {context.query}",
            "",
            "### Retrieved Information:",
            "",
        ]

        current_length = sum(len(line) for line in lines)

        for i, result in enumerate(context.results):
            content = result.get("content", "")
            score = result.get("score", 0)
            metadata = result.get("metadata", {})
            doc_type = metadata.get("type", "unknown")

            entry = f"{i+1}. [{doc_type}] (relevance: {score:.2f})\n   {content}\n"

            if current_length + len(entry) > max_chars:
                lines.append("... (additional results truncated)")
                break

            lines.append(entry)
            current_length += len(entry)

        lines.extend([
            "",
            f"Sources: {', '.join(context.sources[:5])}",
        ])

        return "\n".join(lines)
