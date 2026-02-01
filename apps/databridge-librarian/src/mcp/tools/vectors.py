"""
MCP Tools for Vector Store operations in DataBridge AI V3.

Provides tools for semantic search, embeddings, and RAG operations.
"""

from typing import Optional, List, Dict, Any

from ...vectors import (
    VectorStore,
    HierarchyRAG,
    CollectionType,
    Document,
    get_all_patterns,
    get_patterns_by_industry,
    get_industries,
    IndustryPatternLoader,
)
from ...core.config import get_settings


# Cached instances
_vector_store: Optional[VectorStore] = None
_rag_pipeline: Optional[HierarchyRAG] = None


def _get_vector_store() -> VectorStore:
    """Get or create the vector store instance."""
    global _vector_store
    if _vector_store is None:
        settings = get_settings()
        _vector_store = VectorStore(
            persist_directory=str(settings.vector.db_path)
        )
    return _vector_store


def _get_rag_pipeline() -> HierarchyRAG:
    """Get or create the RAG pipeline instance."""
    global _rag_pipeline
    if _rag_pipeline is None:
        settings = get_settings()
        _rag_pipeline = HierarchyRAG(
            persist_directory=str(settings.vector.db_path)
        )
    return _rag_pipeline


def register_vector_tools(mcp) -> None:
    """Register vector MCP tools with the FastMCP server."""

    # ==================== Collection Management ====================

    @mcp.tool()
    def list_vector_collections() -> Dict[str, Any]:
        """
        List all vector collections in the store.

        Returns a list of collections with their names, document counts,
        and metadata. Collections are prefixed with 'databridge_v3_'.

        Returns:
            Dictionary with:
            - success: Boolean indicating operation success
            - collections: List of collection info objects
            - message: Status message
        """
        store = _get_vector_store()
        result = store.list_collections()
        return result.to_dict()

    @mcp.tool()
    def create_vector_collection(
        collection_name: str,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new vector collection.

        Collections are used to organize different types of embeddings:
        - hierarchies: Hierarchy nodes and structures
        - mappings: Source mappings
        - industry_patterns: Pre-built industry templates
        - whitepaper_concepts: FP&A concepts and definitions
        - client_knowledge: Client-specific knowledge

        Args:
            collection_name: Name of the collection to create
            description: Optional description

        Returns:
            Dictionary with operation status
        """
        store = _get_vector_store()
        metadata = {"description": description} if description else None
        result = store.create_collection(collection_name, metadata=metadata)
        return result.to_dict()

    @mcp.tool()
    def delete_vector_collection(
        collection_name: str,
    ) -> Dict[str, Any]:
        """
        Delete a vector collection and all its documents.

        Warning: This permanently deletes all embeddings in the collection.

        Args:
            collection_name: Name of the collection to delete

        Returns:
            Dictionary with operation status
        """
        store = _get_vector_store()
        result = store.delete_collection(collection_name)
        return result.to_dict()

    @mcp.tool()
    def get_collection_stats(
        collection_name: str,
    ) -> Dict[str, Any]:
        """
        Get statistics for a vector collection.

        Args:
            collection_name: Name of the collection

        Returns:
            Dictionary with collection stats including document count
        """
        store = _get_vector_store()
        result = store.get_collection_stats(collection_name)
        return result.to_dict()

    # ==================== Document Operations ====================

    @mcp.tool()
    def add_to_vector_store(
        collection_name: str,
        content: str,
        document_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Add a document to a vector collection.

        The document will be automatically embedded using sentence-transformers.

        Args:
            collection_name: Collection to add to
            content: Text content to embed and store
            document_id: Optional unique ID (auto-generated if not provided)
            metadata: Optional metadata dictionary

        Returns:
            Dictionary with operation status
        """
        store = _get_vector_store()
        doc = Document(
            id=document_id or "",
            content=content,
            metadata=metadata or {},
        )
        result = store.upsert(collection_name, [doc])
        return result.to_dict()

    @mcp.tool()
    def semantic_search(
        collection_name: str,
        query: str,
        n_results: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Search for similar documents using semantic similarity.

        Uses vector embeddings to find documents that are semantically
        similar to the query, even if they don't share exact keywords.

        Args:
            collection_name: Collection to search
            query: Search query text
            n_results: Number of results to return (default: 5)
            filter_metadata: Optional metadata filter (e.g., {"type": "hierarchy"})

        Returns:
            Dictionary with search results including:
            - id: Document ID
            - content: Document text
            - score: Similarity score (0-1)
            - metadata: Document metadata
        """
        store = _get_vector_store()
        result = store.search(
            collection_name,
            query=query,
            n_results=n_results,
            where=filter_metadata,
        )
        return result.to_dict()

    @mcp.tool()
    def get_documents_by_id(
        collection_name: str,
        document_ids: List[str],
    ) -> Dict[str, Any]:
        """
        Retrieve documents by their IDs.

        Args:
            collection_name: Collection to retrieve from
            document_ids: List of document IDs

        Returns:
            Dictionary with documents
        """
        store = _get_vector_store()
        result = store.get_by_id(collection_name, document_ids)
        return result.to_dict()

    @mcp.tool()
    def delete_documents(
        collection_name: str,
        document_ids: Optional[List[str]] = None,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Delete documents from a collection.

        Either document_ids or filter_metadata must be provided.

        Args:
            collection_name: Collection to delete from
            document_ids: Specific document IDs to delete
            filter_metadata: Metadata filter for deletion

        Returns:
            Dictionary with operation status
        """
        store = _get_vector_store()
        result = store.delete_documents(
            collection_name,
            ids=document_ids,
            where=filter_metadata,
        )
        return result.to_dict()

    # ==================== RAG Operations ====================

    @mcp.tool()
    def index_hierarchy_for_search(
        hierarchy_id: str,
        hierarchy_name: str,
        description: Optional[str] = None,
        levels: Optional[List[str]] = None,
        formula_group: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Index a hierarchy for semantic search.

        Creates an embedding of the hierarchy that can be searched
        semantically. Useful for finding similar hierarchies or
        understanding hierarchy relationships.

        Args:
            hierarchy_id: Unique identifier
            hierarchy_name: Display name
            description: Optional description
            levels: List of level values (level_1 through level_10)
            formula_group: Optional formula group name
            project_id: Optional project ID for filtering

        Returns:
            Dictionary with indexing status
        """
        rag = _get_rag_pipeline()

        hierarchy = {
            "hierarchy_id": hierarchy_id,
            "hierarchy_name": hierarchy_name,
            "description": description or "",
            "formula_group": formula_group or "",
        }

        # Add levels
        if levels:
            for i, level in enumerate(levels[:10], 1):
                hierarchy[f"level_{i}"] = level

        result = rag.index_hierarchy(hierarchy, project_id=project_id)
        return result.to_dict()

    @mcp.tool()
    def search_hierarchies_semantic(
        query: str,
        project_id: Optional[str] = None,
        n_results: int = 10,
        min_score: float = 0.5,
    ) -> Dict[str, Any]:
        """
        Search hierarchies using semantic similarity.

        Finds hierarchies that are conceptually related to the query,
        even if they don't contain the exact search terms.

        Args:
            query: Natural language search query
            project_id: Optional project filter
            n_results: Maximum results (default: 10)
            min_score: Minimum similarity score (default: 0.5)

        Returns:
            Dictionary with search results
        """
        rag = _get_rag_pipeline()
        context = rag.search_hierarchies(
            query,
            project_id=project_id,
            n_results=n_results,
            min_score=min_score,
        )
        return context.to_dict()

    @mcp.tool()
    def get_rag_context(
        query: str,
        project_id: Optional[str] = None,
        include_hierarchies: bool = True,
        include_patterns: bool = True,
        include_concepts: bool = True,
        max_results: int = 10,
    ) -> Dict[str, Any]:
        """
        Get comprehensive RAG context for a query.

        Retrieves relevant context from hierarchies, industry patterns,
        and whitepaper concepts to provide rich context for AI responses.

        Args:
            query: Natural language query
            project_id: Optional project filter for hierarchies
            include_hierarchies: Include hierarchy search (default: True)
            include_patterns: Include industry patterns (default: True)
            include_concepts: Include whitepaper concepts (default: True)
            max_results: Maximum total results (default: 10)

        Returns:
            Dictionary with combined context including:
            - results: List of relevant items
            - context_text: Formatted text for prompts
            - sources: List of source references
        """
        rag = _get_rag_pipeline()
        context = rag.get_context(
            query,
            project_id=project_id,
            include_hierarchies=include_hierarchies,
            include_patterns=include_patterns,
            include_concepts=include_concepts,
            max_results=max_results,
        )
        return context.to_dict()

    @mcp.tool()
    def format_context_for_prompt(
        query: str,
        project_id: Optional[str] = None,
        max_tokens: int = 2000,
    ) -> Dict[str, Any]:
        """
        Get formatted RAG context ready for LLM prompts.

        Retrieves and formats context in a structured way that can be
        directly included in AI prompts for enhanced responses.

        Args:
            query: Natural language query
            project_id: Optional project filter
            max_tokens: Approximate max tokens (default: 2000)

        Returns:
            Dictionary with:
            - context_text: Formatted context string
            - total_results: Number of sources used
        """
        rag = _get_rag_pipeline()
        context = rag.get_context(query, project_id=project_id)
        formatted = rag.format_for_prompt(context, max_tokens=max_tokens)

        return {
            "success": True,
            "context_text": formatted,
            "total_results": context.total_results,
            "sources": context.sources[:5],
        }

    # ==================== Industry Patterns ====================

    @mcp.tool()
    def list_industries() -> Dict[str, Any]:
        """
        List all available industries with pattern counts.

        Returns industries that have pre-built hierarchy patterns
        including Oil & Gas, Manufacturing, Healthcare, Private Equity,
        Retail, and Construction.

        Returns:
            Dictionary with industry list and metadata
        """
        industries = get_industries()
        return {
            "success": True,
            "industries": industries,
            "total": len(industries),
        }

    @mcp.tool()
    def list_industry_patterns(
        industry: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List industry patterns, optionally filtered by industry.

        Industry patterns are pre-built hierarchy templates that
        represent best practices for specific industries.

        Args:
            industry: Optional industry filter (e.g., "oil_gas", "manufacturing")

        Returns:
            Dictionary with pattern list
        """
        if industry:
            patterns = get_patterns_by_industry(industry)
        else:
            patterns = get_all_patterns()

        # Summarize patterns for display
        summaries = []
        for p in patterns:
            summaries.append({
                "id": p["id"],
                "industry": p["industry"],
                "name": p["name"],
                "type": p["hierarchy_type"],
                "description": p["description"][:200] + "..." if len(p["description"]) > 200 else p["description"],
            })

        return {
            "success": True,
            "patterns": summaries,
            "total": len(summaries),
            "filter_industry": industry,
        }

    @mcp.tool()
    def search_industry_patterns(
        query: str,
        industry: Optional[str] = None,
        n_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Search industry patterns semantically.

        Finds industry patterns that match your requirements based
        on semantic similarity, not just keyword matching.

        Args:
            query: Search query (e.g., "oil and gas production costs")
            industry: Optional industry filter
            n_results: Number of results (default: 5)

        Returns:
            Dictionary with matching patterns
        """
        rag = _get_rag_pipeline()
        context = rag.search_patterns(query, industry=industry, n_results=n_results)
        return context.to_dict()

    @mcp.tool()
    def load_industry_patterns(
        industry: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Load industry patterns into the vector store.

        This indexes all patterns for semantic search. Run this once
        to enable pattern search functionality.

        Args:
            industry: Optional - load only patterns for specific industry

        Returns:
            Dictionary with loading summary
        """
        rag = _get_rag_pipeline()
        loader = IndustryPatternLoader(rag)

        if industry:
            result = loader.load_patterns_by_industry(industry)
        else:
            result = loader.load_all_patterns()

        return {
            "success": True,
            "message": "Industry patterns loaded",
            "summary": result,
        }

    # ==================== Concept Operations ====================

    @mcp.tool()
    def index_concept(
        title: str,
        definition: str,
        category: Optional[str] = None,
        examples: Optional[List[str]] = None,
        related: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Index a concept for semantic search.

        Concepts can be FP&A terms, accounting definitions, or
        industry-specific terminology.

        Args:
            title: Concept title
            definition: Concept definition
            category: Optional category (e.g., "accounting", "finance")
            examples: Optional list of examples
            related: Optional list of related concepts

        Returns:
            Dictionary with indexing status
        """
        rag = _get_rag_pipeline()

        concept = {
            "title": title,
            "definition": definition,
            "category": category or "general",
            "examples": examples or [],
            "related": related or [],
        }

        result = rag.index_concept(concept)
        return result.to_dict()

    @mcp.tool()
    def search_concepts(
        query: str,
        category: Optional[str] = None,
        n_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Search concepts semantically.

        Finds concepts that match your query based on semantic
        similarity. Useful for finding definitions and explanations.

        Args:
            query: Search query
            category: Optional category filter
            n_results: Number of results (default: 5)

        Returns:
            Dictionary with matching concepts
        """
        rag = _get_rag_pipeline()
        context = rag.search_concepts(query, category=category, n_results=n_results)
        return context.to_dict()
