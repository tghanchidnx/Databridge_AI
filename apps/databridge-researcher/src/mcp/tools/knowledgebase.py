"""
MCP Tools for Knowledge Base operations in DataBridge Analytics Researcher.

Provides tools for business glossary, metric definitions, and FP&A concepts.
"""

from typing import Optional, List, Dict, Any

from ...knowledgebase import (
    KnowledgeBaseStore,
    KBCollectionType,
    KBDocument,
    GlossaryLoader,
    get_glossary_terms,
    get_metric_definitions,
    get_fpa_concepts,
)
from ...core.config import get_settings


# Cached instances
_kb_store: Optional[KnowledgeBaseStore] = None


def _get_kb_store() -> KnowledgeBaseStore:
    """Get or create the knowledge base store instance."""
    global _kb_store
    if _kb_store is None:
        settings = get_settings()
        _kb_store = KnowledgeBaseStore(
            persist_directory=str(settings.vector.db_path)
        )
    return _kb_store


def register_knowledgebase_tools(mcp) -> None:
    """Register knowledge base MCP tools with the FastMCP server."""

    # ==================== Glossary Operations ====================

    @mcp.tool()
    def search_business_glossary(
        query: str,
        category: Optional[str] = None,
        n_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Search business glossary terms semantically.

        Finds accounting, finance, and FP&A terminology that matches
        your query using semantic similarity.

        Categories: accounting, finance, fpa, analytics

        Args:
            query: Search query (e.g., "revenue recognition")
            category: Optional category filter
            n_results: Number of results (default: 5)

        Returns:
            Dictionary with matching glossary terms including:
            - term: The glossary term
            - definition: Full definition
            - category: Term category
            - related_terms: Related terminology
        """
        store = _get_kb_store()
        result = store.search(
            KBCollectionType.BUSINESS_GLOSSARY,
            query=query,
            n_results=n_results,
            category=category,
        )
        return result.to_dict()

    @mcp.tool()
    def search_metric_definitions(
        query: str,
        category: Optional[str] = None,
        n_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Search metric definitions semantically.

        Finds financial and operational metrics that match your query.
        Includes formulas, units, and related metrics.

        Categories: revenue, profitability, efficiency, liquidity, growth

        Args:
            query: Search query (e.g., "profit margin calculation")
            category: Optional category filter
            n_results: Number of results (default: 5)

        Returns:
            Dictionary with matching metrics including:
            - name: Metric name
            - definition: What the metric measures
            - formula: Calculation formula
            - unit: Measurement unit (currency, percentage, ratio, days)
            - related_metrics: Related metrics
        """
        store = _get_kb_store()
        result = store.search(
            KBCollectionType.METRIC_DEFINITIONS,
            query=query,
            n_results=n_results,
            category=category,
        )
        return result.to_dict()

    @mcp.tool()
    def search_fpa_concepts(
        query: str,
        category: Optional[str] = None,
        n_results: int = 5,
    ) -> Dict[str, Any]:
        """
        Search FP&A concepts and methodologies.

        Finds financial planning and analysis concepts including
        budgeting methods, variance analysis techniques, and modeling approaches.

        Categories: budgeting, planning, modeling, variance, profitability

        Args:
            query: Search query (e.g., "zero based budgeting")
            category: Optional category filter
            n_results: Number of results (default: 5)

        Returns:
            Dictionary with matching concepts including:
            - concept: Concept name
            - description: Detailed explanation
            - category: Concept category
            - use_cases: Practical applications
        """
        store = _get_kb_store()
        result = store.search(
            KBCollectionType.FPA_CONCEPTS,
            query=query,
            n_results=n_results,
            category=category,
        )
        return result.to_dict()

    # ==================== Knowledge Base Management ====================

    @mcp.tool()
    def load_knowledge_base() -> Dict[str, Any]:
        """
        Load all knowledge base content into the vector store.

        Indexes business glossary terms, metric definitions, and FP&A
        concepts for semantic search. Run this once to enable search.

        Returns:
            Dictionary with loading summary:
            - success: Whether loading succeeded
            - total_loaded: Total documents indexed
            - details: Breakdown by content type
        """
        store = _get_kb_store()
        loader = GlossaryLoader(store)
        return loader.load_all()

    @mcp.tool()
    def get_knowledge_base_stats() -> Dict[str, Any]:
        """
        Get statistics for all knowledge base collections.

        Shows document counts for glossary, metrics, and concepts.

        Returns:
            Dictionary with collection statistics
        """
        store = _get_kb_store()

        collections = [
            KBCollectionType.BUSINESS_GLOSSARY,
            KBCollectionType.METRIC_DEFINITIONS,
            KBCollectionType.FPA_CONCEPTS,
        ]

        stats = {}
        for collection_type in collections:
            result = store.get_collection_stats(collection_type)
            if result.success:
                stats[collection_type.value] = result.data

        return {
            "success": True,
            "collections": stats,
            "total_collections": len(stats),
        }

    @mcp.tool()
    def list_glossary_terms(
        category: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List all available glossary terms.

        Returns the full list of business glossary terms without
        requiring a search query. Useful for browsing available terms.

        Categories: accounting, finance, fpa, analytics

        Args:
            category: Optional category filter

        Returns:
            Dictionary with all glossary terms
        """
        terms = get_glossary_terms()

        if category:
            terms = [t for t in terms if t.get("category") == category]

        # Summarize for display
        summaries = []
        for term in terms:
            summaries.append({
                "term": term["term"],
                "category": term["category"],
                "definition": term["definition"][:150] + "..." if len(term["definition"]) > 150 else term["definition"],
                "related_terms": term.get("related_terms", []),
            })

        return {
            "success": True,
            "terms": summaries,
            "total": len(summaries),
            "filter_category": category,
        }

    @mcp.tool()
    def list_metric_definitions(
        category: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List all available metric definitions.

        Returns the full list of metric definitions without requiring
        a search query. Includes formulas and measurement units.

        Categories: revenue, profitability, efficiency, liquidity, growth

        Args:
            category: Optional category filter

        Returns:
            Dictionary with all metric definitions
        """
        metrics = get_metric_definitions()

        if category:
            metrics = [m for m in metrics if m.get("category") == category]

        # Summarize for display
        summaries = []
        for metric in metrics:
            summaries.append({
                "name": metric["name"],
                "category": metric["category"],
                "definition": metric["definition"],
                "formula": metric["formula"],
                "unit": metric["unit"],
                "related_metrics": metric.get("related_metrics", []),
            })

        return {
            "success": True,
            "metrics": summaries,
            "total": len(summaries),
            "filter_category": category,
        }
