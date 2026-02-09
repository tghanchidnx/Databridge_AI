"""GraphRAG Engine - Anti-hallucination layer for DataBridge AI Pro.

This module provides vector search and RAG capabilities for validating AI outputs.
"""
from typing import Any


def register_graphrag_tools(mcp_instance: Any) -> None:
    """Register GraphRAG tools with the MCP server."""
    try:
        from src.graphrag.mcp_tools import register_graphrag_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def rag_search(query: str, top_k: int = 5) -> str:
            """[Pro] Search for relevant context using GraphRAG.

            Args:
                query: Search query
                top_k: Number of results to return

            Returns:
                Search results with relevance scores
            """
            return '{"error": "GraphRAG tools require full Pro installation"}'

        @mcp_instance.tool()
        def rag_validate_output(content: str, sources: str = "") -> str:
            """[Pro] Validate AI-generated content against sources.

            Args:
                content: Content to validate
                sources: Source documents (JSON)

            Returns:
                Validation results
            """
            return '{"error": "GraphRAG tools require full Pro installation"}'


__all__ = ['register_graphrag_tools']
