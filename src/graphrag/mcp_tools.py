"""
GraphRAG MCP Tools - 10 tools for RAG-enhanced AI interactions.

Tools:
1. rag_configure - Configure RAG engine
2. rag_index_catalog - Index catalog assets
3. rag_index_hierarchies - Index hierarchy structures
4. rag_search - Hybrid retrieval search
5. rag_get_context - Get assembled context
6. rag_validate_output - Proof of Graph validation
7. rag_entity_extract - Extract entities from query
8. rag_explain_lineage - Natural language lineage explanation
9. rag_suggest_similar - Find similar entities
10. rag_get_stats - Get RAG engine statistics
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import (
    RAGConfig, RAGQuery, EmbeddingProvider, VectorStoreType,
)
from .embedding_provider import get_embedding_provider, EmbeddingCache
from .vector_store import get_vector_store
from .entity_extractor import EntityExtractor
from .proof_of_graph import ProofOfGraph
from .retriever import HybridRetriever

logger = logging.getLogger(__name__)

# Module-level state
_config: Optional[RAGConfig] = None
_embedder = None
_vector_store = None
_entity_extractor = None
_proof_of_graph = None
_retriever = None


def _get_catalog_store(settings):
    """Get catalog store if available."""
    try:
        from src.data_catalog.catalog_store import CatalogStore
        return CatalogStore(data_dir=str(Path(settings.data_dir) / "data_catalog"))
    except Exception:
        return None


def _get_hierarchy_service():
    """Get hierarchy service if available."""
    try:
        from src.hierarchy.service import HierarchyService
        return HierarchyService()
    except Exception:
        return None


def _get_lineage_tracker(settings):
    """Get lineage tracker if available."""
    try:
        from src.lineage.lineage_tracker import LineageTracker
        return LineageTracker(data_dir=str(Path(settings.data_dir) / "lineage"))
    except Exception:
        return None


def _get_template_service():
    """Get template service if available."""
    try:
        from src.templates.service import TemplateService
        return TemplateService()
    except Exception:
        return None


def _ensure_initialized(settings) -> bool:
    """Ensure RAG engine is initialized."""
    global _config, _embedder, _vector_store, _entity_extractor, _proof_of_graph, _retriever

    if _retriever is not None:
        return True

    # Use default config if not configured
    if _config is None:
        _config = RAGConfig(
            vector_store_path=str(Path(settings.data_dir) / "graphrag" / "vectors"),
        )

    try:
        # Initialize embedding provider
        cache_dir = str(Path(settings.data_dir) / "graphrag" / "embedding_cache")
        _embedder = get_embedding_provider(
            _config.embedding_provider,
            model=_config.embedding_model,
            cache_dir=cache_dir,
        )

        # Initialize vector store
        _vector_store = get_vector_store(
            _config.vector_store_type,
            db_path=_config.vector_store_path + ".db" if _config.vector_store_type == VectorStoreType.SQLITE else None,
            dimension=_config.embedding_dimension,
        )

        # Get optional services
        catalog = _get_catalog_store(settings)
        hierarchy = _get_hierarchy_service()
        lineage = _get_lineage_tracker(settings)
        templates = _get_template_service()

        # Initialize entity extractor
        _entity_extractor = EntityExtractor(
            catalog_store=catalog,
            hierarchy_service=hierarchy,
        )

        # Initialize proof of graph
        _proof_of_graph = ProofOfGraph(
            catalog_store=catalog,
            hierarchy_service=hierarchy,
            lineage_tracker=lineage,
            strict_mode=_config.strict_validation,
        )

        # Initialize retriever
        _retriever = HybridRetriever(
            embedding_provider=_embedder,
            vector_store=_vector_store,
            catalog_store=catalog,
            lineage_tracker=lineage,
            template_service=templates,
        )

        logger.info("GraphRAG engine initialized")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize GraphRAG: {e}")
        return False


def register_graphrag_tools(mcp, settings):
    """Register all GraphRAG MCP tools."""

    @mcp.tool()
    def rag_configure(
        embedding_provider: str = "openai",
        embedding_model: str = "text-embedding-3-small",
        vector_store: str = "sqlite",
        enable_proof_of_graph: bool = True,
        strict_validation: bool = False,
    ) -> Dict[str, Any]:
        """
        Configure the GraphRAG engine.

        Sets up embedding provider, vector store, and validation options.
        Call this before using other RAG tools.

        Args:
            embedding_provider: Provider for embeddings ("openai", "huggingface", "local")
            embedding_model: Model name for embeddings
            vector_store: Vector database type ("sqlite", "chroma")
            enable_proof_of_graph: Enable anti-hallucination validation
            strict_validation: Treat warnings as errors

        Returns:
            Configuration status

        Example:
            rag_configure(
                embedding_provider="openai",
                embedding_model="text-embedding-3-small",
                vector_store="sqlite"
            )
        """
        global _config, _embedder, _vector_store, _retriever

        try:
            # Map string to enum
            provider_map = {
                "openai": EmbeddingProvider.OPENAI,
                "huggingface": EmbeddingProvider.HUGGINGFACE,
                "local": EmbeddingProvider.LOCAL,
            }
            store_map = {
                "sqlite": VectorStoreType.SQLITE,
                "chroma": VectorStoreType.CHROMA,
            }

            _config = RAGConfig(
                embedding_provider=provider_map.get(embedding_provider, EmbeddingProvider.OPENAI),
                embedding_model=embedding_model,
                vector_store_type=store_map.get(vector_store, VectorStoreType.SQLITE),
                vector_store_path=str(Path(settings.data_dir) / "graphrag" / "vectors"),
                enable_proof_of_graph=enable_proof_of_graph,
                strict_validation=strict_validation,
            )

            # Reset components to reinitialize with new config
            _embedder = None
            _vector_store = None
            _retriever = None

            # Initialize
            if _ensure_initialized(settings):
                return {
                    "status": "configured",
                    "config": {
                        "embedding_provider": embedding_provider,
                        "embedding_model": embedding_model,
                        "vector_store": vector_store,
                        "proof_of_graph_enabled": enable_proof_of_graph,
                    },
                }
            else:
                return {"error": "Failed to initialize RAG engine"}

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_index_catalog(
        asset_types: str = "TABLE,VIEW",
        force_reindex: bool = False,
    ) -> Dict[str, Any]:
        """
        Index catalog assets into the vector store.

        Embeds asset names, descriptions, and column information
        for semantic search.

        Args:
            asset_types: Comma-separated asset types to index
            force_reindex: If True, reindex existing assets

        Returns:
            Indexing statistics

        Example:
            rag_index_catalog(asset_types="TABLE,VIEW,HIERARCHY")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized. Call rag_configure first."}

        try:
            catalog = _get_catalog_store(settings)
            if not catalog:
                return {"error": "Catalog store not available"}

            types = [t.strip() for t in asset_types.split(",")]
            assets = catalog.list_assets(asset_types=types)

            indexed = 0
            skipped = 0

            for asset in assets.get("assets", []):
                asset_id = f"catalog:{asset.get('id', '')}"

                # Check if already indexed
                if not force_reindex:
                    existing = _vector_store.get(asset_id)
                    if existing:
                        skipped += 1
                        continue

                # Build content for embedding
                name = asset.get("name", "")
                desc = asset.get("description", "")
                cols = ", ".join(c.get("name", "") for c in asset.get("columns", [])[:20])
                content = f"{name}: {desc}. Columns: {cols}"

                # Generate embedding and store
                embedding = _embedder.embed(content)
                success = _vector_store.upsert(
                    id=asset_id,
                    embedding=embedding,
                    content=content,
                    metadata={
                        "source_type": "catalog",
                        "asset_type": asset.get("type"),
                        "name": name,
                        **asset,
                    },
                )

                if success:
                    indexed += 1

            return {
                "status": "indexed",
                "indexed": indexed,
                "skipped": skipped,
                "total_in_store": _vector_store.count(),
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_index_hierarchies(
        project_id: Optional[str] = None,
        force_reindex: bool = False,
    ) -> Dict[str, Any]:
        """
        Index hierarchy structures into the vector store.

        Embeds hierarchy names, levels, and source mappings
        for semantic search.

        Args:
            project_id: Optional specific project to index (default: all)
            force_reindex: If True, reindex existing hierarchies

        Returns:
            Indexing statistics

        Example:
            rag_index_hierarchies(project_id="revenue-pl")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            hierarchy_service = _get_hierarchy_service()
            if not hierarchy_service:
                return {"error": "Hierarchy service not available"}

            indexed = 0
            skipped = 0

            projects = hierarchy_service.list_projects()
            if project_id:
                projects = [p for p in projects if p.get("id") == project_id]

            for proj in projects:
                proj_id = proj.get("id", "")

                # Index project
                proj_doc_id = f"hierarchy_project:{proj_id}"
                if not force_reindex and _vector_store.get(proj_doc_id):
                    skipped += 1
                else:
                    content = f"Hierarchy Project: {proj.get('name', '')}. {proj.get('description', '')}"
                    embedding = _embedder.embed(content)
                    _vector_store.upsert(
                        id=proj_doc_id,
                        embedding=embedding,
                        content=content,
                        metadata={
                            "source_type": "hierarchy_project",
                            "project_id": proj_id,
                            **proj,
                        },
                    )
                    indexed += 1

                # Index hierarchies in project
                hierarchies = hierarchy_service.list_hierarchies(proj_id)
                for hier in hierarchies:
                    hier_id = hier.get("hierarchy_id", hier.get("id", ""))
                    doc_id = f"hierarchy:{proj_id}:{hier_id}"

                    if not force_reindex and _vector_store.get(doc_id):
                        skipped += 1
                        continue

                    # Build rich content with correct field names
                    name = hier.get("hierarchy_name", "")
                    levels = hier.get("hierarchy_level", {}) or {}
                    level_parts = []
                    for i in range(1, 16):
                        val = levels.get(f"level_{i}")
                        if val:
                            level_parts.append(f"L{i}: {val}")
                    level_str = " > ".join(level_parts) if level_parts else ""

                    # Include mappings and properties for richer embeddings
                    mappings = hier.get("mapping", [])
                    mapping_strs = []
                    for m in mappings:
                        tbl = m.get("source_table", "")
                        col = m.get("source_column", "")
                        if tbl:
                            mapping_strs.append(f"{tbl}.{col}")

                    properties = hier.get("properties", [])
                    prop_strs = [f"{p.get('name')}={p.get('value')}" for p in properties]

                    formula_config = hier.get("formula_config", {}) or {}
                    formula_group = formula_config.get("formula_group", {}) or {}
                    formula_rules = formula_group.get("rules", [])
                    formula_strs = [f"{r.get('operation')} [{r.get('hierarchy_name', r.get('hierarchy_id', ''))}]" for r in formula_rules]

                    content_parts = [f"Hierarchy: {name}"]
                    if hier.get("description"):
                        content_parts.append(f"Description: {hier['description']}")
                    if level_str:
                        content_parts.append(f"Levels: {level_str}")
                    if mapping_strs:
                        content_parts.append(f"Source Mappings: {', '.join(mapping_strs)}")
                    if prop_strs:
                        content_parts.append(f"Properties: {', '.join(prop_strs)}")
                    if formula_strs:
                        content_parts.append(f"Formula: {' '.join(formula_strs)}")
                    content = ". ".join(content_parts)

                    level_depth = len(level_parts)

                    embedding = _embedder.embed(content)
                    _vector_store.upsert(
                        id=doc_id,
                        embedding=embedding,
                        content=content,
                        metadata={
                            "source_type": "hierarchy",
                            "project_id": proj_id,
                            "hierarchy_id": hier_id,
                            "name": name,
                            "parent_id": hier.get("parent_id"),
                            "is_root": hier.get("is_root", False),
                            "has_mappings": len(mappings) > 0,
                            "has_formula": len(formula_rules) > 0,
                            "property_count": len(properties),
                            "level_depth": level_depth,
                        },
                    )
                    indexed += 1

            return {
                "status": "indexed",
                "indexed": indexed,
                "skipped": skipped,
                "total_in_store": _vector_store.count(),
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_search(
        query: str,
        max_results: int = 10,
        include_lineage: bool = True,
        include_templates: bool = True,
    ) -> Dict[str, Any]:
        """
        Perform hybrid RAG search.

        Combines vector similarity, graph traversal, and keyword search
        using Reciprocal Rank Fusion.

        Args:
            query: Natural language query
            max_results: Maximum results to return
            include_lineage: Include lineage graph search
            include_templates: Include template matching

        Returns:
            Retrieved items with scores and sources

        Example:
            rag_search("What tables feed into the revenue hierarchy?")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            # Build RAG query
            rag_query = RAGQuery(
                query=query,
                max_results=max_results,
                include_lineage=include_lineage,
                include_templates=include_templates,
            )

            # Extract entities
            rag_query = _entity_extractor.enrich_query(rag_query)

            # Perform retrieval
            context = _retriever.retrieve(rag_query)

            return {
                "status": "success",
                "query": query,
                "entities_found": [
                    {"text": e.text, "type": e.entity_type.value, "confidence": e.confidence}
                    for e in rag_query.entities
                ],
                "results": [
                    {
                        "id": item.id,
                        "source": item.source.value,
                        "content": item.content[:200],
                        "score": round(item.score, 4),
                        "rank": item.rank,
                    }
                    for item in context.retrieved_items
                ],
                "retrieval_time_ms": context.retrieval_time_ms,
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_get_context(
        query: str,
        max_results: int = 10,
        format: str = "structured",
    ) -> Dict[str, Any]:
        """
        Get assembled RAG context for LLM prompting.

        Returns structured context including templates, skills,
        lineage paths, and available entities for validation.

        Args:
            query: Natural language query
            max_results: Maximum items to retrieve
            format: Output format ("structured", "prompt")

        Returns:
            Assembled context for LLM

        Example:
            rag_get_context("Build a P&L hierarchy for oil and gas")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            rag_query = RAGQuery(query=query, max_results=max_results)
            rag_query = _entity_extractor.enrich_query(rag_query)
            context = _retriever.retrieve(rag_query)

            if format == "prompt":
                return {
                    "status": "success",
                    "prompt_context": context.to_prompt_context(),
                    "available_tables": context.available_tables,
                    "available_hierarchies": context.available_hierarchies,
                }
            else:
                return {
                    "status": "success",
                    "domain": rag_query.domain,
                    "industry": rag_query.industry,
                    "templates": context.templates[:5],
                    "skills": context.skills[:3],
                    "lineage_paths": context.lineage_paths[:5],
                    "glossary_terms": context.glossary_terms[:5],
                    "catalog_assets": [
                        {"name": a.get("name"), "type": a.get("type")}
                        for a in context.catalog_assets[:10]
                    ],
                    "available_tables": context.available_tables[:20],
                    "retrieval_time_ms": context.retrieval_time_ms,
                }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_validate_output(
        content: str,
        content_type: str = "sql",
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate AI-generated content using Proof of Graph.

        Checks that referenced tables, columns, and hierarchies
        exist in the catalog/hierarchy system.

        Args:
            content: Generated content to validate (SQL, YAML, etc.)
            content_type: Type of content ("sql", "hierarchy", "dbt", "yaml")
            query: Optional original query for context

        Returns:
            Validation result with errors, warnings, and suggestions

        Example:
            rag_validate_output(
                content="SELECT * FROM REVENUE_FACT WHERE region = 'WEST'",
                content_type="sql"
            )
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            # Get context if query provided
            context = None
            if query:
                rag_query = RAGQuery(query=query, max_results=5)
                context = _retriever.retrieve(rag_query)

            # Validate
            result = _proof_of_graph.validate(
                content=content,
                content_type=content_type,
                context=context,
            )

            return {
                "valid": result.valid,
                "errors": result.errors,
                "warnings": result.warnings,
                "referenced_entities": result.referenced_entities,
                "verified_entities": result.verified_entities,
                "missing_entities": result.missing_entities,
                "suggestions": result.suggested_fixes,
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_entity_extract(query: str) -> Dict[str, Any]:
        """
        Extract entities from a natural language query.

        Identifies tables, columns, hierarchies, domains, industries,
        and business terms.

        Args:
            query: Natural language query to analyze

        Returns:
            List of extracted entities with types and confidence

        Example:
            rag_entity_extract("Create a P&L hierarchy for upstream oil and gas with LOE breakdown")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            entities = _entity_extractor.extract(query)

            return {
                "query": query,
                "entities": [
                    {
                        "text": e.text,
                        "type": e.entity_type.value,
                        "confidence": round(e.confidence, 2),
                        "linked_id": e.linked_id,
                    }
                    for e in entities
                ],
                "domain": next((e.text for e in entities if e.entity_type.value == "domain"), None),
                "industry": next((e.text for e in entities if e.entity_type.value == "industry"), None),
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_explain_lineage(
        entity_name: str,
        direction: str = "both",
        max_depth: int = 3,
    ) -> Dict[str, Any]:
        """
        Generate natural language explanation of data lineage.

        Describes upstream sources and downstream consumers
        of a data entity.

        Args:
            entity_name: Name of table, hierarchy, or column
            direction: "upstream", "downstream", or "both"
            max_depth: Maximum traversal depth

        Returns:
            Natural language lineage explanation

        Example:
            rag_explain_lineage("REVENUE_FACT", direction="upstream")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            lineage = _get_lineage_tracker(settings)
            if not lineage:
                return {"error": "Lineage tracker not available"}

            explanations = []

            # Search for matching nodes across all graphs
            for graph_name in lineage.list_graphs():
                graph = lineage.get_graph(graph_name)
                if not graph:
                    continue

                for node in graph.nodes:
                    if entity_name.lower() in node.name.lower():
                        # Get lineage
                        if direction in ("upstream", "both"):
                            upstream = lineage.get_all_upstream(graph_name, node.id)
                            if upstream:
                                path = " → ".join(n.name for n in upstream[:max_depth])
                                explanations.append(f"Upstream: {path} → {node.name}")

                        if direction in ("downstream", "both"):
                            downstream = lineage.get_all_downstream(graph_name, node.id)
                            if downstream:
                                path = " → ".join(n.name for n in downstream[:max_depth])
                                explanations.append(f"Downstream: {node.name} → {path}")

            if not explanations:
                return {
                    "entity": entity_name,
                    "found": False,
                    "explanation": f"No lineage found for '{entity_name}'",
                }

            return {
                "entity": entity_name,
                "found": True,
                "direction": direction,
                "explanations": explanations,
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_suggest_similar(
        text: str,
        entity_type: str = "all",
        top_k: int = 5,
    ) -> Dict[str, Any]:
        """
        Find similar entities using semantic search.

        Useful for finding related tables, hierarchies, or terms.

        Args:
            text: Text to find similar entities for
            entity_type: Filter by type ("table", "hierarchy", "all")
            top_k: Number of suggestions to return

        Returns:
            List of similar entities with similarity scores

        Example:
            rag_suggest_similar("customer revenue", entity_type="table")
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            # Generate embedding for query text
            embedding = _embedder.embed(text)

            # Build filter
            filter_dict = None
            if entity_type != "all":
                filter_dict = {"source_type": entity_type}

            # Search
            results = _vector_store.search(
                query_embedding=embedding,
                top_k=top_k,
                filter=filter_dict,
            )

            return {
                "query": text,
                "suggestions": [
                    {
                        "name": meta.get("name", id),
                        "type": meta.get("source_type", "unknown"),
                        "similarity": round(score, 4),
                        "content": content[:100] if content else "",
                    }
                    for id, score, content, meta in results
                ],
            }

        except Exception as e:
            return {"error": str(e)}

    @mcp.tool()
    def rag_get_stats() -> Dict[str, Any]:
        """
        Get GraphRAG engine statistics.

        Returns indexing stats, cache stats, and configuration.

        Returns:
            Engine statistics and health

        Example:
            rag_get_stats()
        """
        if not _ensure_initialized(settings):
            return {"error": "RAG engine not initialized"}

        try:
            # Vector store stats
            vs_stats = _vector_store.stats()

            # Embedding cache stats
            if hasattr(_embedder, 'cache'):
                cache_stats = _embedder.cache.stats()
            else:
                cache_stats = {}

            return {
                "status": "healthy",
                "config": {
                    "embedding_provider": _config.embedding_provider.value if _config else "unknown",
                    "embedding_model": _config.embedding_model if _config else "unknown",
                    "vector_store": _config.vector_store_type.value if _config else "unknown",
                    "proof_of_graph_enabled": _config.enable_proof_of_graph if _config else False,
                },
                "index": {
                    "total_documents": vs_stats.total_documents,
                    "by_source": vs_stats.by_source,
                    "last_indexed": vs_stats.last_indexed.isoformat() if vs_stats.last_indexed else None,
                },
                "cache": cache_stats,
            }

        except Exception as e:
            return {"error": str(e)}

    logger.info("Registered 10 GraphRAG MCP tools")
    return {
        "tools_registered": 10,
        "categories": {
            "configuration": ["rag_configure"],
            "indexing": ["rag_index_catalog", "rag_index_hierarchies"],
            "retrieval": ["rag_search", "rag_get_context", "rag_suggest_similar"],
            "validation": ["rag_validate_output"],
            "extraction": ["rag_entity_extract", "rag_explain_lineage"],
            "monitoring": ["rag_get_stats"],
        },
    }
