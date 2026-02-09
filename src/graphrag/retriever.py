"""
Hybrid Retriever - Combines multiple retrieval strategies.

Implements Reciprocal Rank Fusion (RRF) for combining:
1. Vector search (semantic similarity)
2. Graph search (lineage traversal)
3. Lexical search (catalog keyword search)
4. Template matching (skill/template relevance)

This hybrid approach ensures we find relevant context regardless
of whether the match is semantic, structural, or keyword-based.
"""
import logging
from typing import Dict, List, Optional, Any

from .types import (
    RAGQuery, RAGContext, RetrievedItem, RetrievalSource,
    EntityType,
)
from .embedding_provider import BaseEmbeddingProvider
from .vector_store import BaseVectorStore

logger = logging.getLogger(__name__)


class HybridRetriever:
    """
    Hybrid retrieval combining vector, graph, and lexical search.

    Uses Reciprocal Rank Fusion (RRF) to combine results from
    multiple retrieval sources into a unified ranking.
    """

    def __init__(
        self,
        embedding_provider: BaseEmbeddingProvider,
        vector_store: BaseVectorStore,
        catalog_store=None,
        lineage_tracker=None,
        template_service=None,
        knowledge_service=None,
        skill_service=None,
    ):
        """
        Initialize the hybrid retriever.

        Args:
            embedding_provider: Provider for generating query embeddings
            vector_store: Vector database for similarity search
            catalog_store: CatalogStore for lexical search
            lineage_tracker: LineageTracker for graph traversal
            template_service: TemplateService for template matching
            knowledge_service: KnowledgeService for client knowledge
            skill_service: SkillService for skill matching
        """
        self.embedder = embedding_provider
        self.vector_store = vector_store
        self.catalog = catalog_store
        self.lineage = lineage_tracker
        self.templates = template_service
        self.knowledge = knowledge_service
        self.skills = skill_service

    def retrieve(self, query: RAGQuery) -> RAGContext:
        """
        Perform hybrid retrieval for a query.

        Args:
            query: RAGQuery with query text, entities, and options

        Returns:
            RAGContext with retrieved items and structured context
        """
        import time
        start = time.time()

        all_results: Dict[str, Dict[str, Any]] = {}

        # 1. Vector Search
        vector_results = self._vector_search(query)
        for item in vector_results:
            if item.id not in all_results:
                all_results[item.id] = {"item": item, "scores": {}}
            all_results[item.id]["scores"]["vector"] = item.score

        # 2. Graph Search (Lineage)
        if query.include_lineage and self.lineage:
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
        if query.include_templates and self.templates:
            template_results = self._template_search(query)
            for item in template_results:
                if item.id not in all_results:
                    all_results[item.id] = {"item": item, "scores": {}}
                all_results[item.id]["scores"]["template"] = item.score

        # Fuse results using RRF
        fused_results = self._reciprocal_rank_fusion(
            all_results,
            weights={
                "vector": query.vector_weight,
                "graph": query.graph_weight,
                "lexical": query.lexical_weight,
                "template": query.template_weight,
            },
        )

        # Limit results
        final_results = fused_results[:query.max_results]

        # Build structured context
        context = self._build_context(query, final_results)

        # Add timing
        context.retrieval_time_ms = int((time.time() - start) * 1000)

        return context

    def _vector_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Perform vector similarity search."""
        try:
            # Generate query embedding
            query_embedding = self.embedder.embed(query.query)

            # Search vector store
            results = self.vector_store.search(
                query_embedding=query_embedding,
                top_k=query.max_results * 2,  # Over-fetch for fusion
                threshold=0.3,  # Minimum similarity
            )

            items = []
            for id, score, content, metadata in results:
                items.append(RetrievedItem(
                    id=id,
                    source=RetrievalSource.VECTOR,
                    content=content,
                    score=score,
                    metadata=metadata,
                ))

            logger.debug(f"Vector search returned {len(items)} results")
            return items

        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []

    def _graph_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search by traversing lineage graph."""
        results = []

        try:
            # Find entities that match lineage nodes
            for entity in query.entities:
                if entity.entity_type in (EntityType.TABLE, EntityType.HIERARCHY, EntityType.COLUMN):
                    # Search for matching nodes in lineage
                    for graph_name in self.lineage.list_graphs():
                        graph = self.lineage.get_graph(graph_name)
                        if not graph:
                            continue

                        for node in graph.nodes:
                            # Check if entity text matches node
                            if entity.text.lower() in node.name.lower():
                                # Get upstream lineage
                                try:
                                    upstream = self.lineage.get_all_upstream(graph_name, node.id)
                                    for i, up_node in enumerate(upstream[:5]):
                                        score = 1.0 / (i + 2)  # Decay by distance
                                        results.append(RetrievedItem(
                                            id=f"lineage:{graph_name}:{up_node.id}:up",
                                            source=RetrievalSource.GRAPH,
                                            content=f"Upstream: {up_node.name} ({up_node.node_type})",
                                            score=score,
                                            metadata={
                                                "graph": graph_name,
                                                "node_id": up_node.id,
                                                "node_type": up_node.node_type,
                                                "direction": "upstream",
                                                "distance": i + 1,
                                            },
                                        ))
                                except Exception:
                                    pass

                                # Get downstream lineage
                                try:
                                    downstream = self.lineage.get_all_downstream(graph_name, node.id)
                                    for i, down_node in enumerate(downstream[:5]):
                                        score = 1.0 / (i + 2)
                                        results.append(RetrievedItem(
                                            id=f"lineage:{graph_name}:{down_node.id}:down",
                                            source=RetrievalSource.GRAPH,
                                            content=f"Downstream: {down_node.name} ({down_node.node_type})",
                                            score=score,
                                            metadata={
                                                "graph": graph_name,
                                                "node_id": down_node.id,
                                                "node_type": down_node.node_type,
                                                "direction": "downstream",
                                                "distance": i + 1,
                                            },
                                        ))
                                except Exception:
                                    pass

            logger.debug(f"Graph search returned {len(results)} results")

        except Exception as e:
            logger.debug(f"Graph search error: {e}")

        return results

    def _lexical_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search catalog using keyword/lexical search."""
        try:
            search_results = self.catalog.search(
                query=query.query,
                limit=query.max_results * 2,
            )

            items = []
            for r in search_results.get("results", []):
                items.append(RetrievedItem(
                    id=f"catalog:{r.get('id', '')}",
                    source=RetrievalSource.LEXICAL,
                    content=f"{r.get('name', '')}: {r.get('description', '')}",
                    score=r.get("relevance_score", 0.5),
                    metadata=r,
                ))

            logger.debug(f"Lexical search returned {len(items)} results")
            return items

        except Exception as e:
            logger.debug(f"Lexical search error: {e}")
            return []

    def _template_search(self, query: RAGQuery) -> List[RetrievedItem]:
        """Search for matching templates and skills."""
        results = []

        try:
            # Search templates
            if self.templates:
                templates = self.templates.list_templates(
                    domain=query.domain,
                    industry=query.industry,
                )

                for t in templates.get("templates", [])[:5]:
                    # Score based on keyword match
                    score = self._calculate_template_score(query, t)
                    results.append(RetrievedItem(
                        id=f"template:{t.get('id', '')}",
                        source=RetrievalSource.TEMPLATE,
                        content=f"Template: {t.get('name', '')} - {t.get('description', '')}",
                        score=score,
                        metadata=t,
                    ))

            # Search skills
            if self.skills and query.include_skills:
                skills = self.skills.list_skills(
                    domain=query.domain,
                    industry=query.industry,
                )

                for s in skills.get("skills", [])[:3]:
                    score = self._calculate_skill_score(query, s)
                    results.append(RetrievedItem(
                        id=f"skill:{s.get('id', '')}",
                        source=RetrievalSource.SKILL,
                        content=f"Skill: {s.get('name', '')} - {s.get('description', '')}",
                        score=score,
                        metadata=s,
                    ))

            logger.debug(f"Template search returned {len(results)} results")

        except Exception as e:
            logger.debug(f"Template search error: {e}")

        return results

    def _calculate_template_score(self, query: RAGQuery, template: Dict) -> float:
        """Calculate relevance score for a template."""
        score = 0.3  # Base score

        query_lower = query.query.lower()
        name = template.get("name", "").lower()
        description = template.get("description", "").lower()
        domain = template.get("domain", "").lower()
        industry = template.get("industry", "").lower()

        # Name match
        if name in query_lower:
            score += 0.4
        elif any(word in query_lower for word in name.split("_")):
            score += 0.2

        # Domain match
        if query.domain and domain == query.domain.lower():
            score += 0.2

        # Industry match
        if query.industry and industry == query.industry.lower():
            score += 0.2

        # Description keyword match
        query_words = set(query_lower.split())
        desc_words = set(description.split())
        overlap = len(query_words & desc_words)
        if overlap > 0:
            score += min(0.1 * overlap, 0.2)

        return min(score, 1.0)

    def _calculate_skill_score(self, query: RAGQuery, skill: Dict) -> float:
        """Calculate relevance score for a skill."""
        score = 0.3

        query_lower = query.query.lower()
        name = skill.get("name", "").lower()
        capabilities = skill.get("capabilities", [])

        # Name match
        if name in query_lower:
            score += 0.4

        # Capability match
        for cap in capabilities:
            if cap.lower() in query_lower:
                score += 0.15

        # Domain/industry match
        if query.domain and skill.get("domain", "").lower() == query.domain.lower():
            score += 0.15
        if query.industry and query.industry.lower() in str(skill.get("industries", [])).lower():
            score += 0.15

        return min(score, 1.0)

    def _reciprocal_rank_fusion(
        self,
        results: Dict[str, Dict[str, Any]],
        weights: Dict[str, float],
        k: int = 60,
    ) -> List[RetrievedItem]:
        """
        Combine results using Reciprocal Rank Fusion.

        RRF score = sum(weight_i / (k + rank_i)) for each source

        Args:
            results: Dict of {id: {item, scores}} from all sources
            weights: Dict of {source: weight}
            k: RRF constant (default 60)

        Returns:
            List of items sorted by fused score
        """
        # Sort each source by score to get ranks
        source_ranks: Dict[str, Dict[str, int]] = {}

        for source in weights.keys():
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

        # Sort by fused score
        sorted_items = sorted(fused_scores.items(), key=lambda x: x[1], reverse=True)

        # Build final list with ranks
        final_items = []
        for rank, (id, score) in enumerate(sorted_items):
            item = results[id]["item"]
            item.score = score
            item.rank = rank + 1
            final_items.append(item)

        return final_items

    def _build_context(
        self,
        query: RAGQuery,
        items: List[RetrievedItem],
    ) -> RAGContext:
        """Build structured RAG context from retrieved items."""
        context = RAGContext(query=query, retrieved_items=items)

        # Organize by source type
        for item in items:
            meta = item.metadata

            if item.source == RetrievalSource.TEMPLATE:
                context.templates.append(meta)
            elif item.source == RetrievalSource.SKILL:
                context.skills.append(meta)
            elif item.source == RetrievalSource.GRAPH:
                context.lineage_paths.append({
                    "path": item.content,
                    "direction": meta.get("direction"),
                    "distance": meta.get("distance"),
                })
            elif item.source == RetrievalSource.LEXICAL:
                if meta.get("type") == "glossary_term":
                    context.glossary_terms.append(meta)
                else:
                    context.catalog_assets.append(meta)
            elif item.source == RetrievalSource.KNOWLEDGE:
                context.knowledge.append(meta)

            # Track available entities for validation
            if meta.get("type") in ("TABLE", "VIEW"):
                table_name = meta.get("name", "")
                if table_name and table_name not in context.available_tables:
                    context.available_tables.append(table_name)

                    # Track columns
                    cols = [c.get("name") for c in meta.get("columns", [])]
                    if cols:
                        context.available_columns[table_name] = cols

            elif meta.get("type") == "HIERARCHY":
                hier_name = meta.get("name", "")
                if hier_name and hier_name not in context.available_hierarchies:
                    context.available_hierarchies.append(hier_name)

            # Populate hierarchy structures from vector store metadata
            if meta.get("source_type") == "hierarchy":
                context.hierarchy_structures.append({
                    "name": meta.get("name", ""),
                    "hierarchy_id": meta.get("hierarchy_id", ""),
                    "project_id": meta.get("project_id", ""),
                    "parent_id": meta.get("parent_id"),
                    "is_root": meta.get("is_root", False),
                    "has_mappings": meta.get("has_mappings", False),
                    "has_formula": meta.get("has_formula", False),
                    "property_count": meta.get("property_count", 0),
                    "level_depth": meta.get("level_depth", 0),
                    "mapping_count": meta.get("mapping_count", 0),
                    "content": item.content,
                })
            elif meta.get("source_type") == "hierarchy_project":
                context.hierarchy_projects.append({
                    "name": meta.get("name", ""),
                    "project_id": meta.get("project_id", ""),
                    "hierarchy_count": meta.get("hierarchy_count", 0),
                    "mapping_count": meta.get("mapping_count", 0),
                })

        return context
