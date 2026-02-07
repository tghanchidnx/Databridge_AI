"""
Catalog Store - Persistence layer for the Data Catalog.

Provides storage and retrieval for:
- Data assets (tables, views, hierarchies, etc.)
- Business glossary terms and domains
- Tags and classifications
- Search indexing
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .types import (
    AssetType,
    CatalogStats,
    ColumnProfile,
    DataAsset,
    DataClassification,
    DataQualityTier,
    GlossaryDomain,
    GlossaryTerm,
    Owner,
    OwnershipRole,
    QualityMetrics,
    SearchQuery,
    SearchResult,
    SearchResults,
    Tag,
    TermStatus,
)

logger = logging.getLogger(__name__)


class CatalogStore:
    """Persistent storage for the data catalog."""

    def __init__(self, data_dir: str = "data/data_catalog"):
        """
        Initialize the catalog store.

        Args:
            data_dir: Directory for storing catalog data
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        self.assets_file = self.data_dir / "assets.json"
        self.glossary_file = self.data_dir / "glossary.json"
        self.domains_file = self.data_dir / "domains.json"
        self.tags_file = self.data_dir / "tags.json"

        # In-memory stores
        self._assets: Dict[str, DataAsset] = {}
        self._glossary_terms: Dict[str, GlossaryTerm] = {}
        self._domains: Dict[str, GlossaryDomain] = {}
        self._tags: Dict[str, Tag] = {}

        # Search index (simple inverted index)
        self._search_index: Dict[str, Set[str]] = {}  # word -> asset_ids

        # Load from disk
        self._load_all()

    # =========================================================================
    # Asset Operations
    # =========================================================================

    def create_asset(self, asset: DataAsset) -> DataAsset:
        """Create a new data asset."""
        if asset.id in self._assets:
            raise ValueError(f"Asset with ID '{asset.id}' already exists")

        asset.created_at = datetime.now()
        asset.updated_at = datetime.now()

        self._assets[asset.id] = asset
        self._index_asset(asset)
        self._save_assets()

        logger.info(f"Created asset: {asset.name} ({asset.asset_type.value})")
        return asset

    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get an asset by ID."""
        return self._assets.get(asset_id)

    def get_asset_by_name(
        self,
        name: str,
        asset_type: Optional[AssetType] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Optional[DataAsset]:
        """Get an asset by name with optional filters."""
        for asset in self._assets.values():
            if asset.name.lower() != name.lower():
                continue
            if asset_type and asset.asset_type != asset_type:
                continue
            if database and asset.database and asset.database.lower() != database.lower():
                continue
            if schema_name and asset.schema_name and asset.schema_name.lower() != schema_name.lower():
                continue
            return asset
        return None

    def update_asset(self, asset_id: str, updates: Dict[str, Any]) -> Optional[DataAsset]:
        """Update an existing asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return None

        # Remove from index before update
        self._unindex_asset(asset)

        # Apply updates
        for key, value in updates.items():
            if hasattr(asset, key):
                setattr(asset, key, value)

        asset.updated_at = datetime.now()

        # Re-index
        self._index_asset(asset)
        self._save_assets()

        logger.info(f"Updated asset: {asset.name}")
        return asset

    def delete_asset(self, asset_id: str) -> bool:
        """Delete an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        self._unindex_asset(asset)
        del self._assets[asset_id]
        self._save_assets()

        logger.info(f"Deleted asset: {asset.name}")
        return True

    def list_assets(
        self,
        asset_type: Optional[AssetType] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        classification: Optional[DataClassification] = None,
        quality_tier: Optional[DataQualityTier] = None,
        owner_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[DataAsset]:
        """List assets with optional filters."""
        results = []

        for asset in self._assets.values():
            # Apply filters
            if asset_type and asset.asset_type != asset_type:
                continue
            if database and asset.database and asset.database.lower() != database.lower():
                continue
            if schema_name and asset.schema_name and asset.schema_name.lower() != schema_name.lower():
                continue
            if classification and asset.classification != classification:
                continue
            if quality_tier and asset.quality_tier != quality_tier:
                continue
            if parent_id and asset.parent_id != parent_id:
                continue

            if tags:
                asset_tag_names = {t.name.lower() for t in asset.tags}
                if not all(t.lower() in asset_tag_names for t in tags):
                    continue

            if owner_id:
                if not any(o.user_id == owner_id for o in asset.owners):
                    continue

            results.append(asset)

        # Sort by name
        results.sort(key=lambda a: a.name.lower())

        # Apply pagination
        return results[offset:offset + limit]

    def get_asset_children(self, parent_id: str) -> List[DataAsset]:
        """Get all child assets of a parent."""
        return [a for a in self._assets.values() if a.parent_id == parent_id]

    def get_asset_lineage(self, asset_id: str, direction: str = "both") -> Dict[str, List[DataAsset]]:
        """Get upstream and/or downstream assets."""
        asset = self._assets.get(asset_id)
        if not asset:
            return {"upstream": [], "downstream": []}

        result = {"upstream": [], "downstream": []}

        if direction in ("upstream", "both"):
            for up_id in asset.upstream_assets:
                up_asset = self._assets.get(up_id)
                if up_asset:
                    result["upstream"].append(up_asset)

        if direction in ("downstream", "both"):
            for down_id in asset.downstream_assets:
                down_asset = self._assets.get(down_id)
                if down_asset:
                    result["downstream"].append(down_asset)

        return result

    # =========================================================================
    # Tag Operations
    # =========================================================================

    def create_tag(self, tag: Tag) -> Tag:
        """Create a new tag."""
        self._tags[tag.name.lower()] = tag
        self._save_tags()
        return tag

    def get_tag(self, name: str) -> Optional[Tag]:
        """Get a tag by name."""
        return self._tags.get(name.lower())

    def list_tags(self, category: Optional[str] = None) -> List[Tag]:
        """List all tags, optionally filtered by category."""
        tags = list(self._tags.values())
        if category:
            tags = [t for t in tags if t.category == category]
        return sorted(tags, key=lambda t: t.name)

    def delete_tag(self, name: str) -> bool:
        """Delete a tag and remove from all assets."""
        if name.lower() not in self._tags:
            return False

        del self._tags[name.lower()]

        # Remove from all assets
        for asset in self._assets.values():
            asset.tags = [t for t in asset.tags if t.name.lower() != name.lower()]

        self._save_tags()
        self._save_assets()
        return True

    def add_tag_to_asset(self, asset_id: str, tag_name: str) -> bool:
        """Add a tag to an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        tag = self._tags.get(tag_name.lower())
        if not tag:
            tag = Tag(name=tag_name)
            self.create_tag(tag)

        asset.add_tag(tag)
        self._save_assets()
        return True

    def remove_tag_from_asset(self, asset_id: str, tag_name: str) -> bool:
        """Remove a tag from an asset."""
        asset = self._assets.get(asset_id)
        if not asset:
            return False

        if asset.remove_tag(tag_name):
            self._save_assets()
            return True
        return False

    # =========================================================================
    # Glossary Operations
    # =========================================================================

    def create_term(self, term: GlossaryTerm) -> GlossaryTerm:
        """Create a new glossary term."""
        if term.id in self._glossary_terms:
            raise ValueError(f"Term with ID '{term.id}' already exists")

        term.created_at = datetime.now()
        term.updated_at = datetime.now()

        self._glossary_terms[term.id] = term
        self._index_term(term)
        self._save_glossary()

        # Update domain term count
        if term.domain:
            for domain in self._domains.values():
                if domain.name.lower() == term.domain.lower():
                    domain.term_count += 1
                    self._save_domains()
                    break

        logger.info(f"Created glossary term: {term.name}")
        return term

    def get_term(self, term_id: str) -> Optional[GlossaryTerm]:
        """Get a glossary term by ID."""
        return self._glossary_terms.get(term_id)

    def get_term_by_name(self, name: str) -> Optional[GlossaryTerm]:
        """Get a glossary term by name."""
        for term in self._glossary_terms.values():
            if term.name.lower() == name.lower():
                return term
        return None

    def update_term(self, term_id: str, updates: Dict[str, Any]) -> Optional[GlossaryTerm]:
        """Update a glossary term."""
        term = self._glossary_terms.get(term_id)
        if not term:
            return None

        self._unindex_term(term)

        for key, value in updates.items():
            if hasattr(term, key):
                setattr(term, key, value)

        term.updated_at = datetime.now()

        self._index_term(term)
        self._save_glossary()

        logger.info(f"Updated glossary term: {term.name}")
        return term

    def delete_term(self, term_id: str) -> bool:
        """Delete a glossary term."""
        term = self._glossary_terms.get(term_id)
        if not term:
            return False

        self._unindex_term(term)
        del self._glossary_terms[term_id]
        self._save_glossary()

        logger.info(f"Deleted glossary term: {term.name}")
        return True

    def list_terms(
        self,
        domain: Optional[str] = None,
        status: Optional[TermStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[GlossaryTerm]:
        """List glossary terms with filters."""
        results = []

        for term in self._glossary_terms.values():
            if domain and term.domain and term.domain.lower() != domain.lower():
                continue
            if status and term.status != status:
                continue
            results.append(term)

        results.sort(key=lambda t: t.name.lower())
        return results[offset:offset + limit]

    def link_term_to_asset(self, term_id: str, asset_id: str) -> bool:
        """Link a glossary term to a data asset."""
        term = self._glossary_terms.get(term_id)
        asset = self._assets.get(asset_id)

        if not term or not asset:
            return False

        if asset_id not in term.linked_asset_ids:
            term.linked_asset_ids.append(asset_id)
            term.updated_at = datetime.now()
            self._save_glossary()

        return True

    def link_term_to_column(self, term_id: str, column_ref: str) -> bool:
        """Link a glossary term to a column (database.schema.table.column)."""
        term = self._glossary_terms.get(term_id)
        if not term:
            return False

        if column_ref not in term.linked_column_refs:
            term.linked_column_refs.append(column_ref)
            term.updated_at = datetime.now()
            self._save_glossary()

        return True

    # =========================================================================
    # Domain Operations
    # =========================================================================

    def create_domain(self, domain: GlossaryDomain) -> GlossaryDomain:
        """Create a new glossary domain."""
        if domain.id in self._domains:
            raise ValueError(f"Domain with ID '{domain.id}' already exists")

        domain.created_at = datetime.now()
        self._domains[domain.id] = domain
        self._save_domains()

        logger.info(f"Created glossary domain: {domain.name}")
        return domain

    def get_domain(self, domain_id: str) -> Optional[GlossaryDomain]:
        """Get a domain by ID."""
        return self._domains.get(domain_id)

    def list_domains(self) -> List[GlossaryDomain]:
        """List all glossary domains."""
        return sorted(self._domains.values(), key=lambda d: d.name)

    def delete_domain(self, domain_id: str) -> bool:
        """Delete a domain."""
        if domain_id not in self._domains:
            return False

        del self._domains[domain_id]
        self._save_domains()
        return True

    # =========================================================================
    # Search Operations
    # =========================================================================

    def search(self, query: SearchQuery) -> SearchResults:
        """Search the catalog."""
        import time
        start_time = time.time()

        results = []
        query_words = self._tokenize(query.query.lower())

        # Find matching asset IDs from index
        matching_ids: Set[str] = set()
        for word in query_words:
            for indexed_word, asset_ids in self._search_index.items():
                if word in indexed_word or indexed_word.startswith(word):
                    matching_ids.update(asset_ids)

        # Score and filter results
        for asset_id in matching_ids:
            asset = self._assets.get(asset_id)
            if not asset:
                continue

            # Apply filters
            if query.asset_types and asset.asset_type not in query.asset_types:
                continue
            if query.classifications and asset.classification not in query.classifications:
                continue
            if query.quality_tier and asset.quality_tier != query.quality_tier:
                continue
            if query.databases and asset.database and asset.database.lower() not in [d.lower() for d in query.databases]:
                continue
            if query.schemas and asset.schema_name and asset.schema_name.lower() not in [s.lower() for s in query.schemas]:
                continue

            if query.tags:
                asset_tag_names = {t.name.lower() for t in asset.tags}
                if not all(t.lower() in asset_tag_names for t in query.tags):
                    continue

            if query.owners:
                asset_owner_ids = {o.user_id for o in asset.owners}
                if not any(o in asset_owner_ids for o in query.owners):
                    continue

            if query.min_quality_score:
                if asset.quality_metrics:
                    score = asset.quality_metrics.overall_score
                    if score is None or score < query.min_quality_score:
                        continue
                else:
                    continue

            # Calculate relevance score
            score = self._calculate_relevance(asset, query_words)

            # Get match highlights
            highlights = self._get_highlights(asset, query_words)

            results.append(SearchResult(
                asset_id=asset.id,
                asset_type=asset.asset_type,
                name=asset.name,
                fully_qualified_name=asset.fully_qualified_name,
                description=asset.description,
                match_score=score,
                match_highlights=highlights,
                tags=[t.name for t in asset.tags],
                owners=[o.name for o in asset.owners],
                quality_tier=asset.quality_tier,
            ))

        # Include glossary terms if requested
        if query.include_glossary:
            for term in self._glossary_terms.values():
                term_text = f"{term.name} {term.definition}".lower()
                if any(word in term_text for word in query_words):
                    score = sum(1 for w in query_words if w in term_text) / len(query_words)
                    results.append(SearchResult(
                        asset_id=term.id,
                        asset_type=AssetType.HIERARCHY,  # Placeholder
                        name=f"[Term] {term.name}",
                        description=term.definition[:200],
                        match_score=score * 0.8,  # Slightly lower priority
                        match_highlights=[term.definition[:100]],
                        tags=[t.name for t in term.tags],
                    ))

        # Sort by relevance
        results.sort(key=lambda r: r.match_score, reverse=True)

        # Apply pagination
        total = len(results)
        results = results[query.offset:query.offset + query.limit]

        took_ms = int((time.time() - start_time) * 1000)

        return SearchResults(
            query=query.query,
            total_count=total,
            results=results,
            offset=query.offset,
            limit=query.limit,
            took_ms=took_ms,
        )

    def _calculate_relevance(self, asset: DataAsset, query_words: List[str]) -> float:
        """Calculate relevance score for an asset."""
        score = 0.0
        total_words = len(query_words)
        if total_words == 0:
            return 0.0

        # Name match (highest weight)
        name_lower = asset.name.lower()
        for word in query_words:
            if word in name_lower:
                score += 3.0
            if name_lower.startswith(word):
                score += 2.0

        # Description match
        if asset.description:
            desc_lower = asset.description.lower()
            for word in query_words:
                if word in desc_lower:
                    score += 1.0

        # Tag match
        tag_names = " ".join(t.name.lower() for t in asset.tags)
        for word in query_words:
            if word in tag_names:
                score += 1.5

        # Column match
        if asset.columns:
            col_text = " ".join(c.column_name.lower() for c in asset.columns)
            for word in query_words:
                if word in col_text:
                    score += 0.5

        # Normalize
        return min(score / (total_words * 5), 1.0)

    def _get_highlights(self, asset: DataAsset, query_words: List[str]) -> List[str]:
        """Get text snippets that match the query."""
        highlights = []

        # Check name
        if any(w in asset.name.lower() for w in query_words):
            highlights.append(f"Name: {asset.name}")

        # Check description
        if asset.description:
            for word in query_words:
                if word in asset.description.lower():
                    # Find context around match
                    idx = asset.description.lower().find(word)
                    start = max(0, idx - 30)
                    end = min(len(asset.description), idx + len(word) + 30)
                    snippet = asset.description[start:end]
                    if start > 0:
                        snippet = "..." + snippet
                    if end < len(asset.description):
                        snippet = snippet + "..."
                    highlights.append(snippet)
                    break

        return highlights[:3]

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into searchable words."""
        # Split on non-alphanumeric
        words = re.split(r'[^a-zA-Z0-9]+', text.lower())
        # Filter short words
        return [w for w in words if len(w) >= 2]

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self) -> CatalogStats:
        """Get catalog statistics."""
        stats = CatalogStats()

        # Asset counts
        stats.total_assets = len(self._assets)
        for asset in self._assets.values():
            asset_type = asset.asset_type.value
            stats.assets_by_type[asset_type] = stats.assets_by_type.get(asset_type, 0) + 1

            classification = asset.classification.value
            stats.assets_by_classification[classification] = stats.assets_by_classification.get(classification, 0) + 1

            tier = asset.quality_tier.value
            stats.assets_by_quality_tier[tier] = stats.assets_by_quality_tier.get(tier, 0) + 1

            if not asset.owners:
                stats.assets_without_owners += 1
            if not asset.description:
                stats.assets_without_descriptions += 1

        # Glossary counts
        stats.total_glossary_terms = len(self._glossary_terms)
        for term in self._glossary_terms.values():
            status = term.status.value
            stats.terms_by_status[status] = stats.terms_by_status.get(status, 0) + 1

            if term.domain:
                stats.terms_by_domain[term.domain] = stats.terms_by_domain.get(term.domain, 0) + 1

        # Tag counts
        stats.total_tags = len(self._tags)
        tag_usage: Dict[str, int] = {}
        for asset in self._assets.values():
            for tag in asset.tags:
                tag_usage[tag.name] = tag_usage.get(tag.name, 0) + 1

        stats.most_used_tags = [
            {"name": name, "count": count}
            for name, count in sorted(tag_usage.items(), key=lambda x: x[1], reverse=True)[:10]
        ]

        # Owner counts
        unique_owners = set()
        for asset in self._assets.values():
            for owner in asset.owners:
                unique_owners.add(owner.user_id)
        stats.total_owners = len(unique_owners)

        stats.catalog_updated_at = datetime.now()
        return stats

    # =========================================================================
    # Indexing
    # =========================================================================

    def _index_asset(self, asset: DataAsset) -> None:
        """Add an asset to the search index."""
        words = set()

        # Index name
        words.update(self._tokenize(asset.name))

        # Index description
        if asset.description:
            words.update(self._tokenize(asset.description))

        # Index tags
        for tag in asset.tags:
            words.update(self._tokenize(tag.name))

        # Index columns
        for col in asset.columns:
            words.update(self._tokenize(col.column_name))
            if col.description:
                words.update(self._tokenize(col.description))

        # Index fully qualified name
        if asset.fully_qualified_name:
            words.update(self._tokenize(asset.fully_qualified_name))

        # Add to index
        for word in words:
            if word not in self._search_index:
                self._search_index[word] = set()
            self._search_index[word].add(asset.id)

    def _unindex_asset(self, asset: DataAsset) -> None:
        """Remove an asset from the search index."""
        for word_set in self._search_index.values():
            word_set.discard(asset.id)

    def _index_term(self, term: GlossaryTerm) -> None:
        """Add a term to the search index."""
        words = set()
        words.update(self._tokenize(term.name))
        words.update(self._tokenize(term.definition))

        for word in words:
            if word not in self._search_index:
                self._search_index[word] = set()
            self._search_index[word].add(f"term:{term.id}")

    def _unindex_term(self, term: GlossaryTerm) -> None:
        """Remove a term from the search index."""
        term_key = f"term:{term.id}"
        for word_set in self._search_index.values():
            word_set.discard(term_key)

    def rebuild_index(self) -> int:
        """Rebuild the entire search index."""
        self._search_index.clear()

        for asset in self._assets.values():
            self._index_asset(asset)

        for term in self._glossary_terms.values():
            self._index_term(term)

        return len(self._search_index)

    # =========================================================================
    # Persistence
    # =========================================================================

    def _save_assets(self) -> None:
        """Save assets to disk."""
        data = {
            asset_id: asset.model_dump(mode="json")
            for asset_id, asset in self._assets.items()
        }
        with open(self.assets_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _save_glossary(self) -> None:
        """Save glossary terms to disk."""
        data = {
            term_id: term.model_dump(mode="json")
            for term_id, term in self._glossary_terms.items()
        }
        with open(self.glossary_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _save_domains(self) -> None:
        """Save domains to disk."""
        data = {
            domain_id: domain.model_dump(mode="json")
            for domain_id, domain in self._domains.items()
        }
        with open(self.domains_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _save_tags(self) -> None:
        """Save tags to disk."""
        data = {
            name: tag.model_dump(mode="json")
            for name, tag in self._tags.items()
        }
        with open(self.tags_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _load_all(self) -> None:
        """Load all data from disk."""
        self._load_assets()
        self._load_glossary()
        self._load_domains()
        self._load_tags()
        self.rebuild_index()

    def _load_assets(self) -> None:
        """Load assets from disk."""
        if not self.assets_file.exists():
            return

        try:
            with open(self.assets_file, "r") as f:
                data = json.load(f)

            for asset_id, asset_data in data.items():
                # Parse nested models
                if "tags" in asset_data:
                    asset_data["tags"] = [Tag(**t) for t in asset_data["tags"]]
                if "owners" in asset_data:
                    asset_data["owners"] = [Owner(**o) for o in asset_data["owners"]]
                if "columns" in asset_data:
                    asset_data["columns"] = [ColumnProfile(**c) for c in asset_data["columns"]]
                if "quality_metrics" in asset_data and asset_data["quality_metrics"]:
                    asset_data["quality_metrics"] = QualityMetrics(**asset_data["quality_metrics"])

                # Handle datetime fields
                for dt_field in ["created_at", "updated_at", "last_scanned_at", "last_accessed_at"]:
                    if dt_field in asset_data and asset_data[dt_field]:
                        if isinstance(asset_data[dt_field], str):
                            asset_data[dt_field] = datetime.fromisoformat(asset_data[dt_field].replace("Z", "+00:00"))

                self._assets[asset_id] = DataAsset(**asset_data)

        except Exception as e:
            logger.error(f"Failed to load assets: {e}")

    def _load_glossary(self) -> None:
        """Load glossary terms from disk."""
        if not self.glossary_file.exists():
            return

        try:
            with open(self.glossary_file, "r") as f:
                data = json.load(f)

            for term_id, term_data in data.items():
                if "tags" in term_data:
                    term_data["tags"] = [Tag(**t) for t in term_data["tags"]]
                if "owner" in term_data and term_data["owner"]:
                    term_data["owner"] = Owner(**term_data["owner"])

                for dt_field in ["created_at", "updated_at", "approved_at"]:
                    if dt_field in term_data and term_data[dt_field]:
                        if isinstance(term_data[dt_field], str):
                            term_data[dt_field] = datetime.fromisoformat(term_data[dt_field].replace("Z", "+00:00"))

                self._glossary_terms[term_id] = GlossaryTerm(**term_data)

        except Exception as e:
            logger.error(f"Failed to load glossary: {e}")

    def _load_domains(self) -> None:
        """Load domains from disk."""
        if not self.domains_file.exists():
            return

        try:
            with open(self.domains_file, "r") as f:
                data = json.load(f)

            for domain_id, domain_data in data.items():
                if "owner" in domain_data and domain_data["owner"]:
                    domain_data["owner"] = Owner(**domain_data["owner"])

                if "created_at" in domain_data and domain_data["created_at"]:
                    if isinstance(domain_data["created_at"], str):
                        domain_data["created_at"] = datetime.fromisoformat(domain_data["created_at"].replace("Z", "+00:00"))

                self._domains[domain_id] = GlossaryDomain(**domain_data)

        except Exception as e:
            logger.error(f"Failed to load domains: {e}")

    def _load_tags(self) -> None:
        """Load tags from disk."""
        if not self.tags_file.exists():
            return

        try:
            with open(self.tags_file, "r") as f:
                data = json.load(f)

            for name, tag_data in data.items():
                self._tags[name] = Tag(**tag_data)

        except Exception as e:
            logger.error(f"Failed to load tags: {e}")
