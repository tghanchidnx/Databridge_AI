"""
Entity Extractor - Extract and link entities from natural language queries.

Identifies:
- Tables, columns, databases, schemas
- Hierarchies and hierarchy projects
- Domains (finance, operations, etc.)
- Industries (oil_gas, manufacturing, etc.)
- Business terms from glossary

Provides entity linking to connect extracted entities to catalog/hierarchy IDs.
"""
import logging
import re
from typing import Dict, List, Optional, Set, Any

from .types import ExtractedEntity, EntityType, RAGQuery

logger = logging.getLogger(__name__)


# Domain patterns
DOMAIN_PATTERNS = {
    "finance": [
        r"\b(p&l|profit\s*(?:and|&)?\s*loss|income\s+statement|balance\s+sheet)\b",
        r"\b(gl|general\s+ledger|coa|chart\s+of\s+accounts)\b",
        r"\b(revenue|expense|asset|liability|equity)\b",
        r"\b(budget|forecast|variance|actuals)\b",
    ],
    "operations": [
        r"\b(geographic|region|country|state|city|location)\b",
        r"\b(department|team|organization|org\s+chart)\b",
        r"\b(asset|equipment|plant|facility)\b",
    ],
    "accounting": [
        r"\b(reconciliation|trial\s+balance|journal\s+entry)\b",
        r"\b(debit|credit|posting|accrual)\b",
        r"\b(cost\s+center|profit\s+center|business\s+unit)\b",
    ],
}

# Industry patterns
INDUSTRY_PATTERNS = {
    "oil_gas": [
        r"\b(well|field|basin|lease|royalty)\b",
        r"\b(loe|lease\s+operating|jib|joint\s+interest)\b",
        r"\b(dd&a|depletion|boe|mcf|bbl)\b",
        r"\b(upstream|midstream|downstream|e&p)\b",
    ],
    "manufacturing": [
        r"\b(plant|bom|bill\s+of\s+materials|wip)\b",
        r"\b(standard\s+cost|variance|scrap|yield)\b",
        r"\b(work\s+order|production|assembly)\b",
    ],
    "saas": [
        r"\b(arr|mrr|annual\s+recurring|monthly\s+recurring)\b",
        r"\b(churn|ltv|cac|cohort)\b",
        r"\b(subscription|renewal|upsell)\b",
    ],
    "transportation": [
        r"\b(fleet|truck|trailer|lane|route)\b",
        r"\b(terminal|warehouse|logistics)\b",
        r"\b(operating\s+ratio|deadhead|utilization)\b",
    ],
}

# SQL/Data patterns
DATA_PATTERNS = {
    "table": r"\b(?:from|join|into|update|table)\s+([a-zA-Z_][\w.]*)",
    "column": r"\b(?:select|where|on|and|or|group\s+by|order\s+by)\s+([a-zA-Z_]\w*)",
    "database": r"\b(?:database|db|use)\s+([a-zA-Z_]\w*)",
    "schema": r"\b(?:schema)\s+([a-zA-Z_]\w*)",
}


class EntityExtractor:
    """
    Extract entities from natural language queries.

    Uses pattern matching and optional NER to identify:
    - Data entities (tables, columns, databases)
    - Domain/industry context
    - Business terms
    """

    def __init__(
        self,
        catalog_store=None,
        hierarchy_service=None,
        glossary_terms: Optional[List[Dict[str, str]]] = None,
    ):
        self.catalog = catalog_store
        self.hierarchy = hierarchy_service
        self.glossary_terms = glossary_terms or []

        # Build lookup sets from catalog/hierarchy
        self._known_tables: Set[str] = set()
        self._known_columns: Set[str] = set()
        self._known_hierarchies: Set[str] = set()
        self._term_map: Dict[str, str] = {}  # term_name -> term_id

        self._build_lookups()

    def _build_lookups(self) -> None:
        """Build lookup tables from catalog and hierarchy."""
        # Tables from catalog
        if self.catalog:
            try:
                assets = self.catalog.list_assets(asset_types=["TABLE", "VIEW"])
                for asset in assets.get("assets", []):
                    name = asset.get("name", "").upper()
                    self._known_tables.add(name)

                    # Index columns
                    for col in asset.get("columns", []):
                        self._known_columns.add(col.get("name", "").upper())
            except Exception as e:
                logger.debug(f"Catalog lookup build failed: {e}")

        # Hierarchies
        if self.hierarchy:
            try:
                projects = self.hierarchy.list_projects()
                for proj in projects:
                    hierarchies = self.hierarchy.list_hierarchies(proj["id"])
                    for h in hierarchies:
                        name = h.get("hierarchy_name", "").upper()
                        if name:
                            self._known_hierarchies.add(name)
                        # Also index hierarchy_id for direct matching
                        hier_id = h.get("hierarchy_id", "").upper()
                        if hier_id:
                            self._known_hierarchies.add(hier_id)
            except Exception as e:
                logger.debug(f"Hierarchy lookup build failed: {e}")

        # Glossary terms
        for term in self.glossary_terms:
            name = term.get("name", "").lower()
            self._term_map[name] = term.get("id", "")

    def extract(self, query: str) -> List[ExtractedEntity]:
        """
        Extract all entities from a query.

        Args:
            query: Natural language query

        Returns:
            List of extracted entities with types and confidence
        """
        entities = []
        query_lower = query.lower()
        query_upper = query.upper()

        # Extract domain
        domain = self._extract_domain(query_lower)
        if domain:
            entities.append(ExtractedEntity(
                text=domain,
                entity_type=EntityType.DOMAIN,
                confidence=0.8,
            ))

        # Extract industry
        industry = self._extract_industry(query_lower)
        if industry:
            entities.append(ExtractedEntity(
                text=industry,
                entity_type=EntityType.INDUSTRY,
                confidence=0.8,
            ))

        # Extract tables
        tables = self._extract_tables(query)
        for table, pos in tables:
            linked_id = table.upper() if table.upper() in self._known_tables else None
            entities.append(ExtractedEntity(
                text=table,
                entity_type=EntityType.TABLE,
                confidence=0.9 if linked_id else 0.6,
                linked_id=linked_id,
                start_pos=pos,
            ))

        # Extract columns
        columns = self._extract_columns(query)
        for col, pos in columns:
            linked = col.upper() in self._known_columns
            entities.append(ExtractedEntity(
                text=col,
                entity_type=EntityType.COLUMN,
                confidence=0.7 if linked else 0.5,
                start_pos=pos,
            ))

        # Extract hierarchy references
        hierarchies = self._extract_hierarchies(query)
        for hier, pos in hierarchies:
            linked_id = hier.upper() if hier.upper() in self._known_hierarchies else None
            entities.append(ExtractedEntity(
                text=hier,
                entity_type=EntityType.HIERARCHY,
                confidence=0.85 if linked_id else 0.6,
                linked_id=linked_id,
                start_pos=pos,
            ))

        # Extract glossary terms
        terms = self._extract_glossary_terms(query_lower)
        for term, term_id in terms:
            entities.append(ExtractedEntity(
                text=term,
                entity_type=EntityType.GLOSSARY_TERM,
                confidence=0.9,
                linked_id=term_id,
            ))

        # Deduplicate by text
        seen = set()
        unique_entities = []
        for e in entities:
            key = (e.text.lower(), e.entity_type)
            if key not in seen:
                seen.add(key)
                unique_entities.append(e)

        return unique_entities

    def _extract_domain(self, query: str) -> Optional[str]:
        """Extract domain from query."""
        for domain, patterns in DOMAIN_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    return domain
        return None

    def _extract_industry(self, query: str) -> Optional[str]:
        """Extract industry from query."""
        for industry, patterns in INDUSTRY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    return industry
        return None

    def _extract_tables(self, query: str) -> List[tuple]:
        """Extract table references. Returns [(name, position), ...]"""
        results = []

        # FROM/JOIN patterns
        pattern = DATA_PATTERNS["table"]
        for match in re.finditer(pattern, query, re.IGNORECASE):
            table = match.group(1)
            # Filter out common keywords
            if table.upper() not in {"SELECT", "FROM", "WHERE", "AND", "OR", "NULL", "TRUE", "FALSE"}:
                results.append((table, match.start(1)))

        # Quoted table names
        quoted_pattern = r'["\']([a-zA-Z_][\w.]*)["\']'
        for match in re.finditer(quoted_pattern, query):
            results.append((match.group(1), match.start(1)))

        return results

    def _extract_columns(self, query: str) -> List[tuple]:
        """Extract column references. Returns [(name, position), ...]"""
        results = []

        # Look for patterns like column_name = or column_name LIKE
        pattern = r'\b([a-zA-Z_]\w*)\s*(?:=|<|>|LIKE|IN|IS|BETWEEN)'
        for match in re.finditer(pattern, query, re.IGNORECASE):
            col = match.group(1)
            if col.upper() not in {"AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "SELECT", "FROM", "WHERE"}:
                results.append((col, match.start(1)))

        return results

    def _extract_hierarchies(self, query: str) -> List[tuple]:
        """Extract hierarchy references. Returns [(name, position), ...]"""
        results = []
        found_names = set()

        # Look for hierarchy keywords
        patterns = [
            r'hierarchy\s+(?:named?\s+)?["\']?([a-zA-Z_][\w\s-]*)["\']?',
            r'([a-zA-Z_][\w\s-]*)\s+hierarchy',
            r'project\s+["\']?([a-zA-Z_][\w\s-]*)["\']?',
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, query, re.IGNORECASE):
                hier = match.group(1).strip()
                if len(hier) > 2 and hier.upper() not in found_names:
                    results.append((hier, match.start(1)))
                    found_names.add(hier.upper())

        # Direct matching against known hierarchies
        query_upper = query.upper()
        for known in self._known_hierarchies:
            if known not in found_names and known in query_upper:
                pos = query_upper.index(known)
                results.append((known, pos))
                found_names.add(known)

        return results

    def _extract_glossary_terms(self, query: str) -> List[tuple]:
        """Extract glossary terms. Returns [(term_name, term_id), ...]"""
        results = []

        for term_name, term_id in self._term_map.items():
            # Case-insensitive whole word match
            pattern = rf'\b{re.escape(term_name)}\b'
            if re.search(pattern, query, re.IGNORECASE):
                results.append((term_name, term_id))

        return results

    def enrich_query(self, query: RAGQuery) -> RAGQuery:
        """
        Enrich a RAGQuery with extracted entities.

        Args:
            query: RAGQuery to enrich

        Returns:
            Enriched RAGQuery with entities, domain, industry
        """
        entities = self.extract(query.query)
        query.entities = entities

        # Set domain if found
        for e in entities:
            if e.entity_type == EntityType.DOMAIN and not query.domain:
                query.domain = e.text
            elif e.entity_type == EntityType.INDUSTRY and not query.industry:
                query.industry = e.text

        return query
