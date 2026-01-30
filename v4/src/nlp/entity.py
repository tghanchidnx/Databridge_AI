"""
Entity Extractor for DataBridge AI V4 Analytics Engine.

Extracts database entities (tables, columns, values) from natural language queries
by matching against a metadata catalog.
"""

import re
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set, Tuple
from enum import Enum

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


class EntityType(str, Enum):
    """Types of database entities."""
    TABLE = "table"
    COLUMN = "column"
    VALUE = "value"
    METRIC = "metric"
    DIMENSION = "dimension"
    TIME_COLUMN = "time_column"
    AGGREGATE = "aggregate"


@dataclass
class Entity:
    """Represents an extracted entity."""

    entity_type: EntityType
    name: str  # The matched catalog name
    original_text: str  # The text from the query
    confidence: float
    table: Optional[str] = None  # For columns, the parent table
    aliases: List[str] = field(default_factory=list)
    data_type: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "entity_type": self.entity_type.value,
            "name": self.name,
            "original_text": self.original_text,
            "confidence": round(self.confidence, 2),
            "table": self.table,
            "data_type": self.data_type,
        }


@dataclass
class CatalogEntry:
    """Represents an entry in the metadata catalog."""

    name: str
    entity_type: EntityType
    table: Optional[str] = None
    data_type: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    description: Optional[str] = None
    is_metric: bool = False
    is_dimension: bool = False


class EntityExtractor:
    """
    Extracts database entities from natural language queries.

    Uses a metadata catalog to match query terms to:
    - Tables
    - Columns (with their parent tables)
    - Metrics (numeric columns used for aggregation)
    - Dimensions (categorical columns used for grouping)
    - Time columns (date/datetime columns)
    """

    # Common metric aliases
    METRIC_ALIASES = {
        "revenue": ["sales", "income", "earnings", "total_revenue", "net_revenue"],
        "cost": ["expense", "costs", "spending", "total_cost"],
        "profit": ["margin", "earnings", "net_income", "gross_profit"],
        "quantity": ["qty", "count", "units", "volume"],
        "amount": ["value", "total", "sum"],
        "budget": ["planned", "forecast", "target"],
        "actual": ["actuals", "realized", "real"],
    }

    # Common dimension aliases
    DIMENSION_ALIASES = {
        "region": ["area", "territory", "geography", "location"],
        "department": ["dept", "division", "team", "group"],
        "category": ["type", "class", "segment", "classification"],
        "customer": ["client", "account", "buyer"],
        "product": ["item", "sku", "merchandise"],
        "date": ["time", "period", "when"],
        "year": ["yr", "fiscal_year", "calendar_year"],
        "month": ["mo", "period"],
        "quarter": ["qtr", "q"],
    }

    def __init__(
        self,
        catalog: Optional[List[CatalogEntry]] = None,
        fuzzy_threshold: float = 80.0,
        enable_fuzzy: bool = True,
    ):
        """
        Initialize the entity extractor.

        Args:
            catalog: List of catalog entries to match against.
            fuzzy_threshold: Minimum fuzzy match score (0-100).
            enable_fuzzy: Whether to use fuzzy matching.
        """
        self.catalog = catalog or []
        self.fuzzy_threshold = fuzzy_threshold
        self.enable_fuzzy = enable_fuzzy and RAPIDFUZZ_AVAILABLE

        # Build lookup structures
        self._build_lookups()

    def _build_lookups(self) -> None:
        """Build lookup dictionaries for fast matching."""
        self._table_names: Dict[str, CatalogEntry] = {}
        self._column_names: Dict[str, List[CatalogEntry]] = {}
        self._all_names: List[Tuple[str, CatalogEntry]] = []

        for entry in self.catalog:
            name_lower = entry.name.lower()

            if entry.entity_type == EntityType.TABLE:
                self._table_names[name_lower] = entry
                self._all_names.append((name_lower, entry))
                # Add aliases
                for alias in entry.aliases:
                    self._table_names[alias.lower()] = entry
                    self._all_names.append((alias.lower(), entry))
            else:
                # Columns, metrics, dimensions
                if name_lower not in self._column_names:
                    self._column_names[name_lower] = []
                self._column_names[name_lower].append(entry)
                self._all_names.append((name_lower, entry))
                # Add aliases
                for alias in entry.aliases:
                    alias_lower = alias.lower()
                    if alias_lower not in self._column_names:
                        self._column_names[alias_lower] = []
                    self._column_names[alias_lower].append(entry)
                    self._all_names.append((alias_lower, entry))

    def add_catalog_entry(self, entry: CatalogEntry) -> None:
        """Add a new entry to the catalog."""
        self.catalog.append(entry)
        self._build_lookups()

    def load_catalog_from_dict(self, catalog_dict: List[Dict[str, Any]]) -> None:
        """
        Load catalog from a list of dictionaries.

        Args:
            catalog_dict: List of catalog entry dictionaries.
        """
        for item in catalog_dict:
            entity_type = EntityType(item.get("entity_type", "column"))
            entry = CatalogEntry(
                name=item["name"],
                entity_type=entity_type,
                table=item.get("table"),
                data_type=item.get("data_type"),
                aliases=item.get("aliases", []),
                description=item.get("description"),
                is_metric=item.get("is_metric", False),
                is_dimension=item.get("is_dimension", False),
            )
            self.catalog.append(entry)
        self._build_lookups()

    def extract(self, query: str) -> List[Entity]:
        """
        Extract entities from a natural language query.

        Args:
            query: Natural language query string.

        Returns:
            List of extracted entities.
        """
        entities = []
        query_lower = query.lower()

        # Tokenize query
        tokens = self._tokenize(query_lower)

        # Try to match each token and token combinations
        matched_spans: Set[Tuple[int, int]] = set()

        # Try multi-word matches first (longest match)
        for n in range(min(4, len(tokens)), 0, -1):
            for i in range(len(tokens) - n + 1):
                span = (i, i + n)
                if self._overlaps_matched(span, matched_spans):
                    continue

                phrase = " ".join(tokens[i:i + n])
                entity = self._match_phrase(phrase)

                if entity:
                    entities.append(entity)
                    matched_spans.add(span)

        # Sort by position in original query
        entities.sort(key=lambda e: query_lower.find(e.original_text.lower()))

        return entities

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into words."""
        # Remove punctuation except underscores
        text = re.sub(r"[^\w\s_]", " ", text)
        # Split on whitespace
        tokens = text.split()
        return tokens

    def _overlaps_matched(
        self, span: Tuple[int, int], matched: Set[Tuple[int, int]]
    ) -> bool:
        """Check if a span overlaps with already matched spans."""
        for m_start, m_end in matched:
            if not (span[1] <= m_start or span[0] >= m_end):
                return True
        return False

    def _match_phrase(self, phrase: str) -> Optional[Entity]:
        """Try to match a phrase to catalog entries."""
        # Exact match
        entity = self._exact_match(phrase)
        if entity:
            return entity

        # Alias match
        entity = self._alias_match(phrase)
        if entity:
            return entity

        # Fuzzy match - only for single words to avoid false positives
        # Multi-word phrases like "sales by order_date" would incorrectly match "sales"
        if self.enable_fuzzy and " " not in phrase:
            entity = self._fuzzy_match(phrase)
            if entity:
                return entity

        return None

    def _exact_match(self, phrase: str) -> Optional[Entity]:
        """Try exact matching against catalog."""
        # Check tables
        if phrase in self._table_names:
            entry = self._table_names[phrase]
            return Entity(
                entity_type=entry.entity_type,
                name=entry.name,
                original_text=phrase,
                confidence=1.0,
                data_type=entry.data_type,
            )

        # Check columns
        if phrase in self._column_names:
            entries = self._column_names[phrase]
            # Return first match (could be improved with context)
            entry = entries[0]
            return Entity(
                entity_type=self._get_entity_type(entry),
                name=entry.name,
                original_text=phrase,
                confidence=1.0,
                table=entry.table,
                data_type=entry.data_type,
            )

        return None

    def _alias_match(self, phrase: str) -> Optional[Entity]:
        """Try matching common aliases."""
        # Check metric aliases
        for canonical, aliases in self.METRIC_ALIASES.items():
            if phrase in aliases or phrase == canonical:
                # Look for matching column in catalog
                for name, entry in self._all_names:
                    if canonical in name or name in aliases:
                        return Entity(
                            entity_type=EntityType.METRIC,
                            name=entry.name,
                            original_text=phrase,
                            confidence=0.9,
                            table=entry.table,
                            data_type=entry.data_type,
                        )

        # Check dimension aliases
        for canonical, aliases in self.DIMENSION_ALIASES.items():
            if phrase in aliases or phrase == canonical:
                for name, entry in self._all_names:
                    if canonical in name or name in aliases:
                        return Entity(
                            entity_type=EntityType.DIMENSION,
                            name=entry.name,
                            original_text=phrase,
                            confidence=0.9,
                            table=entry.table,
                            data_type=entry.data_type,
                        )

        return None

    def _fuzzy_match(self, phrase: str) -> Optional[Entity]:
        """Try fuzzy matching against catalog."""
        if not self._all_names:
            return None

        names = [name for name, _ in self._all_names]
        result = process.extractOne(
            phrase,
            names,
            scorer=fuzz.WRatio,
            score_cutoff=self.fuzzy_threshold,
        )

        if result:
            matched_name, score, idx = result
            _, entry = self._all_names[idx]
            return Entity(
                entity_type=self._get_entity_type(entry),
                name=entry.name,
                original_text=phrase,
                confidence=score / 100.0,
                table=entry.table,
                data_type=entry.data_type,
            )

        return None

    def _get_entity_type(self, entry: CatalogEntry) -> EntityType:
        """Determine the entity type for a catalog entry."""
        if entry.is_metric:
            return EntityType.METRIC
        if entry.is_dimension:
            return EntityType.DIMENSION
        # Check data_type for date/time columns
        if entry.data_type and entry.data_type.lower() in ["date", "datetime", "timestamp", "time"]:
            return EntityType.TIME_COLUMN
        # Also check by column name patterns
        name_lower = entry.name.lower()
        time_keywords = ["date", "datetime", "timestamp", "time", "created", "updated", "modified", "period"]
        if any(kw in name_lower for kw in time_keywords):
            return EntityType.TIME_COLUMN
        return entry.entity_type

    def get_tables(self) -> List[str]:
        """Get list of all table names in catalog."""
        return [e.name for e in self.catalog if e.entity_type == EntityType.TABLE]

    def get_columns(self, table: Optional[str] = None) -> List[str]:
        """Get list of column names, optionally filtered by table."""
        columns = []
        for entry in self.catalog:
            if entry.entity_type != EntityType.TABLE:
                if table is None or entry.table == table:
                    columns.append(entry.name)
        return columns

    def get_metrics(self) -> List[str]:
        """Get list of metric column names."""
        return [e.name for e in self.catalog if e.is_metric]

    def get_dimensions(self) -> List[str]:
        """Get list of dimension column names."""
        return [e.name for e in self.catalog if e.is_dimension]

    def suggest_columns(self, partial: str, limit: int = 5) -> List[str]:
        """
        Suggest column names based on partial input.

        Args:
            partial: Partial column name.
            limit: Maximum suggestions.

        Returns:
            List of suggested column names.
        """
        if not self.enable_fuzzy or not self._all_names:
            # Fallback to prefix matching
            suggestions = []
            partial_lower = partial.lower()
            for name, entry in self._all_names:
                if name.startswith(partial_lower):
                    suggestions.append(entry.name)
            return suggestions[:limit]

        names = [name for name, _ in self._all_names]
        results = process.extract(
            partial.lower(),
            names,
            scorer=fuzz.WRatio,
            limit=limit,
        )

        suggestions = []
        seen = set()
        for matched_name, score, idx in results:
            _, entry = self._all_names[idx]
            if entry.name not in seen:
                suggestions.append(entry.name)
                seen.add(entry.name)

        return suggestions
