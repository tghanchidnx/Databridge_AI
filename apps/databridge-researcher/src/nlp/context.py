"""
Conversation Context for Multi-Turn NL-to-SQL Queries.

Tracks query history, extracted entities, and resolved references
to support contextual follow-up questions.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Set
from collections import deque
import re

from .entity import Entity, EntityType


@dataclass
class QueryTurn:
    """Represents a single turn in a conversation."""

    query_text: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_query: Optional[str] = None
    entities: List[Entity] = field(default_factory=list)
    intent_type: Optional[str] = None
    table: Optional[str] = None
    success: bool = True
    sql: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "query_text": self.query_text,
            "timestamp": self.timestamp.isoformat(),
            "resolved_query": self.resolved_query,
            "entities": [e.to_dict() for e in self.entities],
            "intent_type": self.intent_type,
            "table": self.table,
            "success": self.success,
        }


@dataclass
class ConversationContext:
    """
    Tracks conversation context for multi-turn NL-to-SQL queries.

    Features:
    - Query history with sliding window
    - Entity persistence across turns
    - Pronoun and reference resolution
    - Context-aware disambiguation

    Usage:
        context = ConversationContext(max_turns=10)

        # First query
        context.add_turn(QueryTurn(
            query_text="Show total revenue by region",
            entities=[...],
            table="sales"
        ))

        # Follow-up query with reference
        resolved = context.resolve_references("Now show it by month")
        # Returns: "Show total revenue by month"
    """

    max_turns: int = 10
    session_id: Optional[str] = None

    def __post_init__(self):
        """Initialize internal state."""
        self._turns: deque[QueryTurn] = deque(maxlen=self.max_turns)
        self._entity_cache: Dict[str, Entity] = {}
        self._last_table: Optional[str] = None
        self._last_metrics: List[Entity] = []
        self._last_dimensions: List[Entity] = []
        self._last_filters: Dict[str, Any] = {}

    @property
    def history(self) -> List[QueryTurn]:
        """Get query history as a list."""
        return list(self._turns)

    @property
    def turn_count(self) -> int:
        """Get number of turns in history."""
        return len(self._turns)

    @property
    def last_turn(self) -> Optional[QueryTurn]:
        """Get the most recent turn."""
        return self._turns[-1] if self._turns else None

    @property
    def last_table(self) -> Optional[str]:
        """Get the table from the most recent query."""
        return self._last_table

    @property
    def last_metrics(self) -> List[Entity]:
        """Get metrics from the most recent query."""
        return self._last_metrics

    @property
    def last_dimensions(self) -> List[Entity]:
        """Get dimensions from the most recent query."""
        return self._last_dimensions

    def add_turn(self, turn: QueryTurn) -> None:
        """
        Add a new turn to the conversation history.

        Updates entity cache and context state.

        Args:
            turn: The query turn to add.
        """
        self._turns.append(turn)

        # Update context state
        if turn.table:
            self._last_table = turn.table

        # Categorize and cache entities
        self._last_metrics = []
        self._last_dimensions = []

        for entity in turn.entities:
            # Add to cache by name
            self._entity_cache[entity.name.lower()] = entity

            # Categorize
            if entity.entity_type == EntityType.METRIC:
                self._last_metrics.append(entity)
            elif entity.entity_type == EntityType.DIMENSION:
                self._last_dimensions.append(entity)

    def get_cached_entity(self, name: str) -> Optional[Entity]:
        """
        Get a cached entity by name.

        Args:
            name: Entity name to look up.

        Returns:
            Cached entity or None.
        """
        return self._entity_cache.get(name.lower())

    def get_all_cached_entities(self) -> List[Entity]:
        """Get all cached entities from conversation."""
        return list(self._entity_cache.values())

    def resolve_references(self, query: str) -> str:
        """
        Resolve pronouns and references in a query using context.

        Handles:
        - "it" → last metric/dimension
        - "them" → last multiple entities
        - "that" → last entity
        - "same" → last dimension
        - "those" → last filters or entities

        Args:
            query: Query text with potential references.

        Returns:
            Query with references resolved.
        """
        if not self._turns:
            return query

        resolved = query
        query_lower = query.lower()

        # Reference patterns to resolve
        patterns = {
            # "show it by month" → "show [last_metric] by month"
            r'\bit\b': self._resolve_it,
            # "break them down by" → "break [entities] down by"
            r'\bthem\b': self._resolve_them,
            # "and that for last year" → "and [last_entity] for last year"
            r'\bthat\b': self._resolve_that,
            # "the same by category" → "[last_metric] by category"
            r'\bthe same\b': self._resolve_same,
            # "filter those by" → "filter [last_results] by"
            r'\bthose\b': self._resolve_those,
            # "also include" → carry forward entities
            r'\balso\b': self._resolve_also,
        }

        for pattern, resolver in patterns.items():
            if re.search(pattern, query_lower):
                resolved = resolver(resolved, pattern)

        return resolved

    def _resolve_it(self, query: str, pattern: str) -> str:
        """Resolve 'it' to last metric or primary entity."""
        if self._last_metrics:
            replacement = self._last_metrics[0].name
        elif self._last_dimensions:
            replacement = self._last_dimensions[0].name
        else:
            return query

        return re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    def _resolve_them(self, query: str, pattern: str) -> str:
        """Resolve 'them' to last multiple entities."""
        entities = self._last_metrics or self._last_dimensions
        if not entities:
            return query

        names = [e.name for e in entities[:3]]  # Limit to 3
        replacement = ", ".join(names)
        return re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    def _resolve_that(self, query: str, pattern: str) -> str:
        """Resolve 'that' to last entity mentioned."""
        if self.last_turn and self.last_turn.entities:
            replacement = self.last_turn.entities[0].name
            return re.sub(pattern, replacement, query, flags=re.IGNORECASE)
        return query

    def _resolve_same(self, query: str, pattern: str) -> str:
        """Resolve 'the same' to last metric."""
        if self._last_metrics:
            replacement = self._last_metrics[0].name
            return re.sub(pattern, replacement, query, flags=re.IGNORECASE)
        return query

    def _resolve_those(self, query: str, pattern: str) -> str:
        """Resolve 'those' to last dimension values or entities."""
        if self._last_dimensions:
            replacement = self._last_dimensions[0].name
            return re.sub(pattern, replacement, query, flags=re.IGNORECASE)
        return query

    def _resolve_also(self, query: str, pattern: str) -> str:
        """Handle 'also' by indicating continuation."""
        # 'also' doesn't need replacement, just signals context continuation
        return query

    def infer_missing_context(self, entities: List[Entity]) -> Dict[str, Any]:
        """
        Infer missing context from conversation history.

        If a query has dimensions but no metrics, try to inherit metrics
        from previous turns, and vice versa.

        Args:
            entities: Entities extracted from current query.

        Returns:
            Dictionary with inferred table, metrics, dimensions.
        """
        inferred = {
            "table": None,
            "metrics": [],
            "dimensions": [],
            "time_filter": None,
        }

        # Categorize current entities
        current_metrics = [e for e in entities if e.entity_type == EntityType.METRIC]
        current_dimensions = [e for e in entities if e.entity_type == EntityType.DIMENSION]
        current_tables = [e for e in entities if e.entity_type == EntityType.TABLE]

        # Inherit table if not specified
        if not current_tables and self._last_table:
            inferred["table"] = self._last_table

        # If we have dimensions but no metrics, inherit last metrics
        if current_dimensions and not current_metrics and self._last_metrics:
            inferred["metrics"] = self._last_metrics

        # If we have metrics but no dimensions, inherit last dimensions
        if current_metrics and not current_dimensions and self._last_dimensions:
            inferred["dimensions"] = self._last_dimensions

        # Carry forward time filters if not explicitly changed
        if self._last_filters:
            inferred["time_filter"] = self._last_filters.get("time")

        return inferred

    def update_filters(self, filters: Dict[str, Any]) -> None:
        """
        Update tracked filters from a query.

        Args:
            filters: Filters to track.
        """
        self._last_filters.update(filters)

    def clear(self) -> None:
        """Clear all conversation context."""
        self._turns.clear()
        self._entity_cache.clear()
        self._last_table = None
        self._last_metrics = []
        self._last_dimensions = []
        self._last_filters = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        return {
            "session_id": self.session_id,
            "turn_count": self.turn_count,
            "history": [t.to_dict() for t in self._turns],
            "last_table": self._last_table,
            "cached_entities": list(self._entity_cache.keys()),
        }


# Singleton for default conversation context
_default_context: Optional[ConversationContext] = None


def get_conversation_context() -> ConversationContext:
    """Get the default conversation context singleton."""
    global _default_context
    if _default_context is None:
        _default_context = ConversationContext()
    return _default_context


def reset_conversation_context() -> None:
    """Reset the default conversation context."""
    global _default_context
    _default_context = None


def create_conversation_context(
    session_id: Optional[str] = None,
    max_turns: int = 10,
) -> ConversationContext:
    """
    Create a new conversation context.

    Args:
        session_id: Optional session identifier.
        max_turns: Maximum turns to retain.

    Returns:
        New ConversationContext instance.
    """
    return ConversationContext(max_turns=max_turns, session_id=session_id)
