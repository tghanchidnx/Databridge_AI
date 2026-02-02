"""
Unit tests for conversation context module.

Tests multi-turn conversation tracking, reference resolution,
and entity inheritance.
"""

import pytest
from datetime import datetime, timezone

from src.nlp.context import (
    ConversationContext,
    QueryTurn,
    get_conversation_context,
    reset_conversation_context,
    create_conversation_context,
)
from src.nlp.entity import Entity, EntityType


class TestQueryTurn:
    """Tests for QueryTurn dataclass."""

    def test_basic_creation(self):
        """Test creating a basic query turn."""
        turn = QueryTurn(query_text="Show total revenue")

        assert turn.query_text == "Show total revenue"
        assert turn.timestamp is not None
        assert turn.resolved_query is None
        assert turn.entities == []
        assert turn.success is True

    def test_with_entities(self):
        """Test creating a turn with entities."""
        entities = [
            Entity(entity_type=EntityType.METRIC, name="revenue", original_text="revenue", confidence=0.9),
            Entity(entity_type=EntityType.DIMENSION, name="region", original_text="region", confidence=0.85),
        ]

        turn = QueryTurn(
            query_text="Show revenue by region",
            entities=entities,
            table="sales",
            intent_type="aggregation",
        )

        assert len(turn.entities) == 2
        assert turn.table == "sales"
        assert turn.intent_type == "aggregation"

    def test_to_dict(self):
        """Test serialization to dictionary."""
        turn = QueryTurn(
            query_text="Test query",
            entities=[Entity(entity_type=EntityType.METRIC, name="test", original_text="test", confidence=0.9)],
            table="test_table",
        )

        data = turn.to_dict()

        assert data["query_text"] == "Test query"
        assert data["table"] == "test_table"
        assert "timestamp" in data
        assert len(data["entities"]) == 1


class TestConversationContext:
    """Tests for ConversationContext class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = ConversationContext(max_turns=5)

    def test_initialization(self):
        """Test context initialization."""
        assert self.context.turn_count == 0
        assert self.context.last_turn is None
        assert self.context.last_table is None
        assert self.context.last_metrics == []
        assert self.context.last_dimensions == []

    def test_add_turn(self):
        """Test adding a turn to context."""
        turn = QueryTurn(
            query_text="Show revenue",
            entities=[Entity(entity_type=EntityType.METRIC, name="revenue", original_text="revenue", confidence=0.9)],
            table="sales",
        )

        self.context.add_turn(turn)

        assert self.context.turn_count == 1
        assert self.context.last_turn == turn
        assert self.context.last_table == "sales"

    def test_entity_caching(self):
        """Test that entities are cached across turns."""
        self.context.add_turn(QueryTurn(
            query_text="Show revenue",
            entities=[
                Entity(entity_type=EntityType.METRIC, name="revenue", original_text="revenue", confidence=0.9),
                Entity(entity_type=EntityType.DIMENSION, name="region", original_text="region", confidence=0.85),
            ],
            table="sales",
        ))

        # Check cached entities
        revenue = self.context.get_cached_entity("revenue")
        assert revenue is not None
        assert revenue.entity_type == EntityType.METRIC

        region = self.context.get_cached_entity("region")
        assert region is not None
        assert region.entity_type == EntityType.DIMENSION

        # Case insensitive
        assert self.context.get_cached_entity("REVENUE") is not None

    def test_max_turns_limit(self):
        """Test that max turns limit is enforced."""
        context = ConversationContext(max_turns=3)

        for i in range(5):
            context.add_turn(QueryTurn(query_text=f"Query {i}"))

        assert context.turn_count == 3
        # Should have turns 2, 3, 4 (oldest dropped)
        assert context.history[0].query_text == "Query 2"

    def test_clear(self):
        """Test clearing context."""
        self.context.add_turn(QueryTurn(
            query_text="Test",
            entities=[Entity(entity_type=EntityType.METRIC, name="test", original_text="test", confidence=0.9)],
            table="sales",
        ))

        self.context.clear()

        assert self.context.turn_count == 0
        assert self.context.last_table is None
        assert self.context.get_cached_entity("test") is None


class TestReferenceResolution:
    """Tests for pronoun and reference resolution."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures with sample context."""
        self.context = ConversationContext()

        # Add a turn with entities
        self.context.add_turn(QueryTurn(
            query_text="Show total revenue by region",
            entities=[
                Entity(entity_type=EntityType.METRIC, name="revenue", original_text="revenue", confidence=0.9),
                Entity(entity_type=EntityType.DIMENSION, name="region", original_text="region", confidence=0.85),
            ],
            table="sales",
        ))

    def test_resolve_it(self):
        """Test resolving 'it' to last metric."""
        resolved = self.context.resolve_references("Show it by month")

        assert "revenue" in resolved.lower()
        assert "it" not in resolved.lower()

    def test_resolve_them(self):
        """Test resolving 'them' to multiple entities."""
        # Add turn with multiple metrics
        self.context.add_turn(QueryTurn(
            query_text="Show revenue and profit",
            entities=[
                Entity(entity_type=EntityType.METRIC, name="revenue", original_text="revenue", confidence=0.9),
                Entity(entity_type=EntityType.METRIC, name="profit", original_text="profit", confidence=0.9),
            ],
        ))

        resolved = self.context.resolve_references("Break them down by category")

        assert "revenue" in resolved.lower() or "profit" in resolved.lower()

    def test_resolve_that(self):
        """Test resolving 'that' to last entity."""
        resolved = self.context.resolve_references("Filter that by year")

        # Should replace 'that' with first entity from last turn
        assert "that" not in resolved.lower() or "revenue" in resolved.lower()

    def test_resolve_same(self):
        """Test resolving 'the same' to last metric."""
        resolved = self.context.resolve_references("Show the same by category")

        assert "the same" not in resolved.lower()
        assert "revenue" in resolved.lower()

    def test_no_context_no_resolution(self):
        """Test that empty context doesn't modify query."""
        empty_context = ConversationContext()

        query = "Show it by month"
        resolved = empty_context.resolve_references(query)

        assert resolved == query

    def test_case_insensitive_resolution(self):
        """Test that resolution is case insensitive."""
        resolved1 = self.context.resolve_references("Show IT by month")
        resolved2 = self.context.resolve_references("show it by month")

        assert "it" not in resolved1.lower()
        assert "it" not in resolved2.lower()


class TestContextInference:
    """Tests for inferring missing context."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = ConversationContext()

        # Add a complete turn
        self.context.add_turn(QueryTurn(
            query_text="Show revenue by region",
            entities=[
                Entity(entity_type=EntityType.METRIC, name="revenue", original_text="revenue", confidence=0.9),
                Entity(entity_type=EntityType.DIMENSION, name="region", original_text="region", confidence=0.9),
            ],
            table="sales",
        ))

    def test_inherit_metrics(self):
        """Test inheriting metrics when only dimensions specified."""
        # New query with only dimension
        new_entities = [
            Entity(entity_type=EntityType.DIMENSION, name="category", original_text="category", confidence=0.9),
        ]

        inferred = self.context.infer_missing_context(new_entities)

        # Should inherit revenue as metric
        assert len(inferred["metrics"]) == 1
        assert inferred["metrics"][0].name == "revenue"

    def test_inherit_dimensions(self):
        """Test inheriting dimensions when only metrics specified."""
        # New query with only metric
        new_entities = [
            Entity(entity_type=EntityType.METRIC, name="profit", original_text="profit", confidence=0.9),
        ]

        inferred = self.context.infer_missing_context(new_entities)

        # Should inherit region as dimension
        assert len(inferred["dimensions"]) == 1
        assert inferred["dimensions"][0].name == "region"

    def test_inherit_table(self):
        """Test inheriting table when not specified."""
        new_entities = [
            Entity(entity_type=EntityType.METRIC, name="amount", original_text="amount", confidence=0.9),
        ]

        inferred = self.context.infer_missing_context(new_entities)

        assert inferred["table"] == "sales"

    def test_no_inheritance_when_complete(self):
        """Test that complete queries don't inherit."""
        # New query with both metric and dimension
        new_entities = [
            Entity(entity_type=EntityType.METRIC, name="profit", original_text="profit", confidence=0.9),
            Entity(entity_type=EntityType.DIMENSION, name="category", original_text="category", confidence=0.9),
        ]

        inferred = self.context.infer_missing_context(new_entities)

        # Should not inherit anything
        assert inferred["metrics"] == []
        assert inferred["dimensions"] == []


class TestFilterTracking:
    """Tests for filter tracking across turns."""

    def test_update_filters(self):
        """Test updating tracked filters."""
        context = ConversationContext()

        context.update_filters({"time": {"year": 2024}})
        context.update_filters({"region": "APAC"})

        # Filters should be merged
        inferred = context.infer_missing_context([])
        # Time filter should be tracked
        assert context._last_filters.get("time") == {"year": 2024}
        assert context._last_filters.get("region") == "APAC"


class TestSingletonBehavior:
    """Tests for singleton context management."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        reset_conversation_context()

    def test_get_default_context(self):
        """Test getting default singleton context."""
        context1 = get_conversation_context()
        context2 = get_conversation_context()

        assert context1 is context2

    def test_reset_context(self):
        """Test resetting singleton context."""
        context1 = get_conversation_context()
        context1.add_turn(QueryTurn(query_text="Test"))

        reset_conversation_context()
        context2 = get_conversation_context()

        assert context1 is not context2
        assert context2.turn_count == 0

    def test_create_independent_context(self):
        """Test creating independent contexts."""
        context1 = create_conversation_context(session_id="session1")
        context2 = create_conversation_context(session_id="session2")

        assert context1 is not context2
        assert context1.session_id == "session1"
        assert context2.session_id == "session2"


class TestContextSerialization:
    """Tests for context serialization."""

    def test_to_dict(self):
        """Test converting context to dictionary."""
        context = ConversationContext(session_id="test-session")

        context.add_turn(QueryTurn(
            query_text="Test query",
            entities=[Entity(entity_type=EntityType.METRIC, name="test", original_text="test", confidence=0.9)],
            table="test_table",
        ))

        data = context.to_dict()

        assert data["session_id"] == "test-session"
        assert data["turn_count"] == 1
        assert len(data["history"]) == 1
        assert data["last_table"] == "test_table"
        assert "test" in data["cached_entities"]
