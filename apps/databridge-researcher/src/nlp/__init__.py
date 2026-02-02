"""NLP module for DataBridge AI Researcher Analytics Engine."""

from .intent import IntentClassifier, Intent, IntentType
from .entity import EntityExtractor, Entity, EntityType
from .nl_to_sql import NLToSQLEngine, NLQueryResult
from .context import (
    ConversationContext,
    QueryTurn,
    get_conversation_context,
    reset_conversation_context,
    create_conversation_context,
)

__all__ = [
    # Intent classification
    "IntentClassifier",
    "Intent",
    "IntentType",
    # Entity extraction
    "EntityExtractor",
    "Entity",
    "EntityType",
    # NL-to-SQL
    "NLToSQLEngine",
    "NLQueryResult",
    # Conversation context
    "ConversationContext",
    "QueryTurn",
    "get_conversation_context",
    "reset_conversation_context",
    "create_conversation_context",
]
