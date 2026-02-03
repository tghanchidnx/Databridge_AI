"""
Smart Recommendation Engine for DataBridge AI.

Provides context-aware recommendations when importing CSV files by leveraging:
- Skills (domain expertise)
- Knowledge Base (client-specific patterns)
- Templates (industry hierarchies)
- LLM validation layer
"""

from .recommendation_engine import (
    RecommendationEngine,
    Recommendation,
    RecommendationContext,
    DataProfile,
)

__all__ = [
    "RecommendationEngine",
    "Recommendation",
    "RecommendationContext",
    "DataProfile",
]
