"""
Source Intelligence module for DataBridge AI Librarian.

Provides capabilities for:
- Managing inferred canonical data models
- Interactive refinement of entity relationships
- Column mapping and consolidation
- Entity classification and validation
- Automated discovery orchestration
"""

from .models import (
    SourceColumn,
    SourceTable,
    SourceEntity,
    SourceRelationship,
    CanonicalModel,
    EntityType,
    RelationshipType,
)
from .store import SourceModelStore
from .analyzer import SourceAnalyzer
from .discovery import (
    SourceDiscoveryService,
    DiscoveryConfig,
    DiscoveryResult,
    DiscoveryProgress,
    DiscoveryPhase,
)

__all__ = [
    # Data models
    "SourceColumn",
    "SourceTable",
    "SourceEntity",
    "SourceRelationship",
    "CanonicalModel",
    "EntityType",
    "RelationshipType",
    # Services
    "SourceModelStore",
    "SourceAnalyzer",
    # Discovery
    "SourceDiscoveryService",
    "DiscoveryConfig",
    "DiscoveryResult",
    "DiscoveryProgress",
    "DiscoveryPhase",
]
