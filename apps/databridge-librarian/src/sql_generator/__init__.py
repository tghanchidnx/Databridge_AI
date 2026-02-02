"""
SQL Generator Module for DataBridge AI Librarian.

This module provides services for:
- Pattern detection (fact/dimension/bridge classification)
- View generation (VW_1 tier)
- Schema registry and validation
- Data lineage tracking with NetworkX

Architecture:
    TBL_0 (Source) -> VW_1 (Views) -> [Researcher: DT_2 -> DT_3A -> DT_3]
"""

# Import models (always available)
from .models import (
    GeneratedView,
    SchemaRegistryEntry,
    DataLineageEdge,
    PatternType,
    ColumnType,
    ObjectType,
    TransformationType,
    SQLDialect,
    DetectedPattern,
    ColumnClassification,
)

# Import services
from .pattern_detector import PatternDetectorService, get_pattern_detector
from .schema_registry import SchemaRegistryService, get_schema_registry
from .view_generator import ViewGeneratorService, get_view_generator

# Lineage tracker requires NetworkX
try:
    from .lineage_tracker import LineageTrackerService, get_lineage_tracker
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    LineageTrackerService = None
    get_lineage_tracker = None

__all__ = [
    # Models
    "GeneratedView",
    "SchemaRegistryEntry",
    "DataLineageEdge",
    "PatternType",
    "ColumnType",
    "ObjectType",
    "TransformationType",
    "SQLDialect",
    "DetectedPattern",
    "ColumnClassification",
    # Services
    "PatternDetectorService",
    "get_pattern_detector",
    "SchemaRegistryService",
    "get_schema_registry",
    "ViewGeneratorService",
    "get_view_generator",
    # Lineage (optional)
    "LineageTrackerService",
    "get_lineage_tracker",
    "HAS_NETWORKX",
]
