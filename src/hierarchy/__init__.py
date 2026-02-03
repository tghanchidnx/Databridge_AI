# DataBridge AI - Hierarchy Builder Module
"""
Enterprise-grade hierarchy management for building, managing, and deploying
multi-level data hierarchies with source mappings, formulas, and database deployment.

Includes Flexible Import System supporting four tiers of input complexity:
- Tier 1: Ultra-simple (2-3 columns: source_value, group_name)
- Tier 2: Basic (5-7 columns with parent names)
- Tier 3: Standard (10-12 columns with explicit IDs)
- Tier 4: Enterprise (28+ columns with LEVEL_1-10)
"""

from .types import (
    FormatTier,
    InputFormat,
    ProjectDefaults,
    # Property system
    PropertyCategory,
    AggregationType,
    MeasureType,
    TimeBalance,
    FilterBehavior,
    HierarchyProperty,
    DimensionProperties,
    FactProperties,
    FilterProperties,
    DisplayProperties,
    PropertyTemplate,
    # Core types
    HierarchyFlags,
    SourceMappingFlags,
    SourceMapping,
    FormulaRule,
    FormulaGroup,
    FilterCondition,
    FilterGroup,
    HierarchyLevel,
    SmartHierarchy,
    HierarchyProject,
)

from .service import HierarchyService

# Flexible import (optional, may not always be available)
try:
    from .flexible_import import (
        FlexibleImportService,
        FormatDetector,
    )
    FLEXIBLE_IMPORT_AVAILABLE = True
except ImportError:
    FLEXIBLE_IMPORT_AVAILABLE = False

__all__ = [
    # Format types
    "FormatTier",
    "InputFormat",
    "ProjectDefaults",
    # Property system
    "PropertyCategory",
    "AggregationType",
    "MeasureType",
    "TimeBalance",
    "FilterBehavior",
    "HierarchyProperty",
    "DimensionProperties",
    "FactProperties",
    "FilterProperties",
    "DisplayProperties",
    "PropertyTemplate",
    # Core types
    "HierarchyFlags",
    "SourceMappingFlags",
    "SourceMapping",
    "FormulaRule",
    "FormulaGroup",
    "FilterCondition",
    "FilterGroup",
    "HierarchyLevel",
    "SmartHierarchy",
    "HierarchyProject",
    # Services
    "HierarchyService",
    "FlexibleImportService",
    "FormatDetector",
    "FLEXIBLE_IMPORT_AVAILABLE",
]
