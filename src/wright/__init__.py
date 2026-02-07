"""
Wright Module - Hierarchy-Driven Dimension & Fact Builder.

Named after the Wright Brothers, pioneers of building and flying,
this module builds dimensions and facts from hierarchies.

Capabilities:
- 4-object pipeline: VW_1 (Translation) → DT_2 (Granularity) → DT_3A (Pre-Agg) → DT_3 (Data Mart)
- 7 configuration variables for complete parameterization
- AI-powered hierarchy discovery via Cortex
- 5-level formula precedence engine for calculations

Pipeline Objects:
- VW_1: Translation View - CASE on ID_SOURCE to route to dimension columns
- DT_2: Granularity Table - UNPIVOT + dynamic column mapping + exclusions
- DT_3A: Pre-Aggregation Fact - UNION ALL branches per join pattern, SUM by FK
- DT_3: Data Mart - 5-level formula cascade, surrogate keys, level backfill

Configuration Variables:
1. JOIN_PATTERNS[] - Dynamic UNION ALL branches
2. DYNAMIC_COLUMN_MAP{} - ID_SOURCE → physical column
3. ACCOUNT_SEGMENT - GROSS/NET filter
4. MEASURE_PREFIX - Column name prefix
5. HAS_SIGN_CHANGE - Sign flip flag
6. HAS_EXCLUSIONS - NOT IN subquery flag
7. HAS_GROUP_FILTER - Multi-round filter flag

Components:
- MartConfigGenerator: Create and manage mart configurations
- MartPipelineGenerator: Generate 4-object DDL pipeline
- FormulaPrecedenceEngine: 5-level formula cascade
- CortexDiscoveryAgent: AI-powered hierarchy analysis
"""

from .types import (
    # Enums
    PipelineLayer,
    ObjectType,
    FormulaLogic,
    # Core types
    JoinPattern,
    DynamicColumnMapping,
    MartConfig,
    PipelineObject,
    FormulaPrecedence,
    DataQualityIssue,
    DiscoveryResult,
    PipelineValidationResult,
)

from .config_generator import MartConfigGenerator
from .pipeline_generator import MartPipelineGenerator
from .formula_engine import (
    FormulaPrecedenceEngine,
    create_standard_los_formulas,
)
from .cortex_discovery import CortexDiscoveryAgent
from .mcp_tools import register_mart_factory_tools

# Alias for cleaner imports
register_wright_tools = register_mart_factory_tools

__all__ = [
    # Enums
    "PipelineLayer",
    "ObjectType",
    "FormulaLogic",
    # Core types
    "JoinPattern",
    "DynamicColumnMapping",
    "MartConfig",
    "PipelineObject",
    "FormulaPrecedence",
    "DataQualityIssue",
    "DiscoveryResult",
    "PipelineValidationResult",
    # Generators
    "MartConfigGenerator",
    "MartPipelineGenerator",
    "FormulaPrecedenceEngine",
    "create_standard_los_formulas",
    "CortexDiscoveryAgent",
    # MCP
    "register_mart_factory_tools",
    "register_wright_tools",
]
