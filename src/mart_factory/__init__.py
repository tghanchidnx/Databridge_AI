"""
Mart Factory Module.

Hierarchy-Driven Data Mart Factory for automated pipeline generation:
- 4-object pipeline: VW_1 → DT_2 → DT_3A → DT_3
- 7 configuration variables for parameterization
- AI-powered hierarchy discovery
- 5-level formula precedence engine

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
]
