"""
dbt Integration Module.

Generates dbt projects from DataBridge hierarchies:
- Project scaffolding (dbt_project.yml, profiles.yml)
- SQL model generation (staging, intermediate, marts)
- Source definitions (sources.yml)
- Schema documentation (schema.yml)
- Metrics from formula groups
- CI/CD pipelines (GitHub Actions, GitLab CI, Azure DevOps)

Components:
- DbtProjectGenerator: Project scaffolding
- DbtModelGenerator: SQL model generation
- DbtSourceGenerator: Source definitions
- DbtMetricsGenerator: Metrics from formulas
- CiCdGenerator: CI/CD pipeline generation
"""

from .types import (
    # Enums
    DbtMaterialization,
    DbtModelType,
    CiCdPlatform,
    # Project configuration
    DbtProjectConfig,
    DbtProject,
    # Sources
    DbtSource,
    DbtSourceTable,
    DbtColumn,
    # Models
    DbtModelConfig,
    # Metrics and tests
    DbtMetric,
    DbtTest,
    # CI/CD
    CiCdConfig,
    # Validation
    ValidationResult,
)

from .project_generator import DbtProjectGenerator
from .model_generator import DbtModelGenerator
from .source_generator import DbtSourceGenerator, DbtMetricsGenerator
from .cicd_generator import CiCdGenerator
from .mcp_tools import register_dbt_tools

__all__ = [
    # Types - Enums
    "DbtMaterialization",
    "DbtModelType",
    "CiCdPlatform",
    # Types - Configuration
    "DbtProjectConfig",
    "DbtProject",
    "DbtSource",
    "DbtSourceTable",
    "DbtColumn",
    "DbtModelConfig",
    "DbtMetric",
    "DbtTest",
    "CiCdConfig",
    "ValidationResult",
    # Generators
    "DbtProjectGenerator",
    "DbtModelGenerator",
    "DbtSourceGenerator",
    "DbtMetricsGenerator",
    "CiCdGenerator",
    # MCP
    "register_dbt_tools",
]
