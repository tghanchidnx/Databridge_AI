"""
Project Generation module for DataBridge Discovery.

This module provides tools for generating:
- Complete Librarian projects
- VW_1 tier views
- dbt models
- DDL scripts
- Deployment packages
"""

from databridge_discovery.generation.project_generator import (
    ProjectGenerator,
    GeneratedProject,
    ProjectConfig,
)
from databridge_discovery.generation.view_generator import (
    ViewGenerator,
    GeneratedView,
)
from databridge_discovery.generation.dbt_generator import (
    DbtGenerator,
    DbtModel,
    DbtProject,
)
from databridge_discovery.generation.sql_generator import (
    SQLGenerator,
    GeneratedDDL,
)

__all__ = [
    # Project Generator
    "ProjectGenerator",
    "GeneratedProject",
    "ProjectConfig",
    # View Generator
    "ViewGenerator",
    "GeneratedView",
    # dbt Generator
    "DbtGenerator",
    "DbtModel",
    "DbtProject",
    # SQL Generator
    "SQLGenerator",
    "GeneratedDDL",
]
