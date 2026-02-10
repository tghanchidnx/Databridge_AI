"""DataShield - Confidential Data Scrambling Module

Scrambles data before DataBridge reads it, preserving patterns, metadata,
cardinality, distributions, and referential integrity so data warehouse
design works normally while confidential values remain unreadable.

Phase 33 of DataBridge AI. Requires Pro license.

Six scrambling strategies:
- format_preserving_hash: Strings (codes, IDs, invoice numbers)
- numeric_scaling: Measures (amounts, quantities, rates)
- synthetic_substitution: Names, places (map to synthetic lookup)
- date_shift: Dates (shift by key-derived offset)
- pattern_preserving: Regex-structured values (phone, SSN format)
- passthrough: No scrambling (safe columns)

MCP Tools (12):
- create_shield_project: Create project with encryption key
- list_shield_projects: List all projects
- get_shield_project: Get project details
- delete_shield_project: Remove project and key
- auto_classify_table: Auto-detect column classifications
- add_table_shield: Add table shield config
- remove_table_shield: Remove table from project
- preview_scrambled_data: Before/after preview
- generate_shield_ddl: Generate Snowflake DDL
- deploy_shield_to_snowflake: Execute DDL on Snowflake
- shield_local_file: Scramble CSV/JSON file
- get_shield_status: Status and key health
"""

from .types import (
    ScrambleStrategy,
    ColumnClassification,
    ShieldScope,
    ColumnRule,
    TableShieldConfig,
    ShieldProject,
    CLASSIFICATION_STRATEGY_MAP,
)
from .engine import ScrambleEngine
from .key_manager import KeyManager
from .classifier import auto_classify_columns
from .service import ShieldService
from .snowflake_generator import generate_full_ddl, generate_udfs
from .interceptor import DataShieldInterceptor
from .mcp_tools import register_datashield_tools

__all__ = [
    # Enums
    "ScrambleStrategy",
    "ColumnClassification",
    "ShieldScope",
    # Models
    "ColumnRule",
    "TableShieldConfig",
    "ShieldProject",
    "CLASSIFICATION_STRATEGY_MAP",
    # Core
    "ScrambleEngine",
    "KeyManager",
    "ShieldService",
    "DataShieldInterceptor",
    # Functions
    "auto_classify_columns",
    "generate_full_ddl",
    "generate_udfs",
    # Registration
    "register_datashield_tools",
]
