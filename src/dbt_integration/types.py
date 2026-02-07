"""
Configuration types for dbt Integration.

Defines Pydantic models for:
- dbt project configuration
- Model definitions
- Source definitions
- CI/CD pipeline configuration
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class DbtMaterialization(str, Enum):
    """dbt materialization strategies."""
    VIEW = "view"
    TABLE = "table"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class DbtModelType(str, Enum):
    """Types of dbt models."""
    STAGING = "staging"
    INTERMEDIATE = "intermediate"
    MART = "mart"
    DIM = "dimension"
    FACT = "fact"


class CiCdPlatform(str, Enum):
    """Supported CI/CD platforms."""
    GITHUB_ACTIONS = "github_actions"
    GITLAB_CI = "gitlab_ci"
    AZURE_DEVOPS = "azure_devops"


class DbtProjectConfig(BaseModel):
    """Configuration for a dbt project."""
    name: str = Field(..., description="Project name (lowercase, underscores)")
    version: str = Field(default="1.0.0", description="Project version")
    profile: str = Field(..., description="Profile name for connection")

    # Target configuration
    target_database: Optional[str] = None
    target_schema: Optional[str] = None

    # Model paths
    model_paths: List[str] = Field(default_factory=lambda: ["models"])
    seed_paths: List[str] = Field(default_factory=lambda: ["seeds"])
    test_paths: List[str] = Field(default_factory=lambda: ["tests"])
    analysis_paths: List[str] = Field(default_factory=lambda: ["analyses"])
    macro_paths: List[str] = Field(default_factory=lambda: ["macros"])

    # Default configurations
    default_materialization: DbtMaterialization = DbtMaterialization.VIEW

    # Vars
    vars: Dict[str, Any] = Field(default_factory=dict)

    # Source hierarchy project
    hierarchy_project_id: Optional[str] = None

    model_config = {"extra": "allow"}


class DbtSourceTable(BaseModel):
    """A source table definition."""
    name: str = Field(..., description="Table name in dbt")
    description: Optional[str] = None

    # Database reference
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    identifier: Optional[str] = Field(None, description="Actual table name if different")

    # Columns
    columns: List["DbtColumn"] = Field(default_factory=list)

    # Freshness
    loaded_at_field: Optional[str] = None
    freshness_warn_after: Optional[Dict[str, int]] = None
    freshness_error_after: Optional[Dict[str, int]] = None

    # Tags and meta
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow", "populate_by_name": True}


class DbtColumn(BaseModel):
    """A column definition for sources or models."""
    name: str
    description: Optional[str] = None
    data_type: Optional[str] = None

    # Tests
    tests: List[str] = Field(default_factory=list)

    # Tags and meta
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class DbtSource(BaseModel):
    """A dbt source definition."""
    name: str = Field(..., description="Source name")
    description: Optional[str] = None

    # Database reference
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")

    # Tables
    tables: List[DbtSourceTable] = Field(default_factory=list)

    # Tags and meta
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow", "populate_by_name": True}


class DbtModelConfig(BaseModel):
    """Configuration for a dbt model."""
    name: str = Field(..., description="Model name")
    description: Optional[str] = None
    model_type: DbtModelType = DbtModelType.STAGING

    # Materialization
    materialized: DbtMaterialization = DbtMaterialization.VIEW

    # Schema
    schema_suffix: Optional[str] = None

    # Columns
    columns: List[DbtColumn] = Field(default_factory=list)

    # SQL template components
    sql_select: Optional[str] = None
    sql_from: Optional[str] = None
    sql_where: Optional[str] = None
    sql_cte: Optional[str] = None

    # Source reference
    source_name: Optional[str] = None
    source_table: Optional[str] = None

    # Ref dependencies
    refs: List[str] = Field(default_factory=list)

    # Tags and meta
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    # Tests
    tests: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class DbtMetric(BaseModel):
    """A dbt metric definition."""
    name: str
    label: str
    description: Optional[str] = None

    # Calculation
    type: str = "derived"  # derived, simple, ratio
    expression: str

    # Dimensions
    dimensions: List[str] = Field(default_factory=list)

    # Time grain
    time_grains: List[str] = Field(default_factory=lambda: ["day", "week", "month"])

    # Filters
    filters: List[Dict[str, str]] = Field(default_factory=list)

    # Tags and meta
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class DbtTest(BaseModel):
    """A dbt test definition."""
    name: str
    test_type: str = "generic"  # generic, singular

    # For generic tests
    column_name: Optional[str] = None
    model_name: Optional[str] = None

    # Test configuration
    config: Dict[str, Any] = Field(default_factory=dict)

    # SQL for singular tests
    sql: Optional[str] = None

    model_config = {"extra": "allow"}


class CiCdConfig(BaseModel):
    """Configuration for CI/CD pipeline generation."""
    platform: CiCdPlatform = CiCdPlatform.GITHUB_ACTIONS

    # Triggers
    trigger_branches: List[str] = Field(default_factory=lambda: ["main", "develop"])
    trigger_paths: List[str] = Field(default_factory=lambda: ["models/**", "seeds/**"])

    # Environment
    dbt_version: str = "1.7.0"
    python_version: str = "3.10"

    # Snowflake connection (for secrets)
    snowflake_account_secret: str = "SNOWFLAKE_ACCOUNT"
    snowflake_user_secret: str = "SNOWFLAKE_USER"
    snowflake_password_secret: str = "SNOWFLAKE_PASSWORD"
    snowflake_role_secret: str = "SNOWFLAKE_ROLE"
    snowflake_warehouse_secret: str = "SNOWFLAKE_WAREHOUSE"
    snowflake_database_secret: str = "SNOWFLAKE_DATABASE"

    # Steps
    run_tests: bool = True
    run_docs: bool = True
    deploy_docs: bool = False

    # Notifications
    slack_webhook_secret: Optional[str] = None

    model_config = {"extra": "allow"}


class DbtProject(BaseModel):
    """Complete dbt project definition."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    config: DbtProjectConfig

    # Sources
    sources: List[DbtSource] = Field(default_factory=list)

    # Models
    models: List[DbtModelConfig] = Field(default_factory=list)

    # Metrics
    metrics: List[DbtMetric] = Field(default_factory=list)

    # Tests
    tests: List[DbtTest] = Field(default_factory=list)

    # CI/CD
    cicd_config: Optional[CiCdConfig] = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Generated files cache
    generated_files: Dict[str, str] = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class ValidationResult(BaseModel):
    """Result of project validation."""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    # File checks
    missing_files: List[str] = Field(default_factory=list)
    invalid_files: List[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


# Update forward references
DbtSourceTable.model_rebuild()
