"""
Configuration settings for DataBridge AI Researcher Analytics Engine.

Uses Pydantic Settings for environment-based configuration with nested settings
for different warehouse types and feature configurations.
Extends shared settings from databridge-core when available.
"""

from functools import lru_cache
from pathlib import Path
from typing import Optional, Literal, List

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Try to import shared settings from databridge-core
try:
    from databridge_core.config import (
        BaseAppSettings,
        SnowflakeSettings as BaseSnowflakeSettings,
        PostgreSQLSettings as BasePostgreSQLSettings,
        SecuritySettings,
    )
    _HAS_CORE = True
except ImportError:
    _HAS_CORE = False

    class BaseAppSettings(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="DATABRIDGE_",
            env_file=".env",
            env_file_encoding="utf-8",
            extra="ignore",
        )

    class SecuritySettings(BaseModel):
        master_key: Optional[SecretStr] = Field(default=None)


# Researcher-specific settings that extend shared ones
class SnowflakeSettings(BaseModel):
    """Snowflake connection settings."""

    account: Optional[str] = Field(default=None)
    user: Optional[str] = Field(default=None)
    password: Optional[SecretStr] = Field(default=None)
    warehouse: str = Field(default="COMPUTE_WH")
    database: str = Field(default="ANALYTICS")
    schema_: str = Field(default="PUBLIC", alias="schema")
    role: str = Field(default="ANALYST")
    private_key_path: Optional[str] = Field(default=None)

    def is_configured(self) -> bool:
        """Check if Snowflake is configured."""
        return self.account is not None and self.user is not None

    def get_connection_params(self) -> dict:
        """Get connection parameters as dict."""
        params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_,
            "role": self.role,
        }
        if self.password:
            params["password"] = self.password.get_secret_value()
        return params


class PostgreSQLSettings(BaseModel):
    """PostgreSQL connection settings."""

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="databridge_analytics")
    user: str = Field(default="postgres")
    password: Optional[SecretStr] = Field(default=None)
    ssl_mode: str = Field(default="prefer")

    def is_configured(self) -> bool:
        """Check if PostgreSQL is configured."""
        return self.host is not None and self.database is not None

    def get_connection_url(self, include_password: bool = True) -> str:
        """Generate SQLAlchemy connection URL."""
        password_part = ""
        if include_password and self.password:
            password_part = f":{self.password.get_secret_value()}"
        return f"postgresql://{self.user}{password_part}@{self.host}:{self.port}/{self.database}"


class DatabricksSettings(BaseModel):
    """Databricks connection settings."""

    host: Optional[str] = Field(default=None)
    http_path: Optional[str] = Field(default=None)
    access_token: Optional[SecretStr] = Field(default=None)
    catalog: Optional[str] = Field(default=None)
    schema_: str = Field(default="default", alias="schema")

    def is_configured(self) -> bool:
        """Check if Databricks is configured."""
        return self.host is not None and self.access_token is not None


class SQLServerSettings(BaseModel):
    """SQL Server connection settings."""

    host: Optional[str] = Field(default=None)
    port: int = Field(default=1433)
    database: Optional[str] = Field(default=None)
    user: Optional[str] = Field(default=None)
    password: Optional[SecretStr] = Field(default=None)
    driver: str = Field(default="ODBC Driver 17 for SQL Server")
    trust_server_certificate: bool = Field(default=True)

    def is_configured(self) -> bool:
        """Check if SQL Server is configured."""
        return self.host is not None and self.database is not None


class AnalyticsSettings(BaseModel):
    """Analytics feature settings."""

    anomaly_zscore_threshold: float = Field(default=3.0, gt=0)
    anomaly_iqr_multiplier: float = Field(default=1.5, gt=0)
    trend_min_periods: int = Field(default=6, ge=2)
    variance_materiality_threshold: float = Field(default=0.05, ge=0, le=1)
    max_forecast_periods: int = Field(default=24)

    @field_validator("anomaly_zscore_threshold")
    @classmethod
    def validate_zscore_threshold(cls, v):
        if v <= 0:
            raise ValueError("Z-score threshold must be positive")
        return v


class WorkflowSettings(BaseModel):
    """FP&A workflow settings."""

    forecast_horizon_months: int = Field(default=12)
    forecast_default_method: str = Field(default="trend")
    scenario_default_count: int = Field(default=3)
    scenario_names: List[str] = Field(default=["Base", "Upside", "Downside"])
    close_reminder_days: int = Field(default=5)


class NLPSettings(BaseModel):
    """NL-to-SQL settings."""

    spacy_model: str = Field(default="en_core_web_sm")
    confidence_threshold: float = Field(default=0.7)
    max_suggestions: int = Field(default=5)
    enable_fuzzy_matching: bool = Field(default=True)
    fuzzy_threshold: float = Field(default=70.0)


class VectorStoreSettings(BaseModel):
    """Vector store / RAG settings."""

    path: Path = Field(default=Path("./data/chroma"))
    collection_prefix: str = Field(default="databridge_researcher_")
    embedding_model: str = Field(default="all-MiniLM-L6-v2")
    embedding_dimension: int = Field(default=384)


class LibrarianIntegrationSettings(BaseModel):
    """Librarian Hierarchy Builder integration settings."""

    enabled: bool = Field(default=False)
    api_url: str = Field(default="http://localhost:8000")
    api_key: Optional[SecretStr] = Field(default=None)
    timeout_seconds: int = Field(default=30)


class Settings(BaseAppSettings):
    """
    Researcher Analytics Engine application settings.

    Extends BaseAppSettings from databridge-core with Researcher-specific settings.
    """

    model_config = SettingsConfigDict(
        env_prefix="DATABRIDGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_nested_delimiter="__",
    )

    # Application settings
    app_name: str = Field(default="DataBridge AI Researcher - Analytics Engine")
    version: str = Field(default="4.0.0")
    env: str = Field(default="development")
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")

    # Default warehouse type
    default_warehouse_type: Literal["snowflake", "postgresql", "databricks", "sqlserver"] = Field(
        default="snowflake"
    )

    # Query settings
    query_timeout_seconds: int = Field(default=300)
    max_rows_default: int = Field(default=10000)
    enable_query_cache: bool = Field(default=True)
    query_cache_ttl_seconds: int = Field(default=3600)

    # Nested settings
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    postgresql: PostgreSQLSettings = Field(default_factory=PostgreSQLSettings)
    databricks: DatabricksSettings = Field(default_factory=DatabricksSettings)
    sqlserver: SQLServerSettings = Field(default_factory=SQLServerSettings)
    analytics: AnalyticsSettings = Field(default_factory=AnalyticsSettings)
    workflow: WorkflowSettings = Field(default_factory=WorkflowSettings)
    nlp: NLPSettings = Field(default_factory=NLPSettings)
    vector_store: VectorStoreSettings = Field(default_factory=VectorStoreSettings)
    librarian_integration: LibrarianIntegrationSettings = Field(default_factory=LibrarianIntegrationSettings)

    # MCP Server settings
    mcp_server_name: str = Field(default="databridge-analytics-researcher")

    def get_configured_connectors(self) -> List[str]:
        """Get list of configured connectors."""
        configured = []
        if self.snowflake.is_configured():
            configured.append("snowflake")
        if self.postgresql.is_configured():
            configured.append("postgresql")
        if self.databricks.is_configured():
            configured.append("databricks")
        if self.sqlserver.is_configured():
            configured.append("sqlserver")
        return configured

    def get_default_connector_settings(self):
        """Get settings for the default warehouse type."""
        connector_map = {
            "snowflake": self.snowflake,
            "postgresql": self.postgresql,
            "databricks": self.databricks,
            "sqlserver": self.sqlserver,
        }
        return connector_map.get(self.default_warehouse_type, self.snowflake)

    def ensure_directories(self) -> None:
        """Ensure required directories exist."""
        self.vector_store.path.mkdir(parents=True, exist_ok=True)


# Global settings instance
_settings: Optional[Settings] = None


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def reload_settings() -> Settings:
    """Reload settings, clearing the cache."""
    get_settings.cache_clear()
    return get_settings()
