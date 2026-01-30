"""
Configuration management for DataBridge AI V3 using Pydantic Settings.

Loads configuration from environment variables and .env files.
"""

from pathlib import Path
from typing import Optional
from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""

    model_config = SettingsConfigDict(env_prefix="DATABRIDGE_DB_")

    path: Path = Field(
        default=Path("data/databridge.db"),
        description="Path to SQLite database file",
    )
    echo_sql: bool = Field(
        default=False,
        description="Echo SQL statements to stdout for debugging",
    )
    pool_size: int = Field(
        default=5,
        description="Connection pool size",
    )


class DataSettings(BaseSettings):
    """Data directory and file settings."""

    model_config = SettingsConfigDict(env_prefix="DATABRIDGE_DATA_")

    dir: Path = Field(
        default=Path("data"),
        description="Base data directory",
    )
    audit_log: Path = Field(
        default=Path("data/audit_trail.csv"),
        description="Path to audit trail CSV file",
    )
    workflow_file: Path = Field(
        default=Path("data/workflow.json"),
        description="Path to workflow state file",
    )
    max_rows_display: int = Field(
        default=10,
        description="Maximum rows to display in LLM responses",
    )


class FuzzySettings(BaseSettings):
    """Fuzzy matching configuration."""

    model_config = SettingsConfigDict(env_prefix="DATABRIDGE_FUZZY_")

    threshold: int = Field(
        default=80,
        ge=0,
        le=100,
        description="Default fuzzy match threshold (0-100)",
    )
    scorer: str = Field(
        default="WRatio",
        description="RapidFuzz scorer algorithm",
    )


class VectorSettings(BaseSettings):
    """Vector embedding and ChromaDB settings."""

    model_config = SettingsConfigDict(env_prefix="DATABRIDGE_VECTOR_")

    provider: str = Field(
        default="sentence-transformers",
        description="Embedding provider (sentence-transformers or openai)",
    )
    model: str = Field(
        default="all-MiniLM-L6-v2",
        description="Embedding model name",
    )
    db_path: Path = Field(
        default=Path("data/vectors"),
        description="Path to ChromaDB persistent storage",
    )
    top_k: int = Field(
        default=10,
        description="Default number of results for similarity search",
    )


class BackendSyncSettings(BaseSettings):
    """V2 NestJS backend synchronization settings (optional)."""

    model_config = SettingsConfigDict(env_prefix="NESTJS_")

    enabled: bool = Field(
        default=False,
        description="Enable auto-sync to NestJS backend",
    )
    url: Optional[str] = Field(
        default="http://localhost:3002/api",
        description="NestJS backend API URL",
    )
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key for backend authentication",
    )
    timeout: int = Field(
        default=30,
        description="HTTP request timeout in seconds",
    )


class SnowflakeSettings(BaseSettings):
    """Snowflake connection defaults."""

    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_")

    account: Optional[str] = Field(
        default=None,
        description="Snowflake account identifier",
    )
    user: Optional[str] = Field(
        default=None,
        description="Snowflake username",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Snowflake password",
    )
    warehouse: Optional[str] = Field(
        default=None,
        description="Default Snowflake warehouse",
    )
    database: Optional[str] = Field(
        default=None,
        description="Default Snowflake database",
    )
    schema_name: Optional[str] = Field(
        default=None,
        alias="SNOWFLAKE_SCHEMA",
        description="Default Snowflake schema",
    )
    role: Optional[str] = Field(
        default=None,
        description="Default Snowflake role",
    )
    private_key_path: Optional[Path] = Field(
        default=None,
        description="Path to RSA private key for key-pair auth",
    )


class SecuritySettings(BaseSettings):
    """Security configuration."""

    model_config = SettingsConfigDict(env_prefix="DATABRIDGE_SECURITY_")

    master_key: Optional[SecretStr] = Field(
        default=None,
        description="Master encryption key for credentials",
    )
    two_factor_enabled: bool = Field(
        default=False,
        description="Enable 2FA for sensitive operations",
    )
    api_key_prefix: str = Field(
        default="db_",
        description="Prefix for generated API keys",
    )


class Settings(BaseSettings):
    """
    Main application settings.

    Combines all sub-settings and provides application-level configuration.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Application metadata
    app_name: str = Field(
        default="Headless Databridge_AI - Python",
        description="Application name",
    )
    version: str = Field(
        default="3.0.0",
        description="Application version",
    )
    debug: bool = Field(
        default=False,
        alias="DATABRIDGE_DEBUG",
        description="Enable debug mode",
    )

    # Sub-settings (loaded from environment with prefixes)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    fuzzy: FuzzySettings = Field(default_factory=FuzzySettings)
    vector: VectorSettings = Field(default_factory=VectorSettings)
    backend_sync: BackendSyncSettings = Field(default_factory=BackendSyncSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)

    @field_validator("database", "data", "vector", mode="before")
    @classmethod
    def ensure_paths_exist(cls, v):
        """Ensure data directories exist."""
        if isinstance(v, dict):
            return v
        return v

    def ensure_directories(self) -> None:
        """Create required directories if they don't exist."""
        self.data.dir.mkdir(parents=True, exist_ok=True)
        self.vector.db_path.mkdir(parents=True, exist_ok=True)
        self.data.audit_log.parent.mkdir(parents=True, exist_ok=True)

    @property
    def database_url(self) -> str:
        """Get SQLAlchemy database URL."""
        return f"sqlite:///{self.database.path}"


# Singleton pattern for settings
_settings: Optional[Settings] = None


@lru_cache()
def get_settings() -> Settings:
    """
    Get the application settings singleton.

    Uses LRU cache to ensure only one instance is created.

    Returns:
        Settings: The application settings instance.
    """
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.ensure_directories()
    return _settings


def reload_settings() -> Settings:
    """
    Reload settings from environment.

    Clears the cache and reloads settings.

    Returns:
        Settings: Fresh settings instance.
    """
    global _settings
    get_settings.cache_clear()
    _settings = None
    return get_settings()
