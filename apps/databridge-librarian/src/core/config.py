"""
Configuration management for DataBridge AI Librarian using Pydantic Settings.

Extends the shared BaseAppSettings from databridge-core with Librarian-specific settings.
"""

from pathlib import Path
from typing import Optional
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# Import shared settings from databridge-core
try:
    from databridge_core.config import (
        BaseAppSettings,
        DatabaseSettings,
        SnowflakeSettings,
        PostgreSQLSettings,
        SecuritySettings,
        DataSettings,
        VectorSettings,
    )
except ImportError:
    # Fallback for standalone usage - define locally
    from pydantic import BaseModel

    class DatabaseSettings(BaseModel):
        path: Path = Field(default=Path("data/databridge.db"))
        echo_sql: bool = Field(default=False)
        pool_size: int = Field(default=5)

    class DataSettings(BaseModel):
        dir: Path = Field(default=Path("data"))
        audit_log: Path = Field(default=Path("data/audit_trail.csv"))
        workflow_file: Path = Field(default=Path("data/workflow.json"))
        max_rows_display: int = Field(default=10)

    class VectorSettings(BaseModel):
        provider: str = Field(default="sentence-transformers")
        model: str = Field(default="all-MiniLM-L6-v2")
        db_path: Path = Field(default=Path("data/vectors"))
        top_k: int = Field(default=10)

    class SnowflakeSettings(BaseModel):
        account: Optional[str] = Field(default=None)
        user: Optional[str] = Field(default=None)
        password: Optional[SecretStr] = Field(default=None)
        warehouse: str = Field(default="COMPUTE_WH")
        database: str = Field(default="ANALYTICS")
        schema_: str = Field(default="PUBLIC", alias="schema")
        role: str = Field(default="ANALYST")
        private_key_path: Optional[str] = Field(default=None)

    class PostgreSQLSettings(BaseModel):
        host: str = Field(default="localhost")
        port: int = Field(default=5432)
        database: str = Field(default="databridge")
        user: str = Field(default="postgres")
        password: Optional[SecretStr] = Field(default=None)

    class SecuritySettings(BaseModel):
        master_key: Optional[SecretStr] = Field(default=None)
        two_factor_enabled: bool = Field(default=False)
        api_key_prefix: str = Field(default="db_")

    class BaseAppSettings(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="DATABRIDGE_",
            env_file=".env",
            env_file_encoding="utf-8",
            extra="ignore",
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


class Settings(BaseAppSettings):
    """
    Librarian Hierarchy Builder application settings.

    Extends BaseAppSettings from databridge-core with Librarian-specific settings.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Application metadata
    app_name: str = Field(
        default="DataBridge AI Librarian - Hierarchy Builder",
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

    # Sub-settings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    fuzzy: FuzzySettings = Field(default_factory=FuzzySettings)
    vector: VectorSettings = Field(default_factory=VectorSettings)
    backend_sync: BackendSyncSettings = Field(default_factory=BackendSyncSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)

    # MCP Server settings
    mcp_server_name: str = Field(
        default="databridge-hierarchy-librarian",
        description="MCP server name",
    )

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
