"""
Unified configuration settings for DataBridge AI platform.

Provides base settings classes that can be extended by Librarian and Researcher applications.
Uses Pydantic Settings for environment-based configuration.
"""

from functools import lru_cache
from pathlib import Path
from typing import Optional, List, Literal, TypeVar, Type

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """SQLite database configuration (used by Librarian)."""

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


class SecuritySettings(BaseModel):
    """Security configuration."""

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


class DataSettings(BaseModel):
    """Data directory and file settings."""

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


class VectorSettings(BaseModel):
    """Vector embedding and ChromaDB settings."""

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
    dimension: int = Field(
        default=384,
        description="Embedding dimension",
    )


class BaseAppSettings(BaseSettings):
    """
    Base application settings shared by all DataBridge applications.

    Extend this class in Librarian and Researcher to add app-specific settings.
    """

    model_config = SettingsConfigDict(
        env_prefix="DATABRIDGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_nested_delimiter="__",
    )

    # Application metadata
    app_name: str = Field(
        default="DataBridge AI",
        description="Application name",
    )
    version: str = Field(
        default="1.0.0",
        description="Application version",
    )
    env: str = Field(
        default="development",
        description="Environment (development, staging, production)",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level",
    )

    # Common nested settings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    vector: VectorSettings = Field(default_factory=VectorSettings)
    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    postgresql: PostgreSQLSettings = Field(default_factory=PostgreSQLSettings)

    # MCP Server settings
    mcp_server_name: str = Field(
        default="databridge",
        description="MCP server name",
    )

    def ensure_directories(self) -> None:
        """Create required directories if they don't exist."""
        self.data.dir.mkdir(parents=True, exist_ok=True)
        self.vector.db_path.mkdir(parents=True, exist_ok=True)
        self.data.audit_log.parent.mkdir(parents=True, exist_ok=True)

    @property
    def database_url(self) -> str:
        """Get SQLAlchemy database URL for SQLite."""
        return f"sqlite:///{self.database.path}"

    def get_configured_connectors(self) -> List[str]:
        """Get list of configured connectors."""
        configured = []
        if self.snowflake.is_configured():
            configured.append("snowflake")
        if self.postgresql.is_configured():
            configured.append("postgresql")
        return configured


# Type variable for settings subclasses
T = TypeVar("T", bound=BaseAppSettings)

# Global settings storage
_settings_cache: dict = {}


def get_settings(settings_cls: Type[T] = BaseAppSettings) -> T:
    """
    Get cached settings instance for the given settings class.

    Args:
        settings_cls: The settings class to instantiate.

    Returns:
        Cached settings instance.
    """
    cls_name = settings_cls.__name__
    if cls_name not in _settings_cache:
        _settings_cache[cls_name] = settings_cls()
        _settings_cache[cls_name].ensure_directories()
    return _settings_cache[cls_name]


def reload_settings(settings_cls: Type[T] = BaseAppSettings) -> T:
    """
    Reload settings, clearing the cache for the given class.

    Args:
        settings_cls: The settings class to reload.

    Returns:
        Fresh settings instance.
    """
    cls_name = settings_cls.__name__
    if cls_name in _settings_cache:
        del _settings_cache[cls_name]
    return get_settings(settings_cls)
