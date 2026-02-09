"""Configuration management for DataBridge AI Community Edition."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Community Edition settings - minimal configuration for core features.
    """

    # Application paths
    data_dir: Path = Field(default=Path("data"), description="Directory for data storage")
    workflow_file: Path = Field(default=Path("data/workflow.json"), description="Workflow recipes file")
    audit_log: Path = Field(default=Path("data/audit_trail.csv"), description="Audit trail log")

    # Database connections
    database_url: str = Field(default="", description="SQLAlchemy database connection string")

    # OCR Configuration
    tesseract_path: str = Field(default="", description="Path to Tesseract executable")

    # Fuzzy matching defaults
    fuzzy_threshold: int = Field(default=80, ge=0, le=100, description="Default fuzzy match threshold")

    # Context sensitivity
    max_rows_display: int = Field(default=10, description="Maximum rows to return to LLM")

    # License Configuration (for Pro upgrade path)
    license_key: str = Field(
        default="",
        description="DataBridge license key for Pro/Enterprise features",
        alias="DATABRIDGE_LICENSE_KEY"
    )

    model_config = SettingsConfigDict(
        env_prefix="DATABRIDGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


# Global settings instance
settings = Settings()


def get_config() -> dict:
    """Returns the current application settings as a dictionary."""
    config_dict = settings.model_dump()
    for key, value in config_dict.items():
        if isinstance(value, Path):
            config_dict[key] = str(value)
    return config_dict
