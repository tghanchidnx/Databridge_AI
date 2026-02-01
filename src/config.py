"""Configuration management using Pydantic Settings."""
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database connections
    database_url: str = Field(default="", description="SQLAlchemy database connection string")

    # OCR Configuration
    tesseract_path: str = Field(default="", description="Path to Tesseract executable")

    # API Keys (for LangChain/OpenAI if needed)
    openai_api_key: str = Field(default="", description="OpenAI API key for LangChain")

    # Application paths
    data_dir: Path = Field(default=Path("data"), description="Directory for data storage")
    workflow_file: Path = Field(default=Path("data/workflow.json"), description="Workflow recipes file")
    audit_log: Path = Field(default=Path("data/audit_trail.csv"), description="Audit trail log")

    # Fuzzy matching defaults
    fuzzy_threshold: int = Field(default=80, ge=0, le=100, description="Default fuzzy match threshold")

    # Context sensitivity
    max_rows_display: int = Field(default=10, description="Maximum rows to return to LLM")

    # NestJS Backend Sync Configuration (V2)
    nestjs_backend_url: str = Field(
        default="http://localhost:3002/api",
        description="NestJS backend API URL for sync (V2)"
    )
    nestjs_api_key: str = Field(
        default="v2-dev-key-1",
        description="API key for authenticating with NestJS backend (V2)"
    )
    nestjs_sync_enabled: bool = Field(
        default=True,
        description="Enable automatic sync with NestJS backend"
    )

    # Additional NestJS endpoints for Hierarchy Knowledge Base
    nestjs_connections_endpoint: str = Field(
        default="/connections",
        description="Connections API endpoint"
    )
    nestjs_schema_matcher_endpoint: str = Field(
        default="/schema-matcher",
        description="Schema Matcher API endpoint"
    )
    nestjs_data_matcher_endpoint: str = Field(
        default="/data-matcher",
        description="Data Matcher API endpoint"
    )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


# Global settings instance
settings = Settings()
