"""Configuration management using Pydantic Settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path
import json


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

    # Cortex Agent Configuration
    cortex_default_model: str = Field(
        default="mistral-large",
        description="Default Cortex model (mistral-large, claude-3-sonnet, llama3-70b)"
    )
    cortex_max_reasoning_steps: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum steps in reasoning loop"
    )
    cortex_console_enabled: bool = Field(
        default=True,
        description="Enable communication console"
    )
    cortex_console_log_path: Path = Field(
        default=Path("data/cortex_agent/console.jsonl"),
        description="Console log file path"
    )

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Global settings instance
settings = Settings()

def get_config() -> dict:
    """Returns the current application settings as a dictionary."""
    # Convert Path objects to strings for JSON serialization
    config_dict = settings.model_dump()
    for key, value in config_dict.items():
        if isinstance(value, Path):
            config_dict[key] = str(value)
    return config_dict

def save_config(new_config_dict: dict):
    """
    Simulates saving the application settings.
    NOTE: For pydantic-settings, direct dynamic saving to .env is complex.
    This function currently only validates the input against the Settings model
    and prints a message. A real implementation would involve updating the .env file
    or using a database for persistent settings.
    """
    try:
        # Validate the new config against the Pydantic model
        validated_settings = Settings.model_validate(new_config_dict)
        print(f"Configuration successfully validated (simulated save). Would save: {json.dumps(validated_settings.model_dump(), indent=2)}")
        # In a real scenario, you'd update the .env file or a database here.
        # Example (requires manually writing to .env):
        # with open(".env", "w") as f:
        #    for key, value in new_config_dict.items():
        #        f.write(f"{key.upper()}={value}\n")
    except Exception as e:
        raise ValueError(f"Failed to validate or save configuration: {e}")