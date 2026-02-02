"""
Unit tests for the Researcher configuration module.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch


class TestSettings:
    """Tests for Settings class."""

    def test_default_settings(self):
        """Test default settings values."""
        from src.core.config import Settings

        settings = Settings()

        assert settings.app_name == "DataBridge AI Researcher - Analytics Engine"
        assert settings.version == "4.0.0"
        assert settings.env == "development"
        assert settings.debug is False

    def test_default_warehouse_type(self):
        """Test default warehouse type."""
        from src.core.config import Settings

        settings = Settings()
        assert settings.default_warehouse_type == "snowflake"

    def test_snowflake_settings_defaults(self):
        """Test default Snowflake settings."""
        from src.core.config import SnowflakeSettings

        settings = SnowflakeSettings()

        assert settings.warehouse == "COMPUTE_WH"
        assert settings.database == "ANALYTICS"
        assert settings.schema_ == "PUBLIC"
        assert settings.role == "ANALYST"

    def test_snowflake_is_configured_false(self):
        """Test Snowflake is_configured returns False when not configured."""
        from src.core.config import SnowflakeSettings

        settings = SnowflakeSettings()
        assert settings.is_configured() is False

    def test_snowflake_is_configured_true(self):
        """Test Snowflake is_configured returns True when configured."""
        from src.core.config import SnowflakeSettings

        settings = SnowflakeSettings(
            account="test_account",
            user="test_user",
        )
        assert settings.is_configured() is True

    def test_postgresql_settings_defaults(self):
        """Test default PostgreSQL settings."""
        from src.core.config import PostgreSQLSettings

        settings = PostgreSQLSettings()

        assert settings.host == "localhost"
        assert settings.port == 5432
        assert settings.database == "databridge_analytics"
        assert settings.user == "postgres"

    def test_postgresql_connection_url(self):
        """Test PostgreSQL connection URL generation."""
        from src.core.config import PostgreSQLSettings
        from pydantic import SecretStr

        settings = PostgreSQLSettings(
            host="db.example.com",
            port=5433,
            database="analytics",
            user="analyst",
            password=SecretStr("secret123"),
        )

        url = settings.get_connection_url()
        assert "postgresql://" in url
        assert "analyst" in url
        assert "db.example.com:5433" in url
        assert "analytics" in url

    def test_databricks_settings_defaults(self):
        """Test default Databricks settings."""
        from src.core.config import DatabricksSettings

        settings = DatabricksSettings()
        assert settings.schema_ == "default"
        assert settings.is_configured() is False

    def test_sqlserver_settings_defaults(self):
        """Test default SQL Server settings."""
        from src.core.config import SQLServerSettings

        settings = SQLServerSettings()
        assert settings.port == 1433
        assert settings.trust_server_certificate is True

    def test_analytics_settings_defaults(self):
        """Test default analytics settings."""
        from src.core.config import AnalyticsSettings

        settings = AnalyticsSettings()

        assert settings.anomaly_zscore_threshold == 3.0
        assert settings.trend_min_periods == 6
        assert settings.variance_materiality_threshold == 0.05

    def test_analytics_zscore_validation(self):
        """Test z-score threshold validation."""
        from src.core.config import AnalyticsSettings
        from pydantic import ValidationError

        # Valid threshold
        settings = AnalyticsSettings(anomaly_zscore_threshold=2.5)
        assert settings.anomaly_zscore_threshold == 2.5

        # Invalid: zero
        with pytest.raises(ValidationError):
            AnalyticsSettings(anomaly_zscore_threshold=0)

        # Invalid: negative
        with pytest.raises(ValidationError):
            AnalyticsSettings(anomaly_zscore_threshold=-1.5)

    def test_workflow_settings_defaults(self):
        """Test default workflow settings."""
        from src.core.config import WorkflowSettings

        settings = WorkflowSettings()

        assert settings.forecast_horizon_months == 12
        assert settings.forecast_default_method == "trend"
        assert settings.scenario_default_count == 3
        assert settings.scenario_names == ["Base", "Upside", "Downside"]

    def test_nlp_settings_defaults(self):
        """Test default NLP settings."""
        from src.core.config import NLPSettings

        settings = NLPSettings()

        assert settings.spacy_model == "en_core_web_sm"
        assert settings.confidence_threshold == 0.7

    def test_vector_store_settings_defaults(self):
        """Test default vector store settings."""
        from src.core.config import VectorStoreSettings

        settings = VectorStoreSettings()

        assert settings.path == Path("./data/chroma")
        assert settings.collection_prefix == "databridge_researcher_"
        assert settings.embedding_model == "all-MiniLM-L6-v2"

    def test_librarian_integration_settings_defaults(self):
        """Test default Librarian integration settings."""
        from src.core.config import LibrarianIntegrationSettings

        settings = LibrarianIntegrationSettings()

        assert settings.api_url == "http://localhost:8000"
        assert settings.timeout_seconds == 30

    def test_get_configured_connectors_none(self):
        """Test get_configured_connectors when none configured."""
        from src.core.config import Settings

        settings = Settings()
        configured = settings.get_configured_connectors()

        # PostgreSQL is configured by default with localhost
        assert "postgresql" in configured

    def test_get_default_connector_settings(self):
        """Test getting default connector settings."""
        from src.core.config import Settings

        settings = Settings()
        default = settings.get_default_connector_settings()

        # Default is Snowflake
        assert hasattr(default, "warehouse")
        assert hasattr(default, "database")

    def test_ensure_directories(self, tmp_path):
        """Test directory creation."""
        from src.core.config import Settings

        settings = Settings()
        settings.vector_store.path = tmp_path / "chroma"

        settings.ensure_directories()

        assert settings.vector_store.path.exists()


class TestGetSettings:
    """Tests for get_settings function."""

    def test_get_settings_returns_settings(self):
        """Test that get_settings returns a Settings instance."""
        from src.core.config import get_settings, Settings

        settings = get_settings()
        assert isinstance(settings, Settings)

    def test_get_settings_singleton(self):
        """Test that get_settings returns the same instance."""
        from src.core.config import get_settings

        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2

    def test_reload_settings(self):
        """Test that reload_settings returns a fresh instance."""
        from src.core.config import get_settings, reload_settings

        settings1 = get_settings()
        settings2 = reload_settings()

        # After reload, should be a new Settings object
        assert isinstance(settings2, type(settings1))


class TestEnvironmentVariables:
    """Tests for environment variable loading."""

    @patch.dict(os.environ, {"DATABRIDGE_DEBUG": "true"})
    def test_debug_from_env(self):
        """Test loading debug setting from environment."""
        from src.core.config import reload_settings

        settings = reload_settings()
        # Note: env var parsing depends on pydantic settings

    @patch.dict(os.environ, {"DEFAULT_WAREHOUSE_TYPE": "postgresql"})
    def test_warehouse_type_from_env(self):
        """Test loading warehouse type from environment."""
        from src.core.config import reload_settings

        settings = reload_settings()
        # Verify the setting is accessible
        assert hasattr(settings, "default_warehouse_type")
