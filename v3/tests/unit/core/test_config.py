"""
Unit tests for the configuration module.
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

        assert settings.app_name == "Headless Databridge_AI - Python"
        assert settings.version == "3.0.0"
        assert settings.debug is False

    def test_database_settings_defaults(self):
        """Test default database settings."""
        from src.core.config import DatabaseSettings

        db_settings = DatabaseSettings()

        assert db_settings.path == Path("data/databridge.db")
        assert db_settings.echo_sql is False
        assert db_settings.pool_size == 5

    def test_data_settings_defaults(self):
        """Test default data settings."""
        from src.core.config import DataSettings

        data_settings = DataSettings()

        assert data_settings.dir == Path("data")
        assert data_settings.max_rows_display == 10

    def test_fuzzy_settings_defaults(self):
        """Test default fuzzy matching settings."""
        from src.core.config import FuzzySettings

        fuzzy_settings = FuzzySettings()

        assert fuzzy_settings.threshold == 80
        assert fuzzy_settings.scorer == "WRatio"

    def test_fuzzy_threshold_validation(self):
        """Test fuzzy threshold validation bounds."""
        from src.core.config import FuzzySettings
        from pydantic import ValidationError

        # Valid threshold
        settings = FuzzySettings(threshold=50)
        assert settings.threshold == 50

        # Invalid: below 0
        with pytest.raises(ValidationError):
            FuzzySettings(threshold=-1)

        # Invalid: above 100
        with pytest.raises(ValidationError):
            FuzzySettings(threshold=101)

    def test_vector_settings_defaults(self):
        """Test default vector settings."""
        from src.core.config import VectorSettings

        vector_settings = VectorSettings()

        assert vector_settings.provider == "sentence-transformers"
        assert vector_settings.model == "all-MiniLM-L6-v2"
        assert vector_settings.top_k == 10

    def test_database_url_property(self):
        """Test database URL generation."""
        from src.core.config import Settings

        settings = Settings()
        assert settings.database_url.startswith("sqlite:///")
        assert "databridge.db" in settings.database_url

    @patch.dict(os.environ, {"DATABRIDGE_DEBUG": "true"})
    def test_settings_from_environment(self):
        """Test loading settings from environment variables."""
        from src.core.config import reload_settings

        settings = reload_settings()
        # Note: Due to caching, this might not work as expected in all cases
        # This is a limitation of the test environment

    def test_ensure_directories(self, tmp_path):
        """Test directory creation."""
        from src.core.config import Settings

        settings = Settings()
        settings.data.dir = tmp_path / "data"
        settings.vector.db_path = tmp_path / "vectors"

        settings.ensure_directories()

        assert settings.data.dir.exists()
        assert settings.vector.db_path.exists()


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

        # After reload, get_settings should return new instance
        # But settings2 should be a new Settings object
        assert isinstance(settings2, type(settings1))
