"""
Unit tests for SettingsManager.

Tests settings persistence, API key management, and configuration.
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.core.settings_manager import (
    SettingsManager,
    UserSettings,
    GeneralSettings,
    ConnectionSettings,
    WorkflowPreferences,
    SecuritySettings,
    NotionSettings,
    APIKey,
    get_settings_manager,
)


@pytest.fixture
def temp_settings_dir():
    """Create a temporary directory for settings."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def settings_manager(temp_settings_dir):
    """Create a SettingsManager with temp directory."""
    return SettingsManager(settings_dir=temp_settings_dir)


class TestGeneralSettings:
    """Tests for GeneralSettings dataclass."""

    def test_default_values(self):
        settings = GeneralSettings()
        assert settings.theme == "dark"
        assert settings.language == "en"
        assert settings.auto_save is True
        assert settings.confirm_destructive is True
        assert settings.max_recent_items == 10
        assert settings.telemetry_enabled is False

    def test_to_dict(self):
        settings = GeneralSettings(theme="light", language="es")
        data = settings.to_dict()
        assert data["theme"] == "light"
        assert data["language"] == "es"

    def test_from_dict(self):
        data = {"theme": "light", "language": "fr", "auto_save": False}
        settings = GeneralSettings.from_dict(data)
        assert settings.theme == "light"
        assert settings.language == "fr"
        assert settings.auto_save is False

    def test_from_dict_ignores_unknown_fields(self):
        data = {"theme": "dark", "unknown_field": "value"}
        settings = GeneralSettings.from_dict(data)
        assert settings.theme == "dark"
        assert not hasattr(settings, "unknown_field")


class TestConnectionSettings:
    """Tests for ConnectionSettings dataclass."""

    def test_default_values(self):
        settings = ConnectionSettings()
        assert settings.default_timeout == 30
        assert settings.max_connections == 5
        assert settings.ssl_verify is True
        assert settings.retry_attempts == 3

    def test_roundtrip(self):
        settings = ConnectionSettings(default_timeout=60, max_connections=10)
        data = settings.to_dict()
        restored = ConnectionSettings.from_dict(data)
        assert restored.default_timeout == 60
        assert restored.max_connections == 10


class TestWorkflowPreferences:
    """Tests for WorkflowPreferences dataclass."""

    def test_default_values(self):
        prefs = WorkflowPreferences()
        assert prefs.default_fiscal_year_start_month == 1
        assert prefs.auto_advance_steps is False
        assert prefs.require_approval_for_lock is True
        assert prefs.default_variance_threshold == 0.05

    def test_notification_settings(self):
        prefs = WorkflowPreferences(
            notification_email="test@example.com",
            slack_webhook_url="https://hooks.slack.com/xxx"
        )
        assert prefs.notification_email == "test@example.com"
        assert prefs.slack_webhook_url == "https://hooks.slack.com/xxx"


class TestSecuritySettings:
    """Tests for SecuritySettings dataclass."""

    def test_default_values(self):
        settings = SecuritySettings()
        assert settings.session_timeout_minutes == 60
        assert settings.require_2fa is False
        assert settings.api_rate_limit_per_minute == 100
        assert settings.audit_logging_enabled is True

    def test_ip_ranges(self):
        settings = SecuritySettings(allowed_ip_ranges=["192.168.1.0/24", "10.0.0.0/8"])
        assert len(settings.allowed_ip_ranges) == 2
        assert "192.168.1.0/24" in settings.allowed_ip_ranges


class TestNotionSettings:
    """Tests for NotionSettings dataclass."""

    def test_default_values(self):
        settings = NotionSettings()
        assert settings.enabled is False
        assert settings.api_key == ""
        assert settings.sync_interval_minutes == 60

    def test_api_key_masked_in_dict(self):
        settings = NotionSettings(api_key="secret-api-key", enabled=True)
        data = settings.to_dict()
        assert data["api_key"] == "***configured***"
        assert data["enabled"] is True

    def test_empty_api_key_not_masked(self):
        settings = NotionSettings(api_key="")
        data = settings.to_dict()
        assert data["api_key"] == ""


class TestAPIKey:
    """Tests for APIKey dataclass."""

    def test_default_scopes(self):
        key = APIKey(key_id="db_test1234", key_hash="abc123")
        assert key.scopes == ["read", "write"]
        assert key.is_active is True

    def test_roundtrip(self):
        key = APIKey(
            key_id="db_test1234",
            key_hash="abc123def456",
            description="Test key",
            created_at="2024-01-01T00:00:00",
            scopes=["read"],
        )
        data = key.to_dict()
        restored = APIKey.from_dict(data)
        assert restored.key_id == "db_test1234"
        assert restored.description == "Test key"
        assert restored.scopes == ["read"]


class TestUserSettings:
    """Tests for UserSettings dataclass."""

    def test_default_values(self):
        settings = UserSettings()
        assert isinstance(settings.general, GeneralSettings)
        assert isinstance(settings.connections, ConnectionSettings)
        assert isinstance(settings.workflows, WorkflowPreferences)
        assert isinstance(settings.security, SecuritySettings)
        assert isinstance(settings.notion, NotionSettings)
        assert settings.recent_projects == []
        assert settings.favorites == []

    def test_to_dict(self):
        settings = UserSettings(
            recent_projects=["proj1", "proj2"],
            favorites=["fav1"],
        )
        data = settings.to_dict()
        assert data["recent_projects"] == ["proj1", "proj2"]
        assert data["favorites"] == ["fav1"]
        assert "general" in data
        assert "connections" in data

    def test_from_dict(self):
        data = {
            "general": {"theme": "light"},
            "connections": {"default_timeout": 45},
            "recent_projects": ["p1"],
        }
        settings = UserSettings.from_dict(data)
        assert settings.general.theme == "light"
        assert settings.connections.default_timeout == 45
        assert settings.recent_projects == ["p1"]


class TestSettingsManager:
    """Tests for SettingsManager class."""

    def test_init_creates_directory(self, temp_settings_dir):
        settings_dir = temp_settings_dir / "subdir"
        manager = SettingsManager(settings_dir=settings_dir)
        assert settings_dir.exists()

    def test_init_no_auto_create(self, temp_settings_dir):
        settings_dir = temp_settings_dir / "nonexistent"
        manager = SettingsManager(settings_dir=settings_dir, auto_create=False)
        assert not settings_dir.exists()

    def test_load_default_settings(self, settings_manager):
        settings = settings_manager.load()
        assert isinstance(settings, UserSettings)
        assert settings.general.theme == "dark"

    def test_save_and_load(self, settings_manager):
        settings = settings_manager.load()
        settings.general.theme = "light"
        settings.general.language = "es"
        assert settings_manager.save()

        # Create new manager to reload
        manager2 = SettingsManager(settings_dir=settings_manager.settings_dir)
        loaded = manager2.load()
        assert loaded.general.theme == "light"
        assert loaded.general.language == "es"

    def test_save_creates_backup(self, settings_manager):
        # First save
        settings_manager.load()
        settings_manager.save()

        # Second save should create backup
        settings_manager.get_settings().general.theme = "light"
        settings_manager.save(backup=True)

        backup_file = settings_manager.settings_file.with_suffix(".json.bak")
        assert backup_file.exists()

    def test_get_settings_loads_if_needed(self, settings_manager):
        settings = settings_manager.get_settings()
        assert isinstance(settings, UserSettings)
        assert settings_manager._loaded is True

    def test_update_general(self, settings_manager):
        settings_manager.load()
        result = settings_manager.update_general(theme="light", language="de")
        assert result is True
        assert settings_manager.get_settings().general.theme == "light"
        assert settings_manager.get_settings().general.language == "de"

    def test_update_general_ignores_unknown(self, settings_manager):
        settings_manager.load()
        settings_manager.update_general(unknown_field="value")
        # Should not crash, unknown field ignored

    def test_update_connections(self, settings_manager):
        settings_manager.load()
        settings_manager.update_connections(default_timeout=60, max_connections=10)
        assert settings_manager.get_settings().connections.default_timeout == 60
        assert settings_manager.get_settings().connections.max_connections == 10

    def test_update_workflows(self, settings_manager):
        settings_manager.load()
        settings_manager.update_workflows(
            default_fiscal_year_start_month=7,
            default_variance_threshold=0.1,
        )
        assert settings_manager.get_settings().workflows.default_fiscal_year_start_month == 7
        assert settings_manager.get_settings().workflows.default_variance_threshold == 0.1

    def test_update_security(self, settings_manager):
        settings_manager.load()
        settings_manager.update_security(
            session_timeout_minutes=120,
            require_2fa=True,
        )
        assert settings_manager.get_settings().security.session_timeout_minutes == 120
        assert settings_manager.get_settings().security.require_2fa is True

    def test_update_notion(self, settings_manager):
        settings_manager.load()
        settings_manager.update_notion(
            enabled=True,
            api_key="secret-key",
            workspace_id="ws-123",
        )
        assert settings_manager.get_settings().notion.enabled is True
        assert settings_manager.get_settings().notion.api_key == "secret-key"
        assert settings_manager.get_settings().notion.workspace_id == "ws-123"

    def test_add_recent_project(self, settings_manager):
        settings_manager.load()
        settings_manager.add_recent_project("proj1")
        settings_manager.add_recent_project("proj2")
        settings_manager.add_recent_project("proj3")

        projects = settings_manager.get_settings().recent_projects
        assert projects[0] == "proj3"  # Most recent first
        assert projects[1] == "proj2"
        assert projects[2] == "proj1"

    def test_add_recent_project_moves_existing_to_top(self, settings_manager):
        settings_manager.load()
        settings_manager.add_recent_project("proj1")
        settings_manager.add_recent_project("proj2")
        settings_manager.add_recent_project("proj1")  # Should move to top

        projects = settings_manager.get_settings().recent_projects
        assert projects[0] == "proj1"
        assert projects[1] == "proj2"
        assert len(projects) == 2

    def test_add_recent_project_limits_size(self, settings_manager):
        settings_manager.load()
        for i in range(15):
            settings_manager.add_recent_project(f"proj{i}", max_items=10)

        assert len(settings_manager.get_settings().recent_projects) == 10

    def test_add_recent_query(self, settings_manager):
        settings_manager.load()
        settings_manager.add_recent_query("SELECT * FROM table1")
        settings_manager.add_recent_query("SELECT * FROM table2")

        queries = settings_manager.get_settings().recent_queries
        assert queries[0] == "SELECT * FROM table2"
        assert len(queries) == 2

    def test_toggle_favorite_add(self, settings_manager):
        settings_manager.load()
        result = settings_manager.toggle_favorite("item1")
        assert result is True
        assert "item1" in settings_manager.get_settings().favorites

    def test_toggle_favorite_remove(self, settings_manager):
        settings_manager.load()
        settings_manager.toggle_favorite("item1")
        result = settings_manager.toggle_favorite("item1")
        assert result is False
        assert "item1" not in settings_manager.get_settings().favorites

    def test_export_settings(self, settings_manager):
        settings_manager.load()
        settings_manager.update_general(theme="light")

        exported = settings_manager.export_settings()
        assert exported["general"]["theme"] == "light"
        assert "api_keys" not in exported

    def test_export_settings_with_api_keys(self, settings_manager):
        settings_manager.load()
        settings_manager.generate_api_key(description="Test key")

        exported = settings_manager.export_settings(include_api_keys=True)
        assert "api_keys" in exported
        assert len(exported["api_keys"]) == 1

    def test_import_settings_merge(self, settings_manager):
        settings_manager.load()
        settings_manager.update_general(language="en")

        # Import with merge
        settings_manager.import_settings(
            {"general": {"theme": "light"}},
            merge=True,
        )

        settings = settings_manager.get_settings()
        assert settings.general.theme == "light"
        assert settings.general.language == "en"  # Preserved

    def test_import_settings_replace(self, settings_manager):
        settings_manager.load()
        settings_manager.update_general(language="es", theme="dark")

        # Import with replace
        settings_manager.import_settings(
            {"general": {"theme": "light"}},
            merge=False,
        )

        settings = settings_manager.get_settings()
        assert settings.general.theme == "light"
        assert settings.general.language == "en"  # Reset to default

    def test_reset_to_defaults(self, settings_manager):
        settings_manager.load()
        settings_manager.update_general(theme="light", language="fr")

        result = settings_manager.reset_to_defaults()
        assert result is True

        settings = settings_manager.get_settings()
        assert settings.general.theme == "dark"
        assert settings.general.language == "en"

    def test_load_corrupted_file(self, settings_manager):
        # Write corrupted JSON
        with open(settings_manager.settings_file, "w") as f:
            f.write("{invalid json")

        settings = settings_manager.load()
        assert isinstance(settings, UserSettings)
        assert settings.general.theme == "dark"  # Default


class TestAPIKeyManagement:
    """Tests for API key management."""

    def test_generate_api_key(self, settings_manager):
        settings_manager.load()
        key_id, plain_key = settings_manager.generate_api_key(description="Test")

        assert key_id.startswith("db_")
        assert len(key_id) == 11  # db_ + 8 hex chars
        assert plain_key.startswith(key_id)
        assert len(plain_key) > len(key_id)

    def test_generate_api_key_persists(self, settings_manager):
        settings_manager.load()
        key_id, plain_key = settings_manager.generate_api_key(description="Persisted")

        # Create new manager
        manager2 = SettingsManager(settings_dir=settings_manager.settings_dir)
        manager2.load()

        keys = manager2.list_api_keys()
        assert len(keys) == 1
        assert keys[0]["key_id"] == key_id

    def test_validate_api_key_success(self, settings_manager):
        settings_manager.load()
        key_id, plain_key = settings_manager.generate_api_key()

        result = settings_manager.validate_api_key(plain_key)
        assert result is not None
        assert result.key_id == key_id
        assert result.last_used_at is not None

    def test_validate_api_key_invalid(self, settings_manager):
        settings_manager.load()
        settings_manager.generate_api_key()

        result = settings_manager.validate_api_key("invalid_key")
        assert result is None

    def test_validate_api_key_wrong_secret(self, settings_manager):
        settings_manager.load()
        key_id, _ = settings_manager.generate_api_key()

        # Use correct key_id but wrong secret
        fake_key = f"{key_id}_wrongsecret1234567890123456"
        result = settings_manager.validate_api_key(fake_key)
        assert result is None

    def test_validate_api_key_revoked(self, settings_manager):
        settings_manager.load()
        key_id, plain_key = settings_manager.generate_api_key()
        settings_manager.revoke_api_key(key_id)

        result = settings_manager.validate_api_key(plain_key)
        assert result is None

    def test_validate_api_key_expired(self, settings_manager):
        settings_manager.load()

        # Generate key with past expiration
        key_id, plain_key = settings_manager.generate_api_key(expires_in_days=0)

        # Manually set expiration to past
        api_key = settings_manager._api_keys[key_id]
        api_key.expires_at = (datetime.now() - timedelta(days=1)).isoformat()

        result = settings_manager.validate_api_key(plain_key)
        assert result is None

    def test_list_api_keys(self, settings_manager):
        settings_manager.load()
        settings_manager.generate_api_key(description="Key 1", scopes=["read"])
        settings_manager.generate_api_key(description="Key 2", scopes=["read", "write"])

        keys = settings_manager.list_api_keys()
        assert len(keys) == 2

        # Check no hashes exposed
        for key in keys:
            assert "key_hash" not in key
            assert "key_id" in key
            assert "description" in key

    def test_revoke_api_key(self, settings_manager):
        settings_manager.load()
        key_id, _ = settings_manager.generate_api_key()

        result = settings_manager.revoke_api_key(key_id)
        assert result is True
        assert settings_manager._api_keys[key_id].is_active is False

    def test_revoke_api_key_not_found(self, settings_manager):
        settings_manager.load()
        result = settings_manager.revoke_api_key("nonexistent")
        assert result is False

    def test_delete_api_key(self, settings_manager):
        settings_manager.load()
        key_id, _ = settings_manager.generate_api_key()

        result = settings_manager.delete_api_key(key_id)
        assert result is True
        assert key_id not in settings_manager._api_keys

    def test_delete_api_key_not_found(self, settings_manager):
        settings_manager.load()
        result = settings_manager.delete_api_key("nonexistent")
        assert result is False

    def test_rotate_api_key(self, settings_manager):
        settings_manager.load()
        old_id, old_key = settings_manager.generate_api_key(
            description="Original",
            scopes=["admin"],
        )

        result = settings_manager.rotate_api_key(old_id)
        assert result is not None

        new_id, new_key = result
        assert new_id != old_id
        assert new_key != old_key

        # Old key should be revoked
        assert settings_manager._api_keys[old_id].is_active is False

        # New key should work
        validated = settings_manager.validate_api_key(new_key)
        assert validated is not None
        assert validated.scopes == ["admin"]

    def test_rotate_api_key_not_found(self, settings_manager):
        settings_manager.load()
        result = settings_manager.rotate_api_key("nonexistent")
        assert result is None

    def test_api_key_with_expiration(self, settings_manager):
        settings_manager.load()
        key_id, plain_key = settings_manager.generate_api_key(expires_in_days=30)

        api_key = settings_manager._api_keys[key_id]
        assert api_key.expires_at is not None

        # Should still be valid
        result = settings_manager.validate_api_key(plain_key)
        assert result is not None

    def test_api_key_scopes(self, settings_manager):
        settings_manager.load()
        key_id, _ = settings_manager.generate_api_key(scopes=["read"])

        keys = settings_manager.list_api_keys()
        assert keys[0]["scopes"] == ["read"]


class TestGlobalSettingsManager:
    """Tests for global settings manager instance."""

    def test_get_settings_manager(self, temp_settings_dir):
        with patch("src.core.settings_manager.DEFAULT_SETTINGS_DIR", temp_settings_dir):
            # Reset global instance
            import src.core.settings_manager as sm
            sm._settings_manager = None

            manager1 = get_settings_manager()
            manager2 = get_settings_manager()

            assert manager1 is manager2  # Same instance


class TestSettingsFileOperations:
    """Tests for file I/O edge cases."""

    def test_last_modified_updated_on_save(self, settings_manager):
        settings_manager.load()
        before = settings_manager.get_settings().last_modified

        import time
        time.sleep(0.01)  # Small delay

        settings_manager.save()
        after = settings_manager.get_settings().last_modified

        assert after != before
        assert after > before

    def test_save_without_load(self, settings_manager):
        # Save without loading first
        result = settings_manager.save()
        assert result is True

        # Should create default settings
        settings = settings_manager.get_settings()
        assert settings.general.theme == "dark"

    def test_multiple_managers_same_file(self, temp_settings_dir):
        manager1 = SettingsManager(settings_dir=temp_settings_dir)
        manager1.load()
        manager1.update_general(theme="light")

        manager2 = SettingsManager(settings_dir=temp_settings_dir)
        settings = manager2.load()
        assert settings.general.theme == "light"

    def test_settings_file_permissions(self, settings_manager):
        settings_manager.load()
        settings_manager.save()

        # File should exist and be readable
        assert settings_manager.settings_file.exists()
        with open(settings_manager.settings_file, "r") as f:
            data = json.load(f)
        assert "general" in data
