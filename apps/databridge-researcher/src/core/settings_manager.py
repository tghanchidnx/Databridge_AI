"""
Settings Manager for DataBridge Analytics V4.

Provides persistent settings management with JSON file storage,
complementing the environment-based Pydantic settings.
"""

import json
import os
import hashlib
import secrets
import hmac
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


# Default settings directory
DEFAULT_SETTINGS_DIR = Path.home() / ".databridge"
DEFAULT_SETTINGS_FILE = "settings.json"
DEFAULT_API_KEYS_FILE = "api_keys.json"


@dataclass
class GeneralSettings:
    """General application settings."""
    theme: str = "dark"
    language: str = "en"
    auto_save: bool = True
    confirm_destructive: bool = True
    max_recent_items: int = 10
    telemetry_enabled: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GeneralSettings":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ConnectionSettings:
    """Database connection preferences."""
    default_timeout: int = 30
    max_connections: int = 5
    ssl_verify: bool = True
    connection_pool_enabled: bool = True
    retry_attempts: int = 3
    retry_delay_seconds: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionSettings":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class WorkflowPreferences:
    """FP&A workflow preferences."""
    default_fiscal_year_start_month: int = 1
    auto_advance_steps: bool = False
    require_approval_for_lock: bool = True
    notification_email: str = ""
    slack_webhook_url: str = ""
    default_variance_threshold: float = 0.05
    default_forecast_method: str = "straight_line"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkflowPreferences":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class SecuritySettings:
    """Security-related settings."""
    session_timeout_minutes: int = 60
    require_2fa: bool = False
    totp_issuer: str = "DataBridge Analytics"
    allowed_ip_ranges: List[str] = field(default_factory=list)
    api_rate_limit_per_minute: int = 100
    audit_logging_enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SecuritySettings":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class NotionSettings:
    """Notion integration settings."""
    enabled: bool = False
    api_key: str = ""  # Will be stored encrypted
    workspace_id: str = ""
    documentation_database_id: str = ""
    sync_interval_minutes: int = 60
    auto_sync_enabled: bool = False

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Don't expose API key in dict
        if data.get("api_key"):
            data["api_key"] = "***configured***"
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotionSettings":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class APIKey:
    """Represents an API key."""
    key_id: str
    key_hash: str  # SHA256 hash, never plain text
    description: str = ""
    created_at: str = ""
    last_used_at: Optional[str] = None
    expires_at: Optional[str] = None
    is_active: bool = True
    scopes: List[str] = field(default_factory=lambda: ["read", "write"])

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "APIKey":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class UserSettings:
    """Complete user settings bundle."""
    general: GeneralSettings = field(default_factory=GeneralSettings)
    connections: ConnectionSettings = field(default_factory=ConnectionSettings)
    workflows: WorkflowPreferences = field(default_factory=WorkflowPreferences)
    security: SecuritySettings = field(default_factory=SecuritySettings)
    notion: NotionSettings = field(default_factory=NotionSettings)
    recent_projects: List[str] = field(default_factory=list)
    recent_queries: List[str] = field(default_factory=list)
    favorites: List[str] = field(default_factory=list)
    last_modified: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "general": self.general.to_dict(),
            "connections": self.connections.to_dict(),
            "workflows": self.workflows.to_dict(),
            "security": self.security.to_dict(),
            "notion": self.notion.to_dict(),
            "recent_projects": self.recent_projects,
            "recent_queries": self.recent_queries,
            "favorites": self.favorites,
            "last_modified": self.last_modified,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserSettings":
        return cls(
            general=GeneralSettings.from_dict(data.get("general", {})),
            connections=ConnectionSettings.from_dict(data.get("connections", {})),
            workflows=WorkflowPreferences.from_dict(data.get("workflows", {})),
            security=SecuritySettings.from_dict(data.get("security", {})),
            notion=NotionSettings.from_dict(data.get("notion", {})),
            recent_projects=data.get("recent_projects", []),
            recent_queries=data.get("recent_queries", []),
            favorites=data.get("favorites", []),
            last_modified=data.get("last_modified", ""),
        )


class SettingsManager:
    """
    Manages persistent user settings with JSON file storage.

    Features:
    - Load/save settings to ~/.databridge/settings.json
    - API key management with secure hash storage
    - Environment variable overrides
    - Settings validation
    - Automatic backup on save
    """

    def __init__(
        self,
        settings_dir: Optional[Path] = None,
        auto_create: bool = True,
    ):
        """
        Initialize the settings manager.

        Args:
            settings_dir: Directory for settings files.
            auto_create: Create directory if it doesn't exist.
        """
        self.settings_dir = Path(settings_dir) if settings_dir else DEFAULT_SETTINGS_DIR
        self.settings_file = self.settings_dir / DEFAULT_SETTINGS_FILE
        self.api_keys_file = self.settings_dir / DEFAULT_API_KEYS_FILE

        if auto_create:
            self.settings_dir.mkdir(parents=True, exist_ok=True)

        self._settings: Optional[UserSettings] = None
        self._api_keys: Dict[str, APIKey] = {}
        self._loaded = False

    def load(self) -> UserSettings:
        """
        Load settings from file.

        Returns:
            UserSettings object.
        """
        if self._loaded and self._settings:
            return self._settings

        try:
            if self.settings_file.exists():
                with open(self.settings_file, "r") as f:
                    data = json.load(f)
                self._settings = UserSettings.from_dict(data)
                logger.info(f"Loaded settings from {self.settings_file}")
            else:
                self._settings = UserSettings()
                logger.info("Using default settings")

            # Load API keys
            self._load_api_keys()

            self._loaded = True
            return self._settings

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse settings file: {e}")
            self._settings = UserSettings()
            return self._settings
        except Exception as e:
            logger.error(f"Failed to load settings: {e}")
            self._settings = UserSettings()
            return self._settings

    def save(self, backup: bool = True) -> bool:
        """
        Save settings to file.

        Args:
            backup: Create backup before saving.

        Returns:
            True if successful.
        """
        if not self._settings:
            self._settings = UserSettings()

        try:
            # Create backup
            if backup and self.settings_file.exists():
                backup_file = self.settings_file.with_suffix(".json.bak")
                import shutil
                shutil.copy(self.settings_file, backup_file)

            # Update last modified
            self._settings.last_modified = datetime.now().isoformat()

            # Save settings
            with open(self.settings_file, "w") as f:
                json.dump(self._settings.to_dict(), f, indent=2)

            # Save API keys
            self._save_api_keys()

            logger.info(f"Saved settings to {self.settings_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to save settings: {e}")
            return False

    def get_settings(self) -> UserSettings:
        """Get current settings, loading if necessary."""
        if not self._loaded:
            return self.load()
        return self._settings or UserSettings()

    def update_general(self, **kwargs) -> bool:
        """Update general settings."""
        settings = self.get_settings()
        for key, value in kwargs.items():
            if hasattr(settings.general, key):
                setattr(settings.general, key, value)
        return self.save()

    def update_connections(self, **kwargs) -> bool:
        """Update connection settings."""
        settings = self.get_settings()
        for key, value in kwargs.items():
            if hasattr(settings.connections, key):
                setattr(settings.connections, key, value)
        return self.save()

    def update_workflows(self, **kwargs) -> bool:
        """Update workflow settings."""
        settings = self.get_settings()
        for key, value in kwargs.items():
            if hasattr(settings.workflows, key):
                setattr(settings.workflows, key, value)
        return self.save()

    def update_security(self, **kwargs) -> bool:
        """Update security settings."""
        settings = self.get_settings()
        for key, value in kwargs.items():
            if hasattr(settings.security, key):
                setattr(settings.security, key, value)
        return self.save()

    def update_notion(self, **kwargs) -> bool:
        """Update Notion settings."""
        settings = self.get_settings()
        for key, value in kwargs.items():
            if hasattr(settings.notion, key):
                setattr(settings.notion, key, value)
        return self.save()

    def add_recent_project(self, project_id: str, max_items: int = 10) -> None:
        """Add a project to recent list."""
        settings = self.get_settings()
        if project_id in settings.recent_projects:
            settings.recent_projects.remove(project_id)
        settings.recent_projects.insert(0, project_id)
        settings.recent_projects = settings.recent_projects[:max_items]
        self.save(backup=False)

    def add_recent_query(self, query: str, max_items: int = 20) -> None:
        """Add a query to recent list."""
        settings = self.get_settings()
        if query in settings.recent_queries:
            settings.recent_queries.remove(query)
        settings.recent_queries.insert(0, query)
        settings.recent_queries = settings.recent_queries[:max_items]
        self.save(backup=False)

    def toggle_favorite(self, item_id: str) -> bool:
        """Toggle an item as favorite. Returns new favorite status."""
        settings = self.get_settings()
        if item_id in settings.favorites:
            settings.favorites.remove(item_id)
            is_favorite = False
        else:
            settings.favorites.append(item_id)
            is_favorite = True
        self.save(backup=False)
        return is_favorite

    # ==================== API Key Management ====================

    def _load_api_keys(self) -> None:
        """Load API keys from file."""
        try:
            if self.api_keys_file.exists():
                with open(self.api_keys_file, "r") as f:
                    data = json.load(f)
                self._api_keys = {
                    k: APIKey.from_dict(v) for k, v in data.items()
                }
        except Exception as e:
            logger.error(f"Failed to load API keys: {e}")
            self._api_keys = {}

    def _save_api_keys(self) -> None:
        """Save API keys to file."""
        try:
            data = {k: v.to_dict() for k, v in self._api_keys.items()}
            with open(self.api_keys_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save API keys: {e}")

    def generate_api_key(
        self,
        description: str = "",
        scopes: Optional[List[str]] = None,
        expires_in_days: Optional[int] = None,
    ) -> tuple[str, str]:
        """
        Generate a new API key.

        Args:
            description: Description of the key's purpose.
            scopes: List of permission scopes.
            expires_in_days: Days until expiration (None for no expiry).

        Returns:
            Tuple of (key_id, plain_text_key).
            The plain text key is only returned once and should be stored securely.
        """
        # Generate key: db_{8 char random}_{32 char secret}
        key_id = f"db_{secrets.token_hex(4)}"
        secret = secrets.token_hex(16)
        plain_key = f"{key_id}_{secret}"

        # Hash the full key for storage
        key_hash = hashlib.sha256(plain_key.encode()).hexdigest()

        # Calculate expiration
        expires_at = None
        if expires_in_days:
            from datetime import timedelta
            expires_at = (datetime.now() + timedelta(days=expires_in_days)).isoformat()

        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            description=description,
            created_at=datetime.now().isoformat(),
            scopes=scopes or ["read", "write"],
            expires_at=expires_at,
        )

        self._api_keys[key_id] = api_key
        self._save_api_keys()

        logger.info(f"Generated API key: {key_id}")
        return key_id, plain_key

    def validate_api_key(self, plain_key: str) -> Optional[APIKey]:
        """
        Validate an API key.

        Args:
            plain_key: The full API key to validate.

        Returns:
            APIKey object if valid, None otherwise.
        """
        try:
            # Extract key_id from plain key (format: db_xxxxxxxx_yyyyyyyy...)
            parts = plain_key.split("_", 2)
            if len(parts) < 3 or parts[0] != "db":
                return None

            key_id = f"{parts[0]}_{parts[1]}"

            if key_id not in self._api_keys:
                return None

            api_key = self._api_keys[key_id]

            # Check if active
            if not api_key.is_active:
                return None

            # Check expiration
            if api_key.expires_at:
                expires = datetime.fromisoformat(api_key.expires_at)
                if datetime.now() > expires:
                    return None

            # Validate hash using timing-safe comparison
            expected_hash = api_key.key_hash
            actual_hash = hashlib.sha256(plain_key.encode()).hexdigest()

            if not hmac.compare_digest(expected_hash, actual_hash):
                return None

            # Update last used
            api_key.last_used_at = datetime.now().isoformat()
            self._save_api_keys()

            return api_key

        except Exception as e:
            logger.error(f"API key validation error: {e}")
            return None

    def list_api_keys(self) -> List[Dict[str, Any]]:
        """List all API keys (without hashes)."""
        return [
            {
                "key_id": k.key_id,
                "description": k.description,
                "created_at": k.created_at,
                "last_used_at": k.last_used_at,
                "expires_at": k.expires_at,
                "is_active": k.is_active,
                "scopes": k.scopes,
            }
            for k in self._api_keys.values()
        ]

    def revoke_api_key(self, key_id: str) -> bool:
        """
        Revoke an API key.

        Args:
            key_id: The key ID to revoke.

        Returns:
            True if revoked, False if not found.
        """
        if key_id in self._api_keys:
            self._api_keys[key_id].is_active = False
            self._save_api_keys()
            logger.info(f"Revoked API key: {key_id}")
            return True
        return False

    def delete_api_key(self, key_id: str) -> bool:
        """
        Delete an API key.

        Args:
            key_id: The key ID to delete.

        Returns:
            True if deleted, False if not found.
        """
        if key_id in self._api_keys:
            del self._api_keys[key_id]
            self._save_api_keys()
            logger.info(f"Deleted API key: {key_id}")
            return True
        return False

    def rotate_api_key(
        self,
        key_id: str,
        description: Optional[str] = None,
    ) -> Optional[tuple[str, str]]:
        """
        Rotate an API key (revoke old, generate new).

        Args:
            key_id: The key ID to rotate.
            description: New description (uses old if not provided).

        Returns:
            Tuple of (new_key_id, new_plain_key) or None if old key not found.
        """
        if key_id not in self._api_keys:
            return None

        old_key = self._api_keys[key_id]
        desc = description or old_key.description

        # Revoke old key
        self.revoke_api_key(key_id)

        # Generate new key
        return self.generate_api_key(description=desc, scopes=old_key.scopes)

    # ==================== Export/Import ====================

    def export_settings(self, include_api_keys: bool = False) -> Dict[str, Any]:
        """
        Export settings as dictionary.

        Args:
            include_api_keys: Include API key metadata (not hashes).

        Returns:
            Settings dictionary.
        """
        data = self.get_settings().to_dict()
        if include_api_keys:
            data["api_keys"] = self.list_api_keys()
        return data

    def import_settings(
        self,
        data: Dict[str, Any],
        merge: bool = True,
    ) -> bool:
        """
        Import settings from dictionary.

        Args:
            data: Settings dictionary.
            merge: Merge with existing (True) or replace (False).

        Returns:
            True if successful.
        """
        try:
            if merge:
                current = self.get_settings().to_dict()
                # Deep merge
                self._deep_merge(current, data)
                self._settings = UserSettings.from_dict(current)
            else:
                self._settings = UserSettings.from_dict(data)

            return self.save()
        except Exception as e:
            logger.error(f"Failed to import settings: {e}")
            return False

    def _deep_merge(self, base: Dict, updates: Dict) -> None:
        """Deep merge updates into base dict."""
        for key, value in updates.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def reset_to_defaults(self) -> bool:
        """Reset all settings to defaults."""
        self._settings = UserSettings()
        return self.save()


# Global instance
_settings_manager: Optional[SettingsManager] = None


def get_settings_manager() -> SettingsManager:
    """Get global settings manager instance."""
    global _settings_manager
    if _settings_manager is None:
        _settings_manager = SettingsManager()
    return _settings_manager
