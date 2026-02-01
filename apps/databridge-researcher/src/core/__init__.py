"""Core module for DataBridge AI V4 Analytics Engine."""

from .config import Settings, get_settings

from .settings_manager import (
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

__all__ = [
    # Config (env-based)
    "Settings",
    "get_settings",
    # Settings Manager (JSON persistence)
    "SettingsManager",
    "UserSettings",
    "GeneralSettings",
    "ConnectionSettings",
    "WorkflowPreferences",
    "SecuritySettings",
    "NotionSettings",
    "APIKey",
    "get_settings_manager",
]
