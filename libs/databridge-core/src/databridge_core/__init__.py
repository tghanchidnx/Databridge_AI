"""
DataBridge Core - Shared utilities for DataBridge AI platform.

This library provides common functionality used by both Librarian (Librarian)
and Researcher (Researcher) applications:

- config: Configuration and settings management
- database: SQLAlchemy session management and base connectors
- credentials: Secure credential management
- audit: Audit trail logging
- cli: Rich console utilities and formatters
- mcp: MCP server utilities and tool helpers
"""

__version__ = "1.0.0"

# Config exports
from databridge_core.config import (
    BaseAppSettings,
    DatabaseSettings,
    SnowflakeSettings,
    PostgreSQLSettings,
    SecuritySettings,
    get_settings,
    reload_settings,
)

# Database exports
from databridge_core.database import (
    DatabaseManager,
    get_engine,
    get_session,
    session_scope,
    init_database,
)

# CLI exports
from databridge_core.cli import (
    console,
    get_console,
    format_table,
    format_dict,
    format_error,
    format_success,
)

# Audit exports
from databridge_core.audit import (
    AuditLogger,
    get_audit_logger,
    log_action,
)

# Credentials exports
from databridge_core.credentials import (
    CredentialManager,
    get_credential_manager,
)

# MCP exports
from databridge_core.mcp import (
    create_mcp_server,
    truncate_for_llm,
    format_tool_response,
)

__all__ = [
    "__version__",
    # Config
    "BaseAppSettings",
    "DatabaseSettings",
    "SnowflakeSettings",
    "PostgreSQLSettings",
    "SecuritySettings",
    "get_settings",
    "reload_settings",
    # Database
    "DatabaseManager",
    "get_engine",
    "get_session",
    "session_scope",
    "init_database",
    # CLI
    "console",
    "get_console",
    "format_table",
    "format_dict",
    "format_error",
    "format_success",
    # Audit
    "AuditLogger",
    "get_audit_logger",
    "log_action",
    # Credentials
    "CredentialManager",
    "get_credential_manager",
    # MCP
    "create_mcp_server",
    "truncate_for_llm",
    "format_tool_response",
]
