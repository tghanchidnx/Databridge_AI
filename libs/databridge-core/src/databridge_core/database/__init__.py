"""
DataBridge Core Database Module.

Provides SQLAlchemy session management and database utilities
for both V3 and V4 applications.
"""

from databridge_core.database.session import (
    get_engine,
    get_session,
    session_scope,
    init_database,
    DatabaseManager,
)

__all__ = [
    "get_engine",
    "get_session",
    "session_scope",
    "init_database",
    "DatabaseManager",
]
