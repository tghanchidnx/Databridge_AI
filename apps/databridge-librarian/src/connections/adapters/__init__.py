"""
Database adapter implementations.

Provides concrete implementations of AbstractDatabaseAdapter for various
database platforms. Each adapter provides consistent connection management,
schema introspection, and query execution.

Available Adapters:
    - SnowflakeAdapter: Snowflake data warehouse (implemented)
    - PostgreSQLAdapter: PostgreSQL databases (planned)
    - MySQLAdapter: MySQL/MariaDB databases (planned)
    - SQLServerAdapter: SQL Server databases (planned)
"""

from ..base import AbstractDatabaseAdapter, ColumnInfo, QueryResult, TableInfo
from .snowflake import SnowflakeAdapter

# Placeholders for future adapters
# from .postgresql import PostgreSQLAdapter
# from .mysql import MySQLAdapter
# from .sqlserver import SQLServerAdapter

__all__ = [
    # Base classes
    "AbstractDatabaseAdapter",
    "ColumnInfo",
    "QueryResult",
    "TableInfo",
    # Implemented adapters
    "SnowflakeAdapter",
    # Planned adapters (uncomment when implemented)
    # "PostgreSQLAdapter",
    # "MySQLAdapter",
    # "SQLServerAdapter",
]
