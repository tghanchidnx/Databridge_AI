"""
Database Connectors module for DataBridge Analytics V4.

Provides unified interface for connecting to various data warehouses:
- Snowflake
- Databricks
- SQL Server
- PostgreSQL
- MySQL
- Oracle
- AWS Redshift
- Azure Synapse
"""

from .base import (
    DataWarehouseConnector,
    ConnectorType,
    ConnectionStatus,
    QueryResult,
    TableMetadata,
    ColumnMetadata,
)
from .factory import ConnectorFactory
from .postgresql import PostgreSQLConnector

__all__ = [
    # Base classes
    "DataWarehouseConnector",
    "ConnectorType",
    "ConnectionStatus",
    "QueryResult",
    "TableMetadata",
    "ColumnMetadata",
    # Factory
    "ConnectorFactory",
    # Connectors
    "PostgreSQLConnector",
]
