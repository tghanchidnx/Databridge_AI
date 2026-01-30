"""
Abstract base class for data warehouse connectors.

Defines the interface that all warehouse connectors must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Iterator, Optional


class ConnectorType(str, Enum):
    """Supported connector types."""

    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    REDSHIFT = "redshift"
    SYNAPSE = "synapse"


@dataclass
class ConnectionStatus:
    """Connection status information."""

    connected: bool
    connector_type: ConnectorType
    host: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    last_connected: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class QueryResult:
    """Result of executing a query."""

    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int
    execution_time_ms: float
    query: str
    warnings: list[str] = field(default_factory=list)

    def to_dict_rows(self) -> list[dict[str, Any]]:
        """Convert rows to list of dictionaries."""
        return [dict(zip(self.columns, row)) for row in self.rows]

    def get_column(self, column_name: str) -> list[Any]:
        """Extract a single column as a list."""
        if column_name not in self.columns:
            raise ValueError(f"Column '{column_name}' not found")
        idx = self.columns.index(column_name)
        return [row[idx] for row in self.rows]


@dataclass
class TableMetadata:
    """Metadata for a database table."""

    database: str
    schema: str
    table_name: str
    table_type: str  # TABLE, VIEW, MATERIALIZED VIEW, etc.
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    comment: Optional[str] = None


@dataclass
class ColumnMetadata:
    """Metadata for a table column."""

    database: str
    schema: str
    table_name: str
    column_name: str
    data_type: str
    ordinal_position: int
    is_nullable: bool = True
    default_value: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: Optional[str] = None
    comment: Optional[str] = None

    # Statistics (optional)
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    min_value: Optional[str] = None
    max_value: Optional[str] = None


class DataWarehouseConnector(ABC):
    """
    Abstract base class for data warehouse connectors.

    All warehouse-specific connectors must implement this interface.
    Provides a unified way to:
    - Connect and disconnect
    - Execute SQL queries
    - Retrieve metadata (databases, schemas, tables, columns)
    - Get sample data and statistics
    """

    def __init__(self, name: str):
        """
        Initialize the connector.

        Args:
            name: A unique name for this connection.
        """
        self.name = name
        self._connected = False

    @property
    @abstractmethod
    def connector_type(self) -> ConnectorType:
        """Return the connector type."""
        pass

    @abstractmethod
    def connect(self) -> ConnectionStatus:
        """
        Establish connection to the data warehouse.

        Returns:
            ConnectionStatus: Status of the connection attempt.

        Raises:
            ConnectionError: If connection fails.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the data warehouse."""
        pass

    @abstractmethod
    def test_connection(self) -> ConnectionStatus:
        """
        Test the connection without maintaining it.

        Returns:
            ConnectionStatus: Status of the connection test.
        """
        pass

    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connected

    @abstractmethod
    def execute(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query to execute.
            params: Optional parameters for parameterized queries.
            limit: Optional row limit (added to query if not present).

        Returns:
            QueryResult: Query results with metadata.

        Raises:
            ConnectionError: If not connected.
            QueryError: If query execution fails.
        """
        pass

    def execute_many(
        self,
        query: str,
        params_list: list[dict[str, Any]],
    ) -> int:
        """
        Execute a SQL query with multiple parameter sets.

        Args:
            query: SQL query to execute.
            params_list: List of parameter dictionaries.

        Returns:
            int: Number of rows affected.

        Raises:
            ConnectionError: If not connected.
            QueryError: If query execution fails.
        """
        raise NotImplementedError("execute_many not implemented for this connector")

    @abstractmethod
    def get_databases(self) -> list[str]:
        """
        Get list of accessible databases.

        Returns:
            list[str]: Database names.
        """
        pass

    @abstractmethod
    def get_schemas(self, database: Optional[str] = None) -> list[str]:
        """
        Get list of schemas in a database.

        Args:
            database: Database name (uses current if None).

        Returns:
            list[str]: Schema names.
        """
        pass

    @abstractmethod
    def get_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_views: bool = True,
    ) -> list[TableMetadata]:
        """
        Get list of tables in a schema.

        Args:
            database: Database name (uses current if None).
            schema: Schema name (uses current if None).
            include_views: Whether to include views.

        Returns:
            list[TableMetadata]: Table metadata.
        """
        pass

    @abstractmethod
    def get_columns(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> list[ColumnMetadata]:
        """
        Get column metadata for a table.

        Args:
            table_name: Name of the table.
            database: Database name (uses current if None).
            schema: Schema name (uses current if None).

        Returns:
            list[ColumnMetadata]: Column metadata.
        """
        pass

    def get_sample_data(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 100,
    ) -> QueryResult:
        """
        Get sample rows from a table.

        Args:
            table_name: Name of the table.
            database: Database name (uses current if None).
            schema: Schema name (uses current if None).
            limit: Maximum rows to return.

        Returns:
            QueryResult: Sample data.
        """
        full_name = self._build_qualified_name(table_name, database, schema)
        query = f"SELECT * FROM {full_name}"
        return self.execute(query, limit=limit)

    def get_table_statistics(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get statistics for a table.

        Args:
            table_name: Name of the table.
            database: Database name (uses current if None).
            schema: Schema name (uses current if None).

        Returns:
            dict: Table statistics (row count, size, etc.).
        """
        full_name = self._build_qualified_name(table_name, database, schema)
        result = self.execute(f"SELECT COUNT(*) as row_count FROM {full_name}")
        return {"row_count": result.rows[0][0] if result.rows else 0}

    def stream_results(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> Iterator[list[tuple[Any, ...]]]:
        """
        Stream query results in batches.

        Args:
            query: SQL query to execute.
            params: Optional parameters.
            batch_size: Number of rows per batch.

        Yields:
            list[tuple]: Batch of rows.
        """
        # Default implementation - subclasses can override for true streaming
        result = self.execute(query, params)
        for i in range(0, len(result.rows), batch_size):
            yield result.rows[i : i + batch_size]

    def _build_qualified_name(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        """
        Build a fully qualified table name.

        Args:
            table_name: Table name.
            database: Database name.
            schema: Schema name.

        Returns:
            str: Fully qualified name (database.schema.table).
        """
        parts = []
        if database:
            parts.append(self._quote_identifier(database))
        if schema:
            parts.append(self._quote_identifier(schema))
        parts.append(self._quote_identifier(table_name))
        return ".".join(parts)

    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote an identifier for safe use in SQL.

        Default implementation uses double quotes.
        Subclasses can override for dialect-specific quoting.

        Args:
            identifier: Identifier to quote.

        Returns:
            str: Quoted identifier.
        """
        # Escape any existing double quotes
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def __enter__(self):
        """Context manager entry - connect."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - disconnect."""
        self.disconnect()
        return False
