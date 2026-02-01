"""
PostgreSQL connector implementation.

Provides connectivity to PostgreSQL databases using psycopg2.
"""

import time
from datetime import datetime
from typing import Any, Optional

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

from .base import (
    DataWarehouseConnector,
    ConnectorType,
    ConnectionStatus,
    QueryResult,
    TableMetadata,
    ColumnMetadata,
)


class PostgreSQLConnector(DataWarehouseConnector):
    """
    PostgreSQL database connector.

    Supports both direct connection and connection pooling.
    """

    def __init__(
        self,
        name: str,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: Optional[str] = None,
        schema: str = "public",
        connect_timeout: int = 10,
        application_name: str = "DataBridge Analytics",
    ):
        """
        Initialize PostgreSQL connector.

        Args:
            name: Connection name.
            host: Database host.
            port: Database port.
            database: Database name.
            user: Username.
            password: Password.
            schema: Default schema.
            connect_timeout: Connection timeout in seconds.
            application_name: Application name for pg_stat_activity.
        """
        super().__init__(name)

        if not PSYCOPG2_AVAILABLE:
            raise ImportError(
                "psycopg2 is required for PostgreSQL connector. "
                "Install with: pip install psycopg2-binary"
            )

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self.connect_timeout = connect_timeout
        self.application_name = application_name

        self._connection = None
        self._last_connected: Optional[datetime] = None

    @property
    def connector_type(self) -> ConnectorType:
        """Return the connector type."""
        return ConnectorType.POSTGRESQL

    def connect(self) -> ConnectionStatus:
        """
        Establish connection to PostgreSQL.

        Returns:
            ConnectionStatus: Connection status.
        """
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=self.connect_timeout,
                application_name=self.application_name,
            )
            self._connected = True
            self._last_connected = datetime.now()

            # Set default schema
            with self._connection.cursor() as cursor:
                cursor.execute(f"SET search_path TO {self.schema}, public")
            self._connection.commit()

            return ConnectionStatus(
                connected=True,
                connector_type=self.connector_type,
                host=self.host,
                database=self.database,
                schema=self.schema,
                user=self.user,
                last_connected=self._last_connected,
            )
        except psycopg2.Error as e:
            self._connected = False
            return ConnectionStatus(
                connected=False,
                connector_type=self.connector_type,
                host=self.host,
                database=self.database,
                error=str(e),
            )

    def disconnect(self) -> None:
        """Close the PostgreSQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
        self._connected = False

    def test_connection(self) -> ConnectionStatus:
        """
        Test the connection without maintaining it.

        Returns:
            ConnectionStatus: Connection test result.
        """
        status = self.connect()
        if status.connected:
            self.disconnect()
        return status

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
            limit: Optional row limit.

        Returns:
            QueryResult: Query results.

        Raises:
            ConnectionError: If not connected.
        """
        if not self._connected or not self._connection:
            raise ConnectionError("Not connected to PostgreSQL")

        # Add limit if specified and not already in query
        if limit and "LIMIT" not in query.upper():
            query = f"{query.rstrip(';')} LIMIT {limit}"

        start_time = time.time()
        warnings = []

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, params)

                # Get column names
                columns = []
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]

                # Fetch results
                rows = cursor.fetchall() if cursor.description else []

                # Check for notices/warnings
                if self._connection.notices:
                    warnings = list(self._connection.notices)
                    self._connection.notices.clear()

            self._connection.commit()
            execution_time = (time.time() - start_time) * 1000

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                execution_time_ms=execution_time,
                query=query,
                warnings=warnings,
            )

        except psycopg2.Error as e:
            self._connection.rollback()
            raise RuntimeError(f"Query execution failed: {e}")

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
        """
        if not self._connected or not self._connection:
            raise ConnectionError("Not connected to PostgreSQL")

        try:
            with self._connection.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, params_list)
                row_count = cursor.rowcount
            self._connection.commit()
            return row_count
        except psycopg2.Error as e:
            self._connection.rollback()
            raise RuntimeError(f"Batch execution failed: {e}")

    def get_databases(self) -> list[str]:
        """
        Get list of accessible databases.

        Returns:
            list[str]: Database names.
        """
        query = """
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
              AND has_database_privilege(datname, 'CONNECT')
            ORDER BY datname
        """
        result = self.execute(query)
        return [row[0] for row in result.rows]

    def get_schemas(self, database: Optional[str] = None) -> list[str]:
        """
        Get list of schemas in the current database.

        Args:
            database: Ignored for PostgreSQL (uses current connection).

        Returns:
            list[str]: Schema names.
        """
        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'pg_%'
              AND schema_name != 'information_schema'
            ORDER BY schema_name
        """
        result = self.execute(query)
        return [row[0] for row in result.rows]

    def get_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_views: bool = True,
    ) -> list[TableMetadata]:
        """
        Get list of tables in a schema.

        Args:
            database: Ignored (uses current connection).
            schema: Schema name (uses default if None).
            include_views: Whether to include views.

        Returns:
            list[TableMetadata]: Table metadata.
        """
        schema = schema or self.schema

        table_types = ["BASE TABLE"]
        if include_views:
            table_types.extend(["VIEW", "MATERIALIZED VIEW"])

        type_list = ", ".join(f"'{t}'" for t in table_types)

        query = f"""
            SELECT
                table_catalog as database,
                table_schema as schema,
                table_name,
                table_type,
                obj_description((quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass, 'pg_class') as comment
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type IN ({type_list})
            ORDER BY table_name
        """

        result = self.execute(query, {"schema": schema})

        tables = []
        for row in result.rows:
            tables.append(
                TableMetadata(
                    database=row[0],
                    schema=row[1],
                    table_name=row[2],
                    table_type=row[3],
                    comment=row[4],
                )
            )

        return tables

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
            database: Ignored (uses current connection).
            schema: Schema name (uses default if None).

        Returns:
            list[ColumnMetadata]: Column metadata.
        """
        schema = schema or self.schema

        query = """
            SELECT
                c.table_catalog,
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.ordinal_position,
                c.is_nullable = 'YES' as is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
                CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
                fk.foreign_table_schema || '.' || fk.foreign_table_name || '.' || fk.foreign_column_name as fk_ref,
                col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position) as comment
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name, kcu.table_schema, kcu.table_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name
                AND c.table_schema = pk.table_schema
                AND c.table_name = pk.table_name
            LEFT JOIN (
                SELECT
                    kcu.column_name,
                    kcu.table_schema,
                    kcu.table_name,
                    ccu.table_schema as foreign_table_schema,
                    ccu.table_name as foreign_table_name,
                    ccu.column_name as foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
            ) fk ON c.column_name = fk.column_name
                AND c.table_schema = fk.table_schema
                AND c.table_name = fk.table_name
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """

        result = self.execute(query, {"schema": schema, "table_name": table_name})

        columns = []
        for row in result.rows:
            columns.append(
                ColumnMetadata(
                    database=row[0],
                    schema=row[1],
                    table_name=row[2],
                    column_name=row[3],
                    data_type=row[4],
                    ordinal_position=row[5],
                    is_nullable=row[6],
                    default_value=row[7],
                    is_primary_key=row[8],
                    is_foreign_key=row[9],
                    foreign_key_ref=row[10],
                    comment=row[11],
                )
            )

        return columns

    def get_table_statistics(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get detailed statistics for a table.

        Args:
            table_name: Name of the table.
            database: Ignored (uses current connection).
            schema: Schema name (uses default if None).

        Returns:
            dict: Table statistics.
        """
        schema = schema or self.schema

        # Get row count and table size
        query = """
            SELECT
                pg_total_relation_size(c.oid) as total_bytes,
                pg_table_size(c.oid) as table_bytes,
                pg_indexes_size(c.oid) as index_bytes,
                reltuples::bigint as estimated_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
        """

        result = self.execute(query, {"schema": schema, "table_name": table_name})

        if result.rows:
            row = result.rows[0]
            return {
                "table_name": table_name,
                "schema": schema,
                "total_bytes": row[0],
                "table_bytes": row[1],
                "index_bytes": row[2],
                "estimated_rows": row[3],
            }
        return {"table_name": table_name, "schema": schema}

    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote an identifier for PostgreSQL.

        PostgreSQL uses double quotes for identifiers.
        """
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'
