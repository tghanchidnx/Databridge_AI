"""
SQLAlchemy session management for DataBridge AI platform.

Provides database connection management, session handling, and common utilities
that can be used by both Librarian and Researcher applications.
"""

from contextlib import contextmanager
from typing import Optional, Any, Generator, Type

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase


class DatabaseManager:
    """
    Database connection and session manager.

    Provides a unified interface for managing database connections
    and sessions across different applications.
    """

    def __init__(
        self,
        database_url: str,
        echo: bool = False,
        pool_size: int = 5,
        base: Optional[Type[DeclarativeBase]] = None,
    ):
        """
        Initialize the database manager.

        Args:
            database_url: SQLAlchemy database URL.
            echo: Echo SQL statements to stdout.
            pool_size: Connection pool size.
            base: Optional SQLAlchemy declarative base for schema operations.
        """
        self._database_url = database_url
        self._echo = echo
        self._pool_size = pool_size
        self._base = base
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    @property
    def engine(self) -> Engine:
        """Get or create the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self._database_url,
                echo=self._echo,
                pool_pre_ping=True,
            )
            # Enable foreign key support for SQLite
            if "sqlite" in self._database_url:
                @event.listens_for(self._engine, "connect")
                def set_sqlite_pragma(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    cursor.execute("PRAGMA foreign_keys=ON")
                    cursor.close()

        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get or create the session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine,
            )
        return self._session_factory

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.session_factory()

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.

        Provides automatic commit on success and rollback on error.

        Yields:
            Session: Database session.
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def init_database(self) -> None:
        """Initialize the database schema (create all tables)."""
        if self._base is not None:
            self._base.metadata.create_all(self.engine)

    def reset_database(self) -> None:
        """Reset the database (drop and recreate all tables)."""
        if self._base is not None:
            self._base.metadata.drop_all(self.engine)
            self._base.metadata.create_all(self.engine)

    def dispose(self) -> None:
        """Dispose of the engine and close all connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None


# Global default manager (set by applications)
_default_manager: Optional[DatabaseManager] = None


def set_default_manager(manager: DatabaseManager) -> None:
    """Set the default database manager."""
    global _default_manager
    _default_manager = manager


def get_default_manager() -> Optional[DatabaseManager]:
    """Get the default database manager."""
    return _default_manager


def get_engine() -> Engine:
    """
    Get the SQLAlchemy engine from the default manager.

    Returns:
        Engine: SQLAlchemy database engine.

    Raises:
        RuntimeError: If no default manager is configured.
    """
    if _default_manager is None:
        raise RuntimeError("No default database manager configured. Call set_default_manager() first.")
    return _default_manager.engine


def get_session() -> Session:
    """
    Get a new database session from the default manager.

    Returns:
        Session: New SQLAlchemy session.

    Raises:
        RuntimeError: If no default manager is configured.
    """
    if _default_manager is None:
        raise RuntimeError("No default database manager configured. Call set_default_manager() first.")
    return _default_manager.get_session()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Context manager for database sessions using the default manager.

    Yields:
        Session: Database session.

    Raises:
        RuntimeError: If no default manager is configured.
    """
    if _default_manager is None:
        raise RuntimeError("No default database manager configured. Call set_default_manager() first.")
    with _default_manager.session_scope() as session:
        yield session


def init_database() -> None:
    """Initialize the database schema using the default manager."""
    if _default_manager is None:
        raise RuntimeError("No default database manager configured. Call set_default_manager() first.")
    _default_manager.init_database()
