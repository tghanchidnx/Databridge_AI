"""
SQLAlchemy base classes and mixins for DataBridge AI platform.

Provides common database model functionality used across V3 and V4 applications.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, DateTime, String, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """
    SQLAlchemy declarative base for all DataBridge models.

    Usage:
        from databridge_models import Base

        class MyModel(Base):
            __tablename__ = "my_table"
            id: Mapped[int] = mapped_column(primary_key=True)
    """
    pass


class TimestampMixin:
    """
    Mixin providing created_at and updated_at timestamp columns.

    Usage:
        class MyModel(Base, TimestampMixin):
            __tablename__ = "my_table"
            id: Mapped[int] = mapped_column(primary_key=True)
    """
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )


class AuditMixin(TimestampMixin):
    """
    Mixin providing audit columns (timestamps + user tracking).

    Usage:
        class MyModel(Base, AuditMixin):
            __tablename__ = "my_table"
            id: Mapped[int] = mapped_column(primary_key=True)
    """
    created_by: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True
    )
    updated_by: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True
    )


class SoftDeleteMixin:
    """
    Mixin providing soft delete functionality.

    Usage:
        class MyModel(Base, SoftDeleteMixin):
            __tablename__ = "my_table"
            id: Mapped[int] = mapped_column(primary_key=True)
    """
    is_deleted: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True
    )
    deleted_by: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True
    )

    def soft_delete(self, deleted_by: Optional[str] = None) -> None:
        """Mark the record as deleted."""
        self.is_deleted = True
        self.deleted_at = datetime.utcnow()
        self.deleted_by = deleted_by

    def restore(self) -> None:
        """Restore a soft-deleted record."""
        self.is_deleted = False
        self.deleted_at = None
        self.deleted_by = None
