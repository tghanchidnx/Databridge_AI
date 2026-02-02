"""
DataBridge Models - Shared data models for DataBridge AI platform.

This library provides common data models used by both Librarian (Librarian)
and Researcher (Researcher) applications:

- base: SQLAlchemy declarative base and common mixins
- enums: Shared enumerations (SQLDialect, TableStatus, etc.)
- schemas: Shared Pydantic schemas for validation
- types: Common type definitions
"""

__version__ = "1.0.0"

from databridge_models.enums import (
    SQLDialect,
    TableStatus,
    AggregationType,
    JoinType,
    FormulaType,
)
from databridge_models.base import Base, TimestampMixin, AuditMixin

__all__ = [
    "__version__",
    # Enums
    "SQLDialect",
    "TableStatus",
    "AggregationType",
    "JoinType",
    "FormulaType",
    # Base classes
    "Base",
    "TimestampMixin",
    "AuditMixin",
]
