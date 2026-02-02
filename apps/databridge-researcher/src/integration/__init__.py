"""
Integration module for DataBridge Analytics Researcher.

Handles integration with Librarian Hierarchy Builder:
- LibrarianHierarchyClient for fetching hierarchy structures
- DimensionMapper for mapping Librarian hierarchies to Researcher dimensions
- Shared configuration and credentials
"""

from .librarian_client import (
    LibrarianHierarchyClient,
    LibrarianConnectionMode,
    LibrarianProject,
    LibrarianHierarchy,
    LibrarianMapping,
    LibrarianClientResult,
)

from .dimension_mapper import (
    DimensionMapper,
    DimensionType,
    Dimension,
    DimensionAttribute,
    DimensionMember,
    DimensionMapperResult,
)

__all__ = [
    # Librarian Client
    "LibrarianHierarchyClient",
    "LibrarianConnectionMode",
    "LibrarianProject",
    "LibrarianHierarchy",
    "LibrarianMapping",
    "LibrarianClientResult",
    # Dimension Mapper
    "DimensionMapper",
    "DimensionType",
    "Dimension",
    "DimensionAttribute",
    "DimensionMember",
    "DimensionMapperResult",
]
