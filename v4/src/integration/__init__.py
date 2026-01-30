"""
Integration module for DataBridge Analytics V4.

Handles integration with V3 Hierarchy Builder:
- V3HierarchyClient for fetching hierarchy structures
- DimensionMapper for mapping V3 hierarchies to V4 dimensions
- Shared configuration and credentials
"""

from .v3_client import (
    V3HierarchyClient,
    V3ConnectionMode,
    V3Project,
    V3Hierarchy,
    V3Mapping,
    V3ClientResult,
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
    # V3 Client
    "V3HierarchyClient",
    "V3ConnectionMode",
    "V3Project",
    "V3Hierarchy",
    "V3Mapping",
    "V3ClientResult",
    # Dimension Mapper
    "DimensionMapper",
    "DimensionType",
    "Dimension",
    "DimensionAttribute",
    "DimensionMember",
    "DimensionMapperResult",
]
