"""
Sync Module for DataBridge Researcher.

Handles synchronization with Librarian:
- Event consumption and handling
- Dimension updates from hierarchy changes
- Knowledge base population
- Cache invalidation
"""

from .handlers import (
    SyncHandler,
    HierarchySyncHandler,
    DimensionSyncHandler,
    CacheInvalidationHandler,
)
from .manager import (
    SyncManager,
    SyncStatus,
    SyncResult,
    get_sync_manager,
)

__all__ = [
    # Handlers
    "SyncHandler",
    "HierarchySyncHandler",
    "DimensionSyncHandler",
    "CacheInvalidationHandler",
    # Manager
    "SyncManager",
    "SyncStatus",
    "SyncResult",
    "get_sync_manager",
]
