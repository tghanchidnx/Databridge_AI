"""
Integration module for DataBridge Discovery.

Provides synchronization capabilities with:
- The Librarian (hierarchy management)
- The Researcher (analytics engine)
"""

from databridge_discovery.integration.librarian_sync import (
    LibrarianSync,
    LibrarianSyncConfig,
    LibrarianSyncResult,
)
from databridge_discovery.integration.researcher_sync import (
    ResearcherSync,
    ResearcherSyncConfig,
    ResearcherSyncResult,
)

__all__ = [
    "LibrarianSync",
    "LibrarianSyncConfig",
    "LibrarianSyncResult",
    "ResearcherSync",
    "ResearcherSyncConfig",
    "ResearcherSyncResult",
]
