"""
Bridge modules for Unified AI Agent.

- LibrarianBridge: Book ↔ Librarian conversion and sync
- ResearcherBridge: Analytics and validation via Researcher API
"""

from .librarian_bridge import LibrarianBridge
from .researcher_bridge import ResearcherBridge

__all__ = ["LibrarianBridge", "ResearcherBridge"]
