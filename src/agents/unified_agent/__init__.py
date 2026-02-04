"""
Unified AI Agent for DataBridge AI.

This module provides a single AI agent that operates across Book (Python),
Librarian (NestJS), and Researcher (NestJS) components, enabling seamless
workflows like "analyze Book → promote to Librarian → validate with Researcher".

Components:
- UnifiedAgentContext: Tracks state across all three systems
- LibrarianBridge: Converts between Book and Librarian data models
- ResearcherBridge: Provides analytics on Book/Librarian data
- MCP Tools: 10 new tools for unified operations
"""

from .context import UnifiedAgentContext
from .bridges.librarian_bridge import LibrarianBridge
from .bridges.researcher_bridge import ResearcherBridge

__all__ = [
    "UnifiedAgentContext",
    "LibrarianBridge",
    "ResearcherBridge",
]
