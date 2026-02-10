"""
Unified Agent Context - Tracks state across Book, Librarian, and Researcher.

This module maintains the context for unified operations across all three
systems, including:
- Active Book instance
- Active Librarian project ID
- Database connection IDs for Researcher
- Operation history for undo/redo
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("unified_agent")


@dataclass
class UnifiedOperation:
    """Represents an operation performed across systems."""

    timestamp: str
    operation: str  # e.g., "checkout", "promote", "sync", "analyze"
    source_system: str  # "book", "librarian", "researcher"
    target_system: Optional[str]  # "book", "librarian", "researcher", or None
    details: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UnifiedOperation":
        return cls(**data)


@dataclass
class BookReference:
    """Reference to a Book instance with metadata."""

    name: str
    data_version: str
    last_updated: str
    root_node_count: int
    total_nodes: int
    source_project_id: Optional[str] = None  # Librarian project if checked out

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BookReference":
        return cls(**data)


class UnifiedAgentContext:
    """
    Manages state and context for unified operations across Book, Librarian, and Researcher.

    This context is persisted to disk and can be saved/loaded between sessions.
    """

    def __init__(self, data_dir: Optional[Path] = None):
        """
        Initialize the unified agent context.

        Args:
            data_dir: Directory for persistence (defaults to ./data)
        """
        self.data_dir = data_dir or Path(__file__).parent.parent.parent.parent / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Context file path
        self._context_file = self.data_dir / "unified_agent_context.json"

        # In-memory state
        self._active_books: Dict[str, Any] = {}  # name -> Book instance
        self._book_refs: Dict[str, BookReference] = {}  # name -> BookReference
        self._active_project_id: Optional[str] = None
        self._connection_ids: Dict[str, str] = {}  # alias -> connection_id
        self._operation_history: List[UnifiedOperation] = []

        # Configuration
        self._librarian_base_url: str = "http://localhost:8001/api"
        self._librarian_api_key: str = "dev-key-1"
        self._researcher_base_url: str = "http://localhost:8001/api"
        self._researcher_api_key: str = "dev-key-1"

        # Load persisted state
        self._load()

    # =========================================================================
    # Book Management
    # =========================================================================

    def register_book(self, book: Any, source_project_id: Optional[str] = None) -> str:
        """
        Register a Book instance in the context.

        Args:
            book: Book instance (from Book.book.models)
            source_project_id: Librarian project ID if checked out

        Returns:
            Book name
        """
        name = book.name
        self._active_books[name] = book

        # Count total nodes
        total_nodes = self._count_nodes(book.root_nodes)

        self._book_refs[name] = BookReference(
            name=name,
            data_version=book.data_version,
            last_updated=book.last_updated.isoformat() if hasattr(book.last_updated, 'isoformat') else str(book.last_updated),
            root_node_count=len(book.root_nodes),
            total_nodes=total_nodes,
            source_project_id=source_project_id,
        )

        self._save()
        logger.info(f"Registered Book '{name}' with {total_nodes} nodes")
        return name

    def _count_nodes(self, nodes: List[Any]) -> int:
        """Recursively count all nodes."""
        count = len(nodes)
        for node in nodes:
            if hasattr(node, 'children') and node.children:
                count += self._count_nodes(node.children)
        return count

    def get_book(self, name: str) -> Optional[Any]:
        """Get a registered Book by name."""
        return self._active_books.get(name)

    def get_book_ref(self, name: str) -> Optional[BookReference]:
        """Get a BookReference by name."""
        return self._book_refs.get(name)

    def list_books(self) -> List[Dict[str, Any]]:
        """List all registered Books."""
        return [ref.to_dict() for ref in self._book_refs.values()]

    def unregister_book(self, name: str) -> bool:
        """Remove a Book from the context."""
        if name in self._active_books:
            del self._active_books[name]
        if name in self._book_refs:
            del self._book_refs[name]
            self._save()
            return True
        return False

    # =========================================================================
    # Librarian Project Management
    # =========================================================================

    @property
    def active_project_id(self) -> Optional[str]:
        """Get the active Librarian project ID."""
        return self._active_project_id

    @active_project_id.setter
    def active_project_id(self, project_id: Optional[str]):
        """Set the active Librarian project ID."""
        self._active_project_id = project_id
        self._save()

    # =========================================================================
    # Connection Management
    # =========================================================================

    def set_connection(self, alias: str, connection_id: str):
        """
        Set a database connection ID with an alias.

        Args:
            alias: Short name (e.g., "snowflake", "postgres")
            connection_id: Backend connection ID
        """
        self._connection_ids[alias] = connection_id
        self._save()

    def get_connection(self, alias: str) -> Optional[str]:
        """Get a connection ID by alias."""
        return self._connection_ids.get(alias)

    def list_connections(self) -> Dict[str, str]:
        """List all registered connections."""
        return dict(self._connection_ids)

    # =========================================================================
    # API Configuration
    # =========================================================================

    @property
    def librarian_config(self) -> Dict[str, str]:
        """Get Librarian API configuration."""
        return {
            "base_url": self._librarian_base_url,
            "api_key": self._librarian_api_key,
        }

    @property
    def researcher_config(self) -> Dict[str, str]:
        """Get Researcher API configuration."""
        return {
            "base_url": self._researcher_base_url,
            "api_key": self._researcher_api_key,
        }

    def configure_librarian(self, base_url: str, api_key: str):
        """Configure Librarian API connection."""
        self._librarian_base_url = base_url
        self._librarian_api_key = api_key
        self._save()

    def configure_researcher(self, base_url: str, api_key: str):
        """Configure Researcher API connection."""
        self._researcher_base_url = base_url
        self._researcher_api_key = api_key
        self._save()

    # =========================================================================
    # Operation History
    # =========================================================================

    def record_operation(
        self,
        operation: str,
        source_system: str,
        target_system: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        success: bool = True,
        error_message: Optional[str] = None,
    ):
        """
        Record an operation in the history.

        Args:
            operation: Operation name
            source_system: Source system (book, librarian, researcher)
            target_system: Target system if applicable
            details: Operation details
            success: Whether the operation succeeded
            error_message: Error message if failed
        """
        op = UnifiedOperation(
            timestamp=datetime.now(timezone.utc).isoformat(),
            operation=operation,
            source_system=source_system,
            target_system=target_system,
            details=details or {},
            success=success,
            error_message=error_message,
        )
        self._operation_history.append(op)

        # Keep only last 100 operations
        if len(self._operation_history) > 100:
            self._operation_history = self._operation_history[-100:]

        self._save()

    def get_operation_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent operation history."""
        return [op.to_dict() for op in self._operation_history[-limit:]]

    # =========================================================================
    # Context Summary
    # =========================================================================

    def get_context_summary(self) -> Dict[str, Any]:
        """Get a summary of the current context state."""
        return {
            "active_books": self.list_books(),
            "active_project_id": self._active_project_id,
            "connections": self._connection_ids,
            "librarian_url": self._librarian_base_url,
            "researcher_url": self._researcher_base_url,
            "recent_operations": self.get_operation_history(5),
        }

    # =========================================================================
    # Persistence
    # =========================================================================

    def _save(self):
        """Save context state to disk."""
        state = {
            "book_refs": {k: v.to_dict() for k, v in self._book_refs.items()},
            "active_project_id": self._active_project_id,
            "connection_ids": self._connection_ids,
            "librarian_base_url": self._librarian_base_url,
            "librarian_api_key": self._librarian_api_key,
            "researcher_base_url": self._researcher_base_url,
            "researcher_api_key": self._researcher_api_key,
            "operation_history": [op.to_dict() for op in self._operation_history[-50:]],
        }

        try:
            with open(self._context_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save context: {e}")

    def _load(self):
        """Load context state from disk."""
        if not self._context_file.exists():
            return

        try:
            with open(self._context_file, 'r') as f:
                state = json.load(f)

            self._book_refs = {
                k: BookReference.from_dict(v)
                for k, v in state.get("book_refs", {}).items()
            }
            self._active_project_id = state.get("active_project_id")
            self._connection_ids = state.get("connection_ids", {})
            self._librarian_base_url = state.get("librarian_base_url", self._librarian_base_url)
            self._librarian_api_key = state.get("librarian_api_key", self._librarian_api_key)
            self._researcher_base_url = state.get("researcher_base_url", self._researcher_base_url)
            self._researcher_api_key = state.get("researcher_api_key", self._researcher_api_key)
            self._operation_history = [
                UnifiedOperation.from_dict(op)
                for op in state.get("operation_history", [])
            ]

            logger.info(f"Loaded context with {len(self._book_refs)} book refs")
        except Exception as e:
            logger.error(f"Failed to load context: {e}")

    def clear(self):
        """Clear all context state."""
        self._active_books.clear()
        self._book_refs.clear()
        self._active_project_id = None
        self._connection_ids.clear()
        self._operation_history.clear()
        self._save()
        logger.info("Context cleared")


# Global context instance
_context: Optional[UnifiedAgentContext] = None


def get_context() -> UnifiedAgentContext:
    """Get the global UnifiedAgentContext instance."""
    global _context
    if _context is None:
        _context = UnifiedAgentContext()
    return _context
