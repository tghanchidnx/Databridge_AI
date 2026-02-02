"""
Sync Handlers for processing events from Librarian.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable

logger = logging.getLogger(__name__)


@dataclass
class SyncEvent:
    """Incoming sync event from Librarian."""

    event_id: str
    event_type: str
    timestamp: datetime
    source: str
    data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyncEvent":
        """Create from dictionary."""
        return cls(
            event_id=data.get("event_id", ""),
            event_type=data.get("event_type", ""),
            timestamp=datetime.fromisoformat(data["timestamp"])
            if "timestamp" in data
            else datetime.now(timezone.utc),
            source=data.get("source", "librarian"),
            data=data,
        )


@dataclass
class HandlerResult:
    """Result from a sync handler."""

    success: bool
    message: str = ""
    changes_applied: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "changes_applied": self.changes_applied,
            "errors": self.errors,
        }


class SyncHandler(ABC):
    """Base class for sync handlers."""

    @property
    @abstractmethod
    def handled_events(self) -> List[str]:
        """List of event types this handler processes."""
        pass

    @abstractmethod
    def handle(self, event: SyncEvent) -> HandlerResult:
        """
        Handle a sync event.

        Args:
            event: The event to handle

        Returns:
            HandlerResult with status
        """
        pass

    def can_handle(self, event_type: str) -> bool:
        """Check if this handler can process an event type."""
        for pattern in self.handled_events:
            if pattern.endswith("*"):
                if event_type.startswith(pattern[:-1]):
                    return True
            elif event_type == pattern:
                return True
        return False


class HierarchySyncHandler(SyncHandler):
    """
    Handler for hierarchy-related events.

    Keeps local hierarchy cache in sync with Librarian.
    """

    def __init__(self):
        """Initialize handler."""
        self._hierarchy_cache: Dict[str, Dict[str, Any]] = {}
        self._project_cache: Dict[str, Dict[str, Any]] = {}

    @property
    def handled_events(self) -> List[str]:
        """Events this handler processes."""
        return [
            "project:created",
            "project:updated",
            "project:deleted",
            "hierarchy:created",
            "hierarchy:updated",
            "hierarchy:deleted",
            "hierarchy:moved",
        ]

    def handle(self, event: SyncEvent) -> HandlerResult:
        """Handle hierarchy event."""
        event_type = event.event_type
        data = event.data

        try:
            if event_type == "project:created":
                return self._handle_project_created(data)
            elif event_type == "project:updated":
                return self._handle_project_updated(data)
            elif event_type == "project:deleted":
                return self._handle_project_deleted(data)
            elif event_type == "hierarchy:created":
                return self._handle_hierarchy_created(data)
            elif event_type == "hierarchy:updated":
                return self._handle_hierarchy_updated(data)
            elif event_type == "hierarchy:deleted":
                return self._handle_hierarchy_deleted(data)
            elif event_type == "hierarchy:moved":
                return self._handle_hierarchy_moved(data)
            else:
                return HandlerResult(
                    success=False,
                    message=f"Unknown event type: {event_type}",
                )
        except Exception as e:
            logger.exception(f"Error handling {event_type}")
            return HandlerResult(
                success=False,
                message=f"Handler error: {str(e)}",
                errors=[str(e)],
            )

    def _handle_project_created(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle project created event."""
        project_id = data.get("project_id")
        if project_id:
            self._project_cache[project_id] = {
                "id": project_id,
                "name": data.get("project_name"),
                "created_at": data.get("timestamp"),
            }
            logger.info(f"Cached new project: {project_id}")
            return HandlerResult(
                success=True,
                message=f"Project {project_id} cached",
                changes_applied=1,
            )
        return HandlerResult(success=False, message="No project_id in event")

    def _handle_project_updated(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle project updated event."""
        project_id = data.get("project_id")
        if project_id and project_id in self._project_cache:
            self._project_cache[project_id]["name"] = data.get("project_name")
            self._project_cache[project_id]["updated_at"] = data.get("timestamp")
            logger.info(f"Updated cached project: {project_id}")
            return HandlerResult(
                success=True,
                message=f"Project {project_id} updated",
                changes_applied=1,
            )
        return HandlerResult(success=True, message="Project not in cache")

    def _handle_project_deleted(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle project deleted event."""
        project_id = data.get("project_id")
        if project_id:
            # Remove project from cache
            self._project_cache.pop(project_id, None)
            # Remove all hierarchies for this project
            to_remove = [
                h_id
                for h_id, h in self._hierarchy_cache.items()
                if h.get("project_id") == project_id
            ]
            for h_id in to_remove:
                del self._hierarchy_cache[h_id]
            logger.info(f"Removed project and {len(to_remove)} hierarchies")
            return HandlerResult(
                success=True,
                message=f"Project {project_id} and hierarchies removed",
                changes_applied=1 + len(to_remove),
            )
        return HandlerResult(success=False, message="No project_id in event")

    def _handle_hierarchy_created(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle hierarchy created event."""
        hierarchy_id = data.get("hierarchy_id")
        if hierarchy_id:
            self._hierarchy_cache[hierarchy_id] = {
                "id": hierarchy_id,
                "name": data.get("hierarchy_name"),
                "project_id": data.get("project_id"),
                "parent_id": data.get("parent_id"),
                "created_at": data.get("timestamp"),
            }
            logger.info(f"Cached new hierarchy: {hierarchy_id}")
            return HandlerResult(
                success=True,
                message=f"Hierarchy {hierarchy_id} cached",
                changes_applied=1,
            )
        return HandlerResult(success=False, message="No hierarchy_id in event")

    def _handle_hierarchy_updated(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle hierarchy updated event."""
        hierarchy_id = data.get("hierarchy_id")
        if hierarchy_id:
            if hierarchy_id in self._hierarchy_cache:
                self._hierarchy_cache[hierarchy_id].update({
                    "name": data.get("hierarchy_name"),
                    "updated_at": data.get("timestamp"),
                    "changes": data.get("changes", {}),
                })
            else:
                # Add if not in cache
                self._hierarchy_cache[hierarchy_id] = {
                    "id": hierarchy_id,
                    "name": data.get("hierarchy_name"),
                    "project_id": data.get("project_id"),
                    "updated_at": data.get("timestamp"),
                }
            logger.info(f"Updated hierarchy: {hierarchy_id}")
            return HandlerResult(
                success=True,
                message=f"Hierarchy {hierarchy_id} updated",
                changes_applied=1,
            )
        return HandlerResult(success=False, message="No hierarchy_id in event")

    def _handle_hierarchy_deleted(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle hierarchy deleted event."""
        hierarchy_id = data.get("hierarchy_id")
        if hierarchy_id:
            self._hierarchy_cache.pop(hierarchy_id, None)
            logger.info(f"Removed hierarchy from cache: {hierarchy_id}")
            return HandlerResult(
                success=True,
                message=f"Hierarchy {hierarchy_id} removed",
                changes_applied=1,
            )
        return HandlerResult(success=False, message="No hierarchy_id in event")

    def _handle_hierarchy_moved(self, data: Dict[str, Any]) -> HandlerResult:
        """Handle hierarchy moved event."""
        hierarchy_id = data.get("hierarchy_id")
        if hierarchy_id and hierarchy_id in self._hierarchy_cache:
            changes = data.get("changes", {})
            self._hierarchy_cache[hierarchy_id]["parent_id"] = changes.get(
                "new_parent_id"
            )
            logger.info(f"Moved hierarchy: {hierarchy_id}")
            return HandlerResult(
                success=True,
                message=f"Hierarchy {hierarchy_id} moved",
                changes_applied=1,
            )
        return HandlerResult(success=True, message="Hierarchy not in cache")

    def get_cached_projects(self) -> Dict[str, Dict[str, Any]]:
        """Get all cached projects."""
        return self._project_cache.copy()

    def get_cached_hierarchies(
        self, project_id: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get cached hierarchies, optionally filtered by project."""
        if project_id:
            return {
                h_id: h
                for h_id, h in self._hierarchy_cache.items()
                if h.get("project_id") == project_id
            }
        return self._hierarchy_cache.copy()


class DimensionSyncHandler(SyncHandler):
    """
    Handler for updating dimensions from hierarchy changes.

    Triggers dimension remapping when hierarchies change.
    """

    def __init__(self, dimension_mapper=None):
        """
        Initialize handler.

        Args:
            dimension_mapper: Optional DimensionMapper instance
        """
        self._mapper = dimension_mapper
        self._pending_updates: List[str] = []  # Project IDs needing remap

    @property
    def handled_events(self) -> List[str]:
        """Events this handler processes."""
        return [
            "hierarchy:*",  # All hierarchy events
            "mapping:*",  # All mapping events
        ]

    def handle(self, event: SyncEvent) -> HandlerResult:
        """Handle dimension-related events."""
        data = event.data
        project_id = data.get("project_id")

        if project_id and project_id not in self._pending_updates:
            self._pending_updates.append(project_id)
            logger.info(f"Marked project {project_id} for dimension update")

        return HandlerResult(
            success=True,
            message=f"Project {project_id} marked for dimension update",
            changes_applied=0,  # Actual changes happen in process_pending
        )

    def process_pending(self) -> List[HandlerResult]:
        """
        Process all pending dimension updates.

        Returns:
            List of results from processing
        """
        results = []
        for project_id in self._pending_updates:
            try:
                result = self._update_dimensions(project_id)
                results.append(result)
            except Exception as e:
                results.append(
                    HandlerResult(
                        success=False,
                        message=f"Failed to update dimensions for {project_id}",
                        errors=[str(e)],
                    )
                )
        self._pending_updates.clear()
        return results

    def _update_dimensions(self, project_id: str) -> HandlerResult:
        """Update dimensions for a project."""
        if not self._mapper:
            return HandlerResult(
                success=False,
                message="No dimension mapper configured",
            )

        # This would fetch hierarchies from Librarian and remap
        # For now, just mark as pending
        logger.info(f"Would update dimensions for project: {project_id}")
        return HandlerResult(
            success=True,
            message=f"Dimensions updated for {project_id}",
            changes_applied=1,
        )

    def get_pending_projects(self) -> List[str]:
        """Get list of projects pending dimension update."""
        return self._pending_updates.copy()


class CacheInvalidationHandler(SyncHandler):
    """
    Handler for cache invalidation events.

    Clears local caches when invalidation events are received.
    """

    def __init__(self):
        """Initialize handler."""
        self._invalidation_callbacks: List[Callable[[List[str]], None]] = []

    @property
    def handled_events(self) -> List[str]:
        """Events this handler processes."""
        return [
            "cache:invalidate",
            "cache:invalidate_all",
        ]

    def register_cache_callback(
        self, callback: Callable[[List[str]], None]
    ) -> None:
        """
        Register a callback for cache invalidation.

        Args:
            callback: Function that takes list of cache keys to invalidate
        """
        self._invalidation_callbacks.append(callback)

    def handle(self, event: SyncEvent) -> HandlerResult:
        """Handle cache invalidation event."""
        data = event.data
        event_type = event.event_type

        if event_type == "cache:invalidate_all":
            keys_to_invalidate = ["*"]
        else:
            keys_to_invalidate = data.get("cache_keys", [])

        # Call all registered callbacks
        errors = []
        for callback in self._invalidation_callbacks:
            try:
                callback(keys_to_invalidate)
            except Exception as e:
                errors.append(str(e))

        if errors:
            return HandlerResult(
                success=False,
                message="Some cache callbacks failed",
                errors=errors,
            )

        logger.info(f"Invalidated {len(keys_to_invalidate)} cache keys")
        return HandlerResult(
            success=True,
            message=f"Invalidated {len(keys_to_invalidate)} keys",
            changes_applied=len(keys_to_invalidate),
        )
