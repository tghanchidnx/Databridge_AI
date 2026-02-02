"""
Event Models for the Event System.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List
import uuid


class EventType(str, Enum):
    """Types of events that can be emitted."""

    # Project events
    PROJECT_CREATED = "project:created"
    PROJECT_UPDATED = "project:updated"
    PROJECT_DELETED = "project:deleted"

    # Hierarchy events
    HIERARCHY_CREATED = "hierarchy:created"
    HIERARCHY_UPDATED = "hierarchy:updated"
    HIERARCHY_DELETED = "hierarchy:deleted"
    HIERARCHY_MOVED = "hierarchy:moved"

    # Mapping events
    MAPPING_ADDED = "mapping:added"
    MAPPING_REMOVED = "mapping:removed"
    MAPPING_UPDATED = "mapping:updated"

    # Deployment events
    DEPLOYMENT_STARTED = "deployment:started"
    DEPLOYMENT_COMPLETED = "deployment:completed"
    DEPLOYMENT_FAILED = "deployment:failed"
    DEPLOYMENT_ROLLED_BACK = "deployment:rolled_back"

    # Cache events
    CACHE_INVALIDATE = "cache:invalidate"
    CACHE_INVALIDATE_ALL = "cache:invalidate_all"

    # Sync events
    SYNC_REQUESTED = "sync:requested"
    SYNC_COMPLETED = "sync:completed"


@dataclass
class Event:
    """Base event class."""

    event_type: EventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source: str = "librarian"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Create from dictionary."""
        return cls(
            event_id=data.get("event_id", str(uuid.uuid4())),
            event_type=EventType(data["event_type"]),
            timestamp=datetime.fromisoformat(data["timestamp"])
            if "timestamp" in data
            else datetime.now(timezone.utc),
            source=data.get("source", "librarian"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class ProjectEvent(Event):
    """Event for project changes."""

    project_id: str = ""
    project_name: str = ""
    action: str = ""  # created, updated, deleted

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update(
            {
                "project_id": self.project_id,
                "project_name": self.project_name,
                "action": self.action,
            }
        )
        return data


@dataclass
class HierarchyEvent(Event):
    """Event for hierarchy changes."""

    hierarchy_id: str = ""
    hierarchy_name: str = ""
    project_id: str = ""
    parent_id: Optional[str] = None
    action: str = ""  # created, updated, deleted, moved
    changes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update(
            {
                "hierarchy_id": self.hierarchy_id,
                "hierarchy_name": self.hierarchy_name,
                "project_id": self.project_id,
                "parent_id": self.parent_id,
                "action": self.action,
                "changes": self.changes,
            }
        )
        return data


@dataclass
class MappingEvent(Event):
    """Event for mapping changes."""

    hierarchy_id: str = ""
    project_id: str = ""
    mapping_index: int = 0
    action: str = ""  # added, removed, updated
    mapping_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update(
            {
                "hierarchy_id": self.hierarchy_id,
                "project_id": self.project_id,
                "mapping_index": self.mapping_index,
                "action": self.action,
                "mapping_data": self.mapping_data,
            }
        )
        return data


@dataclass
class DeploymentEvent(Event):
    """Event for deployment changes."""

    deployment_id: str = ""
    project_id: str = ""
    target_database: str = ""
    target_schema: str = ""
    status: str = ""  # started, completed, failed, rolled_back
    scripts_executed: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update(
            {
                "deployment_id": self.deployment_id,
                "project_id": self.project_id,
                "target_database": self.target_database,
                "target_schema": self.target_schema,
                "status": self.status,
                "scripts_executed": self.scripts_executed,
                "error_message": self.error_message,
            }
        )
        return data


@dataclass
class CacheInvalidationEvent(Event):
    """Event for cache invalidation."""

    cache_keys: List[str] = field(default_factory=list)
    invalidate_all: bool = False
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data.update(
            {
                "cache_keys": self.cache_keys,
                "invalidate_all": self.invalidate_all,
                "reason": self.reason,
            }
        )
        return data
