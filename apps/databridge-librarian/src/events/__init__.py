"""
Event System for DataBridge Librarian.

Provides pub/sub functionality for:
- Hierarchy changes (create, update, delete)
- Mapping changes
- Project deployments
- Cache invalidation signals
"""

from .models import (
    EventType,
    Event,
    HierarchyEvent,
    ProjectEvent,
    MappingEvent,
    DeploymentEvent,
)
from .bus import (
    EventBus,
    EventHandler,
    get_event_bus,
)
from .publisher import (
    EventPublisher,
    get_publisher,
)

__all__ = [
    # Models
    "EventType",
    "Event",
    "HierarchyEvent",
    "ProjectEvent",
    "MappingEvent",
    "DeploymentEvent",
    # Bus
    "EventBus",
    "EventHandler",
    "get_event_bus",
    # Publisher
    "EventPublisher",
    "get_publisher",
]
