"""
Event Publisher for emitting events from services.
"""

import logging
from typing import Dict, Any, Optional, List

from .models import (
    EventType,
    Event,
    ProjectEvent,
    HierarchyEvent,
    MappingEvent,
    DeploymentEvent,
    CacheInvalidationEvent,
)
from .bus import EventBus, get_event_bus

logger = logging.getLogger(__name__)


class EventPublisher:
    """
    Publisher for emitting typed events.

    Provides convenience methods for common event types.
    """

    def __init__(self, event_bus: Optional[EventBus] = None):
        """
        Initialize publisher.

        Args:
            event_bus: Optional event bus instance (uses global if not provided)
        """
        self._bus = event_bus or get_event_bus()

    def publish(self, event: Event) -> None:
        """
        Publish a generic event.

        Args:
            event: Event to publish
        """
        self._bus.publish(event)

    # Project events

    def project_created(
        self,
        project_id: str,
        project_name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Emit project created event."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_CREATED,
            project_id=project_id,
            project_name=project_name,
            action="created",
            metadata=metadata or {},
        )
        self._bus.publish(event)
        logger.info(f"Project created: {project_id}")

    def project_updated(
        self,
        project_id: str,
        project_name: str,
        changes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Emit project updated event."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_UPDATED,
            project_id=project_id,
            project_name=project_name,
            action="updated",
            metadata={"changes": changes or {}},
        )
        self._bus.publish(event)
        logger.info(f"Project updated: {project_id}")

    def project_deleted(
        self,
        project_id: str,
        project_name: str,
    ) -> None:
        """Emit project deleted event."""
        event = ProjectEvent(
            event_type=EventType.PROJECT_DELETED,
            project_id=project_id,
            project_name=project_name,
            action="deleted",
        )
        self._bus.publish(event)
        logger.info(f"Project deleted: {project_id}")

    # Hierarchy events

    def hierarchy_created(
        self,
        hierarchy_id: str,
        hierarchy_name: str,
        project_id: str,
        parent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Emit hierarchy created event."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_CREATED,
            hierarchy_id=hierarchy_id,
            hierarchy_name=hierarchy_name,
            project_id=project_id,
            parent_id=parent_id,
            action="created",
            metadata=metadata or {},
        )
        self._bus.publish(event)
        logger.info(f"Hierarchy created: {hierarchy_id}")

    def hierarchy_updated(
        self,
        hierarchy_id: str,
        hierarchy_name: str,
        project_id: str,
        changes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Emit hierarchy updated event."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_UPDATED,
            hierarchy_id=hierarchy_id,
            hierarchy_name=hierarchy_name,
            project_id=project_id,
            action="updated",
            changes=changes or {},
        )
        self._bus.publish(event)
        logger.info(f"Hierarchy updated: {hierarchy_id}")

    def hierarchy_deleted(
        self,
        hierarchy_id: str,
        hierarchy_name: str,
        project_id: str,
    ) -> None:
        """Emit hierarchy deleted event."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_DELETED,
            hierarchy_id=hierarchy_id,
            hierarchy_name=hierarchy_name,
            project_id=project_id,
            action="deleted",
        )
        self._bus.publish(event)
        logger.info(f"Hierarchy deleted: {hierarchy_id}")

    def hierarchy_moved(
        self,
        hierarchy_id: str,
        hierarchy_name: str,
        project_id: str,
        old_parent_id: Optional[str],
        new_parent_id: Optional[str],
    ) -> None:
        """Emit hierarchy moved event."""
        event = HierarchyEvent(
            event_type=EventType.HIERARCHY_MOVED,
            hierarchy_id=hierarchy_id,
            hierarchy_name=hierarchy_name,
            project_id=project_id,
            parent_id=new_parent_id,
            action="moved",
            changes={
                "old_parent_id": old_parent_id,
                "new_parent_id": new_parent_id,
            },
        )
        self._bus.publish(event)
        logger.info(f"Hierarchy moved: {hierarchy_id}")

    # Mapping events

    def mapping_added(
        self,
        hierarchy_id: str,
        project_id: str,
        mapping_index: int,
        mapping_data: Dict[str, Any],
    ) -> None:
        """Emit mapping added event."""
        event = MappingEvent(
            event_type=EventType.MAPPING_ADDED,
            hierarchy_id=hierarchy_id,
            project_id=project_id,
            mapping_index=mapping_index,
            action="added",
            mapping_data=mapping_data,
        )
        self._bus.publish(event)
        logger.info(f"Mapping added to hierarchy: {hierarchy_id}")

    def mapping_removed(
        self,
        hierarchy_id: str,
        project_id: str,
        mapping_index: int,
    ) -> None:
        """Emit mapping removed event."""
        event = MappingEvent(
            event_type=EventType.MAPPING_REMOVED,
            hierarchy_id=hierarchy_id,
            project_id=project_id,
            mapping_index=mapping_index,
            action="removed",
        )
        self._bus.publish(event)
        logger.info(f"Mapping removed from hierarchy: {hierarchy_id}")

    # Deployment events

    def deployment_started(
        self,
        deployment_id: str,
        project_id: str,
        target_database: str,
        target_schema: str,
    ) -> None:
        """Emit deployment started event."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_STARTED,
            deployment_id=deployment_id,
            project_id=project_id,
            target_database=target_database,
            target_schema=target_schema,
            status="started",
        )
        self._bus.publish(event)
        logger.info(f"Deployment started: {deployment_id}")

    def deployment_completed(
        self,
        deployment_id: str,
        project_id: str,
        target_database: str,
        target_schema: str,
        scripts_executed: int,
    ) -> None:
        """Emit deployment completed event."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_COMPLETED,
            deployment_id=deployment_id,
            project_id=project_id,
            target_database=target_database,
            target_schema=target_schema,
            status="completed",
            scripts_executed=scripts_executed,
        )
        self._bus.publish(event)
        logger.info(f"Deployment completed: {deployment_id}")

    def deployment_failed(
        self,
        deployment_id: str,
        project_id: str,
        target_database: str,
        target_schema: str,
        error_message: str,
        scripts_executed: int = 0,
    ) -> None:
        """Emit deployment failed event."""
        event = DeploymentEvent(
            event_type=EventType.DEPLOYMENT_FAILED,
            deployment_id=deployment_id,
            project_id=project_id,
            target_database=target_database,
            target_schema=target_schema,
            status="failed",
            scripts_executed=scripts_executed,
            error_message=error_message,
        )
        self._bus.publish(event)
        logger.error(f"Deployment failed: {deployment_id}")

    # Cache events

    def invalidate_cache(
        self,
        cache_keys: List[str],
        reason: str = "",
    ) -> None:
        """Emit cache invalidation event."""
        event = CacheInvalidationEvent(
            event_type=EventType.CACHE_INVALIDATE,
            cache_keys=cache_keys,
            invalidate_all=False,
            reason=reason,
        )
        self._bus.publish(event)
        logger.info(f"Cache invalidation: {len(cache_keys)} keys")

    def invalidate_all_cache(
        self,
        reason: str = "",
    ) -> None:
        """Emit cache invalidate all event."""
        event = CacheInvalidationEvent(
            event_type=EventType.CACHE_INVALIDATE_ALL,
            cache_keys=[],
            invalidate_all=True,
            reason=reason,
        )
        self._bus.publish(event)
        logger.info("Cache invalidation: all keys")


# Global publisher instance
_publisher: Optional[EventPublisher] = None


def get_publisher() -> EventPublisher:
    """Get the global event publisher instance."""
    global _publisher
    if _publisher is None:
        _publisher = EventPublisher()
    return _publisher


def reset_publisher() -> None:
    """Reset the global publisher (for testing)."""
    global _publisher
    _publisher = None
