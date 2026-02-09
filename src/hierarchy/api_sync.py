"""Sync service for bidirectional MCP <-> NestJS backend communication.

This module provides HTTP-based synchronization between the MCP hierarchy
tools (local JSON storage) and the NestJS backend (MySQL database).

AUTO-SYNC FEATURE:
When auto_sync is enabled, changes made via MCP tools are automatically
pushed to the backend, and vice versa. This ensures the MCP server and
the Web UI stay in sync without manual intervention.
"""
import json
import requests
import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path

# Configure logging for auto-sync
logger = logging.getLogger("hierarchy_sync")


class AutoSyncManager:
    """Manages automatic synchronization between MCP and backend.

    Now includes Event Bus publishing for the AI Orchestrator layer.
    Events are published to notify Excel plugins, Power BI, and other
    AI agents of hierarchy changes in real-time.
    """

    # Event types for the orchestrator
    EVENT_HIERARCHY_CREATED = "hierarchy.created"
    EVENT_HIERARCHY_UPDATED = "hierarchy.updated"
    EVENT_HIERARCHY_DELETED = "hierarchy.deleted"
    EVENT_PROJECT_CREATED = "hierarchy.project.created"
    EVENT_PROJECT_DELETED = "hierarchy.project.deleted"
    EVENT_MAPPING_ADDED = "hierarchy.mapping.added"
    EVENT_SYNC_COMPLETED = "sync.completed"

    def __init__(self, sync_service: 'HierarchyApiSync', local_service: Any = None):
        """
        Initialize auto-sync manager.

        Args:
            sync_service: HierarchyApiSync instance for backend communication
            local_service: HierarchyService instance for local operations
        """
        self.sync_service = sync_service
        self.local_service = local_service
        self._enabled = True
        self._sync_lock = threading.Lock()
        self._pending_syncs: List[Dict] = []
        self._callbacks: List[Callable] = []
        self._event_bus_enabled = True
        self._agent_id = "mcp-hierarchy-service"  # This agent's ID for event publishing

    def set_local_service(self, service: Any):
        """Set the local hierarchy service."""
        self.local_service = service

    def enable(self):
        """Enable auto-sync."""
        self._enabled = True
        logger.info("Auto-sync enabled")

    def disable(self):
        """Disable auto-sync."""
        self._enabled = False
        logger.info("Auto-sync disabled")

    @property
    def is_enabled(self) -> bool:
        """Check if auto-sync is enabled."""
        return self._enabled and self.local_service is not None

    def add_callback(self, callback: Callable):
        """Add a callback to be called after sync operations."""
        self._callbacks.append(callback)

    def enable_event_bus(self):
        """Enable Event Bus publishing to orchestrator."""
        self._event_bus_enabled = True
        logger.info("Event Bus publishing enabled")

    def disable_event_bus(self):
        """Disable Event Bus publishing to orchestrator."""
        self._event_bus_enabled = False
        logger.info("Event Bus publishing disabled")

    def _get_event_type(self, operation: str) -> str:
        """Map operation to event type."""
        event_map = {
            "create_project": self.EVENT_PROJECT_CREATED,
            "delete_project": self.EVENT_PROJECT_DELETED,
            "create_hierarchy": self.EVENT_HIERARCHY_CREATED,
            "update_hierarchy": self.EVENT_HIERARCHY_UPDATED,
            "delete_hierarchy": self.EVENT_HIERARCHY_DELETED,
            "add_mapping": self.EVENT_MAPPING_ADDED,
        }
        return event_map.get(operation, f"hierarchy.{operation}")

    def _publish_event(
        self,
        event_type: str,
        project_id: str,
        hierarchy_id: Optional[str] = None,
        data: Optional[Dict] = None,
    ) -> bool:
        """
        Publish an event to the orchestrator Event Bus.

        Args:
            event_type: Type of event (e.g., 'hierarchy.updated')
            project_id: Project ID affected
            hierarchy_id: Hierarchy ID affected (optional)
            data: Additional event data (optional)

        Returns:
            True if event was published successfully
        """
        if not self._event_bus_enabled:
            return False

        try:
            event_payload = {
                "channel": event_type,
                "payload": {
                    "project_id": project_id,
                    "hierarchy_id": hierarchy_id,
                    "operation": event_type.split(".")[-1],
                    "data": data or {},
                    "source": self._agent_id,
                },
                "timestamp": datetime.now().isoformat(),
                "source": self._agent_id,
            }

            # Post to orchestrator Event Bus endpoint
            orchestrator_url = self.sync_service.base_url.replace("/smart-hierarchy", "").replace("/api", "")
            if not orchestrator_url.endswith("/api"):
                orchestrator_url = f"{orchestrator_url}/api"

            response = requests.post(
                f"{orchestrator_url}/orchestrator/events/publish",
                headers=self.sync_service.headers,
                json=event_payload,
                timeout=5,  # Short timeout for events
            )

            if response.status_code < 400:
                logger.debug(f"Event published: {event_type} for project {project_id}")
                return True
            else:
                logger.warning(f"Event publish failed: {response.status_code}")
                return False

        except requests.exceptions.ConnectionError:
            # Orchestrator not available - not a critical error
            logger.debug("Orchestrator not available for event publishing")
            return False
        except Exception as e:
            logger.warning(f"Event publish error: {e}")
            return False

    def on_local_change(
        self,
        operation: str,
        project_id: str,
        hierarchy_id: Optional[str] = None,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Called when a local change is made. Automatically syncs to backend.

        Args:
            operation: Type of operation (create, update, delete)
            project_id: Project ID affected
            hierarchy_id: Hierarchy ID affected (optional)
            data: Data for the operation (optional)

        Returns:
            Sync result with status
        """
        if not self.is_enabled:
            return {"auto_sync": "disabled", "synced": False}

        with self._sync_lock:
            try:
                result = self._sync_to_backend(operation, project_id, hierarchy_id, data)
                result["auto_sync"] = "enabled"
                result["synced"] = not result.get("error", False)

                # Publish event to orchestrator Event Bus
                if result["synced"]:
                    event_type = self._get_event_type(operation)
                    event_published = self._publish_event(
                        event_type=event_type,
                        project_id=project_id,
                        hierarchy_id=hierarchy_id,
                        data=data,
                    )
                    result["event_published"] = event_published

                # Call registered callbacks with rich event data
                event_data = {
                    "operation": operation,
                    "project_id": project_id,
                    "hierarchy_id": hierarchy_id,
                    "data": data,
                    "timestamp": datetime.now().isoformat(),
                    "sync_result": result,
                }
                for callback in self._callbacks:
                    try:
                        callback(event_data)
                    except Exception as e:
                        logger.warning(f"Callback error: {e}")

                return result

            except Exception as e:
                logger.error(f"Auto-sync error: {e}")
                return {
                    "auto_sync": "enabled",
                    "synced": False,
                    "error": str(e),
                }

    def _sync_to_backend(
        self,
        operation: str,
        project_id: str,
        hierarchy_id: Optional[str],
        data: Optional[Dict],
    ) -> Dict[str, Any]:
        """Perform the actual sync to backend."""
        if operation == "create_project":
            return self.sync_service.create_project(
                name=data.get("name", ""),
                description=data.get("description", ""),
            )

        elif operation == "delete_project":
            success = self.sync_service.delete_project(project_id)
            return {"success": success}

        elif operation == "create_hierarchy":
            return self.sync_service.create_hierarchy(
                project_id=project_id,
                hierarchy_name=data.get("hierarchy_name", ""),
                parent_id=data.get("parent_id"),
                description=data.get("description", ""),
                flags=data.get("flags"),
                hierarchy_id=hierarchy_id,
            )

        elif operation == "update_hierarchy":
            backend_data = self.sync_service._convert_local_to_backend(data, project_id)
            return self.sync_service.update_hierarchy(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                updates=backend_data,
            )

        elif operation == "delete_hierarchy":
            success = self.sync_service.delete_hierarchy(project_id, hierarchy_id)
            return {"success": success}

        elif operation == "add_mapping":
            return self.sync_service.add_source_mapping(
                project_id=project_id,
                hierarchy_id=hierarchy_id,
                mapping=data,
            )

        else:
            return {"error": f"Unknown operation: {operation}"}

    def pull_from_backend(self, project_id: str) -> Dict[str, Any]:
        """
        Pull latest data from backend for a project.

        Args:
            project_id: Backend project ID to pull from

        Returns:
            Sync result
        """
        if not self.is_enabled:
            return {"auto_sync": "disabled", "synced": False}

        return self.sync_service.sync_project_from_backend(
            local_service=self.local_service,
            backend_project_id=project_id,
        )


class HierarchyApiSync:
    """HTTP client for syncing hierarchy data with NestJS backend.

    Supports both manual and automatic synchronization modes.
    When auto_sync is enabled, all write operations automatically
    propagate to the backend.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: int = 30,
        auto_sync: bool = True,
    ):
        """
        Initialize the sync service.

        Args:
            base_url: NestJS backend URL (e.g., 'http://localhost:3001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds
            auto_sync: Enable automatic synchronization (default: True)
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.auto_sync_enabled = auto_sync
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json',
        }
        # Initialize auto-sync manager (local_service set later)
        self.auto_sync_manager = AutoSyncManager(self) if auto_sync else None

    def set_local_service(self, service: Any):
        """Set the local hierarchy service for auto-sync."""
        if self.auto_sync_manager:
            self.auto_sync_manager.set_local_service(service)

    def enable_auto_sync(self):
        """Enable automatic synchronization."""
        self.auto_sync_enabled = True
        if self.auto_sync_manager:
            self.auto_sync_manager.enable()
        else:
            self.auto_sync_manager = AutoSyncManager(self)
        logger.info("Auto-sync enabled")

    def disable_auto_sync(self):
        """Disable automatic synchronization."""
        self.auto_sync_enabled = False
        if self.auto_sync_manager:
            self.auto_sync_manager.disable()
        logger.info("Auto-sync disabled")

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request to the backend."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=self.timeout,
            )

            if response.status_code >= 400:
                return {
                    "error": True,
                    "status_code": response.status_code,
                    "message": response.text,
                }

            return response.json() if response.text else {"success": True}

        except requests.exceptions.ConnectionError:
            return {"error": True, "message": "Backend not reachable"}
        except requests.exceptions.Timeout:
            return {"error": True, "message": "Request timed out"}
        except Exception as e:
            return {"error": True, "message": str(e)}

    # =========================================================================
    # Project Operations
    # =========================================================================

    def create_project(self, name: str, description: str = "") -> Dict[str, Any]:
        """Create a new project in the backend."""
        return self._request("POST", "/smart-hierarchy/projects", {
            "name": name,
            "description": description,
        })

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """Get a project by ID."""
        return self._request("GET", f"/smart-hierarchy/projects/{project_id}")

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects from the backend."""
        result = self._request("GET", "/smart-hierarchy/projects")
        if isinstance(result, dict) and result.get("error"):
            return []
        # Handle different response formats
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def delete_project(self, project_id: str) -> bool:
        """Delete a project."""
        result = self._request("DELETE", f"/smart-hierarchy/projects/{project_id}")
        return not result.get("error", False)

    # =========================================================================
    # Hierarchy Operations
    # =========================================================================

    def create_hierarchy(
        self,
        project_id: str,
        hierarchy_name: str,
        parent_id: Optional[str] = None,
        description: str = "",
        flags: Optional[Dict] = None,
        hierarchy_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new hierarchy in the backend."""
        # Generate hierarchy ID from name if not provided
        if not hierarchy_id:
            # Create slug from name (uppercase, replace spaces/special chars)
            slug = hierarchy_name.upper().replace(" ", "_")
            slug = "".join(c if c.isalnum() or c == "_" else "_" for c in slug)
            slug = slug.strip("_")[:50]
            # Add timestamp suffix for uniqueness
            import time
            hierarchy_id = f"{slug}_{int(time.time()) % 10000}"

        data = {
            "projectId": project_id,
            "hierarchyId": hierarchy_id,
            "hierarchyName": hierarchy_name,
            "description": description,
            "parentId": parent_id,
            "mapping": [],  # Required field, start with empty array
            "flags": flags or {
                "include_flag": True,
                "exclude_flag": False,
                "transform_flag": False,
                "calculation_flag": False,
                "active_flag": True,
                "is_leaf_node": False,
            },
        }

        return self._request("POST", "/smart-hierarchy", data)

    def update_hierarchy(
        self,
        project_id: str,
        hierarchy_id: str,
        updates: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update a hierarchy in the backend."""
        return self._request(
            "PUT",
            f"/smart-hierarchy/project/{project_id}/{hierarchy_id}",
            updates,
        )

    def delete_hierarchy(self, project_id: str, hierarchy_id: str) -> bool:
        """Delete a hierarchy from the backend."""
        result = self._request(
            "DELETE",
            f"/smart-hierarchy/project/{project_id}/{hierarchy_id}",
        )
        return not result.get("error", False)

    def get_hierarchy(self, project_id: str, hierarchy_id: str) -> Optional[Dict]:
        """Get a single hierarchy."""
        result = self._request(
            "GET",
            f"/smart-hierarchy/project/{project_id}/{hierarchy_id}",
        )
        if result.get("error"):
            return None
        return result.get("data") if "data" in result else result

    def list_hierarchies(self, project_id: str) -> List[Dict[str, Any]]:
        """List all hierarchies for a project."""
        result = self._request("GET", f"/smart-hierarchy/project/{project_id}")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    def get_hierarchy_tree(self, project_id: str) -> List[Dict[str, Any]]:
        """Get hierarchies as a tree structure."""
        result = self._request("GET", f"/smart-hierarchy/project/{project_id}/tree")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    # =========================================================================
    # Source Mapping Operations
    # =========================================================================

    def add_source_mapping(
        self,
        project_id: str,
        hierarchy_id: str,
        mapping: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add a source mapping to a hierarchy."""
        # Get current hierarchy
        hierarchy = self.get_hierarchy(project_id, hierarchy_id)
        if not hierarchy:
            return {"error": True, "message": "Hierarchy not found"}

        # Add mapping to existing mappings
        current_mappings = hierarchy.get("mapping", [])
        if isinstance(current_mappings, str):
            current_mappings = json.loads(current_mappings) if current_mappings else []

        # Calculate next mapping index
        max_index = max([m.get("mapping_index", 0) for m in current_mappings], default=0)
        mapping["mapping_index"] = max_index + 1

        current_mappings.append(mapping)

        # Update hierarchy with new mappings
        return self.update_hierarchy(project_id, hierarchy_id, {"mapping": current_mappings})

    # =========================================================================
    # Formula Operations
    # =========================================================================

    def create_formula_group(
        self,
        project_id: str,
        hierarchy_id: str,
        group_name: str,
        rules: List[Dict],
    ) -> Dict[str, Any]:
        """Create or update a formula group for a hierarchy."""
        formula_config = {
            "formula_type": "EXPRESSION",
            "formula_group": {
                "group_name": group_name,
                "main_hierarchy_id": hierarchy_id,
                "rules": rules,
            },
        }

        return self.update_hierarchy(project_id, hierarchy_id, {
            "formulaConfig": formula_config,
            "flags": {"calculation_flag": True},
        })

    # =========================================================================
    # Sync Operations
    # =========================================================================

    def sync_project_from_backend(
        self,
        local_service,
        backend_project_id: str,
        local_project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Pull a project and its hierarchies from backend to local MCP storage.

        Args:
            local_service: The local HierarchyService instance
            backend_project_id: The project ID in the backend
            local_project_id: Optional local project ID (creates new if not provided)

        Returns:
            Sync result with counts
        """
        result = {
            "synced_hierarchies": 0,
            "created_hierarchies": 0,
            "updated_hierarchies": 0,
            "errors": [],
        }

        # Get project from backend
        project = self.get_project(backend_project_id)
        if not project or project.get("error"):
            result["errors"].append(f"Failed to fetch project: {project}")
            return result

        project_data = project.get("data", project)

        # Create or update local project
        if not local_project_id:
            local_project = local_service.create_project(
                name=project_data.get("name", "Synced Project"),
                description=project_data.get("description", ""),
            )
            local_project_id = local_project.id
            result["local_project_id"] = local_project_id

        # Get all hierarchies from backend
        hierarchies = self.list_hierarchies(backend_project_id)

        for hier in hierarchies:
            try:
                # Check if hierarchy exists locally
                local_hier = local_service.get_hierarchy(
                    local_project_id,
                    hier.get("hierarchyId"),
                )

                hier_data = self._convert_backend_to_local(hier)

                if local_hier:
                    # Update existing
                    local_service.update_hierarchy(
                        local_project_id,
                        hier.get("hierarchyId"),
                        hier_data,
                    )
                    result["updated_hierarchies"] += 1
                else:
                    # Create new
                    local_service.create_hierarchy(
                        project_id=local_project_id,
                        hierarchy_name=hier.get("hierarchyName"),
                        parent_id=hier.get("parentId"),
                        description=hier.get("description", ""),
                        flags=hier_data.get("flags"),
                    )
                    result["created_hierarchies"] += 1

                result["synced_hierarchies"] += 1

            except Exception as e:
                result["errors"].append(f"Error syncing {hier.get('hierarchyId')}: {str(e)}")

        return result

    def sync_project_to_backend(
        self,
        local_service,
        local_project_id: str,
        backend_project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Push a local project and its hierarchies to the backend.

        Args:
            local_service: The local HierarchyService instance
            local_project_id: The local project ID
            backend_project_id: Optional backend project ID (creates new if not provided)

        Returns:
            Sync result with counts
        """
        result = {
            "synced_hierarchies": 0,
            "created_hierarchies": 0,
            "updated_hierarchies": 0,
            "errors": [],
        }

        # Get local project
        local_project = local_service.get_project(local_project_id)
        if not local_project:
            result["errors"].append("Local project not found")
            return result

        # Create backend project if needed
        if not backend_project_id:
            backend_project = self.create_project(
                name=local_project.get("name", "Synced Project"),
                description=local_project.get("description", ""),
            )
            if backend_project.get("error"):
                result["errors"].append(f"Failed to create backend project: {backend_project}")
                return result
            backend_project_data = backend_project.get("data", backend_project)
            backend_project_id = backend_project_data.get("id")
            result["backend_project_id"] = backend_project_id

        # Get all local hierarchies
        local_hierarchies = local_service.list_hierarchies(local_project_id)

        for hier in local_hierarchies:
            try:
                hierarchy_id = hier.get("hierarchy_id")

                # Check if hierarchy exists in backend
                backend_hier = self.get_hierarchy(backend_project_id, hierarchy_id)

                hier_data = self._convert_local_to_backend(hier, backend_project_id)

                if backend_hier:
                    # Update existing
                    self.update_hierarchy(backend_project_id, hierarchy_id, hier_data)
                    result["updated_hierarchies"] += 1
                else:
                    # Create new
                    self.create_hierarchy(
                        project_id=backend_project_id,
                        hierarchy_name=hier.get("hierarchy_name"),
                        parent_id=hier.get("parent_id"),
                        description=hier.get("description", ""),
                        flags=hier.get("flags"),
                    )
                    result["created_hierarchies"] += 1

                result["synced_hierarchies"] += 1

            except Exception as e:
                result["errors"].append(f"Error syncing {hier.get('hierarchy_id')}: {str(e)}")

        return result

    def _convert_backend_to_local(self, backend_hier: Dict) -> Dict:
        """Convert backend hierarchy format to local format."""
        return {
            "hierarchy_id": backend_hier.get("hierarchyId"),
            "hierarchy_name": backend_hier.get("hierarchyName"),
            "description": backend_hier.get("description"),
            "parent_id": backend_hier.get("parentId"),
            "is_root": backend_hier.get("isRoot", False),
            "sort_order": backend_hier.get("sortOrder", 0),
            "hierarchy_level": backend_hier.get("hierarchyLevel", {}),
            "flags": backend_hier.get("flags", {}),
            "mapping": backend_hier.get("mapping", []),
            "formula_config": backend_hier.get("formulaConfig"),
            "filter_config": backend_hier.get("filterConfig"),
            "updated_at": backend_hier.get("updatedAt"),
        }

    def _convert_local_to_backend(self, local_hier: Dict, project_id: str) -> Dict:
        """Convert local hierarchy format to backend format."""
        return {
            "projectId": project_id,
            "hierarchyId": local_hier.get("hierarchy_id"),
            "hierarchyName": local_hier.get("hierarchy_name"),
            "description": local_hier.get("description"),
            "parentId": local_hier.get("parent_id"),
            "isRoot": local_hier.get("is_root", False),
            "sortOrder": local_hier.get("sort_order", 0),
            "hierarchyLevel": local_hier.get("hierarchy_level", {}),
            "flags": local_hier.get("flags", {}),
            "mapping": local_hier.get("mapping", []),
            "formulaConfig": local_hier.get("formula_config"),
            "filterConfig": local_hier.get("filter_config"),
        }

    def resolve_conflict(
        self,
        local_hier: Dict,
        backend_hier: Dict,
    ) -> Dict[str, Any]:
        """
        Resolve conflict between local and backend versions.
        Latest updated_at wins.

        Returns:
            The winning version and action taken
        """
        local_updated = local_hier.get("updated_at", "")
        backend_updated = backend_hier.get("updatedAt", backend_hier.get("updated_at", ""))

        # Parse timestamps
        try:
            local_time = datetime.fromisoformat(str(local_updated).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            local_time = datetime.min

        try:
            backend_time = datetime.fromisoformat(str(backend_updated).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            backend_time = datetime.min

        if local_time >= backend_time:
            return {
                "winner": "local",
                "data": local_hier,
                "reason": f"Local updated at {local_time} >= backend at {backend_time}",
            }
        else:
            return {
                "winner": "backend",
                "data": self._convert_backend_to_local(backend_hier),
                "reason": f"Backend updated at {backend_time} > local at {local_time}",
            }

    # =========================================================================
    # Dashboard Operations
    # =========================================================================

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """
        Get dashboard statistics from the backend.

        Returns:
            Statistics including project count, hierarchy count, etc.
        """
        result = self._request("GET", "/smart-hierarchy/dashboard/stats")
        return result

    def get_dashboard_activities(self, limit: int = 10) -> List[Dict]:
        """
        Get recent activities from the dashboard.

        Args:
            limit: Maximum number of activities to return

        Returns:
            List of recent activity entries
        """
        result = self._request("GET", "/smart-hierarchy/dashboard/activities", params={"limit": limit})
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    # =========================================================================
    # Deployment Operations
    # =========================================================================

    def generate_deployment_scripts(
        self,
        project_id: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate deployment scripts for a project.

        Args:
            project_id: Project UUID
            config: Configuration for script generation (table_name, view_name, etc.)

        Returns:
            Generated SQL scripts
        """
        return self._request("POST", f"/smart-hierarchy/projects/{project_id}/deployment/scripts", config)

    def push_to_snowflake(self, dto: Dict[str, Any]) -> Dict[str, Any]:
        """
        Push hierarchy to Snowflake.

        Args:
            dto: Snowflake deployment DTO with connection and target details

        Returns:
            Deployment result
        """
        return self._request("POST", "/smart-hierarchy/deployment/snowflake", dto)

    def get_deployment_history(self, project_id: str, limit: int = 50) -> List[Dict]:
        """
        Get deployment history for a project.

        Args:
            project_id: Project UUID
            limit: Maximum number of entries to return

        Returns:
            List of deployment history entries
        """
        result = self._request("GET", f"/smart-hierarchy/projects/{project_id}/deployment/history", params={"limit": limit})
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    # =========================================================================
    # Backend-side Export/Import Operations
    # =========================================================================

    def export_hierarchy_csv_backend(self, project_id: str) -> str:
        """
        Export hierarchy to CSV via the backend.

        Args:
            project_id: Project UUID

        Returns:
            CSV content as string
        """
        result = self._request("GET", f"/smart-hierarchy/projects/{project_id}/export/csv")
        if isinstance(result, dict) and result.get("error"):
            return ""
        if isinstance(result, dict) and "content" in result:
            return result.get("content", "")
        return result if isinstance(result, str) else ""

    def import_hierarchy_csv_backend(self, project_id: str, csv_content: str) -> Dict[str, Any]:
        """
        Import hierarchy from CSV via the backend.

        Args:
            project_id: Project UUID
            csv_content: CSV content as string

        Returns:
            Import result with statistics
        """
        return self._request("POST", f"/smart-hierarchy/projects/{project_id}/import/csv", {
            "content": csv_content
        })

    # =========================================================================
    # Filter Group Operations
    # =========================================================================

    def create_filter_group(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a filter group.

        Args:
            body: Filter group configuration

        Returns:
            Created filter group
        """
        return self._request("POST", "/smart-hierarchy/filter-groups", body)

    def list_filter_groups(self, project_id: str) -> List[Dict]:
        """
        List filter groups for a project.

        Args:
            project_id: Project UUID

        Returns:
            List of filter groups
        """
        result = self._request("GET", f"/smart-hierarchy/projects/{project_id}/filter-groups")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    # =========================================================================
    # Search Operations
    # =========================================================================

    def search_hierarchies(self, project_id: str, query: str) -> List[Dict]:
        """
        Search hierarchies within a project.

        Args:
            project_id: Project UUID
            query: Search query string

        Returns:
            List of matching hierarchies
        """
        result = self._request("GET", f"/smart-hierarchy/project/{project_id}/search", params={"q": query})
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    # =========================================================================
    # Health Check
    # =========================================================================

    def health_check(self) -> Dict[str, Any]:
        """Check if the backend is reachable and auto-sync status."""
        result = self._request("GET", "/health")
        return {
            "connected": not result.get("error", False),
            "backend_url": self.base_url,
            "auto_sync_enabled": self.auto_sync_enabled,
            "sync_mode": "automatic" if self.auto_sync_enabled else "manual",
            "response": result,
        }

    def get_sync_status(self) -> Dict[str, Any]:
        """Get current synchronization status and configuration."""
        return {
            "auto_sync_enabled": self.auto_sync_enabled,
            "sync_mode": "automatic" if self.auto_sync_enabled else "manual",
            "backend_url": self.base_url,
            "has_local_service": self.auto_sync_manager is not None and
                                  self.auto_sync_manager.local_service is not None,
            "description": (
                "Changes made via MCP are automatically synced to the backend. "
                "Changes in the UI are visible via backend-prefixed tools."
                if self.auto_sync_enabled else
                "Manual sync required. Use sync_to_backend/sync_from_backend tools."
            ),
        }
