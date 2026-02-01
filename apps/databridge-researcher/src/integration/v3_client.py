"""
V3 Hierarchy Client for DataBridge Analytics V4.

Provides access to V3 Hierarchy Builder data for integrated FP&A workflows.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum
import logging

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


logger = logging.getLogger(__name__)


class V3ConnectionMode(str, Enum):
    """Connection mode for V3 access."""
    HTTP = "http"  # Via V3 API/MCP server
    DIRECT = "direct"  # Direct database access


@dataclass
class V3Project:
    """V3 Project representation."""

    id: str
    name: str
    description: str = ""
    client_name: str = ""
    hierarchy_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "client_name": self.client_name,
            "hierarchy_count": self.hierarchy_count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class V3Hierarchy:
    """V3 Hierarchy representation."""

    hierarchy_id: str
    hierarchy_name: str
    project_id: str
    parent_id: Optional[str] = None
    description: str = ""
    levels: Dict[str, str] = field(default_factory=dict)
    formula_group: str = ""
    sort_order: int = 0
    include_flag: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hierarchy_id": self.hierarchy_id,
            "hierarchy_name": self.hierarchy_name,
            "project_id": self.project_id,
            "parent_id": self.parent_id,
            "description": self.description,
            "levels": self.levels,
            "formula_group": self.formula_group,
            "sort_order": self.sort_order,
            "include_flag": self.include_flag,
            "metadata": self.metadata,
        }

    def get_level_path(self, separator: str = " > ") -> str:
        """Get the full level path."""
        level_values = []
        for i in range(1, 11):
            level_key = f"level_{i}"
            if level_key in self.levels and self.levels[level_key]:
                level_values.append(self.levels[level_key])
        return separator.join(level_values)

    def get_depth(self) -> int:
        """Get the hierarchy depth (number of populated levels)."""
        count = 0
        for i in range(1, 11):
            level_key = f"level_{i}"
            if level_key in self.levels and self.levels[level_key]:
                count += 1
        return count


@dataclass
class V3Mapping:
    """V3 Source Mapping representation."""

    hierarchy_id: str
    mapping_index: int
    source_database: str = ""
    source_schema: str = ""
    source_table: str = ""
    source_column: str = ""
    source_uid: str = ""
    precedence_group: int = 1
    include_flag: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hierarchy_id": self.hierarchy_id,
            "mapping_index": self.mapping_index,
            "source_database": self.source_database,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "source_column": self.source_column,
            "source_uid": self.source_uid,
            "precedence_group": self.precedence_group,
            "include_flag": self.include_flag,
        }

    def get_full_path(self) -> str:
        """Get full source path."""
        parts = [
            p for p in [
                self.source_database,
                self.source_schema,
                self.source_table,
                self.source_column,
            ] if p
        ]
        return ".".join(parts)


@dataclass
class V3ClientResult:
    """Result from V3 client operations."""

    success: bool
    message: str = ""
    data: Any = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "errors": self.errors,
        }


class V3HierarchyClient:
    """
    Client for accessing V3 Hierarchy Builder data.

    Provides:
    - Project listing and retrieval
    - Hierarchy listing and tree navigation
    - Source mapping retrieval
    - Caching for performance
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        mode: V3ConnectionMode = V3ConnectionMode.HTTP,
        timeout: float = 30.0,
        cache_enabled: bool = True,
    ):
        """
        Initialize the V3 client.

        Args:
            base_url: V3 API base URL (for HTTP mode).
            mode: Connection mode (HTTP or DIRECT).
            timeout: Request timeout in seconds.
            cache_enabled: Whether to cache responses.
        """
        self.base_url = base_url or "http://localhost:8000"
        self.mode = mode
        self.timeout = timeout
        self.cache_enabled = cache_enabled

        self._cache: Dict[str, Any] = {}
        self._http_client: Optional[Any] = None

    def _get_http_client(self):
        """Get or create HTTP client."""
        if self._http_client is None:
            if not HTTPX_AVAILABLE:
                raise ImportError("httpx is required for HTTP mode")
            self._http_client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout,
            )
        return self._http_client

    def _cache_key(self, operation: str, **kwargs) -> str:
        """Generate cache key."""
        parts = [operation]
        for k, v in sorted(kwargs.items()):
            parts.append(f"{k}={v}")
        return ":".join(parts)

    def _get_cached(self, key: str) -> Optional[Any]:
        """Get from cache."""
        if self.cache_enabled and key in self._cache:
            return self._cache[key]
        return None

    def _set_cached(self, key: str, value: Any) -> None:
        """Set in cache."""
        if self.cache_enabled:
            self._cache[key] = value

    def clear_cache(self) -> None:
        """Clear the cache."""
        self._cache.clear()

    # ==================== Project Operations ====================

    def list_projects(self) -> V3ClientResult:
        """
        List all V3 projects.

        Returns:
            V3ClientResult with list of V3Project objects.
        """
        cache_key = self._cache_key("list_projects")
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                response = client.get("/api/projects")
                response.raise_for_status()
                data = response.json()

                projects = [
                    V3Project(
                        id=p.get("id", p.get("project_id", "")),
                        name=p.get("name", p.get("project_name", "")),
                        description=p.get("description", ""),
                        client_name=p.get("client_name", ""),
                        hierarchy_count=p.get("hierarchy_count", 0),
                        created_at=p.get("created_at"),
                        updated_at=p.get("updated_at"),
                    )
                    for p in data.get("projects", data if isinstance(data, list) else [])
                ]

                result = V3ClientResult(
                    success=True,
                    message=f"Found {len(projects)} projects",
                    data=[p.to_dict() for p in projects],
                )
            else:
                # Direct mode - would use SQLAlchemy
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to list projects: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to list projects: {str(e)}",
                errors=[str(e)],
            )

    def get_project(self, project_id: str) -> V3ClientResult:
        """
        Get a specific V3 project.

        Args:
            project_id: Project identifier.

        Returns:
            V3ClientResult with V3Project.
        """
        cache_key = self._cache_key("get_project", project_id=project_id)
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                response = client.get(f"/api/projects/{project_id}")
                response.raise_for_status()
                data = response.json()

                project = V3Project(
                    id=data.get("id", data.get("project_id", "")),
                    name=data.get("name", data.get("project_name", "")),
                    description=data.get("description", ""),
                    client_name=data.get("client_name", ""),
                    hierarchy_count=data.get("hierarchy_count", 0),
                    created_at=data.get("created_at"),
                    updated_at=data.get("updated_at"),
                )

                result = V3ClientResult(
                    success=True,
                    message="Project found",
                    data=project.to_dict(),
                )
            else:
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to get project: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to get project: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Hierarchy Operations ====================

    def list_hierarchies(
        self,
        project_id: str,
        parent_id: Optional[str] = None,
    ) -> V3ClientResult:
        """
        List hierarchies for a project.

        Args:
            project_id: Project identifier.
            parent_id: Optional parent hierarchy ID to filter.

        Returns:
            V3ClientResult with list of V3Hierarchy objects.
        """
        cache_key = self._cache_key("list_hierarchies", project_id=project_id, parent_id=parent_id or "")
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                params = {}
                if parent_id:
                    params["parent_id"] = parent_id

                response = client.get(f"/api/projects/{project_id}/hierarchies", params=params)
                response.raise_for_status()
                data = response.json()

                hierarchies = [
                    self._parse_hierarchy(h) for h in data.get("hierarchies", data if isinstance(data, list) else [])
                ]

                result = V3ClientResult(
                    success=True,
                    message=f"Found {len(hierarchies)} hierarchies",
                    data=[h.to_dict() for h in hierarchies],
                )
            else:
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to list hierarchies: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to list hierarchies: {str(e)}",
                errors=[str(e)],
            )

    def get_hierarchy(self, hierarchy_id: str) -> V3ClientResult:
        """
        Get a specific hierarchy.

        Args:
            hierarchy_id: Hierarchy identifier.

        Returns:
            V3ClientResult with V3Hierarchy.
        """
        cache_key = self._cache_key("get_hierarchy", hierarchy_id=hierarchy_id)
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                response = client.get(f"/api/hierarchies/{hierarchy_id}")
                response.raise_for_status()
                data = response.json()

                hierarchy = self._parse_hierarchy(data)

                result = V3ClientResult(
                    success=True,
                    message="Hierarchy found",
                    data=hierarchy.to_dict(),
                )
            else:
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to get hierarchy: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to get hierarchy: {str(e)}",
                errors=[str(e)],
            )

    def get_hierarchy_tree(
        self,
        project_id: str,
        root_id: Optional[str] = None,
        max_depth: int = 10,
    ) -> V3ClientResult:
        """
        Get hierarchy tree structure.

        Args:
            project_id: Project identifier.
            root_id: Optional root hierarchy ID.
            max_depth: Maximum depth to traverse.

        Returns:
            V3ClientResult with tree structure.
        """
        cache_key = self._cache_key(
            "get_hierarchy_tree",
            project_id=project_id,
            root_id=root_id or "",
            max_depth=max_depth,
        )
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                params = {"max_depth": max_depth}
                if root_id:
                    params["root_id"] = root_id

                response = client.get(f"/api/projects/{project_id}/tree", params=params)
                response.raise_for_status()
                data = response.json()

                result = V3ClientResult(
                    success=True,
                    message="Tree retrieved",
                    data=data,
                )
            else:
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to get hierarchy tree: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to get hierarchy tree: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Mapping Operations ====================

    def get_mappings(
        self,
        hierarchy_id: str,
    ) -> V3ClientResult:
        """
        Get source mappings for a hierarchy.

        Args:
            hierarchy_id: Hierarchy identifier.

        Returns:
            V3ClientResult with list of V3Mapping objects.
        """
        cache_key = self._cache_key("get_mappings", hierarchy_id=hierarchy_id)
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                response = client.get(f"/api/hierarchies/{hierarchy_id}/mappings")
                response.raise_for_status()
                data = response.json()

                mappings = [
                    V3Mapping(
                        hierarchy_id=m.get("hierarchy_id", hierarchy_id),
                        mapping_index=m.get("mapping_index", i),
                        source_database=m.get("source_database", ""),
                        source_schema=m.get("source_schema", ""),
                        source_table=m.get("source_table", ""),
                        source_column=m.get("source_column", ""),
                        source_uid=m.get("source_uid", ""),
                        precedence_group=m.get("precedence_group", 1),
                        include_flag=m.get("include_flag", True),
                    )
                    for i, m in enumerate(data.get("mappings", data if isinstance(data, list) else []))
                ]

                result = V3ClientResult(
                    success=True,
                    message=f"Found {len(mappings)} mappings",
                    data=[m.to_dict() for m in mappings],
                )
            else:
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to get mappings: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to get mappings: {str(e)}",
                errors=[str(e)],
            )

    def get_all_mappings(self, project_id: str) -> V3ClientResult:
        """
        Get all mappings for a project.

        Args:
            project_id: Project identifier.

        Returns:
            V3ClientResult with all mappings grouped by hierarchy.
        """
        cache_key = self._cache_key("get_all_mappings", project_id=project_id)
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            if self.mode == V3ConnectionMode.HTTP:
                client = self._get_http_client()
                response = client.get(f"/api/projects/{project_id}/mappings")
                response.raise_for_status()
                data = response.json()

                result = V3ClientResult(
                    success=True,
                    message="Mappings retrieved",
                    data=data,
                )
            else:
                result = V3ClientResult(
                    success=False,
                    errors=["Direct mode not implemented"],
                )

            self._set_cached(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Failed to get all mappings: {e}")
            return V3ClientResult(
                success=False,
                message=f"Failed to get all mappings: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Helper Methods ====================

    def _parse_hierarchy(self, data: Dict[str, Any]) -> V3Hierarchy:
        """Parse hierarchy data into V3Hierarchy object."""
        # Extract levels
        levels = {}
        for i in range(1, 11):
            level_key = f"level_{i}"
            if level_key in data and data[level_key]:
                levels[level_key] = data[level_key]

        return V3Hierarchy(
            hierarchy_id=data.get("hierarchy_id", data.get("id", "")),
            hierarchy_name=data.get("hierarchy_name", data.get("name", "")),
            project_id=data.get("project_id", ""),
            parent_id=data.get("parent_id"),
            description=data.get("description", ""),
            levels=levels,
            formula_group=data.get("formula_group", ""),
            sort_order=data.get("sort_order", 0),
            include_flag=data.get("include_flag", True),
            metadata={
                k: v for k, v in data.items()
                if k not in [
                    "hierarchy_id", "id", "hierarchy_name", "name",
                    "project_id", "parent_id", "description",
                    "formula_group", "sort_order", "include_flag",
                ] and not k.startswith("level_")
            },
        )

    def close(self) -> None:
        """Close the client and release resources."""
        if self._http_client is not None:
            self._http_client.close()
            self._http_client = None
        self._cache.clear()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
