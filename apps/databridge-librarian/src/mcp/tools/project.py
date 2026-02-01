"""
MCP tools for project management.

Provides 5 tools for project CRUD operations:
- create_hierarchy_project
- list_hierarchy_projects
- get_hierarchy_project
- update_hierarchy_project
- delete_hierarchy_project
"""

from typing import Optional, List, Dict, Any
from fastmcp import FastMCP

from ...hierarchy.service import (
    HierarchyService,
    DuplicateError,
    ProjectNotFoundError,
)


def register_project_tools(mcp: FastMCP) -> None:
    """Register all project tools with the MCP server."""

    @mcp.tool()
    def create_hierarchy_project(
        name: str,
        description: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new hierarchy project.

        A project is a container for related hierarchies (e.g., "FY2024 P&L").

        Args:
            name: Project name (must be unique).
            description: Optional project description.
            industry: Optional industry category (e.g., "Manufacturing", "Oil & Gas", "SaaS").

        Returns:
            Dictionary with project details including:
            - id: Unique project identifier
            - name: Project name
            - description: Project description
            - industry: Industry category
            - created_at: Creation timestamp
        """
        service = HierarchyService()
        try:
            project = service.create_project(
                name=name,
                description=description,
                industry=industry,
            )
            return {
                "success": True,
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "description": project.description,
                    "industry": project.industry,
                    "created_at": project.created_at.isoformat() if project.created_at else None,
                },
            }
        except DuplicateError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def list_hierarchy_projects(
        industry: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List hierarchy projects with optional filtering.

        Args:
            industry: Filter by industry category.
            search: Search in project names and descriptions.
            limit: Maximum number of results (default 100).
            offset: Number of results to skip for pagination.

        Returns:
            Dictionary with:
            - projects: List of project summaries
            - total: Total count of matching projects
        """
        service = HierarchyService()
        projects = service.list_projects(
            industry=industry,
            search=search,
            limit=limit,
            offset=offset,
        )
        return {
            "success": True,
            "projects": [
                {
                    "id": p.id,
                    "name": p.name,
                    "description": p.description,
                    "industry": p.industry,
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                }
                for p in projects
            ],
            "count": len(projects),
        }

    @mcp.tool()
    def get_hierarchy_project(project_id: str) -> Dict[str, Any]:
        """
        Get a hierarchy project by ID.

        Args:
            project_id: Project ID (supports partial match).

        Returns:
            Dictionary with full project details and statistics.
        """
        service = HierarchyService()
        try:
            project = service.get_project(project_id)
            stats = service.get_project_stats(project.id)
            return {
                "success": True,
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "description": project.description,
                    "industry": project.industry,
                    "created_at": project.created_at.isoformat() if project.created_at else None,
                    "updated_at": project.updated_at.isoformat() if project.updated_at else None,
                },
                "stats": stats,
            }
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def update_hierarchy_project(
        project_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a hierarchy project.

        Args:
            project_id: Project ID to update.
            name: New project name (optional).
            description: New description (optional).
            industry: New industry category (optional).

        Returns:
            Dictionary with updated project details.
        """
        service = HierarchyService()
        try:
            project = service.update_project(
                project_id=project_id,
                name=name,
                description=description,
                industry=industry,
            )
            return {
                "success": True,
                "project": {
                    "id": project.id,
                    "name": project.name,
                    "description": project.description,
                    "industry": project.industry,
                    "updated_at": project.updated_at.isoformat() if project.updated_at else None,
                },
            }
        except (ProjectNotFoundError, DuplicateError) as e:
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def delete_hierarchy_project(
        project_id: str,
        cascade: bool = False,
    ) -> Dict[str, Any]:
        """
        Delete a hierarchy project.

        Args:
            project_id: Project ID to delete.
            cascade: If True, delete all hierarchies and mappings in the project.
                    If False (default), fails if project has hierarchies.

        Returns:
            Dictionary with deletion result.
        """
        service = HierarchyService()
        try:
            service.delete_project(project_id, cascade=cascade)
            return {"success": True, "message": f"Project {project_id} deleted"}
        except ProjectNotFoundError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}
