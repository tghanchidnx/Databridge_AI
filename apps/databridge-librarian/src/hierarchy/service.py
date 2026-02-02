"""
Hierarchy Service for DataBridge AI Librarian.

Provides CRUD operations for projects, hierarchies, and source mappings.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..core.database import (
    Project,
    Hierarchy,
    SourceMapping,
    FormulaGroup,
    FormulaRule,
    session_scope,
    get_session,
)
from ..core.audit import log_action


class HierarchyServiceError(Exception):
    """Base exception for hierarchy service errors."""
    pass


class ProjectNotFoundError(HierarchyServiceError):
    """Raised when a project is not found."""
    pass


class HierarchyNotFoundError(HierarchyServiceError):
    """Raised when a hierarchy is not found."""
    pass


class DuplicateError(HierarchyServiceError):
    """Raised when attempting to create a duplicate entry."""
    pass


class HierarchyService:
    """
    Service layer for hierarchy management operations.

    Provides methods for:
    - Project CRUD operations
    - Hierarchy CRUD operations
    - Source mapping management
    - Formula group management
    - Query and search operations
    """

    def __init__(self, session: Optional[Session] = None, enable_audit: bool = True):
        """
        Initialize the hierarchy service.

        Args:
            session: Optional SQLAlchemy session. If None, creates new sessions per operation.
            enable_audit: Whether to enable audit logging (set False for testing).
        """
        self._session = session
        self._use_external_session = session is not None
        self._enable_audit = enable_audit

    def _get_session(self) -> Session:
        """Get or create a session."""
        if self._session:
            return self._session
        return get_session()

    def _close_session(self, session: Session) -> None:
        """Close session if we created it."""
        if not self._use_external_session:
            session.close()

    def _log_action(self, **kwargs) -> Optional[int]:
        """Log an audit action if enabled."""
        if self._enable_audit:
            return log_action(**kwargs)
        return None

    # =========================================================================
    # Project Operations
    # =========================================================================

    def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        industry: Optional[str] = None,
        created_by: Optional[str] = None,
    ) -> Project:
        """
        Create a new hierarchy project.

        Args:
            name: Project name (must be unique).
            description: Optional project description.
            industry: Optional industry category.
            created_by: Optional creator identifier.

        Returns:
            Project: The created project.

        Raises:
            DuplicateError: If a project with the same name exists.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            # Check for duplicate
            existing = session.query(Project).filter(Project.name == name).first()
            if existing:
                raise DuplicateError(f"Project '{name}' already exists")

            project = Project(
                name=name,
                description=description,
                industry=industry,
                created_by=created_by,
            )
            session.add(project)
            session.flush()

            self._log_action(
                action="create_project",
                entity_type="project",
                entity_id=project.id,
                details={"name": name, "industry": industry},
            )

            if should_commit:
                session.commit()
                # Refresh to load any generated values
                session.refresh(project)
            # Make detached copy with all attributes loaded
            session.expunge(project)
            return project
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def get_project(self, project_id: str) -> Project:
        """
        Get a project by ID.

        Args:
            project_id: Project ID (supports partial match).

        Returns:
            Project: The found project.

        Raises:
            ProjectNotFoundError: If project not found.
        """
        session = self._get_session()
        try:
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")
            return project
        finally:
            self._close_session(session)

    def get_project_by_name(self, name: str) -> Project:
        """
        Get a project by name.

        Args:
            name: Project name.

        Returns:
            Project: The found project.

        Raises:
            ProjectNotFoundError: If project not found.
        """
        session = self._get_session()
        try:
            project = session.query(Project).filter(Project.name == name).first()
            if not project:
                raise ProjectNotFoundError(f"Project not found: {name}")
            return project
        finally:
            self._close_session(session)

    def list_projects(
        self,
        industry: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Project]:
        """
        List projects with optional filtering.

        Args:
            industry: Filter by industry.
            search: Search in name and description.
            limit: Maximum results to return.
            offset: Number of results to skip.

        Returns:
            List[Project]: List of matching projects.
        """
        session = self._get_session()
        try:
            query = session.query(Project)

            if industry:
                query = query.filter(Project.industry == industry)

            if search:
                search_pattern = f"%{search}%"
                query = query.filter(
                    or_(
                        Project.name.ilike(search_pattern),
                        Project.description.ilike(search_pattern),
                    )
                )

            query = query.order_by(Project.created_at.desc())
            query = query.offset(offset).limit(limit)

            return query.all()
        finally:
            self._close_session(session)

    def update_project(
        self,
        project_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> Project:
        """
        Update a project.

        Args:
            project_id: Project ID.
            name: New name (optional).
            description: New description (optional).
            industry: New industry (optional).

        Returns:
            Project: Updated project.

        Raises:
            ProjectNotFoundError: If project not found.
            DuplicateError: If new name already exists.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            if name and name != project.name:
                existing = session.query(Project).filter(Project.name == name).first()
                if existing:
                    raise DuplicateError(f"Project '{name}' already exists")
                project.name = name

            if description is not None:
                project.description = description

            if industry is not None:
                project.industry = industry

            project.updated_at = datetime.now(timezone.utc)

            self._log_action(
                action="update_project",
                entity_type="project",
                entity_id=project.id,
                details={"name": name, "industry": industry},
            )

            if should_commit:
                session.commit()
                session.refresh(project)
            session.expunge(project)
            return project
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def delete_project(self, project_id: str, cascade: bool = False) -> bool:
        """
        Delete a project.

        Args:
            project_id: Project ID.
            cascade: If True, delete all hierarchies and mappings.

        Returns:
            bool: True if deleted.

        Raises:
            ProjectNotFoundError: If project not found.
            HierarchyServiceError: If project has hierarchies and cascade=False.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            # Check for hierarchies
            hierarchy_count = (
                session.query(Hierarchy)
                .filter(Hierarchy.project_id == project.id)
                .count()
            )

            if hierarchy_count > 0 and not cascade:
                raise HierarchyServiceError(
                    f"Project has {hierarchy_count} hierarchies. Use cascade=True to delete all."
                )

            if cascade:
                # Delete mappings first
                hierarchy_ids = [
                    h.hierarchy_id
                    for h in session.query(Hierarchy.hierarchy_id)
                    .filter(Hierarchy.project_id == project.id)
                    .all()
                ]
                if hierarchy_ids:
                    session.query(SourceMapping).filter(
                        SourceMapping.hierarchy_id.in_(hierarchy_ids)
                    ).delete(synchronize_session=False)

                # Delete hierarchies
                session.query(Hierarchy).filter(
                    Hierarchy.project_id == project.id
                ).delete(synchronize_session=False)

            self._log_action(
                action="delete_project",
                entity_type="project",
                entity_id=project.id,
                details={"name": project.name, "cascade": cascade},
            )

            session.delete(project)
            if should_commit:
                session.commit()
            return True
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    # =========================================================================
    # Hierarchy Operations
    # =========================================================================

    def create_hierarchy(
        self,
        project_id: str,
        hierarchy_id: str,
        hierarchy_name: str,
        parent_id: Optional[str] = None,
        description: Optional[str] = None,
        levels: Optional[Dict[str, str]] = None,
        level_sorts: Optional[Dict[str, int]] = None,
        flags: Optional[Dict[str, bool]] = None,
        sort_order: int = 0,
        formula_group: Optional[str] = None,
    ) -> Hierarchy:
        """
        Create a new hierarchy node.

        Args:
            project_id: Parent project ID.
            hierarchy_id: Unique hierarchy identifier.
            hierarchy_name: Display name.
            parent_id: Parent hierarchy ID (optional).
            description: Optional description.
            levels: Dictionary of level values (level_1, level_2, etc.).
            level_sorts: Dictionary of level sort orders.
            flags: Dictionary of boolean flags.
            sort_order: Overall sort order.
            formula_group: Optional formula group name.

        Returns:
            Hierarchy: The created hierarchy.

        Raises:
            ProjectNotFoundError: If project not found.
            DuplicateError: If hierarchy_id already exists.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            # Verify project exists
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            # Check for duplicate hierarchy_id
            existing = (
                session.query(Hierarchy)
                .filter(Hierarchy.hierarchy_id == hierarchy_id)
                .first()
            )
            if existing:
                raise DuplicateError(f"Hierarchy ID '{hierarchy_id}' already exists")

            # Build hierarchy data
            hierarchy_data = {
                "project_id": project.id,
                "hierarchy_id": hierarchy_id,
                "hierarchy_name": hierarchy_name,
                "parent_id": parent_id,
                "description": description,
                "sort_order": sort_order,
            }

            # Add levels
            if levels:
                for key, value in levels.items():
                    if key.startswith("level_") and hasattr(Hierarchy, key):
                        hierarchy_data[key] = value

            # Add level sorts
            if level_sorts:
                for key, value in level_sorts.items():
                    sort_key = f"{key}_sort" if not key.endswith("_sort") else key
                    if hasattr(Hierarchy, sort_key):
                        hierarchy_data[sort_key] = value

            # Add flags
            if flags:
                for key, value in flags.items():
                    if hasattr(Hierarchy, key):
                        hierarchy_data[key] = value

            hierarchy = Hierarchy(**hierarchy_data)
            session.add(hierarchy)
            session.flush()

            self._log_action(
                action="create_hierarchy",
                entity_type="hierarchy",
                entity_id=hierarchy_id,
                details={
                    "project_id": project.id,
                    "name": hierarchy_name,
                    "parent_id": parent_id,
                },
            )

            if should_commit:
                session.commit()
                session.refresh(hierarchy)
            session.expunge(hierarchy)
            return hierarchy
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def get_hierarchy(
        self,
        hierarchy_id: str,
        include_inactive: bool = False,
    ) -> Hierarchy:
        """
        Get a hierarchy by ID.

        Args:
            hierarchy_id: Hierarchy ID.
            include_inactive: Include non-current versions.

        Returns:
            Hierarchy: The found hierarchy.

        Raises:
            HierarchyNotFoundError: If hierarchy not found.
        """
        session = self._get_session()
        try:
            query = session.query(Hierarchy).filter(
                Hierarchy.hierarchy_id == hierarchy_id
            )

            if not include_inactive:
                query = query.filter(Hierarchy.is_current == True)

            hierarchy = query.first()
            if not hierarchy:
                raise HierarchyNotFoundError(f"Hierarchy not found: {hierarchy_id}")
            return hierarchy
        finally:
            self._close_session(session)

    def list_hierarchies(
        self,
        project_id: str,
        parent_id: Optional[str] = None,
        include_inactive: bool = False,
        search: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Hierarchy]:
        """
        List hierarchies in a project.

        Args:
            project_id: Project ID.
            parent_id: Filter by parent (None for roots, specific ID for children).
            include_inactive: Include non-current versions.
            search: Search in name and description.
            limit: Maximum results.
            offset: Results to skip.

        Returns:
            List[Hierarchy]: List of hierarchies.
        """
        session = self._get_session()
        try:
            # Get full project ID
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            query = session.query(Hierarchy).filter(
                Hierarchy.project_id == project.id
            )

            if not include_inactive:
                query = query.filter(Hierarchy.is_current == True)

            if parent_id is not None:
                if parent_id == "":
                    # Root nodes only
                    query = query.filter(Hierarchy.parent_id.is_(None))
                else:
                    query = query.filter(Hierarchy.parent_id == parent_id)

            if search:
                search_pattern = f"%{search}%"
                query = query.filter(
                    or_(
                        Hierarchy.hierarchy_name.ilike(search_pattern),
                        Hierarchy.description.ilike(search_pattern),
                        Hierarchy.hierarchy_id.ilike(search_pattern),
                    )
                )

            query = query.order_by(Hierarchy.sort_order, Hierarchy.hierarchy_name)
            query = query.offset(offset).limit(limit)

            return query.all()
        finally:
            self._close_session(session)

    def update_hierarchy(
        self,
        hierarchy_id: str,
        hierarchy_name: Optional[str] = None,
        parent_id: Optional[str] = None,
        description: Optional[str] = None,
        levels: Optional[Dict[str, str]] = None,
        level_sorts: Optional[Dict[str, int]] = None,
        flags: Optional[Dict[str, bool]] = None,
        sort_order: Optional[int] = None,
        create_version: bool = False,
    ) -> Hierarchy:
        """
        Update a hierarchy.

        Args:
            hierarchy_id: Hierarchy ID.
            hierarchy_name: New name (optional).
            parent_id: New parent ID (optional).
            description: New description (optional).
            levels: Dictionary of level values to update.
            level_sorts: Dictionary of level sort orders to update.
            flags: Dictionary of flags to update.
            sort_order: New sort order (optional).
            create_version: If True, creates a new version (SCD Type 2).

        Returns:
            Hierarchy: Updated hierarchy.

        Raises:
            HierarchyNotFoundError: If hierarchy not found.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            hierarchy = (
                session.query(Hierarchy)
                .filter(Hierarchy.hierarchy_id == hierarchy_id)
                .filter(Hierarchy.is_current == True)
                .first()
            )
            if not hierarchy:
                raise HierarchyNotFoundError(f"Hierarchy not found: {hierarchy_id}")

            if create_version:
                # SCD Type 2: Close current version and create new
                hierarchy.is_current = False
                hierarchy.effective_to = datetime.now(timezone.utc)

                # Create new version
                new_hierarchy = Hierarchy(
                    project_id=hierarchy.project_id,
                    hierarchy_id=hierarchy_id,
                    hierarchy_name=hierarchy_name or hierarchy.hierarchy_name,
                    parent_id=parent_id if parent_id is not None else hierarchy.parent_id,
                    description=description if description is not None else hierarchy.description,
                    sort_order=sort_order if sort_order is not None else hierarchy.sort_order,
                    version_number=hierarchy.version_number + 1,
                )

                # Copy levels
                for i in range(1, 16):
                    level_key = f"level_{i}"
                    sort_key = f"level_{i}_sort"
                    if levels and level_key in levels:
                        setattr(new_hierarchy, level_key, levels[level_key])
                    else:
                        setattr(new_hierarchy, level_key, getattr(hierarchy, level_key))
                    if level_sorts and sort_key in level_sorts:
                        setattr(new_hierarchy, sort_key, level_sorts[sort_key])
                    else:
                        setattr(new_hierarchy, sort_key, getattr(hierarchy, sort_key))

                # Copy flags
                flag_names = [
                    "include_flag", "exclude_flag", "transform_flag",
                    "calculation_flag", "active_flag", "is_leaf_node"
                ]
                for flag in flag_names:
                    if flags and flag in flags:
                        setattr(new_hierarchy, flag, flags[flag])
                    else:
                        setattr(new_hierarchy, flag, getattr(hierarchy, flag))

                session.add(new_hierarchy)
                hierarchy = new_hierarchy
            else:
                # Direct update
                if hierarchy_name is not None:
                    hierarchy.hierarchy_name = hierarchy_name
                if parent_id is not None:
                    hierarchy.parent_id = parent_id if parent_id else None
                if description is not None:
                    hierarchy.description = description
                if sort_order is not None:
                    hierarchy.sort_order = sort_order

                if levels:
                    for key, value in levels.items():
                        if hasattr(hierarchy, key):
                            setattr(hierarchy, key, value)

                if level_sorts:
                    for key, value in level_sorts.items():
                        sort_key = f"{key}_sort" if not key.endswith("_sort") else key
                        if hasattr(hierarchy, sort_key):
                            setattr(hierarchy, sort_key, value)

                if flags:
                    for key, value in flags.items():
                        if hasattr(hierarchy, key):
                            setattr(hierarchy, key, value)

            self._log_action(
                action="update_hierarchy",
                entity_type="hierarchy",
                entity_id=hierarchy_id,
                details={
                    "name": hierarchy_name,
                    "parent_id": parent_id,
                    "versioned": create_version,
                },
            )

            if should_commit:
                session.commit()
                session.refresh(hierarchy)
            session.expunge(hierarchy)
            return hierarchy
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def delete_hierarchy(
        self,
        hierarchy_id: str,
        cascade: bool = False,
        soft_delete: bool = True,
    ) -> bool:
        """
        Delete a hierarchy.

        Args:
            hierarchy_id: Hierarchy ID.
            cascade: Delete children and mappings.
            soft_delete: Mark inactive instead of hard delete.

        Returns:
            bool: True if deleted.

        Raises:
            HierarchyNotFoundError: If hierarchy not found.
            HierarchyServiceError: If has children and cascade=False.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            hierarchy = (
                session.query(Hierarchy)
                .filter(Hierarchy.hierarchy_id == hierarchy_id)
                .filter(Hierarchy.is_current == True)
                .first()
            )
            if not hierarchy:
                raise HierarchyNotFoundError(f"Hierarchy not found: {hierarchy_id}")

            # Check for children
            children = (
                session.query(Hierarchy)
                .filter(Hierarchy.parent_id == hierarchy_id)
                .filter(Hierarchy.is_current == True)
                .count()
            )

            if children > 0 and not cascade:
                raise HierarchyServiceError(
                    f"Hierarchy has {children} children. Use cascade=True to delete all."
                )

            if cascade:
                # Recursively handle children
                child_ids = [
                    h.hierarchy_id
                    for h in session.query(Hierarchy.hierarchy_id)
                    .filter(Hierarchy.parent_id == hierarchy_id)
                    .filter(Hierarchy.is_current == True)
                    .all()
                ]
                for child_id in child_ids:
                    self.delete_hierarchy(child_id, cascade=True, soft_delete=soft_delete)

            # Delete mappings
            session.query(SourceMapping).filter(
                SourceMapping.hierarchy_id == hierarchy_id
            ).delete(synchronize_session=False)

            if soft_delete:
                hierarchy.is_current = False
                hierarchy.active_flag = False
                hierarchy.effective_to = datetime.now(timezone.utc)
            else:
                session.delete(hierarchy)

            self._log_action(
                action="delete_hierarchy",
                entity_type="hierarchy",
                entity_id=hierarchy_id,
                details={"cascade": cascade, "soft_delete": soft_delete},
            )

            if should_commit:
                session.commit()
            return True
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def move_hierarchy(
        self,
        hierarchy_id: str,
        new_parent_id: Optional[str],
        new_sort_order: Optional[int] = None,
    ) -> Hierarchy:
        """
        Move a hierarchy to a new parent.

        Args:
            hierarchy_id: Hierarchy ID to move.
            new_parent_id: New parent ID (None for root).
            new_sort_order: New sort order (optional).

        Returns:
            Hierarchy: Updated hierarchy.

        Raises:
            HierarchyNotFoundError: If hierarchy or new parent not found.
            HierarchyServiceError: If circular reference detected.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            hierarchy = (
                session.query(Hierarchy)
                .filter(Hierarchy.hierarchy_id == hierarchy_id)
                .filter(Hierarchy.is_current == True)
                .first()
            )
            if not hierarchy:
                raise HierarchyNotFoundError(f"Hierarchy not found: {hierarchy_id}")

            # Validate new parent exists
            if new_parent_id:
                new_parent = (
                    session.query(Hierarchy)
                    .filter(Hierarchy.hierarchy_id == new_parent_id)
                    .filter(Hierarchy.is_current == True)
                    .first()
                )
                if not new_parent:
                    raise HierarchyNotFoundError(f"New parent not found: {new_parent_id}")

                # Check for circular reference
                current = new_parent
                while current:
                    if current.hierarchy_id == hierarchy_id:
                        raise HierarchyServiceError(
                            "Cannot move: would create circular reference"
                        )
                    if current.parent_id:
                        current = (
                            session.query(Hierarchy)
                            .filter(Hierarchy.hierarchy_id == current.parent_id)
                            .filter(Hierarchy.is_current == True)
                            .first()
                        )
                    else:
                        break

            hierarchy.parent_id = new_parent_id
            if new_sort_order is not None:
                hierarchy.sort_order = new_sort_order

            self._log_action(
                action="move_hierarchy",
                entity_type="hierarchy",
                entity_id=hierarchy_id,
                details={
                    "new_parent_id": new_parent_id,
                    "new_sort_order": new_sort_order,
                },
            )

            if should_commit:
                session.commit()
                session.refresh(hierarchy)
            session.expunge(hierarchy)
            return hierarchy
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    # =========================================================================
    # Source Mapping Operations
    # =========================================================================

    def add_source_mapping(
        self,
        hierarchy_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_column: str,
        source_uid: Optional[str] = None,
        mapping_index: int = 0,
        precedence_group: str = "DEFAULT",
        flags: Optional[Dict[str, bool]] = None,
    ) -> SourceMapping:
        """
        Add a source mapping to a hierarchy.

        Args:
            hierarchy_id: Target hierarchy ID.
            source_database: Source database name.
            source_schema: Source schema name.
            source_table: Source table name.
            source_column: Source column name.
            source_uid: Optional unique identifier filter.
            mapping_index: Order within hierarchy.
            precedence_group: Precedence group name.
            flags: Boolean flags.

        Returns:
            SourceMapping: Created mapping.

        Raises:
            HierarchyNotFoundError: If hierarchy not found.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            # Verify hierarchy exists
            hierarchy = (
                session.query(Hierarchy)
                .filter(Hierarchy.hierarchy_id == hierarchy_id)
                .filter(Hierarchy.is_current == True)
                .first()
            )
            if not hierarchy:
                raise HierarchyNotFoundError(f"Hierarchy not found: {hierarchy_id}")

            mapping_data = {
                "hierarchy_id": hierarchy_id,
                "source_database": source_database,
                "source_schema": source_schema,
                "source_table": source_table,
                "source_column": source_column,
                "source_uid": source_uid,
                "mapping_index": mapping_index,
                "precedence_group": precedence_group,
            }

            if flags:
                for key, value in flags.items():
                    if hasattr(SourceMapping, key):
                        mapping_data[key] = value

            mapping = SourceMapping(**mapping_data)
            session.add(mapping)
            session.flush()

            self._log_action(
                action="add_source_mapping",
                entity_type="mapping",
                entity_id=str(mapping.id),
                details={
                    "hierarchy_id": hierarchy_id,
                    "source": f"{source_database}.{source_schema}.{source_table}.{source_column}",
                },
            )

            if should_commit:
                session.commit()
                session.refresh(mapping)
            session.expunge(mapping)
            return mapping
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def get_mappings(
        self,
        hierarchy_id: str,
        precedence_group: Optional[str] = None,
    ) -> List[SourceMapping]:
        """
        Get source mappings for a hierarchy.

        Args:
            hierarchy_id: Hierarchy ID.
            precedence_group: Filter by precedence group.

        Returns:
            List[SourceMapping]: List of mappings.
        """
        session = self._get_session()
        try:
            query = session.query(SourceMapping).filter(
                SourceMapping.hierarchy_id == hierarchy_id
            )

            if precedence_group:
                query = query.filter(SourceMapping.precedence_group == precedence_group)

            query = query.order_by(SourceMapping.mapping_index)

            return query.all()
        finally:
            self._close_session(session)

    def remove_source_mapping(self, mapping_id: int) -> bool:
        """
        Remove a source mapping.

        Args:
            mapping_id: Mapping ID.

        Returns:
            bool: True if removed.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            mapping = session.query(SourceMapping).filter(
                SourceMapping.id == mapping_id
            ).first()
            if not mapping:
                return False

            self._log_action(
                action="remove_source_mapping",
                entity_type="mapping",
                entity_id=str(mapping_id),
                details={"hierarchy_id": mapping.hierarchy_id},
            )

            session.delete(mapping)
            if should_commit:
                session.commit()
            return True
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    # =========================================================================
    # Formula Group Operations
    # =========================================================================

    def create_formula_group(
        self,
        project_id: str,
        name: str,
        description: Optional[str] = None,
    ) -> FormulaGroup:
        """
        Create a formula group.

        Args:
            project_id: Project ID.
            name: Group name.
            description: Optional description.

        Returns:
            FormulaGroup: Created group.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            # Verify project exists
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            group = FormulaGroup(
                project_id=project.id,
                name=name,
                description=description,
            )
            session.add(group)
            session.flush()

            self._log_action(
                action="create_formula_group",
                entity_type="formula_group",
                entity_id=str(group.id),
                details={"name": name, "project_id": project.id},
            )

            if should_commit:
                session.commit()
                session.refresh(group)
            session.expunge(group)
            return group
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def add_formula_rule(
        self,
        group_id: int,
        target_hierarchy_id: str,
        source_hierarchy_ids: List[str],
        operation: str,
        rule_order: int = 0,
    ) -> FormulaRule:
        """
        Add a formula rule to a group.

        Args:
            group_id: Formula group ID.
            target_hierarchy_id: Target hierarchy for calculation result.
            source_hierarchy_ids: Source hierarchy IDs for calculation.
            operation: Operation (SUM, SUBTRACT, MULTIPLY, DIVIDE, PERCENT).
            rule_order: Order of rule application.

        Returns:
            FormulaRule: Created rule.
        """
        session = self._get_session()
        should_commit = not self._use_external_session
        try:
            rule = FormulaRule(
                group_id=group_id,
                target_hierarchy_id=target_hierarchy_id,
                source_hierarchy_ids=",".join(source_hierarchy_ids),
                operation=operation.upper(),
                rule_order=rule_order,
            )
            session.add(rule)
            session.flush()

            self._log_action(
                action="add_formula_rule",
                entity_type="formula_rule",
                entity_id=str(rule.id),
                details={
                    "target": target_hierarchy_id,
                    "sources": source_hierarchy_ids,
                    "operation": operation,
                },
            )

            if should_commit:
                session.commit()
                session.refresh(rule)
            session.expunge(rule)
            return rule
        except Exception:
            if should_commit:
                session.rollback()
            raise
        finally:
            self._close_session(session)

    def list_formula_groups(self, project_id: str) -> List[FormulaGroup]:
        """
        List formula groups in a project.

        Args:
            project_id: Project ID.

        Returns:
            List[FormulaGroup]: Formula groups.
        """
        session = self._get_session()
        try:
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            return (
                session.query(FormulaGroup)
                .filter(FormulaGroup.project_id == project.id)
                .order_by(FormulaGroup.name)
                .all()
            )
        finally:
            self._close_session(session)

    # =========================================================================
    # Statistics and Counts
    # =========================================================================

    def get_project_stats(self, project_id: str) -> Dict[str, Any]:
        """
        Get statistics for a project.

        Args:
            project_id: Project ID.

        Returns:
            Dict with counts and statistics.
        """
        session = self._get_session()
        try:
            project = (
                session.query(Project)
                .filter(Project.id.like(f"{project_id}%"))
                .first()
            )
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            hierarchy_count = (
                session.query(Hierarchy)
                .filter(Hierarchy.project_id == project.id)
                .filter(Hierarchy.is_current == True)
                .count()
            )

            root_count = (
                session.query(Hierarchy)
                .filter(Hierarchy.project_id == project.id)
                .filter(Hierarchy.is_current == True)
                .filter(Hierarchy.parent_id.is_(None))
                .count()
            )

            leaf_count = (
                session.query(Hierarchy)
                .filter(Hierarchy.project_id == project.id)
                .filter(Hierarchy.is_current == True)
                .filter(Hierarchy.is_leaf_node == True)
                .count()
            )

            mapping_count = (
                session.query(SourceMapping)
                .join(Hierarchy, SourceMapping.hierarchy_id == Hierarchy.hierarchy_id)
                .filter(Hierarchy.project_id == project.id)
                .filter(Hierarchy.is_current == True)
                .count()
            )

            formula_group_count = (
                session.query(FormulaGroup)
                .filter(FormulaGroup.project_id == project.id)
                .count()
            )

            return {
                "project_id": project.id,
                "project_name": project.name,
                "hierarchy_count": hierarchy_count,
                "root_count": root_count,
                "leaf_count": leaf_count,
                "mapping_count": mapping_count,
                "formula_group_count": formula_group_count,
            }
        finally:
            self._close_session(session)
