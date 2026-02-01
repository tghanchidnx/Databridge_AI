"""
Unit tests for HierarchyService.
"""

import pytest
import uuid


class TestProjectOperations:
    """Tests for project CRUD operations."""

    def test_create_project_success(self, db_session):
        """Test creating a project."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        project = service.create_project(
            name=f"Test Project {uuid.uuid4().hex[:8]}",
            description="Test description",
            industry="Manufacturing",
        )

        assert project.id is not None
        assert "Test Project" in project.name
        assert project.industry == "Manufacturing"

    def test_create_project_duplicate_raises(self, db_session):
        """Test that duplicate project names raise error."""
        from src.hierarchy.service import HierarchyService, DuplicateError

        service = HierarchyService(session=db_session, enable_audit=False)
        name = f"Duplicate Test {uuid.uuid4().hex[:8]}"

        service.create_project(name=name)

        with pytest.raises(DuplicateError):
            service.create_project(name=name)

    def test_get_project(self, db_session):
        """Test getting a project by ID."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        created = service.create_project(
            name=f"Get Test {uuid.uuid4().hex[:8]}",
        )

        retrieved = service.get_project(created.id[:8])
        assert retrieved.id == created.id

    def test_get_project_not_found_raises(self, db_session):
        """Test that getting non-existent project raises error."""
        from src.hierarchy.service import HierarchyService, ProjectNotFoundError

        service = HierarchyService(session=db_session, enable_audit=False)

        with pytest.raises(ProjectNotFoundError):
            service.get_project("nonexistent-id")

    def test_list_projects(self, db_session):
        """Test listing projects."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        unique = uuid.uuid4().hex[:8]

        service.create_project(name=f"List Test 1 {unique}")
        service.create_project(name=f"List Test 2 {unique}")

        projects = service.list_projects(search=unique)
        assert len(projects) >= 2

    def test_update_project(self, db_session):
        """Test updating a project."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        created = service.create_project(
            name=f"Update Test {uuid.uuid4().hex[:8]}",
            industry="Manufacturing",
        )

        updated = service.update_project(
            project_id=created.id,
            description="Updated description",
            industry="Retail",
        )

        assert updated.description == "Updated description"
        assert updated.industry == "Retail"

    def test_delete_project(self, db_session):
        """Test deleting a project."""
        from src.hierarchy.service import HierarchyService, ProjectNotFoundError

        service = HierarchyService(session=db_session, enable_audit=False)
        created = service.create_project(
            name=f"Delete Test {uuid.uuid4().hex[:8]}",
        )

        result = service.delete_project(created.id)
        assert result is True

        with pytest.raises(ProjectNotFoundError):
            service.get_project(created.id)


class TestHierarchyOperations:
    """Tests for hierarchy CRUD operations."""

    @pytest.fixture
    def test_project(self, db_session):
        """Create a test project."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        return service.create_project(
            name=f"Hierarchy Test Project {uuid.uuid4().hex[:8]}",
        )

    def test_create_hierarchy(self, db_session, test_project):
        """Test creating a hierarchy."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        hierarchy = service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"HIER-{uuid.uuid4().hex[:8]}",
            hierarchy_name="Test Hierarchy",
            levels={"level_1": "Revenue", "level_2": "Product"},
        )

        assert hierarchy.hierarchy_name == "Test Hierarchy"
        assert hierarchy.level_1 == "Revenue"
        assert hierarchy.level_2 == "Product"

    def test_create_hierarchy_with_parent(self, db_session, test_project):
        """Test creating a hierarchy with a parent."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        unique = uuid.uuid4().hex[:8]

        parent = service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"PARENT-{unique}",
            hierarchy_name="Parent Node",
        )

        child = service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"CHILD-{unique}",
            hierarchy_name="Child Node",
            parent_id=parent.hierarchy_id,
        )

        assert child.parent_id == parent.hierarchy_id

    def test_get_hierarchy(self, db_session, test_project):
        """Test getting a hierarchy."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        hier_id = f"GET-{uuid.uuid4().hex[:8]}"

        service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=hier_id,
            hierarchy_name="Get Test",
        )

        retrieved = service.get_hierarchy(hier_id)
        assert retrieved.hierarchy_id == hier_id

    def test_list_hierarchies(self, db_session, test_project):
        """Test listing hierarchies."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        unique = uuid.uuid4().hex[:8]

        service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"LIST1-{unique}",
            hierarchy_name=f"List Test 1 {unique}",
        )
        service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"LIST2-{unique}",
            hierarchy_name=f"List Test 2 {unique}",
        )

        hierarchies = service.list_hierarchies(
            project_id=test_project.id,
            search=unique,
        )
        assert len(hierarchies) >= 2

    def test_update_hierarchy(self, db_session, test_project):
        """Test updating a hierarchy."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        hier_id = f"UPDATE-{uuid.uuid4().hex[:8]}"

        service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=hier_id,
            hierarchy_name="Original Name",
        )

        updated = service.update_hierarchy(
            hierarchy_id=hier_id,
            hierarchy_name="Updated Name",
            levels={"level_1": "New Level"},
        )

        assert updated.hierarchy_name == "Updated Name"
        assert updated.level_1 == "New Level"

    def test_move_hierarchy(self, db_session, test_project):
        """Test moving a hierarchy to a new parent."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        unique = uuid.uuid4().hex[:8]

        parent1 = service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"P1-{unique}",
            hierarchy_name="Parent 1",
        )
        parent2 = service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"P2-{unique}",
            hierarchy_name="Parent 2",
        )
        child = service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=f"C-{unique}",
            hierarchy_name="Child",
            parent_id=parent1.hierarchy_id,
        )

        moved = service.move_hierarchy(
            hierarchy_id=child.hierarchy_id,
            new_parent_id=parent2.hierarchy_id,
        )

        assert moved.parent_id == parent2.hierarchy_id

    def test_delete_hierarchy(self, db_session, test_project):
        """Test deleting a hierarchy."""
        from src.hierarchy.service import HierarchyService, HierarchyNotFoundError

        service = HierarchyService(session=db_session, enable_audit=False)
        hier_id = f"DEL-{uuid.uuid4().hex[:8]}"

        service.create_hierarchy(
            project_id=test_project.id,
            hierarchy_id=hier_id,
            hierarchy_name="Delete Test",
        )

        result = service.delete_hierarchy(hier_id, soft_delete=False)
        assert result is True

        with pytest.raises(HierarchyNotFoundError):
            service.get_hierarchy(hier_id)


class TestSourceMappingOperations:
    """Tests for source mapping operations."""

    @pytest.fixture
    def test_hierarchy(self, db_session):
        """Create a test project and hierarchy."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        project = service.create_project(
            name=f"Mapping Test Project {uuid.uuid4().hex[:8]}",
        )
        hier_id = f"MAP-HIER-{uuid.uuid4().hex[:8]}"
        hierarchy = service.create_hierarchy(
            project_id=project.id,
            hierarchy_id=hier_id,
            hierarchy_name="Mapping Test Hierarchy",
        )
        return hierarchy

    def test_add_source_mapping(self, db_session, test_hierarchy):
        """Test adding a source mapping."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        mapping = service.add_source_mapping(
            hierarchy_id=test_hierarchy.hierarchy_id,
            source_database="ANALYTICS",
            source_schema="PUBLIC",
            source_table="FACT_SALES",
            source_column="AMOUNT",
        )

        assert mapping.id is not None
        assert mapping.source_database == "ANALYTICS"

    def test_get_mappings(self, db_session, test_hierarchy):
        """Test getting mappings for a hierarchy."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)

        service.add_source_mapping(
            hierarchy_id=test_hierarchy.hierarchy_id,
            source_database="DB1",
            source_schema="SCHEMA1",
            source_table="TABLE1",
            source_column="COL1",
        )
        service.add_source_mapping(
            hierarchy_id=test_hierarchy.hierarchy_id,
            source_database="DB2",
            source_schema="SCHEMA2",
            source_table="TABLE2",
            source_column="COL2",
        )

        mappings = service.get_mappings(test_hierarchy.hierarchy_id)
        assert len(mappings) >= 2

    def test_remove_source_mapping(self, db_session, test_hierarchy):
        """Test removing a source mapping."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        mapping = service.add_source_mapping(
            hierarchy_id=test_hierarchy.hierarchy_id,
            source_database="REMOVE_DB",
            source_schema="REMOVE_SCHEMA",
            source_table="REMOVE_TABLE",
            source_column="REMOVE_COL",
        )

        result = service.remove_source_mapping(mapping.id)
        assert result is True

        mappings = service.get_mappings(test_hierarchy.hierarchy_id)
        mapping_ids = [m.id for m in mappings]
        assert mapping.id not in mapping_ids


class TestProjectStats:
    """Tests for project statistics."""

    def test_get_project_stats(self, db_session):
        """Test getting project statistics."""
        from src.hierarchy.service import HierarchyService

        service = HierarchyService(session=db_session, enable_audit=False)
        project = service.create_project(
            name=f"Stats Test {uuid.uuid4().hex[:8]}",
        )

        unique = uuid.uuid4().hex[:8]
        service.create_hierarchy(
            project_id=project.id,
            hierarchy_id=f"STAT1-{unique}",
            hierarchy_name="Stats Hierarchy 1",
        )
        service.create_hierarchy(
            project_id=project.id,
            hierarchy_id=f"STAT2-{unique}",
            hierarchy_name="Stats Hierarchy 2",
        )

        stats = service.get_project_stats(project.id)

        assert stats["project_name"] == project.name
        assert stats["hierarchy_count"] >= 2
