"""
Unit tests for the database module.
"""

import pytest
from datetime import datetime


class TestProjectModel:
    """Tests for Project model."""

    def test_create_project(self, db_session, sample_project_data):
        """Test creating a project."""
        from src.core.database import Project

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        assert project.id is not None
        assert project.name == sample_project_data["name"]
        assert project.industry == sample_project_data["industry"]

    def test_project_repr(self, db_session, sample_project_data):
        """Test project string representation."""
        from src.core.database import Project

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        repr_str = repr(project)
        assert "Project" in repr_str
        assert project.name in repr_str


class TestHierarchyModel:
    """Tests for Hierarchy model."""

    def test_create_hierarchy(self, db_session, sample_project_data, sample_hierarchy_data):
        """Test creating a hierarchy."""
        from src.core.database import Project, Hierarchy

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        hierarchy = Hierarchy(
            project_id=project.id,
            **sample_hierarchy_data,
        )
        db_session.add(hierarchy)
        db_session.commit()

        assert hierarchy.id is not None
        assert hierarchy.hierarchy_id == sample_hierarchy_data["hierarchy_id"]
        assert hierarchy.hierarchy_name == sample_hierarchy_data["hierarchy_name"]

    def test_hierarchy_get_level_path(self, db_session, sample_project_data, sample_hierarchy_data):
        """Test getting level path."""
        from src.core.database import Project, Hierarchy

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        hierarchy = Hierarchy(
            project_id=project.id,
            **sample_hierarchy_data,
        )
        db_session.add(hierarchy)
        db_session.commit()

        path = hierarchy.get_level_path()
        assert isinstance(path, list)
        assert len(path) == 3  # level_1, level_2, level_3
        assert path[0] == "Total Revenue"
        assert path[1] == "Product Sales"
        assert path[2] == "Hardware"

    def test_hierarchy_get_depth(self, db_session, sample_project_data, sample_hierarchy_data):
        """Test getting hierarchy depth."""
        from src.core.database import Project, Hierarchy

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        hierarchy = Hierarchy(
            project_id=project.id,
            **sample_hierarchy_data,
        )
        db_session.add(hierarchy)
        db_session.commit()

        depth = hierarchy.get_depth()
        assert depth == 3

    def test_hierarchy_parent_child_relationship(self, db_session, sample_project_data):
        """Test parent-child hierarchy relationship."""
        from src.core.database import Project, Hierarchy

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        # Create parent
        parent = Hierarchy(
            project_id=project.id,
            hierarchy_id="PARENT-001",
            hierarchy_name="Parent Node",
            level_1="Category",
        )
        db_session.add(parent)
        db_session.commit()

        # Create child
        child = Hierarchy(
            project_id=project.id,
            hierarchy_id="CHILD-001",
            hierarchy_name="Child Node",
            parent_id="PARENT-001",
            level_1="Category",
            level_2="Subcategory",
        )
        db_session.add(child)
        db_session.commit()

        assert child.parent_id == parent.hierarchy_id


class TestSourceMappingModel:
    """Tests for SourceMapping model."""

    def test_create_mapping(self, db_session, sample_project_data, sample_hierarchy_data, sample_mapping_data):
        """Test creating a source mapping."""
        from src.core.database import Project, Hierarchy, SourceMapping

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        hierarchy = Hierarchy(project_id=project.id, **sample_hierarchy_data)
        db_session.add(hierarchy)
        db_session.commit()

        mapping = SourceMapping(**sample_mapping_data)
        db_session.add(mapping)
        db_session.commit()

        assert mapping.id is not None
        assert mapping.hierarchy_id == sample_mapping_data["hierarchy_id"]
        assert mapping.source_table == sample_mapping_data["source_table"]

    def test_mapping_full_source_path(self, db_session, sample_project_data, sample_hierarchy_data, sample_mapping_data):
        """Test full source path property."""
        from src.core.database import Project, Hierarchy, SourceMapping

        project = Project(**sample_project_data)
        db_session.add(project)
        db_session.commit()

        hierarchy = Hierarchy(project_id=project.id, **sample_hierarchy_data)
        db_session.add(hierarchy)
        db_session.commit()

        mapping = SourceMapping(**sample_mapping_data)
        db_session.add(mapping)
        db_session.commit()

        path = mapping.full_source_path
        assert "ANALYTICS" in path
        assert "PUBLIC" in path
        assert "FACT_SALES" in path
        assert "AMOUNT" in path


class TestConnectionModel:
    """Tests for Connection model."""

    def test_create_connection(self, db_session, sample_connection_config):
        """Test creating a connection."""
        from src.core.database import Connection

        connection = Connection(**sample_connection_config)
        db_session.add(connection)
        db_session.commit()

        assert connection.id is not None
        assert connection.name == sample_connection_config["name"]
        assert connection.connection_type == sample_connection_config["connection_type"]


class TestSessionManagement:
    """Tests for session management functions."""

    def test_get_engine(self):
        """Test getting database engine."""
        from src.core.database import get_engine

        engine = get_engine()
        assert engine is not None

    def test_get_session(self):
        """Test getting database session."""
        from src.core.database import get_session

        session = get_session()
        assert session is not None
        session.close()

    def test_session_scope_success(self):
        """Test session scope with successful commit."""
        import uuid
        from src.core.database import session_scope, Project, init_database

        init_database()
        unique_name = f"Test Session Project {uuid.uuid4().hex[:8]}"

        with session_scope() as session:
            project = Project(name=unique_name, description="Test")
            session.add(project)
            # Commit happens automatically

    def test_session_scope_rollback(self):
        """Test session scope with rollback on error."""
        import uuid
        from src.core.database import session_scope, Project, init_database

        init_database()
        unique_name = f"Test Rollback Project {uuid.uuid4().hex[:8]}"

        try:
            with session_scope() as session:
                project = Project(name=unique_name, description="Test")
                session.add(project)
                raise ValueError("Test error")
        except ValueError:
            pass  # Expected

        # The project should not be in the database
        with session_scope() as session:
            count = session.query(Project).filter(Project.name == unique_name).count()
            assert count == 0


class TestInitDatabase:
    """Tests for database initialization."""

    def test_init_database(self):
        """Test initializing the database."""
        from src.core.database import init_database, get_engine

        init_database()

        # Check that tables exist
        engine = get_engine()
        from sqlalchemy import inspect

        inspector = inspect(engine)
        tables = inspector.get_table_names()

        assert "projects" in tables
        assert "hierarchies" in tables
        assert "source_mappings" in tables
        assert "connections" in tables
        assert "formula_groups" in tables
        assert "deployment_history" in tables
        assert "client_profiles" in tables
        assert "audit_log" in tables
