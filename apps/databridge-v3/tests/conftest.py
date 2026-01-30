"""
Pytest configuration and shared fixtures for DataBridge AI V3 tests.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Generator
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


@pytest.fixture(scope="session")
def test_db_path(tmp_path_factory) -> Path:
    """Create a temporary database path for testing."""
    return tmp_path_factory.mktemp("data") / "test_databridge.db"


@pytest.fixture(scope="session")
def db_engine(test_db_path):
    """Create test database engine."""
    from src.core.database import Base

    engine = create_engine(f"sqlite:///{test_db_path}", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session(db_engine) -> Generator[Session, None, None]:
    """Create database session for each test with transaction rollback."""
    connection = db_engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def sample_project_data() -> dict:
    """Sample project data for testing with unique name."""
    unique_id = str(uuid.uuid4())[:8]
    return {
        "id": str(uuid.uuid4()),
        "name": f"Test P&L Project {unique_id}",
        "description": "A test project for P&L hierarchy",
        "industry": "Manufacturing",
        "created_by": "test_user",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }


@pytest.fixture
def sample_hierarchy_data() -> dict:
    """Sample hierarchy data for testing."""
    return {
        "hierarchy_id": "TEST-001",
        "hierarchy_name": "Test Revenue",
        "parent_id": None,
        "description": "Total revenue category",
        "level_1": "Total Revenue",
        "level_2": "Product Sales",
        "level_3": "Hardware",
        "level_1_sort": 1,
        "level_2_sort": 1,
        "level_3_sort": 1,
        "include_flag": True,
        "exclude_flag": False,
        "transform_flag": False,
        "calculation_flag": False,
        "active_flag": True,
        "is_leaf_node": False,
        "sort_order": 1,
    }


@pytest.fixture
def sample_mapping_data() -> dict:
    """Sample source mapping data for testing."""
    return {
        "hierarchy_id": "TEST-001",
        "mapping_index": 0,
        "source_database": "ANALYTICS",
        "source_schema": "PUBLIC",
        "source_table": "FACT_SALES",
        "source_column": "AMOUNT",
        "source_uid": None,
        "precedence_group": "DEFAULT",
        "include_flag": True,
        "exclude_flag": False,
    }


@pytest.fixture
def sample_hierarchy_csv() -> str:
    """Sample hierarchy CSV content for import testing."""
    return """HIERARCHY_ID,HIERARCHY_NAME,PARENT_ID,DESCRIPTION,LEVEL_1,LEVEL_2,LEVEL_3,LEVEL_1_SORT,LEVEL_2_SORT,LEVEL_3_SORT,INCLUDE_FLAG,EXCLUDE_FLAG,SORT_ORDER
REV-001,Total Revenue,,Total revenue category,Revenue,,,1,,,TRUE,FALSE,1
REV-002,Product Sales,REV-001,Product revenue,Revenue,Product Sales,,1,1,,TRUE,FALSE,2
REV-003,Hardware,REV-002,Hardware sales,Revenue,Product Sales,Hardware,1,1,1,TRUE,FALSE,3
REV-004,Software,REV-002,Software sales,Revenue,Product Sales,Software,1,1,2,TRUE,FALSE,4"""


@pytest.fixture
def sample_mapping_csv() -> str:
    """Sample mapping CSV content for import testing."""
    return """HIERARCHY_ID,MAPPING_INDEX,SOURCE_DATABASE,SOURCE_SCHEMA,SOURCE_TABLE,SOURCE_COLUMN,SOURCE_UID,PRECEDENCE_GROUP,INCLUDE_FLAG,EXCLUDE_FLAG
REV-003,0,ANALYTICS,PUBLIC,FACT_SALES,AMOUNT,HW%,DEFAULT,TRUE,FALSE
REV-004,0,ANALYTICS,PUBLIC,FACT_SALES,AMOUNT,SW%,DEFAULT,TRUE,FALSE"""


@pytest.fixture
def temp_csv_file(sample_hierarchy_csv) -> Generator[Path, None, None]:
    """Create a temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as f:
        f.write(sample_hierarchy_csv)
        f.flush()
        yield Path(f.name)


@pytest.fixture
def sample_connection_config() -> dict:
    """Sample database connection configuration."""
    return {
        "name": "test-snowflake",
        "connection_type": "snowflake",
        "host": "test-account.snowflakecomputing.com",
        "port": 443,
        "database": "TEST_DB",
        "username": "test_user",
        "extra_config": {
            "warehouse": "TEST_WH",
            "role": "TEST_ROLE",
            "schema": "PUBLIC",
        },
    }


@pytest.fixture
def mock_snowflake_connection(mocker):
    """Mock Snowflake connection for testing without actual connection."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []
    return mock_conn


# Test markers
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "phase1: Foundation tests")
    config.addinivalue_line("markers", "phase2: Hierarchy module tests")
    config.addinivalue_line("markers", "phase3: Reconciliation tests")
    config.addinivalue_line("markers", "phase4: Templates and skills tests")
    config.addinivalue_line("markers", "phase5: Connection tests")
    config.addinivalue_line("markers", "smoke: Quick smoke tests")
    config.addinivalue_line("markers", "slow: Long-running tests")
